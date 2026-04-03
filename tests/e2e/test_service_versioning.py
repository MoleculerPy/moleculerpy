"""E2E tests for service versioning (v0.14.7 — PRD-011).

Tests versioned services in real multi-broker clusters using MemoryTransporter.
Validates that version routing, coexistence, and discovery work end-to-end
without mocks.
"""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action
from moleculerpy.service import Service
from moleculerpy.settings import Settings

# ---------------------------------------------------------------------------
# Versioned test services
# ---------------------------------------------------------------------------


class UserServiceV1(Service):
    """Users service version 1."""

    name = "users"
    version = 1

    @action()
    async def get(self, ctx) -> dict:  # type: ignore[no-untyped-def]
        return {"version": 1, "id": ctx.params.get("id", 0)}

    @action()
    async def list(self, ctx) -> dict:  # type: ignore[no-untyped-def]
        return {"version": 1, "users": ["alice", "bob"]}


class UserServiceV2(Service):
    """Users service version 2 — new response format."""

    name = "users"
    version = 2

    @action()
    async def get(self, ctx) -> dict:  # type: ignore[no-untyped-def]
        return {"version": 2, "user": {"id": ctx.params.get("id", 0), "name": "enhanced"}}

    @action()
    async def list(self, ctx) -> dict:  # type: ignore[no-untyped-def]
        return {"version": 2, "data": [{"name": "alice"}, {"name": "bob"}], "total": 2}


class OrderServiceStaging(Service):
    """Orders service with string version."""

    name = "orders"
    version = "staging"

    @action()
    async def create(self, ctx) -> dict:  # type: ignore[no-untyped-def]
        return {"env": "staging", "order_id": 42}


class UnversionedService(Service):
    """Plain service without version — backwards compatibility."""

    name = "math"

    @action()
    async def add(self, ctx) -> int:  # type: ignore[no-untyped-def]
        return ctx.params["a"] + ctx.params["b"]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def versioned_broker():
    """Single broker with multiple versioned services."""
    settings = Settings(transporter="memory://", prefer_local=False)
    broker = ServiceBroker(id="versioned-node", settings=settings)

    await broker.register(UserServiceV1())
    await broker.register(UserServiceV2())
    await broker.register(OrderServiceStaging())
    await broker.register(UnversionedService())
    await broker.start()

    await asyncio.sleep(0.1)

    yield broker

    await broker.stop()


@pytest_asyncio.fixture
async def versioned_cluster():
    """Two brokers: node-1 has v1, node-2 has v2."""
    settings1 = Settings(transporter="memory://", prefer_local=False)
    broker1 = ServiceBroker(id="node-1", settings=settings1)
    await broker1.register(UserServiceV1())
    await broker1.register(UnversionedService())

    settings2 = Settings(transporter="memory://", prefer_local=False)
    broker2 = ServiceBroker(id="node-2", settings=settings2)
    await broker2.register(UserServiceV2())

    await broker1.start()
    await broker2.start()

    await asyncio.sleep(0.3)

    yield broker1, broker2

    await broker2.stop()
    await broker1.stop()


# ---------------------------------------------------------------------------
# Tests — Single broker, multiple versions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.e2e
class TestVersionedSingleBroker:
    """Test versioned services on a single broker."""

    async def test_call_v1(self, versioned_broker: ServiceBroker) -> None:
        """broker.call('v1.users.get') routes to v1."""
        result = await versioned_broker.call("v1.users.get", {"id": 1})
        assert result["version"] == 1
        assert result["id"] == 1

    async def test_call_v2(self, versioned_broker: ServiceBroker) -> None:
        """broker.call('v2.users.get') routes to v2."""
        result = await versioned_broker.call("v2.users.get", {"id": 1})
        assert result["version"] == 2
        assert result["user"]["id"] == 1
        assert result["user"]["name"] == "enhanced"

    async def test_v1_and_v2_return_different_results(
        self, versioned_broker: ServiceBroker
    ) -> None:
        """Same action name, different versions, different results."""
        r1 = await versioned_broker.call("v1.users.list")
        r2 = await versioned_broker.call("v2.users.list")

        assert r1["version"] == 1
        assert r2["version"] == 2
        assert isinstance(r1["users"], list)
        assert isinstance(r2["data"], list)
        assert r2["total"] == 2

    async def test_string_version(self, versioned_broker: ServiceBroker) -> None:
        """String version 'staging' works: staging.orders.create."""
        result = await versioned_broker.call("staging.orders.create")
        assert result["env"] == "staging"
        assert result["order_id"] == 42

    async def test_unversioned_service(self, versioned_broker: ServiceBroker) -> None:
        """Unversioned service works alongside versioned ones."""
        result = await versioned_broker.call("math.add", {"a": 3, "b": 4})
        assert result == 7

    async def test_plain_name_does_not_match_versioned(
        self, versioned_broker: ServiceBroker
    ) -> None:
        """'users.get' (without version) should fail — only versioned names exist."""
        from moleculerpy.errors import ServiceNotFoundError

        with pytest.raises(ServiceNotFoundError):
            await versioned_broker.call("users.get")


# ---------------------------------------------------------------------------
# Tests — Multi-broker cluster, versions on different nodes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.e2e
class TestVersionedCluster:
    """Test versioned services across brokers via MemoryTransporter."""

    async def test_cross_node_v1_call(
        self, versioned_cluster: tuple[ServiceBroker, ServiceBroker]
    ) -> None:
        """Node-2 can call v1.users.get on node-1."""
        _, broker2 = versioned_cluster
        result = await broker2.call("v1.users.get", {"id": 5})
        assert result["version"] == 1
        assert result["id"] == 5

    async def test_cross_node_v2_call(
        self, versioned_cluster: tuple[ServiceBroker, ServiceBroker]
    ) -> None:
        """Node-1 can call v2.users.get on node-2."""
        broker1, _ = versioned_cluster
        result = await broker1.call("v2.users.get", {"id": 10})
        assert result["version"] == 2
        assert result["user"]["id"] == 10

    async def test_both_versions_from_any_node(
        self, versioned_cluster: tuple[ServiceBroker, ServiceBroker]
    ) -> None:
        """Both versions are accessible from any broker."""
        broker1, broker2 = versioned_cluster

        # From broker1
        r1 = await broker1.call("v1.users.get", {"id": 1})
        r2 = await broker1.call("v2.users.get", {"id": 1})
        assert r1["version"] == 1
        assert r2["version"] == 2

        # From broker2
        r1 = await broker2.call("v1.users.get", {"id": 1})
        r2 = await broker2.call("v2.users.get", {"id": 1})
        assert r1["version"] == 1
        assert r2["version"] == 2

    async def test_unversioned_cross_node(
        self, versioned_cluster: tuple[ServiceBroker, ServiceBroker]
    ) -> None:
        """Unversioned math service on node-1 is callable from node-2."""
        _, broker2 = versioned_cluster
        result = await broker2.call("math.add", {"a": 10, "b": 20})
        assert result == 30
