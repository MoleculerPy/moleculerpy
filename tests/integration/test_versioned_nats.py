"""Integration test: versioned services over real NATS transporter.

Two Python brokers connect to a real NATS server (localhost:4222).
Broker1 runs v1.users, broker2 runs v2.users.
Each broker calls the other's versioned action through NATS.

Requirements:
    - NATS server running on localhost:4222
    - Run: pytest tests/integration/test_versioned_nats.py -v

This test validates:
    - Service versioning works over real NATS (not just MemoryTransporter)
    - INFO packets correctly advertise version/fullName
    - Service discovery resolves versioned action names across nodes
    - Two versions of the same service coexist in a cluster
"""

from __future__ import annotations

import asyncio

import pytest

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action
from moleculerpy.service import Service
from moleculerpy.settings import Settings

NATS_URL = "nats://localhost:4222"


# ---------------------------------------------------------------------------
# Check NATS availability
# ---------------------------------------------------------------------------


def nats_available() -> bool:
    """Check if NATS is reachable."""
    import socket

    try:
        sock = socket.create_connection(("localhost", 4222), timeout=2)
        sock.close()
        return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not nats_available(), reason="NATS is not reachable on localhost:4222"
)


# ---------------------------------------------------------------------------
# Versioned services
# ---------------------------------------------------------------------------


class UserServiceV1(Service):
    """Users API v1 — simple format."""

    name = "users"
    version = 1

    @action()
    async def get(self, ctx) -> dict:  # type: ignore[no-untyped-def]
        return {"version": 1, "id": ctx.params.get("id", 0), "name": "alice"}

    @action()
    async def info(self, ctx) -> dict:  # type: ignore[no-untyped-def]
        return {"api": "v1", "node": self.broker.nodeID if self.broker else "unknown"}


class UserServiceV2(Service):
    """Users API v2 — enhanced format."""

    name = "users"
    version = 2

    @action()
    async def get(self, ctx) -> dict:  # type: ignore[no-untyped-def]
        return {
            "version": 2,
            "user": {
                "id": ctx.params.get("id", 0),
                "name": "alice",
                "email": "alice@example.com",
            },
        }

    @action()
    async def info(self, ctx) -> dict:  # type: ignore[no-untyped-def]
        return {"api": "v2", "node": self.broker.nodeID if self.broker else "unknown"}


class HealthService(Service):
    """Unversioned health service — backwards compat check."""

    name = "health"

    @action()
    async def check(self, ctx) -> dict:  # type: ignore[no-untyped-def]
        return {"status": "ok", "node": self.broker.nodeID if self.broker else "unknown"}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_versioned_services_over_nats() -> None:
    """Two Python brokers with versioned services communicate over real NATS.

    Topology:
        broker1 (py-v1-node) → v1.users + health
        broker2 (py-v2-node) → v2.users

    Calls:
        broker2 → v1.users.get (on broker1 via NATS)
        broker1 → v2.users.get (on broker2 via NATS)
        broker2 → health.check (on broker1 via NATS)
    """
    # Create broker1 with v1 services
    settings1 = Settings(transporter=NATS_URL)
    broker1 = ServiceBroker(id="py-v1-node", settings=settings1)
    await broker1.register(UserServiceV1())
    await broker1.register(HealthService())

    # Create broker2 with v2 service
    settings2 = Settings(transporter=NATS_URL)
    broker2 = ServiceBroker(id="py-v2-node", settings=settings2)
    await broker2.register(UserServiceV2())

    try:
        # Start both brokers
        await broker1.start()
        await broker2.start()

        # Wait for service discovery over NATS
        await asyncio.sleep(3)

        # --- Test 1: broker2 calls v1.users.get on broker1 ---
        result = await broker2.call("v1.users.get", {"id": 42})
        assert result["version"] == 1
        assert result["id"] == 42
        assert result["name"] == "alice"
        print(f"✓ broker2 → v1.users.get: {result}")

        # --- Test 2: broker1 calls v2.users.get on broker2 ---
        result = await broker1.call("v2.users.get", {"id": 99})
        assert result["version"] == 2
        assert result["user"]["id"] == 99
        assert result["user"]["email"] == "alice@example.com"
        print(f"✓ broker1 → v2.users.get: {result}")

        # --- Test 3: Cross-check — v1.info shows it came from broker1 ---
        result = await broker2.call("v1.users.info")
        assert result["api"] == "v1"
        assert result["node"] == "py-v1-node"
        print(f"✓ broker2 → v1.users.info: {result}")

        # --- Test 4: Cross-check — v2.info shows it came from broker2 ---
        result = await broker1.call("v2.users.info")
        assert result["api"] == "v2"
        assert result["node"] == "py-v2-node"
        print(f"✓ broker1 → v2.users.info: {result}")

        # --- Test 5: Unversioned service over NATS ---
        result = await broker2.call("health.check")
        assert result["status"] == "ok"
        assert result["node"] == "py-v1-node"
        print(f"✓ broker2 → health.check: {result}")

        # --- Test 6: Local + remote in same call sequence ---
        # broker1 has v1 locally, calls v2 remotely
        local_result = await broker1.call("v1.users.get", {"id": 1})
        remote_result = await broker1.call("v2.users.get", {"id": 1})
        assert local_result["version"] == 1
        assert remote_result["version"] == 2
        print("✓ broker1 local v1 + remote v2: both work")

        print("\n" + "=" * 60)
        print("ALL 6 NATS INTEGRATION TESTS PASSED")
        print("=" * 60)

    finally:
        await broker2.stop()
        await broker1.stop()


@pytest.mark.asyncio
async def test_versioned_service_not_found_over_nats() -> None:
    """Calling unversioned 'users.get' fails when only versioned exists."""
    from moleculerpy.errors import ServiceNotFoundError

    settings = Settings(transporter=NATS_URL)
    broker = ServiceBroker(id="py-test-notfound", settings=settings)
    await broker.register(UserServiceV1())

    try:
        await broker.start()
        await asyncio.sleep(1)

        # "users.get" (without version) should not resolve
        with pytest.raises(ServiceNotFoundError):
            await broker.call("users.get")

        print("✓ 'users.get' correctly raises ServiceNotFoundError")

    finally:
        await broker.stop()
