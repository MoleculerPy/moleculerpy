"""E2E tests for service discovery.

Tests verify that nodes discover each other and services become
available/unavailable correctly.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action
from moleculerpy.service import Service
from moleculerpy.settings import Settings

if TYPE_CHECKING:
    pass


class DynamicService(Service):
    """Service for testing dynamic registration."""

    name = "dynamic"

    def __init__(self, node_id: str):
        super().__init__(self.name)
        self._node_id = node_id

    @action()
    async def ping(self, ctx) -> dict:
        return {"node_id": self._node_id, "status": "ok"}


@pytest.mark.asyncio
@pytest.mark.e2e
class TestServiceDiscovery:
    """Tests for service discovery functionality."""

    async def test_services_discovered_on_start(
        self, two_broker_cluster: list[ServiceBroker]
    ) -> None:
        """Services are discovered when brokers start."""
        broker = two_broker_cluster[0]

        # Check that actions are registered
        assert broker.registry.get_action("counter.increment") is not None
        assert broker.registry.get_action("echo.echo") is not None

    async def test_remote_services_accessible(
        self, two_broker_cluster: list[ServiceBroker]
    ) -> None:
        """Can call services on remote nodes."""
        broker = two_broker_cluster[0]

        # Call counter multiple times to hit both nodes
        seen_nodes: set[str] = set()
        for _ in range(10):
            result = await broker.call("counter.increment", {})
            seen_nodes.add(result["node_id"])

        # Should see both nodes (service available on both)
        assert len(seen_nodes) == 2

    async def test_node_catalog_updated(self, two_broker_cluster: list[ServiceBroker]) -> None:
        """Node catalog contains all cluster nodes."""
        broker = two_broker_cluster[0]

        nodes = broker.node_catalog.nodes
        # Should have both nodes (self + remote)
        assert len(nodes) >= 2

    async def test_new_node_joins_cluster(self) -> None:
        """New node can join existing cluster."""
        # Start first broker
        settings1 = Settings(transporter="memory://", prefer_local=False)
        broker1 = ServiceBroker(id="existing-node", settings=settings1)
        await broker1.register(DynamicService("existing-node"))
        await broker1.start()

        # Wait for startup
        await asyncio.sleep(0.1)

        # Verify only one node initially
        result1 = await broker1.call("dynamic.ping", {})
        assert result1["node_id"] == "existing-node"

        # Start second broker
        settings2 = Settings(transporter="memory://", prefer_local=False)
        broker2 = ServiceBroker(id="new-node", settings=settings2)
        await broker2.register(DynamicService("new-node"))
        await broker2.start()

        # Wait for discovery
        await asyncio.sleep(0.3)

        try:
            # Now should see both nodes
            seen_nodes: set[str] = set()
            for _ in range(10):
                result = await broker1.call("dynamic.ping", {})
                seen_nodes.add(result["node_id"])

            assert len(seen_nodes) == 2, f"Expected 2 nodes, got {seen_nodes}"

        finally:
            await broker1.stop()
            await broker2.stop()


@pytest.mark.asyncio
@pytest.mark.e2e
class TestNodeDisconnect:
    """Tests for node disconnect handling."""

    async def test_node_disconnect_removes_endpoints(self) -> None:
        """When node disconnects, its endpoints are removed."""
        # Start two brokers
        settings = Settings(transporter="memory://", prefer_local=False)
        broker1 = ServiceBroker(id="permanent-node", settings=settings)
        broker2 = ServiceBroker(id="temporary-node", settings=settings)

        await broker1.register(DynamicService("permanent-node"))
        await broker2.register(DynamicService("temporary-node"))

        await broker1.start()
        await broker2.start()

        # Wait for discovery
        await asyncio.sleep(0.3)

        try:
            # Verify both nodes accessible
            seen_before: set[str] = set()
            for _ in range(10):
                result = await broker1.call("dynamic.ping", {})
                seen_before.add(result["node_id"])
            assert len(seen_before) == 2

            # Stop one broker
            await broker2.stop()

            # Wait for disconnect detection
            await asyncio.sleep(0.3)

            # Now only permanent node should respond
            for _ in range(5):
                result = await broker1.call("dynamic.ping", {})
                assert result["node_id"] == "permanent-node"

        finally:
            await broker1.stop()


@pytest.mark.asyncio
@pytest.mark.e2e
class TestMultipleServices:
    """Tests for multiple services on nodes."""

    async def test_multiple_services_per_node(
        self, three_broker_cluster: list[ServiceBroker]
    ) -> None:
        """Each node can host multiple services."""
        broker = three_broker_cluster[0]

        # All services should be accessible
        echo_result = await broker.call("echo.echo", {"test": 1})
        counter_result = await broker.call("counter.increment", {})
        shard_result = await broker.call("shard-test.store", {"id": "key", "value": "val"})

        assert "node_id" in echo_result
        assert "count" in counter_result
        assert "stored" in shard_result

    async def test_service_actions_registered(
        self, three_broker_cluster: list[ServiceBroker]
    ) -> None:
        """All service actions are registered in registry."""
        broker = three_broker_cluster[0]

        # Verify expected actions exist
        expected_actions = [
            "echo.echo",
            "echo.slow_echo",
            "counter.increment",
            "counter.get_count",
            "counter.get_node_id",
            "shard-test.store",
            "shard-test.retrieve",
        ]

        for action_name in expected_actions:
            assert broker.registry.get_action(action_name) is not None, (
                f"Missing action: {action_name}"
            )
