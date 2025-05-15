"""Unit tests for internal events ($node.*, $broker.*, $transporter.*).

Tests cover:
- $broker.started and $broker.stopped events
- $node.connected, $node.disconnected, $node.updated events
- $transporter.connected and $transporter.disconnected events
- Event payload structure (Moleculer.js compatible)
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from moleculerpy.broker import ServiceBroker
from moleculerpy.node import Node, NodeCatalog
from moleculerpy.registry import Registry
from moleculerpy.settings import Settings


class TestBrokerInternalEvents:
    """Tests for $broker.* internal events."""

    @pytest.mark.asyncio
    async def test_broker_started_event(self) -> None:
        """$broker.started event is emitted on broker.start()."""
        settings = Settings(transporter="memory://")
        broker = ServiceBroker(id="test-node", settings=settings)

        received_events: list[dict[str, Any]] = []

        async def on_started(payload: Any) -> None:
            received_events.append({"event": "$broker.started", "payload": payload})

        broker.local_bus.on("$broker.started", on_started)

        await broker.start()

        # Allow event to be processed
        await asyncio.sleep(0.05)

        assert len(received_events) == 1
        assert received_events[0]["event"] == "$broker.started"
        assert received_events[0]["payload"] == {}

        await broker.stop()

    @pytest.mark.asyncio
    async def test_broker_stopped_event(self) -> None:
        """$broker.stopped event is emitted on broker.stop()."""
        settings = Settings(transporter="memory://")
        broker = ServiceBroker(id="test-node", settings=settings)

        received_events: list[dict[str, Any]] = []

        async def on_stopped(payload: Any) -> None:
            received_events.append({"event": "$broker.stopped", "payload": payload})

        await broker.start()

        broker.local_bus.on("$broker.stopped", on_stopped)

        await broker.stop()

        # Allow event to be processed
        await asyncio.sleep(0.05)

        assert len(received_events) == 1
        assert received_events[0]["event"] == "$broker.stopped"
        assert received_events[0]["payload"] == {}

    @pytest.mark.asyncio
    async def test_broadcast_local_returns_result(self) -> None:
        """broadcast_local() returns True when handlers called."""
        settings = Settings(transporter="memory://")
        broker = ServiceBroker(id="test-node", settings=settings)

        received = []
        broker.local_bus.on("$test.event", received.append)

        result = await broker.broadcast_local("$test.event", {"data": 123})

        assert result is True
        assert received == [{"data": 123}]

    @pytest.mark.asyncio
    async def test_broadcast_local_no_handlers(self) -> None:
        """broadcast_local() returns False when no handlers."""
        settings = Settings(transporter="memory://")
        broker = ServiceBroker(id="test-node", settings=settings)

        result = await broker.broadcast_local("$unknown.event", None)

        assert result is False


class TestNodeCatalogInternalEvents:
    """Tests for $node.* internal events."""

    def _create_catalog_with_broker(self) -> tuple[NodeCatalog, ServiceBroker]:
        """Create a NodeCatalog with a real broker for testing."""
        settings = Settings(transporter="memory://")
        broker = ServiceBroker(id="local-node", settings=settings)
        # The broker already creates node_catalog with broker reference
        return broker.node_catalog, broker

    @pytest.mark.asyncio
    async def test_node_connected_event_new_node(self) -> None:
        """$node.connected emitted for new node with reconnected=False."""
        catalog, broker = self._create_catalog_with_broker()
        received_events: list[dict[str, Any]] = []

        async def on_connected(payload: Any) -> None:
            received_events.append(payload)

        broker.local_bus.on("$node.connected", on_connected)

        # Process new node info
        catalog.process_node_info(
            "new-node-1",
            {
                "cpu": 25.5,
                "services": [],
                "hostname": "node1.local",
            },
        )

        # Allow event to be processed
        await asyncio.sleep(0.05)

        assert len(received_events) == 1
        assert received_events[0]["reconnected"] is False
        assert received_events[0]["node"].id == "new-node-1"

    @pytest.mark.asyncio
    async def test_node_connected_event_reconnected(self) -> None:
        """$node.connected emitted for reconnected node with reconnected=True."""
        catalog, broker = self._create_catalog_with_broker()
        received_events: list[dict[str, Any]] = []

        async def on_connected(payload: Any) -> None:
            received_events.append(payload)

        broker.local_bus.on("$node.connected", on_connected)

        # First connect the node
        catalog.process_node_info("reconnect-node", {"cpu": 10, "services": []})
        await asyncio.sleep(0.02)

        # Mark as disconnected (but keep in catalog)
        node = catalog.get_node("reconnect-node")
        node.available = False

        # Clear previous events
        received_events.clear()

        # Reconnect
        catalog.process_node_info("reconnect-node", {"cpu": 15, "services": []})
        await asyncio.sleep(0.05)

        assert len(received_events) == 1
        assert received_events[0]["reconnected"] is True
        assert received_events[0]["node"].id == "reconnect-node"

    @pytest.mark.asyncio
    async def test_node_updated_event(self) -> None:
        """$node.updated emitted for existing available node."""
        catalog, broker = self._create_catalog_with_broker()
        received_events: list[dict[str, Any]] = []

        async def on_updated(payload: Any) -> None:
            received_events.append(payload)

        broker.local_bus.on("$node.updated", on_updated)

        # First connect
        catalog.process_node_info("update-node", {"cpu": 10, "services": []})
        await asyncio.sleep(0.02)

        # Update info (node is available)
        catalog.process_node_info("update-node", {"cpu": 50, "services": []})
        await asyncio.sleep(0.05)

        assert len(received_events) == 1
        assert received_events[0]["node"].id == "update-node"
        assert received_events[0]["node"].cpu == 50

    @pytest.mark.asyncio
    async def test_node_disconnected_event_unexpected(self) -> None:
        """$node.disconnected emitted with unexpected=True by default."""
        catalog, broker = self._create_catalog_with_broker()
        received_events: list[dict[str, Any]] = []

        async def on_disconnected(payload: Any) -> None:
            received_events.append(payload)

        broker.local_bus.on("$node.disconnected", on_disconnected)

        # First add node
        catalog.process_node_info("disconnect-node", {"cpu": 10, "services": []})
        await asyncio.sleep(0.02)

        # Disconnect (unexpected)
        catalog.disconnect_node("disconnect-node")
        await asyncio.sleep(0.05)

        assert len(received_events) == 1
        assert received_events[0]["unexpected"] is True
        assert received_events[0]["node"].id == "disconnect-node"

    @pytest.mark.asyncio
    async def test_node_disconnected_event_graceful(self) -> None:
        """$node.disconnected emitted with unexpected=False for graceful."""
        catalog, broker = self._create_catalog_with_broker()
        received_events: list[dict[str, Any]] = []

        async def on_disconnected(payload: Any) -> None:
            received_events.append(payload)

        broker.local_bus.on("$node.disconnected", on_disconnected)

        # First add node
        catalog.process_node_info("graceful-node", {"cpu": 10, "services": []})
        await asyncio.sleep(0.02)

        # Disconnect gracefully
        catalog.disconnect_node("graceful-node", unexpected=False)
        await asyncio.sleep(0.05)

        assert len(received_events) == 1
        assert received_events[0]["unexpected"] is False


class TestNodeCatalogWithoutBroker:
    """Tests for NodeCatalog without broker (no events emitted)."""

    def test_process_node_info_without_broker(self) -> None:
        """process_node_info works without broker (no events)."""
        registry = Registry(logger=MagicMock(), prefer_local=True)
        catalog = NodeCatalog(
            registry=registry,
            logger=MagicMock(),
            node_id="local",
            broker=None,  # No broker
        )

        # Should not raise
        catalog.process_node_info("remote-node", {"cpu": 10, "services": []})

        node = catalog.get_node("remote-node")
        assert node is not None
        assert node.cpu == 10

    def test_disconnect_node_without_broker(self) -> None:
        """disconnect_node works without broker (no events)."""
        registry = Registry(logger=MagicMock(), prefer_local=True)
        catalog = NodeCatalog(
            registry=registry,
            logger=MagicMock(),
            node_id="local",
            broker=None,
        )

        catalog.process_node_info("temp-node", {"cpu": 5, "services": []})
        # Should not raise
        catalog.disconnect_node("temp-node")

        assert catalog.get_node("temp-node") is None


class TestTransporterInternalEvents:
    """Tests for $transporter.* internal events."""

    @pytest.mark.asyncio
    async def test_transporter_connected_event(self) -> None:
        """$transporter.connected emitted on transit.connect()."""
        settings = Settings(transporter="memory://")
        broker = ServiceBroker(id="test-node", settings=settings)

        received_events: list[dict[str, Any]] = []

        async def on_connected(payload: Any) -> None:
            received_events.append(payload)

        broker.local_bus.on("$transporter.connected", on_connected)

        await broker.start()
        await asyncio.sleep(0.05)

        # Should have connected event
        assert len(received_events) >= 1
        assert received_events[0]["wasReconnect"] is False

        await broker.stop()

    @pytest.mark.asyncio
    async def test_transporter_disconnected_event(self) -> None:
        """$transporter.disconnected emitted on transit.disconnect()."""
        settings = Settings(transporter="memory://")
        broker = ServiceBroker(id="test-node", settings=settings)

        received_events: list[dict[str, Any]] = []

        async def on_disconnected(payload: Any) -> None:
            received_events.append(payload)

        await broker.start()

        broker.local_bus.on("$transporter.disconnected", on_disconnected)

        await broker.stop()
        await asyncio.sleep(0.05)

        assert len(received_events) == 1
        assert received_events[0]["graceful"] is True


class TestWildcardInternalEvents:
    """Tests for wildcard subscriptions to internal events."""

    @pytest.mark.asyncio
    async def test_subscribe_all_node_events(self) -> None:
        """Can subscribe to all $node.* events with wildcard."""
        settings = Settings(transporter="memory://")
        broker = ServiceBroker(id="test-node", settings=settings)

        received_events: list[str] = []

        def on_node_event(payload: Any) -> None:
            # Determine event type from payload structure
            if "reconnected" in payload:
                received_events.append("$node.connected")
            elif "unexpected" in payload:
                received_events.append("$node.disconnected")
            else:
                received_events.append("$node.updated")

        broker.local_bus.on("$node.*", on_node_event)

        await broker.start()

        # Trigger node events
        broker.node_catalog.process_node_info("ext-node", {"cpu": 10, "services": []})
        await asyncio.sleep(0.03)

        broker.node_catalog.process_node_info("ext-node", {"cpu": 20, "services": []})
        await asyncio.sleep(0.03)

        broker.node_catalog.disconnect_node("ext-node")
        await asyncio.sleep(0.03)

        await broker.stop()

        assert "$node.connected" in received_events
        assert "$node.updated" in received_events
        assert "$node.disconnected" in received_events

    @pytest.mark.asyncio
    async def test_subscribe_all_internal_events(self) -> None:
        """Can subscribe to all internal events with $**."""
        settings = Settings(transporter="memory://")
        broker = ServiceBroker(id="test-node", settings=settings)

        event_count = [0]

        def on_any_internal(payload: Any) -> None:
            event_count[0] += 1

        broker.local_bus.on("$**", on_any_internal)

        await broker.start()
        await asyncio.sleep(0.05)
        await broker.stop()
        await asyncio.sleep(0.05)

        # Should have received multiple internal events
        # $broker.started, $transporter.connected, $broker.stopped, $transporter.disconnected
        assert event_count[0] >= 2  # At minimum started and stopped


class TestMoleculerCompatibility:
    """Tests for Moleculer.js payload compatibility."""

    @pytest.mark.asyncio
    async def test_node_connected_payload_structure(self) -> None:
        """$node.connected payload matches Moleculer.js structure."""
        catalog, broker = TestNodeCatalogInternalEvents()._create_catalog_with_broker()
        received: list[dict[str, Any]] = []

        broker.local_bus.on("$node.connected", received.append)

        catalog.process_node_info("compat-node", {"cpu": 10, "services": []})
        await asyncio.sleep(0.05)

        assert len(received) == 1
        payload = received[0]

        # Moleculer.js compatible structure
        assert "node" in payload
        assert "reconnected" in payload
        assert isinstance(payload["node"], Node)
        assert isinstance(payload["reconnected"], bool)

    @pytest.mark.asyncio
    async def test_node_disconnected_payload_structure(self) -> None:
        """$node.disconnected payload matches Moleculer.js structure."""
        catalog, broker = TestNodeCatalogInternalEvents()._create_catalog_with_broker()
        received: list[dict[str, Any]] = []

        broker.local_bus.on("$node.disconnected", received.append)

        catalog.process_node_info("compat-node-2", {"cpu": 10, "services": []})
        await asyncio.sleep(0.02)
        catalog.disconnect_node("compat-node-2")
        await asyncio.sleep(0.05)

        assert len(received) == 1
        payload = received[0]

        # Moleculer.js compatible structure
        assert "node" in payload
        assert "unexpected" in payload
        assert isinstance(payload["node"], Node)
        assert isinstance(payload["unexpected"], bool)

    @pytest.mark.asyncio
    async def test_transporter_connected_payload_structure(self) -> None:
        """$transporter.connected payload matches Moleculer.js structure."""
        settings = Settings(transporter="memory://")
        broker = ServiceBroker(id="test-node", settings=settings)
        received: list[dict[str, Any]] = []

        broker.local_bus.on("$transporter.connected", received.append)

        await broker.start()
        await asyncio.sleep(0.05)

        assert len(received) >= 1
        payload = received[0]

        # Moleculer.js compatible structure
        assert "wasReconnect" in payload
        assert isinstance(payload["wasReconnect"], bool)

        await broker.stop()
