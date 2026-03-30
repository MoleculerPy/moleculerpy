"""Tests for MemoryTransporter.

Phase 3C: Infrastructure - Memory Transporter for dev/testing.
"""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from moleculerpy.packet import Packet, Topic
from moleculerpy.serializers import JsonSerializer
from moleculerpy.transporter.memory import (
    MemoryTransporter,
    _MemoryBus,
    get_global_bus,
    reset_global_bus,
)


@pytest.fixture(autouse=True)
def clean_bus():
    """Reset global bus before and after each test."""
    reset_global_bus()
    yield
    reset_global_bus()


@pytest.fixture
def mock_transit():
    """Create a mock transit instance."""
    transit = Mock()
    transit.broker = Mock()
    transit.broker.nodeID = "test-node"
    transit.serializer = JsonSerializer()
    return transit


@pytest.fixture
def mock_handler():
    """Create a mock async handler."""
    return AsyncMock()


class TestMemoryBus:
    """Tests for _MemoryBus internal class."""

    @pytest.mark.asyncio
    async def test_subscribe_and_emit(self) -> None:
        """Bus should deliver messages to subscribers."""
        bus = _MemoryBus()
        received = []

        async def handler(topic: str, data: bytes) -> None:
            received.append((topic, data))

        await bus.subscribe("test.topic", handler, "sub-1")
        await bus.emit("test.topic", b"hello")

        # Wait for async task to complete
        await asyncio.sleep(0.01)

        assert len(received) == 1
        assert received[0] == ("test.topic", b"hello")

    @pytest.mark.asyncio
    async def test_multiple_subscribers(self) -> None:
        """Bus should deliver to all subscribers."""
        bus = _MemoryBus()
        received = []

        async def handler1(topic: str, data: bytes) -> None:
            received.append(("h1", data))

        async def handler2(topic: str, data: bytes) -> None:
            received.append(("h2", data))

        await bus.subscribe("test.topic", handler1, "sub-1")
        await bus.subscribe("test.topic", handler2, "sub-2")
        await bus.emit("test.topic", b"message")

        await asyncio.sleep(0.01)

        assert len(received) == 2
        assert ("h1", b"message") in received
        assert ("h2", b"message") in received

    @pytest.mark.asyncio
    async def test_unsubscribe(self) -> None:
        """Bus should remove subscriber on unsubscribe."""
        bus = _MemoryBus()
        received = []

        async def handler(topic: str, data: bytes) -> None:
            received.append(data)

        await bus.subscribe("test.topic", handler, "sub-1")
        await bus.unsubscribe("sub-1")
        await bus.emit("test.topic", b"message")

        await asyncio.sleep(0.01)

        assert len(received) == 0

    @pytest.mark.asyncio
    async def test_get_subscriber_count(self) -> None:
        """Bus should track subscriber count."""
        bus = _MemoryBus()

        async def handler(topic: str, data: bytes) -> None:
            pass

        assert bus.get_subscriber_count("test.topic") == 0

        await bus.subscribe("test.topic", handler, "sub-1")
        assert bus.get_subscriber_count("test.topic") == 1

        await bus.subscribe("test.topic", handler, "sub-2")
        assert bus.get_subscriber_count("test.topic") == 2

        await bus.unsubscribe("sub-1")
        assert bus.get_subscriber_count("test.topic") == 1

    @pytest.mark.asyncio
    async def test_emit_to_nonexistent_topic(self) -> None:
        """Emit to nonexistent topic should not raise."""
        bus = _MemoryBus()
        # Should not raise
        await bus.emit("nonexistent", b"data")

    @pytest.mark.asyncio
    async def test_emit_does_not_wait_for_subscription_lock(self) -> None:
        """Emit hot path should not block on subscribe/unsubscribe lock."""
        bus = _MemoryBus()
        received = []

        async def handler(topic: str, data: bytes) -> None:
            received.append(data)

        await bus.subscribe("test.topic", handler, "sub-1")
        await bus._lock.acquire()
        try:
            await asyncio.wait_for(bus.emit("test.topic", b"hello"), timeout=0.05)
        finally:
            bus._lock.release()

        await asyncio.sleep(0.01)
        assert received == [b"hello"]

    @pytest.mark.asyncio
    async def test_task_tracking(self) -> None:
        """Bus should track running handler tasks."""
        bus = _MemoryBus()
        completed = []

        async def slow_handler(topic: str, data: bytes) -> None:
            await asyncio.sleep(0.1)
            completed.append(data)

        await bus.subscribe("test.topic", slow_handler, "sub-1")

        # Before emit - no tasks
        assert bus.get_running_task_count() == 0

        # Emit message
        await bus.emit("test.topic", b"test")

        # Tasks are created (may or may not be running yet)
        # Wait for completion
        await bus.wait_for_pending_tasks(timeout=1.0)

        # After completion - task count goes to 0 (WeakSet cleans up)
        assert len(completed) == 1

    @pytest.mark.asyncio
    async def test_wait_for_pending_tasks(self) -> None:
        """wait_for_pending_tasks should wait for handlers to complete."""
        bus = _MemoryBus()
        results = []

        async def handler(topic: str, data: bytes) -> None:
            await asyncio.sleep(0.05)
            results.append(data)

        await bus.subscribe("test.topic", handler, "sub-1")
        await bus.emit("test.topic", b"msg1")
        await bus.emit("test.topic", b"msg2")

        # Should wait for all handlers
        await bus.wait_for_pending_tasks(timeout=1.0)

        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_wait_for_pending_tasks_scoped_by_subscriber(self) -> None:
        """Scoped wait should not cancel tasks that belong to other subscribers."""
        bus = _MemoryBus()
        states = {"sub1_canceled": False, "sub2_completed": False}

        async def handler_sub1(topic: str, data: bytes) -> None:
            try:
                await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                states["sub1_canceled"] = True
                raise

        async def handler_sub2(topic: str, data: bytes) -> None:
            await asyncio.sleep(0.1)
            states["sub2_completed"] = True

        await bus.subscribe("test.topic", handler_sub1, "sub-1")
        await bus.subscribe("test.topic", handler_sub2, "sub-2")
        await bus.emit("test.topic", b"msg")

        # Timeout should only cancel sub-1 task, not sub-2.
        await bus.wait_for_pending_tasks(timeout=0.01, subscriber_id="sub-1")
        await asyncio.sleep(0.15)

        assert states["sub1_canceled"] is True
        assert states["sub2_completed"] is True


class TestMemoryTransporter:
    """Tests for MemoryTransporter class."""

    @pytest.mark.asyncio
    async def test_connect_disconnect(self, mock_transit) -> None:
        """Transporter should track connection state."""
        transporter = MemoryTransporter(
            transit=mock_transit,
            node_id="node-1",
        )

        assert not transporter._connected

        await transporter.connect()
        assert transporter._connected

        await transporter.disconnect()
        assert not transporter._connected

    @pytest.mark.asyncio
    async def test_disconnect_waits_for_pending_bus_tasks(self, mock_transit) -> None:
        """Disconnect should wait for pending handler tasks to avoid leaks."""
        bus = AsyncMock()
        transporter = MemoryTransporter(
            transit=mock_transit,
            node_id="node-1",
            bus=bus,
        )

        await transporter.connect()
        await transporter.disconnect()

        bus.unsubscribe.assert_awaited_once_with("node-1")
        bus.wait_for_pending_tasks.assert_awaited_once_with(
            timeout=5.0,
            subscriber_id="node-1",
        )

    @pytest.mark.asyncio
    async def test_publish_requires_connection(self, mock_transit) -> None:
        """Publish should raise if not connected."""
        transporter = MemoryTransporter(
            transit=mock_transit,
            node_id="node-1",
        )

        packet = Packet(Topic.REQUEST, "target", {"action": "test"})

        with pytest.raises(RuntimeError, match="not connected"):
            await transporter.publish(packet)

    @pytest.mark.asyncio
    async def test_subscribe_requires_connection(self, mock_transit) -> None:
        """Subscribe should raise if not connected."""
        transporter = MemoryTransporter(
            transit=mock_transit,
            node_id="node-1",
        )

        with pytest.raises(RuntimeError, match="not connected"):
            await transporter.subscribe("REQ")

    @pytest.mark.asyncio
    async def test_topic_name_generation(self, mock_transit) -> None:
        """Topic names should follow MOL.{cmd}.{node} pattern."""
        transporter = MemoryTransporter(
            transit=mock_transit,
            node_id="node-1",
        )

        assert transporter.get_topic_name("REQ") == "MOL.REQ"
        assert transporter.get_topic_name("REQ", "node-2") == "MOL.REQ.node-2"
        assert transporter.get_topic_name("EVENT") == "MOL.EVENT"

    @pytest.mark.asyncio
    async def test_serialize_adds_version_and_sender(self, mock_transit) -> None:
        """Serialization via transit.serializer should produce valid bytes."""
        serializer = mock_transit.serializer
        payload = {**{"action": "math.add"}, "ver": "4", "sender": "test-node"}
        serialized = serializer.serialize(payload)
        deserialized = serializer.deserialize(serialized)

        assert deserialized["ver"] == "4"
        assert deserialized["sender"] == "test-node"
        assert deserialized["action"] == "math.add"

    @pytest.mark.asyncio
    async def test_deserialize_async_offloads_large_json(self, mock_transit) -> None:
        """Large payloads should deserialize via asyncio.to_thread."""
        serializer = mock_transit.serializer
        payload = {**{"blob": "x" * (1024 * 1024)}, "ver": "4", "sender": "test-node"}
        serialized = serializer.serialize(payload)
        called = {"value": False}

        async def fake_to_thread(func, *args):
            called["value"] = True
            return func(*args)

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr("moleculerpy.serializers.base.asyncio.to_thread", fake_to_thread)
            result = await serializer.deserialize_async(serialized)

        assert called["value"] is True
        assert result["sender"] == "test-node"

    @pytest.mark.asyncio
    async def test_message_handler_ignores_own_messages(self, mock_transit, mock_handler) -> None:
        """Handler should ignore messages from self."""
        transporter = MemoryTransporter(
            transit=mock_transit,
            handler=mock_handler,
            node_id="node-1",
        )

        # Simulate receiving own message using the serializer
        serializer = mock_transit.serializer
        data = serializer.serialize({"action": "test", "ver": "4", "sender": "node-1"})
        await transporter._message_handler("MOL.REQ.node-1", data)

        # Handler should not be called
        mock_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_from_config(self, mock_transit) -> None:
        """from_config should create transporter instance."""
        transporter = MemoryTransporter.from_config(
            config={},
            transit=mock_transit,
            node_id="config-node",
        )

        assert isinstance(transporter, MemoryTransporter)
        assert transporter.node_id == "config-node"
        assert transporter.name == "memory"


class TestMemoryTransporterIntegration:
    """Integration tests for multi-broker communication."""

    @pytest.mark.asyncio
    async def test_two_brokers_communicate(self, mock_transit) -> None:
        """Two brokers should be able to communicate via memory bus."""
        received_messages = []

        async def handler1(packet: Packet) -> None:
            received_messages.append(("broker1", packet))

        async def handler2(packet: Packet) -> None:
            received_messages.append(("broker2", packet))

        # Create two transporters (simulating two brokers)
        transporter1 = MemoryTransporter(
            transit=mock_transit,
            handler=handler1,
            node_id="node-1",
        )
        transporter2 = MemoryTransporter(
            transit=mock_transit,
            handler=handler2,
            node_id="node-2",
        )

        await transporter1.connect()
        await transporter2.connect()

        # Both subscribe to REQUEST
        await transporter1.subscribe("REQ")
        await transporter2.subscribe("REQ")

        # node-1 sends to node-2
        packet = Packet(Topic.REQUEST, "node-2", {"action": "test.action"})
        await transporter1.publish(packet)

        await asyncio.sleep(0.05)

        # Only node-2 should receive (node-1 ignores its own messages)
        assert len(received_messages) == 1
        assert received_messages[0][0] == "broker2"

        await transporter1.disconnect()
        await transporter2.disconnect()

    @pytest.mark.asyncio
    async def test_broadcast_to_all_nodes(self, mock_transit) -> None:
        """Broadcast (no target) should reach all nodes."""
        received_messages = []

        async def handler1(packet: Packet) -> None:
            received_messages.append("broker1")

        async def handler2(packet: Packet) -> None:
            received_messages.append("broker2")

        transporter1 = MemoryTransporter(
            transit=mock_transit,
            handler=handler1,
            node_id="node-1",
        )
        transporter2 = MemoryTransporter(
            transit=mock_transit,
            handler=handler2,
            node_id="node-2",
        )

        await transporter1.connect()
        await transporter2.connect()

        # Subscribe to broadcast topic (no node_id suffix)
        await transporter1.subscribe("EVENT")
        await transporter2.subscribe("EVENT")

        # Broadcast from node-1 (no target = all)
        packet = Packet(Topic.EVENT, None, {"event": "user.created"})
        await transporter1.publish(packet)

        await asyncio.sleep(0.05)

        # node-2 should receive (node-1 ignores own)
        assert "broker2" in received_messages
        # node-1 should NOT receive its own broadcast
        assert received_messages.count("broker1") == 0

        await transporter1.disconnect()
        await transporter2.disconnect()

    @pytest.mark.asyncio
    async def test_cleanup_on_disconnect(self, mock_transit) -> None:
        """Disconnect should remove all subscriptions."""

        async def handler(packet: Packet) -> None:
            pass

        transporter = MemoryTransporter(
            transit=mock_transit,
            handler=handler,
            node_id="node-1",
        )

        await transporter.connect()
        await transporter.subscribe("REQ")
        await transporter.subscribe("EVENT")

        bus = transporter.bus
        assert bus.get_subscriber_count("MOL.REQ") == 1
        assert bus.get_subscriber_count("MOL.EVENT") == 1

        await transporter.disconnect()

        assert bus.get_subscriber_count("MOL.REQ") == 0
        assert bus.get_subscriber_count("MOL.EVENT") == 0


class TestGlobalBusHelpers:
    """Tests for global bus helper functions."""

    def test_reset_global_bus(self) -> None:
        """reset_global_bus should create new bus instance."""
        bus1 = get_global_bus()
        reset_global_bus()
        bus2 = get_global_bus()

        assert bus1 is not bus2

    @pytest.mark.asyncio
    async def test_multiple_transporters_share_global_bus(self, mock_transit) -> None:
        """Transporters without custom bus should share global bus."""
        t1 = MemoryTransporter(transit=mock_transit, node_id="n1")
        t2 = MemoryTransporter(transit=mock_transit, node_id="n2")

        assert t1.bus is t2.bus
        assert t1.bus is get_global_bus()


class TestMemoryTransporterName:
    """Tests for transporter name and factory discovery."""

    def test_transporter_name(self, mock_transit) -> None:
        """Transporter should have correct name."""
        transporter = MemoryTransporter(
            transit=mock_transit,
            node_id="node-1",
        )
        assert transporter.name == "memory"

    def test_has_built_in_balancer(self, mock_transit) -> None:
        """Memory transporter should have built-in balancer."""
        transporter = MemoryTransporter(
            transit=mock_transit,
            node_id="node-1",
        )
        assert transporter.has_built_in_balancer is True
