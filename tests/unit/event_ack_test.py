"""Tests for Event Acknowledgment.

Phase 3C: Reliable event delivery with acknowledgment mechanism.
"""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from moleculerpy.context import Context
from moleculerpy.lifecycle import Lifecycle
from moleculerpy.packet import Packet, Topic
from moleculerpy.transit import Transit


@pytest.fixture
def mock_broker():
    """Create a mock broker."""
    broker = Mock()
    broker.nodeID = "test-node"
    return broker


@pytest.fixture
def mock_registry():
    """Create a mock registry."""
    return Mock()


@pytest.fixture
def mock_node_catalog():
    """Create a mock node catalog."""
    catalog = Mock()
    catalog.local_node = None
    return catalog


@pytest.fixture
def mock_settings():
    """Create mock settings."""
    settings = Mock()
    settings.transporter = "memory://localhost"
    settings.request_timeout = 10.0
    settings.ack_timeout = 5.0
    settings.serializer = "JSON"
    return settings


@pytest.fixture
def mock_logger():
    """Create a mock logger."""
    return Mock()


@pytest.fixture
def mock_lifecycle(mock_broker):
    """Create a mock lifecycle."""
    lifecycle = Mock(spec=Lifecycle)
    lifecycle.broker = mock_broker

    def rebuild_context(payload: dict[str, Any]) -> Context:
        return Context(
            id=payload.get("id", "test-ctx-id"),
            event=payload.get("event"),
            params=payload.get("params") or payload.get("data"),
            meta=payload.get("meta"),
            broker=mock_broker,
            need_ack=payload.get("needAck"),
            ack_id=payload.get("ackID"),
        )

    lifecycle.rebuild_context = rebuild_context
    return lifecycle


@pytest.fixture
def mock_transporter():
    """Create a mock transporter."""
    transporter = Mock()
    transporter.connect = AsyncMock()
    transporter.disconnect = AsyncMock()
    transporter.subscribe = AsyncMock()
    transporter.publish = AsyncMock()
    return transporter


@pytest.fixture
def mock_dependencies(mock_registry, mock_node_catalog, mock_settings, mock_logger, mock_lifecycle):
    """Create mock dependencies for Transit."""
    return {
        "node_id": "test-node",
        "registry": mock_registry,
        "node_catalog": mock_node_catalog,
        "settings": mock_settings,
        "logger": mock_logger,
        "lifecycle": mock_lifecycle,
    }


class TestContextEventAckFields:
    """Tests for Event Ack fields in Context."""

    def test_context_has_need_ack_field(self, mock_broker) -> None:
        """Context should have need_ack field."""
        ctx = Context(id="test-id", broker=mock_broker)
        assert hasattr(ctx, "need_ack")
        assert ctx.need_ack is None

    def test_context_has_ack_id_field(self, mock_broker) -> None:
        """Context should have ack_id field."""
        ctx = Context(id="test-id", broker=mock_broker)
        assert hasattr(ctx, "ack_id")
        assert ctx.ack_id is None

    def test_context_need_ack_can_be_set(self, mock_broker) -> None:
        """Context need_ack can be set to True."""
        ctx = Context(id="test-id", broker=mock_broker, need_ack=True)
        assert ctx.need_ack is True

    def test_context_ack_id_can_be_set(self, mock_broker) -> None:
        """Context ack_id can be set."""
        ctx = Context(id="test-id", broker=mock_broker, ack_id="ack-123")
        assert ctx.ack_id == "ack-123"

    def test_context_marshall_includes_ack_fields(self, mock_broker) -> None:
        """Context.marshall() should include needAck and ackID."""
        ctx = Context(
            id="test-id",
            broker=mock_broker,
            need_ack=True,
            ack_id="ack-456",
        )
        marshalled = ctx.marshall()

        assert "needAck" in marshalled
        assert marshalled["needAck"] is True
        assert "ackID" in marshalled
        assert marshalled["ackID"] == "ack-456"

    def test_context_unmarshall_includes_ack_fields(self, mock_broker) -> None:
        """Context.unmarshall() should include needAck and ackID."""
        ctx = Context(
            id="test-id",
            broker=mock_broker,
            need_ack=False,
            ack_id=None,
        )
        unmarshalled = ctx.unmarshall()

        assert "needAck" in unmarshalled
        assert "ackID" in unmarshalled


class TestLifecycleEventAck:
    """Tests for Event Ack in Lifecycle."""

    def test_create_context_with_need_ack(self, mock_broker) -> None:
        """Lifecycle.create_context should support need_ack."""
        lifecycle = Lifecycle(mock_broker)
        ctx = lifecycle.create_context(
            event="test.event",
            need_ack=True,
            ack_id="ack-789",
        )

        assert ctx.need_ack is True
        assert ctx.ack_id == "ack-789"

    def test_rebuild_context_with_need_ack(self, mock_broker) -> None:
        """Lifecycle.rebuild_context should restore needAck from camelCase."""
        lifecycle = Lifecycle(mock_broker)
        ctx = lifecycle.rebuild_context(
            {
                "id": "test-id",
                "event": "test.event",
                "needAck": True,
                "ackID": "ack-abc",
            }
        )

        assert ctx.need_ack is True
        assert ctx.ack_id == "ack-abc"

    def test_rebuild_context_with_snake_case_ack(self, mock_broker) -> None:
        """Lifecycle.rebuild_context should restore need_ack from snake_case."""
        lifecycle = Lifecycle(mock_broker)
        ctx = lifecycle.rebuild_context(
            {
                "id": "test-id",
                "event": "test.event",
                "need_ack": True,
                "ack_id": "ack-def",
            }
        )

        assert ctx.need_ack is True
        assert ctx.ack_id == "ack-def"

    def test_rebuild_context_with_need_ack_false(self, mock_broker) -> None:
        """Lifecycle.rebuild_context should handle needAck=False correctly.

        Regression test for boolean falsy trap bug.
        """
        lifecycle = Lifecycle(mock_broker)
        ctx = lifecycle.rebuild_context(
            {
                "id": "test-id",
                "event": "test.event",
                "needAck": False,
                "ackID": "",  # Empty string should also be preserved
            }
        )

        # CRITICAL: False should not become None
        assert ctx.need_ack is False
        # Empty string should be preserved, not become None
        assert ctx.ack_id == ""

    def test_rebuild_context_with_need_ack_none(self, mock_broker) -> None:
        """Lifecycle.rebuild_context should fall back to snake_case when camelCase is None."""
        lifecycle = Lifecycle(mock_broker)
        ctx = lifecycle.rebuild_context(
            {
                "id": "test-id",
                "event": "test.event",
                "needAck": None,
                "need_ack": True,  # Fallback value
            }
        )

        assert ctx.need_ack is True


class TestPacketEventAck:
    """Tests for EVENT_ACK packet type."""

    def test_event_ack_topic_exists(self) -> None:
        """Topic enum should have EVENT_ACK."""
        assert hasattr(Topic, "EVENT_ACK")
        assert Topic.EVENT_ACK.value == "EVENTACK"

    def test_event_ack_packet_creation(self) -> None:
        """EVENT_ACK packet can be created."""
        packet = Packet(
            Topic.EVENT_ACK,
            "target-node",
            {"ackID": "ack-123", "success": True},
        )

        assert packet.type == Topic.EVENT_ACK
        assert packet.target == "target-node"
        assert packet.payload["ackID"] == "ack-123"
        assert packet.payload["success"] is True


class TestTransitEventAckHandling:
    """Tests for Event Ack handling in Transit."""

    @pytest.mark.asyncio
    async def test_transit_has_pending_acks(self, mock_dependencies, mock_transporter):
        """Transit should have _pending_acks dict."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)
            assert hasattr(transit, "_pending_acks")
            assert isinstance(transit._pending_acks, dict)

    @pytest.mark.asyncio
    async def test_handle_event_sends_ack_when_required(self, mock_dependencies, mock_transporter):
        """Transit should send ACK when event has needAck=True."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Mock event endpoint
            mock_endpoint = Mock()
            mock_endpoint.is_local = True
            mock_endpoint.wrapped_handler = None
            mock_endpoint.handler = AsyncMock()
            mock_endpoint.name = "test.event"
            mock_dependencies["registry"].get_event = Mock(return_value=mock_endpoint)

            # Create event packet with needAck=True
            packet = Packet(
                Topic.EVENT,
                "test-node",
                {
                    "id": "evt-123",
                    "event": "test.event",
                    "data": {"key": "value"},
                    "needAck": True,
                    "ackID": "ack-123",
                },
            )
            packet.sender = "sender-node"

            # Handle the event
            await transit._handle_event(packet)

            # Check that ACK was sent
            mock_transporter.publish.assert_called_once()
            published_packet = mock_transporter.publish.call_args[0][0]
            assert published_packet.type == Topic.EVENT_ACK
            assert published_packet.target == "sender-node"
            assert published_packet.payload["ackID"] == "ack-123"
            assert published_packet.payload["success"] is True

    @pytest.mark.asyncio
    async def test_handle_event_sends_ack_with_error_on_failure(
        self, mock_dependencies, mock_transporter
    ):
        """Transit should send ACK with error when event handler fails."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Mock event endpoint that raises exception
            mock_endpoint = Mock()
            mock_endpoint.is_local = True
            mock_endpoint.wrapped_handler = None
            mock_endpoint.handler = AsyncMock(side_effect=ValueError("Handler failed"))
            mock_endpoint.name = "test.event"
            mock_dependencies["registry"].get_event = Mock(return_value=mock_endpoint)

            packet = Packet(
                Topic.EVENT,
                "test-node",
                {
                    "id": "evt-456",
                    "event": "test.event",
                    "data": {},
                    "needAck": True,
                    "ackID": "ack-456",
                },
            )
            packet.sender = "sender-node"

            await transit._handle_event(packet)

            # Check that ACK was sent with error
            mock_transporter.publish.assert_called_once()
            published_packet = mock_transporter.publish.call_args[0][0]
            assert published_packet.payload["success"] is False
            assert published_packet.payload["error"] == "Handler failed"

    @pytest.mark.asyncio
    async def test_handle_event_no_ack_when_not_required(self, mock_dependencies, mock_transporter):
        """Transit should not send ACK when needAck is False."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            mock_endpoint = Mock()
            mock_endpoint.is_local = True
            mock_endpoint.wrapped_handler = None
            mock_endpoint.handler = AsyncMock()
            mock_endpoint.name = "test.event"
            mock_dependencies["registry"].get_event = Mock(return_value=mock_endpoint)

            packet = Packet(
                Topic.EVENT,
                "test-node",
                {
                    "id": "evt-789",
                    "event": "test.event",
                    "data": {},
                    "needAck": False,
                },
            )
            packet.sender = "sender-node"

            await transit._handle_event(packet)

            # No ACK should be sent
            mock_transporter.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_event_ack_resolves_pending(self, mock_dependencies, mock_transporter):
        """Transit._handle_event_ack should resolve pending ACK future."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Create pending ACK
            future = asyncio.get_running_loop().create_future()
            transit._pending_acks["ack-pending"] = future

            # Handle ACK response
            packet = Packet(
                Topic.EVENT_ACK,
                "test-node",
                {"ackID": "ack-pending", "success": True},
            )
            packet.sender = "responder-node"

            await transit._handle_event_ack(packet)

            # Future should be resolved
            assert future.done()
            result = future.result()
            assert result["ackID"] == "ack-pending"
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_handle_event_ack_ignores_unknown(self, mock_dependencies, mock_transporter):
        """Transit._handle_event_ack should ignore unknown ACK IDs."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # No pending ACK
            assert "unknown-ack" not in transit._pending_acks

            packet = Packet(
                Topic.EVENT_ACK,
                "test-node",
                {"ackID": "unknown-ack", "success": True},
            )
            packet.sender = "responder-node"

            # Should not raise
            await transit._handle_event_ack(packet)


class TestTransitSendEventWithAck:
    """Tests for Transit.send_event_with_ack method."""

    @pytest.mark.asyncio
    async def test_send_event_with_ack_success(
        self, mock_dependencies, mock_transporter, mock_broker
    ):
        """send_event_with_ack should wait for ACK and return response."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Create context and endpoint
            ctx = Context(
                id="ctx-123",
                event="test.event",
                broker=mock_broker,
            )
            mock_endpoint = Mock()
            mock_endpoint.node_id = "target-node"

            # Simulate ACK response
            async def mock_publish(packet):
                if packet.type == Topic.EVENT:
                    # Simulate receiving ACK after small delay
                    await asyncio.sleep(0.01)
                    ack_packet = Packet(
                        Topic.EVENT_ACK,
                        "test-node",
                        {"ackID": ctx.id, "success": True},
                    )
                    ack_packet.sender = "target-node"
                    await transit._handle_event_ack(ack_packet)

            mock_transporter.publish = mock_publish

            # Call with short timeout
            result = await transit.send_event_with_ack(mock_endpoint, ctx, timeout=1.0)

            assert result["ackID"] == ctx.id
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_send_event_with_ack_timeout(
        self, mock_dependencies, mock_transporter, mock_broker
    ):
        """send_event_with_ack should raise TimeoutError if no ACK received."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            ctx = Context(
                id="ctx-timeout",
                event="test.event",
                broker=mock_broker,
            )
            mock_endpoint = Mock()
            mock_endpoint.node_id = "target-node"

            # No ACK will be sent
            mock_transporter.publish = AsyncMock()

            with pytest.raises(asyncio.TimeoutError) as exc_info:
                await transit.send_event_with_ack(mock_endpoint, ctx, timeout=0.05)

            assert "timed out" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_send_event_with_ack_sets_need_ack(
        self, mock_dependencies, mock_transporter, mock_broker
    ):
        """send_event_with_ack should set context.need_ack=True."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            ctx = Context(
                id="ctx-456",
                event="test.event",
                broker=mock_broker,
            )
            mock_endpoint = Mock()
            mock_endpoint.node_id = "target-node"

            # Simulate immediate ACK
            async def mock_publish(packet):
                if packet.type == Topic.EVENT:
                    # Verify needAck was set
                    assert packet.payload.get("needAck") is True
                    ack_packet = Packet(
                        Topic.EVENT_ACK,
                        "test-node",
                        {"ackID": ctx.id, "success": True},
                    )
                    ack_packet.sender = "target-node"
                    await transit._handle_event_ack(ack_packet)

            mock_transporter.publish = mock_publish

            await transit.send_event_with_ack(mock_endpoint, ctx, timeout=1.0)

            # Verify context was modified
            assert ctx.need_ack is True

    @pytest.mark.asyncio
    async def test_send_event_with_ack_cleanup_on_timeout(
        self, mock_dependencies, mock_transporter, mock_broker
    ):
        """send_event_with_ack should clean up pending ACK on timeout."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            ctx = Context(
                id="ctx-cleanup",
                event="test.event",
                broker=mock_broker,
            )
            mock_endpoint = Mock()
            mock_endpoint.node_id = "target-node"
            mock_transporter.publish = AsyncMock()

            try:
                await transit.send_event_with_ack(mock_endpoint, ctx, timeout=0.01)
            except TimeoutError:
                pass

            # Pending ACK should be cleaned up
            assert ctx.id not in transit._pending_acks


class TestTransitDisconnectCleanup:
    """Tests for ACK cleanup on disconnect."""

    @pytest.mark.asyncio
    async def test_disconnect_cancels_pending_acks(self, mock_dependencies, mock_transporter):
        """Transit.disconnect should cancel all pending ACKs."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Create pending ACKs
            future1 = asyncio.get_running_loop().create_future()
            future2 = asyncio.get_running_loop().create_future()
            transit._pending_acks["ack-1"] = future1
            transit._pending_acks["ack-2"] = future2

            await transit.disconnect()

            # All pending ACKs should be cancelled
            assert future1.cancelled()
            assert future2.cancelled()
            assert len(transit._pending_acks) == 0
