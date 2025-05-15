"""Tests for Transit streaming functionality (Phase 3D).

Tests cover:
- _handle_response_stream method
- Out-of-order chunk handling
- Stream error propagation
- stream_request method
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from moleculerpy.lifecycle import Lifecycle
from moleculerpy.packet import Packet, Topic
from moleculerpy.settings import Settings
from moleculerpy.stream import AsyncStream, StreamState
from moleculerpy.transit import Transit


@pytest.fixture
def mock_registry():
    """Create a mock registry."""
    registry = MagicMock()
    registry.get_action.return_value = None
    registry.get_event.return_value = None
    return registry


@pytest.fixture
def mock_node_catalog():
    """Create a mock node catalog."""
    catalog = MagicMock()
    catalog.local_node = Mock()
    catalog.local_node.get_info.return_value = {"id": "test-node"}
    return catalog


@pytest.fixture
def mock_settings():
    """Create mock settings."""
    settings = Settings(
        transporter="memory://",
        request_timeout=5.0,
    )
    return settings


@pytest.fixture
def mock_lifecycle():
    """Create a mock lifecycle."""
    lifecycle = MagicMock(spec=Lifecycle)
    return lifecycle


@pytest.fixture
def mock_logger():
    """Create a mock logger."""
    logger = MagicMock()
    return logger


@pytest.fixture
def transit(mock_registry, mock_node_catalog, mock_settings, mock_logger, mock_lifecycle):
    """Create a Transit instance for testing."""
    with patch("moleculerpy.transit.Transporter") as MockTransporter:
        MockTransporter.get_by_name.return_value = AsyncMock()
        t = Transit(
            node_id="test-node",
            registry=mock_registry,
            node_catalog=mock_node_catalog,
            settings=mock_settings,
            logger=mock_logger,
            lifecycle=mock_lifecycle,
        )
        t.transporter.publish = AsyncMock()
        return t


def make_packet(payload: dict, sender: str = "remote-node") -> Packet:
    """Helper to create Packet with sender."""
    packet = Packet(Topic.RESPONSE, "test-node", payload)
    packet.sender = sender
    return packet


class TestTransitStreamingSetup:
    """Tests for Transit streaming initialization."""

    def test_pending_streams_initialized(self, transit):
        """Transit initializes _pending_streams dict."""
        assert hasattr(transit, "_pending_streams")
        assert transit._pending_streams == {}


class TestHandleResponseStream:
    """Tests for _handle_response_stream method."""

    @pytest.mark.asyncio
    async def test_stream_init_creates_pending_stream(self, transit):
        """First stream chunk creates pending stream and resolves future."""
        # Set up pending request
        future = asyncio.get_running_loop().create_future()
        transit._pending_requests["ctx-123"] = future

        # Simulate stream init packet (seq=0, stream=true)
        packet = make_packet(
            {
                "id": "ctx-123",
                "stream": True,
                "seq": 0,
                "data": b"first chunk",
                "success": True,
                "meta": {},
            }
        )

        await transit._handle_response_stream(packet)

        # Future should be resolved with AsyncStream
        assert future.done()
        result = future.result()
        assert "stream" in result
        assert isinstance(result["stream"], AsyncStream)

        # Pending stream should be tracked
        assert "ctx-123" in transit._pending_streams

    @pytest.mark.asyncio
    async def test_stream_chunks_delivered_in_order(self, transit):
        """Stream chunks are delivered in sequence order."""
        # Set up pending request
        future = asyncio.get_running_loop().create_future()
        transit._pending_requests["ctx-123"] = future

        # Send chunks in order
        for seq in range(3):
            is_final = seq == 2
            packet = make_packet(
                {
                    "id": "ctx-123",
                    "stream": not is_final,
                    "seq": seq,
                    "data": f"chunk-{seq}".encode(),
                    "success": True,
                    "meta": {},
                }
            )
            await transit._handle_response_stream(packet)

        # Get stream from future
        result = future.result()
        stream = result["stream"]

        # Collect chunks
        chunks = []
        async for chunk in stream:
            chunks.append(chunk)

        assert chunks == [b"chunk-0", b"chunk-1", b"chunk-2"]

    @pytest.mark.asyncio
    async def test_stream_out_of_order_chunks_buffered(self, transit):
        """Out-of-order chunks are buffered and delivered in correct sequence."""
        # Set up pending request
        future = asyncio.get_running_loop().create_future()
        transit._pending_requests["ctx-123"] = future

        # Send chunks out of order: 0, 2, 1, 3 (final)
        out_of_order = [(0, False), (2, False), (1, False), (3, True)]

        for seq, is_final in out_of_order:
            packet = make_packet(
                {
                    "id": "ctx-123",
                    "stream": not is_final,
                    "seq": seq,
                    "data": f"chunk-{seq}".encode(),
                    "success": True,
                    "meta": {},
                }
            )
            await transit._handle_response_stream(packet)

        # Get stream from future
        result = future.result()
        stream = result["stream"]

        # Collect chunks - should be in order despite out-of-order delivery
        chunks = []
        async for chunk in stream:
            chunks.append(chunk)

        assert chunks == [b"chunk-0", b"chunk-1", b"chunk-2", b"chunk-3"]

    @pytest.mark.asyncio
    async def test_stream_error_propagated(self, transit):
        """Stream errors are propagated to AsyncStream."""
        # Set up pending request
        future = asyncio.get_running_loop().create_future()
        transit._pending_requests["ctx-123"] = future

        # First send init
        init_packet = make_packet(
            {
                "id": "ctx-123",
                "stream": True,
                "seq": 0,
                "data": b"init",
                "success": True,
                "meta": {},
            }
        )
        await transit._handle_response_stream(init_packet)

        # Then send error
        error_packet = make_packet(
            {
                "id": "ctx-123",
                "stream": True,
                "seq": 1,
                "data": None,
                "success": True,
                "meta": {"$streamError": {"message": "Stream failed"}},
            }
        )
        await transit._handle_response_stream(error_packet)

        # Stream should not be tracked anymore
        assert "ctx-123" not in transit._pending_streams

    @pytest.mark.asyncio
    async def test_stream_object_mode(self, transit):
        """Object mode streams yield JSON objects."""
        # Set up pending request
        future = asyncio.get_running_loop().create_future()
        transit._pending_requests["ctx-123"] = future

        # Send chunks with object mode
        for seq, data in enumerate([{"id": 1}, {"id": 2}]):
            is_final = seq == 1
            packet = make_packet(
                {
                    "id": "ctx-123",
                    "stream": not is_final,
                    "seq": seq,
                    "data": data,
                    "success": True,
                    "meta": {"$streamObjectMode": True},
                }
            )
            await transit._handle_response_stream(packet)

        # Get stream from future
        result = future.result()
        stream = result["stream"]

        assert stream.object_mode is True

        # Collect objects
        objects = []
        async for obj in stream:
            objects.append(obj)

        assert objects == [{"id": 1}, {"id": 2}]

    @pytest.mark.asyncio
    async def test_stream_unknown_request_ignored(self, transit):
        """Stream chunks for unknown requests are ignored."""
        # No pending request set up

        packet = make_packet(
            {
                "id": "unknown-ctx",
                "stream": True,
                "seq": 0,
                "data": b"data",
                "success": True,
                "meta": {},
            }
        )

        # Should not raise
        await transit._handle_response_stream(packet)

        # Should not create pending stream
        assert "unknown-ctx" not in transit._pending_streams

    @pytest.mark.asyncio
    async def test_stream_close_removes_pending_stream(self, transit):
        """Closing AsyncStream should cleanup pending stream state in Transit."""
        future = asyncio.get_running_loop().create_future()
        transit._pending_requests["ctx-123"] = future

        init_packet = make_packet(
            {
                "id": "ctx-123",
                "stream": True,
                "seq": 0,
                "data": b"init",
                "success": True,
                "meta": {},
            }
        )
        await transit._handle_response_stream(init_packet)

        result = future.result()
        stream = result["stream"]
        assert "ctx-123" in transit._pending_streams

        await stream.aclose()
        assert "ctx-123" not in transit._pending_streams


class TestTransitDisconnectStreams:
    """Tests for stream cleanup on disconnect."""

    @pytest.mark.asyncio
    async def test_disconnect_cancels_pending_streams(self, transit):
        """Disconnect cancels all pending streams."""
        # Set up a pending stream
        from moleculerpy.stream import PendingStream, StreamMetadata

        pending = PendingStream(
            metadata=StreamMetadata(context_id="ctx-123"),
            state=StreamState.ACTIVE,
        )
        transit._pending_streams["ctx-123"] = pending

        await transit.disconnect()

        # Stream should be removed
        assert "ctx-123" not in transit._pending_streams


class TestHandleResponseRouting:
    """Tests for _handle_response routing to stream handler."""

    @pytest.mark.asyncio
    async def test_response_with_stream_routes_to_stream_handler(self, transit):
        """Response with stream=true routes to stream handler."""
        # Set up pending request
        future = asyncio.get_running_loop().create_future()
        transit._pending_requests["ctx-123"] = future

        packet = make_packet(
            {
                "id": "ctx-123",
                "stream": True,
                "seq": 0,
                "data": b"data",
                "success": True,
                "meta": {},
            }
        )

        await transit._handle_response(packet)

        # Future should be resolved with stream
        assert future.done()
        result = future.result()
        assert "stream" in result

    @pytest.mark.asyncio
    async def test_response_without_stream_resolves_normally(self, transit):
        """Response without stream flag resolves normally."""
        # Set up pending request
        future = asyncio.get_running_loop().create_future()
        transit._pending_requests["ctx-123"] = future

        packet = make_packet(
            {
                "id": "ctx-123",
                "data": {"result": "value"},
                "success": True,
            }
        )

        await transit._handle_response(packet)

        # Future should be resolved with data
        assert future.done()
        result = future.result()
        assert result["data"] == {"result": "value"}
