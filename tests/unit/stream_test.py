"""Tests for MoleculerPy streaming protocol (Phase 3D).

Tests cover:
- AsyncStream class
- StreamChunk and StreamMetadata dataclasses
- PendingStream state management
- Out-of-order chunk handling
"""

import asyncio

import pytest

from moleculerpy.stream import (
    AsyncStream,
    PendingStream,
    StreamChunk,
    StreamError,
    StreamMetadata,
    StreamState,
)


class TestStreamChunk:
    """Tests for StreamChunk dataclass."""

    def test_stream_chunk_defaults(self):
        """StreamChunk has sensible defaults."""
        chunk = StreamChunk(seq=0, data=b"hello")
        assert chunk.seq == 0
        assert chunk.data == b"hello"
        assert chunk.is_final is False

    def test_stream_chunk_final(self):
        """StreamChunk can be marked as final."""
        chunk = StreamChunk(seq=5, data=None, is_final=True)
        assert chunk.seq == 5
        assert chunk.data is None
        assert chunk.is_final is True


class TestStreamMetadata:
    """Tests for StreamMetadata dataclass."""

    def test_metadata_defaults(self):
        """StreamMetadata has sensible defaults."""
        meta = StreamMetadata(context_id="ctx-123")
        assert meta.context_id == "ctx-123"
        assert meta.object_mode is False
        assert meta.max_chunk_size == 256 * 1024  # 256KB

    def test_metadata_object_mode(self):
        """StreamMetadata supports object mode."""
        meta = StreamMetadata(context_id="ctx-123", object_mode=True)
        assert meta.object_mode is True


class TestPendingStream:
    """Tests for PendingStream state management."""

    def test_pending_stream_defaults(self):
        """PendingStream initializes with correct defaults."""
        meta = StreamMetadata(context_id="ctx-123")
        pending = PendingStream(metadata=meta)

        assert pending.metadata == meta
        assert pending.state == StreamState.PENDING
        assert pending.next_seq == 0
        assert pending.pool == {}
        assert pending.error is None

    def test_pending_stream_active(self):
        """PendingStream can be set to active state."""
        meta = StreamMetadata(context_id="ctx-123")
        pending = PendingStream(metadata=meta, state=StreamState.ACTIVE)
        assert pending.state == StreamState.ACTIVE


class TestAsyncStream:
    """Tests for AsyncStream async iterator."""

    @pytest.fixture
    def stream_queue(self):
        """Create a queue for testing."""
        return asyncio.Queue()

    @pytest.fixture
    def stream(self, stream_queue):
        """Create an AsyncStream for testing."""
        return AsyncStream(
            context_id="ctx-123",
            queue=stream_queue,
            object_mode=False,
        )

    @pytest.mark.asyncio
    async def test_stream_iteration_single_chunk(self, stream, stream_queue):
        """AsyncStream yields single chunk correctly."""
        # Put chunk and final marker
        await stream_queue.put(StreamChunk(seq=0, data=b"hello", is_final=True))

        chunks = []
        async for chunk in stream:
            chunks.append(chunk)

        assert chunks == [b"hello"]
        assert stream.is_completed

    @pytest.mark.asyncio
    async def test_stream_iteration_multiple_chunks(self, stream, stream_queue):
        """AsyncStream yields multiple chunks in order."""
        # Put chunks
        await stream_queue.put(StreamChunk(seq=0, data=b"hello"))
        await stream_queue.put(StreamChunk(seq=1, data=b" "))
        await stream_queue.put(StreamChunk(seq=2, data=b"world", is_final=True))

        chunks = []
        async for chunk in stream:
            chunks.append(chunk)

        assert chunks == [b"hello", b" ", b"world"]
        assert stream.is_completed

    @pytest.mark.asyncio
    async def test_stream_final_with_no_data(self, stream, stream_queue):
        """AsyncStream handles final chunk with no data."""
        await stream_queue.put(StreamChunk(seq=0, data=b"data"))
        await stream_queue.put(StreamChunk(seq=1, data=None, is_final=True))

        chunks = []
        async for chunk in stream:
            chunks.append(chunk)

        assert chunks == [b"data"]
        assert stream.is_completed

    @pytest.mark.asyncio
    async def test_stream_error_propagation(self, stream, stream_queue):
        """AsyncStream propagates errors."""
        error = Exception("Stream failed")
        await stream_queue.put(StreamChunk(seq=0, data=error, is_final=True))

        with pytest.raises(Exception, match="Stream failed"):
            async for _ in stream:
                pass

        assert stream.has_error

    @pytest.mark.asyncio
    async def test_stream_collect(self, stream, stream_queue):
        """AsyncStream.collect() gathers all chunks."""
        await stream_queue.put(StreamChunk(seq=0, data=b"a"))
        await stream_queue.put(StreamChunk(seq=1, data=b"b"))
        await stream_queue.put(StreamChunk(seq=2, data=b"c", is_final=True))

        chunks = await stream.collect()
        assert chunks == [b"a", b"b", b"c"]

    @pytest.mark.asyncio
    async def test_stream_collect_bytes(self, stream, stream_queue):
        """AsyncStream.collect_bytes() concatenates all bytes."""
        await stream_queue.put(StreamChunk(seq=0, data=b"hello"))
        await stream_queue.put(StreamChunk(seq=1, data=b" "))
        await stream_queue.put(StreamChunk(seq=2, data=b"world", is_final=True))

        result = await stream.collect_bytes()
        assert result == b"hello world"

    @pytest.mark.asyncio
    async def test_stream_collect_bytes_object_mode_error(self, stream_queue):
        """AsyncStream.collect_bytes() raises TypeError in object mode."""
        stream = AsyncStream(
            context_id="ctx-123",
            queue=stream_queue,
            object_mode=True,
        )
        await stream_queue.put(StreamChunk(seq=0, data={"key": "value"}, is_final=True))

        with pytest.raises(TypeError, match="Cannot collect_bytes"):
            await stream.collect_bytes()

    @pytest.mark.asyncio
    async def test_stream_object_mode(self, stream_queue):
        """AsyncStream in object mode yields JSON objects."""
        stream = AsyncStream(
            context_id="ctx-123",
            queue=stream_queue,
            object_mode=True,
        )

        await stream_queue.put(StreamChunk(seq=0, data={"id": 1}))
        await stream_queue.put(StreamChunk(seq=1, data={"id": 2}))
        await stream_queue.put(StreamChunk(seq=2, data={"id": 3}, is_final=True))

        objects = await stream.collect()
        assert objects == [{"id": 1}, {"id": 2}, {"id": 3}]

    @pytest.mark.asyncio
    async def test_stream_with_timeout(self, stream, stream_queue):
        """AsyncStream.with_timeout() yields chunks with timeout."""
        await stream_queue.put(StreamChunk(seq=0, data=b"data"))
        await stream_queue.put(StreamChunk(seq=1, data=None, is_final=True))

        chunks = []
        async for chunk in stream.with_timeout(1.0):
            chunks.append(chunk)

        assert chunks == [b"data"]

    @pytest.mark.asyncio
    async def test_stream_with_timeout_expires(self, stream, stream_queue):
        """AsyncStream.with_timeout() raises TimeoutError when exceeded."""
        # Don't put any chunks

        with pytest.raises(asyncio.TimeoutError):
            async for _ in stream.with_timeout(0.01):
                pass

    @pytest.mark.asyncio
    async def test_stream_aclose(self, stream, stream_queue):
        """AsyncStream.aclose() marks stream as completed."""
        assert not stream.is_completed

        await stream.aclose()

        assert stream.is_completed

        # Subsequent iteration raises StopAsyncIteration
        with pytest.raises(StopAsyncIteration):
            await stream.__anext__()

    @pytest.mark.asyncio
    async def test_stream_aclose_clears_queue_and_notifies_close(self, stream_queue):
        """aclose() should drain buffered chunks and invoke close callback once."""
        closed_ids = []
        stream = AsyncStream(
            context_id="ctx-123",
            queue=stream_queue,
            object_mode=False,
            on_close=closed_ids.append,
        )
        await stream_queue.put(StreamChunk(seq=0, data=b"a"))
        await stream_queue.put(StreamChunk(seq=1, data=b"b"))

        await stream.aclose()

        assert stream_queue.empty()
        assert closed_ids == ["ctx-123"]

    @pytest.mark.asyncio
    async def test_stream_cancelled_iteration_notifies_close(self, stream_queue):
        """Cancelled iteration should notify close callback for cleanup."""
        closed_ids = []
        stream = AsyncStream(
            context_id="ctx-123",
            queue=stream_queue,
            object_mode=False,
            on_close=closed_ids.append,
        )

        task = asyncio.create_task(stream.__anext__())
        await asyncio.sleep(0)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert stream.is_completed
        assert closed_ids == ["ctx-123"]


class TestStreamError:
    """Tests for StreamError exception."""

    def test_stream_error_basic(self):
        """StreamError has correct attributes."""
        error = StreamError("Something went wrong")
        assert str(error) == "Something went wrong"
        assert error.message == "Something went wrong"
        assert error.code is None
        assert error.context_id is None

    def test_stream_error_with_code(self):
        """StreamError can have error code."""
        error = StreamError("Error", code=500, context_id="ctx-123")
        assert error.code == 500
        assert error.context_id == "ctx-123"


class TestStreamStateEnum:
    """Tests for StreamState enum."""

    def test_stream_states(self):
        """All stream states are defined."""
        assert StreamState.PENDING.value == "pending"
        assert StreamState.ACTIVE.value == "active"
        assert StreamState.COMPLETED.value == "completed"
        assert StreamState.ERROR.value == "error"
        assert StreamState.CANCELLED.value == "cancelled"
