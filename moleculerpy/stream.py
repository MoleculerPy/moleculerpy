"""Streaming support for MoleculerPy framework.

This module provides AsyncStream class for handling streaming responses
that are compatible with Moleculer.js streaming protocol.

Protocol (Moleculer.js compatible):
    - stream: true/false flag in REQ/RES packets
    - seq: 0 = init, 1..N = data chunks
    - Final chunk has stream: false (end marker)
    - $streamObjectMode in meta for JSON objects vs bytes
    - $streamError in meta for error propagation
"""

import asyncio
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, cast

__all__ = [
    "AsyncStream",
    "PendingStream",
    "StreamChunk",
    "StreamError",
    "StreamMetadata",
    "StreamState",
]


class StreamState(Enum):
    """State of a stream."""

    PENDING = "pending"  # Waiting for first chunk (seq=0)
    ACTIVE = "active"  # Receiving chunks
    COMPLETED = "completed"  # Final chunk received (stream=false)
    ERROR = "error"  # Error occurred
    CANCELLED = "cancelled"  # Cancelled by user


@dataclass(slots=True, frozen=True)
class StreamChunk:
    """Represents a single chunk in a stream.

    Attributes:
        seq: Sequence number (0 = init, 1..N = data)
        data: Chunk data (bytes or object if object_mode)
        is_final: True if this is the last chunk (stream=false)
    """

    seq: int
    data: Any
    is_final: bool = False


@dataclass(slots=True, frozen=True)
class StreamMetadata:
    """Metadata for a stream.

    Attributes:
        context_id: ID of the originating context
        object_mode: If True, chunks are JSON objects, not bytes
        max_chunk_size: Maximum chunk size (default 256KB like Moleculer.js)
    """

    context_id: str
    object_mode: bool = False
    max_chunk_size: int = 256 * 1024  # 256KB default


@dataclass
class PendingStream:
    """Tracks state of a pending stream.

    Used internally by Transit to manage incoming/outgoing streams.

    Attributes:
        metadata: Stream metadata
        state: Current stream state
        queue: Queue for incoming chunks (for response streams)
        next_seq: Next expected sequence number
        pool: Buffer for out-of-order chunks {seq: chunk}
        error: Error if stream failed
    """

    metadata: StreamMetadata
    state: StreamState = StreamState.PENDING
    queue: "asyncio.Queue[StreamChunk]" = field(default_factory=asyncio.Queue)
    next_seq: int = 0
    pool: dict[int, StreamChunk] = field(default_factory=dict)
    error: Exception | None = None


class AsyncStream[T](AsyncIterator[T]):
    """Async iterator for streaming responses.

    Compatible with Moleculer.js streaming protocol.

    Usage:
        async for chunk in stream:
            process(chunk)

    Or with timeout:
        async for chunk in stream.with_timeout(30.0):
            process(chunk)

    Attributes:
        context_id: ID of the context that initiated the stream
        object_mode: If True, yields parsed JSON objects instead of bytes
    """

    __slots__ = ("_completed", "_error", "_on_close", "_queue", "context_id", "object_mode")

    def __init__(
        self,
        context_id: str,
        queue: "asyncio.Queue[StreamChunk]",
        object_mode: bool = False,
        on_close: Callable[[str], None] | None = None,
    ) -> None:
        """Initialize AsyncStream.

        Args:
            context_id: ID of the originating context
            queue: Queue to receive chunks from
            object_mode: If True, chunks are JSON objects
            on_close: Optional callback invoked once when stream is closed/cancelled
        """
        self.context_id = context_id
        self._queue = queue
        self.object_mode = object_mode
        self._completed = False
        self._error: Exception | None = None
        self._on_close = on_close

    def _notify_close(self) -> None:
        """Invoke on_close callback at most once."""
        callback = self._on_close
        if callback is None:
            return
        self._on_close = None
        callback(self.context_id)

    def _drain_queue(self) -> None:
        """Best-effort queue drain for early stream close/cancel paths."""
        while True:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    def __aiter__(self) -> "AsyncStream[T]":
        """Return self as async iterator."""
        return self

    async def __anext__(self) -> T:
        """Get next chunk from stream.

        Returns:
            Next chunk data

        Raises:
            StopAsyncIteration: When stream is complete
            asyncio.CancelledError: If iteration is cancelled
            Exception: If stream encountered an error
        """
        if self._completed:
            raise StopAsyncIteration

        if self._error:
            raise self._error

        try:
            chunk = await self._queue.get()
        except asyncio.CancelledError:
            self._completed = True
            self._drain_queue()
            self._notify_close()
            raise

        # Check for error marker
        if isinstance(chunk.data, Exception):
            self._error = chunk.data
            self._completed = True
            raise self._error

        # Check for end marker
        if chunk.is_final:
            self._completed = True
            if chunk.data is None:
                raise StopAsyncIteration
            # Return final chunk data before stopping
            return cast(T, chunk.data)

        return cast(T, chunk.data)

    async def with_timeout(self, timeout: float) -> AsyncIterator[T]:
        """Iterate with timeout per chunk.

        Args:
            timeout: Timeout in seconds for each chunk

        Yields:
            Chunk data

        Raises:
            asyncio.TimeoutError: If timeout is exceeded
        """
        while not self._completed:
            if self._error:
                raise self._error

            try:
                chunk = await asyncio.wait_for(self._queue.get(), timeout)
            except TimeoutError:
                self._completed = True
                self._drain_queue()
                self._notify_close()
                raise

            if isinstance(chunk.data, Exception):
                self._error = chunk.data
                self._completed = True
                raise self._error

            if chunk.is_final:
                self._completed = True
                if chunk.data is not None:
                    yield cast(T, chunk.data)
                return

            yield cast(T, chunk.data)

    async def collect(self, timeout: float | None = None) -> list[T]:
        """Collect all chunks into a list.

        Args:
            timeout: Optional total timeout for collection

        Returns:
            List of all chunks
        """
        chunks: list[T] = []

        if timeout:
            async for chunk in self.with_timeout(timeout):
                chunks.append(chunk)
        else:
            async for chunk in self:
                chunks.append(chunk)

        return chunks

    async def collect_bytes(self, timeout: float | None = None) -> bytes:
        """Collect all chunks as bytes.

        Args:
            timeout: Optional total timeout for collection

        Returns:
            Concatenated bytes from all chunks

        Raises:
            TypeError: If object_mode is True
        """
        if self.object_mode:
            raise TypeError("Cannot collect_bytes() on object_mode stream")

        chunks = cast(list[bytes], await self.collect(timeout))
        return b"".join(chunks)

    @property
    def is_completed(self) -> bool:
        """Check if stream is completed."""
        return self._completed

    @property
    def has_error(self) -> bool:
        """Check if stream has an error."""
        return self._error is not None

    async def aclose(self) -> None:
        """Close the stream and mark it as completed.

        This method allows explicit cleanup of the async iterator,
        following PEP 525 async generator protocol.
        """
        self._completed = True
        self._drain_queue()
        self._notify_close()


class StreamError(Exception):
    """Error that occurred during streaming.

    Attributes:
        message: Error message
        code: Error code (optional)
        context_id: ID of the stream context
    """

    __slots__ = ("code", "context_id", "message")

    def __init__(
        self,
        message: str,
        code: int | None = None,
        context_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.context_id = context_id
