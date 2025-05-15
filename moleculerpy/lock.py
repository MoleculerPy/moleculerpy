"""Async lock implementation for cache coordination.

This module provides an async-first lock implementation for preventing
thundering herd problem in caching scenarios. Multiple requests for the
same cache key will queue and wait for the first one to complete.

Moleculer.js compatible: Queue-based lock with FIFO resolution.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable

# Type alias for unlock function
UnlockFunc = Callable[[], Awaitable[None]]

# Module logger
_logger = logging.getLogger(__name__)


class Lock:
    """Async lock with queue-based waiting.

    Implements a lock mechanism where:
    - First acquirer gets the lock immediately
    - Subsequent acquirers wait in a FIFO queue
    - On release, next waiter in queue is resolved

    This prevents thundering herd: only ONE request executes the handler,
    others wait and then get the cached result.

    Example:
        >>> lock = Lock()
        >>> unlock = await lock.acquire("user:123")
        >>> try:
        ...     # Critical section
        ...     result = await expensive_operation()
        ... finally:
        ...     await unlock()

    Thread Safety:
        This implementation is async-safe but NOT thread-safe.
        Use only within a single asyncio event loop.
    """

    __slots__ = ("_locked",)

    def __init__(self) -> None:
        """Initialize empty lock state."""
        # Map: key -> deque of waiting futures
        # If key exists, lock is held; deque contains waiters
        self._locked: dict[str, deque[asyncio.Future[None]]] = {}

    async def acquire(self, key: str) -> UnlockFunc:
        """Acquire lock for a key, waiting if already locked.

        Args:
            key: Lock identifier (typically cache key)

        Returns:
            Async function to release the lock

        Note:
            First caller acquires immediately.
            Subsequent callers wait in queue until released.
        """
        if key not in self._locked:
            # First acquirer: create empty queue, lock acquired
            self._locked[key] = deque()
        else:
            # Already locked: wait in queue
            loop = asyncio.get_running_loop()
            future: asyncio.Future[None] = loop.create_future()
            self._locked[key].append(future)
            await future  # Wait until released

        async def unlock() -> None:
            await self.release(key)

        return unlock

    def is_locked(self, key: str) -> bool:
        """Check if a key is currently locked.

        Args:
            key: Lock identifier

        Returns:
            True if lock is held, False otherwise
        """
        return key in self._locked

    async def release(self, key: str) -> None:
        """Release lock for a key.

        If waiters exist, resolves the next non-cancelled one in FIFO order.
        If no waiters, removes the lock entirely.

        Args:
            key: Lock identifier
        """
        if key not in self._locked:
            _logger.debug("Lock release called for unlocked key: %s", key)
            return  # Already released or never locked

        waiters = self._locked[key]

        # Wake next non-cancelled waiter (skip cancelled futures)
        while waiters:
            next_waiter = waiters.popleft()
            if not next_waiter.done():
                next_waiter.set_result(None)
                return  # Exit after waking one

        # No valid waiters left: remove lock
        del self._locked[key]

    async def try_acquire(self, key: str) -> UnlockFunc | None:
        """Try to acquire lock without waiting.

        Args:
            key: Lock identifier

        Returns:
            Unlock function if acquired, None if already locked

        Example:
            >>> unlock = await lock.try_acquire("key")
            >>> if unlock:
            ...     try:
            ...         # Got lock, do work
            ...         pass
            ...     finally:
            ...         await unlock()
            ... else:
            ...     # Lock held by another, skip
            ...     pass
        """
        if key in self._locked:
            return None  # Already locked, don't wait

        # Acquire lock
        self._locked[key] = deque()

        async def unlock() -> None:
            await self.release(key)

        return unlock

    def pending_count(self, key: str) -> int:
        """Get number of waiters for a key.

        Args:
            key: Lock identifier

        Returns:
            Number of waiting acquirers (0 if not locked)
        """
        if key not in self._locked:
            return 0
        return len(self._locked[key])

    def clear(self) -> None:
        """Clear all locks, cancelling waiters.

        Warning:
            This cancels all waiting futures. Use only for cleanup.
        """
        for waiters in self._locked.values():
            for future in waiters:
                if not future.done():
                    future.cancel()
        self._locked.clear()

    def __repr__(self) -> str:
        locked_count = len(self._locked)
        total_waiters = sum(len(w) for w in self._locked.values())
        return f"Lock(locked={locked_count}, waiters={total_waiters})"
