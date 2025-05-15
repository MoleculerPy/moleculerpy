"""Unit tests for async Lock class.

Tests cover:
    - Basic acquire/release
    - Queue-based waiting (FIFO)
    - try_acquire (non-blocking)
    - Thundering herd simulation
    - Concurrent access patterns
"""

from __future__ import annotations

import asyncio

import pytest

from moleculerpy.lock import Lock


class TestLockBasic:
    """Basic lock operations."""

    @pytest.fixture
    def lock(self) -> Lock:
        """Create a fresh Lock instance."""
        return Lock()

    @pytest.mark.asyncio
    async def test_acquire_returns_unlock_function(self, lock: Lock) -> None:
        """Acquire should return an async unlock function."""
        unlock = await lock.acquire("key1")

        assert callable(unlock)
        assert lock.is_locked("key1")

        await unlock()
        assert not lock.is_locked("key1")

    @pytest.mark.asyncio
    async def test_acquire_same_key_twice_blocks(self, lock: Lock) -> None:
        """Second acquire on same key should block until released."""
        unlock1 = await lock.acquire("key1")

        # Start second acquire (will block)
        acquire_started = asyncio.Event()
        acquire_done = asyncio.Event()
        result: list[str] = []

        async def second_acquire() -> None:
            acquire_started.set()
            unlock2 = await lock.acquire("key1")
            result.append("acquired")
            await unlock2()
            acquire_done.set()

        task = asyncio.create_task(second_acquire())

        # Wait for second acquire to start
        await acquire_started.wait()
        await asyncio.sleep(0.01)  # Give it time to actually block

        # Should still be waiting
        assert not acquire_done.is_set()
        assert result == []

        # Release first lock
        await unlock1()

        # Second should now complete
        await asyncio.wait_for(acquire_done.wait(), timeout=1.0)
        assert result == ["acquired"]

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_different_keys_dont_block(self, lock: Lock) -> None:
        """Different keys should not block each other."""
        unlock1 = await lock.acquire("key1")
        unlock2 = await lock.acquire("key2")

        assert lock.is_locked("key1")
        assert lock.is_locked("key2")

        await unlock1()
        await unlock2()

        assert not lock.is_locked("key1")
        assert not lock.is_locked("key2")

    @pytest.mark.asyncio
    async def test_is_locked_false_for_unknown_key(self, lock: Lock) -> None:
        """is_locked returns False for keys that were never locked."""
        assert not lock.is_locked("unknown")

    @pytest.mark.asyncio
    async def test_release_unknown_key_safe(self, lock: Lock) -> None:
        """Releasing unknown key should not raise."""
        await lock.release("unknown")  # Should not raise


class TestLockQueue:
    """Test FIFO queue behavior."""

    @pytest.fixture
    def lock(self) -> Lock:
        return Lock()

    @pytest.mark.asyncio
    async def test_fifo_order(self, lock: Lock) -> None:
        """Waiters should be resolved in FIFO order."""
        order: list[int] = []
        unlock_first = await lock.acquire("key")

        async def waiter(n: int) -> None:
            unlock = await lock.acquire("key")
            order.append(n)
            await unlock()

        # Start waiters in order
        task1 = asyncio.create_task(waiter(1))
        await asyncio.sleep(0.01)
        task2 = asyncio.create_task(waiter(2))
        await asyncio.sleep(0.01)
        task3 = asyncio.create_task(waiter(3))
        await asyncio.sleep(0.01)

        # Release first lock
        await unlock_first()

        # Wait for all to complete
        await asyncio.gather(task1, task2, task3)

        # Should be in FIFO order
        assert order == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_pending_count(self, lock: Lock) -> None:
        """pending_count should track waiting acquirers."""
        assert lock.pending_count("key") == 0

        unlock1 = await lock.acquire("key")
        assert lock.pending_count("key") == 0  # No waiters yet

        # Start waiter
        started = asyncio.Event()

        async def waiter() -> None:
            started.set()
            await lock.acquire("key")

        task = asyncio.create_task(waiter())
        await started.wait()
        await asyncio.sleep(0.01)

        assert lock.pending_count("key") == 1

        await unlock1()
        await asyncio.sleep(0.01)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


class TestTryAcquire:
    """Test non-blocking try_acquire."""

    @pytest.fixture
    def lock(self) -> Lock:
        return Lock()

    @pytest.mark.asyncio
    async def test_try_acquire_success(self, lock: Lock) -> None:
        """try_acquire succeeds when key is not locked."""
        unlock = await lock.try_acquire("key")

        assert unlock is not None
        assert lock.is_locked("key")

        await unlock()
        assert not lock.is_locked("key")

    @pytest.mark.asyncio
    async def test_try_acquire_fails_when_locked(self, lock: Lock) -> None:
        """try_acquire returns None when key is already locked."""
        unlock1 = await lock.acquire("key")

        unlock2 = await lock.try_acquire("key")
        assert unlock2 is None

        await unlock1()

    @pytest.mark.asyncio
    async def test_try_acquire_after_release(self, lock: Lock) -> None:
        """try_acquire should succeed after previous lock released."""
        unlock1 = await lock.acquire("key")
        await unlock1()

        unlock2 = await lock.try_acquire("key")
        assert unlock2 is not None

        await unlock2()


class TestThunderingHerd:
    """Test thundering herd protection pattern."""

    @pytest.fixture
    def lock(self) -> Lock:
        return Lock()

    @pytest.mark.asyncio
    async def test_only_first_executes_handler(self, lock: Lock) -> None:
        """Only first request should execute handler, others wait."""
        execution_count = 0
        results: list[int] = []

        async def handler() -> int:
            nonlocal execution_count
            execution_count += 1
            await asyncio.sleep(0.05)  # Simulate work
            return 42

        async def cached_operation(request_id: int) -> None:
            unlock = await lock.acquire("cache-key")
            try:
                # Simulate: check cache (miss), execute handler, cache result
                if execution_count == 0:
                    result = await handler()
                else:
                    result = 42  # "cached"
                results.append(result)
            finally:
                await unlock()

        # Start 5 concurrent requests
        tasks = [asyncio.create_task(cached_operation(i)) for i in range(5)]
        await asyncio.gather(*tasks)

        # Handler should only execute once
        assert execution_count == 1
        # All should get same result
        assert results == [42, 42, 42, 42, 42]

    @pytest.mark.asyncio
    async def test_thundering_herd_with_double_check(self, lock: Lock) -> None:
        """Simulate real cache pattern with double-check."""
        cache: dict[str, int] = {}
        handler_calls = 0

        async def expensive_handler() -> int:
            nonlocal handler_calls
            handler_calls += 1
            await asyncio.sleep(0.02)
            return 100

        async def get_with_cache(key: str) -> int:
            # First check (without lock)
            if key in cache:
                return cache[key]

            # Cache miss - acquire lock
            unlock = await lock.acquire(key)
            try:
                # Double-check after acquiring lock
                if key in cache:
                    return cache[key]

                # Execute handler
                result = await expensive_handler()
                cache[key] = result
                return result
            finally:
                await unlock()

        # Start 10 concurrent requests
        tasks = [asyncio.create_task(get_with_cache("mykey")) for _ in range(10)]
        results = await asyncio.gather(*tasks)

        # Handler called only once
        assert handler_calls == 1
        # All got same result
        assert all(r == 100 for r in results)


class TestLockClear:
    """Test clear() method."""

    @pytest.fixture
    def lock(self) -> Lock:
        return Lock()

    @pytest.mark.asyncio
    async def test_clear_removes_all_locks(self, lock: Lock) -> None:
        """clear() should remove all locks."""
        await lock.acquire("key1")
        await lock.acquire("key2")

        assert lock.is_locked("key1")
        assert lock.is_locked("key2")

        lock.clear()

        assert not lock.is_locked("key1")
        assert not lock.is_locked("key2")

    @pytest.mark.asyncio
    async def test_clear_cancels_waiters(self, lock: Lock) -> None:
        """clear() should cancel waiting futures."""
        await lock.acquire("key")

        cancelled = False

        async def waiter() -> None:
            nonlocal cancelled
            try:
                await lock.acquire("key")
            except asyncio.CancelledError:
                cancelled = True
                raise

        task = asyncio.create_task(waiter())
        await asyncio.sleep(0.01)

        lock.clear()

        # Give time for cancellation
        await asyncio.sleep(0.01)
        assert cancelled or task.cancelled()

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


class TestLockRepr:
    """Test string representation."""

    def test_repr_empty(self) -> None:
        """Repr shows 0 locked, 0 waiters for empty lock."""
        lock = Lock()
        assert "locked=0" in repr(lock)
        assert "waiters=0" in repr(lock)

    @pytest.mark.asyncio
    async def test_repr_with_locks(self) -> None:
        """Repr shows correct counts."""
        lock = Lock()
        await lock.acquire("key1")
        await lock.acquire("key2")

        assert "locked=2" in repr(lock)
