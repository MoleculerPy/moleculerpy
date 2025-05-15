"""Unit tests for the Bulkhead Middleware module.

Tests for BulkheadMiddleware that limits concurrent execution of actions.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from moleculerpy.errors import QueueIsFullError
from moleculerpy.middleware.bulkhead import (
    BulkheadConfig,
    BulkheadMiddleware,
    BulkheadState,
)


class TestBulkheadConfig:
    """Tests for BulkheadConfig dataclass."""

    def test_default_values(self):
        """Test BulkheadConfig has correct defaults."""
        config = BulkheadConfig()

        assert config.enabled is True
        assert config.concurrency == 3
        assert config.max_queue_size == 100

    def test_custom_values(self):
        """Test BulkheadConfig with custom values."""
        config = BulkheadConfig(
            enabled=False,
            concurrency=5,
            max_queue_size=50,
        )

        assert config.enabled is False
        assert config.concurrency == 5
        assert config.max_queue_size == 50

    def test_config_is_immutable(self):
        """Test BulkheadConfig is frozen (immutable)."""
        config = BulkheadConfig()

        with pytest.raises(AttributeError):
            config.concurrency = 10  # type: ignore


class TestBulkheadMiddleware:
    """Tests for BulkheadMiddleware."""

    def test_init_defaults(self):
        """Test middleware initializes with defaults."""
        mw = BulkheadMiddleware()

        assert mw.default_concurrency == 3
        assert mw.default_max_queue_size == 100
        assert mw.enabled is True
        assert mw._states == {}

    def test_init_custom_values(self):
        """Test middleware initializes with custom values."""
        mw = BulkheadMiddleware(
            concurrency=5,
            max_queue_size=50,
            enabled=False,
        )

        assert mw.default_concurrency == 5
        assert mw.default_max_queue_size == 50
        assert mw.enabled is False

    def test_init_invalid_concurrency(self):
        """Test middleware rejects invalid concurrency."""
        with pytest.raises(ValueError, match="concurrency must be at least 1"):
            BulkheadMiddleware(concurrency=0)

    def test_init_invalid_max_queue_size(self):
        """Test middleware rejects negative max_queue_size."""
        with pytest.raises(ValueError, match="max_queue_size must be non-negative"):
            BulkheadMiddleware(max_queue_size=-1)

    def test_get_config_default(self):
        """Test _get_config returns defaults when no action config."""
        mw = BulkheadMiddleware(concurrency=5, max_queue_size=50)
        action = MagicMock(spec=[])  # No bulkhead attribute

        config = mw._get_config(action)

        assert config.enabled is True
        assert config.concurrency == 5
        assert config.max_queue_size == 50

    def test_get_config_from_dict(self):
        """Test _get_config merges action dict config."""
        mw = BulkheadMiddleware(concurrency=5, max_queue_size=50)
        action = MagicMock()
        action.bulkhead = {"concurrency": 1, "max_queue_size": 10}

        config = mw._get_config(action)

        assert config.enabled is True  # Default
        assert config.concurrency == 1  # Overridden
        assert config.max_queue_size == 10  # Overridden

    def test_get_config_from_bulkhead_config(self):
        """Test _get_config uses BulkheadConfig directly."""
        mw = BulkheadMiddleware()
        action = MagicMock()
        action.bulkhead = BulkheadConfig(enabled=False, concurrency=2, max_queue_size=5)

        config = mw._get_config(action)

        assert config.enabled is False
        assert config.concurrency == 2
        assert config.max_queue_size == 5

    def test_get_state_creates_new(self):
        """Test _get_state creates state for new action."""
        mw = BulkheadMiddleware()

        state = mw._get_state("test.action")

        assert isinstance(state, BulkheadState)
        assert state.in_flight == 0
        assert len(state.queue) == 0
        assert "test.action" in mw._states

    def test_get_state_returns_existing(self):
        """Test _get_state returns existing state."""
        mw = BulkheadMiddleware()

        state1 = mw._get_state("test.action")
        state1.in_flight = 5  # Modify state
        state2 = mw._get_state("test.action")

        assert state1 is state2
        assert state2.in_flight == 5

    def test_get_stats(self):
        """Test get_stats returns current statistics."""
        mw = BulkheadMiddleware()
        state = mw._get_state("test.action")
        state.in_flight = 2
        state.queue.append(MagicMock())  # Fake queue item

        stats = mw.get_stats("test.action")

        assert stats == {"in_flight": 2, "queue_size": 1}

    def test_get_stats_unknown_action(self):
        """Test get_stats returns zeros for unknown action."""
        mw = BulkheadMiddleware()

        stats = mw.get_stats("unknown.action")

        assert stats == {"in_flight": 0, "queue_size": 0}

    def test_reset_all(self):
        """Test reset clears all states."""
        mw = BulkheadMiddleware()
        mw._get_state("action1")
        mw._get_state("action2")

        mw.reset()

        assert mw._states == {}

    def test_reset_specific_action(self):
        """Test reset clears specific action state."""
        mw = BulkheadMiddleware()
        mw._get_state("action1")
        mw._get_state("action2")

        mw.reset("action1")

        assert "action1" not in mw._states
        assert "action2" in mw._states


class TestBulkheadMiddlewareHandler:
    """Tests for BulkheadMiddleware handler wrapping."""

    @pytest.fixture
    def middleware(self):
        """Create middleware with low limits for testing."""
        return BulkheadMiddleware(concurrency=2, max_queue_size=3)

    @pytest.fixture
    def action(self):
        """Create mock action."""
        action = MagicMock()
        action.name = "test.action"
        return action

    @pytest.fixture
    def ctx(self):
        """Create mock context."""
        ctx = MagicMock()
        ctx.node_id = "test-node"
        return ctx

    @pytest.mark.asyncio
    async def test_handler_passes_through_when_disabled(self, middleware, action, ctx):
        """Test handler passes through when bulkhead is disabled."""
        action.bulkhead = {"enabled": False}
        next_handler = AsyncMock(return_value="result")

        handler = await middleware.local_action(next_handler, action)
        result = await handler(ctx)

        assert result == "result"
        next_handler.assert_called_once_with(ctx)

    @pytest.mark.asyncio
    async def test_handler_executes_under_limit(self, middleware, action, ctx):
        """Test handler executes immediately when under concurrency limit."""
        next_handler = AsyncMock(return_value="result")

        handler = await middleware.local_action(next_handler, action)
        result = await handler(ctx)

        assert result == "result"
        next_handler.assert_called_once_with(ctx)

        # Check stats
        stats = middleware.get_stats("test.action")
        assert stats["in_flight"] == 0  # Released after completion

    @pytest.mark.asyncio
    async def test_handler_queues_when_at_limit(self, middleware, action, ctx):
        """Test handler queues request when at concurrency limit."""
        # Block to keep first requests running
        block_event = asyncio.Event()
        release_event = asyncio.Event()
        call_count = 0

        async def slow_handler(ctx):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:  # First 2 calls block
                await block_event.wait()
            release_event.set()
            return f"result-{call_count}"

        handler = await middleware.local_action(slow_handler, action)

        # Start 2 concurrent requests (at limit)
        task1 = asyncio.create_task(handler(ctx))
        task2 = asyncio.create_task(handler(ctx))
        await asyncio.sleep(0.01)  # Let them start

        # Third request should be queued
        task3 = asyncio.create_task(handler(ctx))
        await asyncio.sleep(0.01)

        stats = middleware.get_stats("test.action")
        assert stats["in_flight"] == 2
        assert stats["queue_size"] == 1

        # Release blocked requests
        block_event.set()
        await asyncio.gather(task1, task2, task3)

        # All completed
        stats = middleware.get_stats("test.action")
        assert stats["in_flight"] == 0
        assert stats["queue_size"] == 0

    @pytest.mark.asyncio
    async def test_handler_rejects_when_queue_full(self, middleware, action, ctx):
        """Test handler raises QueueIsFullError when queue is full."""
        # Block to keep requests in queue
        block_event = asyncio.Event()

        async def slow_handler(ctx):
            await block_event.wait()
            return "result"

        handler = await middleware.local_action(slow_handler, action)

        # Fill concurrency (2) + queue (3) = 5 total
        tasks = []
        for _ in range(5):
            tasks.append(asyncio.create_task(handler(ctx)))
            await asyncio.sleep(0.01)

        # 6th request should be rejected
        with pytest.raises(QueueIsFullError) as exc_info:
            await handler(ctx)

        assert exc_info.value.action_name == "test.action"

        # Cleanup
        block_event.set()
        await asyncio.gather(*tasks)

    @pytest.mark.asyncio
    async def test_handler_handles_cancellation(self, middleware, action, ctx):
        """Test handler handles cancellation while queued."""
        block_event = asyncio.Event()

        async def slow_handler(ctx):
            await block_event.wait()
            return "result"

        handler = await middleware.local_action(slow_handler, action)

        # Fill concurrency
        task1 = asyncio.create_task(handler(ctx))
        task2 = asyncio.create_task(handler(ctx))
        await asyncio.sleep(0.01)

        # Queue a request
        task3 = asyncio.create_task(handler(ctx))
        await asyncio.sleep(0.01)

        # Cancel the queued request
        task3.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task3

        # Queue should be empty now
        stats = middleware.get_stats("test.action")
        assert stats["queue_size"] == 0

        # Cleanup
        block_event.set()
        await asyncio.gather(task1, task2)

    @pytest.mark.asyncio
    async def test_handler_releases_on_error(self, middleware, action, ctx):
        """Test handler releases slot when handler raises."""
        next_handler = AsyncMock(side_effect=ValueError("test error"))

        handler = await middleware.local_action(next_handler, action)

        with pytest.raises(ValueError, match="test error"):
            await handler(ctx)

        # Slot should be released
        stats = middleware.get_stats("test.action")
        assert stats["in_flight"] == 0

    @pytest.mark.asyncio
    async def test_remote_action_uses_bulkhead(self, middleware, action, ctx):
        """Test remote_action also applies bulkhead."""
        next_handler = AsyncMock(return_value="result")

        handler = await middleware.remote_action(next_handler, action)
        result = await handler(ctx)

        assert result == "result"


class TestBulkheadConcurrency:
    """Tests for concurrent behavior of BulkheadMiddleware."""

    @pytest.mark.asyncio
    async def test_fifo_ordering(self):
        """Test queued requests are processed in FIFO order."""
        mw = BulkheadMiddleware(concurrency=1, max_queue_size=10)
        action = MagicMock()
        action.name = "test.action"

        results = []
        block_event = asyncio.Event()

        async def handler(ctx):
            if ctx.id == 1:
                await block_event.wait()
            results.append(ctx.id)
            return ctx.id

        wrapped = await mw.local_action(handler, action)

        # Create contexts with IDs
        ctxs = [MagicMock(id=i, node_id="test") for i in range(1, 6)]

        # Start all requests
        tasks = [asyncio.create_task(wrapped(ctx)) for ctx in ctxs]
        await asyncio.sleep(0.05)  # Let them queue

        # Release first request
        block_event.set()
        await asyncio.gather(*tasks)

        # Should be processed in order
        assert results == [1, 2, 3, 4, 5]

    @pytest.mark.asyncio
    async def test_high_concurrency(self):
        """Test middleware handles high concurrency."""
        mw = BulkheadMiddleware(concurrency=10, max_queue_size=100)
        action = MagicMock()
        action.name = "test.action"

        call_count = 0

        async def handler(ctx):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.001)  # Simulate work
            return call_count

        wrapped = await mw.local_action(handler, action)

        # Run 50 concurrent requests
        ctxs = [MagicMock(node_id="test") for _ in range(50)]
        results = await asyncio.gather(*[wrapped(ctx) for ctx in ctxs])

        assert len(results) == 50
        assert call_count == 50

        stats = mw.get_stats("test.action")
        assert stats["in_flight"] == 0
        assert stats["queue_size"] == 0
