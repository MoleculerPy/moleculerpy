"""Unit tests for the Debounce Middleware module.

Tests for DebounceMiddleware that provides event debouncing
for deferring execution after a burst of events.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from moleculerpy.middleware.debounce import DebounceMiddleware


class TestDebounceMiddlewareInit:
    """Tests for DebounceMiddleware initialization."""

    def test_init_default_logger(self):
        """Test middleware initializes with default logger."""
        mw = DebounceMiddleware()

        assert mw.logger is not None
        assert mw.logger.name == "moleculerpy.middleware.debounce"

    def test_init_custom_logger(self):
        """Test middleware with custom logger."""
        custom_logger = MagicMock()
        mw = DebounceMiddleware(logger=custom_logger)

        assert mw.logger is custom_logger

    def test_repr(self):
        """Test __repr__ returns readable string."""
        mw = DebounceMiddleware()

        assert repr(mw) == "DebounceMiddleware()"


class TestNoDebounce:
    """Tests for events without debounce."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return DebounceMiddleware()

    @pytest.mark.asyncio
    async def test_no_debounce_returns_original(self, middleware):
        """Test handler returned as-is when debounce not configured."""

        async def handler(ctx):
            return "result"

        event = MagicMock()
        event.name = "test.event"
        event.debounce = 0

        wrapped = await middleware.local_event(handler, event)

        assert wrapped is handler

    @pytest.mark.asyncio
    async def test_no_debounce_attribute(self, middleware):
        """Test handler returned as-is when debounce attribute missing."""

        async def handler(ctx):
            return "result"

        event = MagicMock(spec=["name"])
        event.name = "test.event"

        wrapped = await middleware.local_event(handler, event)

        assert wrapped is handler

    @pytest.mark.asyncio
    async def test_negative_debounce(self, middleware):
        """Test handler returned as-is when debounce is negative."""

        async def handler(ctx):
            return "result"

        event = MagicMock()
        event.name = "test.event"
        event.debounce = -100

        wrapped = await middleware.local_event(handler, event)

        assert wrapped is handler


class TestDebounceBasic:
    """Tests for basic debounce behavior."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return DebounceMiddleware()

    @pytest.mark.asyncio
    async def test_debounce_defers_execution(self, middleware):
        """Test debounce defers handler execution."""
        call_count = 0

        async def handler(ctx):
            nonlocal call_count
            call_count += 1

        event = MagicMock()
        event.name = "test.event"
        event.debounce = 50  # 50ms

        wrapped = await middleware.local_event(handler, event)
        ctx = MagicMock()

        # Call handler
        await wrapped(ctx)

        # Should not be called immediately
        assert call_count == 0

        # Wait for debounce period
        await asyncio.sleep(0.1)

        # Should be called now
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_debounce_returns_immediately(self, middleware):
        """Test debounce wrapper returns immediately."""
        call_count = 0

        async def slow_handler(ctx):
            nonlocal call_count
            await asyncio.sleep(0.5)
            call_count += 1

        event = MagicMock()
        event.name = "test.event"
        event.debounce = 50

        wrapped = await middleware.local_event(slow_handler, event)
        ctx = MagicMock()

        # Call should return immediately
        start = asyncio.get_event_loop().time()
        result = await wrapped(ctx)
        elapsed = asyncio.get_event_loop().time() - start

        # Should return None immediately (not wait for handler)
        assert result is None
        assert elapsed < 0.1

        # Clean up
        await asyncio.sleep(0.6)

    @pytest.mark.asyncio
    async def test_debounce_cancels_previous(self, middleware):
        """Test new event cancels previous timer."""
        call_count = 0
        received_values = []

        async def handler(ctx):
            nonlocal call_count
            call_count += 1
            received_values.append(ctx.params.get("value"))

        event = MagicMock()
        event.name = "test.event"
        event.debounce = 100  # 100ms

        wrapped = await middleware.local_event(handler, event)

        # Rapid fire events
        for i in range(5):
            ctx = MagicMock()
            ctx.params = {"value": i}
            await wrapped(ctx)
            await asyncio.sleep(0.02)  # 20ms between events

        # Should not be called yet (still within debounce)
        assert call_count == 0

        # Wait for debounce + execution
        await asyncio.sleep(0.2)

        # Should be called only once with the LAST value
        assert call_count == 1
        assert received_values == [4]  # Last value

    @pytest.mark.asyncio
    async def test_debounce_multiple_calls_single_execution(self, middleware):
        """Test multiple rapid calls result in single execution."""
        call_count = 0

        async def handler(ctx):
            nonlocal call_count
            call_count += 1

        event = MagicMock()
        event.name = "test.event"
        event.debounce = 50

        wrapped = await middleware.local_event(handler, event)

        # Call many times rapidly
        for _ in range(10):
            ctx = MagicMock()
            await wrapped(ctx)

        # Wait for debounce
        await asyncio.sleep(0.1)

        # Should only execute once
        assert call_count == 1


class TestDebounceEdgeCases:
    """Tests for debounce edge cases."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return DebounceMiddleware()

    @pytest.mark.asyncio
    async def test_debounce_separate_event_handlers(self, middleware):
        """Test each event has its own debounce timer."""
        call_counts = {"event1": 0, "event2": 0}

        async def handler1(ctx):
            call_counts["event1"] += 1

        async def handler2(ctx):
            call_counts["event2"] += 1

        event1 = MagicMock()
        event1.name = "event1"
        event1.debounce = 50

        event2 = MagicMock()
        event2.name = "event2"
        event2.debounce = 50

        wrapped1 = await middleware.local_event(handler1, event1)
        wrapped2 = await middleware.local_event(handler2, event2)

        # Call both events
        await wrapped1(MagicMock())
        await wrapped2(MagicMock())

        # Wait for debounce
        await asyncio.sleep(0.1)

        # Both should execute
        assert call_counts["event1"] == 1
        assert call_counts["event2"] == 1

    @pytest.mark.asyncio
    async def test_debounce_handler_error_logged(self, middleware):
        """Test handler errors are logged but don't raise."""

        async def failing_handler(ctx):
            raise ValueError("Handler error")

        event = MagicMock()
        event.name = "test.event"
        event.debounce = 50

        wrapped = await middleware.local_event(failing_handler, event)
        ctx = MagicMock()

        # Call handler
        await wrapped(ctx)

        # Wait for debounce - should not raise
        await asyncio.sleep(0.1)

    @pytest.mark.asyncio
    async def test_debounce_error_event_failure_is_logged(self):
        """Failure to emit $debounce.error should be logged, not silently ignored."""
        logger = MagicMock()
        middleware = DebounceMiddleware(logger=logger)

        async def failing_handler(ctx):
            raise ValueError("Handler error")

        event = MagicMock()
        event.name = "test.event"
        event.debounce = 30

        wrapped = await middleware.local_event(failing_handler, event)
        ctx = MagicMock()
        ctx.broker = MagicMock()
        ctx.broker.broadcast_local = AsyncMock(side_effect=RuntimeError("emit failed"))

        await wrapped(ctx)
        await asyncio.sleep(0.1)

        assert logger.warning.called
        assert "$debounce.error" in logger.warning.call_args[0][0]

    @pytest.mark.asyncio
    async def test_debounce_preserves_latest_context(self, middleware):
        """Test debounce preserves the latest context."""
        received_ctx = None

        async def handler(ctx):
            nonlocal received_ctx
            received_ctx = ctx

        event = MagicMock()
        event.name = "test.event"
        event.debounce = 50

        wrapped = await middleware.local_event(handler, event)

        # Call with different contexts
        ctx1 = MagicMock()
        ctx1.id = "ctx1"
        ctx2 = MagicMock()
        ctx2.id = "ctx2"
        ctx3 = MagicMock()
        ctx3.id = "ctx3"

        await wrapped(ctx1)
        await wrapped(ctx2)
        await wrapped(ctx3)

        # Wait for debounce
        await asyncio.sleep(0.1)

        # Should have the last context
        assert received_ctx is ctx3


class TestDebounceIntegration:
    """Integration-style tests for debounce patterns."""

    @pytest.mark.asyncio
    async def test_config_reload_pattern(self):
        """Test typical config reload debounce pattern."""
        mw = DebounceMiddleware()
        reload_count = 0

        async def reload_config(ctx):
            nonlocal reload_count
            reload_count += 1

        event = MagicMock()
        event.name = "config.changed"
        event.debounce = 100  # 100ms

        wrapped = await mw.local_event(reload_config, event)

        # Simulate rapid config changes (e.g., file saves)
        for _ in range(20):
            ctx = MagicMock()
            await wrapped(ctx)
            await asyncio.sleep(0.01)  # 10ms between changes

        # Wait for debounce
        await asyncio.sleep(0.2)

        # Should only reload once
        assert reload_count == 1

    @pytest.mark.asyncio
    async def test_search_debounce_pattern(self):
        """Test search input debounce pattern."""
        mw = DebounceMiddleware()
        search_queries = []

        async def perform_search(ctx):
            search_queries.append(ctx.params.get("query"))

        event = MagicMock()
        event.name = "search.input"
        event.debounce = 100

        wrapped = await mw.local_event(perform_search, event)

        # Simulate typing "hello"
        for char in "hello":
            ctx = MagicMock()
            ctx.params = {"query": char}
            await wrapped(ctx)
            await asyncio.sleep(0.02)

        # Wait for debounce
        await asyncio.sleep(0.15)

        # Should only search for the final query
        assert len(search_queries) == 1
        assert search_queries[0] == "o"  # Last character

    @pytest.mark.asyncio
    async def test_batch_updates_pattern(self):
        """Test batch updates debounce pattern."""
        mw = DebounceMiddleware()
        process_calls = []

        async def process_batch(ctx):
            process_calls.append(ctx.params.get("items"))

        event = MagicMock()
        event.name = "items.updated"
        event.debounce = 50

        wrapped = await mw.local_event(process_batch, event)

        # Simulate multiple item updates
        accumulated = []
        for i in range(10):
            accumulated.append(i)
            ctx = MagicMock()
            ctx.params = {"items": accumulated.copy()}
            await wrapped(ctx)

        # Wait for debounce
        await asyncio.sleep(0.1)

        # Should process with all items
        assert len(process_calls) == 1
        assert process_calls[0] == list(range(10))
