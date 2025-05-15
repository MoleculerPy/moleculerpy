"""Unit tests for the Throttle Middleware module.

Tests for ThrottleMiddleware that provides event throttling
for rate limiting execution frequency.
"""

import asyncio
from unittest.mock import MagicMock

import pytest

from moleculerpy.middleware.throttle import ThrottleMiddleware


class TestThrottleMiddlewareInit:
    """Tests for ThrottleMiddleware initialization."""

    def test_init_default_logger(self):
        """Test middleware initializes with default logger."""
        mw = ThrottleMiddleware()

        assert mw.logger is not None
        assert mw.logger.name == "moleculerpy.middleware.throttle"

    def test_init_custom_logger(self):
        """Test middleware with custom logger."""
        custom_logger = MagicMock()
        mw = ThrottleMiddleware(logger=custom_logger)

        assert mw.logger is custom_logger

    def test_repr(self):
        """Test __repr__ returns readable string."""
        mw = ThrottleMiddleware()

        assert repr(mw) == "ThrottleMiddleware()"


class TestNoThrottle:
    """Tests for events without throttle."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ThrottleMiddleware()

    @pytest.mark.asyncio
    async def test_no_throttle_returns_original(self, middleware):
        """Test handler returned as-is when throttle not configured."""

        async def handler(ctx):
            return "result"

        event = MagicMock()
        event.name = "test.event"
        event.throttle = 0

        wrapped = await middleware.local_event(handler, event)

        assert wrapped is handler

    @pytest.mark.asyncio
    async def test_no_throttle_attribute(self, middleware):
        """Test handler returned as-is when throttle attribute missing."""

        async def handler(ctx):
            return "result"

        event = MagicMock(spec=["name"])
        event.name = "test.event"

        wrapped = await middleware.local_event(handler, event)

        assert wrapped is handler

    @pytest.mark.asyncio
    async def test_negative_throttle(self, middleware):
        """Test handler returned as-is when throttle is negative."""

        async def handler(ctx):
            return "result"

        event = MagicMock()
        event.name = "test.event"
        event.throttle = -100

        wrapped = await middleware.local_event(handler, event)

        assert wrapped is handler


class TestThrottleBasic:
    """Tests for basic throttle behavior."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ThrottleMiddleware()

    @pytest.mark.asyncio
    async def test_first_call_executes(self, middleware):
        """Test first call executes immediately."""
        call_count = 0

        async def handler(ctx):
            nonlocal call_count
            call_count += 1
            return "result"

        event = MagicMock()
        event.name = "test.event"
        event.throttle = 100  # 100ms

        wrapped = await middleware.local_event(handler, event)
        ctx = MagicMock()

        # First call should execute
        await wrapped(ctx)
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_rapid_calls_throttled(self, middleware):
        """Test rapid calls are throttled."""
        call_count = 0

        async def handler(ctx):
            nonlocal call_count
            call_count += 1

        event = MagicMock()
        event.name = "test.event"
        event.throttle = 100  # 100ms

        wrapped = await middleware.local_event(handler, event)
        ctx = MagicMock()

        # Rapid fire calls
        for _ in range(10):
            await wrapped(ctx)

        # Only first should execute
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_throttle_allows_after_interval(self, middleware):
        """Test call allowed after throttle interval passes."""
        call_count = 0

        async def handler(ctx):
            nonlocal call_count
            call_count += 1

        event = MagicMock()
        event.name = "test.event"
        event.throttle = 50  # 50ms

        wrapped = await middleware.local_event(handler, event)
        ctx = MagicMock()

        # First call
        await wrapped(ctx)
        assert call_count == 1

        # Wait for throttle to pass
        await asyncio.sleep(0.1)

        # Second call should execute
        await wrapped(ctx)
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_throttle_returns_none_when_skipped(self, middleware):
        """Test skipped calls return None."""

        async def handler(ctx):
            return "result"

        event = MagicMock()
        event.name = "test.event"
        event.throttle = 100

        wrapped = await middleware.local_event(handler, event)
        ctx = MagicMock()

        # First call returns result
        result1 = await wrapped(ctx)
        assert result1 == "result"

        # Second call (throttled) returns None
        result2 = await wrapped(ctx)
        assert result2 is None


class TestThrottleVsDebounce:
    """Tests comparing throttle vs debounce behavior."""

    @pytest.mark.asyncio
    async def test_throttle_executes_first(self):
        """Test throttle executes FIRST event (unlike debounce)."""
        mw = ThrottleMiddleware()
        received_values = []

        async def handler(ctx):
            received_values.append(ctx.params.get("value"))

        event = MagicMock()
        event.name = "test.event"
        event.throttle = 100

        wrapped = await mw.local_event(handler, event)

        # Send events with different values
        for i in range(5):
            ctx = MagicMock()
            ctx.params = {"value": i}
            await wrapped(ctx)

        # Throttle: only FIRST value executed
        assert received_values == [0]

    @pytest.mark.asyncio
    async def test_throttle_periodic_execution(self):
        """Test throttle allows periodic execution."""
        mw = ThrottleMiddleware()
        call_times = []

        async def handler(ctx):
            call_times.append(asyncio.get_event_loop().time())

        event = MagicMock()
        event.name = "test.event"
        event.throttle = 50  # 50ms

        wrapped = await mw.local_event(handler, event)
        ctx = MagicMock()

        # Call with intervals
        await wrapped(ctx)  # t=0: executes
        await asyncio.sleep(0.06)  # t=60ms
        await wrapped(ctx)  # Executes (>50ms passed)
        await asyncio.sleep(0.06)  # t=120ms
        await wrapped(ctx)  # Executes (>50ms passed)

        # Should have 3 executions
        assert len(call_times) == 3


class TestThrottleEdgeCases:
    """Tests for throttle edge cases."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ThrottleMiddleware()

    @pytest.mark.asyncio
    async def test_separate_event_throttles(self, middleware):
        """Test each event has its own throttle state."""
        call_counts = {"event1": 0, "event2": 0}

        async def handler1(ctx):
            call_counts["event1"] += 1

        async def handler2(ctx):
            call_counts["event2"] += 1

        event1 = MagicMock()
        event1.name = "event1"
        event1.throttle = 100

        event2 = MagicMock()
        event2.name = "event2"
        event2.throttle = 100

        wrapped1 = await middleware.local_event(handler1, event1)
        wrapped2 = await middleware.local_event(handler2, event2)

        # Both events should execute their first call
        await wrapped1(MagicMock())
        await wrapped2(MagicMock())

        assert call_counts["event1"] == 1
        assert call_counts["event2"] == 1

        # Rapid calls to both should be throttled
        for _ in range(5):
            await wrapped1(MagicMock())
            await wrapped2(MagicMock())

        # Still only 1 each
        assert call_counts["event1"] == 1
        assert call_counts["event2"] == 1

    @pytest.mark.asyncio
    async def test_handler_error_resets_throttle(self, middleware):
        """Test handler error doesn't break throttle."""
        call_count = 0

        async def failing_handler(ctx):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("First call fails")
            return "success"

        event = MagicMock()
        event.name = "test.event"
        event.throttle = 50

        wrapped = await middleware.local_event(failing_handler, event)
        ctx = MagicMock()

        # First call raises
        with pytest.raises(ValueError):
            await wrapped(ctx)

        # Immediate retry is throttled
        result = await wrapped(ctx)
        assert result is None

        # After interval, can retry
        await asyncio.sleep(0.1)
        result = await wrapped(ctx)
        assert result == "success"


class TestThrottleIntegration:
    """Integration-style tests for throttle patterns."""

    @pytest.mark.asyncio
    async def test_api_rate_limiting_pattern(self):
        """Test typical API rate limiting pattern."""
        mw = ThrottleMiddleware()
        api_calls = []

        async def call_external_api(ctx):
            api_calls.append(ctx.params.get("endpoint"))

        event = MagicMock()
        event.name = "api.call"
        event.throttle = 100  # Max 10 calls per second

        wrapped = await mw.local_event(call_external_api, event)

        # Simulate burst of API calls
        for i in range(20):
            ctx = MagicMock()
            ctx.params = {"endpoint": f"/api/resource/{i}"}
            await wrapped(ctx)

        # Only first call executed
        assert len(api_calls) == 1
        assert api_calls[0] == "/api/resource/0"

    @pytest.mark.asyncio
    async def test_logging_throttle_pattern(self):
        """Test log throttling pattern."""
        mw = ThrottleMiddleware()
        log_messages = []

        async def log_handler(ctx):
            log_messages.append(ctx.params.get("message"))

        event = MagicMock()
        event.name = "log.warning"
        event.throttle = 50  # Max one log per 50ms

        wrapped = await mw.local_event(log_handler, event)

        # Burst of warnings
        for i in range(10):
            ctx = MagicMock()
            ctx.params = {"message": f"Warning {i}"}
            await wrapped(ctx)

        # Only first logged
        assert log_messages == ["Warning 0"]

        # After interval, next warning logged
        await asyncio.sleep(0.1)
        ctx = MagicMock()
        ctx.params = {"message": "Warning after interval"}
        await wrapped(ctx)

        assert log_messages == ["Warning 0", "Warning after interval"]
