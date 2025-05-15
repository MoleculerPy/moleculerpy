"""Unit tests for the ContextTracker Middleware module.

Tests for ContextTrackerMiddleware that provides graceful shutdown support
by tracking active request contexts.
"""

import asyncio
from unittest.mock import MagicMock

import pytest

from moleculerpy.middleware.context_tracker import (
    ContextTrackerMiddleware,
    GracefulStopTimeoutError,
)


class TestGracefulStopTimeoutError:
    """Tests for GracefulStopTimeoutError exception."""

    def test_default_message(self):
        """Test error with default message."""
        error = GracefulStopTimeoutError()

        assert "Shutdown timeout reached" in str(error)
        assert error.code == 500
        assert error.type == "GRACEFUL_STOP_TIMEOUT"
        assert error.service_name is None

    def test_message_with_service(self):
        """Test error with service name."""
        error = GracefulStopTimeoutError(service_name="users")

        assert "users" in str(error)
        assert error.service_name == "users"
        assert error.data == {"service": "users"}

    def test_custom_message(self):
        """Test error with custom message."""
        error = GracefulStopTimeoutError(message="Custom timeout")

        assert str(error) == "Custom timeout"


class TestContextTrackerMiddlewareInit:
    """Tests for ContextTrackerMiddleware initialization."""

    def test_init_defaults(self):
        """Test middleware initializes with defaults."""
        mw = ContextTrackerMiddleware()

        assert mw.logger is not None
        assert mw.logger.name == "moleculerpy.middleware.context_tracker"
        assert mw._broker is None
        assert mw._default_timeout == 5000
        assert mw._poll_interval == 100

    def test_init_custom_timeout(self):
        """Test middleware with custom timeout."""
        mw = ContextTrackerMiddleware(shutdown_timeout=10000)

        assert mw._default_timeout == 10000

    def test_init_custom_poll_interval(self):
        """Test middleware with custom poll interval."""
        mw = ContextTrackerMiddleware(poll_interval=50)

        assert mw._poll_interval == 50

    def test_init_custom_logger(self):
        """Test middleware with custom logger."""
        custom_logger = MagicMock()
        mw = ContextTrackerMiddleware(logger=custom_logger)

        assert mw.logger is custom_logger

    def test_repr(self):
        """Test __repr__ returns readable string."""
        mw = ContextTrackerMiddleware(shutdown_timeout=3000)

        assert repr(mw) == "ContextTrackerMiddleware(timeout=3000ms)"


class TestTrackingConfiguration:
    """Tests for tracking configuration checks."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ContextTrackerMiddleware()

    def test_tracking_enabled_no_broker(self, middleware):
        """Test tracking enabled when no broker set."""
        assert middleware._is_tracking_enabled() is True

    def test_tracking_enabled_no_settings(self, middleware):
        """Test tracking enabled when broker has no settings."""
        broker = MagicMock(spec=["id"])
        middleware.broker_created(broker)

        assert middleware._is_tracking_enabled() is True

    def test_tracking_enabled_no_tracking_config(self, middleware):
        """Test tracking enabled when no tracking config."""
        broker = MagicMock()
        broker.settings = MagicMock(spec=["log_level"])
        middleware.broker_created(broker)

        assert middleware._is_tracking_enabled() is True

    def test_tracking_enabled_explicit_true(self, middleware):
        """Test tracking enabled when explicitly set to True."""
        broker = MagicMock()
        broker.settings = MagicMock()
        broker.settings.tracking = {"enabled": True}
        middleware.broker_created(broker)

        assert middleware._is_tracking_enabled() is True

    def test_tracking_disabled_explicit(self, middleware):
        """Test tracking disabled when explicitly set."""
        broker = MagicMock()
        broker.settings = MagicMock()
        broker.settings.tracking = {"enabled": False}
        middleware.broker_created(broker)

        assert middleware._is_tracking_enabled() is False


class TestContextTrackingDecision:
    """Tests for per-context tracking decisions."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        mw = ContextTrackerMiddleware()
        broker = MagicMock()
        broker.settings = MagicMock()
        broker.settings.tracking = {"enabled": True}
        mw.broker_created(broker)
        return mw

    def test_should_track_default(self, middleware):
        """Test context should be tracked by default."""
        ctx = MagicMock()
        ctx.options = None

        assert middleware._should_track_context(ctx) is True

    def test_should_track_explicit_true(self, middleware):
        """Test context tracked when explicitly enabled."""
        ctx = MagicMock()
        ctx.options = MagicMock()
        ctx.options.tracking = True

        assert middleware._should_track_context(ctx) is True

    def test_should_not_track_explicit_false(self, middleware):
        """Test context not tracked when explicitly disabled."""
        ctx = MagicMock()
        ctx.options = MagicMock()
        ctx.options.tracking = False

        assert middleware._should_track_context(ctx) is False

    def test_should_not_track_global_disabled(self):
        """Test context not tracked when global tracking disabled."""
        mw = ContextTrackerMiddleware()
        broker = MagicMock()
        broker.settings = MagicMock()
        broker.settings.tracking = {"enabled": False}
        mw.broker_created(broker)

        ctx = MagicMock()
        ctx.options = None

        assert mw._should_track_context(ctx) is False


class TestContextAddRemove:
    """Tests for context add/remove operations."""

    @pytest.fixture
    def middleware(self):
        """Create middleware with broker."""
        mw = ContextTrackerMiddleware()
        broker = MagicMock()
        broker._tracked_contexts = []
        mw.broker_created(broker)
        return mw

    def test_add_local_context(self, middleware):
        """Test adding local context to service list."""
        service = MagicMock()
        service._tracked_contexts = []

        ctx = MagicMock()
        ctx.service = service

        middleware._add_context(ctx)

        assert ctx in service._tracked_contexts
        assert ctx not in middleware._broker._tracked_contexts

    def test_add_remote_context(self, middleware):
        """Test adding remote context to broker list."""
        ctx = MagicMock()
        ctx.service = None

        middleware._add_context(ctx)

        assert ctx in middleware._broker._tracked_contexts

    def test_remove_local_context(self, middleware):
        """Test removing local context from service list."""
        service = MagicMock()
        ctx = MagicMock()
        ctx.service = service
        service._tracked_contexts = [ctx]

        middleware._remove_context(ctx)

        assert ctx not in service._tracked_contexts

    def test_remove_remote_context(self, middleware):
        """Test removing remote context from broker list."""
        ctx = MagicMock()
        ctx.service = None
        middleware._broker._tracked_contexts.append(ctx)

        middleware._remove_context(ctx)

        assert ctx not in middleware._broker._tracked_contexts

    def test_remove_already_removed(self, middleware):
        """Test removing context that was already removed."""
        ctx = MagicMock()
        ctx.service = None

        # Should not raise
        middleware._remove_context(ctx)

    def test_add_context_no_tracking_list(self, middleware):
        """Test adding context when tracking list not initialized."""
        service = MagicMock(spec=["name"])
        ctx = MagicMock()
        ctx.service = service

        # Should not raise
        middleware._add_context(ctx)


class TestWaitForContexts:
    """Tests for graceful shutdown waiting logic."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ContextTrackerMiddleware(poll_interval=10)

    @pytest.mark.asyncio
    async def test_empty_list_returns_immediately(self, middleware):
        """Test empty list returns without waiting."""
        await middleware._wait_for_contexts([], 1000)

    @pytest.mark.asyncio
    async def test_list_clears_before_timeout(self, middleware):
        """Test waiting succeeds when list clears."""
        tracked = [MagicMock()]

        async def clear_list():
            await asyncio.sleep(0.02)
            tracked.clear()

        task = asyncio.create_task(clear_list())
        await middleware._wait_for_contexts(tracked, 1000)
        task.cancel()

    @pytest.mark.asyncio
    async def test_timeout_raises_error(self, middleware):
        """Test timeout raises GracefulStopTimeoutError."""
        tracked = [MagicMock()]

        with pytest.raises(GracefulStopTimeoutError):
            await middleware._wait_for_contexts(tracked, 50)

        # List should be cleared
        assert len(tracked) == 0

    @pytest.mark.asyncio
    async def test_timeout_with_service_name(self, middleware):
        """Test timeout error includes service name."""
        tracked = [MagicMock()]

        with pytest.raises(GracefulStopTimeoutError) as exc_info:
            await middleware._wait_for_contexts(tracked, 50, "users")

        assert exc_info.value.service_name == "users"


class TestLifecycleHooks:
    """Tests for lifecycle hooks."""

    def test_broker_created_initializes_list(self):
        """Test broker_created initializes tracking list."""
        mw = ContextTrackerMiddleware()
        broker = MagicMock(spec=["id"])

        mw.broker_created(broker)

        assert hasattr(broker, "_tracked_contexts")
        assert broker._tracked_contexts == []

    def test_service_starting_initializes_list(self):
        """Test service_starting initializes tracking list."""
        mw = ContextTrackerMiddleware()
        service = MagicMock()
        service.name = "users"

        mw.service_starting(service)

        assert hasattr(service, "_tracked_contexts")
        assert service._tracked_contexts == []

    @pytest.mark.asyncio
    async def test_service_stopping_empty_list(self):
        """Test service_stopping with no pending contexts."""
        mw = ContextTrackerMiddleware()
        service = MagicMock()
        service.name = "users"
        service._tracked_contexts = []
        service.settings = {}

        await mw.service_stopping(service)

    @pytest.mark.asyncio
    async def test_service_stopping_waits(self):
        """Test service_stopping waits for contexts."""
        mw = ContextTrackerMiddleware(poll_interval=10)
        service = MagicMock()
        service.name = "users"
        service._tracked_contexts = [MagicMock()]
        service.settings = {"$shutdown_timeout": 500}

        async def clear_list():
            await asyncio.sleep(0.02)
            service._tracked_contexts.clear()

        task = asyncio.create_task(clear_list())
        await mw.service_stopping(service)
        task.cancel()

    @pytest.mark.asyncio
    async def test_service_stopping_custom_timeout(self):
        """Test service_stopping uses service-specific timeout."""
        mw = ContextTrackerMiddleware(poll_interval=10, shutdown_timeout=10000)
        service = MagicMock()
        service.name = "users"
        service._tracked_contexts = [MagicMock()]
        service.settings = {"$shutdown_timeout": 50}  # Very short timeout

        # Should timeout quickly due to service-specific timeout
        await mw.service_stopping(service)

        # List should be cleared by timeout
        assert len(service._tracked_contexts) == 0

    @pytest.mark.asyncio
    async def test_broker_stopping_empty_list(self):
        """Test broker_stopping with no pending contexts."""
        mw = ContextTrackerMiddleware()
        broker = MagicMock()
        broker._tracked_contexts = []
        broker.settings = None
        mw._broker = broker

        await mw.broker_stopping(broker)

    @pytest.mark.asyncio
    async def test_broker_stopping_waits(self):
        """Test broker_stopping waits for remote contexts."""
        mw = ContextTrackerMiddleware(poll_interval=10)
        broker = MagicMock()
        broker._tracked_contexts = [MagicMock()]
        broker.settings = MagicMock()
        broker.settings.tracking = {"shutdown_timeout": 500}
        mw._broker = broker

        async def clear_list():
            await asyncio.sleep(0.02)
            broker._tracked_contexts.clear()

        task = asyncio.create_task(clear_list())
        await mw.broker_stopping(broker)
        task.cancel()


class TestHandlerWrappers:
    """Tests for action/event handler wrappers."""

    @pytest.fixture
    def middleware(self):
        """Create middleware with broker."""
        mw = ContextTrackerMiddleware()
        broker = MagicMock()
        broker._tracked_contexts = []
        broker.settings = MagicMock()
        broker.settings.tracking = {"enabled": True}
        mw.broker_created(broker)
        return mw

    @pytest.fixture
    def action(self):
        """Create mock action."""
        action = MagicMock()
        action.name = "test.action"
        return action

    @pytest.fixture
    def event(self):
        """Create mock event."""
        event = MagicMock()
        event.name = "test.event"
        return event

    @pytest.mark.asyncio
    async def test_local_action_tracks_context(self, middleware, action):
        """Test local_action tracks context during execution."""
        service = MagicMock()
        service._tracked_contexts = []

        ctx = MagicMock()
        ctx.service = service
        ctx.options = None

        tracked_during_execution = []

        async def capture_tracked(ctx):
            tracked_during_execution.extend(service._tracked_contexts)
            return "result"

        handler = await middleware.local_action(capture_tracked, action)
        result = await handler(ctx)

        assert result == "result"
        assert ctx in tracked_during_execution
        assert ctx not in service._tracked_contexts  # Removed after completion

    @pytest.mark.asyncio
    async def test_local_action_removes_on_error(self, middleware, action):
        """Test local_action removes context on error."""
        service = MagicMock()
        service._tracked_contexts = []

        ctx = MagicMock()
        ctx.service = service
        ctx.options = None

        async def failing_handler(ctx):
            raise ValueError("test error")

        handler = await middleware.local_action(failing_handler, action)

        with pytest.raises(ValueError):
            await handler(ctx)

        assert ctx not in service._tracked_contexts

    @pytest.mark.asyncio
    async def test_local_action_skips_when_disabled(self, middleware, action):
        """Test local_action skips tracking when disabled."""
        service = MagicMock()
        service._tracked_contexts = []

        ctx = MagicMock()
        ctx.service = service
        ctx.options = MagicMock()
        ctx.options.tracking = False

        tracked_during_execution = []

        async def capture_tracked(ctx):
            tracked_during_execution.extend(service._tracked_contexts)
            return "result"

        handler = await middleware.local_action(capture_tracked, action)
        await handler(ctx)

        assert len(tracked_during_execution) == 0  # Not tracked

    @pytest.mark.asyncio
    async def test_remote_action_tracks_context(self, middleware, action):
        """Test remote_action tracks context at broker level."""
        ctx = MagicMock()
        ctx.service = None
        ctx.options = None

        tracked_during_execution = []

        async def capture_tracked(ctx):
            tracked_during_execution.extend(middleware._broker._tracked_contexts)
            return "result"

        handler = await middleware.remote_action(capture_tracked, action)
        result = await handler(ctx)

        assert result == "result"
        assert ctx in tracked_during_execution
        assert ctx not in middleware._broker._tracked_contexts

    @pytest.mark.asyncio
    async def test_local_event_tracks_context(self, middleware, event):
        """Test local_event tracks context during execution."""
        service = MagicMock()
        service._tracked_contexts = []

        ctx = MagicMock()
        ctx.service = service
        ctx.options = None

        tracked_during_execution = []

        async def capture_tracked(ctx):
            tracked_during_execution.extend(service._tracked_contexts)

        handler = await middleware.local_event(capture_tracked, event)
        await handler(ctx)

        assert ctx in tracked_during_execution
        assert ctx not in service._tracked_contexts


class TestConcurrentTracking:
    """Tests for concurrent context tracking."""

    @pytest.fixture
    def middleware(self):
        """Create middleware with broker."""
        mw = ContextTrackerMiddleware()
        broker = MagicMock()
        broker._tracked_contexts = []
        broker.settings = MagicMock()
        broker.settings.tracking = {"enabled": True}
        mw.broker_created(broker)
        return mw

    @pytest.mark.asyncio
    async def test_multiple_concurrent_contexts(self, middleware):
        """Test tracking multiple concurrent contexts."""
        service = MagicMock()
        service._tracked_contexts = []

        action = MagicMock()
        action.name = "test.action"

        contexts = [MagicMock() for _ in range(5)]
        for ctx in contexts:
            ctx.service = service
            ctx.options = None

        max_tracked = 0

        async def slow_handler(ctx):
            nonlocal max_tracked
            max_tracked = max(max_tracked, len(service._tracked_contexts))
            await asyncio.sleep(0.01)
            return "result"

        handler = await middleware.local_action(slow_handler, action)

        # Run all handlers concurrently
        results = await asyncio.gather(*[handler(ctx) for ctx in contexts])

        assert all(r == "result" for r in results)
        assert max_tracked > 1  # Multiple tracked at same time
        assert len(service._tracked_contexts) == 0  # All removed


class TestIntegrationPatterns:
    """Integration-style tests for common usage patterns."""

    @pytest.mark.asyncio
    async def test_graceful_shutdown_pattern(self):
        """Test typical graceful shutdown flow."""
        mw = ContextTrackerMiddleware(poll_interval=10, shutdown_timeout=500)

        # Setup broker
        broker = MagicMock()
        broker._tracked_contexts = []
        broker.settings = MagicMock()
        broker.settings.tracking = {"enabled": True, "shutdown_timeout": 500}
        mw.broker_created(broker)

        # Setup service
        service = MagicMock()
        service.name = "orders"
        service.settings = {}
        mw.service_starting(service)

        # Create action handler
        action = MagicMock()
        action.name = "orders.process"

        async def process_order(ctx):
            await asyncio.sleep(0.05)  # Simulate work
            return {"processed": True}

        handler = await mw.local_action(process_order, action)

        # Start processing
        ctx = MagicMock()
        ctx.service = service
        ctx.options = None

        process_task = asyncio.create_task(handler(ctx))

        # Wait a bit then stop service
        await asyncio.sleep(0.01)
        assert len(service._tracked_contexts) == 1

        # Stop service - should wait for processing to complete
        await mw.service_stopping(service)

        # Ensure task completed
        result = await process_task
        assert result == {"processed": True}

    @pytest.mark.asyncio
    async def test_mixed_local_remote_tracking(self):
        """Test tracking both local and remote requests."""
        mw = ContextTrackerMiddleware(poll_interval=10)

        broker = MagicMock()
        broker._tracked_contexts = []
        broker.settings = MagicMock()
        broker.settings.tracking = {"enabled": True}
        mw.broker_created(broker)

        service = MagicMock()
        service.name = "mixed"
        service._tracked_contexts = []

        action = MagicMock()
        action.name = "mixed.action"

        # Local context
        local_ctx = MagicMock()
        local_ctx.service = service
        local_ctx.options = None

        # Remote context
        remote_ctx = MagicMock()
        remote_ctx.service = None
        remote_ctx.options = None

        async def handler(ctx):
            return "done"

        local_handler = await mw.local_action(handler, action)
        remote_handler = await mw.remote_action(handler, action)

        # Track during execution
        local_task = asyncio.create_task(local_handler(local_ctx))
        remote_task = asyncio.create_task(remote_handler(remote_ctx))

        await asyncio.gather(local_task, remote_task)

        # Both should be removed after completion
        assert len(service._tracked_contexts) == 0
        assert len(broker._tracked_contexts) == 0
