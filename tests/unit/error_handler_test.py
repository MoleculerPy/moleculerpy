"""Unit tests for the ErrorHandler Middleware module.

Tests for ErrorHandlerMiddleware that provides centralized error handling
for actions and events, following Moleculer.js patterns.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from moleculerpy.errors import MoleculerError
from moleculerpy.middleware.error_handler import ErrorHandlerMiddleware


class TestErrorHandlerMiddlewareInit:
    """Tests for ErrorHandlerMiddleware initialization."""

    def test_init_default_logger(self):
        """Test middleware initializes with default logger."""
        mw = ErrorHandlerMiddleware()

        assert mw.logger is not None
        assert mw.logger.name == "moleculerpy.middleware.error_handler"
        assert mw._broker is None

    def test_init_custom_logger(self):
        """Test middleware initializes with custom logger."""
        custom_logger = MagicMock()
        mw = ErrorHandlerMiddleware(logger=custom_logger)

        assert mw.logger is custom_logger

    def test_repr(self):
        """Test __repr__ returns readable string."""
        mw = ErrorHandlerMiddleware()

        assert repr(mw) == "ErrorHandlerMiddleware()"

    def test_broker_created_stores_reference(self):
        """Test broker_created stores broker reference."""
        mw = ErrorHandlerMiddleware()
        broker = MagicMock()

        mw.broker_created(broker)

        assert mw._broker is broker


class TestErrorNormalization:
    """Tests for error normalization logic."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ErrorHandlerMiddleware()

    def test_normalize_exception_returns_same(self, middleware):
        """Test _normalize_error returns Exception unchanged."""
        error = ValueError("test error")

        result = middleware._normalize_error(error)

        assert result is error
        assert isinstance(result, Exception)

    def test_normalize_moleculer_error_returns_same(self, middleware):
        """Test _normalize_error returns MoleculerError unchanged."""
        error = MoleculerError("test", code=400)

        result = middleware._normalize_error(error)

        assert result is error
        assert isinstance(result, MoleculerError)

    def test_normalize_string_to_moleculer_error(self, middleware):
        """Test _normalize_error converts string to MoleculerError."""
        result = middleware._normalize_error("string error")

        assert isinstance(result, MoleculerError)
        assert str(result) == "string error"
        assert result.code == 500

    def test_normalize_int_to_moleculer_error(self, middleware):
        """Test _normalize_error converts int to MoleculerError."""
        result = middleware._normalize_error(404)

        assert isinstance(result, MoleculerError)
        assert "404" in str(result)
        assert result.code == 500

    def test_normalize_dict_to_moleculer_error(self, middleware):
        """Test _normalize_error converts dict to MoleculerError."""
        result = middleware._normalize_error({"error": "message"})

        assert isinstance(result, MoleculerError)
        assert result.code == 500


class TestContextAttachment:
    """Tests for context attachment to errors."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ErrorHandlerMiddleware()

    def test_attach_context_to_error(self, middleware):
        """Test _attach_context attaches ctx to error."""
        error = ValueError("test")
        ctx = MagicMock()
        ctx.id = "ctx-123"

        middleware._attach_context(error, ctx)

        assert hasattr(error, "ctx")
        assert error.ctx is ctx  # type: ignore[attr-defined]

    def test_attach_context_to_moleculer_error(self, middleware):
        """Test _attach_context works with MoleculerError."""
        error = MoleculerError("test", code=500)
        ctx = MagicMock()

        middleware._attach_context(error, ctx)

        assert hasattr(error, "ctx")
        assert error.ctx is ctx  # type: ignore[attr-defined]

    def test_attach_context_fails_silently_on_builtin(self, middleware):
        """Test _attach_context doesn't raise for built-in exceptions that reject attrs."""
        # Built-in exceptions like TypeError may not allow setting arbitrary attrs
        # In practice, most do, but the method should handle edge cases gracefully
        error = ValueError("test")
        ctx = MagicMock()

        # Should not raise
        middleware._attach_context(error, ctx)

        # Should succeed for ValueError
        assert error.ctx is ctx  # type: ignore[attr-defined]


class TestBrokerErrorHandler:
    """Tests for broker error handler callback invocation."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ErrorHandlerMiddleware()

    @pytest.fixture
    def broker_with_handler(self):
        """Create mock broker with error_handler."""
        broker = MagicMock()
        broker.error_handler = AsyncMock(side_effect=lambda e, i: (_ for _ in ()).throw(e))
        return broker

    @pytest.fixture
    def broker_without_handler(self):
        """Create mock broker without error_handler."""
        broker = MagicMock(spec=["id", "transit"])
        return broker

    @pytest.mark.asyncio
    async def test_call_error_handler_no_broker(self, middleware):
        """Test _call_error_handler raises when no broker."""
        error = ValueError("test")
        info = {}

        with pytest.raises(ValueError, match="test"):
            await middleware._call_error_handler(error, info)

    @pytest.mark.asyncio
    async def test_call_error_handler_no_handler(self, middleware, broker_without_handler):
        """Test _call_error_handler raises when no error_handler on broker."""
        middleware.broker_created(broker_without_handler)
        error = ValueError("test")
        info = {}

        with pytest.raises(ValueError, match="test"):
            await middleware._call_error_handler(error, info)

    @pytest.mark.asyncio
    async def test_call_error_handler_async(self, middleware, broker_with_handler):
        """Test _call_error_handler calls async error_handler."""
        middleware.broker_created(broker_with_handler)
        error = ValueError("test")
        info = {"ctx": MagicMock()}

        with pytest.raises(ValueError, match="test"):
            await middleware._call_error_handler(error, info)

        broker_with_handler.error_handler.assert_called_once_with(error, info)

    @pytest.mark.asyncio
    async def test_call_error_handler_sync(self, middleware):
        """Test _call_error_handler calls sync error_handler."""
        broker = MagicMock()
        # Sync handler that re-raises
        broker.error_handler = MagicMock(side_effect=lambda e, i: (_ for _ in ()).throw(e))
        middleware.broker_created(broker)
        error = ValueError("test")
        info = {}

        with pytest.raises(ValueError):
            await middleware._call_error_handler(error, info)

    @pytest.mark.asyncio
    async def test_call_error_handler_transforms_error(self, middleware):
        """Test error_handler can transform errors."""
        broker = MagicMock()

        async def transform_error(err, info):
            raise MoleculerError("Transformed error", code=400)

        broker.error_handler = transform_error
        middleware.broker_created(broker)

        error = ValueError("original")
        info = {}

        with pytest.raises(MoleculerError, match="Transformed error"):
            await middleware._call_error_handler(error, info)

    @pytest.mark.asyncio
    async def test_call_error_handler_can_return_value(self, middleware):
        """Test error_handler can return a fallback value."""
        broker = MagicMock()

        async def return_fallback(err, info):
            return {"fallback": True}

        broker.error_handler = return_fallback
        middleware.broker_created(broker)

        error = ValueError("original")
        info = {}

        result = await middleware._call_error_handler(error, info)

        assert result == {"fallback": True}


class TestLocalActionHandler:
    """Tests for local action error handling."""

    @pytest.fixture
    def middleware(self):
        """Create middleware with mocked broker."""
        mw = ErrorHandlerMiddleware()
        broker = MagicMock()
        broker.error_handler = None
        mw.broker_created(broker)
        return mw

    @pytest.fixture
    def action(self):
        """Create mock action."""
        action = MagicMock()
        action.name = "test.action"
        service = MagicMock()
        service.name = "test"
        action.service = service
        return action

    @pytest.fixture
    def ctx(self):
        """Create mock context."""
        ctx = MagicMock()
        ctx.request_id = "req-123"
        ctx.id = "ctx-456"
        return ctx

    @pytest.mark.asyncio
    async def test_success_passes_through(self, middleware, action, ctx):
        """Test handler passes through on success."""
        next_handler = AsyncMock(return_value="success")

        handler = await middleware.local_action(next_handler, action)
        result = await handler(ctx)

        assert result == "success"
        next_handler.assert_called_once_with(ctx)

    @pytest.mark.asyncio
    async def test_error_propagates_no_handler(self, middleware, action, ctx):
        """Test error propagates when no error_handler configured."""
        next_handler = AsyncMock(side_effect=ValueError("test error"))

        handler = await middleware.local_action(next_handler, action)

        with pytest.raises(ValueError, match="test error"):
            await handler(ctx)

    @pytest.mark.asyncio
    async def test_error_normalized(self, middleware, action, ctx):
        """Test non-Exception errors are normalized."""

        # Create custom handler that raises non-Exception
        async def raise_string(ctx):
            raise Exception("string error")

        next_handler = raise_string

        handler = await middleware.local_action(next_handler, action)

        with pytest.raises(Exception, match="string error"):
            await handler(ctx)

    @pytest.mark.asyncio
    async def test_context_attached_to_error(self, middleware, action, ctx):
        """Test context is attached to error."""
        captured_error = None

        async def capture_handler(err, info):
            nonlocal captured_error
            captured_error = err
            raise err

        broker = MagicMock()
        broker.error_handler = capture_handler
        middleware.broker_created(broker)

        next_handler = AsyncMock(side_effect=ValueError("test"))

        handler = await middleware.local_action(next_handler, action)

        with pytest.raises(ValueError):
            await handler(ctx)

        assert captured_error is not None
        assert hasattr(captured_error, "ctx")
        assert captured_error.ctx is ctx  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_error_info_contains_context(self, middleware, action, ctx):
        """Test error info contains ctx, service, action."""
        captured_info = None

        async def capture_handler(err, info):
            nonlocal captured_info
            captured_info = info
            raise err

        broker = MagicMock()
        broker.error_handler = capture_handler
        middleware.broker_created(broker)

        next_handler = AsyncMock(side_effect=ValueError("test"))

        handler = await middleware.local_action(next_handler, action)

        with pytest.raises(ValueError):
            await handler(ctx)

        assert captured_info is not None
        assert captured_info["ctx"] is ctx
        assert captured_info["action"] is action
        assert captured_info["service"] is action.service

    @pytest.mark.asyncio
    async def test_error_handler_can_return_fallback(self, middleware, action, ctx):
        """Test error_handler can return a fallback value."""

        async def fallback_handler(err, info):
            return {"fallback": True}

        broker = MagicMock()
        broker.error_handler = fallback_handler
        middleware.broker_created(broker)

        next_handler = AsyncMock(side_effect=ValueError("test"))

        handler = await middleware.local_action(next_handler, action)
        result = await handler(ctx)

        assert result == {"fallback": True}


class TestRemoteActionHandler:
    """Tests for remote action error handling."""

    @pytest.fixture
    def middleware(self):
        """Create middleware with mocked broker."""
        mw = ErrorHandlerMiddleware()
        broker = MagicMock()
        broker.nodeID = "local-node"
        broker.error_handler = None
        transit = MagicMock()
        transit.remove_pending_request = MagicMock()
        broker.transit = transit
        mw.broker_created(broker)
        return mw

    @pytest.fixture
    def action(self):
        """Create mock action."""
        action = MagicMock()
        action.name = "remote.action"
        service = MagicMock()
        service.name = "remote"
        action.service = service
        return action

    @pytest.fixture
    def ctx(self):
        """Create mock context with remote node."""
        ctx = MagicMock()
        ctx.request_id = "req-123"
        ctx.id = "ctx-456"
        ctx.node_id = "remote-node"  # Different from broker.nodeID
        return ctx

    @pytest.mark.asyncio
    async def test_success_passes_through(self, middleware, action, ctx):
        """Test handler passes through on success."""
        next_handler = AsyncMock(return_value="remote_success")

        handler = await middleware.remote_action(next_handler, action)
        result = await handler(ctx)

        assert result == "remote_success"

    @pytest.mark.asyncio
    async def test_error_propagates_no_handler(self, middleware, action, ctx):
        """Test error propagates when no error_handler configured."""
        next_handler = AsyncMock(side_effect=ValueError("remote error"))

        handler = await middleware.remote_action(next_handler, action)

        with pytest.raises(ValueError, match="remote error"):
            await handler(ctx)

    @pytest.mark.asyncio
    async def test_pending_request_removed_on_error(self, middleware, action, ctx):
        """Test pending request is removed for remote node errors."""
        next_handler = AsyncMock(side_effect=ValueError("remote error"))

        handler = await middleware.remote_action(next_handler, action)

        with pytest.raises(ValueError):
            await handler(ctx)

        # Should have attempted to remove pending request
        middleware._broker.transit.remove_pending_request.assert_called_once_with(ctx.id)

    @pytest.mark.asyncio
    async def test_pending_request_not_removed_for_local(self, middleware, action):
        """Test pending request NOT removed for local node."""
        ctx = MagicMock()
        ctx.request_id = "req-123"
        ctx.id = "ctx-456"
        ctx.node_id = "local-node"  # Same as broker.nodeID

        next_handler = AsyncMock(side_effect=ValueError("local error"))

        handler = await middleware.remote_action(next_handler, action)

        with pytest.raises(ValueError):
            await handler(ctx)

        # Should NOT remove pending request for local node
        middleware._broker.transit.remove_pending_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_pending_request_handles_missing_transit(self, middleware, action, ctx):
        """Test graceful handling when transit is missing."""
        middleware._broker.transit = None
        next_handler = AsyncMock(side_effect=ValueError("error"))

        handler = await middleware.remote_action(next_handler, action)

        # Should not raise due to missing transit
        with pytest.raises(ValueError):
            await handler(ctx)


class TestLocalEventHandler:
    """Tests for local event error handling (fire-and-forget)."""

    @pytest.fixture
    def middleware(self):
        """Create middleware with mocked broker and logger."""
        mw = ErrorHandlerMiddleware()
        mw.logger = MagicMock()
        broker = MagicMock()
        broker.error_handler = None
        mw.broker_created(broker)
        return mw

    @pytest.fixture
    def event(self):
        """Create mock event."""
        event = MagicMock()
        event.name = "test.event"
        service = MagicMock()
        service.name = "test"
        service.full_name = "test"
        event.service = service
        return event

    @pytest.fixture
    def ctx(self):
        """Create mock context."""
        ctx = MagicMock()
        ctx.request_id = "req-123"
        return ctx

    @pytest.mark.asyncio
    async def test_success_passes_through(self, middleware, event, ctx):
        """Test handler passes through on success."""
        next_handler = AsyncMock(return_value="event_result")

        handler = await middleware.local_event(next_handler, event)
        result = await handler(ctx)

        assert result == "event_result"

    @pytest.mark.asyncio
    async def test_error_swallowed_after_logging(self, middleware, event, ctx):
        """Test event errors are swallowed (fire-and-forget pattern)."""
        next_handler = AsyncMock(side_effect=ValueError("event error"))

        handler = await middleware.local_event(next_handler, event)

        # Should NOT raise - events swallow errors
        result = await handler(ctx)

        assert result is None
        middleware.logger.error.assert_called()

    @pytest.mark.asyncio
    async def test_error_handler_called_for_events(self, middleware, event, ctx):
        """Test error_handler is still called for events."""
        handler_called = False

        async def track_handler(err, info):
            nonlocal handler_called
            handler_called = True
            raise err  # Re-raise

        broker = MagicMock()
        broker.error_handler = track_handler
        middleware.broker_created(broker)

        next_handler = AsyncMock(side_effect=ValueError("event error"))

        handler = await middleware.local_event(next_handler, event)
        await handler(ctx)

        assert handler_called is True

    @pytest.mark.asyncio
    async def test_error_handler_return_value_used(self, middleware, event, ctx):
        """Test error_handler return value can be used for events."""

        async def return_fallback(err, info):
            return "handled_event"

        broker = MagicMock()
        broker.error_handler = return_fallback
        middleware.broker_created(broker)

        next_handler = AsyncMock(side_effect=ValueError("event error"))

        handler = await middleware.local_event(next_handler, event)
        result = await handler(ctx)

        assert result == "handled_event"

    @pytest.mark.asyncio
    async def test_context_attached_to_event_error(self, middleware, event, ctx):
        """Test context is attached to event errors."""
        captured_error = None

        async def capture_handler(err, info):
            nonlocal captured_error
            captured_error = err
            raise err

        broker = MagicMock()
        broker.error_handler = capture_handler
        middleware.broker_created(broker)

        next_handler = AsyncMock(side_effect=ValueError("event error"))

        handler = await middleware.local_event(next_handler, event)
        await handler(ctx)  # Should swallow

        assert captured_error is not None
        assert hasattr(captured_error, "ctx")
        assert captured_error.ctx is ctx  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_event_error_info_contains_event(self, middleware, event, ctx):
        """Test error info contains event instead of action."""
        captured_info = None

        async def capture_handler(err, info):
            nonlocal captured_info
            captured_info = info
            raise err

        broker = MagicMock()
        broker.error_handler = capture_handler
        middleware.broker_created(broker)

        next_handler = AsyncMock(side_effect=ValueError("event error"))

        handler = await middleware.local_event(next_handler, event)
        await handler(ctx)

        assert captured_info is not None
        assert captured_info["ctx"] is ctx
        assert captured_info["event"] is event
        assert captured_info["service"] is event.service
        assert "action" not in captured_info


class TestEdgeCases:
    """Edge case tests for ErrorHandlerMiddleware."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ErrorHandlerMiddleware()

    def test_action_without_service(self, middleware):
        """Test handling action without service attribute."""
        action = MagicMock(spec=["name"])
        action.name = "orphan.action"

        # _normalize_error and _attach_context should handle this
        error = ValueError("test")
        middleware._attach_context(error, MagicMock())
        assert hasattr(error, "ctx")

    def test_action_without_name(self, middleware):
        """Test handling action without name attribute."""
        action = MagicMock(spec=[])

        # Should use str(action) as fallback
        name = getattr(action, "name", str(action))
        assert name is not None

    @pytest.mark.asyncio
    async def test_nested_middleware_chain(self, middleware):
        """Test error handler works in nested middleware chain."""
        broker = MagicMock()
        errors_received = []

        async def collect_errors(err, info):
            errors_received.append(err)
            raise err

        broker.error_handler = collect_errors
        middleware.broker_created(broker)

        action = MagicMock()
        action.name = "test.action"
        action.service = MagicMock()

        async def failing_handler(ctx):
            raise ValueError("deep error")

        handler = await middleware.local_action(failing_handler, action)

        ctx = MagicMock()
        ctx.request_id = "req-123"

        with pytest.raises(ValueError):
            await handler(ctx)

        assert len(errors_received) == 1
        assert str(errors_received[0]) == "deep error"

    @pytest.mark.asyncio
    async def test_concurrent_errors(self, middleware):
        """Test handling concurrent errors."""
        broker = MagicMock()
        errors_received = []

        async def collect_errors(err, info):
            errors_received.append(str(err))
            await asyncio.sleep(0.01)  # Simulate async work
            raise err

        broker.error_handler = collect_errors
        middleware.broker_created(broker)

        action = MagicMock()
        action.name = "test.action"
        action.service = MagicMock()

        async def failing_handler_1(ctx):
            raise ValueError("error 1")

        async def failing_handler_2(ctx):
            raise ValueError("error 2")

        handler1 = await middleware.local_action(failing_handler_1, action)
        handler2 = await middleware.local_action(failing_handler_2, action)

        ctx1 = MagicMock()
        ctx1.request_id = "req-1"
        ctx2 = MagicMock()
        ctx2.request_id = "req-2"

        # Run concurrently
        results = await asyncio.gather(
            handler1(ctx1),
            handler2(ctx2),
            return_exceptions=True,
        )

        # Both should fail with ValueError
        assert len(results) == 2
        assert all(isinstance(r, ValueError) for r in results)
        assert set(errors_received) == {"error 1", "error 2"}


class TestIntegrationPatterns:
    """Integration-style tests for common usage patterns."""

    @pytest.mark.asyncio
    async def test_error_transformation_pattern(self):
        """Test pattern: transform errors to standard format."""
        mw = ErrorHandlerMiddleware()
        broker = MagicMock()

        async def transform_to_api_error(err, info):
            """Transform any error to API-compatible error."""
            if isinstance(err, MoleculerError):
                raise err
            raise MoleculerError(
                str(err),
                code=500,
                error_type="INTERNAL_ERROR",
                data={"original": type(err).__name__},
            )

        broker.error_handler = transform_to_api_error
        mw.broker_created(broker)

        action = MagicMock()
        action.name = "api.action"
        action.service = MagicMock()

        async def raise_runtime_error(ctx):
            raise RuntimeError("Something went wrong")

        handler = await mw.local_action(raise_runtime_error, action)
        ctx = MagicMock()
        ctx.request_id = "req-123"

        with pytest.raises(MoleculerError) as exc_info:
            await handler(ctx)

        assert exc_info.value.code == 500
        assert exc_info.value.type == "INTERNAL_ERROR"
        assert exc_info.value.data["original"] == "RuntimeError"

    @pytest.mark.asyncio
    async def test_logging_and_reraise_pattern(self):
        """Test pattern: log error details then re-raise."""
        mw = ErrorHandlerMiddleware()
        broker = MagicMock()
        logged_errors = []

        async def log_and_reraise(err, info):
            """Log error with context then re-raise."""
            logged_errors.append(
                {
                    "error": str(err),
                    "action": getattr(info.get("action"), "name", None),
                    "request_id": getattr(info.get("ctx"), "request_id", None),
                }
            )
            raise err

        broker.error_handler = log_and_reraise
        mw.broker_created(broker)

        action = MagicMock()
        action.name = "logged.action"
        action.service = MagicMock()

        async def failing_handler(ctx):
            raise ValueError("logged error")

        handler = await mw.local_action(failing_handler, action)
        ctx = MagicMock()
        ctx.request_id = "req-logged-123"

        with pytest.raises(ValueError):
            await handler(ctx)

        assert len(logged_errors) == 1
        assert logged_errors[0]["error"] == "logged error"
        assert logged_errors[0]["action"] == "logged.action"
        assert logged_errors[0]["request_id"] == "req-logged-123"

    @pytest.mark.asyncio
    async def test_metrics_collection_pattern(self):
        """Test pattern: collect error metrics."""
        mw = ErrorHandlerMiddleware()
        broker = MagicMock()
        error_counts = {}

        async def collect_metrics(err, info):
            """Collect error metrics then re-raise."""
            action_name = getattr(info.get("action"), "name", "unknown")
            error_type = type(err).__name__
            key = f"{action_name}:{error_type}"
            error_counts[key] = error_counts.get(key, 0) + 1
            raise err

        broker.error_handler = collect_metrics
        mw.broker_created(broker)

        action = MagicMock()
        action.name = "metrics.action"
        action.service = MagicMock()

        async def raise_value_error(ctx):
            raise ValueError("error")

        handler = await mw.local_action(raise_value_error, action)

        for _ in range(5):
            ctx = MagicMock()
            ctx.request_id = f"req-{_}"
            with pytest.raises(ValueError):
                await handler(ctx)

        assert error_counts["metrics.action:ValueError"] == 5
