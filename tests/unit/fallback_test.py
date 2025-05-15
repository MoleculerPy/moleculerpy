"""Unit tests for the Fallback Middleware module.

Tests for FallbackMiddleware that provides graceful degradation on failure.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from moleculerpy.middleware.fallback import FallbackMiddleware


class TestFallbackMiddleware:
    """Tests for FallbackMiddleware."""

    def test_init_default_logger(self):
        """Test middleware initializes with default logger."""
        mw = FallbackMiddleware()

        assert mw.logger is not None
        assert mw.logger.name == "moleculerpy.middleware.fallback"

    def test_init_custom_logger(self):
        """Test middleware initializes with custom logger."""
        custom_logger = MagicMock()
        mw = FallbackMiddleware(logger=custom_logger)

        assert mw.logger is custom_logger


class TestFallbackResolution:
    """Tests for fallback resolution logic."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return FallbackMiddleware()

    def test_resolve_none(self, middleware):
        """Test _resolve_fallback returns None for None input."""
        action = MagicMock()

        result = middleware._resolve_fallback(action, None)

        assert result is None

    def test_resolve_callable(self, middleware):
        """Test _resolve_fallback returns callable directly."""
        action = MagicMock()

        def my_fallback(ctx, err):
            return "fallback"

        result = middleware._resolve_fallback(action, my_fallback)

        assert result is my_fallback

    def test_resolve_static_value(self, middleware):
        """Test _resolve_fallback returns static value."""
        action = MagicMock()

        result = middleware._resolve_fallback(action, {"default": "value"})

        assert result == {"default": "value"}

    def test_resolve_method_name(self, middleware):
        """Test _resolve_fallback resolves method name from service."""
        service = MagicMock()
        service.get_default = MagicMock(return_value="default")
        action = MagicMock()
        action.service = service

        result = middleware._resolve_fallback(action, "get_default")

        assert result is service.get_default

    def test_resolve_method_name_not_found(self, middleware):
        """Test _resolve_fallback returns None for missing method."""
        service = MagicMock(spec=["name"])
        service.name = "test"
        action = MagicMock()
        action.service = service

        result = middleware._resolve_fallback(action, "nonexistent_method")

        assert result is None

    def test_resolve_method_name_no_service(self, middleware):
        """Test _resolve_fallback returns None when no service."""
        action = MagicMock(spec=[])  # No service attribute

        result = middleware._resolve_fallback(action, "get_default")

        assert result is None


class TestFallbackInvocation:
    """Tests for fallback invocation logic."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return FallbackMiddleware()

    @pytest.fixture
    def ctx(self):
        """Create mock context."""
        ctx = MagicMock()
        return ctx

    @pytest.mark.asyncio
    async def test_invoke_static_value(self, middleware, ctx):
        """Test _invoke_fallback with static value."""
        error = ValueError("test error")

        result = await middleware._invoke_fallback(ctx, error, "static_value")

        assert result == "static_value"
        assert ctx.fallback_result is True

    @pytest.mark.asyncio
    async def test_invoke_sync_callable(self, middleware, ctx):
        """Test _invoke_fallback with sync callable."""
        error = ValueError("test error")

        def fallback_fn(ctx, err):
            return f"fallback for {err}"

        result = await middleware._invoke_fallback(ctx, error, fallback_fn)

        assert result == "fallback for test error"
        assert ctx.fallback_result is True

    @pytest.mark.asyncio
    async def test_invoke_async_callable(self, middleware, ctx):
        """Test _invoke_fallback with async callable."""
        error = ValueError("test error")

        async def async_fallback(ctx, err):
            await asyncio.sleep(0.001)
            return f"async fallback for {err}"

        result = await middleware._invoke_fallback(ctx, error, async_fallback)

        assert result == "async fallback for test error"
        assert ctx.fallback_result is True

    @pytest.mark.asyncio
    async def test_invoke_dict_value(self, middleware, ctx):
        """Test _invoke_fallback with dict static value."""
        error = ValueError("test error")
        fallback_data = {"id": 0, "name": "Default"}

        result = await middleware._invoke_fallback(ctx, error, fallback_data)

        assert result == {"id": 0, "name": "Default"}
        assert ctx.fallback_result is True


class TestFallbackLocalAction:
    """Tests for local action fallback handling."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return FallbackMiddleware()

    @pytest.fixture
    def action(self):
        """Create mock action."""
        action = MagicMock()
        action.name = "test.action"
        action.fallback = None
        return action

    @pytest.fixture
    def ctx(self):
        """Create mock context."""
        ctx = MagicMock()
        ctx.options = None
        return ctx

    @pytest.mark.asyncio
    async def test_success_no_fallback_needed(self, middleware, action, ctx):
        """Test handler passes through on success."""
        next_handler = AsyncMock(return_value="success")

        handler = await middleware.local_action(next_handler, action)
        result = await handler(ctx)

        assert result == "success"
        next_handler.assert_called_once_with(ctx)

    @pytest.mark.asyncio
    async def test_error_no_fallback_configured(self, middleware, action, ctx):
        """Test error propagates when no fallback configured."""
        next_handler = AsyncMock(side_effect=ValueError("test error"))

        handler = await middleware.local_action(next_handler, action)

        with pytest.raises(ValueError, match="test error"):
            await handler(ctx)

    @pytest.mark.asyncio
    async def test_call_time_fallback_static(self, middleware, action, ctx):
        """Test call-time fallback with static value."""
        ctx.options = MagicMock()
        ctx.options.fallback_response = "call_fallback"
        next_handler = AsyncMock(side_effect=ValueError("test error"))

        handler = await middleware.local_action(next_handler, action)
        result = await handler(ctx)

        assert result == "call_fallback"
        assert ctx.fallback_result is True

    @pytest.mark.asyncio
    async def test_call_time_fallback_function(self, middleware, action, ctx):
        """Test call-time fallback with function."""
        ctx.options = MagicMock()
        ctx.options.fallback_response = lambda c, e: f"fallback: {e}"
        next_handler = AsyncMock(side_effect=ValueError("test error"))

        handler = await middleware.local_action(next_handler, action)
        result = await handler(ctx)

        assert result == "fallback: test error"

    @pytest.mark.asyncio
    async def test_action_level_fallback_function(self, middleware, action, ctx):
        """Test action-level fallback with function."""
        action.fallback = lambda c, e: f"action_fallback: {e}"
        next_handler = AsyncMock(side_effect=ValueError("test error"))

        handler = await middleware.local_action(next_handler, action)
        result = await handler(ctx)

        assert result == "action_fallback: test error"

    @pytest.mark.asyncio
    async def test_action_level_fallback_method_name(self, middleware, action, ctx):
        """Test action-level fallback with method name."""
        service = MagicMock()
        service.get_fallback = MagicMock(return_value="service_fallback")
        action.service = service
        action.fallback = "get_fallback"
        next_handler = AsyncMock(side_effect=ValueError("test error"))

        handler = await middleware.local_action(next_handler, action)
        result = await handler(ctx)

        assert result == "service_fallback"
        service.get_fallback.assert_called_once()

    @pytest.mark.asyncio
    async def test_call_time_takes_priority(self, middleware, action, ctx):
        """Test call-time fallback takes priority over action-level."""
        ctx.options = MagicMock()
        ctx.options.fallback_response = "call_fallback"
        action.fallback = lambda c, e: "action_fallback"
        next_handler = AsyncMock(side_effect=ValueError("test error"))

        handler = await middleware.local_action(next_handler, action)
        result = await handler(ctx)

        assert result == "call_fallback"  # Call-time wins

    @pytest.mark.asyncio
    async def test_async_fallback_function(self, middleware, action, ctx):
        """Test async fallback function."""

        async def async_fallback(ctx, error):
            await asyncio.sleep(0.001)
            return "async_result"

        action.fallback = async_fallback
        next_handler = AsyncMock(side_effect=ValueError("test error"))

        handler = await middleware.local_action(next_handler, action)
        result = await handler(ctx)

        assert result == "async_result"

    @pytest.mark.asyncio
    async def test_fallback_receives_context_and_error(self, middleware, action, ctx):
        """Test fallback receives correct context and error."""
        received_ctx = None
        received_error = None

        def capture_fallback(c, e):
            nonlocal received_ctx, received_error
            received_ctx = c
            received_error = e
            return "captured"

        action.fallback = capture_fallback
        test_error = ValueError("specific error")
        next_handler = AsyncMock(side_effect=test_error)

        handler = await middleware.local_action(next_handler, action)
        await handler(ctx)

        assert received_ctx is ctx
        assert received_error is test_error


class TestFallbackRemoteAction:
    """Tests for remote action fallback handling."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return FallbackMiddleware()

    @pytest.fixture
    def action(self):
        """Create mock action."""
        action = MagicMock()
        action.name = "remote.action"
        action.fallback = lambda c, e: "action_fallback"  # Should be ignored
        return action

    @pytest.fixture
    def ctx(self):
        """Create mock context."""
        ctx = MagicMock()
        ctx.options = None
        return ctx

    @pytest.mark.asyncio
    async def test_success_passes_through(self, middleware, action, ctx):
        """Test remote handler passes through on success."""
        next_handler = AsyncMock(return_value="remote_success")

        handler = await middleware.remote_action(next_handler, action)
        result = await handler(ctx)

        assert result == "remote_success"

    @pytest.mark.asyncio
    async def test_call_time_fallback_works(self, middleware, action, ctx):
        """Test call-time fallback works for remote actions."""
        ctx.options = MagicMock()
        ctx.options.fallback_response = "remote_fallback"
        next_handler = AsyncMock(side_effect=ValueError("remote error"))

        handler = await middleware.remote_action(next_handler, action)
        result = await handler(ctx)

        assert result == "remote_fallback"

    @pytest.mark.asyncio
    async def test_action_level_fallback_ignored(self, middleware, action, ctx):
        """Test action-level fallback is ignored for remote actions."""
        # action.fallback is set but should be ignored
        next_handler = AsyncMock(side_effect=ValueError("remote error"))

        handler = await middleware.remote_action(next_handler, action)

        with pytest.raises(ValueError, match="remote error"):
            await handler(ctx)

    @pytest.mark.asyncio
    async def test_no_fallback_propagates_error(self, middleware, action, ctx):
        """Test error propagates when no call-time fallback."""
        next_handler = AsyncMock(side_effect=ValueError("remote error"))

        handler = await middleware.remote_action(next_handler, action)

        with pytest.raises(ValueError, match="remote error"):
            await handler(ctx)


class TestFallbackEdgeCases:
    """Edge case tests for FallbackMiddleware."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return FallbackMiddleware()

    @pytest.mark.asyncio
    async def test_fallback_with_none_options(self, middleware):
        """Test handler works when ctx.options is None."""
        action = MagicMock()
        action.name = "test.action"
        # Set a callable fallback so it works
        action.fallback = lambda ctx, err: "fallback_result"
        ctx = MagicMock()
        ctx.options = None

        next_handler = AsyncMock(side_effect=ValueError("test"))

        handler = await middleware.local_action(next_handler, action)
        result = await handler(ctx)

        # Should use action-level fallback
        assert result == "fallback_result"
        assert ctx.fallback_result is True

    @pytest.mark.asyncio
    async def test_fallback_sets_flag_on_context(self, middleware):
        """Test fallback_result flag is set on context."""
        action = MagicMock()
        action.name = "test.action"
        action.fallback = lambda c, e: "fallback"
        ctx = MagicMock()
        ctx.options = None

        next_handler = AsyncMock(side_effect=ValueError("test"))

        handler = await middleware.local_action(next_handler, action)
        await handler(ctx)

        assert ctx.fallback_result is True

    @pytest.mark.asyncio
    async def test_fallback_preserves_exception_details(self, middleware):
        """Test fallback function receives full exception."""

        class CustomError(Exception):
            def __init__(self, code, message):
                self.code = code
                self.message = message
                super().__init__(message)

        received_error = None

        def capture_error(ctx, err):
            nonlocal received_error
            received_error = err
            return "handled"

        action = MagicMock()
        action.name = "test.action"
        action.fallback = capture_error
        ctx = MagicMock()
        ctx.options = None

        test_error = CustomError(500, "Server error")
        next_handler = AsyncMock(side_effect=test_error)

        handler = await middleware.local_action(next_handler, action)
        await handler(ctx)

        assert received_error is test_error
        assert received_error.code == 500
        assert received_error.message == "Server error"

    @pytest.mark.asyncio
    async def test_fallback_can_raise_different_error(self, middleware):
        """Test fallback can raise a different error."""

        def error_fallback(ctx, err):
            raise RuntimeError("Fallback error")

        action = MagicMock()
        action.name = "test.action"
        action.fallback = error_fallback
        ctx = MagicMock()
        ctx.options = None

        next_handler = AsyncMock(side_effect=ValueError("original"))

        handler = await middleware.local_action(next_handler, action)

        with pytest.raises(RuntimeError, match="Fallback error"):
            await handler(ctx)
