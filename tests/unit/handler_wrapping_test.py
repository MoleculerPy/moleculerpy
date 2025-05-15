"""Tests for handler wrapping at registration time (Moleculer pattern).

This module tests the MiddlewareHandler class and the integration with
broker registration to ensure middleware is applied at registration time,
not at call time.

Key behaviors tested:
1. Middleware wraps handlers at registration time via MiddlewareHandler
2. wrapped_handler is set on Action/Event objects after registration
3. broker.call() uses pre-wrapped handlers
4. Middleware chain executes in correct order
5. Direct service method calls also go through middleware (via wrapped_handler)
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from moleculerpy.broker import Broker
from moleculerpy.decorators import action, event
from moleculerpy.middleware import Middleware, MiddlewareHandler
from moleculerpy.registry import Action, Event
from moleculerpy.service import Service


# Automatically mock transit operations
@pytest.fixture(autouse=True)
def mock_transit_operations():
    """Mock all transit operations to avoid network connections."""
    with (
        patch("moleculerpy.transit.Transit.connect", new_callable=AsyncMock),
        patch("moleculerpy.transit.Transit.disconnect", new_callable=AsyncMock),
        patch("moleculerpy.transit.Transit.request", new_callable=AsyncMock),
        patch("moleculerpy.transit.Transit.publish", new_callable=AsyncMock),
    ):
        yield


class TrackingMiddleware(Middleware):
    """Middleware that tracks all hook calls and modifications."""

    def __init__(self, name: str = "Tracking"):
        self.name = name
        self.wrap_calls: list[dict] = []
        self.execution_log: list[str] = []

    def reset(self) -> None:
        self.wrap_calls.clear()
        self.execution_log.clear()

    async def local_action(self, next_handler, action: Action):
        """Track local action wrapping."""
        action_name = getattr(action, "name", str(action))
        self.wrap_calls.append(
            {
                "hook": "local_action",
                "action_name": action_name,
                "middleware": self.name,
            }
        )

        async def wrapped(ctx):
            self.execution_log.append(f"{self.name}:before:{action.name}")
            ctx.params[f"touched_by_{self.name}"] = True
            result = await next_handler(ctx)
            self.execution_log.append(f"{self.name}:after:{action.name}")
            if isinstance(result, dict):
                result[f"processed_by_{self.name}"] = True
            return result

        return wrapped

    async def remote_action(self, next_handler, action: Action):
        """Track remote action wrapping."""
        self.wrap_calls.append(
            {
                "hook": "remote_action",
                "action_name": action.name,
                "middleware": self.name,
            }
        )

        async def wrapped(ctx):
            self.execution_log.append(f"{self.name}:remote_before:{action.name}")
            result = await next_handler(ctx)
            self.execution_log.append(f"{self.name}:remote_after:{action.name}")
            return result

        return wrapped

    async def local_event(self, next_handler, event: Event):
        """Track local event wrapping."""
        self.wrap_calls.append(
            {
                "hook": "local_event",
                "event_name": event.name,
                "middleware": self.name,
            }
        )

        async def wrapped(ctx):
            self.execution_log.append(f"{self.name}:event_before:{event.name}")
            result = await next_handler(ctx)
            self.execution_log.append(f"{self.name}:event_after:{event.name}")
            return result

        return wrapped


class TestService(Service):
    """Simple test service with actions and events."""

    __test__ = False

    def __init__(self):
        super().__init__("test")
        self.call_count = 0
        self.last_params = None

    @action()
    async def echo(self, ctx):
        self.call_count += 1
        self.last_params = dict(ctx.params)
        return {"echo": ctx.params.get("message", ""), "count": self.call_count}

    @action(timeout=5.0)
    async def slow(self, ctx):
        await asyncio.sleep(ctx.params.get("delay", 0.1))
        return {"delayed": True}

    @event()
    async def user_created(self, ctx):
        self.last_params = dict(ctx.params)


class TestMiddlewareHandler:
    """Tests for MiddlewareHandler class."""

    @pytest.mark.asyncio
    async def test_wrap_handler_no_middlewares(self):
        """Handler returned unchanged when no middlewares registered."""
        broker = MagicMock()
        broker.middlewares = []
        handler_manager = MiddlewareHandler(broker)

        original = AsyncMock(return_value={"result": "ok"})
        action_meta = MagicMock(name="test.action")

        wrapped = await handler_manager.wrap_handler("local_action", original, action_meta)

        # Should return same handler
        assert wrapped is original

    @pytest.mark.asyncio
    async def test_wrap_handler_single_middleware(self):
        """Handler is wrapped by single middleware."""
        mw = TrackingMiddleware("MW1")
        broker = MagicMock()
        broker.middlewares = [mw]
        handler_manager = MiddlewareHandler(broker)

        async def original(ctx):
            return {"original": True}

        action_meta = MagicMock()
        action_meta.name = "test.action"

        wrapped = await handler_manager.wrap_handler("local_action", original, action_meta)

        # Should be different from original
        assert wrapped is not original

        # Middleware should have recorded the wrap call
        assert len(mw.wrap_calls) == 1
        assert mw.wrap_calls[0]["hook"] == "local_action"
        assert mw.wrap_calls[0]["action_name"] == "test.action"

    @pytest.mark.asyncio
    async def test_wrap_handler_multiple_middlewares_order(self):
        """Multiple middlewares wrap in correct order (first is outermost)."""
        mw1 = TrackingMiddleware("MW1")
        mw2 = TrackingMiddleware("MW2")

        broker = MagicMock()
        broker.middlewares = [mw1, mw2]
        handler_manager = MiddlewareHandler(broker)

        async def original(ctx):
            return {"original": True}

        action_meta = MagicMock()
        action_meta.name = "test.action"

        wrapped = await handler_manager.wrap_handler("local_action", original, action_meta)

        # Execute wrapped handler
        ctx = MagicMock()
        ctx.params = {}
        await wrapped(ctx)

        # Execution order: MW1 before -> MW2 before -> original -> MW2 after -> MW1 after
        assert mw1.execution_log == ["MW1:before:test.action", "MW1:after:test.action"]
        assert mw2.execution_log == ["MW2:before:test.action", "MW2:after:test.action"]

        # Both middlewares touched params
        assert ctx.params.get("touched_by_MW1") is True
        assert ctx.params.get("touched_by_MW2") is True

    @pytest.mark.asyncio
    async def test_wrap_handler_only_applicable_hooks(self):
        """Only middlewares with matching hooks are applied."""

        class PartialMiddleware(Middleware):
            """Middleware that only implements local_action, not local_event."""

            def __init__(self):
                self.local_action_called = False

            async def local_action(self, next_handler, action):
                self.local_action_called = True

                async def wrapped(ctx):
                    return await next_handler(ctx)

                return wrapped

            # Override local_event to NOT wrap (return original)
            async def local_event(self, next_handler, event):
                # Return original unchanged - no wrapping
                return next_handler

        mw = PartialMiddleware()
        broker = MagicMock()
        broker.middlewares = [mw]
        handler_manager = MiddlewareHandler(broker)

        original = AsyncMock()
        event_meta = MagicMock()
        event_meta.name = "test.event"

        # Wrap for local_event - PartialMiddleware returns original unchanged
        wrapped = await handler_manager.wrap_handler("local_event", original, event_meta)

        # Should return original since middleware doesn't wrap local_event
        assert wrapped is original
        assert not mw.local_action_called


class TestBrokerHandlerWrapping:
    """Tests for handler wrapping during broker registration."""

    @pytest.mark.asyncio
    async def test_action_wrapped_at_registration(self):
        """Actions get wrapped_handler set during registration."""
        mw = TrackingMiddleware("RegMW")
        broker = Broker(id="test-broker", middlewares=[mw])
        service = TestService()

        await broker.register(service)

        # Find the registered action
        action_obj = broker.registry.get_action("test.echo")

        # wrapped_handler should be set
        assert action_obj is not None
        assert action_obj.wrapped_handler is not None
        assert action_obj.wrapped_handler is not action_obj.handler

        # Middleware wrap call should have been recorded
        # Debug: print wrap_calls if test fails
        for i, call in enumerate(mw.wrap_calls):
            print(f"wrap_calls[{i}]: {call}")

        wrap_calls = [c for c in mw.wrap_calls if c.get("action_name") == "test.echo"]
        assert len(wrap_calls) == 1, (
            f"Expected 1 call for test.echo, got {len(wrap_calls)}. All calls: {mw.wrap_calls}"
        )

    @pytest.mark.asyncio
    async def test_call_uses_wrapped_handler(self):
        """broker.call() uses pre-wrapped handler."""
        mw = TrackingMiddleware("CallMW")
        broker = Broker(id="test-broker", middlewares=[mw])
        service = TestService()

        await broker.register(service)
        mw.reset()  # Clear wrap calls from registration

        await broker.start()
        mw.execution_log.clear()  # Clear lifecycle hook logs

        result = await broker.call("test.echo", params={"message": "hello"})

        # Handler was executed through middleware
        assert "CallMW:before:test.echo" in mw.execution_log
        assert "CallMW:after:test.echo" in mw.execution_log

        # Result was processed
        assert result["echo"] == "hello"
        assert result.get("processed_by_CallMW") is True

        # Params were touched
        assert service.last_params.get("touched_by_CallMW") is True

        await broker.stop()

    @pytest.mark.asyncio
    async def test_multiple_calls_same_wrapped_handler(self):
        """Multiple calls use the same pre-wrapped handler (no re-wrapping)."""
        mw = TrackingMiddleware("MultiMW")
        broker = Broker(id="test-broker", middlewares=[mw])
        service = TestService()

        await broker.register(service)

        # Record wrap calls count after registration
        initial_wrap_count = len(mw.wrap_calls)

        await broker.start()

        # Make multiple calls
        await broker.call("test.echo", params={"message": "1"})
        await broker.call("test.echo", params={"message": "2"})
        await broker.call("test.echo", params={"message": "3"})

        # Wrap count should not have increased
        assert len(mw.wrap_calls) == initial_wrap_count

        # But service was called 3 times
        assert service.call_count == 3

        await broker.stop()

    @pytest.mark.asyncio
    async def test_event_wrapped_at_registration(self):
        """Events get wrapped_handler set during registration."""
        mw = TrackingMiddleware("EventMW")
        broker = Broker(id="test-broker", middlewares=[mw])
        service = TestService()

        await broker.register(service)

        # Find the registered event
        event_obj = broker.registry.get_event("user_created")

        # wrapped_handler should be set
        assert event_obj is not None
        assert event_obj.wrapped_handler is not None

    @pytest.mark.asyncio
    async def test_middleware_chain_order_in_broker(self):
        """Middleware chain executes in correct order through broker."""
        execution_order: list[str] = []

        class OrderTrackingMW(Middleware):
            def __init__(self, name: str):
                self.name = name

            async def local_action(self, next_handler, action):
                async def wrapped(ctx):
                    execution_order.append(f"{self.name}:before")
                    result = await next_handler(ctx)
                    execution_order.append(f"{self.name}:after")
                    return result

                return wrapped

        mw1 = OrderTrackingMW("First")
        mw2 = OrderTrackingMW("Second")
        mw3 = OrderTrackingMW("Third")

        broker = Broker(id="test-broker", middlewares=[mw1, mw2, mw3])

        class SimpleService(Service):
            def __init__(self):
                super().__init__("simple")

            @action()
            async def act(self, ctx):
                execution_order.append("handler")
                return {}

        service = SimpleService()
        await broker.register(service)
        await broker.start()

        await broker.call("simple.act", params={})

        # Order: First before -> Second before -> Third before -> handler -> Third after -> Second after -> First after
        expected = [
            "First:before",
            "Second:before",
            "Third:before",
            "handler",
            "Third:after",
            "Second:after",
            "First:after",
        ]
        assert execution_order == expected

        await broker.stop()


class TestWrappedHandlerFallback:
    """Tests for fallback behavior when wrapped_handler is not set."""

    @pytest.mark.asyncio
    async def test_call_with_no_middleware(self):
        """broker.call() works without middleware (uses raw handler)."""
        broker = Broker(id="test-broker", middlewares=[])
        service = TestService()

        await broker.register(service)
        await broker.start()

        result = await broker.call("test.echo", params={"message": "no middleware"})

        assert result["echo"] == "no middleware"
        assert service.call_count == 1

        await broker.stop()

    @pytest.mark.asyncio
    async def test_call_with_none_wrapped_handler(self):
        """broker.call() falls back to raw handler if wrapped_handler is None."""
        broker = Broker(id="test-broker", middlewares=[])
        service = TestService()

        await broker.register(service)

        # Manually set wrapped_handler to None
        action_obj = broker.registry.get_action("test.echo")
        action_obj.wrapped_handler = None

        await broker.start()

        # Should still work using raw handler
        result = await broker.call("test.echo", params={"message": "fallback"})

        assert result["echo"] == "fallback"

        await broker.stop()


class TestReverseStopHooks:
    """Tests for reverse order stop hooks (LIFO cleanup)."""

    @pytest.mark.asyncio
    async def test_stop_hooks_reverse_order(self):
        """Stop hooks execute in reverse order for proper cleanup."""
        stop_order: list[str] = []

        class OrderTrackingMW(Middleware):
            def __init__(self, name: str):
                self._name = name

            async def broker_stopping(self, broker):
                stop_order.append(f"{self._name}:stopping")

            async def broker_stopped(self, broker):
                stop_order.append(f"{self._name}:stopped")

        mw1 = OrderTrackingMW("First")
        mw2 = OrderTrackingMW("Second")
        mw3 = OrderTrackingMW("Third")

        broker = Broker(id="test-broker", middlewares=[mw1, mw2, mw3])
        await broker.start()

        stop_order.clear()
        await broker.stop()

        # Stop hooks should execute in reverse order (LIFO)
        # Third -> Second -> First (reverse of registration)
        assert stop_order == [
            "Third:stopping",
            "Second:stopping",
            "First:stopping",
            "Third:stopped",
            "Second:stopped",
            "First:stopped",
        ]

    @pytest.mark.asyncio
    async def test_services_stop_in_reverse_order(self):
        """Services stop in reverse registration order."""
        stop_order: list[str] = []

        class StopTrackingService(Service):
            def __init__(self, name: str):
                super().__init__(name)

            async def stopped(self):
                stop_order.append(f"{self.name}:stopped")

        broker = Broker(id="test-broker")

        svc1 = StopTrackingService("first")
        svc2 = StopTrackingService("second")
        svc3 = StopTrackingService("third")

        await broker.register(svc1)
        await broker.register(svc2)
        await broker.register(svc3)

        await broker.start()
        await broker.stop()

        # Services should stop in reverse order
        assert stop_order == ["third:stopped", "second:stopped", "first:stopped"]


class TestMiddlewareError:
    """Tests for MiddlewareError exception class."""

    def test_middleware_error_basic(self):
        """Test MiddlewareError with message only."""
        from moleculerpy.middleware.handler import MiddlewareError

        err = MiddlewareError("Test error")
        assert str(err) == "Test error"
        assert err.middleware_name is None
        assert err.hook_name is None

    def test_middleware_error_with_context(self):
        """Test MiddlewareError with middleware and hook names."""
        from moleculerpy.middleware.handler import MiddlewareError

        err = MiddlewareError(
            "Failed to wrap handler", middleware_name="RetryMiddleware", hook_name="local_action"
        )
        assert str(err) == "Failed to wrap handler"
        assert err.middleware_name == "RetryMiddleware"
        assert err.hook_name == "local_action"


class TestMiddlewareHandlerCoverage:
    """Additional tests for MiddlewareHandler coverage gaps."""

    @pytest.mark.asyncio
    async def test_clear_cache(self):
        """Test that clear_cache() clears the hook cache."""
        mw = TrackingMiddleware("CacheMW")
        broker = MagicMock()
        broker.middlewares = [mw]
        handler_manager = MiddlewareHandler(broker)

        # Populate cache by getting hooks
        hooks1 = handler_manager._get_hooks("local_action")
        assert len(hooks1) == 1

        # Cache should be populated
        assert "local_action" in handler_manager._hook_cache

        # Clear cache
        handler_manager.clear_cache()

        # Cache should be empty
        assert len(handler_manager._hook_cache) == 0

    @pytest.mark.asyncio
    async def test_wrap_handler_exception_handling(self):
        """Test exception handling during middleware wrapping."""
        from moleculerpy.middleware.handler import MiddlewareError

        class FailingMiddleware(Middleware):
            """Middleware that fails during wrapping."""

            async def local_action(self, next_handler, action):
                raise ValueError("Middleware initialization failed")

        mw = FailingMiddleware()
        broker = MagicMock()
        broker.middlewares = [mw]
        handler_manager = MiddlewareHandler(broker)

        async def original(ctx):
            return {"result": "ok"}

        action_meta = MagicMock()
        action_meta.name = "test.action"

        with pytest.raises(MiddlewareError) as exc_info:
            await handler_manager.wrap_handler("local_action", original, action_meta)

        assert exc_info.value.middleware_name == "FailingMiddleware"
        assert exc_info.value.hook_name == "local_action"
        assert "Middleware initialization failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_wrap_handler_cancelled_error(self):
        """Test that CancelledError is propagated during wrapping."""

        class CancellingMiddleware(Middleware):
            """Middleware that simulates cancellation."""

            async def local_action(self, next_handler, action):
                raise asyncio.CancelledError()

        mw = CancellingMiddleware()
        broker = MagicMock()
        broker.middlewares = [mw]
        handler_manager = MiddlewareHandler(broker)

        async def original(ctx):
            return {"result": "ok"}

        action_meta = MagicMock()
        action_meta.name = "test.action"

        with pytest.raises(asyncio.CancelledError):
            await handler_manager.wrap_handler("local_action", original, action_meta)

    @pytest.mark.asyncio
    async def test_wrap_handler_sync_returns_handler(self):
        """Test wrap_handler returns handler when hook returns sync value."""

        class SyncMiddleware(Middleware):
            """Middleware with sync hook (returns handler directly, not awaitable)."""

            def local_action(self, next_handler, action):
                # Return handler directly (sync) - not a coroutine
                async def wrapped(ctx):
                    ctx.params["sync_wrapped"] = True
                    return await next_handler(ctx)

                return wrapped

        mw = SyncMiddleware()
        broker = MagicMock()
        broker.middlewares = [mw]
        handler_manager = MiddlewareHandler(broker)

        async def original(ctx):
            return {"original": True}

        action_meta = MagicMock()
        action_meta.name = "test.action"

        wrapped = await handler_manager.wrap_handler("local_action", original, action_meta)

        # Execute and verify wrapping
        ctx = MagicMock()
        ctx.params = {}
        result = await wrapped(ctx)

        assert ctx.params.get("sync_wrapped") is True
        assert result["original"] is True

    def test_wrap_handler_sync_deprecated(self):
        """Test that wrap_handler_sync raises DeprecationWarning."""
        import warnings

        mw = TrackingMiddleware("DeprecatedMW")
        broker = MagicMock()
        broker.middlewares = [mw]
        handler_manager = MiddlewareHandler(broker)

        async def original(ctx):
            return {"result": "ok"}

        action_meta = MagicMock()
        action_meta.name = "test.action"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _result, pending = handler_manager.wrap_handler_sync(
                "local_action", original, action_meta
            )
            for _middleware, coro in pending:
                coro.close()

            # Should emit DeprecationWarning
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "wrap_handler_sync is deprecated" in str(w[0].message)

    def test_wrap_handler_sync_no_middlewares(self):
        """Test wrap_handler_sync with no middlewares."""
        import warnings

        broker = MagicMock()
        broker.middlewares = []
        handler_manager = MiddlewareHandler(broker)

        async def original(ctx):
            return {"result": "ok"}

        action_meta = MagicMock()
        action_meta.name = "test.action"

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result, pending = handler_manager.wrap_handler_sync(
                "local_action", original, action_meta
            )

            # Should return original handler and empty list
            assert result is original
            assert pending == []

    def test_wrap_handler_sync_with_async_middleware(self):
        """Test wrap_handler_sync collects pending coroutines from async middleware."""
        import warnings

        class AsyncOnlyMiddleware(Middleware):
            """Middleware with async hook."""

            async def local_action(self, next_handler, action):
                async def wrapped(ctx):
                    return await next_handler(ctx)

                return wrapped

        mw = AsyncOnlyMiddleware()
        broker = MagicMock()
        broker.middlewares = [mw]
        handler_manager = MiddlewareHandler(broker)

        async def original(ctx):
            return {"result": "ok"}

        action_meta = MagicMock()
        action_meta.name = "test.action"

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            _result, pending = handler_manager.wrap_handler_sync(
                "local_action", original, action_meta
            )

            # Should have pending coroutines from async middleware
            assert len(pending) == 1
            # Close coroutine to avoid warning
            pending[0][1].close()
