"""Unit tests for the ActionHook Middleware module.

Tests for ActionHookMiddleware that provides before/after/error hooks
for intercepting action execution.
"""

import asyncio
from unittest.mock import MagicMock

import pytest

from moleculerpy.middleware.action_hook import ActionHookMiddleware, _match_pattern


class TestPatternMatching:
    """Tests for hook pattern matching."""

    def test_wildcard_matches_all(self):
        """Test '*' matches any action."""
        assert _match_pattern("*", "create") is True
        assert _match_pattern("*", "users.get") is True
        assert _match_pattern("*", "anything") is True

    def test_pipe_or_pattern(self):
        """Test pipe-separated patterns (create|update)."""
        assert _match_pattern("create|update", "create") is True
        assert _match_pattern("create|update", "update") is True
        assert _match_pattern("create|update", "delete") is False

    def test_fnmatch_wildcard(self):
        """Test fnmatch-style wildcards."""
        assert _match_pattern("user.*", "user.get") is True
        assert _match_pattern("user.*", "user.create") is True
        assert _match_pattern("user.*", "post.get") is False

    def test_exact_match(self):
        """Test exact name matching."""
        assert _match_pattern("create", "create") is True
        assert _match_pattern("create", "update") is False

    def test_prefix_wildcard(self):
        """Test prefix wildcards."""
        assert _match_pattern("*-account", "create-account") is True
        assert _match_pattern("*-account", "delete-account") is True
        assert _match_pattern("*-account", "create-user") is False


class TestActionHookMiddlewareInit:
    """Tests for ActionHookMiddleware initialization."""

    def test_init_default_logger(self):
        """Test middleware initializes with default logger."""
        mw = ActionHookMiddleware()

        assert mw.logger is not None
        assert mw.logger.name == "moleculerpy.middleware.action_hook"

    def test_init_custom_logger(self):
        """Test middleware with custom logger."""
        custom_logger = MagicMock()
        mw = ActionHookMiddleware(logger=custom_logger)

        assert mw.logger is custom_logger

    def test_repr(self):
        """Test __repr__ returns readable string."""
        mw = ActionHookMiddleware()

        assert repr(mw) == "ActionHookMiddleware()"


class TestHookResolution:
    """Tests for hook resolution logic."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ActionHookMiddleware()

    def test_resolve_callable(self, middleware):
        """Test resolving callable hook."""

        def my_hook(ctx):
            pass

        result = middleware._resolve_hook(my_hook, None)

        assert result is my_hook

    def test_resolve_async_callable(self, middleware):
        """Test resolving async callable hook."""

        async def my_hook(ctx):
            pass

        result = middleware._resolve_hook(my_hook, None)

        assert result is my_hook

    def test_resolve_string_method(self, middleware):
        """Test resolving string to service method."""
        service = MagicMock()
        service.name = "test"
        service.validate = MagicMock()

        result = middleware._resolve_hook("validate", service)

        assert result is service.validate

    def test_resolve_string_not_found(self, middleware):
        """Test resolving non-existent method returns None."""
        service = MagicMock(spec=["name"])
        service.name = "test"

        result = middleware._resolve_hook("nonexistent", service)

        assert result is None

    def test_resolve_none(self, middleware):
        """Test resolving None returns None."""
        result = middleware._resolve_hook(None, None)

        assert result is None


class TestMatchingHooks:
    """Tests for collecting matching hooks."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ActionHookMiddleware()

    def test_get_matching_hooks_global(self, middleware):
        """Test getting global '*' hooks."""
        hook_fn = MagicMock()
        hooks_config = {"*": hook_fn}

        result = middleware._get_matching_hooks(hooks_config, "create", None)

        assert hook_fn in result

    def test_get_matching_hooks_pattern(self, middleware):
        """Test getting pattern-matched hooks."""
        create_hook = MagicMock()
        hooks_config = {
            "create|update": create_hook,
        }

        result = middleware._get_matching_hooks(hooks_config, "create", None)

        assert create_hook in result

    def test_get_matching_hooks_multiple(self, middleware):
        """Test getting multiple matching hooks."""
        global_hook = MagicMock()
        create_hook = MagicMock()
        hooks_config = {
            "*": global_hook,
            "create": create_hook,
        }

        result = middleware._get_matching_hooks(hooks_config, "create", None)

        assert global_hook in result
        assert create_hook in result
        assert len(result) == 2

    def test_get_matching_hooks_list(self, middleware):
        """Test getting hooks from list."""
        hook1 = MagicMock()
        hook2 = MagicMock()
        hooks_config = {"*": [hook1, hook2]}

        result = middleware._get_matching_hooks(hooks_config, "create", None)

        assert hook1 in result
        assert hook2 in result

    def test_get_matching_hooks_none(self, middleware):
        """Test empty config returns empty list."""
        result = middleware._get_matching_hooks(None, "create", None)

        assert result == []


class TestBeforeHooks:
    """Tests for before hooks execution."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ActionHookMiddleware()

    @pytest.mark.asyncio
    async def test_before_hook_called(self, middleware):
        """Test before hook is called before handler."""
        execution_order = []

        async def before_hook(ctx):
            execution_order.append("before")

        async def handler(ctx):
            execution_order.append("handler")
            return "result"

        action = MagicMock()
        action.name = "test.action"
        action.hooks = {"before": before_hook}
        action.service = None

        wrapped = await middleware.local_action(handler, action)
        ctx = MagicMock()
        result = await wrapped(ctx)

        assert execution_order == ["before", "handler"]
        assert result == "result"

    @pytest.mark.asyncio
    async def test_before_hook_can_raise(self, middleware):
        """Test before hook can prevent handler execution."""
        handler_called = False

        async def before_hook(ctx):
            raise ValueError("Validation failed")

        async def handler(ctx):
            nonlocal handler_called
            handler_called = True
            return "result"

        action = MagicMock()
        action.name = "test.action"
        action.hooks = {"before": before_hook}
        action.service = None

        wrapped = await middleware.local_action(handler, action)
        ctx = MagicMock()

        with pytest.raises(ValueError, match="Validation failed"):
            await wrapped(ctx)

        assert handler_called is False

    @pytest.mark.asyncio
    async def test_multiple_before_hooks_sequential(self, middleware):
        """Test multiple before hooks execute sequentially."""
        execution_order = []

        async def hook1(ctx):
            await asyncio.sleep(0.01)
            execution_order.append("hook1")

        async def hook2(ctx):
            execution_order.append("hook2")

        async def handler(ctx):
            execution_order.append("handler")
            return "result"

        service = MagicMock()
        service.name = "test"
        service.hooks = {"before": {"*": [hook1, hook2]}}
        service.schema = None

        action = MagicMock()
        action.name = "test.action"
        action.hooks = {}
        action.service = service

        wrapped = await middleware.local_action(handler, action)
        ctx = MagicMock()
        await wrapped(ctx)

        assert execution_order == ["hook1", "hook2", "handler"]


class TestAfterHooks:
    """Tests for after hooks execution."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ActionHookMiddleware()

    @pytest.mark.asyncio
    async def test_after_hook_called(self, middleware):
        """Test after hook is called after handler."""
        execution_order = []

        async def after_hook(ctx, res):
            execution_order.append("after")
            return res

        async def handler(ctx):
            execution_order.append("handler")
            return "result"

        action = MagicMock()
        action.name = "test.action"
        action.hooks = {"after": after_hook}
        action.service = None

        wrapped = await middleware.local_action(handler, action)
        ctx = MagicMock()
        result = await wrapped(ctx)

        assert execution_order == ["handler", "after"]
        assert result == "result"

    @pytest.mark.asyncio
    async def test_after_hook_can_modify_result(self, middleware):
        """Test after hook can modify the result."""

        async def after_hook(ctx, res):
            return {"modified": True, "original": res}

        async def handler(ctx):
            return "original_result"

        action = MagicMock()
        action.name = "test.action"
        action.hooks = {"after": after_hook}
        action.service = None

        wrapped = await middleware.local_action(handler, action)
        ctx = MagicMock()
        result = await wrapped(ctx)

        assert result == {"modified": True, "original": "original_result"}

    @pytest.mark.asyncio
    async def test_after_hooks_chain_results(self, middleware):
        """Test multiple after hooks chain results."""

        async def hook1(ctx, res):
            return res + "_hook1"

        async def hook2(ctx, res):
            return res + "_hook2"

        async def handler(ctx):
            return "original"

        action = MagicMock()
        action.name = "test.action"
        action.hooks = {"after": hook1}
        action.service = None

        service = MagicMock()
        service.name = "test"
        service.hooks = {"after": {"*": hook2}}
        service.schema = None
        action.service = service

        wrapped = await middleware.local_action(handler, action)
        ctx = MagicMock()
        result = await wrapped(ctx)

        # Action hook runs first, then service hook
        assert result == "original_hook1_hook2"


class TestErrorHooks:
    """Tests for error hooks execution."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ActionHookMiddleware()

    @pytest.mark.asyncio
    async def test_error_hook_called(self, middleware):
        """Test error hook is called on handler error."""
        error_received = None

        async def error_hook(ctx, err):
            nonlocal error_received
            error_received = err
            raise err

        async def handler(ctx):
            raise ValueError("Handler error")

        action = MagicMock()
        action.name = "test.action"
        action.hooks = {"error": error_hook}
        action.service = None

        wrapped = await middleware.local_action(handler, action)
        ctx = MagicMock()

        with pytest.raises(ValueError, match="Handler error"):
            await wrapped(ctx)

        assert error_received is not None
        assert str(error_received) == "Handler error"

    @pytest.mark.asyncio
    async def test_error_hook_can_transform(self, middleware):
        """Test error hook can transform the error."""

        async def error_hook(ctx, err):
            raise RuntimeError(f"Transformed: {err}")

        async def handler(ctx):
            raise ValueError("Original error")

        action = MagicMock()
        action.name = "test.action"
        action.hooks = {"error": error_hook}
        action.service = None

        wrapped = await middleware.local_action(handler, action)
        ctx = MagicMock()

        with pytest.raises(RuntimeError, match="Transformed"):
            await wrapped(ctx)

    @pytest.mark.asyncio
    async def test_error_hook_not_called_on_success(self, middleware):
        """Test error hook is not called when handler succeeds."""
        error_called = False

        async def error_hook(ctx, err):
            nonlocal error_called
            error_called = True
            raise err

        async def handler(ctx):
            return "success"

        action = MagicMock()
        action.name = "test.action"
        action.hooks = {"error": error_hook}
        action.service = None

        wrapped = await middleware.local_action(handler, action)
        ctx = MagicMock()
        result = await wrapped(ctx)

        assert result == "success"
        assert error_called is False


class TestServiceLevelHooks:
    """Tests for service-level hooks."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ActionHookMiddleware()

    @pytest.mark.asyncio
    async def test_service_schema_hooks(self, middleware):
        """Test hooks from service.schema.hooks."""
        before_called = False

        async def before_hook(ctx):
            nonlocal before_called
            before_called = True

        schema = MagicMock()
        schema.hooks = {"before": {"*": before_hook}}

        service = MagicMock()
        service.name = "test"
        service.schema = schema
        service.hooks = None

        action = MagicMock()
        action.name = "test.action"
        action.hooks = {}
        action.service = service

        async def handler(ctx):
            return "result"

        wrapped = await middleware.local_action(handler, action)
        ctx = MagicMock()
        await wrapped(ctx)

        assert before_called is True

    @pytest.mark.asyncio
    async def test_service_direct_hooks(self, middleware):
        """Test hooks from service.hooks (no schema)."""
        before_called = False

        async def before_hook(ctx):
            nonlocal before_called
            before_called = True

        service = MagicMock()
        service.name = "test"
        service.schema = None
        service.hooks = {"before": {"*": before_hook}}

        action = MagicMock()
        action.name = "test.action"
        action.hooks = {}
        action.service = service

        async def handler(ctx):
            return "result"

        wrapped = await middleware.local_action(handler, action)
        ctx = MagicMock()
        await wrapped(ctx)

        assert before_called is True

    @pytest.mark.asyncio
    async def test_string_hook_resolution(self, middleware):
        """Test string hook names resolved to service methods."""
        method_called = False

        async def validate_method(ctx):
            nonlocal method_called
            method_called = True

        service = MagicMock()
        service.name = "test"
        service.schema = None
        service.hooks = {"before": {"*": "validate_method"}}
        service.validate_method = validate_method

        action = MagicMock()
        action.name = "test.action"
        action.hooks = {}
        action.service = service

        async def handler(ctx):
            return "result"

        wrapped = await middleware.local_action(handler, action)
        ctx = MagicMock()
        await wrapped(ctx)

        assert method_called is True


class TestHookOrder:
    """Tests for hook execution order."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ActionHookMiddleware()

    @pytest.mark.asyncio
    async def test_full_hook_order(self, middleware):
        """Test complete hook execution order."""
        execution_order = []

        async def global_before(ctx):
            execution_order.append("global_before")

        async def pattern_before(ctx):
            execution_order.append("pattern_before")

        async def action_before(ctx):
            execution_order.append("action_before")

        async def handler(ctx):
            execution_order.append("handler")
            return "result"

        async def action_after(ctx, res):
            execution_order.append("action_after")
            return res

        async def pattern_after(ctx, res):
            execution_order.append("pattern_after")
            return res

        async def global_after(ctx, res):
            execution_order.append("global_after")
            return res

        service = MagicMock()
        service.name = "test"
        service.schema = None
        service.hooks = {
            "before": {
                "*": global_before,
                "action": pattern_before,
            },
            "after": {
                "*": global_after,
                "action": pattern_after,
            },
        }

        action = MagicMock()
        action.name = "test.action"
        action.hooks = {
            "before": action_before,
            "after": action_after,
        }
        action.service = service

        wrapped = await middleware.local_action(handler, action)
        ctx = MagicMock()
        await wrapped(ctx)

        # Expected order:
        # 1. Service global before (*)
        # 2. Service pattern before (action)
        # 3. Action before
        # 4. Handler
        # 5. Action after
        # 6. Service pattern after (action)
        # 7. Service global after (*)
        assert execution_order == [
            "global_before",
            "pattern_before",
            "action_before",
            "handler",
            "action_after",
            "pattern_after",
            "global_after",
        ]


class TestNoHooksOptimization:
    """Tests for no-hooks optimization."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ActionHookMiddleware()

    @pytest.mark.asyncio
    async def test_no_hooks_returns_original(self, middleware):
        """Test handler returned as-is when no hooks configured."""

        async def handler(ctx):
            return "result"

        action = MagicMock()
        action.name = "test.action"
        action.hooks = {}
        action.service = None

        wrapped = await middleware.local_action(handler, action)

        # Should return original handler, not a wrapper
        assert wrapped is handler

    @pytest.mark.asyncio
    async def test_empty_service_hooks(self, middleware):
        """Test no wrapper when service hooks are empty."""

        async def handler(ctx):
            return "result"

        service = MagicMock()
        service.name = "test"
        service.schema = None
        service.hooks = {}

        action = MagicMock()
        action.name = "test.action"
        action.hooks = {}
        action.service = service

        wrapped = await middleware.local_action(handler, action)

        assert wrapped is handler


class TestSyncHooks:
    """Tests for synchronous hooks."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ActionHookMiddleware()

    @pytest.mark.asyncio
    async def test_sync_before_hook(self, middleware):
        """Test synchronous before hook."""
        called = False

        def sync_before(ctx):
            nonlocal called
            called = True

        async def handler(ctx):
            return "result"

        action = MagicMock()
        action.name = "test.action"
        action.hooks = {"before": sync_before}
        action.service = None

        wrapped = await middleware.local_action(handler, action)
        ctx = MagicMock()
        await wrapped(ctx)

        assert called is True

    @pytest.mark.asyncio
    async def test_sync_after_hook(self, middleware):
        """Test synchronous after hook."""

        def sync_after(ctx, res):
            return {"modified": res}

        async def handler(ctx):
            return "original"

        action = MagicMock()
        action.name = "test.action"
        action.hooks = {"after": sync_after}
        action.service = None

        wrapped = await middleware.local_action(handler, action)
        ctx = MagicMock()
        result = await wrapped(ctx)

        assert result == {"modified": "original"}
