"""ActionHook Middleware for the MoleculerPy framework.

This module implements action hooks (before/after/error) for intercepting
action execution at key lifecycle points.

Features:
    - before hooks: Execute before action handler
    - after hooks: Execute after successful action (can modify result)
    - error hooks: Execute on action error (must re-raise)
    - Service-level hooks with pattern matching
    - Action-level hooks with highest precedence
    - String hook references resolved to service methods

Example usage:
    from moleculerpy import Service

    class UserService(Service):
        name = "users"

        # Service-level hooks
        hooks = {
            "before": {
                "*": "authorize",  # Runs before all actions
                "create|update": "validate",  # Runs before create and update
            },
            "after": {
                "*": "log_result",
            },
            "error": {
                "*": "handle_error",
            },
        }

        async def authorize(self, ctx):
            if not ctx.meta.get("user"):
                raise UnauthorizedError()

        async def validate(self, ctx):
            if not ctx.params.get("email"):
                raise ValidationError("Email required")

        async def log_result(self, ctx, res):
            self.logger.info(f"Result: {res}")
            return res  # Can modify result

        async def handle_error(self, ctx, err):
            self.logger.error(f"Error: {err}")
            raise err  # Must re-raise

        # Action-level hooks
        actions = {
            "create": {
                "hooks": {
                    "before": lambda ctx: ctx.params.update(created_at=time.time()),
                    "after": lambda ctx, res: {**res, "success": True},
                },
                "handler": ...,
            },
        }

Moleculer.js compatible:
    - Service hooks in schema.hooks
    - Action hooks in action.hooks
    - Pattern matching with wildcards
    - String hook names resolved to service methods
"""

from __future__ import annotations

import asyncio
import fnmatch
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, cast

from moleculerpy.middleware.base import Middleware

if TYPE_CHECKING:
    from moleculerpy.context import Context
    from moleculerpy.protocols import ActionProtocol
    from moleculerpy.service import Service

__all__ = ["ActionHookMiddleware"]

# Type aliases
HookFunc = Callable[..., Awaitable[Any] | Any]
HandlerType = Callable[["Context"], Awaitable[Any]]


def _match_pattern(pattern: str, action_name: str) -> bool:
    """Check if action name matches a hook pattern.

    Supports:
        - "*" matches all
        - "create|update" matches "create" OR "update"
        - "user.*" matches "user.get", "user.create", etc.
        - Standard fnmatch patterns

    Args:
        pattern: Hook pattern to match
        action_name: Action name to test

    Returns:
        True if pattern matches action name
    """
    if pattern == "*":
        return True

    # Handle pipe-separated patterns (create|update)
    if "|" in pattern:
        parts = pattern.split("|")
        return any(_match_pattern(p.strip(), action_name) for p in parts)

    # Use fnmatch for wildcards
    return fnmatch.fnmatch(action_name, pattern)


class ActionHookMiddleware(Middleware):
    """Middleware for action lifecycle hooks.

    Provides before/after/error hooks for intercepting action execution.
    Hooks can be defined at service level (in schema.hooks) or action
    level (in action.hooks).

    Execution order:
        1. before["*"] (global)
        2. before[matched patterns]
        3. action.hooks.before
        4. ACTION HANDLER
        5. action.hooks.after
        6. after[matched patterns]
        7. after["*"] (global)

    Error handling:
        1. action.hooks.error
        2. error[matched patterns]
        3. error["*"] (global)

    Attributes:
        logger: Logger for hook events
    """

    __slots__ = ("logger",)

    def __init__(self, logger: logging.Logger | None = None) -> None:
        """Initialize the ActionHookMiddleware.

        Args:
            logger: Optional logger for hook events
        """
        super().__init__()
        self.logger = logger or logging.getLogger("moleculerpy.middleware.action_hook")

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return "ActionHookMiddleware()"

    def _resolve_hook(
        self,
        hook: str | HookFunc | None,
        service: Service | None,
    ) -> HookFunc | None:
        """Resolve a hook to a callable.

        String hooks are resolved to service methods.

        Args:
            hook: Hook function or method name
            service: Service instance for method resolution

        Returns:
            Resolved hook callable or None
        """
        if hook is None:
            return None

        if callable(hook):
            return hook

        # String reference to service method
        if isinstance(hook, str) and service is not None:
            method = getattr(service, hook, None)
            if callable(method):
                return cast(HookFunc, method)
            self.logger.warning(f"Hook method '{hook}' not found on service '{service.name}'")
            return None

        return None

    def _get_matching_hooks(
        self,
        hooks_config: dict[str, Any] | None,
        action_name: str,
        service: Service | None,
        global_first: bool = True,
    ) -> list[HookFunc]:
        """Get all hooks matching an action name.

        Args:
            hooks_config: Hook configuration dict (e.g., hooks["before"])
            action_name: Action name to match
            service: Service for method resolution
            global_first: If True, global "*" hooks come first; if False, they come last

        Returns:
            List of resolved hook callables
        """
        if not hooks_config:
            return []

        result: list[HookFunc] = []
        global_hooks: list[HookFunc] = []

        # Collect global "*" hooks
        global_hook = hooks_config.get("*")
        if global_hook:
            hooks_list = global_hook if isinstance(global_hook, list) else [global_hook]
            for h in hooks_list:
                resolved = self._resolve_hook(h, service)
                if resolved:
                    global_hooks.append(resolved)

        # Collect pattern-matched hooks (excluding "*")
        pattern_hooks: list[HookFunc] = []
        for pattern, hook_value in hooks_config.items():
            if pattern == "*":
                continue

            if _match_pattern(pattern, action_name):
                hooks_list = hook_value if isinstance(hook_value, list) else [hook_value]
                for h in hooks_list:
                    resolved = self._resolve_hook(h, service)
                    if resolved:
                        pattern_hooks.append(resolved)

        # Order depends on hook type
        if global_first:
            # For before hooks: global first, then patterns
            result.extend(global_hooks)
            result.extend(pattern_hooks)
        else:
            # For after hooks: patterns first, then global
            result.extend(pattern_hooks)
            result.extend(global_hooks)

        return result

    async def _call_hook(
        self,
        hook: HookFunc,
        ctx: Context,
        *args: Any,
    ) -> Any:
        """Call a hook function (sync or async).

        Args:
            hook: Hook function to call
            ctx: Request context
            *args: Additional arguments (result for after, error for error)

        Returns:
            Hook result (for after hooks)
        """
        try:
            result = hook(ctx, *args)
            if asyncio.iscoroutine(result):
                return await result
            return result
        except Exception:
            raise

    async def _call_before_hooks(
        self,
        hooks: list[HookFunc],
        ctx: Context,
    ) -> None:
        """Call all before hooks sequentially.

        Args:
            hooks: List of before hooks
            ctx: Request context

        Raises:
            Exception: If any hook raises
        """
        for hook in hooks:
            await self._call_hook(hook, ctx)

    async def _call_after_hooks(
        self,
        hooks: list[HookFunc],
        ctx: Context,
        result: Any,
    ) -> Any:
        """Call all after hooks sequentially.

        Each hook can modify the result by returning a new value.

        Args:
            hooks: List of after hooks
            ctx: Request context
            result: Action result

        Returns:
            Possibly modified result
        """
        current_result = result
        for hook in hooks:
            hook_result = await self._call_hook(hook, ctx, current_result)
            # If hook returns a value, use it as new result
            if hook_result is not None:
                current_result = hook_result
        return current_result

    async def _call_error_hooks(
        self,
        hooks: list[HookFunc],
        ctx: Context,
        error: Exception,
    ) -> None:
        """Call all error hooks sequentially.

        Error hooks must re-raise the error (or a transformed error).

        Args:
            hooks: List of error hooks
            ctx: Request context
            error: Original error

        Raises:
            Exception: Re-raised by hook or original error
        """
        for hook in hooks:
            try:
                await self._call_hook(hook, ctx, error)
                # If hook didn't raise, re-raise original
                raise error
            except Exception:
                raise

        # No error hooks or none raised - re-raise original
        raise error

    async def local_action(
        self,
        next_handler: HandlerType,
        action: ActionProtocol,
    ) -> HandlerType:
        """Wrap local action with hooks.

        Args:
            next_handler: The next handler in the chain
            action: The action being registered

        Returns:
            Wrapped handler with hooks
        """
        # Get action and service info
        action_name = getattr(action, "name", "")
        service = getattr(action, "service", None)

        # Get service-level hooks config
        service_hooks: dict[str, Any] = {}
        if service is not None:
            schema = getattr(service, "schema", None)
            if schema is not None:
                service_hooks = getattr(schema, "hooks", None) or {}
            # Also check service.hooks directly
            if not service_hooks:
                service_hooks = getattr(service, "hooks", None) or {}

        # Get action-level hooks
        action_hooks = getattr(action, "hooks", None) or {}

        # Extract action name without service prefix for pattern matching
        # e.g., "users.create" -> "create"
        short_name = action_name.split(".")[-1] if "." in action_name else action_name

        # Collect before hooks
        before_hooks: list[HookFunc] = []
        # Service-level before hooks
        before_hooks.extend(
            self._get_matching_hooks(service_hooks.get("before"), short_name, service)
        )
        # Action-level before hook (highest precedence, runs last before handler)
        action_before = self._resolve_hook(action_hooks.get("before"), service)
        if action_before:
            before_hooks.append(action_before)

        # Collect after hooks (reverse order - action first, global last)
        after_hooks: list[HookFunc] = []
        # Action-level after hook
        action_after = self._resolve_hook(action_hooks.get("after"), service)
        if action_after:
            after_hooks.append(action_after)
        # Service-level after hooks (global_first=False: patterns first, then global "*")
        after_hooks.extend(
            self._get_matching_hooks(
                service_hooks.get("after"), short_name, service, global_first=False
            )
        )

        # Collect error hooks (action first, then patterns, then global)
        error_hooks: list[HookFunc] = []
        # Action-level error hook
        action_error = self._resolve_hook(action_hooks.get("error"), service)
        if action_error:
            error_hooks.append(action_error)
        # Service-level error hooks (global_first=False: patterns first, then global "*")
        error_hooks.extend(
            self._get_matching_hooks(
                service_hooks.get("error"), short_name, service, global_first=False
            )
        )

        # If no hooks configured, return original handler
        if not before_hooks and not after_hooks and not error_hooks:
            return next_handler

        async def hooked_handler(ctx: Context) -> Any:
            try:
                # Run before hooks
                if before_hooks:
                    await self._call_before_hooks(before_hooks, ctx)

                # Run action handler
                result = await next_handler(ctx)

                # Run after hooks
                if after_hooks:
                    result = await self._call_after_hooks(after_hooks, ctx, result)

                return result

            except Exception as err:
                # Run error hooks
                if error_hooks:
                    await self._call_error_hooks(error_hooks, ctx, err)
                raise

        return hooked_handler
