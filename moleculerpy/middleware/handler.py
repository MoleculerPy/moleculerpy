"""Middleware handler for action/event wrapping at registration time.

This module implements the Moleculer pattern of wrapping handlers
at service registration time, not at call time. This ensures that
middleware (retry, circuit breaker, timeout, etc.) is applied
consistently regardless of how the action is invoked.

The key insight from Moleculer:
    // At REGISTRATION time (not at call time)
    wrappedHandler = broker.middlewares.wrapHandler("localAction", handler, action)
    service.actions[name] = wrappedHandler  // Already wrapped!

This ensures that even direct service.action() calls go through middleware.
"""

from __future__ import annotations

import asyncio
import logging
import warnings
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable

if TYPE_CHECKING:
    from ..broker import ServiceBroker

T = TypeVar("T")

# Type alias for async handler functions
HandlerType = "Callable[[Any], Awaitable[T]]"

# Module logger for debugging
_logger = logging.getLogger(__name__)


class MiddlewareError(Exception):
    """Raised when a middleware hook fails during handler wrapping.

    Attributes:
        middleware_name: Name of the middleware that failed
        hook_name: Name of the hook that failed
        original_error: The underlying exception
    """

    def __init__(
        self,
        message: str,
        middleware_name: str | None = None,
        hook_name: str | None = None,
    ) -> None:
        super().__init__(message)
        self.middleware_name = middleware_name
        self.hook_name = hook_name


@runtime_checkable
class ActionMetadata(Protocol):
    """Protocol for action metadata passed to middleware.

    This protocol defines the minimal interface that action objects
    must implement to be used with middleware wrapping.
    """

    name: str
    handler: Any  # HandlerType | None
    timeout: float | None


@runtime_checkable
class EventMetadata(Protocol):
    """Protocol for event metadata passed to middleware.

    This protocol defines the minimal interface that event objects
    must implement to be used with middleware wrapping.
    """

    name: str
    handler: Any  # HandlerType | None


# Union type for metadata
MetadataType = ActionMetadata | EventMetadata


class MiddlewareHandler:
    """Manages middleware wrapping for action and event handlers.

    This class implements the Moleculer pattern: wrap handlers at registration
    time (in registry.register()) rather than at call time (in broker.call()).

    Benefits:
        - Middleware always applies, even for direct service method calls
        - Handler wrapping happens once (at registration), not on every call
        - More consistent behavior with the original Moleculer framework

    Example:
        handler = MiddlewareHandler(broker)

        # Wrap action handler at registration
        wrapped = await handler.wrap_handler(
            "local_action",
            original_handler,
            action_metadata
        )

        # The wrapped handler includes all middleware
        action.wrapped_handler = wrapped

    Attributes:
        broker: The service broker instance containing middleware list
    """

    __slots__ = ("_hook_cache", "broker")

    def __init__(self, broker: ServiceBroker) -> None:
        """Initialize the middleware handler.

        Args:
            broker: The service broker instance containing middleware list
        """
        self.broker = broker
        # Cache for resolved hooks (hook_name -> list of callables)
        self._hook_cache: dict[str, list[tuple[str, Any]]] = {}

    def _get_hooks(self, hook_name: str) -> list[tuple[str, Any]]:
        """Get cached list of hooks for a given hook name.

        This method caches the hook lookup to avoid repeated getattr calls
        during handler wrapping.

        Args:
            hook_name: Name of the middleware hook

        Returns:
            List of (middleware_name, hook_callable) tuples in reverse order
        """
        if hook_name not in self._hook_cache:
            hooks: list[tuple[str, Any]] = []
            for middleware in reversed(self.broker.middlewares):
                hook = getattr(middleware, hook_name, None)
                if hook is not None and callable(hook):
                    mw_name = type(middleware).__name__
                    hooks.append((mw_name, hook))
            self._hook_cache[hook_name] = hooks
        return self._hook_cache[hook_name]

    def clear_cache(self) -> None:
        """Clear the hook cache.

        Call this method if middlewares are modified after initialization.
        """
        self._hook_cache.clear()

    async def wrap_handler(
        self,
        hook_name: str,
        handler: Any,  # HandlerType
        metadata: Any,  # MetadataType - using Any for compatibility
    ) -> Any:  # HandlerType
        """Wrap a handler with all registered middleware hooks.

        This method applies middleware in reverse order so that the first
        middleware in the list is the outermost wrapper (executed first).

        Middleware execution order (for middlewares=[MW1, MW2, MW3]):
            MW1.pre → MW2.pre → MW3.pre → handler → MW3.post → MW2.post → MW1.post

        This is achieved by wrapping in reverse order:
            handler → MW3(handler) → MW2(MW3(handler)) → MW1(MW2(MW3(handler)))

        Args:
            hook_name: Which middleware hook to apply:
                - "local_action": For local action handlers
                - "remote_action": For remote action handlers
                - "local_event": For local event handlers
            handler: The raw handler function to wrap
            metadata: Action or Event metadata for middleware context

        Returns:
            Handler wrapped with all applicable middleware hooks

        Raises:
            MiddlewareError: If a middleware hook fails
            asyncio.CancelledError: If wrapping is cancelled (always propagated)
        """
        if not self.broker.middlewares:
            return handler

        current_handler = handler
        hooks = self._get_hooks(hook_name)

        for mw_name, hook in hooks:
            try:
                result = hook(current_handler, metadata)

                # Proper awaitable check (not just coroutines)
                # This catches: coroutines, Tasks, Futures, and any awaitable object
                if isinstance(result, Awaitable):
                    current_handler = await result
                else:
                    current_handler = result

            except asyncio.CancelledError:
                # Always propagate cancellation - this is not an error
                _logger.debug("Handler wrapping cancelled in %s.%s", mw_name, hook_name)
                raise

            except Exception as err:
                # Explicit error path with context preservation
                _logger.error(
                    "Middleware %s.%s failed: %s",
                    mw_name,
                    hook_name,
                    err,
                    exc_info=True,
                )
                raise MiddlewareError(
                    f"Middleware {mw_name}.{hook_name} failed: {err}",
                    middleware_name=mw_name,
                    hook_name=hook_name,
                ) from err

        return current_handler

    def wrap_handler_sync(
        self,
        hook_name: str,
        handler: Any,
        metadata: Any,
    ) -> tuple[Any, list[Any]]:
        """DEPRECATED: This method creates dangling coroutines.

        This method is deprecated because it cannot properly handle
        async middleware hooks. The returned coroutines are never awaited
        in the correct order, leading to:
        - RuntimeWarning about unawaited coroutines
        - Incorrect middleware chain construction
        - Potential memory leaks

        Use wrap_handler() in async context instead.

        Args:
            hook_name: Which middleware hook to apply
            handler: The raw handler function to wrap
            metadata: Action or Event metadata

        Returns:
            Tuple of (current_handler, list_of_pending_coroutines)

        Raises:
            DeprecationWarning: Always raised when called
        """
        warnings.warn(
            "wrap_handler_sync is deprecated and creates dangling coroutines. "
            "Use 'await wrap_handler()' instead. "
            "This method will be removed in a future version.",
            DeprecationWarning,
            stacklevel=2,
        )

        if not self.broker.middlewares:
            return handler, []

        current_handler = handler
        pending_coroutines: list[Any] = []

        for middleware in reversed(self.broker.middlewares):
            hook = getattr(middleware, hook_name, None)
            if hook is not None and callable(hook):
                result = hook(current_handler, metadata)
                if isinstance(result, Awaitable):
                    # These coroutines are problematic - they need to be
                    # awaited in order, but we can't do that here
                    pending_coroutines.append((middleware, result))
                else:
                    current_handler = result

        return current_handler, pending_coroutines


__all__ = [
    "ActionMetadata",
    "EventMetadata",
    "HandlerType",
    "MetadataType",
    "MiddlewareError",
    "MiddlewareHandler",
]
