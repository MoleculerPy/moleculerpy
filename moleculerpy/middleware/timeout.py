"""Timeout middleware for enforcing request timeouts.

This module provides the TimeoutMiddleware class that enforces request
timeouts with support for per-action overrides.

Timeout resolution priority:
    1. ctx.options.timeout (explicit per-call override, highest priority)
    2. ctx.meta["timeout"] (fallback per-call override)
    3. action.timeout (@action decorator setting)
    4. middleware.default_timeout (constructor param)
    5. broker.settings.request_timeout (global default)
    6. DEFAULT_TIMEOUT_SECONDS (30.0)

Example usage:
    from moleculerpy import ServiceBroker, Settings
    from moleculerpy.middleware import TimeoutMiddleware

    settings = Settings(
        request_timeout=30.0,  # Global default: 30 seconds
        middlewares=[TimeoutMiddleware()],
    )

    broker = ServiceBroker(id="node-1", settings=settings)

    # Per-action timeout via decorator
    @action(timeout=5.0)  # Override: 5 seconds for this action
    async def slow_action(ctx):
        ...

    # Per-call timeout
    await broker.call("service.action", params={}, meta={"timeout": 10.0})
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable

from moleculerpy.middleware.base import Middleware

if TYPE_CHECKING:
    pass

T = TypeVar("T")

# Type alias for async handler functions
HandlerType = "Callable[[Any], Awaitable[T]]"

# Default timeout in seconds (used as final fallback)
DEFAULT_TIMEOUT_SECONDS: float = 30.0


@runtime_checkable
class SettingsProtocol(Protocol):
    """Protocol for broker settings with request_timeout."""

    request_timeout: float


@runtime_checkable
class BrokerProtocol(Protocol):
    """Protocol for broker with settings."""

    settings: SettingsProtocol


@runtime_checkable
class ContextProtocol(Protocol):
    """Protocol for request context.

    This protocol defines the minimal interface for context objects
    used by TimeoutMiddleware.
    """

    meta: dict[str, Any]

    @property
    def broker(self) -> BrokerProtocol | None:
        """The broker instance, if available."""
        ...


@runtime_checkable
class ActionProtocol(Protocol):
    """Protocol for action metadata."""

    name: str
    timeout: float | None


class RequestTimeoutError(Exception):
    """Exception raised when a request exceeds its timeout limit.

    Attributes:
        action_name: Name of the action that timed out
        timeout_seconds: The timeout value that was exceeded
        elapsed_seconds: How long the request actually took before timing out
    """

    __slots__ = ("action_name", "elapsed_seconds", "timeout_seconds")

    def __init__(
        self,
        action_name: str,
        timeout: float,
        elapsed: float,
        message: str | None = None,
    ) -> None:
        """Initialize the timeout error.

        Args:
            action_name: Name of the action that timed out
            timeout: Timeout limit in seconds
            elapsed: Elapsed time in seconds before timeout
            message: Optional custom error message
        """
        self.action_name = action_name
        self.timeout_seconds = timeout
        self.elapsed_seconds = elapsed

        if message is None:
            message = (
                f"Request to '{action_name}' timed out after {elapsed:.2f}s (limit: {timeout}s)"
            )
        super().__init__(message)


class TimeoutMiddleware(Middleware):
    """Middleware that enforces request timeouts.

    This middleware wraps action handlers to enforce timeout limits.
    If a handler takes longer than the configured timeout, a
    RequestTimeoutError is raised.

    Timeout resolution (in order of priority):
        1. ctx.meta["timeout"] - Explicit per-call override
        2. action.timeout - Per-action setting from @action decorator
        3. self.default_timeout - Middleware default (constructor param)
        4. broker.settings.request_timeout - Global broker setting
        5. DEFAULT_TIMEOUT_SECONDS (30.0) - Ultimate fallback

    Example:
        # Global timeout via settings
        settings = Settings(request_timeout=30.0)

        # Per-action timeout
        @action(timeout=5.0)
        async def quick_action(ctx): ...

        # Per-call timeout
        await broker.call("svc.action", meta={"timeout": 10.0})

    Interaction with other middleware:
        - RetryMiddleware: Will retry on asyncio.TimeoutError (default retryable)
        - CircuitBreaker: Counts timeout as trip event (default trip_errors)

    Note:
        This middleware should typically be registered AFTER retry middleware
        so that each retry attempt gets its own timeout.

    Attributes:
        default_timeout: Default timeout in seconds (None = use broker setting)
    """

    __slots__ = ("default_timeout",)

    def __init__(self, default_timeout: float | None = None) -> None:
        """Initialize the timeout middleware.

        Args:
            default_timeout: Default timeout in seconds. If None, uses
                broker.settings.request_timeout (default: 30.0)
        """
        self.default_timeout = default_timeout

    def _create_timeout_handler(
        self,
        next_handler: Any,  # HandlerType
        action_name: str,
        action_timeout: float | None,
    ) -> Any:  # HandlerType
        """Create a timeout-enforcing wrapper handler.

        This method is the single implementation for both local and remote
        action wrapping, following the DRY principle.

        Args:
            next_handler: The next handler in the middleware chain
            action_name: Name of the action (for error messages)
            action_timeout: Per-action timeout from decorator (pre-resolved)

        Returns:
            Wrapped handler that enforces timeout
        """
        # Pre-resolved values (captured in closure at wrap time)
        middleware_timeout = self.default_timeout

        async def timeout_handler(ctx: Any) -> Any:
            # Fast path: resolve timeout with explicit priority
            # 1a. ctx.options.timeout (highest priority - explicit per-call)
            timeout: float | None = None

            options = getattr(ctx, "options", None)
            if options is not None:
                options_timeout = getattr(options, "timeout", None)
                if options_timeout is not None:
                    timeout = float(options_timeout)

            # 1b. ctx.meta["timeout"] (fallback for per-call override)
            if timeout is None:
                meta = getattr(ctx, "meta", None)
                if meta is not None:
                    meta_timeout = meta.get("timeout") if hasattr(meta, "get") else None
                    if meta_timeout is not None:
                        timeout = float(meta_timeout)

            # 2. Per-action timeout (pre-resolved)
            if timeout is None and action_timeout is not None:
                timeout = action_timeout

            # 3. Middleware default (pre-resolved)
            if timeout is None and middleware_timeout is not None:
                timeout = middleware_timeout

            # 4. Broker settings
            if timeout is None:
                broker = getattr(ctx, "broker", None)
                if broker is not None:
                    settings = getattr(broker, "settings", None)
                    if settings is not None:
                        broker_timeout = getattr(settings, "request_timeout", None)
                        if broker_timeout is not None:
                            timeout = float(broker_timeout)

            # 5. Ultimate fallback
            if timeout is None:
                timeout = DEFAULT_TIMEOUT_SECONDS

            start_time = time.perf_counter()

            try:
                return await asyncio.wait_for(next_handler(ctx), timeout=timeout)

            except asyncio.CancelledError:
                # Always propagate cancellation - this is NOT a timeout
                # CancelledError means the task was explicitly cancelled,
                # not that it timed out
                raise

            except TimeoutError:
                elapsed = time.perf_counter() - start_time
                raise RequestTimeoutError(
                    action_name=action_name,
                    timeout=timeout,
                    elapsed=elapsed,
                ) from None

        return timeout_handler

    async def local_action(
        self,
        next_handler: Any,  # HandlerType
        action: Any,  # ActionProtocol
    ) -> Any:  # HandlerType
        """Wrap local action handler with timeout enforcement.

        Args:
            next_handler: The next handler in the middleware chain
            action: Action metadata

        Returns:
            Wrapped handler that enforces timeout
        """
        # Pre-resolve action-level values at wrap time (not per-call)
        action_name = getattr(action, "name", "unknown")
        action_timeout = getattr(action, "timeout", None)

        return self._create_timeout_handler(
            next_handler=next_handler,
            action_name=action_name,
            action_timeout=action_timeout,
        )

    async def remote_action(
        self,
        next_handler: Any,  # HandlerType
        action: Any,  # ActionProtocol
    ) -> Any:  # HandlerType
        """Wrap remote action handler with timeout enforcement.

        For remote actions, the timeout applies to the entire round-trip
        including network latency, serialization, and remote execution.

        Args:
            next_handler: The next handler in the middleware chain
            action: Action metadata

        Returns:
            Wrapped handler that enforces timeout
        """
        # Pre-resolve action-level values at wrap time (not per-call)
        action_name = getattr(action, "name", "unknown")
        action_timeout = getattr(action, "timeout", None)

        return self._create_timeout_handler(
            next_handler=next_handler,
            action_name=action_name,
            action_timeout=action_timeout,
        )


__all__ = [
    "DEFAULT_TIMEOUT_SECONDS",
    "ActionProtocol",
    "ContextProtocol",
    "RequestTimeoutError",
    "TimeoutMiddleware",
]
