"""ContextTracker Middleware for the MoleculerPy framework.

This module implements context tracking for graceful shutdown support.
It monitors active request contexts (both local and remote) to ensure
all pending requests complete before shutdown or timeout.

Features:
    - Tracks active contexts at broker and service level
    - Graceful shutdown waits for pending requests
    - Configurable timeout with GracefulStopTimeoutError
    - Per-context tracking override via ctx.options.tracking
    - Supports local actions, remote actions, and events

Example usage:
    from moleculerpy import ServiceBroker, Settings
    from moleculerpy.middleware import ContextTrackerMiddleware

    settings = Settings(
        middlewares=[ContextTrackerMiddleware()],
        tracking={
            "enabled": True,
            "shutdown_timeout": 5000,  # ms
        },
    )

    broker = ServiceBroker(id="my-node", settings=settings)

Moleculer.js compatible:
    - broker.options.tracking.enabled
    - broker.options.tracking.shutdownTimeout
    - service.settings.$shutdownTimeout (per-service override)
    - ctx.options.tracking (per-context override)
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any
from weakref import WeakKeyDictionary

from moleculerpy.errors import MoleculerError
from moleculerpy.middleware.base import Middleware

if TYPE_CHECKING:
    from moleculerpy.broker import ServiceBroker
    from moleculerpy.context import Context
    from moleculerpy.protocols import ActionProtocol, EventProtocol
    from moleculerpy.service import Service

__all__ = ["ContextTrackerMiddleware", "GracefulStopTimeoutError"]

# Type alias for action/event handlers
HandlerType = Callable[["Context"], Awaitable[Any]]


class GracefulStopTimeoutError(MoleculerError):
    """Error raised when graceful shutdown times out.

    This error indicates that the shutdown timeout was exceeded while
    waiting for active requests to complete.

    Attributes:
        service_name: Name of the service that timed out (if applicable)
    """

    def __init__(
        self,
        message: str | None = None,
        service_name: str | None = None,
    ) -> None:
        """Initialize GracefulStopTimeoutError.

        Args:
            message: Custom error message
            service_name: Name of the service that timed out
        """
        if message is None:
            if service_name:
                message = (
                    f"Shutdown timeout reached for service '{service_name}'. "
                    "Forcing stop with active requests still pending."
                )
            else:
                message = (
                    "Shutdown timeout reached. Forcing stop with active requests still pending."
                )

        super().__init__(
            message=message,
            code=500,
            error_type="GRACEFUL_STOP_TIMEOUT",
            data={"service": service_name} if service_name else None,
        )
        self.service_name = service_name


class ContextTrackerMiddleware(Middleware):
    """Middleware for tracking active request contexts.

    Monitors active contexts to enable graceful shutdown. Tracks contexts
    at both broker level (remote requests) and service level (local requests).

    During shutdown, waits for all tracked contexts to complete or until
    the shutdown timeout is exceeded.

    Attributes:
        logger: Logger for tracking events
        _broker: Reference to the broker instance
        _default_timeout: Default shutdown timeout in milliseconds
        _poll_interval: Polling interval for shutdown check (ms)
    """

    __slots__ = (
        "_broker",
        "_broker_contexts",
        "_default_timeout",
        "_poll_interval",
        "_service_contexts",
        "logger",
    )

    def __init__(
        self,
        shutdown_timeout: int = 5000,
        poll_interval: int = 100,
        logger: logging.Logger | None = None,
    ) -> None:
        """Initialize the ContextTrackerMiddleware.

        Args:
            shutdown_timeout: Default shutdown timeout in milliseconds
            poll_interval: Polling interval for shutdown check (ms)
            logger: Optional logger for tracking events
        """
        super().__init__()
        self.logger = logger or logging.getLogger("moleculerpy.middleware.context_tracker")
        self._broker: ServiceBroker | None = None
        self._broker_contexts: WeakKeyDictionary[ServiceBroker, list[Context]] = WeakKeyDictionary()
        self._service_contexts: WeakKeyDictionary[Service, list[Context]] = WeakKeyDictionary()
        self._default_timeout = shutdown_timeout
        self._poll_interval = poll_interval

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return f"ContextTrackerMiddleware(timeout={self._default_timeout}ms)"

    def _is_tracking_enabled(self) -> bool:
        """Check if tracking is enabled in broker settings.

        Returns:
            True if tracking is enabled, False otherwise
        """
        if self._broker is None:
            return True

        settings = getattr(self._broker, "settings", None)
        if settings is None:
            return True

        tracking = getattr(settings, "tracking", None)
        if tracking is None:
            return True

        if isinstance(tracking, dict):
            return bool(tracking.get("enabled", True))

        return True

    def _should_track_context(self, ctx: Context) -> bool:
        """Check if a specific context should be tracked.

        Args:
            ctx: The context to check

        Returns:
            True if context should be tracked, False otherwise
        """
        if not self._is_tracking_enabled():
            return False

        # Check per-context override
        options = getattr(ctx, "options", None)
        if options is not None:
            tracking = getattr(options, "tracking", None)
            if tracking is not None:
                return bool(tracking)

        return True

    def _add_context(self, ctx: Context) -> None:
        """Add a context to the tracking list.

        Adds to service-level list for local requests,
        or broker-level list for remote requests.

        Args:
            ctx: The context to track
        """
        service = getattr(ctx, "service", None)
        if service is not None:
            # Local request - track at service level
            tracked = self._get_service_contexts(service)
            if tracked is not None:
                tracked.append(ctx)
        elif self._broker is not None:
            # Remote request - track at broker level
            tracked = self._get_broker_contexts(self._broker)
            if tracked is not None:
                tracked.append(ctx)

    def _remove_context(self, ctx: Context) -> None:
        """Remove a context from the tracking list.

        Args:
            ctx: The context to remove
        """
        service = getattr(ctx, "service", None)
        if service is not None:
            # Local request - remove from service level
            tracked = self._get_service_contexts(service)
            if tracked is not None:
                try:
                    tracked.remove(ctx)
                except ValueError:
                    pass  # Already removed
        elif self._broker is not None:
            # Remote request - remove from broker level
            tracked = self._get_broker_contexts(self._broker)
            if tracked is not None:
                try:
                    tracked.remove(ctx)
                except ValueError:
                    pass  # Already removed

    def _get_broker_contexts(self, broker: ServiceBroker) -> list[Context] | None:
        """Return broker tracking storage with backward-compatible fallback."""
        tracked = self._broker_contexts.get(broker)
        if tracked is not None:
            return tracked

        legacy_tracked = getattr(broker, "_tracked_contexts", None)
        if isinstance(legacy_tracked, list):
            return legacy_tracked
        return None

    def _get_service_contexts(self, service: Service) -> list[Context] | None:
        """Return service tracking storage with backward-compatible fallback."""
        tracked = self._service_contexts.get(service)
        if tracked is not None:
            return tracked

        legacy_tracked = getattr(service, "_tracked_contexts", None)
        if isinstance(legacy_tracked, list):
            return legacy_tracked
        return None

    async def _wait_for_contexts(
        self,
        tracked_list: list[Context],
        timeout_ms: int,
        service_name: str | None = None,
    ) -> None:
        """Wait for all tracked contexts to complete.

        Polls the tracked list until empty or timeout is exceeded.

        Args:
            tracked_list: List of tracked contexts
            timeout_ms: Timeout in milliseconds
            service_name: Service name for error reporting

        Raises:
            GracefulStopTimeoutError: If timeout is exceeded
        """
        if not tracked_list:
            return

        timeout_sec = timeout_ms / 1000.0
        poll_sec = self._poll_interval / 1000.0
        elapsed = 0.0

        while tracked_list:
            if elapsed >= timeout_sec:
                self.logger.error(
                    f"Graceful stop timeout reached. {len(tracked_list)} request(s) still pending."
                )
                # Clear the list to allow garbage collection
                tracked_list.clear()
                raise GracefulStopTimeoutError(service_name=service_name)

            await asyncio.sleep(poll_sec)
            elapsed += poll_sec

        self.logger.debug("All tracked contexts completed successfully")

    # ========== Lifecycle Hooks ==========

    def broker_created(self, broker: ServiceBroker) -> None:
        """Initialize broker-level tracking storage.

        Args:
            broker: The broker instance
        """
        self._broker = broker
        tracked: list[Context] = []
        self._broker_contexts[broker] = tracked
        setattr(broker, "_tracked_contexts", tracked)
        self.logger.debug("Broker context tracking initialized")

    def service_starting(self, service: Service) -> None:
        """Initialize service-level tracking storage.

        Args:
            service: The service instance
        """
        tracked: list[Context] = []
        self._service_contexts[service] = tracked
        service._tracked_contexts = tracked
        self.logger.debug(f"Service '{service.name}' context tracking initialized")

    async def service_stopping(self, service: Service) -> None:
        """Wait for service's active contexts before stopping.

        Args:
            service: The service being stopped
        """
        tracked = self._get_service_contexts(service)
        if tracked is None or not tracked:
            return

        # Get service-specific timeout or use default
        settings = getattr(service, "settings", {})
        timeout = settings.get("$shutdown_timeout", self._default_timeout)

        self.logger.info(
            f"Waiting for {len(tracked)} active request(s) "
            f"in service '{service.name}' (timeout: {timeout}ms)"
        )

        try:
            await self._wait_for_contexts(tracked, timeout, service.name)
        except GracefulStopTimeoutError:
            self.logger.warning(
                f"Service '{service.name}' shutdown timed out with pending requests"
            )

    async def broker_stopping(self, broker: ServiceBroker) -> None:
        """Wait for broker's active contexts before stopping.

        Args:
            broker: The broker being stopped
        """
        tracked = self._get_broker_contexts(broker)
        if tracked is None or not tracked:
            return

        # Get broker-level timeout
        settings = getattr(broker, "settings", None)
        if settings is not None:
            tracking_config = getattr(settings, "tracking", {})
            if isinstance(tracking_config, dict):
                timeout = tracking_config.get("shutdown_timeout", self._default_timeout)
            else:
                timeout = self._default_timeout
        else:
            timeout = self._default_timeout

        self.logger.info(
            f"Waiting for {len(tracked)} active remote request(s) (timeout: {timeout}ms)"
        )

        try:
            await self._wait_for_contexts(tracked, timeout)
        except GracefulStopTimeoutError:
            self.logger.warning("Broker shutdown timed out with pending remote requests")

    # ========== Handler Wrappers ==========

    async def local_action(
        self,
        next_handler: HandlerType,
        action: ActionProtocol,
    ) -> HandlerType:
        """Wrap local action with context tracking.

        Args:
            next_handler: The next handler in the chain
            action: The action being registered

        Returns:
            Wrapped handler with context tracking
        """

        async def tracking_wrapper(ctx: Context) -> Any:
            if not self._should_track_context(ctx):
                return await next_handler(ctx)

            self._add_context(ctx)
            try:
                return await next_handler(ctx)
            finally:
                self._remove_context(ctx)

        return tracking_wrapper

    async def remote_action(
        self,
        next_handler: HandlerType,
        action: ActionProtocol,
    ) -> HandlerType:
        """Wrap remote action with context tracking.

        Args:
            next_handler: The next handler in the chain
            action: The action being registered

        Returns:
            Wrapped handler with context tracking
        """

        async def tracking_wrapper(ctx: Context) -> Any:
            if not self._should_track_context(ctx):
                return await next_handler(ctx)

            self._add_context(ctx)
            try:
                return await next_handler(ctx)
            finally:
                self._remove_context(ctx)

        return tracking_wrapper

    async def local_event(
        self,
        next_handler: HandlerType,
        event: EventProtocol,
    ) -> HandlerType:
        """Wrap local event with context tracking.

        Args:
            next_handler: The next handler in the chain
            event: The event being registered

        Returns:
            Wrapped handler with context tracking
        """

        async def tracking_wrapper(ctx: Context) -> Any:
            if not self._should_track_context(ctx):
                return await next_handler(ctx)

            self._add_context(ctx)
            try:
                return await next_handler(ctx)
            finally:
                self._remove_context(ctx)

        return tracking_wrapper
