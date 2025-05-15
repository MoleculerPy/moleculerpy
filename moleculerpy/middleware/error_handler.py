"""ErrorHandler Middleware for the MoleculerPy framework.

This module implements centralized error handling for actions and events.
It catches errors, normalizes them to MoleculerError, and calls the broker's
error_handler callback for custom error processing.

Features:
    - Converts non-Exception errors to MoleculerError
    - Attaches context to error (error.ctx)
    - Removes pending requests for failed remote calls
    - Calls broker.error_handler(error, info) for custom handling
    - Events swallow errors after logging (fire-and-forget pattern)

Example usage:
    from moleculerpy import ServiceBroker, Settings
    from moleculerpy.middleware import ErrorHandlerMiddleware

    # Custom error handler
    async def my_error_handler(error, info):
        # Transform error, log to external service, etc.
        logger.error(f"Error in {info.get('action')}: {error}")
        raise error  # Re-raise or return transformed error

    settings = Settings(
        middlewares=[ErrorHandlerMiddleware()],
    )

    broker = ServiceBroker(id="my-node", settings=settings)
    broker.error_handler = my_error_handler

Moleculer.js compatible:
    - broker.errorHandler → broker.error_handler
    - Error context attachment (error.ctx)
    - Pending request cleanup for remote calls
    - Event error swallowing with logging
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from moleculerpy.errors import MoleculerError
from moleculerpy.middleware.base import Middleware

if TYPE_CHECKING:
    from moleculerpy.broker import ServiceBroker
    from moleculerpy.context import Context
    from moleculerpy.protocols import ActionProtocol, EventProtocol

__all__ = ["ErrorHandlerMiddleware"]

# Type alias for error handler function
ErrorHandlerFunc = Callable[[Exception, dict[str, Any]], Awaitable[Any]]

# Type alias for action/event handlers
HandlerType = Callable[["Context"], Awaitable[Any]]


class ErrorHandlerMiddleware(Middleware):
    """Middleware for centralized error handling.

    Catches all errors from action and event handlers, normalizes them,
    and delegates to the broker's error_handler callback for custom processing.

    For actions:
        - Errors are caught and passed to broker.error_handler
        - If error_handler re-raises, error propagates to caller
        - Pending requests are cleaned up for failed remote calls

    For events:
        - Errors are caught and passed to broker.error_handler
        - If error_handler throws, error is logged (not propagated)
        - Fire-and-forget pattern: events don't return errors to emitter

    Attributes:
        logger: Logger for error events
        _broker: Reference to the broker instance
    """

    __slots__ = ("_broker", "logger")

    def __init__(self, logger: logging.Logger | None = None) -> None:
        """Initialize the ErrorHandlerMiddleware.

        Args:
            logger: Optional logger for error events
        """
        super().__init__()
        self.logger = logger or logging.getLogger("moleculerpy.middleware.error_handler")
        self._broker: ServiceBroker | None = None

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return "ErrorHandlerMiddleware()"

    def broker_created(self, broker: ServiceBroker) -> None:
        """Store broker reference when created.

        Args:
            broker: The broker instance
        """
        self._broker = broker

    def _normalize_error(self, error: Any) -> Exception:
        """Normalize any error to an Exception instance.

        Args:
            error: The error (Exception or other value)

        Returns:
            Exception instance (MoleculerError if not already Exception)
        """
        if isinstance(error, Exception):
            return error
        # Convert non-Exception to MoleculerError
        return MoleculerError(str(error), code=500)

    def _attach_context(self, error: Exception, ctx: Context) -> None:
        """Attach context to error for debugging.

        Args:
            error: The exception to attach context to
            ctx: The context to attach
        """
        # Store ctx as non-enumerable-like attribute
        # In Python we can just set it directly
        try:
            error.ctx = ctx  # type: ignore[attr-defined]
        except (AttributeError, TypeError):
            # Some built-in exceptions don't allow setting attributes
            pass

    async def _call_error_handler(
        self,
        error: Exception,
        info: dict[str, Any],
    ) -> Any:
        """Call the broker's error handler if available.

        Args:
            error: The exception that occurred
            info: Context info (ctx, service, action/event)

        Returns:
            Result from error_handler or re-raises error

        Raises:
            Exception: Re-raised from error_handler or original error
        """
        if self._broker is None:
            raise error

        # Check if broker has error_handler
        handler = getattr(self._broker, "error_handler", None)
        if handler is None:
            raise error

        # Call the error handler
        result = handler(error, info)
        if asyncio.iscoroutine(result):
            return await result
        return result

    async def local_action(
        self,
        next_handler: HandlerType,
        action: ActionProtocol,
    ) -> HandlerType:
        """Wrap local action with error handling.

        Args:
            next_handler: The next handler in the chain
            action: The action being registered

        Returns:
            Wrapped handler with error handling
        """
        action_name = getattr(action, "name", str(action))
        service = getattr(action, "service", None)

        async def error_handler_wrapper(ctx: Context) -> Any:
            try:
                return await next_handler(ctx)
            except Exception as err:
                # Normalize error
                error = self._normalize_error(err)

                # Attach context
                self._attach_context(error, ctx)

                # Log debug message
                self.logger.debug(
                    f"The '{action_name}' request is rejected.",
                    extra={"request_id": ctx.request_id},
                    exc_info=error,
                )

                # Build error info
                info: dict[str, Any] = {
                    "ctx": ctx,
                    "service": service,
                    "action": action,
                }

                # Call broker error handler
                return await self._call_error_handler(error, info)

        return error_handler_wrapper

    async def remote_action(
        self,
        next_handler: HandlerType,
        action: ActionProtocol,
    ) -> HandlerType:
        """Wrap remote action with error handling.

        Args:
            next_handler: The next handler in the chain
            action: The action being registered

        Returns:
            Wrapped handler with error handling
        """
        action_name = getattr(action, "name", str(action))
        service = getattr(action, "service", None)

        async def error_handler_wrapper(ctx: Context) -> Any:
            try:
                return await next_handler(ctx)
            except Exception as err:
                # Normalize error
                error = self._normalize_error(err)

                # Check if this is a remote call that failed before reaching target
                if self._broker and hasattr(self._broker, "transit"):
                    broker_node_id = getattr(self._broker, "nodeID", None)
                    ctx_node_id = getattr(ctx, "node_id", None)
                    if ctx_node_id and ctx_node_id != broker_node_id:
                        # Remove pending request (request didn't reach target)
                        transit = getattr(self._broker, "transit", None)
                        if transit and hasattr(transit, "remove_pending_request"):
                            transit.remove_pending_request(ctx.id)

                # Attach context
                self._attach_context(error, ctx)

                # Log debug message
                self.logger.debug(
                    f"The '{action_name}' request is rejected.",
                    extra={"request_id": ctx.request_id},
                    exc_info=error,
                )

                # Build error info
                info: dict[str, Any] = {
                    "ctx": ctx,
                    "service": service,
                    "action": action,
                }

                # Call broker error handler
                return await self._call_error_handler(error, info)

        return error_handler_wrapper

    async def local_event(
        self,
        next_handler: HandlerType,
        event: EventProtocol,
    ) -> HandlerType:
        """Wrap local event with error handling.

        Events use fire-and-forget pattern - errors are logged but not propagated.

        Args:
            next_handler: The next handler in the chain
            event: The event being registered

        Returns:
            Wrapped handler with error handling
        """
        event_name = getattr(event, "name", str(event))
        service = getattr(event, "service", None)
        service_name = getattr(service, "full_name", getattr(service, "name", "unknown"))

        async def error_handler_wrapper(ctx: Context) -> Any:
            try:
                return await next_handler(ctx)
            except Exception as err:
                # Normalize error
                error = self._normalize_error(err)

                # Attach context
                self._attach_context(error, ctx)

                # Log debug message
                self.logger.debug(
                    f"Error occurred in the '{event_name}' event handler "
                    f"in the '{service_name}' service.",
                    extra={"request_id": ctx.request_id},
                    exc_info=error,
                )

                # Build error info
                info: dict[str, Any] = {
                    "ctx": ctx,
                    "service": service,
                    "event": event,
                }

                # Try to call broker error handler
                try:
                    return await self._call_error_handler(error, info)
                except Exception as handler_error:
                    # Error handler threw or re-raised
                    # For events, we just log and swallow (fire-and-forget)
                    self.logger.error(
                        f"Error in event handler '{event_name}': {handler_error}",
                        exc_info=handler_error,
                    )
                    return None

        return error_handler_wrapper
