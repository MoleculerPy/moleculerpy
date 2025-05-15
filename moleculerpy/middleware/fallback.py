"""Fallback Middleware for the MoleculerPy framework.

This module implements the Fallback pattern to provide graceful degradation
when actions fail. It supports both call-time and action-level fallbacks.

Features:
    - Call-time fallback via ctx.options.fallback_response
    - Action-level fallback via @action(fallback=...)
    - Static values or callable fallbacks
    - Full access to context and error in fallback functions
    - Tracks fallback usage via ctx.fallback_result flag

Example usage:
    from moleculerpy import ServiceBroker, Settings
    from moleculerpy.middleware import FallbackMiddleware

    settings = Settings(
        middlewares=[FallbackMiddleware()]
    )

    broker = ServiceBroker(id="my-node", settings=settings)

    # Call-time fallback
    result = await broker.call(
        "users.get",
        {"id": 123},
        options={"fallback_response": {"id": 0, "name": "Guest"}}
    )

    # Action-level fallback
    @action(fallback="get_default_user")
    async def get_user(ctx):
        return await external_api.fetch(ctx.params["id"])

    async def get_default_user(ctx, error):
        return {"id": 0, "name": "Default User"}

Moleculer.js compatible:
    - ctx.options.fallbackResponse → ctx.options.fallback_response
    - action.fallback (method name or function)
    - ctx.fallbackResult → ctx.fallback_result
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from moleculerpy.middleware.base import HandlerType, Middleware

if TYPE_CHECKING:
    from moleculerpy.context import Context
    from moleculerpy.protocols import ActionProtocol

__all__ = ["FallbackMiddleware"]

# Type for fallback functions
FallbackFunc = Callable[["Context", Exception], Any | Awaitable[Any]]


class FallbackMiddleware(Middleware):
    """Middleware that provides fallback responses when actions fail.

    Fallback priority:
        1. ctx.options.fallback_response (call-time, highest priority)
        2. action.fallback (action definition)
        3. No fallback → re-raise error

    The fallback can be:
        - Static value: {"default": "value"}
        - Function: (ctx, error) -> response
        - Method name (string): resolved on service instance

    Attributes:
        logger: Logger for fallback events
    """

    __slots__ = ("logger",)

    def __init__(self, logger: logging.Logger | None = None) -> None:
        """Initialize the FallbackMiddleware.

        Args:
            logger: Optional logger for fallback events
        """
        self.logger = logger or logging.getLogger("moleculerpy.middleware.fallback")

    def _resolve_fallback(
        self,
        action: ActionProtocol,
        fallback_value: Any,
    ) -> FallbackFunc | Any | None:
        """Resolve fallback to a callable or static value.

        Args:
            action: The action with potential service reference
            fallback_value: The fallback definition (function, string, or value)

        Returns:
            Resolved fallback callable, static value, or None
        """
        if fallback_value is None:
            return None

        # If it's already callable, return it
        if callable(fallback_value):
            return fallback_value

        # If it's a string, try to resolve as method on service
        if isinstance(fallback_value, str):
            service = getattr(action, "service", None)
            if service is not None:
                method = getattr(service, fallback_value, None)
                if callable(method):
                    return method
                self.logger.warning(
                    "Fallback method '%s' not found on service '%s'",
                    fallback_value,
                    getattr(service, "name", "unknown"),
                )
            return None

        # Static value - wrap in a function that returns it
        return fallback_value

    async def _invoke_fallback(
        self,
        ctx: Context,
        error: Exception,
        fallback: FallbackFunc | Any,
    ) -> Any:
        """Invoke the fallback and return the result.

        Args:
            ctx: The request context
            error: The error that triggered fallback
            fallback: The fallback (callable or static value)

        Returns:
            Fallback response
        """
        # Mark that fallback was used
        ctx.fallback_result = True

        # If fallback is callable, invoke it
        if callable(fallback):
            result = fallback(ctx, error)
            # Handle async fallback functions
            if isinstance(result, Awaitable):
                return await result
            return result

        # Static value
        return fallback

    async def local_action(self, next_handler: HandlerType[Any], action: Any) -> HandlerType[Any]:
        """Wrap local action with fallback support.

        Args:
            next_handler: The next handler in the chain
            action: The action being registered

        Returns:
            Wrapped handler with fallback support
        """
        # Get action-level fallback if defined
        action_fallback = getattr(action, "fallback", None)
        action_name: str = getattr(action, "name", str(action))

        async def fallback_handler(ctx: Context) -> Any:
            """Handler with fallback support."""
            try:
                return await next_handler(ctx)
            except Exception as error:
                # Priority 1: Call-time fallback_response
                options = getattr(ctx, "options", None)
                if options is not None:
                    call_fallback = getattr(options, "fallback_response", None)
                    if call_fallback is not None:
                        self.logger.debug(
                            "Using call-time fallback for action '%s': %s",
                            action_name,
                            type(error).__name__,
                        )
                        return await self._invoke_fallback(ctx, error, call_fallback)

                # Priority 2: Action-level fallback
                if action_fallback is not None:
                    resolved = self._resolve_fallback(action, action_fallback)
                    if resolved is not None:
                        self.logger.debug(
                            "Using action fallback for '%s': %s",
                            action_name,
                            type(error).__name__,
                        )
                        return await self._invoke_fallback(ctx, error, resolved)

                # No fallback configured - re-raise
                raise

        return fallback_handler

    async def remote_action(self, next_handler: HandlerType[Any], action: Any) -> HandlerType[Any]:
        """Wrap remote action with fallback support.

        For remote actions, only call-time fallback_response is supported
        (same as Moleculer.js behavior).

        Args:
            next_handler: The next handler in the chain
            action: The action being registered

        Returns:
            Wrapped handler with fallback support
        """
        action_name: str = getattr(action, "name", str(action))

        async def fallback_handler(ctx: Context) -> Any:
            """Handler with call-time fallback support."""
            try:
                return await next_handler(ctx)
            except Exception as error:
                # Only call-time fallback for remote actions
                options = getattr(ctx, "options", None)
                if options is not None:
                    call_fallback = getattr(options, "fallback_response", None)
                    if call_fallback is not None:
                        self.logger.debug(
                            "Using call-time fallback for remote action '%s': %s",
                            action_name,
                            type(error).__name__,
                        )
                        return await self._invoke_fallback(ctx, error, call_fallback)

                # No fallback - re-raise
                raise

        return fallback_handler
