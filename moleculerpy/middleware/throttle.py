"""Throttle Middleware for the MoleculerPy framework.

This module implements event throttling to limit execution rate.
Unlike debounce (which executes the LAST event after quiet period),
throttle executes the FIRST event and ignores subsequent events
until the interval passes.

Features:
    - Rate limiting for event handlers
    - First-event-wins (executes immediately, then blocks)
    - Configurable throttle interval per event
    - Non-blocking: returns immediately for skipped events

Example usage:
    from moleculerpy import Service

    class MetricsService(Service):
        name = "metrics"

        events = {
            "metrics.collect": {
                "throttle": 1000,  # Max once per second
                "handler": "on_metrics_collect",
            },
        }

        async def on_metrics_collect(self, ctx):
            # Called at most once per second, even if events
            # arrive more frequently
            await self.aggregate_metrics(ctx.params)

Use cases:
    - Rate limiting API calls
    - Preventing excessive logging
    - Limiting notification frequency
    - Resource usage monitoring

Moleculer.js compatible:
    - event.throttle: time in milliseconds
    - Non-blocking execution (returns immediately for skipped)
    - First-event-wins pattern

Difference from Debounce:
    - Debounce: waits for quiet period, executes LAST event
    - Throttle: executes FIRST event, ignores until interval passes
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from moleculerpy.middleware.base import Middleware

if TYPE_CHECKING:
    from moleculerpy.context import Context
    from moleculerpy.protocols import EventProtocol

__all__ = ["ThrottleMiddleware"]

# Type alias for event handlers
HandlerType = Callable[["Context"], Awaitable[Any]]


class ThrottleMiddleware(Middleware):
    """Middleware for throttling event handlers.

    Limits the rate at which event handlers can be executed.
    The first event is executed immediately, then subsequent
    events are ignored until the throttle interval passes.

    This is useful for:
        - Rate limiting external API calls
        - Preventing log spam
        - Limiting resource-intensive operations
        - Controlling notification frequency

    Attributes:
        logger: Logger for throttle events
    """

    __slots__ = ("logger",)

    def __init__(self, logger: logging.Logger | None = None) -> None:
        """Initialize the ThrottleMiddleware.

        Args:
            logger: Optional logger for throttle events
        """
        super().__init__()
        self.logger = logger or logging.getLogger("moleculerpy.middleware.throttle")

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return "ThrottleMiddleware()"

    async def local_event(
        self,
        next_handler: HandlerType,
        event: EventProtocol,
    ) -> HandlerType:
        """Wrap local event with throttle logic.

        If event.throttle > 0, wraps the handler to limit execution rate.
        Otherwise returns the original handler unchanged.

        Args:
            next_handler: The next handler in the chain
            event: The event being registered

        Returns:
            Wrapped handler with throttle or original handler
        """
        # Get throttle time from event config
        throttle_ms = getattr(event, "throttle", 0)

        # If no throttle configured, return original handler
        if not throttle_ms or throttle_ms <= 0:
            return next_handler

        event_name = getattr(event, "name", str(event))
        throttle_sec = throttle_ms / 1000.0

        self.logger.debug(f"Throttle enabled for event '{event_name}' ({throttle_ms}ms)")

        # Closure state for last execution time and lock for thread safety
        last_execution: float = 0.0
        lock = asyncio.Lock()

        async def throttle_wrapper(ctx: Context) -> Any:
            """Throttle wrapper that limits execution rate."""
            nonlocal last_execution

            current_time = time.monotonic()

            # Check and update under lock to prevent race conditions
            async with lock:
                elapsed = current_time - last_execution

                # If enough time has passed, execute the handler
                if elapsed >= throttle_sec:
                    last_execution = current_time
                    should_execute = True
                else:
                    should_execute = False
                    remaining = throttle_sec - elapsed

            if should_execute:
                return await next_handler(ctx)

            # Otherwise, skip this event
            self.logger.debug(f"Throttled event '{event_name}' ({remaining:.3f}s remaining)")
            return None

        return throttle_wrapper
