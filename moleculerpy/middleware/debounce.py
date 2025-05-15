"""Debounce Middleware for the MoleculerPy framework.

This module implements event debouncing to defer execution until a
"quiet period" after a burst of events.

Features:
    - Timer-based deferral with automatic cancellation
    - Only the last event in a burst is executed
    - Configurable debounce time per event
    - Non-blocking: returns immediately, executes later

Example usage:
    from moleculerpy import Service

    class ConfigService(Service):
        name = "config"

        events = {
            "config.changed": {
                "debounce": 5000,  # Wait 5 seconds of inactivity
                "handler": "on_config_changed",
            },
        }

        async def on_config_changed(self, ctx):
            # Only called once after 5 seconds of no new events
            await self.reload_config()

Use cases:
    - Configuration file change batching
    - Search input debouncing
    - Index rebuild after multiple file changes
    - Expensive operation deduplication

Moleculer.js compatible:
    - event.debounce: time in milliseconds
    - Non-blocking execution (returns immediately)
    - Timer-based cancellation on new events
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from moleculerpy.middleware.base import Middleware

if TYPE_CHECKING:
    from moleculerpy.context import Context
    from moleculerpy.protocols import EventProtocol

__all__ = ["DebounceMiddleware"]

# Type alias for event handlers
HandlerType = Callable[["Context"], Awaitable[Any]]


class DebounceMiddleware(Middleware):
    """Middleware for debouncing event handlers.

    Defers event execution until a configurable "quiet period" passes
    without any new events of the same type. Each new event resets the
    timer, ensuring only the last event in a burst is processed.

    This is useful for:
        - Batching configuration changes
        - Deduplicating rapid-fire events
        - Reducing load from frequent updates

    Attributes:
        logger: Logger for debounce events
    """

    __slots__ = ("_pending_tasks", "_timers", "logger")

    def __init__(self, logger: logging.Logger | None = None) -> None:
        """Initialize the DebounceMiddleware.

        Args:
            logger: Optional logger for debounce events
        """
        super().__init__()
        self.logger = logger or logging.getLogger("moleculerpy.middleware.debounce")
        # Track active timers and tasks for cleanup
        self._timers: dict[str, asyncio.TimerHandle | None] = {}
        self._pending_tasks: set[asyncio.Task[None]] = set()

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return "DebounceMiddleware()"

    async def stopped(self) -> None:
        """Cleanup on broker stop.

        Cancels all pending timers and waits for active tasks to complete.
        This prevents resource leaks when the broker shuts down.
        """
        # Cancel all active timers
        for event_name, timer_handle in list(self._timers.items()):
            if timer_handle is not None:
                timer_handle.cancel()
                self.logger.debug(f"Cancelled debounce timer for '{event_name}'")
        self._timers.clear()

        # Cancel pending tasks
        for task in list(self._pending_tasks):
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._pending_tasks.clear()
        self.logger.debug("DebounceMiddleware cleanup complete")

    async def local_event(
        self,
        next_handler: HandlerType,
        event: EventProtocol,
    ) -> HandlerType:
        """Wrap local event with debounce logic.

        If event.debounce > 0, wraps the handler to defer execution.
        Otherwise returns the original handler unchanged.

        Args:
            next_handler: The next handler in the chain
            event: The event being registered

        Returns:
            Wrapped handler with debounce or original handler
        """
        # Get debounce time from event config
        debounce_ms = getattr(event, "debounce", 0)

        # If no debounce configured, return original handler
        if not debounce_ms or debounce_ms <= 0:
            return next_handler

        event_name = getattr(event, "name", str(event))
        debounce_sec = debounce_ms / 1000.0

        self.logger.debug(f"Debounce enabled for event '{event_name}' ({debounce_ms}ms)")

        # Initialize timer tracking for this event
        self._timers[event_name] = None

        # Closure state for last context
        last_ctx: Context | None = None

        async def execute_handler() -> None:
            """Execute the actual handler after debounce period."""
            nonlocal last_ctx
            self._timers[event_name] = None

            if last_ctx is not None:
                ctx_to_use = last_ctx
                last_ctx = None
                try:
                    await next_handler(ctx_to_use)
                except Exception as err:
                    # Log error and emit internal event for observability
                    self.logger.error(f"Error in debounced handler for '{event_name}': {err}")
                    # Try to emit error event if broker available
                    broker = getattr(ctx_to_use, "broker", None)
                    if broker is not None and hasattr(broker, "broadcast_local"):
                        try:
                            await broker.broadcast_local(
                                "$debounce.error",
                                {
                                    "event": event_name,
                                    "error": str(err),
                                    "errorType": type(err).__name__,
                                },
                            )
                        except Exception as emit_err:
                            self.logger.warning(
                                "Failed to emit $debounce.error for '%s': %s",
                                event_name,
                                emit_err,
                            )

        def schedule_execution() -> None:
            """Schedule handler execution after debounce period."""
            # Create a task to run the async handler
            task = asyncio.create_task(execute_handler(), name=f"moleculerpy:debounce:{event_name}")
            # Track task for cleanup
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)

        async def debounce_wrapper(ctx: Context) -> None:
            """Debounce wrapper that defers execution."""
            nonlocal last_ctx

            # Store the latest context
            last_ctx = ctx

            # Cancel previous timer if exists
            current_timer = self._timers.get(event_name)
            if current_timer is not None:
                current_timer.cancel()
                self.logger.debug(f"Debounce timer reset for '{event_name}'")

            # Schedule new execution after debounce period
            loop = asyncio.get_running_loop()
            new_timer = loop.call_later(debounce_sec, schedule_execution)
            self._timers[event_name] = new_timer

            # Return immediately (non-blocking)

        return debounce_wrapper
