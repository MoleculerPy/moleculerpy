"""Local event bus for internal framework events.

This module provides an EventEmitter-like class for broadcasting internal events
($ prefixed) within the local broker instance. These events are NOT sent over
the network transporter - they are for local coordination and monitoring only.

Internal events include:
- $node.connected, $node.disconnected, $node.updated
- $broker.started, $broker.stopped, $broker.error
- $transporter.connected, $transporter.disconnected
- $circuit-breaker.opened, $circuit-breaker.closed

Compatible with Moleculer.js internal event system.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections import defaultdict
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from functools import lru_cache
from re import Pattern
from typing import Any

logger = logging.getLogger("moleculerpy.local_bus")

# Type alias for event handlers
EventHandler = Callable[[Any], Coroutine[Any, Any, None] | None]


@lru_cache(maxsize=256)
def _compile_pattern(pattern: str) -> Pattern[str] | None:
    """Compile wildcard pattern to regex (cached for performance).

    Args:
        pattern: Wildcard pattern (e.g., "$node.*")

    Returns:
        Compiled regex pattern or None if no wildcards
    """
    if "**" in pattern:
        # $** matches everything, $node.** matches $node.anything
        regex = "^" + re.escape(pattern).replace(r"\*\*", ".*") + "$"
        return re.compile(regex)
    elif "*" in pattern:
        # $node.* matches $node.connected (single segment)
        regex = "^" + re.escape(pattern).replace(r"\*", "[^.]*") + "$"
        return re.compile(regex)
    return None


@dataclass(frozen=True, slots=True)
class LocalBusOptions:
    """Configuration options for LocalBus.

    Attributes:
        wildcard: Enable wildcard pattern matching (e.g., "$node.*")
        max_listeners: Maximum listeners per event (0 = unlimited)
    """

    wildcard: bool = True
    max_listeners: int = 100


@dataclass(slots=True)
class EventRegistration:
    """Represents a registered event handler.

    Note: Not frozen because we need to track state in emit().

    Attributes:
        handler: The callback function
        once: Whether to remove after first invocation
    """

    handler: EventHandler
    once: bool = False


class LocalBus:
    """Internal event emitter for framework-level events.

    This is the MoleculerPy equivalent of Moleculer.js's `localBus` (EventEmitter2).
    It handles internal $ prefixed events that should not be broadcast over
    the network transporter.

    Features:
    - Wildcard pattern matching ($node.* matches $node.connected)
    - Async handler support
    - Once handlers (auto-remove after first call)
    - Max listeners limit

    Example:
        bus = LocalBus()

        async def on_connected(payload):
            print(f"Node connected: {payload['node'].id}")

        bus.on("$node.connected", on_connected)
        await bus.emit("$node.connected", {"node": node, "reconnected": False})
    """

    __slots__ = ("_all_handlers", "_handlers", "_options", "_wildcard_patterns")

    def __init__(self, options: LocalBusOptions | None = None) -> None:
        """Initialize the LocalBus.

        Args:
            options: Configuration options for the event bus
        """
        self._options = options or LocalBusOptions()
        self._handlers: dict[str, list[EventRegistration]] = defaultdict(list)
        self._all_handlers: list[EventRegistration] = []
        # Cache: maps wildcard patterns to their compiled regex
        self._wildcard_patterns: dict[str, Pattern[str]] = {}

    def on(self, event: str, handler: EventHandler) -> LocalBus:
        """Subscribe to an event.

        Args:
            event: Event name (supports wildcards like "$node.*")
            handler: Async or sync callback function

        Returns:
            Self for method chaining
        """
        if len(self._handlers[event]) >= self._options.max_listeners > 0:
            raise RuntimeError(
                f"Max listeners ({self._options.max_listeners}) exceeded for '{event}'"
            )

        self._handlers[event].append(EventRegistration(handler=handler, once=False))
        # Track wildcard patterns for optimized matching
        if self._options.wildcard and ("*" in event):
            compiled = _compile_pattern(event)
            if compiled:
                self._wildcard_patterns[event] = compiled
        return self

    def once(self, event: str, handler: EventHandler) -> LocalBus:
        """Subscribe to an event for a single invocation.

        The handler will be automatically removed after the first call.

        Args:
            event: Event name
            handler: Async or sync callback function

        Returns:
            Self for method chaining
        """
        self._handlers[event].append(EventRegistration(handler=handler, once=True))
        # Track wildcard patterns for optimized matching
        if self._options.wildcard and ("*" in event):
            compiled = _compile_pattern(event)
            if compiled:
                self._wildcard_patterns[event] = compiled
        return self

    def on_any(self, handler: EventHandler) -> LocalBus:
        """Subscribe to all events.

        Args:
            handler: Callback that receives (event_name, payload)

        Returns:
            Self for method chaining
        """
        self._all_handlers.append(EventRegistration(handler=handler, once=False))
        return self

    def off(self, event: str, handler: EventHandler | None = None) -> LocalBus:
        """Unsubscribe from an event.

        Args:
            event: Event name
            handler: Specific handler to remove (None removes all)

        Returns:
            Self for method chaining
        """
        if handler is None:
            self._handlers[event] = []
            # Remove from wildcard cache if no handlers left
            self._wildcard_patterns.pop(event, None)
        else:
            self._handlers[event] = [reg for reg in self._handlers[event] if reg.handler != handler]
            # Remove from wildcard cache if no handlers left
            if not self._handlers[event]:
                self._wildcard_patterns.pop(event, None)
        return self

    def off_all(self) -> LocalBus:
        """Remove all event handlers.

        Returns:
            Self for method chaining
        """
        self._handlers.clear()
        self._all_handlers.clear()
        self._wildcard_patterns.clear()
        return self

    def listeners(self, event: str) -> list[EventHandler]:
        """Get all listeners for an event.

        Args:
            event: Event name

        Returns:
            List of handler functions
        """
        return [reg.handler for reg in self._handlers.get(event, [])]

    def listener_count(self, event: str | None = None) -> int:
        """Get the number of listeners.

        Args:
            event: Event name (None for total count)

        Returns:
            Number of registered listeners
        """
        if event is None:
            return sum(len(regs) for regs in self._handlers.values())
        return len(self._handlers.get(event, []))

    async def emit(self, event: str, payload: Any = None) -> bool:
        """Emit an event to all matching handlers.

        Supports wildcard matching if enabled in options:
        - "$node.*" matches "$node.connected", "$node.disconnected"
        - "$**" matches all events

        Args:
            event: Event name
            payload: Data to pass to handlers

        Returns:
            True if any handlers were called
        """
        handlers_called = False
        handlers_to_remove: list[tuple[str, EventRegistration]] = []

        # Collect matching handlers
        matching_handlers: list[tuple[str, EventRegistration]] = []

        # Exact match
        for reg in self._handlers.get(event, []):
            matching_handlers.append((event, reg))

        # Wildcard matching (optimized: only check patterns with wildcards)
        if self._options.wildcard and self._wildcard_patterns:
            for pattern, compiled_regex in self._wildcard_patterns.items():
                if compiled_regex.match(event):
                    for reg in self._handlers.get(pattern, []):
                        matching_handlers.append((pattern, reg))

        # Call all matching handlers
        for pattern, reg in matching_handlers:
            handlers_called = True
            try:
                result = reg.handler(payload)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as err:
                # Silently ignore handler errors (like Moleculer.js) but log for debugging
                logger.debug("Handler error for event '%s': %s", event, err)

            if reg.once:
                handlers_to_remove.append((pattern, reg))

        # Call "any" handlers
        for reg in self._all_handlers:
            handlers_called = True
            try:
                result = reg.handler({"event": event, "payload": payload})
                if asyncio.iscoroutine(result):
                    await result
            except Exception as err:
                logger.debug("on_any handler error for event '%s': %s", event, err)

            if reg.once:
                self._all_handlers.remove(reg)

        # Remove "once" handlers
        for pattern, reg in handlers_to_remove:
            if reg in self._handlers[pattern]:
                self._handlers[pattern].remove(reg)

        return handlers_called

    def _matches_pattern(self, pattern: str, event: str) -> bool:
        """Check if an event matches a wildcard pattern.

        Supports:
        - "*" matches any single segment ($node.* → $node.connected)
        - "**" matches any segments ($** → any event)

        Args:
            pattern: Pattern with wildcards
            event: Actual event name

        Returns:
            True if the event matches the pattern
        """
        # Use cached compiled regex for performance
        compiled = self._wildcard_patterns.get(pattern)
        if compiled:
            return bool(compiled.match(event))

        # Fallback: compile on demand (for patterns not in _wildcard_patterns)
        compiled = _compile_pattern(pattern)
        if compiled:
            return bool(compiled.match(event))

        return pattern == event


# Singleton-like factory for testing
_default_bus: LocalBus | None = None


def get_local_bus() -> LocalBus:
    """Get or create the default LocalBus instance.

    Returns:
        LocalBus instance
    """
    global _default_bus  # noqa: PLW0603
    if _default_bus is None:
        _default_bus = LocalBus()
    return _default_bus


def reset_local_bus() -> None:
    """Reset the default LocalBus (for testing)."""
    global _default_bus  # noqa: PLW0603
    _default_bus = None
