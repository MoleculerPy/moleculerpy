"""Bulkhead Middleware for the MoleculerPy framework.

This module implements the Bulkhead pattern to limit concurrent execution
of actions, preventing resource exhaustion and providing backpressure.

Features:
    - Per-action concurrency limiting (semaphore-like)
    - Optional queue with configurable max size
    - FIFO queue processing (fair ordering)
    - QueueIsFullError when queue exceeds limit (retryable, HTTP 429)

Example usage:
    from moleculerpy import ServiceBroker, Settings
    from moleculerpy.middleware import BulkheadMiddleware

    settings = Settings(
        middlewares=[
            BulkheadMiddleware(
                concurrency=3,       # Max 3 concurrent requests per action
                max_queue_size=10,   # Max 10 queued requests
            )
        ]
    )

    broker = ServiceBroker(id="my-node", settings=settings)

Per-action override:
    @action(bulkhead={"concurrency": 1, "max_queue_size": 5})
    async def critical_action(ctx):
        # This action has its own isolated bulkhead
        pass

Moleculer.js compatible:
    - Same configuration options (concurrency, maxQueueSize)
    - QueueIsFullError on queue overflow
    - Per-action isolated bulkheads
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from moleculerpy.errors import QueueIsFullError
from moleculerpy.middleware.base import HandlerType, Middleware

if TYPE_CHECKING:
    from moleculerpy.context import Context
    from moleculerpy.middleware.metrics import Counter, Gauge, MetricRegistry
    from moleculerpy.protocols import ActionProtocol

__all__ = ["BulkheadConfig", "BulkheadMiddleware"]


@dataclass(frozen=True, slots=True)
class BulkheadConfig:
    """Configuration for bulkhead behavior.

    Attributes:
        enabled: Whether bulkhead is enabled for this action
        concurrency: Maximum concurrent requests allowed
        max_queue_size: Maximum queue size (0 = unlimited)
    """

    enabled: bool = True
    concurrency: int = 3
    max_queue_size: int = 100


@dataclass(slots=True)
class QueueItem:
    """Item in the bulkhead queue.

    Attributes:
        ctx: The request context
        future: Future to resolve when request can proceed
    """

    ctx: Context
    future: asyncio.Future[None]


@dataclass
class BulkheadState:
    """Per-action bulkhead state.

    Attributes:
        in_flight: Current number of executing requests
        queue: FIFO queue of pending requests
        lock: Lock for thread-safe state mutations
    """

    in_flight: int = 0
    queue: deque[QueueItem] = field(default_factory=deque)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class BulkheadMiddleware(Middleware):
    """Middleware that limits concurrent execution of actions.

    The Bulkhead pattern isolates failures by limiting concurrency,
    preventing one slow action from consuming all resources.

    Attributes:
        default_concurrency: Default max concurrent requests
        default_max_queue_size: Default max queue size
        enabled: Whether bulkhead is enabled by default
    """

    __slots__ = (
        "_bulkhead_in_flight",
        "_bulkhead_queue_size",
        "_bulkhead_rejected_total",
        "_states",
        "default_concurrency",
        "default_max_queue_size",
        "enabled",
        "registry",
    )

    def __init__(
        self,
        concurrency: int = 3,
        max_queue_size: int = 100,
        enabled: bool = True,
        registry: MetricRegistry | None = None,
    ) -> None:
        """Initialize the BulkheadMiddleware.

        Args:
            concurrency: Default max concurrent requests per action
            max_queue_size: Default max queue size per action (0 = unlimited)
            enabled: Whether bulkhead is enabled by default
            registry: Optional MetricRegistry for collecting bulkhead metrics
        """
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        if max_queue_size < 0:
            raise ValueError("max_queue_size must be non-negative")

        self.default_concurrency = concurrency
        self.default_max_queue_size = max_queue_size
        self.enabled = enabled
        self.registry = registry
        self._states: dict[str, BulkheadState] = {}

        # Metrics (created when broker starts)
        self._bulkhead_in_flight: Gauge | None = None
        self._bulkhead_queue_size: Gauge | None = None
        self._bulkhead_rejected_total: Counter | None = None

    def broker_created(self, broker: Any) -> None:
        """Initialize metrics when broker is created.

        Args:
            broker: The broker instance
        """
        if self.registry is None:
            return

        # Create bulkhead metrics
        self._bulkhead_in_flight = self.registry.gauge(
            "moleculer_bulkhead_in_flight",
            "Number of requests currently being executed",
            label_names=("action",),
        )

        self._bulkhead_queue_size = self.registry.gauge(
            "moleculer_bulkhead_queue_size",
            "Number of requests currently queued",
            label_names=("action",),
        )

        self._bulkhead_rejected_total = self.registry.counter(
            "moleculer_bulkhead_rejected_total",
            "Total number of requests rejected due to queue overflow",
            label_names=("action",),
        )

    def _get_config(self, action: ActionProtocol) -> BulkheadConfig:
        """Get bulkhead configuration for an action.

        Merges action-level config with middleware defaults.

        Args:
            action: The action to get config for

        Returns:
            BulkheadConfig with merged settings
        """
        # Get action-level bulkhead config if defined
        action_config = getattr(action, "bulkhead", None)

        if action_config is None:
            return BulkheadConfig(
                enabled=self.enabled,
                concurrency=self.default_concurrency,
                max_queue_size=self.default_max_queue_size,
            )

        # Merge with defaults
        if isinstance(action_config, dict):
            return BulkheadConfig(
                enabled=action_config.get("enabled", self.enabled),
                concurrency=action_config.get("concurrency", self.default_concurrency),
                max_queue_size=action_config.get("max_queue_size", self.default_max_queue_size),
            )

        # If it's already a BulkheadConfig, use it directly
        if isinstance(action_config, BulkheadConfig):
            return action_config

        return BulkheadConfig(
            enabled=self.enabled,
            concurrency=self.default_concurrency,
            max_queue_size=self.default_max_queue_size,
        )

    def _get_state(self, action_name: str) -> BulkheadState:
        """Get or create bulkhead state for an action.

        Args:
            action_name: The action name

        Returns:
            BulkheadState for the action
        """
        if action_name not in self._states:
            self._states[action_name] = BulkheadState()
        return self._states[action_name]

    async def _process_queue(
        self,
        state: BulkheadState,
        config: BulkheadConfig,
    ) -> None:
        """Process queued requests when capacity becomes available.

        Args:
            state: The bulkhead state
            config: The bulkhead configuration
        """
        async with state.lock:
            while state.queue and state.in_flight < config.concurrency:
                item = state.queue.popleft()
                if not item.future.done():
                    state.in_flight += 1
                    item.future.set_result(None)

    async def local_action(self, next_handler: HandlerType[Any], action: Any) -> HandlerType[Any]:
        """Wrap local action with bulkhead protection.

        Args:
            next_handler: The next handler in the chain
            action: The action being registered

        Returns:
            Wrapped handler with bulkhead protection
        """
        config = self._get_config(action)

        # If bulkhead is disabled for this action, pass through
        if not config.enabled:
            return next_handler

        action_name: str = getattr(action, "name", str(action))
        state = self._get_state(action_name)

        async def bulkhead_handler(ctx: Context) -> Any:
            """Handler with bulkhead protection.

            This implementation uses explicit flags instead of scope inspection
            to track whether the request was queued. The lock is acquired once
            in the finally block to both decrement in_flight and process the
            queue atomically.
            """
            # Track whether we're queued (explicit flag, not "future" in dir())
            queued = False
            future: asyncio.Future[None] | None = None
            item: QueueItem | None = None

            async with state.lock:
                # Fast path: under concurrency limit
                if state.in_flight < config.concurrency:
                    state.in_flight += 1

                    # Update in_flight metric
                    if self._bulkhead_in_flight:
                        self._bulkhead_in_flight.set(
                            state.in_flight,
                            labels={"action": action_name},
                        )
                else:
                    # Check queue size limit
                    if config.max_queue_size > 0 and len(state.queue) >= config.max_queue_size:
                        # Update rejected counter
                        if self._bulkhead_rejected_total:
                            self._bulkhead_rejected_total.inc(labels={"action": action_name})

                        raise QueueIsFullError(
                            action_name=action_name,
                            queue_size=len(state.queue),
                        )

                    # Queue the request
                    loop = asyncio.get_running_loop()
                    future = loop.create_future()
                    item = QueueItem(ctx=ctx, future=future)
                    state.queue.append(item)
                    queued = True

                    # Update queue_size metric
                    if self._bulkhead_queue_size:
                        self._bulkhead_queue_size.set(
                            len(state.queue),
                            labels={"action": action_name},
                        )

            # Wait for turn if queued (using explicit flag)
            if queued:
                assert future is not None and item is not None
                try:
                    await future
                except asyncio.CancelledError:
                    # Handle cancellation while waiting in queue
                    async with state.lock:
                        try:
                            state.queue.remove(item)
                            # Successfully removed: we never got a slot,
                            # so don't decrement in_flight

                            # Update queue_size metric
                            if self._bulkhead_queue_size:
                                self._bulkhead_queue_size.set(
                                    len(state.queue),
                                    labels={"action": action_name},
                                )
                        except ValueError:
                            # Already removed by _process_queue: we got our slot
                            # and were about to execute, so decrement in_flight
                            state.in_flight -= 1

                            # Update in_flight metric
                            if self._bulkhead_in_flight:
                                self._bulkhead_in_flight.set(
                                    state.in_flight,
                                    labels={"action": action_name},
                                )
                    raise

            try:
                # Execute the handler
                return await next_handler(ctx)
            finally:
                # Release slot and process queue under single lock
                # (avoids double lock acquisition)
                async with state.lock:
                    state.in_flight -= 1

                    # Process queue inline instead of calling _process_queue
                    while state.queue and state.in_flight < config.concurrency:
                        next_item = state.queue.popleft()
                        if not next_item.future.done():
                            state.in_flight += 1
                            next_item.future.set_result(None)

                    # Update metrics after processing queue
                    if self._bulkhead_in_flight:
                        self._bulkhead_in_flight.set(
                            state.in_flight,
                            labels={"action": action_name},
                        )

                    if self._bulkhead_queue_size:
                        self._bulkhead_queue_size.set(
                            len(state.queue),
                            labels={"action": action_name},
                        )

        return bulkhead_handler

    async def remote_action(self, next_handler: HandlerType[Any], action: Any) -> HandlerType[Any]:
        """Wrap remote action with bulkhead protection.

        Remote actions also benefit from bulkhead to limit outgoing requests.

        Args:
            next_handler: The next handler in the chain
            action: The action being registered

        Returns:
            Wrapped handler with bulkhead protection
        """
        # Use same logic as local_action
        return await self.local_action(next_handler, action)

    def get_stats(self, action_name: str) -> dict[str, int]:
        """Get current bulkhead statistics for an action.

        Args:
            action_name: The action name

        Returns:
            Dictionary with in_flight and queue_size
        """
        state = self._states.get(action_name)
        if state is None:
            return {"in_flight": 0, "queue_size": 0}
        return {"in_flight": state.in_flight, "queue_size": len(state.queue)}

    def reset(self, action_name: str | None = None) -> None:
        """Reset bulkhead state (for testing).

        Args:
            action_name: Specific action to reset, or None for all
        """
        if action_name is None:
            self._states.clear()
        elif action_name in self._states:
            del self._states[action_name]
