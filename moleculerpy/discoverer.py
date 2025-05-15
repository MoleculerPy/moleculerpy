"""Service discovery mechanism for the MoleculerPy framework.

This module provides the Discoverer class which handles periodic heartbeat
broadcasting to maintain cluster topology awareness.
"""

import asyncio
import random
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .broker import ServiceBroker
    from .transit import Transit


class Discoverer:
    """Handles service discovery and cluster topology maintenance.

    The Discoverer manages periodic heartbeat broadcasts to announce
    node presence and maintain awareness of the cluster topology.

    Note: Task creation is deferred to the start() method to avoid
    "no running event loop" errors when instantiated outside async context.
    """

    def __init__(self, broker: "ServiceBroker") -> None:
        """Initialize the discoverer.

        Args:
            broker: Service broker instance

        Note:
            Tasks are NOT created here. Call start() after entering
            async context to begin heartbeat broadcasts.
        """
        self.broker = broker
        self.transit: Transit = broker.transit
        self._tasks: list[asyncio.Task[None]] = []
        self._started: bool = False

        # Use configurable heartbeat interval from settings
        self.heartbeat_interval = broker.settings.heartbeat_interval

    async def start(self) -> None:
        """Start the discoverer and begin periodic heartbeat broadcasts.

        This method should be called after the broker's transit is connected.
        Safe to call multiple times - subsequent calls are no-ops.
        """
        if self._started:
            return
        self._started = True
        self._setup_timers()

    def _setup_timers(self) -> None:
        """Set up periodic tasks for service discovery.

        This is called from start() after we're in an async context.
        """
        heartbeat_interval = self.heartbeat_interval

        async def periodic_beat() -> None:
            """Send periodic heartbeat broadcasts with jitter.

            Jitter (±500ms) prevents thundering herd when many nodes
            start simultaneously and would otherwise send heartbeats
            at exactly the same intervals.
            """
            try:
                while True:
                    # Add jitter of ±500ms to prevent thundering herd
                    jitter = random.uniform(-0.5, 0.5)
                    sleep_time = max(0.1, heartbeat_interval + jitter)
                    await asyncio.sleep(sleep_time)
                    await self.transit.beat()
            except asyncio.CancelledError:
                # Task cancellation is expected during shutdown
                pass
            except Exception as e:
                # Log the error but don't stop the heartbeat
                if hasattr(self.broker, "logger"):
                    self.broker.logger.error(f"Error in periodic beat: {e}")

        # Create and track the heartbeat task
        task = asyncio.create_task(periodic_beat(), name="moleculerpy:discoverer-beat")
        self._tasks.append(task)

    async def stop(self) -> None:
        """Stop the discoverer and cancel all running tasks.

        This method gracefully cancels all tasks created by the discoverer
        and waits for them to complete or timeout.
        """
        if not self._tasks:
            return

        # Cancel all tasks
        for task in self._tasks:
            if not task.done() and not task.cancelled():
                task.cancel()

        # Wait for all tasks to complete with timeout
        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True), timeout=1.0
                )
            except TimeoutError:
                # Some tasks didn't finish in time, but that's acceptable
                pass
            except Exception:
                # Ignore exceptions from cancelled tasks
                pass
            finally:
                self._tasks.clear()
