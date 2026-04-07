"""Service discovery mechanism for the MoleculerPy framework.

This module provides the Discoverer class which handles periodic heartbeat
broadcasting, remote node health checking, and offline node cleanup.

Matches Node.js Moleculer base discoverer (registry/discoverers/base.js):
- beat(): periodic heartbeat broadcast
- checkRemoteNodes(): mark unavailable nodes whose heartbeat timed out
- checkOfflineNodes(): remove nodes silent for cleanOfflineNodesTimeout
"""

import asyncio
import random
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .broker import ServiceBroker
    from .transit import Transit

# Default timeout for removing completely offline nodes (seconds).
# Matches Node.js Moleculer: `cleanOfflineNodesTimeout: 10 * 60` (10 minutes).
CLEAN_OFFLINE_NODES_TIMEOUT: float = 600.0

# Interval for the offline node cleanup timer (seconds).
OFFLINE_CHECK_INTERVAL: float = 60.0


class Discoverer:
    """Handles service discovery and cluster topology maintenance.

    Manages three periodic tasks (matching Node.js Moleculer base discoverer):
    1. beat — broadcast heartbeat at heartbeat_interval ± 500ms jitter
    2. check_remote_nodes — mark nodes unavailable after heartbeat_timeout
    3. check_offline_nodes — remove nodes silent for 10+ minutes

    Note: Task creation is deferred to the start() method to avoid
    "no running event loop" errors when instantiated outside async context.
    """

    _DISCOVER_COOLDOWN: float = 30.0  # Seconds before retrying DISCOVER for same node

    def __init__(self, broker: "ServiceBroker") -> None:
        self.broker = broker
        self.transit: Transit = broker.transit
        self._tasks: list[asyncio.Task[None]] = []
        self._started: bool = False

        self.heartbeat_interval = broker.settings.heartbeat_interval
        self.heartbeat_timeout = broker.settings.heartbeat_timeout

        # Rate-limit discover_node: track pending DISCOVER requests with timestamps.
        # Entries expire after _DISCOVER_COOLDOWN seconds so retries are possible
        # if the INFO response is lost. Cleared in clear_discover_pending().
        self._discover_pending: dict[str, float] = {}

    async def start(self) -> None:
        """Start the discoverer and begin periodic tasks.

        Safe to call multiple times — subsequent calls are no-ops.
        """
        if self._started:
            return
        self._started = True
        self._setup_timers()

    def _setup_timers(self) -> None:
        """Set up periodic tasks for service discovery.

        Creates three tasks matching Node.js startHeartbeatTimers():
        1. Heartbeat broadcast (heartbeat_interval ± 500ms jitter)
        2. Remote node check (every heartbeat_timeout seconds)
        3. Offline node cleanup (every 60 seconds)
        """
        if self.heartbeat_interval <= 0:
            return

        # Timer 1: periodic heartbeat
        async def periodic_beat() -> None:
            try:
                while True:
                    jitter = random.uniform(-0.5, 0.5)
                    sleep_time = max(0.1, self.heartbeat_interval + jitter)
                    await asyncio.sleep(sleep_time)
                    try:
                        await self.transit.beat()
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        self.broker.logger.error(f"Heartbeat failed, will retry: {e}")
            except asyncio.CancelledError:
                pass

        # Timer 2: check remote node heartbeats (matches Node.js checkRemoteNodes)
        async def periodic_check_nodes() -> None:
            try:
                while True:
                    await asyncio.sleep(self.heartbeat_timeout)
                    try:
                        self.check_remote_nodes()
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        self.broker.logger.error(f"Remote node check failed: {e}")
            except asyncio.CancelledError:
                pass

        # Timer 3: clean offline nodes (matches Node.js checkOfflineNodes)
        async def periodic_clean_offline() -> None:
            try:
                while True:
                    await asyncio.sleep(OFFLINE_CHECK_INTERVAL)
                    try:
                        self.check_offline_nodes()
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        self.broker.logger.error(f"Offline node cleanup failed: {e}")
            except asyncio.CancelledError:
                pass

        self._tasks.append(asyncio.create_task(periodic_beat(), name="moleculerpy:discoverer-beat"))
        self._tasks.append(
            asyncio.create_task(periodic_check_nodes(), name="moleculerpy:discoverer-check-nodes")
        )
        self._tasks.append(
            asyncio.create_task(
                periodic_clean_offline(), name="moleculerpy:discoverer-clean-offline"
            )
        )

    # ------------------------------------------------------------------
    # Discovery methods (moved from Transit for SRP)
    # ------------------------------------------------------------------

    async def discover_all(self) -> None:
        """Broadcast DISCOVER to find all nodes in the cluster."""
        from .packet import Packet, Topic  # noqa: PLC0415

        await self.transit.publish(Packet(Topic.DISCOVER, None, {}))

    async def discover_node(self, node_id: str) -> None:
        """Send targeted DISCOVER to a specific node.

        Matches Node.js base discoverer discoverNode(nodeID).
        """
        from .packet import Packet, Topic  # noqa: PLC0415

        await self.transit.publish(Packet(Topic.DISCOVER, node_id, {}))

    async def request_discovery(self, sender: str, reason: str) -> None:
        """Rate-limited discovery request. Prevents DISCOVER flooding.

        Entries in _discover_pending expire after _DISCOVER_COOLDOWN seconds
        so retries are possible if the INFO response is lost.
        """
        now = time.time()
        last = self._discover_pending.get(sender)
        if last is not None and (now - last) < self._DISCOVER_COOLDOWN:
            return  # Cooldown active — skip

        self._discover_pending[sender] = now
        self.broker.logger.debug(f"Heartbeat from {reason} node '{sender}', requesting INFO")
        try:
            await self.discover_node(sender)
        except Exception as e:
            self.broker.logger.warning(f"Failed to send targeted DISCOVER to '{sender}': {e}")
            self._discover_pending.pop(sender, None)

    def clear_discover_pending(self, node_id: str) -> None:
        """Clear pending discover entry — called when INFO received."""
        self._discover_pending.pop(node_id, None)

    # ------------------------------------------------------------------
    # Health check timers
    # ------------------------------------------------------------------

    def check_remote_nodes(self) -> None:
        """Check all registered remote nodes are available.

        Matches Node.js Moleculer base.js checkRemoteNodes():
        Iterates all nodes. If a remote, available node hasn't sent a heartbeat
        within heartbeat_timeout seconds, mark it as disconnected.
        """
        now = time.time()
        node_catalog = self.transit.node_catalog

        for node_id, node in list(node_catalog.nodes.items()):
            # Skip local node and already-unavailable nodes
            if getattr(node, "local", False) or not getattr(node, "available", True):
                continue

            last_hb = getattr(node, "lastHeartbeatTime", None)
            if last_hb is None:
                # Not received the first heartbeat yet — initialize
                node.lastHeartbeatTime = now
                continue

            if now - last_hb > self.heartbeat_timeout:
                self.broker.logger.warning(
                    f"Heartbeat not received from '{node_id}' node "
                    f"(last: {now - last_hb:.1f}s ago, timeout: {self.heartbeat_timeout}s)"
                )
                node_catalog.disconnect_node(node_id, unexpected=True)

        # Evict stale entries from _discover_pending (prevents unbounded growth)
        expired = [
            k for k, ts in self._discover_pending.items() if now - ts > self._DISCOVER_COOLDOWN
        ]
        for k in expired:
            self._discover_pending.pop(k, None)

    def check_offline_nodes(self) -> None:
        """Check offline nodes. Remove which are older than 10 minutes.

        Matches Node.js Moleculer base.js checkOfflineNodes():
        Iterates all nodes. If a remote, unavailable node hasn't sent a heartbeat
        for cleanOfflineNodesTimeout (default 600s), remove it from registry.
        """
        now = time.time()
        node_catalog = self.transit.node_catalog

        for node_id, node in list(node_catalog.nodes.items()):
            # Only process offline remote nodes
            if getattr(node, "local", False) or getattr(node, "available", True):
                continue

            last_hb = getattr(node, "lastHeartbeatTime", None)
            if last_hb is None:
                node.lastHeartbeatTime = now
                continue

            if now - last_hb > CLEAN_OFFLINE_NODES_TIMEOUT:
                self.broker.logger.warning(
                    f"Removing offline '{node_id}' node from registry "
                    f"(silent for {now - last_hb:.0f}s)"
                )
                node_catalog.remove_node(node_id)

    async def stop(self) -> None:
        """Stop the discoverer and cancel all running tasks."""
        if not self._started:
            return
        self._started = False

        for task in self._tasks:
            if not task.done() and not task.cancelled():
                task.cancel()

        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True), timeout=1.0
                )
            except TimeoutError:
                pass
            except Exception:
                pass
            finally:
                self._tasks.clear()
