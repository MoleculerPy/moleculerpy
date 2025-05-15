"""Latency monitoring for the MoleculerPy framework.

This module provides the LatencyMonitor class for measuring network latency
between nodes using PING/PONG protocol, following Moleculer.js patterns.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import deque
from typing import TYPE_CHECKING, Any

from .packet import Packet, Topic

if TYPE_CHECKING:
    from .broker import ServiceBroker


class LatencyMonitor:
    """Monitors network latency between nodes using PING/PONG protocol.

    Implements Master-Slave architecture where one broker (master) sends
    periodic PING packets to all nodes and collects PONG responses to
    measure round-trip latency.

    Attributes:
        broker: Service broker instance
        is_master: Whether this node is the latency master
        host_latency: Average latency per hostname in milliseconds
        ping_interval: Interval between ping cycles in seconds
    """

    # Default configuration
    DEFAULT_PING_INTERVAL: float = 10.0  # seconds
    DEFAULT_SAMPLE_COUNT: int = 5  # samples for averaging
    DEFAULT_PING_TIMEOUT: float = 5.0  # seconds

    def __init__(
        self,
        broker: ServiceBroker,
        is_master: bool = False,
        ping_interval: float | None = None,
        sample_count: int | None = None,
    ) -> None:
        """Initialize LatencyMonitor.

        Args:
            broker: Service broker instance
            is_master: Whether this node should act as latency master
            ping_interval: Interval between ping cycles (default: 10s)
            sample_count: Number of samples for averaging (default: 5)
        """
        self.broker = broker
        self._is_master = is_master
        self._ping_interval = ping_interval or self.DEFAULT_PING_INTERVAL
        self._sample_count = sample_count or self.DEFAULT_SAMPLE_COUNT

        # Latency storage: hostname -> average_ms
        self._host_latency: dict[str, float] = {}

        # Sample storage: hostname -> deque[ms] (O(1) append, auto-bounded)
        # Using deque with maxlen for efficient sliding window
        self._samples: dict[str, deque[float]] = {}
        self._sample_count_val = self._sample_count  # Cache for lambda

        # Pending pings: ping_id -> (timestamp_ms, node_id)
        self._pending_pings: dict[str, tuple[float, str]] = {}

        # Background task
        self._ping_task: asyncio.Task[None] | None = None
        self._started: bool = False

    @property
    def is_master(self) -> bool:
        """Whether this node is the latency master."""
        return self._is_master

    @is_master.setter
    def is_master(self, value: bool) -> None:
        """Set master status and start/stop ping cycle accordingly."""
        was_master = self._is_master
        self._is_master = value

        # Start/stop ping cycle based on master status
        if value and not was_master and self._started:
            self._start_ping_cycle()
        elif not value and was_master:
            self._stop_ping_cycle()

    @property
    def host_latency(self) -> dict[str, float]:
        """Get average latency per hostname (read-only copy)."""
        return self._host_latency.copy()

    def get_host_latency(self, hostname: str | None) -> float | None:
        """Get average latency for a specific host.

        Args:
            hostname: Host identifier (usually node hostname)

        Returns:
            Average latency in milliseconds, or None if not available
        """
        if hostname is None:
            return None
        return self._host_latency.get(hostname)

    def get_node_latency(self, node_id: str) -> float | None:
        """Get latency for a node by ID.

        Uses node's hostname from node_catalog to lookup latency.

        Args:
            node_id: Node identifier

        Returns:
            Average latency in milliseconds, or None if not available
        """
        node = self.broker.node_catalog.get_node(node_id)
        if node and node.hostname:
            return self.get_host_latency(node.hostname)
        # Fallback to node_id as hostname
        return self.get_host_latency(node_id)

    async def start(self) -> None:
        """Start the latency monitor.

        If this node is master, starts the periodic ping cycle.
        """
        if self._started:
            return
        self._started = True

        if self._is_master:
            self._start_ping_cycle()

        self.broker.logger.debug("LatencyMonitor started (master=%s)", self._is_master)

    async def stop(self) -> None:
        """Stop the latency monitor and cancel background tasks."""
        self._stop_ping_cycle()
        self._pending_pings.clear()
        self._started = False

        self.broker.logger.debug("LatencyMonitor stopped")

    def _start_ping_cycle(self) -> None:
        """Start the periodic ping cycle task."""
        if self._ping_task is not None:
            return

        async def ping_cycle() -> None:
            """Periodic task to send PING to all nodes."""
            try:
                while True:
                    await asyncio.sleep(self._ping_interval)
                    await self._send_ping_to_all()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.broker.logger.error("Error in ping cycle: %s", e)

        self._ping_task = asyncio.create_task(ping_cycle(), name="moleculerpy:latency-ping")

    def _stop_ping_cycle(self) -> None:
        """Stop the periodic ping cycle task."""
        if self._ping_task is not None:
            self._ping_task.cancel()
            self._ping_task = None

    async def _send_ping_to_all(self) -> None:
        """Send PING to all remote nodes in parallel."""
        nodes = self.broker.node_catalog.nodes
        local_id = self.broker.nodeID

        # Collect all ping tasks for parallel execution (~10x faster for N nodes)
        tasks = [
            self.send_ping(node_id)
            for node_id, node in nodes.items()
            if node_id != local_id and node.available
        ]

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def send_ping(self, node_id: str) -> str:
        """Send a PING packet to a specific node.

        Args:
            node_id: Target node ID

        Returns:
            Ping ID for correlation
        """
        ping_id = str(uuid.uuid4())
        timestamp = time.time() * 1000  # milliseconds

        # Track pending ping
        self._pending_pings[ping_id] = (timestamp, node_id)

        payload = {
            "id": ping_id,
            "time": timestamp,
            "sender": self.broker.nodeID,
        }

        await self.broker.transit.publish(Packet(Topic.PING, node_id, payload))
        return ping_id

    async def handle_ping(self, sender: str, payload: dict[str, Any]) -> None:
        """Handle incoming PING packet by sending PONG response.

        Args:
            sender: Node ID that sent the PING
            payload: PING packet payload containing id and time
        """
        ping_id = payload.get("id")
        ping_time = payload.get("time")

        if not ping_id or ping_time is None:
            return

        # Send PONG response
        pong_payload = {
            "id": ping_id,
            "time": ping_time,
            "arrived": time.time() * 1000,
            "sender": self.broker.nodeID,
        }

        await self.broker.transit.publish(Packet(Topic.PONG, sender, pong_payload))

    async def handle_pong(self, sender: str, payload: dict[str, Any]) -> None:
        """Handle incoming PONG packet and calculate latency.

        Args:
            sender: Node ID that sent the PONG
            payload: PONG packet payload containing id, time, and arrived
        """
        ping_id = payload.get("id")
        if not ping_id:
            return

        # Get pending ping data
        pending = self._pending_pings.pop(ping_id, None)
        if not pending:
            return

        original_time, expected_node = pending

        # Verify sender matches expected node
        if sender != expected_node:
            self.broker.logger.warning(
                "PONG from unexpected node: expected=%s, got=%s", expected_node, sender
            )
            return

        # Calculate round-trip time
        now = time.time() * 1000
        elapsed_ms = now - original_time

        # Get hostname for the node
        node = self.broker.node_catalog.get_node(sender)
        hostname = node.hostname if node and node.hostname else sender

        # Add sample and update average
        self._add_sample(hostname, elapsed_ms)

        self.broker.logger.debug(
            "Latency to %s: %.2fms (avg: %.2fms)",
            hostname,
            elapsed_ms,
            self._host_latency.get(hostname, 0),
        )

    def _add_sample(self, hostname: str, latency_ms: float) -> None:
        """Add a latency sample and update average.

        Uses deque with maxlen for O(1) append with automatic eviction
        of oldest samples (vs O(n) for list.pop(0)).

        Args:
            hostname: Host identifier
            latency_ms: Measured latency in milliseconds
        """
        # Get or create bounded deque for this hostname
        if hostname not in self._samples:
            self._samples[hostname] = deque(maxlen=self._sample_count)

        samples = self._samples[hostname]
        samples.append(latency_ms)  # O(1), auto-evicts oldest if full

        # Update average
        self._host_latency[hostname] = sum(samples) / len(samples)

    def clear_samples(self, hostname: str | None = None) -> None:
        """Clear latency samples.

        Args:
            hostname: Specific host to clear, or None to clear all
        """
        if hostname:
            self._samples.pop(hostname, None)
            self._host_latency.pop(hostname, None)
        else:
            self._samples.clear()
            self._host_latency.clear()

    def _get_or_create_samples(self, hostname: str) -> deque[float]:
        """Get or create a sample deque for a hostname.

        Args:
            hostname: Host identifier

        Returns:
            Bounded deque for samples
        """
        if hostname not in self._samples:
            self._samples[hostname] = deque(maxlen=self._sample_count)
        return self._samples[hostname]
