"""Latency-based load balancing strategy for MoleculerPy.

This module implements the latency-aware load balancing strategy that selects
endpoints based on network latency measurements, following Moleculer.js patterns.
"""

from __future__ import annotations

import random
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from .base import BaseStrategy

if TYPE_CHECKING:
    from collections.abc import Sequence

    from ..context import Context
    from ..registry import Action


@runtime_checkable
class LatencyProvider(Protocol):
    """Protocol for latency data providers.

    Any object with get_host_latency method satisfies this protocol.
    Moleculer.js compatible: uses hostname for lookup, not node_id.
    """

    def get_host_latency(self, hostname: str | None) -> float | None:
        """Get latency for a host by hostname."""
        ...


class LatencyStrategy(BaseStrategy):
    """Load balancing strategy that selects endpoints based on network latency.

    This strategy samples random endpoints and selects the one with the lowest
    network latency. If an endpoint has latency below the threshold, it's selected
    immediately (fast path optimization).

    Compatible with Moleculer.js LatencyStrategy.

    Options:
        sampleCount (int): Number of random endpoints to sample (default: 5)
        lowLatency (float): Latency threshold for fast path selection in ms (default: 10.0)

    Requires:
        A LatencyProvider (typically LatencyMonitor) to be set via `set_latency_provider()`.
        Without it, falls back to random selection.

    Example:
        >>> strategy = LatencyStrategy({"sampleCount": 5, "lowLatency": 20.0})
        >>> strategy.set_latency_provider(latency_monitor)
        >>> endpoint = strategy.select(endpoints, ctx)
    """

    __slots__ = ("_latency_provider",)

    @staticmethod
    def _as_int(value: object, default: int) -> int:
        if isinstance(value, (int, float, str, bytes, bytearray)):
            try:
                return int(value)
            except (TypeError, ValueError):
                return default
        return default

    @staticmethod
    def _as_float(value: object, default: float) -> float:
        if isinstance(value, (int, float, str, bytes, bytearray)):
            try:
                return float(value)
            except (TypeError, ValueError):
                return default
        return default

    def __init__(self, opts: dict[str, object] | None = None) -> None:
        """Initialize LatencyStrategy.

        Args:
            opts: Strategy options (sampleCount, lowLatency)
        """
        super().__init__(opts)
        self._latency_provider: LatencyProvider | None = None

    def set_latency_provider(self, provider: LatencyProvider | None) -> None:
        """Set the latency data provider.

        Args:
            provider: Object implementing LatencyProvider protocol (e.g., LatencyMonitor)
        """
        self._latency_provider = provider

    def select(
        self,
        endpoints: Sequence[Action],
        ctx: Context | None = None,
    ) -> Action | None:
        """Select endpoint with lowest network latency.

        Algorithm (matches Moleculer.js exactly):
        1. Determine sample count (clipped to list length)
        2. If sampling all endpoints, iterate sequentially
           If sampling subset, pick random endpoints
        3. For each sample:
           - Skip if latency is None (no data yet)
           - If latency < lowLatency threshold, return immediately (fast path)
           - Track minimum latency seen
        4. Return endpoint with minimum latency, or random if no latency data

        Args:
            endpoints: List of available action endpoints
            ctx: Optional context (unused in this strategy)

        Returns:
            Selected endpoint, or None if list is empty
        """
        if not endpoints:
            return None

        if len(endpoints) == 1:
            return endpoints[0]

        # Fallback to random if no latency provider
        if self._latency_provider is None:
            return random.choice(list(endpoints))

        # Get options
        sample_count = self._as_int(self.opts.get("sampleCount", 5), 5)
        low_latency_threshold = self._as_float(self.opts.get("lowLatency", 10.0), 10.0)

        # Clip sample count to list length (Moleculer.js behavior)
        count = (
            len(endpoints) if sample_count <= 0 or sample_count > len(endpoints) else sample_count
        )
        use_sequential = count == len(endpoints)

        endpoints_list = list(endpoints)
        min_latency_endpoint: Action | None = None
        min_latency: float = float("inf")

        for i in range(count):
            # Moleculer.js: sequential if full list, random otherwise
            if use_sequential:
                ep = endpoints_list[i]
            else:
                ep = endpoints_list[random.randint(0, len(endpoints_list) - 1)]
            # Get hostname from node (Moleculer.js uses hostname, not node_id)
            node = ep.node
            hostname = node.hostname if node else None

            # Get latency from provider by hostname
            latency = self._latency_provider.get_host_latency(hostname)

            if latency is None:
                # No latency data - skip this endpoint for latency-based selection
                continue

            # Fast path: if latency is below threshold, select immediately
            if latency < low_latency_threshold:
                return ep

            # Track minimum
            if latency < min_latency:
                min_latency = latency
                min_latency_endpoint = ep

        # Return endpoint with minimum latency, or random fallback
        if min_latency_endpoint is not None:
            return min_latency_endpoint

        # Fallback: random selection (no latency data available)
        return endpoints_list[random.randint(0, len(endpoints_list) - 1)]

    def __repr__(self) -> str:
        sample_count = self.opts.get("sampleCount", 5)
        low_latency = self.opts.get("lowLatency", 10.0)
        has_provider = self._latency_provider is not None
        return (
            f"LatencyStrategy(sampleCount={sample_count}, "
            f"lowLatency={low_latency}, hasProvider={has_provider})"
        )
