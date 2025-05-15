"""CpuUsage load balancing strategy for MoleculerPy.

This module implements the CPU-aware load balancing strategy that selects
endpoints based on their node's CPU utilization, following Moleculer.js patterns.
"""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

from .base import BaseStrategy

if TYPE_CHECKING:
    from collections.abc import Sequence

    from ..context import Context
    from ..registry import Action


class CpuUsageStrategy(BaseStrategy):
    """Load balancing strategy that selects endpoints based on CPU usage.

    This strategy samples random endpoints and selects the one with the lowest
    CPU usage. If an endpoint has CPU below the threshold, it's selected immediately
    (fast path optimization).

    Compatible with Moleculer.js CpuUsageStrategy.

    Options:
        sampleCount (int): Number of random endpoints to sample (default: 3)
        lowCpuUsage (float): CPU threshold for fast path selection (default: 10.0)

    Example:
        >>> strategy = CpuUsageStrategy({"sampleCount": 5, "lowCpuUsage": 15.0})
        >>> endpoint = strategy.select(endpoints, ctx)
    """

    __slots__ = ()

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

    def select(
        self,
        endpoints: Sequence[Action],
        ctx: Context | None = None,
    ) -> Action | None:
        """Select endpoint with lowest CPU usage.

        Algorithm (matches Moleculer.js exactly):
        1. Determine sample count (clipped to list length)
        2. If sampling all endpoints, iterate sequentially
           If sampling subset, pick random endpoints
        3. For each sample:
           - Skip if CPU is None (no data yet)
           - If CPU < lowCpuUsage threshold, return immediately (fast path)
           - Track minimum CPU seen
        4. Return endpoint with minimum CPU, or random if no CPU data

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

        # Get options
        sample_count = self._as_int(self.opts.get("sampleCount", 3), 3)
        low_cpu_threshold = self._as_float(self.opts.get("lowCpuUsage", 10.0), 10.0)

        # Clip sample count to list length (Moleculer.js behavior)
        count = (
            len(endpoints) if sample_count <= 0 or sample_count > len(endpoints) else sample_count
        )
        use_sequential = count == len(endpoints)

        min_cpu_endpoint: Action | None = None
        min_cpu: float = float("inf")

        endpoints_list = list(endpoints)

        for i in range(count):
            # Moleculer.js: sequential if full list, random otherwise
            if use_sequential:
                ep = endpoints_list[i]
            else:
                ep = endpoints_list[random.randint(0, len(endpoints_list) - 1)]

            # Get CPU from node (if available)
            node = ep.node
            if node is None:
                continue

            cpu = node.cpu
            # Moleculer.js: skip endpoints with null CPU (not treat as 100%)
            if cpu is None:
                continue

            # Fast path: if CPU is below threshold, select immediately
            if cpu < low_cpu_threshold:
                return ep

            # Track minimum
            if cpu < min_cpu:
                min_cpu = cpu
                min_cpu_endpoint = ep

        # Return endpoint with minimum CPU, or random fallback
        if min_cpu_endpoint is not None:
            return min_cpu_endpoint

        # Fallback: random selection (no CPU data available)
        return endpoints_list[random.randint(0, len(endpoints_list) - 1)]

    def __repr__(self) -> str:
        sample_count = self.opts.get("sampleCount", 3)
        low_cpu = self.opts.get("lowCpuUsage", 10.0)
        return f"CpuUsageStrategy(sampleCount={sample_count}, lowCpuUsage={low_cpu})"
