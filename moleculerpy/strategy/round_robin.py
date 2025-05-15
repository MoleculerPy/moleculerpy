"""Round-robin load balancing strategy for MoleculerPy.

Cycles through endpoints in order, distributing requests evenly.
Thread-safe implementation using threading.Lock().
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from .base import BaseStrategy

if TYPE_CHECKING:
    from collections.abc import Sequence

    from ..context import Context
    from ..registry import Action


class RoundRobinStrategy(BaseStrategy):
    """Round-robin load balancing strategy.

    Cycles through endpoints in order, ensuring even distribution of requests.
    Thread-safe: uses a lock for atomic counter increments.

    Example:
        >>> strategy = RoundRobinStrategy()
        >>> endpoints = [action1, action2, action3]
        >>> strategy.select(endpoints)  # Returns action1
        >>> strategy.select(endpoints)  # Returns action2
        >>> strategy.select(endpoints)  # Returns action3
        >>> strategy.select(endpoints)  # Returns action1 (cycles)
    """

    __slots__ = ("_counter", "_lock")

    def __init__(self, opts: dict[str, object] | None = None) -> None:
        """Initialize round-robin strategy with counter at 0.

        Args:
            opts: Optional configuration (unused by RoundRobin)
        """
        super().__init__(opts)
        self._counter: int = 0
        self._lock = threading.Lock()

    def select(
        self,
        endpoints: Sequence[Action],
        ctx: Context | None = None,
    ) -> Action | None:
        """Select next endpoint in round-robin order.

        Args:
            endpoints: List of available action endpoints
            ctx: Unused, kept for interface compatibility

        Returns:
            Next endpoint in cycle, or None if list is empty
        """
        if not endpoints:
            return None

        with self._lock:
            index = self._counter % len(endpoints)
            self._counter += 1

        return endpoints[index]

    @property
    def counter(self) -> int:
        """Current counter value (for testing/debugging)."""
        return self._counter

    def reset(self) -> None:
        """Reset counter to 0 (useful for testing)."""
        with self._lock:
            self._counter = 0

    def __repr__(self) -> str:
        return f"RoundRobinStrategy(counter={self._counter})"
