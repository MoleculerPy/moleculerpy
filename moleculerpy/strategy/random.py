"""Random load balancing strategy for MoleculerPy.

Selects endpoints randomly for even statistical distribution over time.
"""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

from .base import BaseStrategy

if TYPE_CHECKING:
    from collections.abc import Sequence

    from ..context import Context
    from ..registry import Action


class RandomStrategy(BaseStrategy):
    """Random load balancing strategy.

    Selects a random endpoint from the list. Over time, this provides
    statistically even distribution without maintaining state.

    Example:
        >>> strategy = RandomStrategy()
        >>> endpoints = [action1, action2, action3]
        >>> strategy.select(endpoints)  # Returns random action
    """

    __slots__ = ("_rng",)

    def __init__(self, seed: int | None = None, opts: dict[str, object] | None = None) -> None:
        """Initialize random strategy.

        Args:
            seed: Optional seed for reproducible randomness (useful for testing)
            opts: Optional configuration (unused by Random)
        """
        super().__init__(opts)
        self._rng = random.Random(seed)

    def select(
        self,
        endpoints: Sequence[Action],
        ctx: Context | None = None,
    ) -> Action | None:
        """Select a random endpoint.

        Args:
            endpoints: List of available action endpoints
            ctx: Unused, kept for interface compatibility

        Returns:
            Random endpoint, or None if list is empty
        """
        if not endpoints:
            return None

        return self._rng.choice(endpoints)

    def __repr__(self) -> str:
        return "RandomStrategy()"
