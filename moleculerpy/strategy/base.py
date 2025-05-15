"""Base strategy interface for load balancing in MoleculerPy.

This module defines the protocol (interface) for load balancing strategies,
following the Moleculer-Go pattern with Python Protocol for duck typing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Sequence

    from ..context import Context
    from ..registry import Action


@runtime_checkable
class Endpoint(Protocol):
    """Protocol for action endpoints that can be selected by strategies.

    Any object with `node_id` and `is_local` attributes satisfies this protocol.
    This matches Moleculer-Go's Selector interface.
    """

    @property
    def node_id(self) -> str:
        """Node ID hosting this endpoint."""
        ...

    @property
    def is_local(self) -> bool:
        """Whether this endpoint is local to the current node."""
        ...


@runtime_checkable
class Strategy(Protocol):
    """Protocol for load balancing strategies.

    Strategies select one endpoint from a list of available endpoints.
    This matches Moleculer-Go's Strategy interface.
    """

    def select(
        self,
        endpoints: Sequence[Action],
        ctx: Context | None = None,
    ) -> Action | None:
        """Select one endpoint from the list.

        Args:
            endpoints: List of available action endpoints
            ctx: Optional context for context-aware strategies (e.g., Shard)

        Returns:
            Selected endpoint, or None if list is empty
        """
        ...


class BaseStrategy:
    """Base class for load balancing strategies.

    Provides common functionality and enforces the Strategy protocol.
    Subclasses must implement the `select` method.

    Args:
        opts: Optional configuration dictionary for strategy-specific settings.
            Used by advanced strategies like CpuUsage, Latency, Shard.

    Example:
        >>> class ShardStrategy(BaseStrategy):
        ...     def __init__(self, opts: dict | None = None):
        ...         super().__init__(opts)
        ...         self.shard_key = self.opts.get("shardKey", "id")
    """

    __slots__ = ("_opts",)

    def __init__(self, opts: dict[str, object] | None = None) -> None:
        """Initialize strategy with optional configuration.

        Args:
            opts: Strategy-specific options (e.g., shardKey for Shard strategy)
        """
        self._opts: dict[str, object] = opts or {}

    @property
    def opts(self) -> dict[str, object]:
        """Strategy configuration options."""
        return self._opts

    def select(
        self,
        endpoints: Sequence[Action],
        ctx: Context | None = None,
    ) -> Action | None:
        """Select one endpoint from the list.

        Args:
            endpoints: List of available action endpoints
            ctx: Optional context for context-aware strategies

        Returns:
            Selected endpoint, or None if list is empty

        Raises:
            NotImplementedError: If not overridden by subclass
        """
        raise NotImplementedError("Subclasses must implement select()")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"
