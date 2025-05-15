"""Load balancing strategies for MoleculerPy.

This module provides various load balancing strategies for distributing
requests across multiple service instances, following Moleculer.js patterns.

Available Strategies:
    - RoundRobin (default): Cycles through endpoints in order
    - Random: Selects endpoints randomly

Example:
    >>> from moleculerpy.strategy import resolve, RoundRobinStrategy
    >>>
    >>> # Resolve by name (case-insensitive)
    >>> strategy = resolve("RoundRobin")
    >>>
    >>> # Or instantiate directly
    >>> strategy = RoundRobinStrategy()
    >>>
    >>> # Register custom strategy (function or decorator)
    >>> from moleculerpy.strategy import register
    >>> register("MyCustom", MyCustomStrategy)
    >>>
    >>> # Or as decorator:
    >>> @register("MyCustom")
    >>> class MyCustomStrategy(BaseStrategy):
    ...     def select(self, endpoints, ctx=None):
    ...         return endpoints[0] if endpoints else None
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Literal, TypeVar, overload

from .base import BaseStrategy, Endpoint, Strategy
from .cpu_usage import CpuUsageStrategy
from .latency import LatencyStrategy
from .random import RandomStrategy
from .round_robin import RoundRobinStrategy
from .shard import ShardStrategy

# Type alias for built-in strategy names (compile-time safety)
BuiltinStrategyName = Literal["RoundRobin", "Random", "CpuUsage", "Shard", "Latency"]

__all__ = [  # noqa: RUF022
    # Protocols
    "Strategy",
    "Endpoint",
    # Base class
    "BaseStrategy",
    # Strategies
    "RoundRobinStrategy",
    "RandomStrategy",
    "CpuUsageStrategy",
    "ShardStrategy",
    "LatencyStrategy",
    # Type aliases
    "BuiltinStrategyName",
    # Functions
    "resolve",
    "register",
    "get_by_name",
    # Exceptions
    "StrategyResolutionError",
]

# Strategy registry (like Moleculer.js Strategies object)
_STRATEGIES: dict[str, type[BaseStrategy]] = {
    "RoundRobin": RoundRobinStrategy,
    "Random": RandomStrategy,
    "CpuUsage": CpuUsageStrategy,
    "Shard": ShardStrategy,
    "Latency": LatencyStrategy,
}

# Lowercase lookup cache for O(1) case-insensitive access
_STRATEGIES_LOWER: dict[str, type[BaseStrategy]] | None = None


def _get_lowercase_cache() -> dict[str, type[BaseStrategy]]:
    """Get or build lowercase lookup cache (lazy initialization)."""
    global _STRATEGIES_LOWER  # noqa: PLW0603
    if _STRATEGIES_LOWER is None:
        _STRATEGIES_LOWER = {k.lower(): v for k, v in _STRATEGIES.items()}
    return _STRATEGIES_LOWER


def _invalidate_cache() -> None:
    """Invalidate lowercase cache when registry changes."""
    global _STRATEGIES_LOWER  # noqa: PLW0603
    _STRATEGIES_LOWER = None


class StrategyResolutionError(ValueError):
    """Raised when strategy resolution fails."""

    pass


def get_by_name(name: str) -> type[BaseStrategy] | None:
    """Get strategy class by name (case-insensitive, O(1) lookup).

    Args:
        name: Strategy name to look up

    Returns:
        Strategy class if found, None otherwise
    """
    if not name:
        return None

    return _get_lowercase_cache().get(name.lower())


def resolve(opt: str | type[BaseStrategy] | BaseStrategy | None = None) -> BaseStrategy:
    """Resolve strategy by name, class, or instance.

    This follows Moleculer.js pattern for flexible strategy configuration.

    Args:
        opt: Strategy specification:
            - None: Returns RoundRobin (default)
            - str: Strategy name (case-insensitive)
            - type: Strategy class to instantiate
            - BaseStrategy: Already instantiated strategy (returned as-is)

    Returns:
        Instantiated strategy

    Raises:
        StrategyResolutionError: If strategy name is invalid

    Example:
        >>> resolve()  # RoundRobinStrategy()
        >>> resolve("Random")  # RandomStrategy()
        >>> resolve(RoundRobinStrategy)  # RoundRobinStrategy()
        >>> resolve(RandomStrategy())  # Returns same instance
    """
    # Default to RoundRobin
    if opt is None:
        return RoundRobinStrategy()

    # Already an instance
    if isinstance(opt, BaseStrategy):
        return opt

    # Strategy class
    if isinstance(opt, type) and issubclass(opt, BaseStrategy):
        return opt()

    # Strategy name (string)
    if isinstance(opt, str):
        strategy_cls = get_by_name(opt)
        if strategy_cls is not None:
            return strategy_cls()
        raise StrategyResolutionError(f"Invalid strategy type '{opt}'")

    raise StrategyResolutionError(f"Cannot resolve strategy from {type(opt).__name__}: {opt!r}")


_T = TypeVar("_T", bound=type[BaseStrategy])


# Overloads for register() to support both function call and decorator patterns
@overload
def register(name: str, strategy_cls: type[BaseStrategy]) -> None:
    """Register a custom strategy (function call)."""
    ...


@overload
def register(name: str) -> Callable[[_T], _T]:
    """Register a custom strategy (decorator)."""
    ...


def register(
    name: str, strategy_cls: type[BaseStrategy] | None = None
) -> None | Callable[[_T], _T]:
    """Register a custom strategy.

    Can be used as a function call or as a decorator.

    Args:
        name: Name to register the strategy under
        strategy_cls: Strategy class (must be subclass of BaseStrategy).
            If None, returns a decorator.

    Returns:
        None when called with strategy_cls, or a decorator when called with name only.

    Raises:
        TypeError: If strategy_cls is not a BaseStrategy subclass

    Example (function call):
        >>> class MyStrategy(BaseStrategy):
        ...     def select(self, endpoints, ctx=None):
        ...         return endpoints[0] if endpoints else None
        ...
        >>> register("MyCustom", MyStrategy)
        >>> strategy = resolve("MyCustom")

    Example (decorator):
        >>> @register("MyCustom")
        ... class MyStrategy(BaseStrategy):
        ...     def select(self, endpoints, ctx=None):
        ...         return endpoints[0] if endpoints else None
        ...
        >>> strategy = resolve("MyCustom")
    """
    # Decorator pattern: @register("Name")
    if strategy_cls is None:

        def decorator(cls: _T) -> _T:
            if not isinstance(cls, type) or not issubclass(cls, BaseStrategy):
                raise TypeError(f"strategy_cls must be a BaseStrategy subclass, got {type(cls)}")
            _STRATEGIES[name] = cls
            _invalidate_cache()  # Clear lowercase cache
            return cls

        return decorator

    # Function call pattern: register("Name", StrategyClass)
    if not isinstance(strategy_cls, type) or not issubclass(strategy_cls, BaseStrategy):
        raise TypeError(f"strategy_cls must be a BaseStrategy subclass, got {type(strategy_cls)}")

    _STRATEGIES[name] = strategy_cls
    _invalidate_cache()  # Clear lowercase cache
    return None
