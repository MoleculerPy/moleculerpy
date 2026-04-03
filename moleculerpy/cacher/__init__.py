"""Cacher package for MoleculerPy framework.

This package provides caching backends for action result caching,
following Moleculer.js patterns with Pythonic implementation.

Available backends:
    - MemoryCacher: In-memory cache with TTL (default)
    - RedisCacher: Distributed Redis cache (production)
    - MemoryLRUCacher: LRU eviction with max size (v0.14.10)

Usage:
    >>> from moleculerpy.cacher import MemoryCacher, RedisCacher, resolve
    >>>
    >>> # Memory cacher (development)
    >>> cacher = MemoryCacher(ttl=60)
    >>>
    >>> # Redis cacher (production)
    >>> cacher = RedisCacher("redis://localhost:6379/0")
    >>> cacher = RedisCacher({"redis": "redis://localhost:6379", "ttl": 3600})
    >>>
    >>> # Via resolver (string or dict config)
    >>> cacher = resolve("memory")
    >>> cacher = resolve({"type": "redis", "redis": "redis://localhost:6379"})
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import (
    BaseCacher,
    CacheItem,
    CacheKey,
    CacherOptions,
    CacherProtocol,
    KeygenFunc,
    Keys,
    Meta,
    Params,
)
from .memory import MemoryCacher
from .memory_lru import MemoryLRUCacher
from .redis import RedisCacher

if TYPE_CHECKING:
    pass

# Registry of cacher types
_CACHER_REGISTRY: dict[str, type[BaseCacher]] = {
    "memory": MemoryCacher,
    "Memory": MemoryCacher,
    "MemoryLRU": MemoryLRUCacher,
    "memory-lru": MemoryLRUCacher,
    "redis": RedisCacher,
    "Redis": RedisCacher,
}


def register(name: str, cacher_class: type[BaseCacher]) -> None:
    """Register a custom cacher type.

    Args:
        name: Cacher type name (e.g., "redis")
        cacher_class: Class implementing BaseCacher
    """
    _CACHER_REGISTRY[name] = cacher_class


def resolve(config: bool | str | dict[str, Any] | BaseCacher | None) -> BaseCacher | None:
    """Resolve cacher from configuration.

    Args:
        config: Cacher configuration:
            - None/False: No caching
            - True: Default MemoryCacher
            - str: Cacher type name (e.g., "memory")
            - dict: {"type": "memory", "ttl": 60, ...}
            - BaseCacher: Return as-is

    Returns:
        BaseCacher instance or None if disabled

    Raises:
        ValueError: If cacher type is unknown

    Example:
        >>> cacher = resolve(True)  # MemoryCacher()
        >>> cacher = resolve("memory")  # MemoryCacher()
        >>> cacher = resolve({"type": "memory", "ttl": 30})
        >>> cacher = resolve(None)  # None
    """
    if config is None or config is False:
        return None

    if isinstance(config, BaseCacher):
        return config

    if config is True:
        return MemoryCacher()

    if isinstance(config, str):
        cacher_class = _CACHER_REGISTRY.get(config)
        if cacher_class is None:
            raise ValueError(f"Unknown cacher type: {config!r}")
        return cacher_class()

    if isinstance(config, dict):
        cacher_type = config.get("type", "memory")
        cacher_class = _CACHER_REGISTRY.get(cacher_type)
        if cacher_class is None:
            raise ValueError(f"Unknown cacher type: {cacher_type!r}")

        # Extract options (exclude 'type')
        opts = {k: v for k, v in config.items() if k != "type"}
        return cacher_class(**opts)

    raise TypeError(f"Invalid cacher config type: {type(config).__name__}")


__all__ = [  # noqa: RUF022
    # Classes
    "BaseCacher",
    "MemoryCacher",
    "MemoryLRUCacher",
    "RedisCacher",
    "CacheItem",
    "CacherOptions",
    # Protocols & Types
    "CacherProtocol",
    "CacheKey",
    "KeygenFunc",
    "Keys",
    "Meta",
    "Params",
    # Functions
    "resolve",
    "register",
]
