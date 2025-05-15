"""Caching middleware for MoleculerPy framework.

This module implements action result caching via middleware,
following Moleculer.js CachingMiddleware patterns.

Features:
    - Automatic cache key generation
    - Per-action cache configuration
    - TTL support (global + per-action override)
    - Lock-based thundering herd protection
    - Cache bypass via ctx.meta["$cache"] = False
    - ctx.cached_result flag for cache hits

Usage:
    # Action with caching enabled
    @action(cache=True)
    async def get_user(self, ctx):
        return await db.get_user(ctx.params["id"])

    # Action with custom cache config
    @action(cache={"ttl": 60, "keys": ["id"]})
    async def get_user(self, ctx):
        return await db.get_user(ctx.params["id"])
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypedDict

from .base import HandlerType, Middleware

if TYPE_CHECKING:
    from ..cacher.base import BaseCacher, KeygenFunc
    from ..context import Context
    from ..registry import Action


class CacheLockOptions(TypedDict, total=False):
    """Lock configuration for cache entry.

    Attributes:
        enabled: Enable lock for thundering herd protection (default: True)
        ttl: Lock timeout in milliseconds (default: 15000)
        stale_time: Refresh cache if TTL below this (seconds)
    """

    enabled: bool
    ttl: int
    stale_time: int | None


class CacheOptions(TypedDict, total=False):
    """Cache configuration for an action.

    Attributes:
        enabled: Enable/disable caching (or callable for dynamic)
        ttl: Time-to-live in seconds
        keys: Specific param/meta keys for cache key
        keygen: Custom key generator function
        lock: Lock configuration for thundering herd
    """

    enabled: bool | Callable[[Any], bool]
    ttl: int | None
    keys: list[str] | None
    keygen: Any  # KeygenFunc
    lock: CacheLockOptions


@dataclass(frozen=True, slots=True)
class ParsedCacheOptions:
    """Parsed and normalized cache options (immutable).

    Attributes:
        enabled: Whether caching is enabled (bool or callable)
        ttl: TTL in seconds (None = use cacher default)
        keys: Keys for cache key generation
        keygen: Custom key generator
        lock_enabled: Whether to use locking
        lock_ttl: Lock timeout in seconds
        stale_time: Refresh if TTL below this

    Note:
        This dataclass is frozen (immutable) to prevent accidental
        modification after parsing.
    """

    enabled: bool | Callable[[Any], bool] = True
    ttl: int | None = None
    keys: tuple[str, ...] | None = None  # tuple for hashability with frozen
    keygen: KeygenFunc | None = None
    lock_enabled: bool = True
    lock_ttl: float = 15.0
    stale_time: int | None = None


def _parse_cache_options(cache_config: bool | dict[str, Any] | None) -> ParsedCacheOptions:
    """Parse action cache configuration into normalized options.

    Args:
        cache_config: Cache config from action definition
            - True: Enable with defaults
            - False/None: Disabled
            - dict: Custom configuration

    Returns:
        ParsedCacheOptions with normalized values

    Raises:
        TypeError: If cache_config is an unsupported type
    """
    match cache_config:
        case None | False:
            return ParsedCacheOptions(enabled=False)

        case True:
            return ParsedCacheOptions(enabled=True)

        case dict():
            # Parse lock options
            lock_config = cache_config.get("lock", {})
            lock_enabled = lock_config.get("enabled", True)
            lock_ttl = lock_config.get("ttl", 15000) / 1000  # ms -> seconds
            stale_time = lock_config.get("stale_time")

            # Convert keys list to tuple for frozen dataclass
            keys_list = cache_config.get("keys")
            keys_tuple = tuple(keys_list) if keys_list else None

            return ParsedCacheOptions(
                enabled=cache_config.get("enabled", True),
                ttl=cache_config.get("ttl"),
                keys=keys_tuple,
                keygen=cache_config.get("keygen"),
                lock_enabled=lock_enabled,
                lock_ttl=lock_ttl,
                stale_time=stale_time,
            )

        case _:
            raise TypeError(f"Invalid cache config type: {type(cache_config).__name__}")


class CachingMiddleware(Middleware):
    """Middleware that caches action results.

    Wraps action handlers to:
        1. Check cache before executing handler
        2. Return cached result on hit (sets ctx.cached_result = True)
        3. Execute handler on miss
        4. Store result in cache with TTL
        5. Handle thundering herd via locking

    Attributes:
        cacher: Cache backend instance
    """

    def __init__(self, cacher: BaseCacher) -> None:
        """Initialize caching middleware.

        Args:
            cacher: Cache backend (MemoryCacher, RedisCacher, etc.)
        """
        self.cacher = cacher

    async def local_action(
        self,
        next_handler: HandlerType[Any],
        action: Action,
    ) -> HandlerType[Any]:
        """Wrap local action with caching logic.

        Args:
            next_handler: Original action handler
            action: Action definition with cache config

        Returns:
            Wrapped handler with caching
        """
        # Get cache config from action
        # Check both cache_ttl (legacy) and cache dict
        cache_config = getattr(action, "cache", None)

        # Legacy: cache_ttl sets cache=True with that TTL
        if cache_config is None and action.cache_ttl is not None:
            cache_config = {"enabled": True, "ttl": action.cache_ttl}

        opts = _parse_cache_options(cache_config)

        # If caching disabled for this action, return original handler
        if opts.enabled is False:
            return next_handler

        # Create cached handler
        async def cached_handler(ctx: Context) -> Any:
            # 1. Check if dynamically disabled via callable
            if callable(opts.enabled):
                if not opts.enabled(ctx):
                    return await next_handler(ctx)

            # 2. Check meta disable flag: ctx.meta["$cache"] = False
            if ctx.meta.get("$cache") is False:
                return await next_handler(ctx)

            # 3. Check cacher health
            if self.cacher.connected is False:
                return await next_handler(ctx)

            # 4. Generate cache key
            # Convert tuple back to list for get_cache_key (expects list | None)
            keys_list = list(opts.keys) if opts.keys else None
            cache_key = self.cacher.get_cache_key(
                action.name,
                ctx.params,
                ctx.meta,
                keys_list,
                opts.keygen,
            )

            # 5. With locking (thundering herd protection)
            if opts.lock_enabled:
                return await self._get_with_lock(ctx, next_handler, cache_key, opts)

            # 6. Without locking (simpler path)
            return await self._get_without_lock(ctx, next_handler, cache_key, opts)

        return cached_handler

    async def _get_without_lock(
        self,
        ctx: Context,
        handler: HandlerType[Any],
        cache_key: str,
        opts: ParsedCacheOptions,
    ) -> Any:
        """Get from cache without locking.

        Simple path: check cache, execute handler on miss, store result.

        Args:
            ctx: Request context
            handler: Original action handler
            cache_key: Cache key
            opts: Cache options

        Returns:
            Cached or fresh result
        """
        # Try cache
        cached = await self.cacher.get(cache_key)
        if cached is not None:
            ctx.cached_result = True
            return cached

        # Cache miss: execute handler
        result = await handler(ctx)

        # Store in cache (fire and forget)
        await self.cacher.set(cache_key, result, opts.ttl)

        return result

    async def _get_with_lock(
        self,
        ctx: Context,
        handler: HandlerType[Any],
        cache_key: str,
        opts: ParsedCacheOptions,
    ) -> Any:
        """Get from cache with locking for thundering herd protection.

        Flow:
            1. Check cache
            2. On hit: return cached (no lock needed)
            3. On miss: acquire lock
            4. Double-check cache (another request may have filled it)
            5. Execute handler
            6. Store result
            7. Release lock

        Args:
            ctx: Request context
            handler: Original action handler
            cache_key: Cache key
            opts: Cache options

        Returns:
            Cached or fresh result
        """
        # First check: try cache without lock
        cached = await self.cacher.get(cache_key)
        if cached is not None:
            ctx.cached_result = True
            return cached

        # Cache miss: acquire lock
        unlock = await self.cacher.lock(cache_key, opts.lock_ttl)

        try:
            # Double-check: another request may have cached while we waited
            cached = await self.cacher.get(cache_key)
            if cached is not None:
                ctx.cached_result = True
                return cached

            # Still miss: execute handler
            result = await handler(ctx)

            # Store in cache
            await self.cacher.set(cache_key, result, opts.ttl)

            return result

        finally:
            # Always release lock
            await unlock()


def create_caching_middleware(cacher: BaseCacher) -> dict[str, Any]:
    """Factory function to create caching middleware dict.

    Used by BaseCacher.middleware() method.

    Args:
        cacher: Cache backend instance

    Returns:
        Middleware configuration dict for broker
    """
    middleware = CachingMiddleware(cacher)

    return {
        "name": "Cacher",
        "local_action": middleware.local_action,
        # Future: could add remote_action for distributed cache
    }
