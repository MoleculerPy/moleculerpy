"""LRU in-memory cache backend for MoleculerPy framework.

This module implements an LRU (Least Recently Used) memory caching backend
with configurable max size and TTL support, following Moleculer.js
MemoryLRUCacher patterns.

Features:
    - O(1) LRU eviction via collections.OrderedDict
    - Configurable max size with automatic LRU eviction on overflow
    - Lazy + periodic TTL expiration
    - Optional deep cloning
    - Pattern-based cleanup

Differences from MemoryCacher:
    - Bounded size with LRU eviction (no unbounded growth)
    - get_with_ttl() returns (data, None) — TTL not available from LRU
    - lock.staleTime is not supported (warns on init)
"""

from __future__ import annotations

import asyncio
import copy
import fnmatch
import time
from collections import OrderedDict
from typing import TYPE_CHECKING, Any

from .base import BaseCacher, CacheItem, KeygenFunc

if TYPE_CHECKING:
    from ..broker import ServiceBroker


class MemoryLRUCacher(BaseCacher):
    """LRU in-memory cache backend with configurable max size and TTL.

    Uses collections.OrderedDict for O(1) LRU eviction. When the cache
    exceeds `max` items, the least recently used entry is evicted.

    Compatible with Moleculer.js MemoryLRUCacher behavior:
        - get_with_ttl() returns None for TTL (not tracked internally)
        - lock.staleTime warning on init

    Example:
        >>> cacher = MemoryLRUCacher(max=500, ttl=60)
        >>> await cacher.set("user:123", {"name": "Alice"})
        >>> data = await cacher.get("user:123")
        >>> await cacher.clean("user:*")

    Attributes:
        cache: Internal OrderedDict storage (LRU order)
        max: Maximum number of entries before LRU eviction
        ttl: Default TTL in seconds
        clone: Whether to deep copy data
    """

    __slots__ = ("_cleanup_interval", "_cleanup_task", "cache", "max")

    # Background cleanup runs every 30 seconds
    CLEANUP_INTERVAL = 30.0

    def __init__(
        self,
        max: int = 1000,
        ttl: int | None = None,
        prefix: str | None = None,
        max_params_length: int | None = None,
        clone: bool = False,
        keygen: KeygenFunc | None = None,
        cleanup_interval: float | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize LRU memory cacher.

        Args:
            max: Maximum number of entries (default: 1000). LRU eviction
                 kicks in when cache exceeds this limit.
            ttl: Default TTL in seconds (None = no expiry)
            prefix: Key prefix (default: "MOL-")
            max_params_length: Max key length before hashing
            clone: Deep copy data on get/set (default: False)
            keygen: Custom key generator function
            cleanup_interval: Background cleanup interval (default: 30s)
            **kwargs: Additional options (ignored)
        """
        super().__init__(
            ttl=ttl,
            prefix=prefix,
            max_params_length=max_params_length,
            clone=clone,
            keygen=keygen,
            **kwargs,
        )

        if max < 1:
            raise ValueError(f"MemoryLRUCacher max must be >= 1, got {max}")
        self.max = max

        # LRU storage: insertion order == recency order (most recent at end)
        self.cache: OrderedDict[str, CacheItem] = OrderedDict()

        # Background cleanup
        self._cleanup_interval = cleanup_interval or self.CLEANUP_INTERVAL
        self._cleanup_task: asyncio.Task[None] | None = None

    def init(self, broker: ServiceBroker) -> None:
        """Initialize with broker reference.

        Warns if lock.staleTime is configured (not supported in LRU mode,
        matching Node.js behavior).

        Args:
            broker: Service broker instance
        """
        super().init(broker)

        # Node.js compat: warn if staleTime is set (not supported)
        lock_opts = self.opts.get("lock")
        if isinstance(lock_opts, dict) and lock_opts.get("staleTime") is not None:
            broker.logger.warning("MemoryLRUCacher: 'lock.staleTime' option is not supported.")

        # Clear cache on transporter reconnect (may have missed invalidations)
        if hasattr(broker, "local_bus"):
            broker.local_bus.on("$transporter.connected", self._on_reconnect)

    async def _on_reconnect(self, _payload: dict[str, Any]) -> None:
        """Handle transporter reconnection by clearing cache."""
        if self.broker:
            self.broker.logger.debug("MemoryLRUCacher: clearing cache on reconnect")
        await self.clean()

    async def start(self) -> None:
        """Start background cleanup task."""
        await super().start()
        self._start_cleanup_task()

    async def stop(self) -> None:
        """Stop background cleanup and clear cache."""
        self._stop_cleanup_task()
        self.cache.clear()
        await super().stop()

    def _start_cleanup_task(self) -> None:
        """Start periodic TTL cleanup task."""
        if self._cleanup_task is not None:
            return

        async def cleanup_loop() -> None:
            """Background task to remove expired entries."""
            try:
                while True:
                    await asyncio.sleep(self._cleanup_interval)
                    self._check_ttl()
            except asyncio.CancelledError:
                pass

        self._cleanup_task = asyncio.create_task(
            cleanup_loop(), name="moleculerpy:memory-lru-cache-cleanup"
        )

    def _stop_cleanup_task(self) -> None:
        """Stop periodic cleanup task."""
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            self._cleanup_task = None

    def _check_ttl(self) -> int:
        """Remove expired entries from cache.

        Iterates a snapshot of keys to safely delete during iteration.
        Called periodically by background task (mirrors Node.js cache.prune()).

        Returns:
            Number of expired entries removed
        """
        now = time.time_ns() // 1_000_000
        expired_keys: list[str] = []

        for key in list(self.cache.keys()):
            item = self.cache[key]
            if item.is_expired(now):
                expired_keys.append(key)

        for key in expired_keys:
            del self.cache[key]

        return len(expired_keys)

    # ─────────────────────────────────────────────────────────────
    # Core cache methods
    # ─────────────────────────────────────────────────────────────

    async def get(self, key: str) -> Any | None:
        """Get value from cache, updating LRU recency.

        On hit, moves item to end of OrderedDict (most recently used).
        Performs lazy expiration: if expired, deletes and returns None.

        Args:
            key: Cache key (without prefix)

        Returns:
            Cached value (possibly cloned) or None if not found/expired
        """
        full_key = self.prefix + key
        item = self.cache.get(full_key)

        if item is None:
            return None

        now = time.time_ns() // 1_000_000
        if item.is_expired(now):
            del self.cache[full_key]
            return None

        # Update recency: move to end (most recently used)
        self.cache.move_to_end(full_key)

        if self.clone:
            return copy.deepcopy(item.data)
        return item.data

    async def set(self, key: str, data: Any, ttl: int | None = None) -> Any:
        """Store value in cache with LRU eviction on overflow.

        After setting, moves item to end (most recently used).
        Evicts least recently used entry if cache exceeds `max`.

        Args:
            key: Cache key (without prefix)
            data: Value to cache
            ttl: TTL in seconds (None = use default)

        Returns:
            The stored data (possibly cloned)
        """
        full_key = self.prefix + key

        effective_ttl = ttl if ttl is not None else self.ttl

        expire: float | None = None
        if effective_ttl is not None and effective_ttl > 0:
            now_ms = time.time_ns() // 1_000_000
            expire = now_ms + effective_ttl * 1000

        stored_data = copy.deepcopy(data) if self.clone else data

        self.cache[full_key] = CacheItem(data=stored_data, expire=expire)

        # Update recency
        self.cache.move_to_end(full_key)

        # LRU eviction: remove least recently used (front of OrderedDict)
        while len(self.cache) > self.max:
            self.cache.popitem(last=False)

        return stored_data

    async def delete(self, keys: str | list[str]) -> None:
        """Delete one or more keys from cache.

        Args:
            keys: Single key or list of keys (without prefix)
        """
        if isinstance(keys, str):
            keys = [keys]

        for key in keys:
            full_key = self.prefix + key
            self.cache.pop(full_key, None)

    async def clean(self, pattern: str = "**") -> None:
        """Clear cache entries matching pattern.

        Pattern matching uses fnmatch (glob-style):
            - "*" matches any characters within a segment
            - "**" matches everything
            - "user:*" matches keys starting with "user:"

        Args:
            pattern: Glob-like pattern (default: "**" = all)
        """
        if pattern == "**":
            self.cache.clear()
            return

        fnmatch_pattern = pattern.replace("**", "*")
        full_pattern = self.prefix + fnmatch_pattern
        keys_to_delete: list[str] = []

        for key in self.cache:
            if fnmatch.fnmatch(key, full_pattern):
                keys_to_delete.append(key)

        for key in keys_to_delete:
            del self.cache[key]

    # ─────────────────────────────────────────────────────────────
    # Extended methods
    # ─────────────────────────────────────────────────────────────

    async def get_with_ttl(self, key: str) -> tuple[Any | None, float | None]:
        """Get value with TTL.

        Note: TTL is not available from LRU internals (Node.js compat).
        Always returns None for TTL component.

        Args:
            key: Cache key (without prefix)

        Returns:
            Tuple of (data, None) — TTL always None (not available)
        """
        data = await self.get(key)
        return (data, None)

    def has(self, key: str) -> bool:
        """Check if key exists and is not expired.

        Args:
            key: Cache key (without prefix)

        Returns:
            True if key exists and not expired
        """
        full_key = self.prefix + key
        item = self.cache.get(full_key)

        if item is None:
            return False

        if item.is_expired(time.time_ns() // 1_000_000):
            del self.cache[full_key]
            return False

        return True

    def size(self) -> int:
        """Get number of cached items.

        Returns:
            Number of items in cache
        """
        return len(self.cache)

    def keys(self) -> list[str]:
        """Get all cache keys (without prefix).

        Returns:
            List of cache keys
        """
        prefix_len = len(self.prefix)
        return [key[prefix_len:] for key in self.cache]

    def __repr__(self) -> str:
        return (
            f"MemoryLRUCacher(max={self.max}, ttl={self.ttl}, "
            f"clone={self.clone}, size={len(self.cache)})"
        )
