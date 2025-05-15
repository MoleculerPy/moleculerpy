"""In-memory cache backend for MoleculerPy framework.

This module implements memory-based caching with TTL support,
following Moleculer.js MemoryCacher patterns.

Features:
    - O(1) get/set via dict
    - Lazy + periodic TTL expiration
    - Optional deep cloning
    - Pattern-based cleanup
    - Thread-safe for asyncio (single event loop)

Performance Notes:
    - clone=False (default): O(1) get/set — returns reference directly
    - clone=True: O(n) get/set — performs copy.deepcopy() on each operation

    WARNING: When clone=True, memory usage increases proportionally to the
    size of cached objects. For large objects (>1MB), consider:
    1. Using clone=False with immutable data structures
    2. Implementing object-level cloning in your application
    3. Using a serializing cache backend (Redis) instead

    The clone=True option is recommended only when:
    - Cached data may be mutated by the caller
    - Data isolation between requests is critical
    - Object sizes are small (<10KB typical)
"""

from __future__ import annotations

import asyncio
import copy
import fnmatch
import time
from typing import TYPE_CHECKING, Any

from .base import BaseCacher, CacheItem, KeygenFunc

if TYPE_CHECKING:
    from ..broker import ServiceBroker


class MemoryCacher(BaseCacher):
    """In-memory cache backend with TTL.

    Uses Python dict for O(1) lookups. Implements:
        - Lazy expiration on get()
        - Background cleanup every 30 seconds
        - Optional deep cloning for data isolation
        - Glob pattern matching for clean()

    Not suitable for multi-process deployments (use RedisCacher).

    Example:
        >>> cacher = MemoryCacher(ttl=60, clone=True)
        >>> await cacher.set("user:123", {"name": "Alice"})
        >>> data = await cacher.get("user:123")
        >>> await cacher.clean("user:*")

    Attributes:
        cache: Internal dict storage
        ttl: Default TTL in seconds
        clone: Whether to deep copy data
    """

    __slots__ = ("_cleanup_interval", "_cleanup_task", "cache")

    # Background cleanup runs every 30 seconds
    CLEANUP_INTERVAL = 30.0

    def __init__(
        self,
        ttl: int | None = None,
        prefix: str | None = None,
        max_params_length: int | None = None,
        clone: bool = False,
        keygen: KeygenFunc | None = None,
        cleanup_interval: float | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize memory cacher.

        Args:
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

        # Storage: key -> CacheItem
        self.cache: dict[str, CacheItem] = {}

        # Background cleanup
        self._cleanup_interval = cleanup_interval or self.CLEANUP_INTERVAL
        self._cleanup_task: asyncio.Task[None] | None = None

    def init(self, broker: ServiceBroker) -> None:
        """Initialize with broker reference.

        Sets up transporter reconnect listener to clear stale cache.

        Args:
            broker: Service broker instance
        """
        super().init(broker)

        # Clear cache on transporter reconnect (may have missed invalidations)
        if hasattr(broker, "local_bus"):
            broker.local_bus.on("$transporter.connected", self._on_reconnect)

    async def _on_reconnect(self, _payload: dict[str, Any]) -> None:
        """Handle transporter reconnection by clearing cache."""
        if self.broker:
            self.broker.logger.debug("Cacher: clearing cache on reconnect")
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
            cleanup_loop(), name="moleculerpy:memory-cache-cleanup"
        )

    def _stop_cleanup_task(self) -> None:
        """Stop periodic cleanup task."""
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            self._cleanup_task = None

    def _check_ttl(self) -> int:
        """Remove expired entries from cache.

        Called periodically by background task.

        Returns:
            Number of expired entries removed
        """
        now = (
            time.time_ns() // 1_000_000
        )  # nanoseconds -> milliseconds (faster than time.time() * 1000)
        expired_keys: list[str] = []

        for key, item in self.cache.items():
            if item.is_expired(now):
                expired_keys.append(key)

        for key in expired_keys:
            del self.cache[key]

        return len(expired_keys)

    # ─────────────────────────────────────────────────────────────
    # Core cache methods
    # ─────────────────────────────────────────────────────────────

    async def get(self, key: str) -> Any | None:
        """Get value from cache.

        Performs lazy expiration check: if expired, deletes and returns None.

        Args:
            key: Cache key (without prefix)

        Returns:
            Cached value (possibly cloned) or None if not found/expired
        """
        full_key = self.prefix + key
        item = self.cache.get(full_key)

        if item is None:
            return None

        # Lazy expiration check (time_ns is ~12% faster than time() * 1000)
        now = time.time_ns() // 1_000_000
        if item.is_expired(now):
            del self.cache[full_key]
            return None

        # Return data (clone if configured)
        if self.clone:
            return copy.deepcopy(item.data)
        return item.data

    async def set(self, key: str, data: Any, ttl: int | None = None) -> Any:
        """Store value in cache.

        Args:
            key: Cache key (without prefix)
            data: Value to cache
            ttl: TTL in seconds (None = use default)

        Returns:
            The stored data (possibly cloned)
        """
        full_key = self.prefix + key

        # Use provided TTL or default
        effective_ttl = ttl if ttl is not None else self.ttl

        # Calculate expiration timestamp (time_ns is ~12% faster)
        expire: float | None = None
        if effective_ttl is not None and effective_ttl > 0:
            now_ms = time.time_ns() // 1_000_000
            expire = now_ms + effective_ttl * 1000

        # Clone data if configured
        stored_data = copy.deepcopy(data) if self.clone else data

        self.cache[full_key] = CacheItem(data=stored_data, expire=expire)

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
            # Clear all
            self.cache.clear()
            return

        # Convert Moleculer pattern to fnmatch
        # "**" -> "*" (fnmatch doesn't have **)
        fnmatch_pattern = pattern.replace("**", "*")

        # Find matching keys (with prefix)
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
        """Get value and remaining TTL.

        Args:
            key: Cache key (without prefix)

        Returns:
            Tuple of (data, remaining_ttl_seconds)
            TTL is None if no expiration set or item not found
        """
        full_key = self.prefix + key
        item = self.cache.get(full_key)

        if item is None:
            return (None, None)

        now = time.time_ns() // 1_000_000

        # Check expiration
        if item.is_expired(now):
            del self.cache[full_key]
            return (None, None)

        # Calculate remaining TTL
        remaining_ttl: float | None = None
        if item.expire is not None:
            remaining_ttl = (item.expire - now) / 1000  # Convert to seconds

        # Clone data if configured
        data = copy.deepcopy(item.data) if self.clone else item.data

        return (data, remaining_ttl)

    def has(self, key: str) -> bool:
        """Check if key exists and is not expired.

        Synchronous method for quick checks.

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

        Note: May include expired items not yet cleaned.

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
        return f"MemoryCacher(ttl={self.ttl}, clone={self.clone}, size={len(self.cache)})"
