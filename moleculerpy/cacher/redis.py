"""Redis cacher implementation for MoleculerPy framework.

This module provides distributed caching using Redis, following
Moleculer.js RedisCacher patterns.

Features:
    - Distributed caching across multiple processes/nodes
    - TTL support with Redis EXPIRE
    - Pattern-based cleanup using SCAN (non-blocking)
    - Pluggable serialization (JSON, MessagePack, etc.)
    - Optional distributed locking (future: Redlock)
    - Metrics integration
    - Cluster support
    - Graceful error handling

Usage:
    from moleculerpy.cacher import RedisCacher

    # From Redis URL
    cacher = RedisCacher("redis://localhost:6379/0")

    # With options
    cacher = RedisCacher({
        "redis": {"host": "localhost", "port": 6379, "db": 0},
        "prefix": "myapp",
        "ttl": 3600,
        "serializer": "JSON"
    })

    # Cluster mode
    cacher = RedisCacher({
        "cluster": {
            "nodes": [
                {"host": "node1", "port": 6379},
                {"host": "node2", "port": 6379}
            ]
        }
    })
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any

try:
    import redis.asyncio as aioredis
except ImportError as err:
    raise ImportError(
        "RedisCacher requires 'redis' package. Install with: pip install redis[hiredis]"
    ) from err

from .base import BaseCacher

if TYPE_CHECKING:
    from ..broker import ServiceBroker

logger = logging.getLogger(__name__)


class JSONSerializer:
    """Simple JSON serializer for cache values."""

    def serialize(self, data: Any) -> bytes:
        """Serialize data to bytes."""
        return json.dumps(data).encode("utf-8")

    def deserialize(self, data: bytes) -> Any:
        """Deserialize bytes to data."""
        return json.loads(data.decode("utf-8"))


class RedisCacher(BaseCacher):
    """Redis-based distributed cacher.

    Provides distributed caching across multiple processes/nodes using Redis.
    Compatible with Moleculer.js RedisCacher API.

    Attributes:
        client: Redis client instance
        prefix: Key prefix for namespacing
        default_ttl: Default TTL in seconds
        serializer: Serializer for values
        connected: Connection status flag
    """

    def __init__(
        self,
        opts: str | dict[str, Any],
    ) -> None:
        """Initialize Redis cacher.

        Args:
            opts: Redis URL string or configuration dict:
                - redis: Connection options (host, port, db, password, etc.)
                - cluster: Cluster configuration (nodes, options)
                - prefix: Key prefix (default: "MOL-")
                - ttl: Default TTL in seconds
                - serializer: Serializer name or instance (default: JSON)
                - ping_interval: Optional periodic PING in seconds
        """
        super().__init__()
        self.redis_url: str | None = None

        # Parse options
        if isinstance(opts, str):
            # Redis URL string
            self.redis_url = opts
            self.opts: dict[str, Any] = {"redis": opts}
        else:
            self.opts = opts
            self.redis_url = opts.get("redis") if isinstance(opts.get("redis"), str) else None

        # Connection settings
        self.client: aioredis.Redis | None = None
        self.connected = False

        # Key prefix (namespacing)
        self.prefix = self.opts.get("prefix", "MOL-")

        # Default TTL in seconds
        self.default_ttl: int | None = self.opts.get("ttl")

        # Serializer (initialized in init())
        self.serializer = JSONSerializer()

        # Ping interval (optional periodic health check)
        self.ping_interval = self.opts.get("ping_interval")
        self._ping_task: asyncio.Task[None] | None = None

    def init(self, broker: ServiceBroker) -> None:
        """Initialize cacher with broker reference.

        Args:
            broker: ServiceBroker instance
        """
        super().init(broker)

        # Create logger (use broker's logger factory)
        if hasattr(broker, "_create_logger"):
            self.logger = broker._create_logger("REDIS-CACHER")
        else:
            self.logger = logger

        # Add namespace to prefix if configured
        if broker.namespace and broker.namespace != "":
            self.prefix = f"MOL-{broker.namespace}-"

        self.logger.info(f"Initializing Redis cacher with prefix '{self.prefix}'")

    async def connect(self) -> None:
        """Connect to Redis server.

        Raises:
            ConnectionError: If connection fails
        """
        # Check if already connected (client exists and ping works)
        if self.client is not None:
            try:
                await self._await_maybe(self.client.ping())
                self.connected = True
                return  # Already connected
            except Exception:
                # Connection lost, reconnect
                self.client = None
                self.connected = False

        try:
            # Check if cluster mode
            if "cluster" in self.opts:
                # Cluster mode (future enhancement)
                raise NotImplementedError("Redis Cluster support coming soon")

            # Standard mode
            if self.redis_url:
                self.client = aioredis.Redis.from_url(
                    self.redis_url,
                    decode_responses=False,  # Binary mode for serialization
                )
            else:
                redis_opts = self.opts.get("redis", {})
                self.client = aioredis.Redis(**redis_opts, decode_responses=False)

            # Test connection
            await self._await_maybe(self.client.ping())
            self.connected = True

            self.logger.info("Redis cacher connected")

            # Optional: Start periodic ping
            if self.ping_interval and self.ping_interval > 0:
                self._ping_task = asyncio.create_task(
                    self._ping_loop(),
                    name="moleculerpy:redis-cache-ping",
                )

        except Exception as e:
            self.connected = False
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise ConnectionError(f"Redis cacher connection failed: {e}") from e

    async def disconnect(self) -> None:
        """Disconnect from Redis gracefully."""
        # Stop ping task
        if self._ping_task:
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
            self._ping_task = None

        # Close Redis connection
        if self.client:
            try:
                if hasattr(self.client, "aclose"):
                    await self.client.aclose()
                else:
                    await self.client.close()
                self.logger.info("Redis cacher disconnected")
            except Exception as e:
                self.logger.debug(f"Redis disconnect error (ignored): {e}")
            finally:
                self.client = None
                self.connected = False

    async def _ping_loop(self) -> None:
        """Periodic ping task for connection monitoring."""
        try:
            while self.connected and self.client:
                interval = (
                    self.ping_interval if isinstance(self.ping_interval, (int, float)) else 0.0
                )
                await asyncio.sleep(interval)
                try:
                    await self._await_maybe(self.client.ping())
                except Exception as e:
                    self.logger.warning(f"Ping failed: {e}")
                    self.connected = False
                    # Broadcast error event
                    if self.broker:
                        await self.broker.broadcast_local(
                            "$cacher.error",
                            {"error": str(e), "module": "cacher", "type": "PING_FAILED"},
                        )
        except asyncio.CancelledError:
            pass

    def _get_prefixed_key(self, key: str) -> str:
        """Add prefix to key for namespacing.

        Args:
            key: Original cache key

        Returns:
            Prefixed key for Redis
        """
        return self.prefix + key

    async def _await_maybe(self, value: Awaitable[Any] | Any) -> Any:
        """Await value only when client method returns an awaitable."""
        if isinstance(value, Awaitable):
            return await value
        return value

    async def get(self, key: str) -> Any:
        """Retrieve value from Redis cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found

        Raises:
            Exception: If Redis operation fails
        """
        if not self.connected or not self.client:
            self.logger.debug(f"Cache GET {key}: not connected")
            return None

        prefixed_key = self._get_prefixed_key(key)

        try:
            # Use get() with decode_responses=False returns bytes
            data = await self.client.get(prefixed_key)

            if data is None:
                self.logger.debug(f"Cache MISS: {key}")
                return None

            # Deserialize
            try:
                result = self.serializer.deserialize(data)
                self.logger.debug(f"Cache HIT: {key}")
                return result
            except Exception as e:
                self.logger.error(f"Failed to deserialize cached value for {key}: {e}")
                # Delete corrupted entry
                await self.client.delete(prefixed_key)
                return None

        except Exception as e:
            self.logger.error(f"Redis GET error for {key}: {e}")
            return None  # Graceful fallthrough on cache error

    async def set(
        self,
        key: str,
        data: Any,
        ttl: int | None = None,
    ) -> None:
        """Store value in Redis cache.

        Args:
            key: Cache key
            data: Data to cache
            ttl: Time-to-live in seconds (uses default_ttl if None)

        Raises:
            Exception: If Redis operation fails
        """
        if not self.connected or not self.client:
            self.logger.debug(f"Cache SET {key}: not connected")
            return

        prefixed_key = self._get_prefixed_key(key)

        # Use provided TTL or default
        if ttl is None:
            ttl = self.default_ttl

        try:
            # Serialize data
            serialized = self.serializer.serialize(data)

            # SET with optional TTL (atomic operation)
            if ttl:
                await self.client.set(prefixed_key, serialized, ex=ttl)
            else:
                await self.client.set(prefixed_key, serialized)

            self.logger.debug(f"Cache SET: {key} (ttl={ttl}s)")

        except Exception as e:
            self.logger.error(f"Redis SET error for {key}: {e}")
            # Don't raise — cache failures should not break app

    async def delete(self, keys: str | list[str]) -> None:
        """Delete one or more keys from cache.

        Args:
            keys: Single key or list of keys

        Returns:
        Raises:
            Exception: If Redis operation fails
        """
        if not self.connected or not self.client:
            self.logger.debug("Cache DELETE: not connected")
            return

        # Normalize to list
        if isinstance(keys, str):
            keys = [keys]

        # Prefix all keys
        prefixed_keys = [self._get_prefixed_key(k) for k in keys]

        try:
            count = await self.client.delete(*prefixed_keys)
            self.logger.debug(f"Cache DELETE: {count} keys")
        except Exception as e:
            self.logger.error(f"Redis DELETE error: {e}")
            return

    async def clean(self, match: str | list[str] = "*") -> None:
        """Delete keys matching pattern using SCAN.

        Uses non-blocking SCAN instead of KEYS command.

        Args:
            match: Pattern or list of patterns (supports * wildcard)

        Raises:
            Exception: If Redis operation fails
        """
        if not self.connected or not self.client:
            self.logger.debug("Cache CLEAN: not connected")
            return

        # Normalize to list
        patterns = [match] if isinstance(match, str) else match

        # Process each pattern sequentially
        for raw_pattern in patterns:
            # Replace ** with * (Moleculer.js compatibility)
            pattern = raw_pattern.replace("**", "*")
            prefixed_pattern = self._get_prefixed_key(pattern)

            try:
                # Use SCAN for non-blocking iteration
                cursor = 0
                while True:
                    cursor, keys = await self.client.scan(
                        cursor=cursor,
                        match=prefixed_pattern,
                        count=100,  # Batch size
                    )

                    if keys:
                        # Delete batch
                        await self.client.delete(*keys)
                        self.logger.debug(f"Cache CLEAN: deleted {len(keys)} keys")

                    if cursor == 0:
                        break

                self.logger.debug(f"Cache CLEAN complete: {pattern}")

            except Exception as e:
                self.logger.error(f"Redis CLEAN error for {pattern}: {e}")

    async def get_with_ttl(self, key: str) -> tuple[Any | None, float | None]:
        """Get value and remaining TTL in one operation.

        Uses Redis pipeline for atomic two-command fetch.

        Args:
            key: Cache key

        Returns:
            Tuple of cached data and remaining TTL in seconds.
            Missing keys and keys without expiry return `None` for TTL.

        Raises:
            Exception: If Redis operation fails
        """
        if not self.connected or not self.client:
            return (None, None)

        prefixed_key = self._get_prefixed_key(key)

        try:
            # Pipeline: GET + TTL (reduces RTT from 2 to 1)
            pipe = self.client.pipeline()
            pipe.get(prefixed_key)
            pipe.ttl(prefixed_key)
            results = await pipe.execute()

            data_bytes, ttl = results

            # Parse data
            data = None
            if data_bytes:
                try:
                    data = self.serializer.deserialize(data_bytes)
                    self.logger.debug(f"Cache HIT with TTL: {key} (ttl={ttl}s)")
                except Exception as e:
                    self.logger.error(f"Failed to deserialize {key}: {e}")
                    # Delete corrupted entry
                    await self.client.delete(prefixed_key)
                    ttl = -2

            normalized_ttl: float | None
            if ttl is None or ttl < 0:
                normalized_ttl = None
            else:
                normalized_ttl = float(ttl)

            return (data, normalized_ttl)

        except Exception as e:
            self.logger.error(f"Redis GET_WITH_TTL error for {key}: {e}")
            return (None, None)

    async def get_cache_keys(self) -> list[dict[str, str]]:
        """List all cache keys using SCAN.

        Returns:
            List of dictionaries with "key" field (without prefix)

        Raises:
            Exception: If Redis operation fails
        """
        if not self.connected or not self.client:
            return []

        keys = []
        pattern = self._get_prefixed_key("*")

        try:
            cursor = 0
            while True:
                cursor, batch = await self.client.scan(
                    cursor=cursor,
                    match=pattern,
                    count=100,
                )

                # Remove prefix from keys
                prefix_len = len(self.prefix)
                for key_bytes in batch:
                    key = key_bytes.decode("utf-8") if isinstance(key_bytes, bytes) else key_bytes
                    if key.startswith(self.prefix):
                        keys.append({"key": key[prefix_len:]})

                if cursor == 0:
                    break

            return keys

        except Exception as e:
            self.logger.error(f"Redis GET_CACHE_KEYS error: {e}")
            return []

    # Lock methods (TODO: Implement Redlock for distributed locking)
    # For now, inherit from BaseCacher which provides in-memory lock fallback

    async def close(self) -> None:
        """Close Redis connection gracefully."""
        await self.disconnect()
