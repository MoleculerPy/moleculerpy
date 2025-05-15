"""Base cacher interface and types for MoleculerPy framework.

This module defines the abstract base class and protocols for cache backends,
following Moleculer.js patterns with modern Python typing.

Key concepts:
    - BaseCacher: ABC with required methods (get, set, del, clean)
    - CacheItem: Dataclass for cached item with TTL
    - CacherOptions: TypedDict for configuration options
    - Cache key generation with SHA256 for long keys
    - CacherProtocol: Protocol for structural typing
    - CacheKey: NewType for type-safe cache keys
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    NewType,
    Protocol,
    TypedDict,
    runtime_checkable,
)

from ..lock import Lock, UnlockFunc

if TYPE_CHECKING:
    from ..broker import ServiceBroker


# ─────────────────────────────────────────────────────────────
# Type Aliases for Readability
# ─────────────────────────────────────────────────────────────

#: Request parameters dictionary
type Params = dict[str, Any]

#: Request metadata dictionary
type Meta = dict[str, Any]

#: Cache key filter keys
type Keys = list[str] | None

#: Type-safe cache key (prevents accidental string mixing)
CacheKey = NewType("CacheKey", str)


class CacherOptions(TypedDict, total=False):
    """Configuration options for cachers.

    Attributes:
        ttl: Default time-to-live in seconds (None = no expiry)
        prefix: Key prefix (default: "MOL-")
        max_params_length: Max params length before SHA256 hashing
        clone: Whether to deep copy cached data
        keygen: Custom key generator function
    """

    ttl: int | None
    prefix: str
    max_params_length: int | None
    clone: bool


@dataclass(slots=True)
class CacheItem:
    """A cached item with optional expiration.

    Attributes:
        data: The cached value
        expire: Expiration timestamp (ms) or None for no expiry
    """

    data: Any
    expire: float | None = None

    def is_expired(self, now: float) -> bool:
        """Check if item has expired.

        Args:
            now: Current timestamp in milliseconds

        Returns:
            True if expired, False otherwise
        """
        if self.expire is None:
            return False
        return self.expire < now


#: Custom key generator function signature
type KeygenFunc = Callable[[str, Params, Meta, Keys], str]


# ─────────────────────────────────────────────────────────────
# Protocols for Structural Typing
# ─────────────────────────────────────────────────────────────


@runtime_checkable
class CacherProtocol(Protocol):
    """Protocol for cache backends (structural typing).

    Any class implementing these methods is automatically compatible,
    without needing to inherit from BaseCacher.

    Example:
        >>> class CustomCacher:
        ...     async def get(self, key: str) -> Any | None: ...
        ...     async def set(self, key: str, data: Any, ttl: int | None = None) -> Any: ...
        ...     async def delete(self, keys: str | list[str]) -> None: ...
        ...     async def clean(self, pattern: str = "**") -> None: ...
        ...
        >>> isinstance(CustomCacher(), CacherProtocol)
        True
    """

    async def get(self, key: str) -> Any | None:
        """Get value from cache."""
        ...

    async def set(self, key: str, data: Any, ttl: int | None = None) -> Any:
        """Store value in cache."""
        ...

    async def delete(self, keys: str | list[str]) -> None:
        """Delete one or more keys from cache."""
        ...

    async def clean(self, pattern: str = "**") -> None:
        """Clear cache entries matching pattern."""
        ...

    async def lock(self, key: str, ttl: float = 15.0) -> UnlockFunc:
        """Acquire lock for a cache key."""
        ...


class BaseCacher(ABC):
    """Abstract base class for cache backends.

    Provides common functionality:
        - Key generation with hashing
        - Lock for thundering herd protection
        - Broker integration
        - Metrics tracking (future)

    Subclasses must implement:
        - get(key): Retrieve cached value
        - set(key, data, ttl): Store value
        - delete(keys): Remove keys
        - clean(pattern): Clear by pattern

    Example:
        >>> class RedisCacher(BaseCacher):
        ...     async def get(self, key: str) -> Any | None:
        ...         return await self.redis.get(self.prefix + key)
    """

    __slots__ = (
        "_lock",
        "broker",
        "clone",
        "connected",
        "keygen",
        "max_params_length",
        "opts",
        "prefix",
        "ttl",
    )

    # Default options
    DEFAULT_PREFIX = "MOL-"
    DEFAULT_TTL: int | None = None
    DEFAULT_MAX_PARAMS_LENGTH = 1024

    def __init__(
        self,
        ttl: int | None = None,
        prefix: str | None = None,
        max_params_length: int | None = None,
        clone: bool = False,
        keygen: KeygenFunc | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize base cacher.

        Args:
            ttl: Default TTL in seconds (None = no expiry)
            prefix: Key prefix (default: "MOL-")
            max_params_length: Max key length before hashing (default: 1024)
            clone: Deep copy data on get/set (default: False)
            keygen: Custom key generator function
            **kwargs: Additional backend-specific options
        """
        self.opts: dict[str, Any] = kwargs
        self.prefix = prefix or self.DEFAULT_PREFIX
        self.ttl = ttl if ttl is not None else self.DEFAULT_TTL
        self.clone = clone
        self.max_params_length = max_params_length or self.DEFAULT_MAX_PARAMS_LENGTH
        self.keygen = keygen

        # Set during init()
        self.broker: ServiceBroker | None = None
        self.connected: bool | None = None

        # Lock for cache coordination
        self._lock = Lock()

    def init(self, broker: ServiceBroker) -> None:
        """Initialize cacher with broker reference.

        Called by broker during startup. Sets up:
            - Broker reference
            - Prefix with namespace
            - Event listeners (transporter reconnect)

        Args:
            broker: Service broker instance
        """
        self.broker = broker
        self.connected = True

        # Prefix with namespace: "MOL-namespace-"
        namespace = getattr(broker.settings, "namespace", None)
        if namespace:
            self.prefix = f"{self.prefix}{namespace}-"

    async def start(self) -> None:
        """Start cacher (called by broker).

        Override in subclasses for connection setup.
        """
        return None

    async def stop(self) -> None:
        """Stop cacher (called by broker).

        Override in subclasses for cleanup.
        """
        self._lock.clear()

    # ─────────────────────────────────────────────────────────────
    # Abstract methods (must implement in subclasses)
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    async def get(self, key: str) -> Any | None:
        """Get value from cache.

        Args:
            key: Cache key (without prefix)

        Returns:
            Cached value or None if not found/expired
        """
        ...

    @abstractmethod
    async def set(self, key: str, data: Any, ttl: int | None = None) -> Any:
        """Store value in cache.

        Args:
            key: Cache key (without prefix)
            data: Value to cache
            ttl: TTL in seconds (None = use default)

        Returns:
            The stored data (possibly cloned)
        """
        ...

    @abstractmethod
    async def delete(self, keys: str | list[str]) -> None:
        """Delete one or more keys from cache.

        Args:
            keys: Single key or list of keys
        """
        ...

    @abstractmethod
    async def clean(self, pattern: str = "**") -> None:
        """Clear cache entries matching pattern.

        Args:
            pattern: Glob-like pattern (default: "**" = all)
        """
        ...

    # ─────────────────────────────────────────────────────────────
    # Optional methods (override for specific backends)
    # ─────────────────────────────────────────────────────────────

    async def get_with_ttl(self, key: str) -> tuple[Any | None, float | None]:
        """Get value and remaining TTL.

        Default implementation returns (data, None).
        Override for backends that support TTL tracking.

        Args:
            key: Cache key

        Returns:
            Tuple of (data, remaining_ttl_seconds)
        """
        data = await self.get(key)
        return (data, None)

    # ─────────────────────────────────────────────────────────────
    # Lock methods
    # ─────────────────────────────────────────────────────────────

    async def lock(self, key: str, ttl: float = 15.0) -> UnlockFunc:
        """Acquire lock for a cache key with timeout.

        Used for thundering herd protection: only one request
        executes the handler, others wait for cached result.

        Args:
            key: Cache key to lock
            ttl: Lock timeout in seconds (raises TimeoutError if exceeded)

        Returns:
            Async function to release lock

        Raises:
            asyncio.TimeoutError: If lock acquisition times out
        """
        lock_key = self.prefix + key + "-lock"
        return await asyncio.wait_for(self._lock.acquire(lock_key), timeout=ttl)

    @asynccontextmanager
    async def lock_context(self, key: str, ttl: float = 15.0) -> AsyncIterator[None]:
        """Async context manager for lock acquisition.

        Provides convenient `async with` syntax for lock management.

        Args:
            key: Cache key to lock
            ttl: Lock timeout in seconds

        Yields:
            None (lock is held during context)

        Raises:
            asyncio.TimeoutError: If lock acquisition times out

        Example:
            >>> async with cacher.lock_context("user:123"):
            ...     # critical section - lock is held
            ...     result = await expensive_operation()
        """
        unlock = await self.lock(key, ttl)
        try:
            yield
        finally:
            await unlock()

    async def try_lock(self, key: str, ttl: int = 15) -> UnlockFunc | None:
        """Try to acquire lock without waiting.

        Args:
            key: Cache key to lock
            ttl: Lock timeout in seconds (unused in memory lock)

        Returns:
            Unlock function if acquired, None if already locked
        """
        return await self._lock.try_acquire(self.prefix + key + "-lock")

    # ─────────────────────────────────────────────────────────────
    # Key generation
    # ─────────────────────────────────────────────────────────────

    def get_cache_key(
        self,
        action_name: str,
        params: dict[str, Any] | None,
        meta: dict[str, Any] | None,
        keys: list[str] | None = None,
        action_keygen: KeygenFunc | None = None,
    ) -> str:
        """Generate cache key for an action call.

        Priority:
            1. action_keygen (per-action custom)
            2. self.keygen (cacher-wide custom)
            3. default_keygen (built-in)

        Args:
            action_name: Full action name (e.g., "users.get")
            params: Request parameters
            meta: Request metadata
            keys: Specific param/meta keys to include
            action_keygen: Action-specific key generator

        Returns:
            Cache key string
        """
        if action_keygen is not None:
            return action_keygen(action_name, params or {}, meta or {}, keys)

        if self.keygen is not None:
            return self.keygen(action_name, params or {}, meta or {}, keys)

        return self.default_keygen(action_name, params, meta, keys)

    def default_keygen(
        self,
        action_name: str,
        params: dict[str, Any] | None,
        meta: dict[str, Any] | None,
        keys: list[str] | None,
    ) -> str:
        """Default cache key generator.

        Key format: "{action_name}:{hashed_params}"

        Args:
            action_name: Full action name
            params: Request parameters
            meta: Request metadata
            keys: Specific fields to include (None = all params)

        Returns:
            Cache key string

        Examples:
            >>> cacher.default_keygen("users.get", None, None, None)
            "users.get:"
            >>> cacher.default_keygen("users.get", {"id": 123}, None, ["id"])
            "users.get:123"
            >>> cacher.default_keygen("users.get", {"id": 123}, {"tenantId": "t1"}, ["id", "#tenantId"])
            "users.get:123|t1"
        """
        # Always include colon separator (Moleculer.js compatible)
        key_prefix = f"{action_name}:"

        if not params and not meta:
            return key_prefix

        if keys is not None and len(keys) > 0:
            # Specific keys requested
            if len(keys) == 1:
                # Fast path: single key
                val = self._get_param_meta_value(keys[0], params, meta)
                return key_prefix + self._hash_key(self._stringify(val))

            # Multiple keys: combine with separator
            parts: list[str] = []
            for key in keys:
                val = self._get_param_meta_value(key, params, meta)
                parts.append(self._stringify(val))

            return key_prefix + self._hash_key("|".join(parts))

        # No specific keys: hash entire params
        return key_prefix + self._hash_key(self._serialize(params or {}))

    def _get_param_meta_value(
        self,
        key: str,
        params: dict[str, Any] | None,
        meta: dict[str, Any] | None,
    ) -> Any:
        """Get value from params or meta by key.

        Keys starting with "#" are looked up in meta.

        Args:
            key: Field name (or "#field" for meta)
            params: Request parameters
            meta: Request metadata

        Returns:
            Value or None if not found
        """
        if key.startswith("#"):
            # From meta: "#tenantId" -> meta["tenantId"]
            meta_key = key[1:]
            return (meta or {}).get(meta_key)
        else:
            # From params
            return (params or {}).get(key)

    def _hash_key(self, key: str) -> str:
        """Hash key if too long.

        Uses SHA256 base64 for keys exceeding max_params_length.

        Args:
            key: Original key string

        Returns:
            Original key or hashed version
        """
        if len(key) <= self.max_params_length:
            return key

        # SHA256 hash, base64 encoded (base64 imported at module level)
        hash_bytes = hashlib.sha256(key.encode()).digest()
        hash_b64 = base64.b64encode(hash_bytes).decode()

        # Keep prefix + hash
        prefix_len = min(20, self.max_params_length // 4)
        return key[:prefix_len] + hash_b64

    def _stringify(self, value: Any) -> str:
        """Convert value to string for key generation.

        Args:
            value: Any value

        Returns:
            String representation
        """
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, (int, float, bool)):
            return str(value)
        # Complex types: serialize
        return self._serialize(value)

    def _serialize(self, data: Any) -> str:
        """Serialize data to JSON string.

        Args:
            data: Data to serialize

        Returns:
            JSON string (sorted keys for consistency)
        """
        return json.dumps(data, sort_keys=True, separators=(",", ":"))

    # ─────────────────────────────────────────────────────────────
    # Middleware factory
    # ─────────────────────────────────────────────────────────────

    def middleware(self) -> dict[str, Any]:
        """Create caching middleware for broker.

        Returns middleware dict with:
            - name: "Cacher"
            - local_action: Hook to wrap action handlers

        Returns:
            Middleware configuration dict
        """
        # Import here to avoid circular dependency
        from ..middleware.caching import create_caching_middleware  # noqa: PLC0415

        return create_caching_middleware(self)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"ttl={self.ttl}, "
            f"prefix={self.prefix!r}, "
            f"connected={self.connected})"
        )
