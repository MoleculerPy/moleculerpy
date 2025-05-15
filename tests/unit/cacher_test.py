"""Unit tests for caching system.

Tests cover:
    - MemoryCacher operations (get, set, delete, clean)
    - TTL expiration (lazy + periodic)
    - Cache key generation
    - Clone behavior
    - Cacher resolver
"""

from __future__ import annotations

import asyncio
import time
from typing import Any
from unittest.mock import MagicMock

import pytest

from moleculerpy.cacher import MemoryCacher, resolve
from moleculerpy.cacher.base import CacheItem


class TestCacheItem:
    """Test CacheItem dataclass."""

    def test_not_expired_when_no_expire(self) -> None:
        """Item with no expire never expires."""
        item = CacheItem(data="test", expire=None)
        assert not item.is_expired(time.time() * 1000)

    def test_not_expired_before_time(self) -> None:
        """Item not expired before its time."""
        now = time.time() * 1000
        item = CacheItem(data="test", expire=now + 10000)  # +10s
        assert not item.is_expired(now)

    def test_expired_after_time(self) -> None:
        """Item expired after its time."""
        now = time.time() * 1000
        item = CacheItem(data="test", expire=now - 1000)  # -1s
        assert item.is_expired(now)


class TestMemoryCacherBasic:
    """Basic MemoryCacher operations."""

    @pytest.fixture
    def cacher(self) -> MemoryCacher:
        """Create cacher without starting cleanup task."""
        return MemoryCacher(ttl=60)

    @pytest.mark.asyncio
    async def test_set_and_get(self, cacher: MemoryCacher) -> None:
        """Basic set and get."""
        await cacher.set("key1", {"name": "Alice"})
        result = await cacher.get("key1")

        assert result == {"name": "Alice"}

    @pytest.mark.asyncio
    async def test_get_missing_returns_none(self, cacher: MemoryCacher) -> None:
        """Get missing key returns None."""
        result = await cacher.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_single_key(self, cacher: MemoryCacher) -> None:
        """Delete removes a key."""
        await cacher.set("key1", "value1")
        await cacher.delete("key1")

        result = await cacher.get("key1")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_multiple_keys(self, cacher: MemoryCacher) -> None:
        """Delete multiple keys at once."""
        await cacher.set("key1", "value1")
        await cacher.set("key2", "value2")
        await cacher.set("key3", "value3")

        await cacher.delete(["key1", "key2"])

        assert await cacher.get("key1") is None
        assert await cacher.get("key2") is None
        assert await cacher.get("key3") == "value3"

    @pytest.mark.asyncio
    async def test_clean_all(self, cacher: MemoryCacher) -> None:
        """Clean with ** clears entire cache."""
        await cacher.set("key1", "value1")
        await cacher.set("key2", "value2")

        await cacher.clean("**")

        assert cacher.size() == 0

    @pytest.mark.asyncio
    async def test_clean_pattern(self, cacher: MemoryCacher) -> None:
        """Clean with pattern removes matching keys."""
        await cacher.set("user:1", "Alice")
        await cacher.set("user:2", "Bob")
        await cacher.set("product:1", "Widget")

        await cacher.clean("user:*")

        assert await cacher.get("user:1") is None
        assert await cacher.get("user:2") is None
        assert await cacher.get("product:1") == "Widget"

    @pytest.mark.asyncio
    async def test_has_returns_true_for_existing(self, cacher: MemoryCacher) -> None:
        """has() returns True for existing key."""
        await cacher.set("key1", "value1")
        assert cacher.has("key1")

    @pytest.mark.asyncio
    async def test_has_returns_false_for_missing(self, cacher: MemoryCacher) -> None:
        """has() returns False for missing key."""
        assert not cacher.has("nonexistent")

    @pytest.mark.asyncio
    async def test_size(self, cacher: MemoryCacher) -> None:
        """size() returns correct count."""
        assert cacher.size() == 0

        await cacher.set("key1", "value1")
        assert cacher.size() == 1

        await cacher.set("key2", "value2")
        assert cacher.size() == 2

    @pytest.mark.asyncio
    async def test_keys(self, cacher: MemoryCacher) -> None:
        """keys() returns all keys without prefix."""
        await cacher.set("key1", "value1")
        await cacher.set("key2", "value2")

        keys = cacher.keys()
        assert set(keys) == {"key1", "key2"}


class TestMemoryCacherTTL:
    """TTL expiration tests."""

    @pytest.mark.asyncio
    async def test_get_returns_none_for_expired(self) -> None:
        """Expired items return None on get (lazy check)."""
        cacher = MemoryCacher(ttl=1)  # 1 second TTL

        await cacher.set("key1", "value1")
        assert await cacher.get("key1") == "value1"

        # Wait for expiration
        await asyncio.sleep(1.1)

        assert await cacher.get("key1") is None

    @pytest.mark.asyncio
    async def test_per_key_ttl_override(self) -> None:
        """Per-key TTL overrides default."""
        cacher = MemoryCacher(ttl=10)  # 10 second default

        await cacher.set("short", "value", ttl=1)  # 1 second override
        await cacher.set("long", "value")  # Uses default

        await asyncio.sleep(1.1)

        assert await cacher.get("short") is None
        assert await cacher.get("long") == "value"

    @pytest.mark.asyncio
    async def test_no_ttl_means_no_expiry(self) -> None:
        """None TTL means item never expires."""
        cacher = MemoryCacher(ttl=None)

        await cacher.set("key1", "value1")

        # Check internal expire is None
        full_key = cacher.prefix + "key1"
        item = cacher.cache[full_key]
        assert item.expire is None

    @pytest.mark.asyncio
    async def test_get_with_ttl(self) -> None:
        """get_with_ttl returns data and remaining TTL."""
        cacher = MemoryCacher(ttl=10)

        await cacher.set("key1", "value1")
        data, remaining = await cacher.get_with_ttl("key1")

        assert data == "value1"
        assert remaining is not None
        assert 9 < remaining <= 10  # Should be close to 10s

    @pytest.mark.asyncio
    async def test_get_with_ttl_missing_key(self) -> None:
        """get_with_ttl returns (None, None) for missing key."""
        cacher = MemoryCacher()

        data, ttl = await cacher.get_with_ttl("nonexistent")
        assert data is None
        assert ttl is None

    @pytest.mark.asyncio
    async def test_check_ttl_removes_expired(self) -> None:
        """_check_ttl removes expired items."""
        cacher = MemoryCacher(ttl=1)

        await cacher.set("key1", "value1")
        await cacher.set("key2", "value2")

        # Wait for expiration
        await asyncio.sleep(1.1)

        # Manually trigger cleanup
        removed = cacher._check_ttl()

        assert removed == 2
        assert cacher.size() == 0

    @pytest.mark.asyncio
    async def test_has_removes_expired_on_check(self) -> None:
        """has() removes expired item and returns False."""
        cacher = MemoryCacher(ttl=1)

        await cacher.set("key1", "value1")
        assert cacher.has("key1")

        await asyncio.sleep(1.1)

        assert not cacher.has("key1")
        assert cacher.size() == 0  # Item removed


class TestMemoryCacherClone:
    """Clone behavior tests."""

    @pytest.mark.asyncio
    async def test_clone_false_shares_reference(self) -> None:
        """Without clone, mutations affect cached data."""
        cacher = MemoryCacher(clone=False)

        original = {"name": "Alice", "items": [1, 2, 3]}
        await cacher.set("key1", original)

        # Mutate original
        original["name"] = "Bob"
        original["items"].append(4)

        # Cached data is also mutated (same reference)
        cached = await cacher.get("key1")
        assert cached["name"] == "Bob"
        assert cached["items"] == [1, 2, 3, 4]

    @pytest.mark.asyncio
    async def test_clone_true_isolates_data(self) -> None:
        """With clone=True, mutations don't affect cache."""
        cacher = MemoryCacher(clone=True)

        original = {"name": "Alice", "items": [1, 2, 3]}
        await cacher.set("key1", original)

        # Mutate original
        original["name"] = "Bob"
        original["items"].append(4)

        # Cached data is unchanged (deep copy)
        cached = await cacher.get("key1")
        assert cached["name"] == "Alice"
        assert cached["items"] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_clone_true_returns_copy(self) -> None:
        """With clone=True, get returns a copy."""
        cacher = MemoryCacher(clone=True)

        await cacher.set("key1", {"name": "Alice"})

        # Get and mutate
        result1 = await cacher.get("key1")
        result1["name"] = "Bob"

        # Next get still returns original
        result2 = await cacher.get("key1")
        assert result2["name"] == "Alice"


class TestCacheKeyGeneration:
    """Cache key generation tests."""

    @pytest.fixture
    def cacher(self) -> MemoryCacher:
        return MemoryCacher()

    def test_key_without_params(self, cacher: MemoryCacher) -> None:
        """Action name with trailing colon when no params."""
        key = cacher.default_keygen("users.get", None, None, None)
        assert key == "users.get:"

    def test_key_with_single_key(self, cacher: MemoryCacher) -> None:
        """Single key extracted from params."""
        key = cacher.default_keygen(
            "users.get",
            {"id": 123, "extra": "ignored"},
            None,
            ["id"],
        )
        assert key == "users.get:123"

    def test_key_with_multiple_keys(self, cacher: MemoryCacher) -> None:
        """Multiple keys combined with separator."""
        key = cacher.default_keygen(
            "users.get",
            {"id": 123, "tenant": "acme"},
            None,
            ["id", "tenant"],
        )
        assert key == "users.get:123|acme"

    def test_key_with_meta_key(self, cacher: MemoryCacher) -> None:
        """Meta keys prefixed with #."""
        key = cacher.default_keygen(
            "users.get",
            {"id": 123},
            {"tenantId": "t1"},
            ["id", "#tenantId"],
        )
        assert key == "users.get:123|t1"

    def test_key_hashes_all_params_when_no_keys(self, cacher: MemoryCacher) -> None:
        """Without specific keys, entire params are hashed."""
        key = cacher.default_keygen(
            "users.search",
            {"query": "test", "limit": 10},
            None,
            None,
        )
        assert key.startswith("users.search:")
        # JSON serialized and possibly hashed
        assert len(key) > len("users.search:")

    def test_long_key_is_hashed(self) -> None:
        """Keys exceeding max_params_length are hashed."""
        cacher = MemoryCacher(max_params_length=50)

        long_value = "x" * 100
        key = cacher.default_keygen(
            "action",
            {"long": long_value},
            None,
            ["long"],
        )

        # Should be much shorter than original
        assert len(key) < len(long_value)
        # Contains base64 hash
        assert "=" in key or "/" in key or "+" in key or len(key) == len("action:") + 20 + 44

    def test_custom_keygen(self) -> None:
        """Custom keygen function is used."""

        def my_keygen(
            action: str,
            params: dict[str, Any],
            meta: dict[str, Any],
            keys: list[str] | None,
        ) -> str:
            return f"custom:{action}:{params.get('id', 'none')}"

        cacher = MemoryCacher(keygen=my_keygen)

        key = cacher.get_cache_key("users.get", {"id": 42}, {}, None, None)
        assert key == "custom:users.get:42"

    def test_action_keygen_has_priority(self) -> None:
        """Action-specific keygen overrides cacher keygen."""

        def cacher_keygen(*args: Any) -> str:
            return "cacher-key"

        def action_keygen(*args: Any) -> str:
            return "action-key"

        cacher = MemoryCacher(keygen=cacher_keygen)

        key = cacher.get_cache_key("action", {}, {}, None, action_keygen)
        assert key == "action-key"


class TestCacherResolver:
    """Test cacher resolver function."""

    def test_resolve_none(self) -> None:
        """None returns None."""
        assert resolve(None) is None

    def test_resolve_false(self) -> None:
        """False returns None."""
        assert resolve(False) is None

    def test_resolve_true(self) -> None:
        """True returns MemoryCacher."""
        cacher = resolve(True)
        assert isinstance(cacher, MemoryCacher)

    def test_resolve_string_memory(self) -> None:
        """'memory' string returns MemoryCacher."""
        cacher = resolve("memory")
        assert isinstance(cacher, MemoryCacher)

    def test_resolve_string_unknown_raises(self) -> None:
        """Unknown string raises ValueError."""
        with pytest.raises(ValueError, match="Unknown cacher type"):
            resolve("unknown")

    def test_resolve_dict_with_type(self) -> None:
        """Dict with type and options."""
        cacher = resolve({"type": "memory", "ttl": 30, "clone": True})

        assert isinstance(cacher, MemoryCacher)
        assert cacher.ttl == 30
        assert cacher.clone is True

    def test_resolve_dict_default_type(self) -> None:
        """Dict without type defaults to memory."""
        cacher = resolve({"ttl": 60})

        assert isinstance(cacher, MemoryCacher)
        assert cacher.ttl == 60

    def test_resolve_existing_cacher(self) -> None:
        """Existing cacher instance returned as-is."""
        original = MemoryCacher(ttl=99)
        result = resolve(original)

        assert result is original

    def test_resolve_invalid_type_raises(self) -> None:
        """Invalid type raises TypeError."""
        with pytest.raises(TypeError, match="Invalid cacher config type"):
            resolve(123)  # type: ignore[arg-type]


class TestMemoryCacherPrefix:
    """Test key prefix handling."""

    @pytest.mark.asyncio
    async def test_default_prefix(self) -> None:
        """Default prefix is MOL-."""
        cacher = MemoryCacher()
        assert cacher.prefix == "MOL-"

        await cacher.set("key1", "value1")

        # Internal key has prefix
        assert "MOL-key1" in cacher.cache

    @pytest.mark.asyncio
    async def test_custom_prefix(self) -> None:
        """Custom prefix is used."""
        cacher = MemoryCacher(prefix="APP-")
        assert cacher.prefix == "APP-"

        await cacher.set("key1", "value1")
        assert "APP-key1" in cacher.cache

    @pytest.mark.asyncio
    async def test_init_adds_namespace(self) -> None:
        """init() adds namespace to prefix."""
        cacher = MemoryCacher(prefix="MOL-")

        # Mock broker with settings
        broker = MagicMock()
        broker.settings.namespace = "myapp"

        cacher.init(broker)

        assert cacher.prefix == "MOL-myapp-"


class TestMemoryCacherLock:
    """Test lock integration."""

    @pytest.fixture
    def cacher(self) -> MemoryCacher:
        return MemoryCacher()

    @pytest.mark.asyncio
    async def test_lock_acquire_release(self, cacher: MemoryCacher) -> None:
        """lock() and unlock work correctly."""
        unlock = await cacher.lock("key1")

        assert cacher._lock.is_locked(cacher.prefix + "key1-lock")

        await unlock()

        assert not cacher._lock.is_locked(cacher.prefix + "key1-lock")

    @pytest.mark.asyncio
    async def test_try_lock_success(self, cacher: MemoryCacher) -> None:
        """try_lock succeeds when not locked."""
        unlock = await cacher.try_lock("key1")
        assert unlock is not None

        await unlock()

    @pytest.mark.asyncio
    async def test_try_lock_fails_when_locked(self, cacher: MemoryCacher) -> None:
        """try_lock returns None when already locked."""
        unlock1 = await cacher.lock("key1")

        unlock2 = await cacher.try_lock("key1")
        assert unlock2 is None

        await unlock1()


class TestMemoryCacherLifecycle:
    """Test start/stop lifecycle."""

    @pytest.mark.asyncio
    async def test_start_creates_cleanup_task(self) -> None:
        """start() creates background cleanup task."""
        cacher = MemoryCacher(cleanup_interval=100)  # Long interval

        assert cacher._cleanup_task is None

        await cacher.start()

        assert cacher._cleanup_task is not None
        assert not cacher._cleanup_task.done()

        await cacher.stop()

    @pytest.mark.asyncio
    async def test_stop_cancels_cleanup_task(self) -> None:
        """stop() cancels cleanup task and clears cache."""
        cacher = MemoryCacher()

        await cacher.start()
        await cacher.set("key1", "value1")

        await cacher.stop()

        assert cacher._cleanup_task is None
        assert cacher.size() == 0

    @pytest.mark.asyncio
    async def test_stop_clears_locks(self) -> None:
        """stop() clears all locks."""
        cacher = MemoryCacher()

        await cacher.lock("key1")
        await cacher.lock("key2")

        await cacher.stop()

        # Locks should be cleared
        assert not cacher._lock.is_locked(cacher.prefix + "key1-lock")


class TestMemoryCacherRepr:
    """Test string representation."""

    def test_repr(self) -> None:
        """repr shows key attributes."""
        cacher = MemoryCacher(ttl=60, clone=True)

        rep = repr(cacher)
        assert "MemoryCacher" in rep
        assert "ttl=60" in rep
        assert "clone=True" in rep
