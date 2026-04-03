"""Unit tests for MemoryLRUCacher.

Tests cover:
    - Constructor defaults and custom options
    - Basic set/get operations
    - TTL expiration
    - LRU eviction behavior
    - delete, clean, has, size, keys
    - get_with_ttl (always returns None for TTL)
    - Cacher resolver by name
"""

from __future__ import annotations

import asyncio
import time

import pytest

from moleculerpy.cacher import resolve
from moleculerpy.cacher.memory_lru import MemoryLRUCacher


class TestMemoryLRUCacherConstructor:
    """Constructor parameter tests."""

    def test_default_max(self) -> None:
        """Default max is 1000."""
        cacher = MemoryLRUCacher()
        assert cacher.max == 1000

    def test_custom_max(self) -> None:
        """Custom max is stored correctly."""
        cacher = MemoryLRUCacher(max=5)
        assert cacher.max == 5

    def test_default_ttl_none(self) -> None:
        """Default TTL is None (no expiry)."""
        cacher = MemoryLRUCacher()
        assert cacher.ttl is None

    def test_custom_ttl(self) -> None:
        """Custom TTL is stored correctly."""
        cacher = MemoryLRUCacher(ttl=60)
        assert cacher.ttl == 60

    def test_initial_size_zero(self) -> None:
        """Cache starts empty."""
        cacher = MemoryLRUCacher(max=10)
        assert cacher.size() == 0


class TestMemoryLRUCacherBasic:
    """Basic set/get/delete/clean operations."""

    @pytest.fixture
    def cacher(self) -> MemoryLRUCacher:
        return MemoryLRUCacher(max=100, ttl=60)

    @pytest.mark.asyncio
    async def test_set_and_get_roundtrip(self, cacher: MemoryLRUCacher) -> None:
        """Basic set then get returns the stored value."""
        await cacher.set("key1", {"name": "Alice"})
        result = await cacher.get("key1")
        assert result == {"name": "Alice"}

    @pytest.mark.asyncio
    async def test_get_missing_key_returns_none(self, cacher: MemoryLRUCacher) -> None:
        """get() for unknown key returns None."""
        result = await cacher.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_overwrites_existing(self, cacher: MemoryLRUCacher) -> None:
        """set() on existing key replaces the value."""
        await cacher.set("key1", "old")
        await cacher.set("key1", "new")
        result = await cacher.get("key1")
        assert result == "new"

    @pytest.mark.asyncio
    async def test_delete_single_key(self, cacher: MemoryLRUCacher) -> None:
        """delete() removes a single key."""
        await cacher.set("key1", "value1")
        await cacher.delete("key1")
        assert await cacher.get("key1") is None

    @pytest.mark.asyncio
    async def test_delete_list_of_keys(self, cacher: MemoryLRUCacher) -> None:
        """delete() accepts a list and removes all specified keys."""
        await cacher.set("key1", "v1")
        await cacher.set("key2", "v2")
        await cacher.set("key3", "v3")

        await cacher.delete(["key1", "key2"])

        assert await cacher.get("key1") is None
        assert await cacher.get("key2") is None
        assert await cacher.get("key3") == "v3"

    @pytest.mark.asyncio
    async def test_clean_all(self, cacher: MemoryLRUCacher) -> None:
        """clean('**') clears all entries."""
        await cacher.set("key1", "v1")
        await cacher.set("key2", "v2")

        await cacher.clean("**")

        assert cacher.size() == 0

    @pytest.mark.asyncio
    async def test_clean_pattern(self, cacher: MemoryLRUCacher) -> None:
        """clean('users.*') removes only matching keys."""
        await cacher.set("users.1", "Alice")
        await cacher.set("users.2", "Bob")
        await cacher.set("products.1", "Widget")

        await cacher.clean("users.*")

        assert await cacher.get("users.1") is None
        assert await cacher.get("users.2") is None
        assert await cacher.get("products.1") == "Widget"


class TestMemoryLRUCacherTTL:
    """TTL expiration behavior."""

    @pytest.mark.asyncio
    async def test_set_with_ttl_expires(self) -> None:
        """Item set with TTL=1 returns None after expiry."""
        cacher = MemoryLRUCacher(max=10, ttl=1)

        await cacher.set("key1", "value1")
        assert await cacher.get("key1") == "value1"

        await asyncio.sleep(1.1)

        assert await cacher.get("key1") is None

    @pytest.mark.asyncio
    async def test_per_key_ttl_override(self) -> None:
        """Per-key TTL overrides the cacher default."""
        cacher = MemoryLRUCacher(max=10, ttl=30)

        await cacher.set("short", "v", ttl=1)
        await cacher.set("long", "v")

        await asyncio.sleep(1.1)

        assert await cacher.get("short") is None
        assert await cacher.get("long") == "v"

    @pytest.mark.asyncio
    async def test_no_ttl_means_no_expiry(self) -> None:
        """None TTL means item never expires."""
        cacher = MemoryLRUCacher(max=10, ttl=None)

        await cacher.set("key1", "value1")

        full_key = cacher.prefix + "key1"
        item = cacher.cache[full_key]
        assert item.expire is None

    @pytest.mark.asyncio
    async def test_check_ttl_removes_expired(self) -> None:
        """_check_ttl() removes expired items and returns count."""
        cacher = MemoryLRUCacher(max=10, ttl=1)

        await cacher.set("key1", "v1")
        await cacher.set("key2", "v2")

        await asyncio.sleep(1.1)

        removed = cacher._check_ttl()
        assert removed == 2
        assert cacher.size() == 0


class TestMemoryLRUCacherEviction:
    """LRU eviction tests."""

    @pytest.mark.asyncio
    async def test_lru_eviction_on_overflow(self) -> None:
        """When max=3, inserting a 4th item evicts the least recently used."""
        cacher = MemoryLRUCacher(max=3)

        await cacher.set("a", 1)
        await cacher.set("b", 2)
        await cacher.set("c", 3)

        # Insert 4th: 'a' was inserted first and never accessed → evicted
        await cacher.set("d", 4)

        assert cacher.size() == 3
        assert await cacher.get("a") is None  # evicted
        assert await cacher.get("b") == 2
        assert await cacher.get("c") == 3
        assert await cacher.get("d") == 4

    @pytest.mark.asyncio
    async def test_get_updates_recency(self) -> None:
        """get() moves accessed item to most-recently-used position."""
        cacher = MemoryLRUCacher(max=3)

        await cacher.set("a", 1)
        await cacher.set("b", 2)
        await cacher.set("c", 3)

        # Access 'a' → 'a' is now most recent; 'b' is LRU
        await cacher.get("a")

        # Insert 4th → 'b' should be evicted (it's now LRU)
        await cacher.set("d", 4)

        assert cacher.size() == 3
        assert await cacher.get("b") is None  # evicted
        assert await cacher.get("a") == 1
        assert await cacher.get("c") == 3
        assert await cacher.get("d") == 4

    @pytest.mark.asyncio
    async def test_correct_item_evicted_not_recently_accessed(self) -> None:
        """The item that was NOT recently accessed is evicted, not others."""
        cacher = MemoryLRUCacher(max=2)

        await cacher.set("x", 10)
        await cacher.set("y", 20)

        # Access 'x' to make 'y' LRU
        await cacher.get("x")

        # Overflow: 'y' should be evicted
        await cacher.set("z", 30)

        assert await cacher.get("y") is None
        assert await cacher.get("x") == 10
        assert await cacher.get("z") == 30

    @pytest.mark.asyncio
    async def test_size_never_exceeds_max(self) -> None:
        """Size never goes above the configured max."""
        max_size = 5
        cacher = MemoryLRUCacher(max=max_size)

        for i in range(20):
            await cacher.set(f"key{i}", i)
            assert cacher.size() <= max_size


class TestMemoryLRUCacherExtended:
    """Extended API: get_with_ttl, has, size, keys."""

    @pytest.fixture
    def cacher(self) -> MemoryLRUCacher:
        return MemoryLRUCacher(max=10, ttl=60)

    @pytest.mark.asyncio
    async def test_get_with_ttl_returns_data_and_none(self, cacher: MemoryLRUCacher) -> None:
        """get_with_ttl() returns (data, None) — TTL not available for LRU."""
        await cacher.set("key1", "value1")
        data, ttl = await cacher.get_with_ttl("key1")

        assert data == "value1"
        assert ttl is None

    @pytest.mark.asyncio
    async def test_get_with_ttl_missing_key(self, cacher: MemoryLRUCacher) -> None:
        """get_with_ttl() for missing key returns (None, None)."""
        data, ttl = await cacher.get_with_ttl("nonexistent")
        assert data is None
        assert ttl is None

    @pytest.mark.asyncio
    async def test_has_returns_true_for_existing(self, cacher: MemoryLRUCacher) -> None:
        """has() returns True for existing key."""
        await cacher.set("key1", "value1")
        assert cacher.has("key1") is True

    @pytest.mark.asyncio
    async def test_has_returns_false_for_missing(self, cacher: MemoryLRUCacher) -> None:
        """has() returns False for missing key."""
        assert cacher.has("nonexistent") is False

    @pytest.mark.asyncio
    async def test_size_returns_count(self, cacher: MemoryLRUCacher) -> None:
        """size() returns the correct number of entries."""
        assert cacher.size() == 0
        await cacher.set("k1", "v1")
        assert cacher.size() == 1
        await cacher.set("k2", "v2")
        assert cacher.size() == 2

    @pytest.mark.asyncio
    async def test_keys_returns_list_without_prefix(self, cacher: MemoryLRUCacher) -> None:
        """keys() returns all keys stripped of the prefix."""
        await cacher.set("alpha", 1)
        await cacher.set("beta", 2)

        result = cacher.keys()
        assert set(result) == {"alpha", "beta"}


class TestMemoryLRUCacherResolver:
    """Cacher resolver integration."""

    def test_resolve_by_name_MemoryLRU(self) -> None:
        """resolve('MemoryLRU') returns MemoryLRUCacher."""
        cacher = resolve("MemoryLRU")
        assert isinstance(cacher, MemoryLRUCacher)

    def test_resolve_by_name_memory_lru(self) -> None:
        """resolve('memory-lru') returns MemoryLRUCacher."""
        cacher = resolve("memory-lru")
        assert isinstance(cacher, MemoryLRUCacher)

    def test_resolve_dict_type_MemoryLRU_with_options(self) -> None:
        """resolve({type: 'MemoryLRU', max: 50, ttl: 30}) creates configured cacher."""
        cacher = resolve({"type": "MemoryLRU", "max": 50, "ttl": 30})
        assert isinstance(cacher, MemoryLRUCacher)
        assert cacher.max == 50
        assert cacher.ttl == 30
