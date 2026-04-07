"""Unit + integration tests for Redis Cacher.

Tests both mock-based unit tests and real Redis integration tests.
Integration tests require Redis on localhost:6381 (skip if unavailable).
"""

import asyncio
import socket
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from moleculerpy.cacher.redis import RedisCacher


def _redis_available() -> bool:
    """Check if Redis is available for integration tests."""
    try:
        with socket.create_connection(("localhost", 6381), timeout=1.0):
            return True
    except OSError:
        return False


REDIS_URL = "redis://localhost:6381/15"  # Use DB 15 for test isolation
skip_no_redis = pytest.mark.skipif(not _redis_available(), reason="Redis not available on 6381")


# ---------------------------------------------------------------------------
# Unit tests (no Redis needed)
# ---------------------------------------------------------------------------


class TestRedisCacherInit:
    def test_from_url(self):
        cacher = RedisCacher(REDIS_URL)
        assert cacher.redis_url == REDIS_URL
        assert cacher.prefix == "MOL-"

    def test_from_dict(self):
        cacher = RedisCacher({"redis": {"host": "localhost", "port": 6379}, "prefix": "TEST-"})
        assert cacher.prefix == "TEST-"
        assert cacher.redis_url is None

    def test_with_ttl(self):
        cacher = RedisCacher({"redis": REDIS_URL, "ttl": 60})
        assert cacher.default_ttl == 60

    def test_init_with_broker(self):
        cacher = RedisCacher(REDIS_URL)
        broker = MagicMock()
        broker.namespace = "myapp"
        broker.settings = MagicMock()
        broker.settings.namespace = "myapp"
        broker._create_logger = MagicMock(return_value=MagicMock())
        cacher.init(broker)
        assert cacher.prefix == "MOL-myapp-"


# ---------------------------------------------------------------------------
# Integration tests (real Redis)
# ---------------------------------------------------------------------------


@skip_no_redis
class TestRedisCacherIntegration:
    @pytest_asyncio.fixture
    async def cacher(self):
        c = RedisCacher({"redis": REDIS_URL, "ttl": 10, "prefix": "TEST-"})
        broker = MagicMock()
        broker.namespace = None
        broker._create_logger = MagicMock(return_value=MagicMock())
        broker.settings = MagicMock()
        broker.settings.namespace = None
        c.init(broker)
        await c.start()
        # Clean test prefix before test
        await c.clean("*")
        yield c
        await c.clean("*")
        await c.stop()

    @pytest.mark.asyncio
    async def test_start_stop_lifecycle(self):
        c = RedisCacher(REDIS_URL)
        broker = MagicMock()
        broker.namespace = None
        broker._create_logger = MagicMock(return_value=MagicMock())
        broker.settings = MagicMock()
        broker.settings.namespace = None
        c.init(broker)

        assert not c.connected
        await c.start()
        assert c.connected
        await c.stop()
        assert not c.connected

    @pytest.mark.asyncio
    async def test_get_set(self, cacher):
        await cacher.set("user:1", {"name": "Alice", "age": 30})
        result = await cacher.get("user:1")
        assert result == {"name": "Alice", "age": 30}

    @pytest.mark.asyncio
    async def test_get_miss(self, cacher):
        result = await cacher.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_with_ttl(self, cacher):
        await cacher.set("expiring", "value", ttl=1)
        assert await cacher.get("expiring") == "value"
        await asyncio.sleep(1.5)
        assert await cacher.get("expiring") is None

    @pytest.mark.asyncio
    async def test_delete_single(self, cacher):
        await cacher.set("to-delete", "data")
        await cacher.delete("to-delete")
        assert await cacher.get("to-delete") is None

    @pytest.mark.asyncio
    async def test_delete_multiple(self, cacher):
        await cacher.set("a", 1)
        await cacher.set("b", 2)
        await cacher.delete(["a", "b"])
        assert await cacher.get("a") is None
        assert await cacher.get("b") is None

    @pytest.mark.asyncio
    async def test_clean_pattern(self, cacher):
        await cacher.set("user:1", "alice")
        await cacher.set("user:2", "bob")
        await cacher.set("order:1", "pizza")
        await cacher.clean("user:*")
        assert await cacher.get("user:1") is None
        assert await cacher.get("user:2") is None
        assert await cacher.get("order:1") == "pizza"

    @pytest.mark.asyncio
    async def test_get_with_ttl(self, cacher):
        await cacher.set("ttl-test", "data", ttl=30)
        data, ttl = await cacher.get_with_ttl("ttl-test")
        assert data == "data"
        assert ttl is not None
        assert 0 < ttl <= 30

    @pytest.mark.asyncio
    async def test_get_cache_keys(self, cacher):
        await cacher.set("key1", "v1")
        await cacher.set("key2", "v2")
        keys = await cacher.get_cache_keys()
        key_names = [k["key"] for k in keys]
        assert "key1" in key_names
        assert "key2" in key_names

    @pytest.mark.asyncio
    async def test_complex_values(self, cacher):
        """Test caching complex nested structures."""
        data = {
            "users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            "meta": {"total": 2, "page": 1},
            "flags": [True, False, None],
        }
        await cacher.set("complex", data)
        result = await cacher.get("complex")
        assert result == data

    @pytest.mark.asyncio
    async def test_clean_all(self, cacher):
        await cacher.set("x", 1)
        await cacher.set("y", 2)
        await cacher.clean("*")
        assert await cacher.get("x") is None
        assert await cacher.get("y") is None


@skip_no_redis
class TestRedisCacherWithBroker:
    """Test Redis cacher integrated with actual ServiceBroker."""

    @pytest.mark.asyncio
    async def test_broker_with_redis_cacher(self):
        """Full lifecycle: broker.start → cache action → broker.stop."""
        from moleculerpy import Service, ServiceBroker, Settings, action

        class UserService(Service):
            name = "users"

            @action(cache=True)
            async def get(self, ctx):
                return {"id": ctx.params["id"], "name": f"User-{ctx.params['id']}"}

        cacher = RedisCacher({"redis": REDIS_URL, "ttl": 60, "prefix": "BROKER-TEST-"})
        broker = ServiceBroker(
            id="cache-test",
            settings=Settings(transporter="memory://", log_level="CRITICAL"),
            cacher=cacher,
        )
        await broker.register(UserService())
        await broker.start()

        try:
            # First call — cache miss
            r1 = await broker.call("users.get", {"id": 42})
            assert r1["name"] == "User-42"

            # Second call — should hit cache (same result)
            r2 = await broker.call("users.get", {"id": 42})
            assert r2 == r1

            # Verify key exists in Redis
            keys = await cacher.get_cache_keys()
            assert len(keys) >= 1
        finally:
            await cacher.clean("*")
            await broker.stop()
