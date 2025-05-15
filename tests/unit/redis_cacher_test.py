"""Unit tests for RedisCacher."""

from unittest.mock import AsyncMock, Mock

import pytest

from moleculerpy.cacher import RedisCacher


class TestRedisCacherInit:
    """Test RedisCacher initialization."""

    def test_init_with_url_string(self):
        """Test initialization with Redis URL string."""
        cacher = RedisCacher("redis://localhost:6379/0")

        assert cacher.redis_url == "redis://localhost:6379/0"
        assert cacher.prefix == "MOL-"
        assert cacher.default_ttl is None
        assert cacher.connected is False

    def test_init_with_options_dict(self):
        """Test initialization with options dictionary."""
        cacher = RedisCacher(
            {
                "redis": {"host": "localhost", "port": 6379, "db": 0},
                "prefix": "myapp-",
                "ttl": 3600,
            }
        )

        assert cacher.prefix == "myapp-"
        assert cacher.default_ttl == 3600
        assert cacher.connected is False

    def test_init_with_prefix_from_broker_namespace(self):
        """Test that broker namespace updates prefix."""
        cacher = RedisCacher("redis://localhost:6379")

        broker = Mock(spec=["namespace", "logger", "_create_logger", "settings"])
        broker.namespace = "api"
        broker.logger = Mock()
        broker._create_logger = Mock(return_value=Mock())
        broker.settings = Mock()
        broker.settings.namespace = "api"

        cacher.init(broker)

        assert cacher.prefix == "MOL-api-"


class TestRedisCacherConnect:
    """Test RedisCacher connection management (integration with real Valkey)."""

    @pytest.mark.asyncio
    async def test_connect_to_valkey(self):
        """Test successful connection to real Valkey server."""
        cacher = RedisCacher("redis://localhost:6381/0")
        broker = Mock(spec=["namespace", "logger", "_create_logger", "settings"])
        broker.namespace = ""
        broker.logger = Mock()
        broker._create_logger = Mock(return_value=Mock())
        broker.settings = Mock()
        broker.settings.namespace = ""
        cacher.init(broker)

        # Real connection to Valkey (skip in restricted CI/sandbox envs)
        try:
            await cacher.connect()
        except Exception as err:
            pytest.skip(f"Valkey is not reachable in this environment: {err}")

        assert cacher.connected is True
        assert cacher.client is not None

        await cacher.disconnect()
        assert cacher.connected is False

    @pytest.mark.asyncio
    async def test_disconnect_cleans_up(self):
        """Test disconnect cleanup."""
        cacher = RedisCacher("redis://localhost:6379")
        broker = Mock(spec=["namespace", "logger", "_create_logger", "settings"])
        broker.namespace = ""
        broker.logger = Mock()
        broker._create_logger = Mock(return_value=Mock())
        broker.settings = Mock()
        broker.settings.namespace = ""
        cacher.init(broker)

        # Setup mock client
        mock_client = AsyncMock()
        mock_client.aclose = AsyncMock()
        cacher.client = mock_client
        cacher.connected = True

        await cacher.disconnect()

        assert cacher.connected is False
        assert cacher.client is None
        mock_client.aclose.assert_called_once()


class TestRedisCacherOperations:
    """Test Redis cache operations."""

    @pytest.fixture
    def cacher(self):
        """Create initialized cacher with mocked Redis."""
        cacher = RedisCacher("redis://localhost:6379/0")
        broker = Mock(spec=["namespace", "logger", "_create_logger", "settings"])
        broker.namespace = ""
        broker.logger = Mock()
        broker._create_logger = Mock(return_value=Mock())
        broker.settings = Mock()
        broker.settings.namespace = ""
        cacher.init(broker)

        # Mock client
        mock_client = AsyncMock()
        cacher.client = mock_client
        cacher.connected = True

        return cacher

    @pytest.mark.asyncio
    async def test_get_miss(self, cacher):
        """Test cache miss returns None."""
        cacher.client.get = AsyncMock(return_value=None)

        result = await cacher.get("nonexistent")

        assert result is None
        cacher.client.get.assert_called_once_with("MOL-nonexistent")

    @pytest.mark.asyncio
    async def test_get_hit(self, cacher):
        """Test cache hit returns deserialized data."""
        cached_data = b'{"user": "alice", "id": 123}'
        cacher.client.get = AsyncMock(return_value=cached_data)

        result = await cacher.get("user:123")

        assert result == {"user": "alice", "id": 123}
        cacher.client.get.assert_called_once_with("MOL-user:123")

    @pytest.mark.asyncio
    async def test_get_when_disconnected(self, cacher):
        """Test get returns None when not connected."""
        cacher.connected = False

        result = await cacher.get("key")

        assert result is None
        cacher.client.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_set_with_ttl(self, cacher):
        """Test set with TTL."""
        cacher.client.set = AsyncMock()

        await cacher.set("key", {"data": 123}, ttl=60)

        cacher.client.set.assert_called_once()
        call_args = cacher.client.set.call_args
        assert call_args[0][0] == "MOL-key"
        assert call_args[1]["ex"] == 60

    @pytest.mark.asyncio
    async def test_set_without_ttl(self, cacher):
        """Test set without TTL."""
        cacher.client.set = AsyncMock()

        await cacher.set("key", {"data": 123})

        cacher.client.set.assert_called_once()
        call_args = cacher.client.set.call_args
        assert call_args[0][0] == "MOL-key"
        assert "ex" not in call_args[1]

    @pytest.mark.asyncio
    async def test_set_uses_default_ttl(self, cacher):
        """Test set uses default TTL when not specified."""
        cacher.default_ttl = 3600
        cacher.client.set = AsyncMock()

        await cacher.set("key", "value")

        call_args = cacher.client.set.call_args
        assert call_args[1]["ex"] == 3600

    @pytest.mark.asyncio
    async def test_delete_single_key(self, cacher):
        """Test delete single key."""
        cacher.client.delete = AsyncMock(return_value=1)

        await cacher.delete("user:123")
        cacher.client.delete.assert_called_once_with("MOL-user:123")

    @pytest.mark.asyncio
    async def test_delete_multiple_keys(self, cacher):
        """Test delete multiple keys."""
        cacher.client.delete = AsyncMock(return_value=3)

        await cacher.delete(["key1", "key2", "key3"])
        cacher.client.delete.assert_called_once_with("MOL-key1", "MOL-key2", "MOL-key3")

    @pytest.mark.asyncio
    async def test_clean_pattern(self, cacher):
        """Test clean with pattern using SCAN."""
        # Mock SCAN iterations
        cacher.client.scan = AsyncMock(
            side_effect=[
                (10, [b"MOL-user:1", b"MOL-user:2"]),  # First batch
                (0, [b"MOL-user:3"]),  # Final batch (cursor=0)
            ]
        )
        cacher.client.delete = AsyncMock()

        await cacher.clean("user:*")

        # Should have called SCAN twice
        assert cacher.client.scan.call_count == 2
        # Should have deleted in batches
        assert cacher.client.delete.call_count == 2

    @pytest.mark.asyncio
    async def test_get_with_ttl(self, cacher):
        """Test get_with_ttl returns data and TTL."""
        # Mock pipeline
        mock_pipe = AsyncMock()
        mock_pipe.get = Mock(return_value=mock_pipe)
        mock_pipe.ttl = Mock(return_value=mock_pipe)
        mock_pipe.execute = AsyncMock(
            return_value=[
                b'{"value": 42}',
                120,  # TTL in seconds
            ]
        )

        cacher.client.pipeline = Mock(return_value=mock_pipe)

        data, ttl = await cacher.get_with_ttl("key")

        assert data == {"value": 42}
        assert ttl == 120.0

    @pytest.mark.asyncio
    async def test_get_with_ttl_miss(self, cacher):
        """Test get_with_ttl for non-existent key."""
        mock_pipe = AsyncMock()
        mock_pipe.get = Mock(return_value=mock_pipe)
        mock_pipe.ttl = Mock(return_value=mock_pipe)
        mock_pipe.execute = AsyncMock(
            return_value=[
                None,  # Data
                -2,  # TTL (-2 = key not found)
            ]
        )

        cacher.client.pipeline = Mock(return_value=mock_pipe)

        data, ttl = await cacher.get_with_ttl("missing")

        assert data is None
        assert ttl is None

    @pytest.mark.asyncio
    async def test_get_cache_keys(self, cacher):
        """Test get_cache_keys lists all keys."""
        # Mock SCAN
        cacher.client.scan = AsyncMock(
            side_effect=[
                (5, [b"MOL-user:1", b"MOL-user:2"]),
                (0, [b"MOL-post:1"]),
            ]
        )

        keys = await cacher.get_cache_keys()

        assert len(keys) == 3
        assert {"key": "user:1"} in keys
        assert {"key": "user:2"} in keys
        assert {"key": "post:1"} in keys


class TestRedisCacherEdgeCases:
    """Test edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_get_with_deserialization_error(self):
        """Test get handles deserialization errors gracefully."""
        cacher = RedisCacher("redis://localhost:6379")
        broker = Mock(spec=["namespace", "logger", "_create_logger", "settings"])
        broker.namespace = ""
        broker.logger = Mock()
        broker._create_logger = Mock(return_value=Mock())
        broker.settings = Mock()
        broker.settings.namespace = ""
        cacher.init(broker)

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=b"invalid json{")
        mock_client.delete = AsyncMock()

        cacher.client = mock_client
        cacher.connected = True

        result = await cacher.get("corrupted")

        assert result is None
        # Should delete corrupted entry
        mock_client.delete.assert_called_once_with("MOL-corrupted")

    @pytest.mark.asyncio
    async def test_operations_when_disconnected(self):
        """Test all operations gracefully handle disconnection."""
        cacher = RedisCacher("redis://localhost:6379")
        broker = Mock(spec=["namespace", "logger", "_create_logger", "settings"])
        broker.namespace = ""
        broker.logger = Mock()
        broker._create_logger = Mock(return_value=Mock())
        broker.settings = Mock()
        broker.settings.namespace = ""
        cacher.init(broker)

        # Not connected
        cacher.connected = False

        # All operations should return safely
        assert await cacher.get("key") is None
        await cacher.set("key", "value")  # No-op
        await cacher.delete("key")
        await cacher.clean("*")  # No-op
        result = await cacher.get_with_ttl("key")
        assert result == (None, None)
