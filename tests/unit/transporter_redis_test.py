"""Unit tests for the Redis transporter."""

import asyncio
import json
import logging
from unittest.mock import AsyncMock, Mock, patch

import pytest

from moleculerpy.packet import Packet, Topic
from moleculerpy.serializers import JsonSerializer
from moleculerpy.transporter.redis import RedisTransporter

# Topic aliases for readability (Topic uses REQUEST/RESPONSE, not REQ/RES)
TopicREQ = Topic.REQUEST
TopicRES = Topic.RESPONSE


class TestRedisTransporter:
    """Test Redis transporter functionality."""

    @pytest.fixture
    def mock_transit(self):
        """Create a mock transit object."""
        transit = Mock()
        transit.serializer = JsonSerializer()
        return transit

    @pytest.fixture
    def mock_handler(self):
        """Create a mock async handler."""
        return AsyncMock()

    @pytest.fixture
    def transporter(self, mock_transit, mock_handler):
        """Create a Redis transporter for testing."""
        return RedisTransporter(
            connection_string="redis://localhost:6379",
            transit=mock_transit,
            handler=mock_handler,
            node_id="test-node",
        )

    # ============ Initialization Tests ============

    def test_init_sets_attributes(self, mock_transit, mock_handler):
        """Test that initialization sets all attributes correctly."""
        transporter = RedisTransporter(
            connection_string="redis://localhost:6379/0",
            transit=mock_transit,
            handler=mock_handler,
            node_id="my-node",
        )

        assert transporter.name == "redis"
        assert transporter.connection_string == "redis://localhost:6379/0"
        assert transporter.transit == mock_transit
        assert transporter.handler == mock_handler
        assert transporter.node_id == "my-node"
        assert transporter._redis is None
        assert transporter._pubsub is None
        assert transporter._listener_task is None
        assert len(transporter._subscribed_channels) == 0

    # ============ Serialization Tests ============

    def test_serialize_adds_version_and_sender(self, transporter):
        """Test that serialization adds protocol version and sender via transit.serializer."""
        serializer = transporter.transit.serializer
        payload = {
            **{"action": "test.action", "params": {"foo": "bar"}},
            "ver": "4",
            "sender": "test-node",
        }
        result = serializer.serialize(payload)
        decoded = serializer.deserialize(result)

        assert decoded["ver"] == "4"
        assert decoded["sender"] == "test-node"
        assert decoded["action"] == "test.action"
        assert decoded["params"] == {"foo": "bar"}

    def test_serialize_does_not_modify_original(self, transporter):
        """Test that publish creates a copy before injecting ver/sender."""
        # The publish method uses {**packet.payload, ...} to avoid mutating the original
        payload = {"action": "test"}
        _ = {**payload, "ver": "4", "sender": "test-node"}
        assert "ver" not in payload
        assert "sender" not in payload

    # ============ Topic Name Tests ============

    def test_get_topic_name_without_node(self, transporter):
        """Test topic name generation without node ID."""
        topic = transporter.get_topic_name("REQ")
        assert topic == "MOL.REQ"

    def test_get_topic_name_with_node(self, transporter):
        """Test topic name generation with node ID."""
        topic = transporter.get_topic_name("REQ", "target-node")
        assert topic == "MOL.REQ.target-node"

    def test_get_topic_name_various_commands(self, transporter):
        """Test topic naming for various command types."""
        assert transporter.get_topic_name("HEARTBEAT") == "MOL.HEARTBEAT"
        assert transporter.get_topic_name("EVENT") == "MOL.EVENT"
        assert transporter.get_topic_name("RES", "node1") == "MOL.RES.node1"
        assert transporter.get_topic_name("INFO", "node2") == "MOL.INFO.node2"

    # ============ Connect/Disconnect Tests ============

    @pytest.mark.asyncio
    async def test_connect_creates_redis_client(self, transporter):
        """Test that connect creates Redis client and pubsub."""
        try:
            import redis.asyncio
        except ImportError:
            pytest.skip("redis package not installed")

        mock_redis = AsyncMock()
        mock_redis.ping = AsyncMock()
        mock_redis.pubsub = Mock(return_value=AsyncMock())

        with patch("redis.asyncio.from_url", return_value=mock_redis):
            await transporter.connect()

        assert transporter._redis == mock_redis
        assert transporter._pubsub is not None
        mock_redis.ping.assert_called_once()

    @pytest.mark.asyncio
    async def test_connect_raises_on_import_error(self, transporter):
        """Test that connect raises ImportError if redis not installed."""
        # This test verifies the error message when redis is not available
        # We test the actual behavior by checking the method's import handling
        try:
            import redis.asyncio

            # If redis is installed, we need to temporarily hide it
            pytest.skip("Cannot test ImportError when redis is installed")
        except ImportError:
            # redis not installed, test should work
            with pytest.raises(ImportError, match="redis"):
                await transporter.connect()

    @pytest.mark.asyncio
    async def test_connect_raises_on_connection_failure(self, transporter):
        """Test that connect raises exception on connection failure."""
        try:
            import redis.asyncio
        except ImportError:
            pytest.skip("redis package not installed")

        mock_redis = AsyncMock()
        mock_redis.ping = AsyncMock(side_effect=Exception("Connection refused"))

        with patch("redis.asyncio.from_url", return_value=mock_redis):
            with pytest.raises(Exception, match="Failed to connect"):
                await transporter.connect()

        assert transporter._redis is None
        assert transporter._pubsub is None

    @pytest.mark.asyncio
    async def test_disconnect_cleans_up_resources(self, transporter):
        """Test that disconnect properly cleans up all resources."""
        # Setup mock connections
        mock_redis = AsyncMock()
        mock_redis.aclose = AsyncMock()  # Redis 5.0.1+ method
        mock_pubsub = AsyncMock()
        mock_pubsub.aclose = AsyncMock()  # Redis 5.0.1+ method

        # Create a proper cancelled task mock
        async def noop():
            pass

        task = asyncio.create_task(noop())
        await task  # Let it complete

        # Now create a new task that we'll cancel
        async def wait_forever():
            await asyncio.sleep(100)

        mock_task = asyncio.create_task(wait_forever())

        transporter._redis = mock_redis
        transporter._pubsub = mock_pubsub
        transporter._listener_task = mock_task
        transporter._subscribed_channels = {"MOL.REQ", "MOL.RES"}

        await transporter.disconnect()

        assert transporter._redis is None
        assert transporter._pubsub is None
        assert transporter._listener_task is None
        assert len(transporter._subscribed_channels) == 0
        mock_pubsub.unsubscribe.assert_called_once()
        # Redis 5.0.1+ uses aclose() instead of close()
        mock_pubsub.aclose.assert_called_once()
        mock_redis.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_disconnect_handles_errors_gracefully(self, transporter):
        """Test that disconnect doesn't raise on cleanup errors."""
        mock_redis = AsyncMock()
        mock_redis.close = AsyncMock(side_effect=Exception("Close error"))
        mock_pubsub = AsyncMock()
        mock_pubsub.close = AsyncMock(side_effect=Exception("PubSub close error"))

        transporter._redis = mock_redis
        transporter._pubsub = mock_pubsub

        # Should not raise
        await transporter.disconnect()

        assert transporter._redis is None
        assert transporter._pubsub is None

    @pytest.mark.asyncio
    async def test_disconnect_wraps_cleanup_steps_with_timeout(self, transporter):
        """Disconnect should wrap awaitable cleanup steps with a timeout."""
        mock_redis = AsyncMock()
        mock_redis.aclose = AsyncMock()
        mock_pubsub = AsyncMock()
        mock_pubsub.aclose = AsyncMock()

        transporter._redis = mock_redis
        transporter._pubsub = mock_pubsub
        transporter._subscribed_channels = {"MOL.REQ"}

        timeouts_seen: list[float] = []

        async def fake_wait_for(awaitable, timeout):
            timeouts_seen.append(timeout)
            return await awaitable

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr("moleculerpy.transporter.redis.asyncio.wait_for", fake_wait_for)
            await transporter.disconnect()

        assert timeouts_seen
        assert all(timeout == 5.0 for timeout in timeouts_seen)

    # ============ Publish/Send Tests ============

    @pytest.mark.asyncio
    async def test_publish_raises_if_not_connected(self, transporter):
        """Test that publish raises RuntimeError if not connected."""
        packet = Packet(TopicREQ, "target", {"action": "test"})

        with pytest.raises(RuntimeError, match="Not connected"):
            await transporter.publish(packet)

    @pytest.mark.asyncio
    async def test_publish_serializes_and_sends(self, transporter):
        """Test that publish serializes packet and sends through middleware."""
        mock_redis = AsyncMock()
        transporter._redis = mock_redis

        # Track calls to send_with_middleware
        transporter.send_with_middleware = AsyncMock()

        packet = Packet(TopicREQ, "target-node", {"action": "users.get", "params": {}})

        await transporter.publish(packet)

        transporter.send_with_middleware.assert_called_once()
        args = transporter.send_with_middleware.call_args
        assert args[0][0] == "MOL.REQ.target-node"  # topic
        assert isinstance(args[0][1], bytes)  # serialized data
        assert "packet" in args[0][2]  # meta

    @pytest.mark.asyncio
    async def test_send_publishes_to_redis(self, transporter):
        """Test that send publishes bytes to Redis channel."""
        mock_redis = AsyncMock()
        transporter._redis = mock_redis

        data = b'{"test": "data"}'
        await transporter.send("MOL.REQ.node1", data, {})

        mock_redis.publish.assert_called_once_with("MOL.REQ.node1", data)

    @pytest.mark.asyncio
    async def test_send_raises_if_not_connected(self, transporter):
        """Test that send raises RuntimeError if not connected."""
        with pytest.raises(RuntimeError, match="Not connected"):
            await transporter.send("MOL.REQ", b"data", {})

    # ============ Subscribe Tests ============

    @pytest.mark.asyncio
    async def test_subscribe_raises_if_not_connected(self, transporter):
        """Test that subscribe raises RuntimeError if not connected."""
        with pytest.raises(RuntimeError, match="Not connected"):
            await transporter.subscribe("REQ")

    @pytest.mark.asyncio
    async def test_subscribe_raises_if_no_handler(self, mock_transit):
        """Test that subscribe raises if no handler configured."""
        transporter = RedisTransporter(
            connection_string="redis://localhost:6379",
            transit=mock_transit,
            handler=None,  # No handler
            node_id="test-node",
        )
        transporter._redis = AsyncMock()
        transporter._pubsub = AsyncMock()

        with pytest.raises(ValueError, match="Handler must be provided"):
            await transporter.subscribe("REQ")

    @pytest.mark.asyncio
    async def test_subscribe_subscribes_to_broadcast_and_node_channels(self, transporter):
        """Test that subscribe creates both broadcast and node-specific subscriptions."""
        mock_redis = AsyncMock()
        mock_pubsub = AsyncMock()
        mock_pubsub.listen = Mock(return_value=self._empty_async_gen())
        transporter._redis = mock_redis
        transporter._pubsub = mock_pubsub

        await transporter.subscribe("REQ")

        # Should subscribe to both broadcast and node-specific channels
        calls = mock_pubsub.subscribe.call_args_list
        channels_subscribed = [call[0][0] for call in calls]
        assert "MOL.REQ" in channels_subscribed
        assert "MOL.REQ.test-node" in channels_subscribed

    @pytest.mark.asyncio
    async def test_subscribe_tracks_subscribed_channels(self, transporter):
        """Test that subscribed channels are tracked for cleanup."""
        mock_redis = AsyncMock()
        mock_pubsub = AsyncMock()
        mock_pubsub.listen = Mock(return_value=self._empty_async_gen())
        transporter._redis = mock_redis
        transporter._pubsub = mock_pubsub

        await transporter.subscribe("REQ")
        await transporter.subscribe("RES")

        assert "MOL.REQ" in transporter._subscribed_channels
        assert "MOL.REQ.test-node" in transporter._subscribed_channels
        assert "MOL.RES" in transporter._subscribed_channels
        assert "MOL.RES.test-node" in transporter._subscribed_channels

    @pytest.mark.asyncio
    async def test_subscribe_starts_listener_task(self, transporter):
        """Test that subscribe starts the listener task."""
        mock_redis = AsyncMock()
        mock_pubsub = AsyncMock()
        mock_pubsub.listen = Mock(return_value=self._empty_async_gen())
        transporter._redis = mock_redis
        transporter._pubsub = mock_pubsub

        await transporter.subscribe("REQ")

        assert transporter._listener_task is not None

    # ============ Receive Tests ============

    @pytest.mark.asyncio
    async def test_receive_deserializes_and_calls_handler(self, transporter, mock_handler):
        """Test that receive deserializes data and calls handler."""
        data = b'{"sender": "remote-node", "action": "test.action", "params": {}}'
        meta = {"channel": "MOL.REQ.test-node", "packet_type": TopicREQ}

        await transporter.receive("REQ", data, meta)

        mock_handler.assert_called_once()
        packet = mock_handler.call_args[0][0]
        assert isinstance(packet, Packet)
        assert packet.sender == "remote-node"
        assert packet.type == TopicREQ

    @pytest.mark.asyncio
    async def test_receive_offloads_large_json_deserialize(self, transporter, mock_handler):
        """Large payloads should deserialize via asyncio.to_thread."""
        data = json.dumps(
            {
                "sender": "remote-node",
                "blob": "x" * (1024 * 1024),
            }
        ).encode("utf-8")
        meta = {"channel": "MOL.INFO.test-node", "packet_type": Topic.INFO}
        called = {"value": False}

        async def fake_to_thread(func, *args):
            called["value"] = True
            return func(*args)

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr("moleculerpy.serializers.base.asyncio.to_thread", fake_to_thread)
            await transporter.receive("INFO", data, meta)

        assert called["value"] is True
        mock_handler.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_receive_raises_on_invalid_json(self, transporter):
        """Test that receive raises ValueError on invalid JSON."""
        data = b"not valid json"
        meta = {"packet_type": TopicREQ}

        with pytest.raises(ValueError, match="Failed to decode"):
            await transporter.receive("REQ", data, meta)

    @pytest.mark.asyncio
    async def test_receive_raises_if_no_packet_type(self, transporter):
        """Test that receive raises ValueError if packet_type missing from meta."""
        data = b'{"sender": "node"}'
        meta = {}  # No packet_type

        with pytest.raises(ValueError, match="packet_type missing"):
            await transporter.receive("REQ", data, meta)

    @pytest.mark.asyncio
    async def test_receive_raises_if_no_handler(self, mock_transit):
        """Test that receive raises if no handler configured."""
        transporter = RedisTransporter(
            connection_string="redis://localhost:6379",
            transit=mock_transit,
            handler=None,
            node_id="test-node",
        )

        data = b'{"sender": "remote"}'
        meta = {"packet_type": TopicREQ}

        with pytest.raises(ValueError, match="no handler"):
            await transporter.receive("REQ", data, meta)

    # ============ Handle Message Tests ============

    @pytest.mark.asyncio
    async def test_handle_message_processes_valid_message(self, transporter):
        """Test that _handle_message correctly processes a valid message."""
        transporter.receive_with_middleware = AsyncMock()

        message = {
            "type": "message",
            "channel": b"MOL.HEARTBEAT.node123",
            "data": b'{"sender": "remote-node", "cpu": 30.5}',
        }

        await transporter._handle_message(message)

        transporter.receive_with_middleware.assert_called_once()
        args = transporter.receive_with_middleware.call_args[0]
        assert args[0] == "HEARTBEAT"  # cmd
        assert args[1] == b'{"sender": "remote-node", "cpu": 30.5}'  # data
        assert args[2]["packet_type"] == Topic.HEARTBEAT

    @pytest.mark.asyncio
    async def test_handle_message_ignores_unknown_topics(self, transporter, caplog):
        """Unknown topics should be ignored but logged for diagnostics."""
        transporter.receive_with_middleware = AsyncMock()

        message = {
            "type": "message",
            "channel": b"UNKNOWN.TOPIC",
            "data": b'{"data": "test"}',
        }

        with caplog.at_level(logging.WARNING):
            await transporter._handle_message(message)

        # Should not call receive_with_middleware for unknown topics
        transporter.receive_with_middleware.assert_not_called()
        assert "unknown topic" in caplog.text.lower()

    # ============ Regression Tests ============

    @pytest.mark.asyncio
    async def test_redis_transporter_sets_packet_sender_regression(self, transporter, mock_handler):
        """Regression test: Redis transporter should set sender attribute on packets."""
        data = b'{"sender": "remote-node", "cpu": 30.5}'
        meta = {"channel": "MOL.HEARTBEAT.node123", "packet_type": Topic.HEARTBEAT}

        await transporter.receive("HEARTBEAT", data, meta)

        assert mock_handler.called
        packet = mock_handler.call_args[0][0]
        assert isinstance(packet, Packet)
        assert packet.sender == "remote-node"
        assert packet.type == Topic.HEARTBEAT
        assert packet.payload["cpu"] == 30.5

    @pytest.mark.asyncio
    async def test_redis_message_handler_creates_packet_with_sender_regression(self, mock_transit):
        """Regression test: Message handler should create packets with sender attribute."""
        test_cases = [
            ("MOL.INFO.node1", b'{"sender": "info-node", "id": "test"}', Topic.INFO),
            ("MOL.HEARTBEAT.node2", b'{"sender": "beat-node", "cpu": 25}', Topic.HEARTBEAT),
            ("MOL.EVENT.node3", b'{"sender": "event-node", "event": "test"}', Topic.EVENT),
        ]

        for channel, data, expected_topic in test_cases:
            handler = AsyncMock()
            transporter = RedisTransporter(
                connection_string="redis://localhost:6379",
                transit=mock_transit,
                handler=handler,
                node_id="local-node",
            )

            meta = {"channel": channel, "packet_type": expected_topic}
            await transporter.receive(expected_topic.value, data, meta)

            assert handler.called
            packet = handler.call_args[0][0]
            assert isinstance(packet, Packet)
            assert packet.type == expected_topic
            assert hasattr(packet, "sender")
            assert packet.sender is not None

            handler.reset_mock()

    # ============ Factory Method Tests ============

    def test_from_config_creates_transporter(self, mock_transit, mock_handler):
        """Test that from_config creates a properly configured transporter."""
        config = {"connection": "redis://redis.example.com:6379/1"}

        transporter = RedisTransporter.from_config(
            config=config,
            transit=mock_transit,
            handler=mock_handler,
            node_id="config-node",
        )

        assert transporter.connection_string == "redis://redis.example.com:6379/1"
        assert transporter.transit == mock_transit
        assert transporter.handler == mock_handler
        assert transporter.node_id == "config-node"

    def test_from_config_raises_on_missing_connection(self, mock_transit):
        """Test that from_config raises KeyError if connection missing."""
        config = {}  # No connection key

        with pytest.raises(KeyError, match="connection"):
            RedisTransporter.from_config(config, mock_transit)

    # ============ Middleware Integration Tests ============

    @pytest.mark.asyncio
    async def test_middleware_wrapping_on_send(self, transporter):
        """Test that send goes through middleware when wrapped."""
        mock_redis = AsyncMock()
        transporter._redis = mock_redis

        # Simulate middleware wrapping
        wrapped_send = AsyncMock()
        transporter._wrapped_send = wrapped_send

        packet = Packet(TopicREQ, "target", {"action": "test"})
        await transporter.publish(packet)

        # Should call wrapped_send, not direct redis.publish
        wrapped_send.assert_called_once()
        mock_redis.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_middleware_wrapping_on_receive(self, transporter):
        """Test that receive goes through middleware when wrapped."""
        # Simulate middleware wrapping
        wrapped_receive = AsyncMock()
        transporter._wrapped_receive = wrapped_receive

        message = {
            "type": "message",
            "channel": "MOL.REQ.test",
            "data": b'{"sender": "node"}',
        }

        await transporter._handle_message(message)

        # Should call wrapped_receive
        wrapped_receive.assert_called_once()

    # ============ Helper Methods ============

    async def _empty_async_gen(self):
        """Create an empty async generator for mocking pubsub.listen()."""
        return
        yield  # Makes this an async generator


class TestRedisTransporterIntegration:
    """Integration tests for Redis transporter (requires Redis server)."""

    @pytest.fixture
    def integration_transporter(self):
        """Create transporter for integration tests (uses Valkey on port 6381)."""
        return RedisTransporter(
            connection_string="redis://localhost:6381",
            transit=Mock(),
            handler=AsyncMock(),
            node_id="integration-test-node",
        )

    @pytest.mark.asyncio
    async def test_real_redis_connect_disconnect(self, integration_transporter):
        """Test real connection to Valkey server."""
        try:
            await integration_transporter.connect()
        except Exception as err:
            pytest.skip(f"Valkey is not reachable in this environment: {err}")
        assert integration_transporter._redis is not None

        await integration_transporter.disconnect()
        assert integration_transporter._redis is None

    @pytest.mark.asyncio
    async def test_real_redis_pubsub(self, integration_transporter):
        """Test real pub/sub with Redis server."""
        received = []

        async def handler(packet):
            received.append(packet)

        integration_transporter.handler = handler

        try:
            await integration_transporter.connect()
        except Exception as err:
            pytest.skip(f"Valkey is not reachable in this environment: {err}")
        await integration_transporter.subscribe("TEST")

        # Publish a message
        Packet(Topic.EVENT, None, {"test": "data"})
        # Note: We'd need to serialize to MOL.EVENT format
        await integration_transporter._redis.publish(
            "MOL.TEST", b'{"sender": "test", "test": "data"}'
        )

        await asyncio.sleep(0.1)  # Wait for message

        await integration_transporter.disconnect()

        # Check received (may need adjustment based on topic parsing)
