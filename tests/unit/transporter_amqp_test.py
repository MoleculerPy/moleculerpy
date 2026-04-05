"""Unit tests for the AMQP transporter."""

import json
import logging
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from moleculerpy.packet import Packet, Topic
from moleculerpy.serializers import JsonSerializer
from moleculerpy.transporter.amqp import (
    AUTO_DELETE_QUEUES_DEFAULT_MS,
    AmqpTransporter,
)


def _mock_transit() -> Mock:
    """Create a mock transit with a real JsonSerializer."""
    transit = Mock()
    transit.serializer = JsonSerializer()
    return transit


def _make_transporter(**kwargs) -> AmqpTransporter:
    """Create an AmqpTransporter with sensible defaults."""
    defaults = {
        "urls": ["amqp://localhost:5672"],
        "transit": _mock_transit(),
        "handler": AsyncMock(),
        "node_id": "test-node",
    }
    defaults.update(kwargs)
    return AmqpTransporter(**defaults)


class TestAmqpTransporterInit:
    """Test AmqpTransporter initialization."""

    def test_default_values(self):
        t = _make_transporter()
        assert t.name == "amqp"
        assert t.has_built_in_balancer is True
        assert t.prefetch == 1
        assert t.auto_delete_queues == AUTO_DELETE_QUEUES_DEFAULT_MS
        assert t.event_time_to_live is None
        assert t.heartbeat_time_to_live is None
        assert t.prefix == "MOL"
        assert t._connection is None
        assert t._channel is None

    def test_custom_prefetch(self):
        t = _make_transporter(prefetch=5)
        assert t.prefetch == 5

    def test_custom_ttl(self):
        t = _make_transporter(event_time_to_live=5000, heartbeat_time_to_live=3000)
        assert t.event_time_to_live == 5000
        assert t.heartbeat_time_to_live == 3000

    def test_auto_delete_disabled(self):
        t = _make_transporter(auto_delete_queues=-1)
        assert t.auto_delete_queues == -1


class TestAmqpTopicNames:
    """Test topic/queue name generation."""

    def test_topic_without_node_id(self):
        t = _make_transporter()
        assert t.get_topic_name("DISCOVER") == "MOL.DISCOVER"

    def test_topic_with_node_id(self):
        t = _make_transporter()
        assert t.get_topic_name("REQ", "node-1") == "MOL.REQ.node-1"

    def test_all_command_types(self):
        t = _make_transporter()
        for cmd in [
            "REQ",
            "RES",
            "EVENT",
            "DISCOVER",
            "INFO",
            "HEARTBEAT",
            "DISCONNECT",
            "PING",
            "PONG",
        ]:
            assert t.get_topic_name(cmd) == f"MOL.{cmd}"
            assert t.get_topic_name(cmd, "x") == f"MOL.{cmd}.x"


class TestAmqpQueueOptions:
    """Test _get_queue_options — matches Node.js _getQueueOptions()."""

    def test_request_with_auto_delete(self):
        t = _make_transporter(auto_delete_queues=120000)
        opts = t._get_queue_options(Topic.REQUEST, balanced_queue=False)
        assert opts["expires"] == 120000

    def test_request_balanced_no_expires(self):
        """Balanced request queues must NOT expire (persistent work queue)."""
        t = _make_transporter(auto_delete_queues=120000)
        opts = t._get_queue_options(Topic.REQUEST, balanced_queue=True)
        assert "expires" not in opts

    def test_request_auto_delete_disabled(self):
        t = _make_transporter(auto_delete_queues=-1)
        opts = t._get_queue_options(Topic.REQUEST, balanced_queue=False)
        assert "expires" not in opts

    def test_response_with_auto_delete(self):
        t = _make_transporter(auto_delete_queues=120000)
        opts = t._get_queue_options(Topic.RESPONSE)
        assert opts["expires"] == 120000

    def test_event_with_ttl(self):
        t = _make_transporter(event_time_to_live=5000)
        opts = t._get_queue_options(Topic.EVENT)
        assert opts["message_ttl"] == 5000

    def test_heartbeat_auto_delete(self):
        t = _make_transporter()
        opts = t._get_queue_options(Topic.HEARTBEAT)
        assert opts["auto_delete"] is True

    def test_heartbeat_with_ttl(self):
        t = _make_transporter(heartbeat_time_to_live=3000)
        opts = t._get_queue_options(Topic.HEARTBEAT)
        assert opts["auto_delete"] is True
        assert opts["message_ttl"] == 3000

    def test_discover_auto_delete(self):
        t = _make_transporter()
        opts = t._get_queue_options(Topic.DISCOVER)
        assert opts["auto_delete"] is True

    def test_info_auto_delete(self):
        t = _make_transporter()
        opts = t._get_queue_options(Topic.INFO)
        assert opts["auto_delete"] is True

    def test_ping_pong_auto_delete(self):
        t = _make_transporter()
        for pt in [Topic.PING, Topic.PONG]:
            opts = t._get_queue_options(pt)
            assert opts["auto_delete"] is True

    def test_queue_options_merged(self):
        t = _make_transporter(queue_options={"durable": True})
        opts = t._get_queue_options(Topic.DISCOVER)
        assert opts["auto_delete"] is True
        assert opts["durable"] is True


class TestAmqpFilterQueueArgs:
    """Test _filter_queue_args conversion."""

    def test_auto_delete(self):
        result = AmqpTransporter._filter_queue_args({"auto_delete": True})
        assert result == {"auto_delete": True}

    def test_expires(self):
        result = AmqpTransporter._filter_queue_args({"expires": 120000})
        assert result == {"arguments": {"x-expires": 120000}}

    def test_message_ttl(self):
        result = AmqpTransporter._filter_queue_args({"message_ttl": 5000})
        assert result == {"arguments": {"x-message-ttl": 5000}}

    def test_combined(self):
        result = AmqpTransporter._filter_queue_args(
            {
                "auto_delete": True,
                "expires": 120000,
                "message_ttl": 5000,
            }
        )
        assert result == {
            "auto_delete": True,
            "arguments": {"x-expires": 120000, "x-message-ttl": 5000},
        }

    def test_empty(self):
        result = AmqpTransporter._filter_queue_args({})
        assert result == {}


class TestAmqpFromConfig:
    """Test from_config factory method."""

    def test_basic_url(self):
        config = {"connection": "amqp://localhost:5672"}
        t = AmqpTransporter.from_config(config, _mock_transit(), AsyncMock(), "n1")
        assert t.urls == ["amqp://localhost:5672"]
        assert t.prefetch == 1

    def test_cluster_urls_semicolon(self):
        config = {"connection": "amqp://h1:5672;amqp://h2:5672"}
        t = AmqpTransporter.from_config(config, _mock_transit(), AsyncMock(), "n1")
        assert t.urls == ["amqp://h1:5672", "amqp://h2:5672"]

    def test_cluster_urls_list(self):
        config = {"url": ["amqp://h1", "amqp://h2", "amqp://h3"]}
        t = AmqpTransporter.from_config(config, _mock_transit(), AsyncMock(), "n1")
        assert len(t.urls) == 3

    def test_with_options(self):
        config = {
            "connection": "amqp://localhost",
            "prefetch": 5,
            "eventTimeToLive": 5000,
            "autoDeleteQueues": False,
        }
        t = AmqpTransporter.from_config(config, _mock_transit(), AsyncMock(), "n1")
        assert t.prefetch == 5
        assert t.event_time_to_live == 5000
        assert t.auto_delete_queues == -1  # False → -1

    def test_auto_delete_true_default(self):
        config = {"connection": "amqp://localhost", "autoDeleteQueues": True}
        t = AmqpTransporter.from_config(config, _mock_transit(), AsyncMock(), "n1")
        assert t.auto_delete_queues == AUTO_DELETE_QUEUES_DEFAULT_MS

    def test_auto_delete_number(self):
        config = {"connection": "amqp://localhost", "autoDeleteQueues": 60000}
        t = AmqpTransporter.from_config(config, _mock_transit(), AsyncMock(), "n1")
        assert t.auto_delete_queues == 60000

    def test_missing_connection_uses_default(self):
        t = AmqpTransporter.from_config({}, _mock_transit(), AsyncMock(), "n1")
        assert t.urls == ["amqp://localhost"]

    def test_python_style_options(self):
        config = {
            "connection": "amqp://localhost",
            "event_time_to_live": 3000,
            "heartbeat_time_to_live": 2000,
            "auto_delete_queues": 90000,
        }
        t = AmqpTransporter.from_config(config, _mock_transit(), AsyncMock(), "n1")
        assert t.event_time_to_live == 3000
        assert t.heartbeat_time_to_live == 2000
        assert t.auto_delete_queues == 90000


class TestAmqpParseAutoDelete:
    """Test _parse_auto_delete normalization."""

    def test_true_returns_default(self):
        assert AmqpTransporter._parse_auto_delete(True) == AUTO_DELETE_QUEUES_DEFAULT_MS

    def test_false_returns_minus_one(self):
        assert AmqpTransporter._parse_auto_delete(False) == -1

    def test_number_passthrough(self):
        assert AmqpTransporter._parse_auto_delete(60000) == 60000

    def test_minus_one(self):
        assert AmqpTransporter._parse_auto_delete(-1) == -1


class TestAmqpSend:
    """Test send routing logic."""

    @pytest.mark.asyncio
    async def test_send_no_channel_silent(self):
        t = _make_transporter()
        t._channel = None
        result = await t.send("MOL.HEARTBEAT", b"data", {"packet": None, "balanced": False})
        assert result is None

    @pytest.mark.asyncio
    async def test_publish_raises_when_disconnected(self):
        t = _make_transporter()
        t._channel = None
        packet = Packet(Topic.HEARTBEAT, "test-node", {})
        with pytest.raises(RuntimeError, match="Not connected"):
            await t.publish(packet)

    @pytest.mark.asyncio
    async def test_publish_serializes_payload(self):
        t = _make_transporter()
        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        # declare_exchange returns an exchange for broadcast publish
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.default_exchange = mock_exchange
        t._channel = mock_channel

        packet = Packet(Topic.HEARTBEAT, "test-node", {"cpu": 50})
        packet.target = None

        # Mock aio_pika.Message to avoid import dependency in send()
        with patch(
            "moleculerpy.transporter.amqp.AmqpTransporter.send", new_callable=AsyncMock
        ) as mock_send:
            await t.publish(packet)
            # publish() calls send_with_middleware → send
            mock_send.assert_awaited_once()
            # Verify topic and data are correct
            call_args = mock_send.call_args
            assert call_args[0][0] == "MOL.HEARTBEAT"  # topic
            assert b'"cpu": 50' in call_args[0][1]  # serialized payload


class TestAmqpReceive:
    """Test receive pipeline."""

    @pytest.mark.asyncio
    async def test_receive_deserializes_and_calls_handler(self):
        handler = AsyncMock()
        t = _make_transporter(handler=handler)

        data = json.dumps({"sender": "remote", "action": "test"}).encode()
        meta = {"packet_type": Topic.REQUEST}

        await t.receive("REQ", data, meta)

        handler.assert_awaited_once()
        packet = handler.call_args[0][0]
        assert packet.sender == "remote"
        assert packet.type == Topic.REQUEST

    @pytest.mark.asyncio
    async def test_receive_bad_json_drops(self, caplog):
        handler = AsyncMock()
        t = _make_transporter(handler=handler)

        with caplog.at_level(logging.WARNING):
            await t.receive("INFO", b"not json", {"packet_type": Topic.INFO})

        handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_receive_no_handler_raises(self):
        t = _make_transporter(handler=None)
        data = json.dumps({"sender": "x"}).encode()
        with pytest.raises(ValueError, match="no handler"):
            await t.receive("INFO", data, {"packet_type": Topic.INFO})


class TestAmqpMakeConsumer:
    """Test _make_consumer callback creation."""

    @pytest.mark.asyncio
    async def test_consumer_no_ack(self):
        handler = AsyncMock()
        t = _make_transporter(handler=handler)

        consumer = t._make_consumer("HEARTBEAT", need_ack=False)
        msg = MagicMock()
        msg.body = json.dumps({"sender": "s1", "cpu": 10}).encode()

        await consumer(msg)

        handler.assert_awaited_once()
        msg.ack.assert_not_called()

    @pytest.mark.asyncio
    async def test_consumer_with_ack_success(self):
        handler = AsyncMock()
        t = _make_transporter(handler=handler)
        t._channel = AsyncMock()  # Channel must be alive for ACK

        consumer = t._make_consumer("REQ", need_ack=True)
        msg = AsyncMock()
        msg.body = json.dumps({"sender": "s1", "action": "test"}).encode()

        await consumer(msg)

        handler.assert_awaited_once()
        msg.ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_consumer_with_ack_failure_nacks(self):
        handler = AsyncMock(side_effect=RuntimeError("boom"))
        t = _make_transporter(handler=handler)
        t._channel = AsyncMock()  # Channel must be alive for NACK

        consumer = t._make_consumer("REQ", need_ack=True)
        msg = AsyncMock()
        msg.body = json.dumps({"sender": "s1", "action": "test"}).encode()

        await consumer(msg)  # Should not raise

        msg.nack.assert_awaited_once()


class TestAmqpDisconnect:
    """Test disconnect lifecycle."""

    @pytest.mark.asyncio
    async def test_disconnect_cleans_state(self):
        t = _make_transporter()
        t._channel = AsyncMock()
        t._connection = AsyncMock()
        t._bindings = []

        await t.disconnect()

        assert t._channel is None
        assert t._connection is None
        assert t._disconnecting is True

    @pytest.mark.asyncio
    async def test_disconnect_no_connection_safe(self):
        t = _make_transporter()
        await t.disconnect()  # Should not raise


class TestAmqpRegistry:
    """Test transporter registry integration."""

    def test_amqp_in_registry(self):
        from moleculerpy.transporter.base import Transporter

        config = {"connection": "amqp://localhost:5672"}
        t = Transporter.get_by_name("amqp", config, _mock_transit(), AsyncMock(), "n1")
        assert isinstance(t, AmqpTransporter)


class TestAmqpBalancedGroups:
    """Test balanced request/event methods."""

    @pytest.mark.asyncio
    async def test_publish_balanced_request_raises_disconnected(self):
        t = _make_transporter()
        t._channel = None
        packet = Packet(Topic.REQUEST, "node", {"action": "math.add"})
        with pytest.raises(RuntimeError, match="Not connected"):
            await t.publish_balanced_request(packet)

    @pytest.mark.asyncio
    async def test_publish_balanced_event_raises_disconnected(self):
        t = _make_transporter()
        t._channel = None
        packet = Packet(Topic.EVENT, "node", {"event": "user.created"})
        with pytest.raises(RuntimeError, match="Not connected"):
            await t.publish_balanced_event(packet, "mygroup")

    @pytest.mark.asyncio
    async def test_subscribe_balanced_request_raises_disconnected(self):
        t = _make_transporter()
        t._channel = None
        with pytest.raises(RuntimeError, match="Not connected"):
            await t.subscribe_balanced_request("math.add")

    @pytest.mark.asyncio
    async def test_subscribe_balanced_event_raises_disconnected(self):
        t = _make_transporter()
        t._channel = None
        with pytest.raises(RuntimeError, match="Not connected"):
            await t.subscribe_balanced_event("user.created", "group1")
