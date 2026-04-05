"""Unit tests for the Kafka transporter."""

import json
import logging
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from moleculerpy.packet import Packet, Topic
from moleculerpy.serializers import JsonSerializer
from moleculerpy.transporter.kafka import KafkaTransporter


def _mock_transit() -> Mock:
    transit = Mock()
    transit.serializer = JsonSerializer()
    return transit


def _make_transporter(**kwargs) -> KafkaTransporter:
    defaults = {
        "bootstrap_servers": "localhost:9092",
        "transit": _mock_transit(),
        "handler": AsyncMock(),
        "node_id": "test-node",
    }
    defaults.update(kwargs)
    return KafkaTransporter(**defaults)


class TestKafkaInit:
    def test_defaults(self):
        t = _make_transporter()
        assert t.name == "kafka"
        assert t.has_built_in_balancer is False
        assert t.partition == 0
        assert t.group_id == "test-node"
        assert t.prefix == "MOL"

    def test_custom_group_id(self):
        t = _make_transporter(group_id="custom-group")
        assert t.group_id == "custom-group"

    def test_custom_partition(self):
        t = _make_transporter(partition=2)
        assert t.partition == 2


class TestKafkaTopicNames:
    def test_without_node(self):
        t = _make_transporter()
        assert t.get_topic_name("DISCOVER") == "MOL.DISCOVER"

    def test_with_node(self):
        t = _make_transporter()
        assert t.get_topic_name("REQ", "node-1") == "MOL.REQ.node-1"

    def test_all_commands(self):
        t = _make_transporter()
        for cmd in ["REQ", "RES", "EVENT", "DISCOVER", "INFO", "HEARTBEAT", "PING", "PONG"]:
            assert t.get_topic_name(cmd) == f"MOL.{cmd}"


class TestKafkaFromConfig:
    def test_basic(self):
        config = {"connection": "kafka://localhost:9092"}
        t = KafkaTransporter.from_config(config, _mock_transit(), AsyncMock(), "n1")
        assert t.bootstrap_servers == "localhost:9092"

    def test_host_key(self):
        config = {"host": "broker:9092"}
        t = KafkaTransporter.from_config(config, _mock_transit(), AsyncMock(), "n1")
        assert t.bootstrap_servers == "broker:9092"

    def test_with_consumer_group(self):
        config = {
            "connection": "kafka://localhost:9092",
            "consumer": {"group_id": "my-group"},
        }
        t = KafkaTransporter.from_config(config, _mock_transit(), AsyncMock(), "n1")
        assert t.group_id == "my-group"

    def test_with_partition(self):
        config = {
            "connection": "kafka://localhost:9092",
            "publish": {"partition": 3},
        }
        t = KafkaTransporter.from_config(config, _mock_transit(), AsyncMock(), "n1")
        assert t.partition == 3

    def test_default_host(self):
        t = KafkaTransporter.from_config({}, _mock_transit(), AsyncMock(), "n1")
        assert t.bootstrap_servers == "localhost:9092"


class TestKafkaSubscribe:
    @pytest.mark.asyncio
    async def test_subscribe_is_noop(self):
        """Kafka subscribe() is a no-op — all subs via make_subscriptions."""
        t = _make_transporter()
        await t.subscribe("DISCOVER")  # Should not raise
        await t.subscribe("REQ", "node-1")  # Should not raise


class TestKafkaSend:
    @pytest.mark.asyncio
    async def test_send_no_producer_silent(self):
        t = _make_transporter()
        t._producer = None
        result = await t.send("MOL.HEARTBEAT", b"data", {})
        assert result is None

    @pytest.mark.asyncio
    async def test_publish_raises_when_disconnected(self):
        t = _make_transporter()
        t._producer = None
        packet = Packet(Topic.HEARTBEAT, "test-node", {})
        with pytest.raises(RuntimeError, match="Not connected"):
            await t.publish(packet)

    @pytest.mark.asyncio
    async def test_publish_serializes(self):
        t = _make_transporter()

        with patch(
            "moleculerpy.transporter.kafka.KafkaTransporter.send", new_callable=AsyncMock
        ) as mock_send:
            t._producer = AsyncMock()
            packet = Packet(Topic.HEARTBEAT, "test-node", {"cpu": 50})
            packet.target = None
            await t.publish(packet)

            mock_send.assert_awaited_once()
            args = mock_send.call_args[0]
            assert args[0] == "MOL.HEARTBEAT"
            assert b'"cpu": 50' in args[1]

    @pytest.mark.asyncio
    async def test_publish_with_target(self):
        t = _make_transporter()

        with patch(
            "moleculerpy.transporter.kafka.KafkaTransporter.send", new_callable=AsyncMock
        ) as mock_send:
            t._producer = AsyncMock()
            packet = Packet(Topic.REQUEST, "test-node", {"action": "users.get"})
            packet.target = "remote-node"
            await t.publish(packet)

            args = mock_send.call_args[0]
            assert args[0] == "MOL.REQ.remote-node"


class TestKafkaReceive:
    @pytest.mark.asyncio
    async def test_receive_deserializes(self):
        handler = AsyncMock()
        t = _make_transporter(handler=handler)
        data = json.dumps({"sender": "remote", "action": "test"}).encode()
        await t.receive("REQ", data, {"packet_type": Topic.REQUEST})
        handler.assert_awaited_once()
        packet = handler.call_args[0][0]
        assert packet.sender == "remote"

    @pytest.mark.asyncio
    async def test_receive_bad_json(self, caplog):
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


class TestKafkaConsumeLoop:
    @pytest.mark.asyncio
    async def test_consume_loop_routes_message(self):
        handler = AsyncMock()
        t = _make_transporter(handler=handler)

        msg = MagicMock()
        msg.topic = "MOL.HEARTBEAT.node1"
        msg.value = json.dumps({"sender": "kafka-node", "cpu": 30}).encode()

        # Simulate consumer yielding one message then stopping
        async def fake_consumer():
            yield msg

        t._consumer = fake_consumer()
        await t._consume_loop()

        handler.assert_awaited_once()
        packet = handler.call_args[0][0]
        assert packet.sender == "kafka-node"

    @pytest.mark.asyncio
    async def test_consume_loop_skips_short_topic(self, caplog):
        handler = AsyncMock()
        t = _make_transporter(handler=handler)

        msg = MagicMock()
        msg.topic = "MOL"
        msg.value = b"{}"

        async def fake_consumer():
            yield msg

        t._consumer = fake_consumer()
        with caplog.at_level(logging.WARNING):
            await t._consume_loop()

        handler.assert_not_called()
        assert "short topic" in caplog.text


class TestKafkaDisconnect:
    @pytest.mark.asyncio
    async def test_disconnect_cleans_state(self):
        t = _make_transporter()
        t._producer = AsyncMock()
        t._consumer = AsyncMock()
        t._consume_task = None

        await t.disconnect()

        assert t._producer is None
        assert t._consumer is None
        assert t._shutting_down is True

    @pytest.mark.asyncio
    async def test_disconnect_no_connection_safe(self):
        t = _make_transporter()
        await t.disconnect()


class TestKafkaRegistry:
    def test_kafka_in_registry(self):
        from moleculerpy.transporter.base import Transporter

        config = {"connection": "kafka://localhost:9092"}
        t = Transporter.get_by_name("kafka", config, _mock_transit(), AsyncMock(), "n1")
        assert isinstance(t, KafkaTransporter)
