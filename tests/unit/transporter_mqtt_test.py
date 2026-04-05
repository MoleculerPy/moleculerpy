"""Unit tests for the MQTT transporter."""

import asyncio
import json
import logging
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from moleculerpy.packet import Packet, Topic
from moleculerpy.serializers import JsonSerializer
from moleculerpy.transporter.mqtt import MqttTransporter


def _mock_transit() -> Mock:
    """Create a mock transit with a real JsonSerializer."""
    transit = Mock()
    transit.serializer = JsonSerializer()
    return transit


def _make_transporter(**kwargs) -> MqttTransporter:
    """Create an MqttTransporter with sensible defaults."""
    defaults = {
        "connection_string": "mqtt://localhost:1883",
        "transit": _mock_transit(),
        "handler": AsyncMock(),
        "node_id": "local-node",
    }
    defaults.update(kwargs)
    return MqttTransporter(**defaults)


class TestMqttTransporterInit:
    """Test MqttTransporter initialization and configuration."""

    def test_default_values(self):
        t = _make_transporter()
        assert t.name == "mqtt"
        assert t.has_built_in_balancer is False
        assert t.qos == 0
        assert t.topic_separator == "."
        assert t.prefix == "MOL"
        assert t._client is None

    def test_custom_qos(self):
        t = _make_transporter(qos=1)
        assert t.qos == 1

    def test_custom_topic_separator(self):
        t = _make_transporter(topic_separator="/")
        assert t.topic_separator == "/"


class TestMqttTopicNames:
    """Test topic name generation — must match Node.js format."""

    def test_topic_without_node_id(self):
        t = _make_transporter()
        assert t.get_topic_name("DISCOVER") == "MOL.DISCOVER"

    def test_topic_with_node_id(self):
        t = _make_transporter()
        assert t.get_topic_name("REQ", "node-1") == "MOL.REQ.node-1"

    def test_topic_with_slash_separator(self):
        t = _make_transporter(topic_separator="/")
        assert t.get_topic_name("DISCOVER") == "MOL/DISCOVER"
        assert t.get_topic_name("REQ", "node-1") == "MOL/REQ/node-1"

    def test_all_command_types(self):
        t = _make_transporter()
        commands = [
            "REQ",
            "RES",
            "EVENT",
            "DISCOVER",
            "INFO",
            "HEARTBEAT",
            "DISCONNECT",
            "PING",
            "PONG",
        ]
        for cmd in commands:
            topic = t.get_topic_name(cmd)
            assert topic == f"MOL.{cmd}"
            topic_targeted = t.get_topic_name(cmd, "node-x")
            assert topic_targeted == f"MOL.{cmd}.node-x"


class TestMqttParseConnectionString:
    """Test connection string parsing."""

    def test_mqtt_url(self):
        host, port = MqttTransporter._parse_connection_string("mqtt://localhost:1883")
        assert host == "localhost"
        assert port == 1883

    def test_mqtts_url(self):
        host, port = MqttTransporter._parse_connection_string("mqtts://broker.example.com:8883")
        assert host == "broker.example.com"
        assert port == 8883

    def test_plain_host_port(self):
        host, port = MqttTransporter._parse_connection_string("myhost:1884")
        assert host == "myhost"
        assert port == 1884

    def test_plain_host_only(self):
        host, port = MqttTransporter._parse_connection_string("myhost")
        assert host == "myhost"
        assert port == 1883

    def test_mqtt_url_default_port(self):
        host, port = MqttTransporter._parse_connection_string("mqtt://localhost")
        assert host == "localhost"
        assert port == 1883

    def test_tcp_scheme(self):
        host, port = MqttTransporter._parse_connection_string("tcp://broker:1883")
        assert host == "broker"
        assert port == 1883


class TestMqttFromConfig:
    """Test from_config factory method."""

    def test_from_config_basic(self):
        config = {"connection": "mqtt://localhost:1883"}
        t = MqttTransporter.from_config(config, _mock_transit(), AsyncMock(), "node-1")
        assert t.connection_string == "mqtt://localhost:1883"
        assert t.qos == 0
        assert t.topic_separator == "."

    def test_from_config_with_options(self):
        config = {
            "connection": "mqtt://broker:1883",
            "qos": 1,
            "topicSeparator": "/",
        }
        t = MqttTransporter.from_config(config, _mock_transit(), AsyncMock(), "node-1")
        assert t.qos == 1
        assert t.topic_separator == "/"

    def test_from_config_python_style_separator(self):
        config = {
            "connection": "mqtt://broker:1883",
            "topic_separator": "/",
        }
        t = MqttTransporter.from_config(config, _mock_transit(), AsyncMock(), "node-1")
        assert t.topic_separator == "/"

    def test_from_config_missing_connection_raises(self):
        with pytest.raises(KeyError, match="connection"):
            MqttTransporter.from_config({}, _mock_transit())


class TestMqttHandleMessage:
    """Test incoming message handling."""

    @pytest.mark.asyncio
    async def test_handle_heartbeat_message(self):
        handler = AsyncMock()
        t = _make_transporter(handler=handler)

        msg = MagicMock()
        msg.topic = MagicMock()
        msg.topic.__str__ = Mock(return_value="MOL.HEARTBEAT.node123")
        msg.payload = b'{"sender": "remote-node", "cpu": 30.5}'

        await t._handle_message(msg)

        handler.assert_awaited_once()
        packet = handler.call_args[0][0]
        assert isinstance(packet, Packet)
        assert packet.sender == "remote-node"
        assert packet.type == Topic.HEARTBEAT
        assert packet.payload["cpu"] == 30.5

    @pytest.mark.asyncio
    async def test_handle_multiple_packet_types(self):
        """Test all common packet types are routed correctly."""
        test_cases = [
            ("MOL.INFO.node1", b'{"sender": "info-node", "id": "test"}', Topic.INFO),
            ("MOL.HEARTBEAT.node2", b'{"sender": "beat-node", "cpu": 25}', Topic.HEARTBEAT),
            ("MOL.EVENT.node3", b'{"sender": "event-node", "event": "test"}', Topic.EVENT),
            ("MOL.REQ.node4", b'{"sender": "req-node", "action": "test"}', Topic.REQUEST),
            ("MOL.RES.node5", b'{"sender": "res-node", "data": "ok"}', Topic.RESPONSE),
            ("MOL.DISCOVER", b'{"sender": "disc-node"}', Topic.DISCOVER),
            ("MOL.DISCONNECT", b'{"sender": "dc-node"}', Topic.DISCONNECT),
        ]

        for topic_str, data, expected_type in test_cases:
            handler = AsyncMock()
            t = _make_transporter(handler=handler)
            msg = MagicMock()
            msg.topic.__str__ = Mock(return_value=topic_str)
            msg.payload = data

            await t._handle_message(msg)

            assert handler.called, f"Handler not called for {topic_str}"
            packet = handler.call_args[0][0]
            assert packet.type == expected_type, f"Wrong type for {topic_str}"

    @pytest.mark.asyncio
    async def test_handle_message_with_slash_separator(self):
        handler = AsyncMock()
        t = _make_transporter(handler=handler, topic_separator="/")

        msg = MagicMock()
        msg.topic.__str__ = Mock(return_value="MOL/HEARTBEAT/node123")
        msg.payload = b'{"sender": "remote-node", "cpu": 10}'

        await t._handle_message(msg)

        handler.assert_awaited_once()
        packet = handler.call_args[0][0]
        assert packet.type == Topic.HEARTBEAT

    @pytest.mark.asyncio
    async def test_handle_unknown_topic_warns(self, caplog):
        handler = AsyncMock()
        t = _make_transporter(handler=handler)
        msg = MagicMock()
        msg.topic.__str__ = Mock(return_value="UNKNOWN.TOPIC")
        msg.payload = b'{"sender": "remote-node"}'

        with caplog.at_level(logging.WARNING):
            await t._handle_message(msg)

        handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_short_topic_warns(self, caplog):
        handler = AsyncMock()
        t = _make_transporter(handler=handler)
        msg = MagicMock()
        msg.topic.__str__ = Mock(return_value="MOL")
        msg.payload = b"{}"

        with caplog.at_level(logging.WARNING):
            await t._handle_message(msg)

        handler.assert_not_called()
        assert "short topic" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_handle_string_payload_converted_to_bytes(self):
        handler = AsyncMock()
        t = _make_transporter(handler=handler)
        msg = MagicMock()
        msg.topic.__str__ = Mock(return_value="MOL.INFO.node1")
        msg.payload = '{"sender": "str-node"}'  # string, not bytes

        await t._handle_message(msg)

        handler.assert_awaited_once()
        packet = handler.call_args[0][0]
        assert packet.sender == "str-node"


class TestMqttSend:
    """Test send/publish operations."""

    @pytest.mark.asyncio
    async def test_send_no_client_returns_none(self):
        """Node.js: if (!this.client) return; — silent no-op."""
        t = _make_transporter()
        t._client = None
        result = await t.send("MOL.HEARTBEAT", b"data", {})
        assert result is None

    @pytest.mark.asyncio
    async def test_send_publishes_to_client(self):
        t = _make_transporter()
        t._client = AsyncMock()
        await t.send("MOL.REQ.node-1", b"payload", {})
        t._client.publish.assert_awaited_once_with("MOL.REQ.node-1", b"payload", qos=0)

    @pytest.mark.asyncio
    async def test_send_uses_configured_qos(self):
        t = _make_transporter(qos=2)
        t._client = AsyncMock()
        await t.send("MOL.EVENT", b"data", {})
        t._client.publish.assert_awaited_once_with("MOL.EVENT", b"data", qos=2)

    @pytest.mark.asyncio
    async def test_publish_serializes_and_sends(self):
        t = _make_transporter()
        t._client = AsyncMock()

        packet = Packet(Topic.HEARTBEAT, "local-node", {"cpu": 50})
        packet.target = None
        await t.publish(packet)

        t._client.publish.assert_awaited_once()
        call_args = t._client.publish.call_args
        assert call_args[0][0] == "MOL.HEARTBEAT"
        # Verify serialized payload contains sender and ver
        sent_data = call_args[0][1]
        parsed = json.loads(sent_data)
        assert parsed["sender"] == "local-node"
        assert parsed["ver"] == "4"
        assert parsed["cpu"] == 50

    @pytest.mark.asyncio
    async def test_publish_with_target_node(self):
        t = _make_transporter()
        t._client = AsyncMock()

        packet = Packet(Topic.REQUEST, "local-node", {"action": "users.get"})
        packet.target = "remote-node"
        await t.publish(packet)

        call_args = t._client.publish.call_args
        assert call_args[0][0] == "MOL.REQ.remote-node"

    @pytest.mark.asyncio
    async def test_publish_raises_when_disconnected(self):
        t = _make_transporter()
        t._client = None
        packet = Packet(Topic.HEARTBEAT, "local-node", {})
        with pytest.raises(RuntimeError, match="Not connected"):
            await t.publish(packet)


class TestMqttSubscribe:
    """Test subscribe operations."""

    @pytest.mark.asyncio
    async def test_subscribe_calls_client(self):
        t = _make_transporter()
        t._client = AsyncMock()

        await t.subscribe("DISCOVER")

        t._client.subscribe.assert_awaited_once_with("MOL.DISCOVER", qos=0)

    @pytest.mark.asyncio
    async def test_subscribe_with_node_id(self):
        t = _make_transporter()
        t._client = AsyncMock()

        await t.subscribe("REQ", "local-node")

        t._client.subscribe.assert_awaited_once_with("MOL.REQ.local-node", qos=0)

    @pytest.mark.asyncio
    async def test_subscribe_uses_configured_qos(self):
        t = _make_transporter(qos=1)
        t._client = AsyncMock()

        await t.subscribe("EVENT", "node-1")

        t._client.subscribe.assert_awaited_once_with("MOL.EVENT.node-1", qos=1)

    @pytest.mark.asyncio
    async def test_subscribe_raises_when_disconnected(self):
        t = _make_transporter()
        t._client = None
        with pytest.raises(RuntimeError, match="Not connected"):
            await t.subscribe("DISCOVER")


class TestMqttReceive:
    """Test receive (deserialization) pipeline."""

    @pytest.mark.asyncio
    async def test_receive_deserializes_and_calls_handler(self):
        handler = AsyncMock()
        t = _make_transporter(handler=handler)

        data = json.dumps({"sender": "remote-node", "action": "test"}).encode()
        meta = {"packet_type": Topic.REQUEST}

        await t.receive("REQ", data, meta)

        handler.assert_awaited_once()
        packet = handler.call_args[0][0]
        assert packet.sender == "remote-node"
        assert packet.type == Topic.REQUEST

    @pytest.mark.asyncio
    async def test_receive_bad_json_drops_with_warning(self, caplog):
        handler = AsyncMock()
        t = _make_transporter(handler=handler)
        meta = {"packet_type": Topic.INFO}

        with caplog.at_level(logging.WARNING):
            await t.receive("INFO", b"not valid json", meta)

        handler.assert_not_called()
        assert "failed to decode" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_receive_missing_packet_type_raises(self):
        handler = AsyncMock()
        t = _make_transporter(handler=handler)
        data = json.dumps({"sender": "node"}).encode()

        with pytest.raises(ValueError, match="packet_type"):
            await t.receive("INFO", data, {})

    @pytest.mark.asyncio
    async def test_receive_no_handler_raises(self):
        t = _make_transporter(handler=None)
        data = json.dumps({"sender": "node"}).encode()
        meta = {"packet_type": Topic.INFO}

        with pytest.raises(ValueError, match="no handler"):
            await t.receive("INFO", data, meta)


class TestMqttDisconnect:
    """Test disconnect lifecycle."""

    @pytest.mark.asyncio
    async def test_disconnect_cleans_up_client(self):
        t = _make_transporter()
        mock_client = AsyncMock()
        mock_client.__aexit__ = AsyncMock()
        t._client = mock_client
        t._listener_task = None

        await t.disconnect()

        mock_client.__aexit__.assert_awaited_once()
        assert t._client is None
        assert t._shutting_down is True

    @pytest.mark.asyncio
    async def test_disconnect_no_client_is_safe(self):
        t = _make_transporter()
        t._client = None
        t._listener_task = None
        await t.disconnect()  # Should not raise

    @pytest.mark.asyncio
    async def test_disconnect_cancels_listener_task(self):
        t = _make_transporter()
        mock_client = AsyncMock()
        mock_client.__aexit__ = AsyncMock()
        t._client = mock_client

        # Create a real asyncio future that resolves immediately
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        fut.set_result(None)

        # Wrap in a task-like Mock that has done() and cancel()
        real_task = asyncio.ensure_future(asyncio.sleep(0))
        t._listener_task = real_task

        await t.disconnect()

        assert t._listener_task is None


class TestMqttRegistry:
    """Test transporter registry integration."""

    def test_mqtt_in_transporter_registry(self):
        """MqttTransporter should be discoverable via get_by_name."""
        from moleculerpy.transporter.base import Transporter

        config = {"connection": "mqtt://localhost:1883"}
        transit = _mock_transit()

        t = Transporter.get_by_name("mqtt", config, transit, AsyncMock(), "node-1")
        assert isinstance(t, MqttTransporter)
        assert t.connection_string == "mqtt://localhost:1883"
