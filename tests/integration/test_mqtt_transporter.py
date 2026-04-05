"""Integration tests for MQTT transporter with real Mosquitto broker.

Requires: Docker Mosquitto running on localhost:1883
Start with: docker compose -f tests/integration/docker-compose.yaml up -d mosquitto
"""

import asyncio
import json
import logging

import pytest

logger = logging.getLogger(__name__)

# Check if aiomqtt is available
try:
    import aiomqtt

    HAS_AIOMQTT = True
except ImportError:
    HAS_AIOMQTT = False

# Check if Mosquitto is reachable
MOSQUITTO_URL = "mqtt://localhost:1883"


async def _check_mosquitto() -> bool:
    """Check if Mosquitto broker is reachable."""
    if not HAS_AIOMQTT:
        return False
    try:
        async with aiomqtt.Client(hostname="localhost", port=1883) as client:
            await client.publish("test/ping", b"pong")
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def mosquitto_available():
    """Skip tests if Mosquitto is not available."""
    available = asyncio.get_event_loop().run_until_complete(_check_mosquitto())
    if not available:
        pytest.skip("Mosquitto is not reachable at localhost:1883")


@pytest.fixture
def mock_transit():
    """Create a mock transit with real JsonSerializer."""
    from unittest.mock import Mock

    from moleculerpy.serializers import JsonSerializer

    transit = Mock()
    transit.serializer = JsonSerializer()
    return transit


class TestMqttIntegration:
    """Integration tests with real Mosquitto broker."""

    @pytest.mark.asyncio
    async def test_connect_and_disconnect(self, mosquitto_available, mock_transit):
        """Test basic connect/disconnect to Mosquitto."""
        from moleculerpy.transporter.mqtt import MqttTransporter

        t = MqttTransporter(
            connection_string=MOSQUITTO_URL,
            transit=mock_transit,
            handler=None,
            node_id="test-node-1",
        )

        await t.connect()
        assert t._client is not None
        assert t._listener_task is not None

        await t.disconnect()
        assert t._client is None

    @pytest.mark.asyncio
    async def test_subscribe_and_receive(self, mosquitto_available, mock_transit):
        """Test subscribing and receiving a message through MQTT."""
        from unittest.mock import AsyncMock

        from moleculerpy.transporter.mqtt import MqttTransporter

        received = asyncio.Event()
        received_packet = {}

        async def handler(packet):
            received_packet["data"] = packet
            received.set()

        t = MqttTransporter(
            connection_string=MOSQUITTO_URL,
            transit=mock_transit,
            handler=handler,
            node_id="test-receiver",
        )

        await t.connect()
        try:
            # Subscribe to HEARTBEAT
            await t.subscribe("HEARTBEAT")

            # Give subscription time to register
            await asyncio.sleep(0.2)

            # Publish a heartbeat message via raw MQTT
            payload = json.dumps({"sender": "test-sender", "cpu": 42.5}).encode()
            await t._client.publish("MOL.HEARTBEAT", payload, qos=0)

            # Wait for message
            try:
                await asyncio.wait_for(received.wait(), timeout=5.0)
            except TimeoutError:
                pytest.fail("Did not receive MQTT message within 5 seconds")

            assert received_packet["data"].sender == "test-sender"
            assert received_packet["data"].payload["cpu"] == 42.5
        finally:
            await t.disconnect()

    @pytest.mark.asyncio
    async def test_publish_and_receive_between_nodes(self, mosquitto_available, mock_transit):
        """Test two nodes communicating through MQTT broker."""
        from unittest.mock import Mock

        from moleculerpy.packet import Packet, Topic
        from moleculerpy.serializers import JsonSerializer
        from moleculerpy.transporter.mqtt import MqttTransporter

        received = asyncio.Event()
        received_packets = []

        async def receiver_handler(packet):
            received_packets.append(packet)
            received.set()

        # Create two transporter instances (simulating two nodes)
        transit1 = Mock()
        transit1.serializer = JsonSerializer()
        transit2 = Mock()
        transit2.serializer = JsonSerializer()

        sender = MqttTransporter(
            connection_string=MOSQUITTO_URL,
            transit=transit1,
            handler=None,
            node_id="sender-node",
        )
        receiver = MqttTransporter(
            connection_string=MOSQUITTO_URL,
            transit=transit2,
            handler=receiver_handler,
            node_id="receiver-node",
        )

        await sender.connect()
        await receiver.connect()

        try:
            # Receiver subscribes to targeted INFO
            await receiver.subscribe("INFO", "receiver-node")
            await asyncio.sleep(0.2)

            # Sender publishes INFO to receiver
            packet = Packet(Topic.INFO, "sender-node", {"services": ["users", "posts"]})
            packet.target = "receiver-node"
            await sender.publish(packet)

            # Wait for message
            try:
                await asyncio.wait_for(received.wait(), timeout=5.0)
            except TimeoutError:
                pytest.fail("Receiver did not get message within 5 seconds")

            assert len(received_packets) == 1
            pkt = received_packets[0]
            assert pkt.sender == "sender-node"
            assert pkt.payload["services"] == ["users", "posts"]
            assert pkt.payload["ver"] == "4"
        finally:
            await sender.disconnect()
            await receiver.disconnect()

    @pytest.mark.asyncio
    async def test_qos_1_delivery(self, mosquitto_available, mock_transit):
        """Test that QoS 1 messages are delivered reliably."""
        from moleculerpy.transporter.mqtt import MqttTransporter

        received = asyncio.Event()
        count = {"n": 0}

        async def handler(packet):
            count["n"] += 1
            if count["n"] >= 3:
                received.set()

        t = MqttTransporter(
            connection_string=MOSQUITTO_URL,
            transit=mock_transit,
            handler=handler,
            node_id="qos-test",
            qos=1,
        )

        await t.connect()
        try:
            await t.subscribe("HEARTBEAT")
            await asyncio.sleep(0.2)

            # Send 3 messages with QoS 1
            for i in range(3):
                payload = json.dumps({"sender": "qos-sender", "seq": i}).encode()
                await t._client.publish("MOL.HEARTBEAT", payload, qos=1)

            try:
                await asyncio.wait_for(received.wait(), timeout=5.0)
            except TimeoutError:
                pytest.fail(f"Only received {count['n']}/3 QoS 1 messages")

            assert count["n"] >= 3
        finally:
            await t.disconnect()

    @pytest.mark.asyncio
    async def test_topic_separator_slash(self, mosquitto_available, mock_transit):
        """Test custom topic separator (/) works end-to-end."""
        from moleculerpy.transporter.mqtt import MqttTransporter

        received = asyncio.Event()
        received_data = {}

        async def handler(packet):
            received_data["packet"] = packet
            received.set()

        t = MqttTransporter(
            connection_string=MOSQUITTO_URL,
            transit=mock_transit,
            handler=handler,
            node_id="slash-test",
            topic_separator="/",
        )

        await t.connect()
        try:
            await t.subscribe("DISCOVER")
            await asyncio.sleep(0.2)

            # Topic should be MOL/DISCOVER
            payload = json.dumps({"sender": "slash-sender"}).encode()
            await t._client.publish("MOL/DISCOVER", payload, qos=0)

            try:
                await asyncio.wait_for(received.wait(), timeout=5.0)
            except TimeoutError:
                pytest.fail("Did not receive message with / separator")

            assert received_data["packet"].sender == "slash-sender"
        finally:
            await t.disconnect()

    @pytest.mark.asyncio
    async def test_registry_resolves_mqtt(self, mosquitto_available, mock_transit):
        """Test that 'mqtt' resolves via Transporter.get_by_name."""
        from unittest.mock import AsyncMock

        from moleculerpy.transporter.base import Transporter
        from moleculerpy.transporter.mqtt import MqttTransporter

        t = Transporter.get_by_name(
            "mqtt",
            {"connection": MOSQUITTO_URL},
            mock_transit,
            AsyncMock(),
            "registry-test",
        )
        assert isinstance(t, MqttTransporter)

        # Verify it can actually connect
        await t.connect()
        assert t._client is not None
        await t.disconnect()
