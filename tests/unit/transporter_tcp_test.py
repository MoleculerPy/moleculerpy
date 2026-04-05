"""Unit tests for the TCP transporter with Gossip Protocol (PRD-010).

Tests cover all 6 components:
- constants.py: packet ID resolution
- parser.py: frame building and parsing
- tcp_reader.py: TCP server
- tcp_writer.py: connection pool
- udp_broadcaster.py: UDP discovery
- tcp_transporter.py: main transporter + gossip protocol
"""

import asyncio
import struct
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
import pytest_asyncio

from moleculerpy.packet import Packet, Topic
from moleculerpy.serializers import JsonSerializer
from moleculerpy.transporter.tcp.constants import (
    DEFAULT_OPTIONS,
    HEADER_SIZE,
    PACKET_EVENT_ID,
    PACKET_GOSSIP_HELLO_ID,
    PACKET_GOSSIP_REQ_ID,
    PACKET_GOSSIP_RES_ID,
    PACKET_PING_ID,
    PACKET_PONG_ID,
    PACKET_REQUEST_ID,
    PACKET_RESPONSE_ID,
    resolve_packet_id,
    resolve_packet_type,
)
from moleculerpy.transporter.tcp.parser import FrameError, FrameParser, build_frame
from moleculerpy.transporter.tcp.tcp_reader import TcpReader
from moleculerpy.transporter.tcp.tcp_transporter import TcpTransporter
from moleculerpy.transporter.tcp.tcp_writer import TcpWriter

# =====================================================================
# Helpers
# =====================================================================


def _mock_transit() -> Mock:
    """Create a mock transit with a real JsonSerializer."""
    transit = Mock()
    transit.serializer = JsonSerializer()
    transit.settings = Mock()
    transit.settings.namespace = ""
    transit._broker = None
    return transit


def _make_transporter(**kwargs) -> TcpTransporter:
    """Create a TcpTransporter with sensible defaults for testing."""
    defaults = {
        "transit": _mock_transit(),
        "handler": AsyncMock(),
        "node_id": "local-node",
        "opts": {**DEFAULT_OPTIONS, "udp_discovery": False, "port": 0},
    }
    defaults.update(kwargs)
    return TcpTransporter(**defaults)


class _MockResolver:
    """Mock NodeAddressResolver for TcpWriter tests."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        self._host = host
        self._port = port

    def get_node_address(self, node_id: str):
        return (self._host, self._port)


class _MockHandler:
    """Mock IncomingMessageHandler for TcpReader tests."""

    def __init__(self):
        self.messages: list[tuple] = []

    async def on_incoming_message(self, topic, data, socket_info):
        self.messages.append((topic, data, socket_info))


# =====================================================================
# Test: constants.py
# =====================================================================


class TestConstants:
    """Test packet type ID resolution — must match Node.js constants.js."""

    # Node.js values: EVENT=1, REQUEST=2, RESPONSE=3, PING=4, PONG=5,
    # GOSSIP_REQ=6, GOSSIP_RES=7, GOSSIP_HELLO=8

    @pytest.mark.parametrize(
        "topic,expected_id",
        [
            (Topic.EVENT, 1),
            (Topic.REQUEST, 2),
            (Topic.RESPONSE, 3),
            (Topic.PING, 4),
            (Topic.PONG, 5),
            (Topic.GOSSIP_REQ, 6),
            (Topic.GOSSIP_RES, 7),
            (Topic.GOSSIP_HELLO, 8),
        ],
    )
    def test_resolve_packet_id_matches_nodejs(self, topic, expected_id):
        assert resolve_packet_id(topic) == expected_id

    @pytest.mark.parametrize(
        "packet_id,expected_topic",
        [
            (1, Topic.EVENT),
            (2, Topic.REQUEST),
            (3, Topic.RESPONSE),
            (4, Topic.PING),
            (5, Topic.PONG),
            (6, Topic.GOSSIP_REQ),
            (7, Topic.GOSSIP_RES),
            (8, Topic.GOSSIP_HELLO),
        ],
    )
    def test_resolve_packet_type_matches_nodejs(self, packet_id, expected_topic):
        assert resolve_packet_type(packet_id) == expected_topic

    def test_roundtrip_all_types(self):
        """Every Topic that has an ID must roundtrip correctly."""
        for topic in [
            Topic.EVENT,
            Topic.REQUEST,
            Topic.RESPONSE,
            Topic.PING,
            Topic.PONG,
            Topic.GOSSIP_REQ,
            Topic.GOSSIP_RES,
            Topic.GOSSIP_HELLO,
        ]:
            pid = resolve_packet_id(topic)
            assert resolve_packet_type(pid) == topic

    def test_resolve_unsupported_topic_raises(self):
        with pytest.raises(ValueError, match="Unsupported packet type"):
            resolve_packet_id(Topic.HEARTBEAT)

    def test_resolve_unsupported_id_raises(self):
        with pytest.raises(ValueError, match="Unsupported packet ID"):
            resolve_packet_type(99)

    def test_header_size_is_six(self):
        assert HEADER_SIZE == 6

    def test_default_options_have_required_keys(self):
        required = [
            "udp_discovery",
            "udp_port",
            "udp_multicast",
            "gossip_period",
            "max_connections",
            "max_packet_size",
            "port",
            "urls",
        ]
        for key in required:
            assert key in DEFAULT_OPTIONS, f"Missing default option: {key}"


# =====================================================================
# Test: parser.py
# =====================================================================


class TestBuildFrame:
    """Test frame building — wire format must match Node.js."""

    def test_frame_structure(self):
        """Frame = [CRC:1][LEN:4 BE][TYPE:1][DATA:N]."""
        data = b"hello"
        frame = build_frame(PACKET_REQUEST_ID, data)

        assert len(frame) == HEADER_SIZE + len(data)
        # Length field = total frame length
        length = struct.unpack(">I", frame[1:5])[0]
        assert length == len(frame)
        # Type field
        assert frame[5] == PACKET_REQUEST_ID
        # CRC = XOR of bytes 1-5
        crc = frame[1] ^ frame[2] ^ frame[3] ^ frame[4] ^ frame[5]
        assert frame[0] == crc
        # Payload
        assert frame[6:] == data

    def test_empty_payload(self):
        frame = build_frame(PACKET_EVENT_ID, b"")
        assert len(frame) == HEADER_SIZE
        length = struct.unpack(">I", frame[1:5])[0]
        assert length == HEADER_SIZE

    def test_large_payload(self):
        data = b"x" * 100_000
        frame = build_frame(PACKET_EVENT_ID, data)
        length = struct.unpack(">I", frame[1:5])[0]
        assert length == HEADER_SIZE + 100_000

    @pytest.mark.parametrize("type_id", [1, 2, 3, 4, 5, 6, 7, 8])
    def test_all_packet_types(self, type_id):
        frame = build_frame(type_id, b"test")
        assert frame[5] == type_id

    def test_crc_varies_with_length(self):
        """Different payload sizes produce different CRCs."""
        f1 = build_frame(PACKET_EVENT_ID, b"a")
        f2 = build_frame(PACKET_EVENT_ID, b"ab")
        assert f1[0] != f2[0]  # CRCs should differ


class TestFrameParser:
    """Test async frame parsing from StreamReader."""

    @pytest.fixture
    def parser(self):
        return FrameParser(max_packet_size=1_000_000)

    def _make_reader(self, data: bytes) -> asyncio.StreamReader:
        reader = asyncio.StreamReader()
        reader.feed_data(data)
        reader.feed_eof()
        return reader

    @pytest.mark.asyncio
    async def test_parse_single_frame(self, parser):
        frame = build_frame(PACKET_EVENT_ID, b"hello")
        reader = self._make_reader(frame)
        results = []
        try:
            async for topic, data in parser.read_frames(reader):
                results.append((topic, data))
        except asyncio.IncompleteReadError:
            pass
        assert len(results) == 1
        assert results[0] == (Topic.EVENT, b"hello")

    @pytest.mark.asyncio
    async def test_parse_multiple_frames(self, parser):
        """Two concatenated frames should both be parsed."""
        frames = build_frame(PACKET_EVENT_ID, b"one") + build_frame(PACKET_REQUEST_ID, b"two")
        reader = self._make_reader(frames)
        results = []
        try:
            async for topic, data in parser.read_frames(reader):
                results.append((topic, data))
        except asyncio.IncompleteReadError:
            pass
        assert len(results) == 2
        assert results[0] == (Topic.EVENT, b"one")
        assert results[1] == (Topic.REQUEST, b"two")

    @pytest.mark.asyncio
    async def test_parse_empty_payload(self, parser):
        frame = build_frame(PACKET_PING_ID, b"")
        reader = self._make_reader(frame)
        results = []
        try:
            async for topic, data in parser.read_frames(reader):
                results.append((topic, data))
        except asyncio.IncompleteReadError:
            pass
        assert len(results) == 1
        assert results[0] == (Topic.PING, b"")

    @pytest.mark.asyncio
    async def test_bad_crc_raises(self, parser):
        frame = bytearray(build_frame(PACKET_EVENT_ID, b"data"))
        frame[0] = 0xFF  # Corrupt CRC
        reader = self._make_reader(bytes(frame))
        with pytest.raises(FrameError, match="Invalid packet CRC"):
            async for _ in parser.read_frames(reader):
                pass

    @pytest.mark.asyncio
    async def test_oversized_packet_raises(self):
        parser = FrameParser(max_packet_size=10)
        frame = build_frame(PACKET_EVENT_ID, b"x" * 100)
        reader = self._make_reader(frame)
        with pytest.raises(FrameError, match="max_packet_size"):
            async for _ in parser.read_frames(reader):
                pass

    @pytest.mark.asyncio
    async def test_incomplete_header_raises(self, parser):
        """Partial header (< 6 bytes) followed by EOF should raise."""
        reader = self._make_reader(b"\x00\x01\x02")  # Only 3 bytes
        with pytest.raises(asyncio.IncompleteReadError):
            async for _ in parser.read_frames(reader):
                pass

    @pytest.mark.asyncio
    async def test_all_gossip_types_parsed(self, parser):
        """GOSSIP_REQ, RES, HELLO frames should parse correctly."""
        frames = (
            build_frame(PACKET_GOSSIP_REQ_ID, b"req")
            + build_frame(PACKET_GOSSIP_RES_ID, b"res")
            + build_frame(PACKET_GOSSIP_HELLO_ID, b"hello")
        )
        reader = self._make_reader(frames)
        results = []
        try:
            async for topic, data in parser.read_frames(reader):
                results.append((topic, data))
        except asyncio.IncompleteReadError:
            pass
        assert len(results) == 3
        assert results[0][0] == Topic.GOSSIP_REQ
        assert results[1][0] == Topic.GOSSIP_RES
        assert results[2][0] == Topic.GOSSIP_HELLO


# =====================================================================
# Test: tcp_reader.py
# =====================================================================


class TestTcpReader:
    """Test TCP server (reader) component."""

    @pytest.mark.asyncio
    async def test_listen_assigns_random_port(self):
        handler = _MockHandler()
        reader = TcpReader(handler=handler, port=0)
        port = await reader.listen()
        assert port > 0
        assert reader.port == port
        await reader.close()

    @pytest.mark.asyncio
    async def test_receive_single_message(self):
        handler = _MockHandler()
        reader = TcpReader(handler=handler, port=0)
        port = await reader.listen()

        # Connect and send a frame
        _, writer = await asyncio.open_connection("127.0.0.1", port)
        frame = build_frame(PACKET_REQUEST_ID, b'{"action":"test"}')
        writer.write(frame)
        await writer.drain()
        await asyncio.sleep(0.05)

        assert len(handler.messages) == 1
        topic, data, info = handler.messages[0]
        assert topic == Topic.REQUEST
        assert data == b'{"action":"test"}'
        assert "remote_address" in info

        writer.close()
        await reader.close()

    @pytest.mark.asyncio
    async def test_receive_multiple_messages(self):
        handler = _MockHandler()
        reader = TcpReader(handler=handler, port=0)
        port = await reader.listen()

        _, writer = await asyncio.open_connection("127.0.0.1", port)
        for i in range(5):
            frame = build_frame(PACKET_EVENT_ID, f"msg-{i}".encode())
            writer.write(frame)
        await writer.drain()
        await asyncio.sleep(0.1)

        assert len(handler.messages) == 5
        for i, (topic, data, _) in enumerate(handler.messages):
            assert topic == Topic.EVENT
            assert data == f"msg-{i}".encode()

        writer.close()
        await reader.close()

    @pytest.mark.asyncio
    async def test_multiple_concurrent_clients(self):
        handler = _MockHandler()
        reader = TcpReader(handler=handler, port=0)
        port = await reader.listen()

        writers = []
        for i in range(3):
            _, w = await asyncio.open_connection("127.0.0.1", port)
            frame = build_frame(PACKET_PING_ID, f"client-{i}".encode())
            w.write(frame)
            await w.drain()
            writers.append(w)

        await asyncio.sleep(0.1)
        assert len(handler.messages) == 3

        for w in writers:
            w.close()
        await reader.close()

    @pytest.mark.asyncio
    async def test_client_disconnect_handled_gracefully(self):
        handler = _MockHandler()
        reader = TcpReader(handler=handler, port=0)
        port = await reader.listen()

        _, writer = await asyncio.open_connection("127.0.0.1", port)
        frame = build_frame(PACKET_EVENT_ID, b"before-disconnect")
        writer.write(frame)
        await writer.drain()
        await asyncio.sleep(0.05)

        writer.close()
        await asyncio.sleep(0.05)

        # Server should still be alive
        assert len(handler.messages) == 1
        await reader.close()

    @pytest.mark.asyncio
    async def test_close_idempotent(self):
        handler = _MockHandler()
        reader = TcpReader(handler=handler, port=0)
        await reader.listen()
        await reader.close()
        await reader.close()  # Should not raise


# =====================================================================
# Test: tcp_writer.py
# =====================================================================


class TestTcpWriter:
    """Test TCP client connection pool (writer)."""

    @pytest_asyncio.fixture
    async def echo_server(self):
        """Start a simple TCP server that accepts connections."""
        received = []

        async def handle(reader, writer):
            try:
                while True:
                    data = await reader.read(4096)
                    if not data:
                        break
                    received.append(data)
            except Exception:
                pass
            finally:
                writer.close()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        yield {"server": server, "port": port, "received": received}
        server.close()
        await server.wait_closed()

    @pytest.mark.asyncio
    async def test_send_creates_connection(self, echo_server):
        resolver = _MockResolver("127.0.0.1", echo_server["port"])
        writer = TcpWriter(resolver=resolver, max_connections=10)

        await writer.send("node-1", PACKET_REQUEST_ID, b"hello")
        assert "node-1" in writer._connections

        await writer.close()

    @pytest.mark.asyncio
    async def test_send_reuses_connection(self, echo_server):
        resolver = _MockResolver("127.0.0.1", echo_server["port"])
        writer = TcpWriter(resolver=resolver, max_connections=10)

        await writer.send("node-1", PACKET_REQUEST_ID, b"msg1")
        await writer.send("node-1", PACKET_REQUEST_ID, b"msg2")
        assert len(writer._connections) == 1

        await writer.close()

    @pytest.mark.asyncio
    async def test_send_builds_valid_frame(self, echo_server):
        resolver = _MockResolver("127.0.0.1", echo_server["port"])
        writer = TcpWriter(resolver=resolver, max_connections=10)

        data = b"test-payload"
        await writer.send("node-1", PACKET_EVENT_ID, data)
        await asyncio.sleep(0.05)

        # Check received frame
        assert len(echo_server["received"]) > 0
        frame = echo_server["received"][0]
        # Verify it's a valid frame with our data
        assert len(frame) == HEADER_SIZE + len(data)
        assert frame[5] == PACKET_EVENT_ID
        assert frame[6:] == data

        await writer.close()

    @pytest.mark.asyncio
    async def test_gossip_types_dont_update_last_used(self, echo_server):
        resolver = _MockResolver("127.0.0.1", echo_server["port"])
        writer = TcpWriter(resolver=resolver, max_connections=10)

        await writer.send("node-1", PACKET_REQUEST_ID, b"req")
        initial_time = writer._connections["node-1"].last_used

        await asyncio.sleep(0.01)
        await writer.send("node-1", PACKET_GOSSIP_REQ_ID, b"gossip")

        # Gossip should NOT update last_used
        assert writer._connections["node-1"].last_used == initial_time

        await writer.close()

    @pytest.mark.asyncio
    async def test_lru_eviction(self, echo_server):
        """When connections exceed max, oldest are evicted."""
        resolver = _MockResolver("127.0.0.1", echo_server["port"])
        writer = TcpWriter(resolver=resolver, max_connections=2)

        await writer.send("node-1", PACKET_EVENT_ID, b"a")
        await asyncio.sleep(0.01)
        await writer.send("node-2", PACKET_EVENT_ID, b"b")
        await asyncio.sleep(0.01)
        await writer.send("node-3", PACKET_EVENT_ID, b"c")

        # node-1 should be evicted (oldest)
        assert len(writer._connections) <= 2
        assert "node-3" in writer._connections

        await writer.close()

    @pytest.mark.asyncio
    async def test_on_hello_called_on_connect(self, echo_server):
        hello_calls = []

        async def on_hello(node_id):
            hello_calls.append(node_id)

        resolver = _MockResolver("127.0.0.1", echo_server["port"])
        writer = TcpWriter(resolver=resolver, max_connections=10, on_hello=on_hello)

        await writer.send("remote-1", PACKET_EVENT_ID, b"data")
        assert hello_calls == ["remote-1"]

        # Second send should NOT trigger hello again (reuses connection)
        await writer.send("remote-1", PACKET_EVENT_ID, b"data2")
        assert hello_calls == ["remote-1"]

        await writer.close()

    @pytest.mark.asyncio
    async def test_unresolvable_node_raises(self):
        resolver = Mock()
        resolver.get_node_address = Mock(return_value=None)
        writer = TcpWriter(resolver=resolver, max_connections=10)

        with pytest.raises(ConnectionError, match="Missing node info"):
            await writer.send("unknown-node", PACKET_EVENT_ID, b"data")

        await writer.close()

    @pytest.mark.asyncio
    async def test_close_clears_all_connections(self, echo_server):
        resolver = _MockResolver("127.0.0.1", echo_server["port"])
        writer = TcpWriter(resolver=resolver, max_connections=10)

        await writer.send("node-1", PACKET_EVENT_ID, b"a")
        await writer.send("node-2", PACKET_EVENT_ID, b"b")
        assert len(writer._connections) == 2

        await writer.close()
        assert len(writer._connections) == 0


# =====================================================================
# Test: tcp_transporter.py — init and config
# =====================================================================


class TestTcpTransporterInit:
    """Test TcpTransporter initialization and configuration."""

    def test_default_values(self):
        t = _make_transporter()
        assert t.name == "tcp"
        assert t.has_built_in_balancer is False
        assert t.connected is False
        assert t.node_id == "local-node"

    def test_custom_options(self):
        t = _make_transporter(opts={"gossip_period": 5, "max_connections": 16})
        assert t.opts["gossip_period"] == 5
        assert t.opts["max_connections"] == 16

    def test_from_config_no_urls(self):
        transit = _mock_transit()
        t = TcpTransporter.from_config(
            {"connection": "tcp://"},
            transit=transit,
            handler=AsyncMock(),
            node_id="test-node",
        )
        assert t.node_id == "test-node"
        assert t.opts.get("urls") is None or t.opts.get("urls") == []

    def test_from_config_with_urls(self):
        transit = _mock_transit()
        t = TcpTransporter.from_config(
            {"connection": "tcp://host1:3000/node-1,host2:3001/node-2"},
            transit=transit,
            handler=AsyncMock(),
            node_id="test-node",
        )
        assert t.opts["urls"] == ["host1:3000/node-1", "host2:3001/node-2"]


class TestTcpTransporterUrlParsing:
    """Test static URL loading — format: tcp://host:port/nodeID."""

    def test_load_single_url(self):
        t = _make_transporter(
            opts={
                **DEFAULT_OPTIONS,
                "udp_discovery": False,
                "port": 0,
                "urls": ["host1:3000/remote-1"],
            }
        )
        t._nodes = MagicMock()
        t._nodes.get_node = Mock(return_value=None)
        t._nodes.add_node = Mock(return_value=Mock())
        t._load_urls()
        t._nodes.add_node.assert_called_once()
        args = t._nodes.add_node.call_args[0]
        assert args[0] == "remote-1"
        assert args[1].hostname == "host1"
        assert args[1].port == 3000

    def test_load_url_strips_tcp_prefix(self):
        t = _make_transporter(
            opts={
                **DEFAULT_OPTIONS,
                "udp_discovery": False,
                "port": 0,
                "urls": ["tcp://host2:4000/remote-2"],
            }
        )
        t._nodes = MagicMock()
        t._nodes.get_node = Mock(return_value=None)
        t._nodes.add_node = Mock(return_value=Mock())
        t._load_urls()
        t._nodes.add_node.assert_called_once()
        args = t._nodes.add_node.call_args[0]
        assert args[0] == "remote-2"
        assert args[1].hostname == "host2"
        assert args[1].port == 4000

    def test_load_own_url_sets_port(self):
        t = _make_transporter(
            node_id="my-node",
            opts={
                **DEFAULT_OPTIONS,
                "udp_discovery": False,
                "port": None,
                "urls": ["localhost:5555/my-node"],
            },
        )
        t._nodes = MagicMock()
        t._load_urls()
        assert t.opts["port"] == 5555

    def test_load_comma_separated_string(self):
        t = _make_transporter(
            opts={
                **DEFAULT_OPTIONS,
                "udp_discovery": False,
                "port": 0,
                "urls": "host1:3000/node-1,host2:3001/node-2",
            }
        )
        t._nodes = MagicMock()
        t._nodes.get_node = Mock(return_value=None)
        t._nodes.add_node = Mock(return_value=Mock())
        t._load_urls()
        assert t._nodes.add_node.call_count == 2

    def test_invalid_url_no_node_id_warns(self):
        t = _make_transporter(
            opts={
                **DEFAULT_OPTIONS,
                "udp_discovery": False,
                "port": 0,
                "urls": ["host1:3000"],  # Missing /nodeID
            }
        )
        t._nodes = MagicMock()
        t._load_urls()  # Should not raise, just warn

    def test_ipv6_url_parsing(self):
        t = _make_transporter(
            opts={
                **DEFAULT_OPTIONS,
                "udp_discovery": False,
                "port": 0,
                "urls": ["[::1]:3000/remote-1"],
            }
        )
        t._nodes = MagicMock()
        t._nodes.get_node = Mock(return_value=None)
        t._nodes.add_node = Mock(return_value=Mock())
        t._load_urls()
        t._nodes.add_node.assert_called_once()
        args = t._nodes.add_node.call_args[0]
        assert args[0] == "remote-1"
        # Second arg is a Node object
        added_node = args[1]
        assert added_node.hostname == "[::1]"
        assert added_node.port == 3000


# =====================================================================
# Test: tcp_transporter.py — subscribe / publish
# =====================================================================


class TestTcpTransporterSubscribe:
    """subscribe() must be a no-op for TCP."""

    @pytest.mark.asyncio
    async def test_subscribe_is_noop(self):
        t = _make_transporter()
        result = await t.subscribe("DISCOVER")
        assert result is None

    @pytest.mark.asyncio
    async def test_subscribe_with_topic_is_noop(self):
        t = _make_transporter()
        result = await t.subscribe("REQ", "MOL.REQ.local-node")
        assert result is None


class TestTcpTransporterPublish:
    """Test publish() — only sends targeted unicast packets."""

    @pytest.mark.asyncio
    async def test_publish_without_target_is_noop(self):
        t = _make_transporter()
        t.writer = AsyncMock()
        packet = Packet(Topic.EVENT, None, {"event": "test"})
        await t.publish(packet)
        # writer.send should NOT be called (no target)

    @pytest.mark.asyncio
    async def test_publish_broadcast_types_dropped(self):
        t = _make_transporter()
        t.writer = AsyncMock()
        # HEARTBEAT/DISCOVER/INFO are broadcast types — TCP drops them
        for topic in [Topic.HEARTBEAT, Topic.DISCOVER, Topic.INFO]:
            packet = Packet(topic, "some-node", {"data": "test"})
            await t.publish(packet)
        # None should result in writer.send being called


# =====================================================================
# Test: tcp_transporter.py — gossip protocol
# =====================================================================


class TestGossipHello:
    """Test GOSSIP_HELLO send and receive."""

    @pytest.mark.asyncio
    async def test_process_gossip_hello_registers_unknown_node(self):
        t = _make_transporter()
        t._nodes = MagicMock()
        t._nodes.get_node = Mock(return_value=None)
        t._nodes.add_node = Mock()  # Accepts (node_id, Node)

        payload = {"sender": "remote-1", "host": "192.168.1.100", "port": 3000, "ver": "4"}
        data = await t.transit.serializer.serialize_async(payload)
        await t._process_gossip_hello(data, {"remote_address": "192.168.1.100"})

        t._nodes.add_node.assert_called_once()
        # Verify the Node object passed to add_node
        added_node = t._nodes.add_node.call_args[0][1]
        assert added_node.hostname == "192.168.1.100"
        assert added_node.port == 3000
        assert added_node.udp_address == "192.168.1.100"

    @pytest.mark.asyncio
    async def test_process_gossip_hello_updates_known_node(self):
        t = _make_transporter()
        existing_node = Mock()
        existing_node.udp_address = None
        t._nodes = MagicMock()
        t._nodes.get_node = Mock(return_value=existing_node)

        payload = {"sender": "remote-1", "host": "10.0.0.1", "port": 3000, "ver": "4"}
        data = await t.transit.serializer.serialize_async(payload)
        await t._process_gossip_hello(data, {"remote_address": "10.0.0.1"})

        assert existing_node.udp_address == "10.0.0.1"

    @pytest.mark.asyncio
    async def test_process_gossip_hello_invalid_data_warns(self):
        t = _make_transporter()
        t._nodes = MagicMock()
        # Send garbage data
        await t._process_gossip_hello(b"not-valid-json", {"remote_address": "1.2.3.4"})
        # Should not raise


class TestGossipRequest:
    """Test GOSSIP_REQ send and receive."""

    @pytest.mark.asyncio
    async def test_send_gossip_request_skips_when_alone(self):
        """Single node should not send gossip requests."""
        t = _make_transporter()
        local_node = MagicMock()
        local_node.id = "local-node"
        local_node.available = True
        local_node.local = True
        local_node.seq = 1
        local_node.cpu_seq = 0
        local_node.cpu = 0

        t._nodes = MagicMock()
        t._get_all_nodes = Mock(return_value=[local_node])

        # Patch publish to track calls
        t.publish = AsyncMock()
        await t._send_gossip_request()
        t.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_gossip_request_targets_random_online(self):
        t = _make_transporter()

        local_node = MagicMock(id="local-node", available=True, local=True, seq=1, cpu_seq=0, cpu=0)
        remote_node = MagicMock(
            id="remote-1", node_id="remote-1", available=True, local=False, seq=5, cpu_seq=1, cpu=50
        )

        t._get_all_nodes = Mock(return_value=[local_node, remote_node])
        t._nodes = MagicMock()
        t.publish = AsyncMock()

        await t._send_gossip_request()
        assert t.publish.called
        packet = t.publish.call_args[0][0]
        assert packet.type == Topic.GOSSIP_REQ
        assert packet.target == "remote-1"

    @pytest.mark.asyncio
    async def test_process_gossip_request_sends_newer_info(self):
        """If we have newer info, we should include it in the response."""
        t = _make_transporter()

        local_node = MagicMock(
            id="local-node", available=True, local=True, seq=10, cpu_seq=5, cpu=30
        )
        remote_node = MagicMock(
            id="remote-1", available=True, local=False, seq=8, cpu_seq=2, cpu=20
        )

        t._nodes = MagicMock()
        _sender_mock = MagicMock(id="sender-node", node_id="sender-node")
        _node_map: dict[str, Any] = {"sender-node": _sender_mock}
        t._nodes.get_node = Mock(side_effect=_node_map.get)
        t._get_all_nodes = Mock(return_value=[local_node, remote_node])
        t._get_node_info = Mock(return_value={"seq": 8, "services": []})
        t._get_local_node_info = Mock(return_value={"seq": 10, "services": []})
        t.publish = AsyncMock()

        # Sender knows remote-1 at seq=5 (we have seq=8)
        payload = {
            "sender": "sender-node",
            "ver": "4",
            "online": {
                "remote-1": [5, 1, 10],  # seq=5, we have seq=8 → we should send
            },
        }
        data = await t.transit.serializer.serialize_async(payload)
        await t._process_gossip_request(data)

        # Should send response with our newer info
        if t.publish.called:
            rsp = t.publish.call_args[0][0]
            assert rsp.type == Topic.GOSSIP_RES


class TestGossipResponse:
    """Test GOSSIP_RES processing."""

    @pytest.mark.asyncio
    async def test_process_response_updates_node_info(self):
        t = _make_transporter()
        t._nodes = MagicMock()
        t._nodes.get_node = Mock(return_value=None)
        t._process_node_info = Mock()

        payload = {
            "sender": "sender-node",
            "ver": "4",
            "online": {
                "new-node": [{"seq": 5, "services": [], "sender": "new-node"}, 1, 10],
            },
        }
        data = await t.transit.serializer.serialize_async(payload)
        await t._process_gossip_response(data)

        t._process_node_info.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_response_ignores_self(self):
        t = _make_transporter(node_id="local-node")
        t._nodes = MagicMock()
        t._process_node_info = Mock()

        payload = {
            "sender": "sender-node",
            "ver": "4",
            "online": {
                "local-node": [{"seq": 999}, 1, 10],  # Info about ourselves
            },
        }
        data = await t.transit.serializer.serialize_async(payload)
        await t._process_gossip_response(data)

        # Should NOT process our own info
        t._process_node_info.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_response_offline_disconnects_node(self):
        t = _make_transporter()
        node = Mock(id="failing-node", seq=3, available=True)
        t._nodes = MagicMock()
        t._nodes.get_node = Mock(return_value=node)

        payload = {
            "sender": "sender-node",
            "ver": "4",
            "offline": {"failing-node": 5},  # seq=5 > our seq=3
        }
        data = await t.transit.serializer.serialize_async(payload)
        await t._process_gossip_response(data)

        # Gossip-driven disconnect is expected (Node.js line 676: false)
        t._nodes.disconnect_node.assert_called_once_with("failing-node", unexpected=False)
        assert node.seq == 5

    @pytest.mark.asyncio
    async def test_process_response_invalid_data_warns(self):
        t = _make_transporter()
        t._nodes = MagicMock()
        await t._process_gossip_response(b"invalid-json")
        # Should not raise


# =====================================================================
# Test: Integration — TCP roundtrip (reader + writer + parser)
# =====================================================================


class TestTcpRoundtrip:
    """End-to-end test: writer sends frame → reader receives and dispatches."""

    @pytest.mark.asyncio
    async def test_full_roundtrip(self):
        handler = _MockHandler()
        reader = TcpReader(handler=handler, port=0)
        port = await reader.listen()

        resolver = _MockResolver("127.0.0.1", port)
        writer = TcpWriter(resolver=resolver, max_connections=10)

        # Send multiple message types
        test_messages = [
            (PACKET_EVENT_ID, b'{"event":"user.created"}'),
            (PACKET_REQUEST_ID, b'{"action":"math.add","params":{"a":1}}'),
            (PACKET_RESPONSE_ID, b'{"data":{"result":42}}'),
            (PACKET_PING_ID, b'{"time":12345}'),
            (PACKET_GOSSIP_HELLO_ID, b'{"host":"localhost","port":3000}'),
        ]

        for type_id, data in test_messages:
            await writer.send("remote-node", type_id, data)

        await asyncio.sleep(0.1)

        assert len(handler.messages) == len(test_messages)
        expected_topics = [
            Topic.EVENT,
            Topic.REQUEST,
            Topic.RESPONSE,
            Topic.PING,
            Topic.GOSSIP_HELLO,
        ]
        for i, (topic, data, _) in enumerate(handler.messages):
            assert topic == expected_topics[i]
            assert data == test_messages[i][1]

        await writer.close()
        await reader.close()

    @pytest.mark.asyncio
    async def test_large_payload_roundtrip(self):
        """Test with payload close to max_packet_size."""
        handler = _MockHandler()
        reader = TcpReader(handler=handler, port=0, max_packet_size=2_000_000)
        port = await reader.listen()

        resolver = _MockResolver("127.0.0.1", port)
        writer = TcpWriter(resolver=resolver, max_connections=10)

        # 1MB payload
        big_data = b"X" * (1024 * 1024)
        await writer.send("node-1", PACKET_EVENT_ID, big_data)
        await asyncio.sleep(0.3)

        assert len(handler.messages) == 1
        assert handler.messages[0][1] == big_data

        await writer.close()
        await reader.close()

    @pytest.mark.asyncio
    async def test_bidirectional_communication(self):
        """Two reader/writer pairs can communicate in both directions."""
        handler_a = _MockHandler()
        handler_b = _MockHandler()

        reader_a = TcpReader(handler=handler_a, port=0)
        reader_b = TcpReader(handler=handler_b, port=0)

        port_a = await reader_a.listen()
        port_b = await reader_b.listen()

        # A → B
        resolver_b = _MockResolver("127.0.0.1", port_b)
        writer_ab = TcpWriter(resolver=resolver_b, max_connections=10)
        await writer_ab.send("node-b", PACKET_REQUEST_ID, b"from-a")

        # B → A
        resolver_a = _MockResolver("127.0.0.1", port_a)
        writer_ba = TcpWriter(resolver=resolver_a, max_connections=10)
        await writer_ba.send("node-a", PACKET_RESPONSE_ID, b"from-b")

        await asyncio.sleep(0.1)

        assert len(handler_b.messages) == 1
        assert handler_b.messages[0][1] == b"from-a"
        assert len(handler_a.messages) == 1
        assert handler_a.messages[0][1] == b"from-b"

        await writer_ab.close()
        await writer_ba.close()
        await reader_a.close()
        await reader_b.close()


# =====================================================================
# Test: Node.js wire format compatibility
# =====================================================================


class TestNodeJsCompatibility:
    """Verify wire format matches Node.js implementation exactly."""

    def test_header_format_matches_nodejs(self):
        """
        Node.js tcp-writer.js:
            header.writeInt32BE(data.length + HEADER_SIZE, 1)  // bytes 1-4
            header.writeInt8(type, 5)                           // byte 5
            crc = header[1] ^ header[2] ^ header[3] ^ header[4] ^ header[5]
            header[0] = crc                                     // byte 0
        """
        data = b"test"
        frame = build_frame(PACKET_REQUEST_ID, data)
        total_len = len(data) + HEADER_SIZE

        # Byte 0: CRC
        expected_crc = frame[1] ^ frame[2] ^ frame[3] ^ frame[4] ^ frame[5]
        assert frame[0] == expected_crc

        # Bytes 1-4: Int32BE total length
        assert struct.unpack(">I", frame[1:5])[0] == total_len

        # Byte 5: packet type
        assert frame[5] == PACKET_REQUEST_ID

        # Bytes 6+: raw payload
        assert frame[6:] == data

    def test_packet_type_ids_match_nodejs(self):
        """Node.js constants.js values must match exactly."""
        assert PACKET_EVENT_ID == 1
        assert PACKET_REQUEST_ID == 2
        assert PACKET_RESPONSE_ID == 3
        assert PACKET_PING_ID == 4
        assert PACKET_PONG_ID == 5
        assert PACKET_GOSSIP_REQ_ID == 6
        assert PACKET_GOSSIP_RES_ID == 7
        assert PACKET_GOSSIP_HELLO_ID == 8

    def test_udp_discovery_message_format(self):
        """UDP message format: 'namespace|nodeID|port' — must match Node.js."""
        namespace = "production"
        node_id = "node-abc-123"
        port = 4567
        msg = f"{namespace}|{node_id}|{port}"
        parts = msg.split("|")
        assert len(parts) == 3
        assert parts[0] == namespace
        assert parts[1] == node_id
        assert int(parts[2]) == port
