"""Unit tests for CBOR and ProtoBuf serializers (PRD-019).

Tests cover:
- CBOR: roundtrip, types, error handling, size comparison
- ProtoBuf: roundtrip per packet type, custom field conversion, error handling
- Both: resolve_serializer registry, import guards
"""

import json
from typing import Any
from unittest.mock import patch

import pytest

from moleculerpy.serializers import resolve_serializer
from moleculerpy.serializers.base import BaseSerializer
from moleculerpy.serializers.cbor import CBOR_AVAILABLE, CborSerializer
from moleculerpy.serializers.json import JsonSerializer
from moleculerpy.serializers.protobuf import PROTOBUF_AVAILABLE, ProtoBufSerializer

# =====================================================================
# CBOR Serializer
# =====================================================================


@pytest.mark.skipif(not CBOR_AVAILABLE, reason="cbor2 not installed")
class TestCborSerializer:
    """Test CborSerializer — schema-less binary, like MsgPack."""

    @pytest.fixture
    def serializer(self) -> CborSerializer:
        return CborSerializer()

    def test_roundtrip_simple(self, serializer: CborSerializer) -> None:
        payload = {"action": "math.add", "ver": "4", "sender": "node-1"}
        data = serializer.serialize(payload)
        result = serializer.deserialize(data)
        assert result == payload

    def test_roundtrip_nested(self, serializer: CborSerializer) -> None:
        payload = {
            "action": "users.create",
            "params": {"name": "John", "age": 30, "tags": ["admin", "user"]},
            "meta": {"auth": True, "token": "abc123"},
            "ver": "4",
            "sender": "node-1",
        }
        data = serializer.serialize(payload)
        result = serializer.deserialize(data)
        assert result == payload

    def test_roundtrip_empty_payload(self, serializer: CborSerializer) -> None:
        payload: dict[str, Any] = {}
        data = serializer.serialize(payload)
        result = serializer.deserialize(data)
        assert result == payload

    def test_roundtrip_numeric_values(self, serializer: CborSerializer) -> None:
        payload = {"cpu": 75, "latency": 0.005, "seq": 42, "ver": "4", "sender": "n1"}
        data = serializer.serialize(payload, packet_type="HEARTBEAT")
        result = serializer.deserialize(data, packet_type="HEARTBEAT")
        assert result["cpu"] == 75
        assert result["latency"] == 0.005

    def test_smaller_than_json(self, serializer: CborSerializer) -> None:
        """CBOR should be more compact than JSON."""
        payload = {
            "action": "math.add",
            "params": {"a": 1, "b": 2},
            "ver": "4",
            "sender": "test-node",
        }
        cbor_data = serializer.serialize(payload)
        json_data = json.dumps(payload).encode()
        assert len(cbor_data) < len(json_data)

    def test_serialize_error_raises(self, serializer: CborSerializer) -> None:
        # cbor2 can't serialize certain types
        from moleculerpy.errors import SerializationError

        with pytest.raises(SerializationError, match="CBOR serialize"):
            serializer.serialize({"func": lambda: None})  # type: ignore[dict-item]

    def test_deserialize_invalid_data(self, serializer: CborSerializer) -> None:
        from moleculerpy.errors import SerializationError

        with pytest.raises(SerializationError):
            serializer.deserialize(b"\xff\xff\xff")

    def test_deserialize_non_dict_raises(self, serializer: CborSerializer) -> None:
        import cbor2

        from moleculerpy.errors import SerializationError

        data = cbor2.dumps([1, 2, 3])  # Array, not dict
        with pytest.raises(SerializationError, match="Expected dict"):
            serializer.deserialize(data)

    def test_resolve_by_name(self) -> None:
        s = resolve_serializer("cbor")
        assert isinstance(s, CborSerializer)

    def test_resolve_case_insensitive(self) -> None:
        s = resolve_serializer("CBOR")
        assert isinstance(s, CborSerializer)


class TestCborImportGuard:
    """Test CBOR serializer when cbor2 is not installed."""

    def test_import_error_when_unavailable(self) -> None:
        with patch("moleculerpy.serializers.cbor.CBOR_AVAILABLE", False):
            with pytest.raises(ImportError, match="cbor2"):
                CborSerializer()


# =====================================================================
# ProtoBuf Serializer
# =====================================================================


@pytest.mark.skipif(not PROTOBUF_AVAILABLE, reason="protobuf not installed")
class TestProtoBufSerializer:
    """Test ProtoBufSerializer — schema-based, per-packet-type encoding."""

    @pytest.fixture
    def serializer(self) -> ProtoBufSerializer:
        return ProtoBufSerializer()

    def test_roundtrip_request(self, serializer: ProtoBufSerializer) -> None:
        payload = {
            "action": "math.add",
            "params": {"a": 1, "b": 2},
            "ver": "4",
            "sender": "node-1",
            "meta": {"auth": True},
        }
        data = serializer.serialize(payload, packet_type="REQ")
        result = serializer.deserialize(data, packet_type="REQ")
        assert result["action"] == "math.add"
        assert result["params"] == {"a": 1, "b": 2}
        assert result["meta"] == {"auth": True}
        assert result["ver"] == "4"
        assert result["sender"] == "node-1"

    def test_roundtrip_response(self, serializer: ProtoBufSerializer) -> None:
        payload = {
            "success": True,
            "data": {"result": 42},
            "ver": "4",
            "sender": "node-1",
            "id": "req-123",
            "error": None,
        }
        data = serializer.serialize(payload, packet_type="RES")
        result = serializer.deserialize(data, packet_type="RES")
        assert result["data"] == {"result": 42}

    def test_roundtrip_event(self, serializer: ProtoBufSerializer) -> None:
        payload = {
            "event": "user.created",
            "data": {"id": 1, "name": "John"},
            "ver": "4",
            "sender": "node-1",
            "meta": {"source": "api"},
        }
        data = serializer.serialize(payload, packet_type="EVENT")
        result = serializer.deserialize(data, packet_type="EVENT")
        assert result["event"] == "user.created"
        assert result["data"] == {"id": 1, "name": "John"}
        assert result["meta"] == {"source": "api"}

    def test_roundtrip_info(self, serializer: ProtoBufSerializer) -> None:
        payload = {
            "services": [{"name": "math", "actions": {"add": {}}}],
            "ver": "4",
            "sender": "node-1",
            "hostname": "localhost",
            "ipList": ["127.0.0.1"],
        }
        data = serializer.serialize(payload, packet_type="INFO")
        result = serializer.deserialize(data, packet_type="INFO")
        assert result["services"] == [{"name": "math", "actions": {"add": {}}}]
        assert result["hostname"] == "localhost"

    def test_roundtrip_discover(self, serializer: ProtoBufSerializer) -> None:
        payload = {"ver": "4", "sender": "node-1"}
        data = serializer.serialize(payload, packet_type="DISCOVER")
        result = serializer.deserialize(data, packet_type="DISCOVER")
        assert result["ver"] == "4"
        assert result["sender"] == "node-1"

    def test_roundtrip_heartbeat(self, serializer: ProtoBufSerializer) -> None:
        payload = {"ver": "4", "sender": "node-1", "cpu": 75}
        data = serializer.serialize(payload, packet_type="HEARTBEAT")
        result = serializer.deserialize(data, packet_type="HEARTBEAT")
        assert result["cpu"] == 75

    def test_roundtrip_ping_pong(self, serializer: ProtoBufSerializer) -> None:
        ping = {"ver": "4", "sender": "n1", "time": 1234567890, "id": "ping-1"}
        data = serializer.serialize(ping, packet_type="PING")
        result = serializer.deserialize(data, packet_type="PING")
        # Proto int64 may return as string from MessageToDict
        assert int(result["time"]) == 1234567890

        pong = {
            "ver": "4",
            "sender": "n2",
            "time": 1234567890,
            "arrived": 1234567891,
            "id": "ping-1",
        }
        data2 = serializer.serialize(pong, packet_type="PONG")
        result2 = serializer.deserialize(data2, packet_type="PONG")
        assert int(result2["arrived"]) == 1234567891

    def test_roundtrip_gossip_hello(self, serializer: ProtoBufSerializer) -> None:
        payload = {"ver": "4", "sender": "n1", "host": "192.168.1.1", "port": 3000}
        data = serializer.serialize(payload, packet_type="GOSSIP_HELLO")
        result = serializer.deserialize(data, packet_type="GOSSIP_HELLO")
        assert result["host"] == "192.168.1.1"
        assert result["port"] == 3000

    def test_roundtrip_gossip_req(self, serializer: ProtoBufSerializer) -> None:
        payload = {
            "ver": "4",
            "sender": "n1",
            "online": {"n1": [1, 0, 0], "n2": [3, 1, 50]},
            "offline": {"n3": 5},
        }
        data = serializer.serialize(payload, packet_type="GOSSIP_REQ")
        result = serializer.deserialize(data, packet_type="GOSSIP_REQ")
        assert result["online"] == {"n1": [1, 0, 0], "n2": [3, 1, 50]}
        assert result["offline"] == {"n3": 5}

    def test_roundtrip_gossip_res(self, serializer: ProtoBufSerializer) -> None:
        payload = {
            "ver": "4",
            "sender": "n1",
            "online": {"n2": [{"seq": 5, "services": []}, 1, 10]},
        }
        data = serializer.serialize(payload, packet_type="GOSSIP_RES")
        result = serializer.deserialize(data, packet_type="GOSSIP_RES")
        assert "n2" in result["online"]

    def test_smaller_than_json(self, serializer: ProtoBufSerializer) -> None:
        """ProtoBuf should be more compact than JSON."""
        payload = {
            "action": "math.add",
            "params": {"a": 1, "b": 2},
            "ver": "4",
            "sender": "test-node",
        }
        proto_data = serializer.serialize(payload)
        json_data = json.dumps(payload).encode()
        assert len(proto_data) < len(json_data)

    def test_deserialize_empty_data_raises(self, serializer: ProtoBufSerializer) -> None:
        from moleculerpy.errors import SerializationError

        with pytest.raises(SerializationError, match="Empty"):
            serializer.deserialize(b"")

    def test_deserialize_invalid_type_index(self, serializer: ProtoBufSerializer) -> None:
        from moleculerpy.errors import SerializationError

        with pytest.raises(SerializationError, match=r"No proto message"):
            serializer.deserialize(b"garbage", packet_type="NONEXISTENT")

    def test_resolve_by_name(self) -> None:
        s = resolve_serializer("protobuf")
        assert isinstance(s, ProtoBufSerializer)


class TestProtoBufImportGuard:
    """Test ProtoBuf serializer when protobuf is not installed."""

    def test_import_error_when_unavailable(self) -> None:
        with patch("moleculerpy.serializers.protobuf.PROTOBUF_AVAILABLE", False):
            with pytest.raises(ImportError, match="protobuf"):
                ProtoBufSerializer()


# =====================================================================
# Size comparison across all serializers
# =====================================================================


class TestSerializerSizeComparison:
    """Compare payload sizes across all serializers."""

    @pytest.mark.skipif(not CBOR_AVAILABLE, reason="cbor2 not installed")
    @pytest.mark.skipif(not PROTOBUF_AVAILABLE, reason="protobuf not installed")
    def test_size_comparison_request(self) -> None:
        payload = {
            "action": "users.create",
            "params": {"name": "John Doe", "email": "john@example.com", "age": 30},
            "ver": "4",
            "sender": "gateway-node-001",
            "meta": {"auth": True, "requestId": "abc-123-def-456"},
        }

        json_s = JsonSerializer()
        cbor_s = CborSerializer()
        proto_s = ProtoBufSerializer()

        json_data = json_s.serialize(payload)
        cbor_data = cbor_s.serialize(payload)
        proto_data = proto_s.serialize(payload)

        print(f"\n  JSON:     {len(json_data):4d} bytes")
        print(
            f"  CBOR:     {len(cbor_data):4d} bytes ({100 - len(cbor_data) * 100 // len(json_data)}% smaller)"
        )
        print(
            f"  ProtoBuf: {len(proto_data):4d} bytes ({100 - len(proto_data) * 100 // len(json_data)}% smaller)"
        )

        # Both should be smaller than JSON
        assert len(cbor_data) < len(json_data)
        assert len(proto_data) < len(json_data)


# =====================================================================
# Registry
# =====================================================================


class TestSerializerRegistry:
    """Test serializer name resolution."""

    def test_resolve_json(self) -> None:
        assert isinstance(resolve_serializer("json"), JsonSerializer)

    @pytest.mark.skipif(not CBOR_AVAILABLE, reason="cbor2 not installed")
    def test_resolve_cbor(self) -> None:
        assert isinstance(resolve_serializer("cbor"), CborSerializer)

    @pytest.mark.skipif(not PROTOBUF_AVAILABLE, reason="protobuf not installed")
    def test_resolve_protobuf(self) -> None:
        assert isinstance(resolve_serializer("protobuf"), ProtoBufSerializer)

    def test_resolve_unknown_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown serializer"):
            resolve_serializer("avro")
