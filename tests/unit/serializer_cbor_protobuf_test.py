"""Unit tests for CBOR and ProtoBuf serializers (PRD-019).

Tests cover:
- CBOR: roundtrip, types, error handling, size comparison
- ProtoBuf: roundtrip per packet type, custom field conversion, error handling
- Both: resolve_serializer registry, import guards
"""

import json
from datetime import UTC
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

    def test_heartbeat_extra_fields_dropped_for_nodejs_parity(
        self, serializer: ProtoBufSerializer
    ) -> None:
        # PacketHeartbeat wire format matches Node.js exactly: only ver/sender/cpu.
        # Field numbers 4-7 are reserved (formerly seq/instanceID/memory/cpuSeq).
        # Extra keys in payload must be silently dropped during serialization.
        payload = {
            "ver": "4",
            "sender": "node-1",
            "cpu": 50.5,
            "seq": 42,
            "instanceID": "abc-123-instance",
            "memory": 1024.75,
            "cpuSeq": 7,
        }
        data = serializer.serialize(payload, packet_type="HEARTBEAT")
        result = serializer.deserialize(data, packet_type="HEARTBEAT")
        assert result["cpu"] == 50.5
        assert result["sender"] == "node-1"
        for dropped in ("seq", "instanceID", "memory", "cpuSeq"):
            assert dropped not in result

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


# =====================================================================
# AUDIT-DRIVEN TESTS — coverage gaps identified by expert panel
# =====================================================================


class TestPayloadSizeLimit:
    """Test MAX_PAYLOAD_BYTES enforcement (security-HIGH1)."""

    def test_json_sync_deserialize_rejects_oversized(self) -> None:
        from moleculerpy.errors import SerializationError

        s = JsonSerializer()
        oversized = b"x" * (BaseSerializer.MAX_PAYLOAD_BYTES + 1)
        with pytest.raises(SerializationError, match="Payload too large"):
            s.deserialize(oversized)

    @pytest.mark.skipif(not CBOR_AVAILABLE, reason="cbor2 not installed")
    def test_cbor_sync_deserialize_rejects_oversized(self) -> None:
        from moleculerpy.errors import SerializationError

        s = CborSerializer()
        oversized = b"x" * (BaseSerializer.MAX_PAYLOAD_BYTES + 1)
        with pytest.raises(SerializationError, match="Payload too large"):
            s.deserialize(oversized)

    def test_json_serialize_rejects_oversized_output(self) -> None:
        # serialize produces oversized result → should raise
        from moleculerpy.errors import SerializationError

        s = JsonSerializer()
        huge_payload: dict[str, Any] = {"data": "x" * (BaseSerializer.MAX_PAYLOAD_BYTES + 10)}
        with pytest.raises(SerializationError, match="Serialized payload too large"):
            s.serialize(huge_payload)


@pytest.mark.skipif(not CBOR_AVAILABLE, reason="cbor2 not installed")
class TestCborTagSecurity:
    """Test CBOR tag handling (security-HIGH2).

    NOTE: cbor2 tag_hook is ONLY called for unknown tags. Well-known tags
    (0, 1 = datetime, 2, 3 = bigint, 4, 5 = decimal, etc.) have hardcoded
    decoders that run BEFORE tag_hook. Our tag_hook protects against:
    - Unknown/custom tags from attackers
    - Expensive regex compilation (tag 35)
    - Tag 259 (for Maps — disabled in Node.js cbor-x too)

    For full protection against type confusion, downstream code must validate
    value types after deserialization.
    """

    def test_cbor_unknown_tag_rejected(self) -> None:
        """Unknown/custom tags should be rejected by tag_hook → None."""
        import cbor2 as cbor2_mod

        s = CborSerializer()
        # Tag 65535 is definitively unknown — tag_hook handles it
        original = {"custom": cbor2_mod.CBORTag(65535, "some_value")}
        encoded = cbor2_mod.dumps(original)
        result = s.deserialize(encoded)
        # Unknown tag was rejected → value is None
        assert result["custom"] is None

    def test_cbor_plain_dict_still_works(self) -> None:
        """tag_hook only rejects unknown tags, not normal maps."""
        s = CborSerializer()
        payload = {"a": 1, "b": [1, 2, 3], "c": "hello", "d": None, "e": True}
        data = s.serialize(payload)
        assert s.deserialize(data) == payload

    def test_cbor_datetime_tag_decoded(self) -> None:
        """Tag 0/1 (datetime) is a well-known tag with hardcoded decoder.

        cbor2 decodes it to datetime BEFORE tag_hook is called. Downstream
        code must be aware that CBOR deserialize may return datetime objects
        when Node.js cbor-x sends tag 0/1.
        """
        from datetime import datetime, timezone

        import cbor2 as cbor2_mod

        s = CborSerializer()
        original = {"ts": datetime(2026, 4, 6, 0, 0, 0, tzinfo=UTC)}
        encoded = cbor2_mod.dumps(original)
        result = s.deserialize(encoded)
        # Datetime is preserved (not rejected by tag_hook)
        # This documents the current behavior
        assert "ts" in result


@pytest.mark.skipif(not PROTOBUF_AVAILABLE, reason="protobuf not installed")
class TestProtoBufDataTypes:
    """Test all 4 DataType values: UNDEFINED, NULL, JSON, BUFFER (logic-CRIT2)."""

    @pytest.fixture
    def serializer(self) -> ProtoBufSerializer:
        return ProtoBufSerializer()

    def test_params_dict_datatype_json(self, serializer: ProtoBufSerializer) -> None:
        """dict → DATATYPE_JSON roundtrip."""
        payload = {"action": "x", "params": {"a": 1}, "ver": "4", "sender": "n1"}
        data = serializer.serialize(payload, packet_type="REQ")
        result = serializer.deserialize(data, packet_type="REQ")
        assert result["params"] == {"a": 1}

    def test_params_list_datatype_json(self, serializer: ProtoBufSerializer) -> None:
        """list → DATATYPE_JSON roundtrip."""
        payload = {"action": "x", "params": [1, 2, 3], "ver": "4", "sender": "n1"}
        data = serializer.serialize(payload, packet_type="REQ")
        result = serializer.deserialize(data, packet_type="REQ")
        assert result["params"] == [1, 2, 3]

    def test_params_str_datatype_json(self, serializer: ProtoBufSerializer) -> None:
        """str → DATATYPE_JSON roundtrip (logic-HIGH3)."""
        payload = {"action": "x", "params": "hello", "ver": "4", "sender": "n1"}
        data = serializer.serialize(payload, packet_type="REQ")
        result = serializer.deserialize(data, packet_type="REQ")
        assert result["params"] == "hello"

    def test_params_bytes_datatype_buffer(self, serializer: ProtoBufSerializer) -> None:
        """bytes → DATATYPE_BUFFER roundtrip (logic-CRIT2)."""
        payload = {"action": "x", "params": b"raw_bytes", "ver": "4", "sender": "n1"}
        data = serializer.serialize(payload, packet_type="REQ")
        result = serializer.deserialize(data, packet_type="REQ")
        assert result["params"] == b"raw_bytes"
        assert isinstance(result["params"], bytes)

    def test_params_none_datatype_null(self, serializer: ProtoBufSerializer) -> None:
        """None → DATATYPE_NULL roundtrip (logic-HIGH1)."""
        payload = {"action": "x", "params": None, "ver": "4", "sender": "n1"}
        data = serializer.serialize(payload, packet_type="REQ")
        result = serializer.deserialize(data, packet_type="REQ")
        assert result["params"] is None

    def test_params_missing_datatype_undefined(self, serializer: ProtoBufSerializer) -> None:
        """Missing field → DATATYPE_UNDEFINED → removed from result."""
        payload = {"action": "x", "ver": "4", "sender": "n1"}
        data = serializer.serialize(payload, packet_type="REQ")
        result = serializer.deserialize(data, packet_type="REQ")
        assert "params" not in result

    def test_data_bytes_in_event(self, serializer: ProtoBufSerializer) -> None:
        """EVENT packet with data=bytes roundtrip."""
        payload = {"event": "file.uploaded", "data": b"\x00\xff\x42", "ver": "4", "sender": "n1"}
        data = serializer.serialize(payload, packet_type="EVENT")
        result = serializer.deserialize(data, packet_type="EVENT")
        assert result["data"] == b"\x00\xff\x42"


@pytest.mark.skipif(not PROTOBUF_AVAILABLE, reason="protobuf not installed")
class TestProtoBufAllPacketTypes:
    """Ensure all 12 packet types serialize+deserialize correctly."""

    @pytest.fixture
    def serializer(self) -> ProtoBufSerializer:
        return ProtoBufSerializer()

    def test_disconnect_packet(self, serializer: ProtoBufSerializer) -> None:
        """DISCONNECT has same fields as DISCOVER — requires explicit packet_type."""
        payload = {"ver": "4", "sender": "node-1"}
        data = serializer.serialize(payload, packet_type="DISCONNECT")
        result = serializer.deserialize(data, packet_type="DISCONNECT")
        assert result["ver"] == "4"
        assert result["sender"] == "node-1"

    def test_response_with_error(self, serializer: ProtoBufSerializer) -> None:
        """RES with error dict — tests _STRINGIFY_FIELDS error handling."""
        payload = {
            "success": False,
            "ver": "4",
            "sender": "node-1",
            "id": "req-123",
            "error": {"name": "ValidationError", "message": "bad input", "code": 422},
            "data": None,
        }
        data = serializer.serialize(payload, packet_type="RES")
        result = serializer.deserialize(data, packet_type="RES")
        assert result["error"] == {"name": "ValidationError", "message": "bad input", "code": 422}

    def test_info_with_config_and_metadata(self, serializer: ProtoBufSerializer) -> None:
        """INFO with all STRINGIFY fields: services, config, metadata."""
        payload = {
            "services": [{"name": "math"}],
            "config": {"logLevel": "info"},
            "metadata": {"region": "eu-west-1"},
            "ver": "4",
            "sender": "node-1",
            "hostname": "localhost",
        }
        data = serializer.serialize(payload, packet_type="INFO")
        result = serializer.deserialize(data, packet_type="INFO")
        assert result["services"] == [{"name": "math"}]
        assert result["config"] == {"logLevel": "info"}
        assert result["metadata"] == {"region": "eu-west-1"}


@pytest.mark.skipif(not PROTOBUF_AVAILABLE, reason="protobuf not installed")
class TestProtoBufResolveHeuristic:
    """Test _resolve_packet_type heuristic fallback (logic-CRIT1)."""

    @pytest.fixture
    def serializer(self) -> ProtoBufSerializer:
        return ProtoBufSerializer()

    @pytest.mark.parametrize(
        "payload,expected",
        [
            ({"action": "x", "ver": "4"}, "REQ"),
            ({"success": True, "ver": "4"}, "RES"),
            ({"event": "x", "ver": "4"}, "EVENT"),
            ({"services": [], "ver": "4"}, "INFO"),
            ({"arrived": 1, "ver": "4"}, "PONG"),
            ({"time": 1, "ver": "4"}, "PING"),
            ({"host": "x", "port": 3, "ver": "4"}, "GOSSIP_HELLO"),
        ],
    )
    def test_resolve_heuristic(
        self, serializer: ProtoBufSerializer, payload: dict[str, Any], expected: str
    ) -> None:
        resolved = serializer._resolve_packet_type(payload)
        assert resolved == expected

    def test_resolve_disconnect_ambiguous(self, serializer: ProtoBufSerializer) -> None:
        """DISCONNECT cannot be distinguished from DISCOVER — returns DISCOVER."""
        payload = {"ver": "4", "sender": "n1"}
        # Heuristic defaults to DISCOVER for ambiguous minimal packets
        assert serializer._resolve_packet_type(payload) == "DISCOVER"

    def test_serialize_disconnect_requires_explicit_type(
        self, serializer: ProtoBufSerializer
    ) -> None:
        """User must pass packet_type='DISCONNECT' explicitly — heuristic guesses DISCOVER."""
        payload = {"ver": "4", "sender": "n1"}
        # Without packet_type, serialize produces DISCOVER bytes (not DISCONNECT)
        data_as_discover = serializer.serialize(payload)
        data_as_disconnect = serializer.serialize(payload, packet_type="DISCONNECT")
        # Same bytes because schemas are identical at wire level (ver+sender only)
        # But semantically the caller must pass the type
        assert data_as_discover == data_as_disconnect  # proto3 same wire format


@pytest.mark.skipif(not PROTOBUF_AVAILABLE, reason="protobuf not installed")
class TestProtoBufBruteforceDeserialize:
    """Test bruteforce deserialize fallback (arch-HIGH2, security-MED1)."""

    @pytest.fixture
    def serializer(self) -> ProtoBufSerializer:
        return ProtoBufSerializer()

    def test_bruteforce_decodes_something(self, serializer: ProtoBufSerializer) -> None:
        """Deserialize without packet_type falls back to bruteforce.

        WARNING: proto3 has no type tag. Any message can decode against any schema.
        Bruteforce returns the FIRST type that parses with meaningful content —
        this may NOT be the original type. Test documents that bruteforce returns
        something (not raises) for valid proto bytes, not that it returns the right type.
        """
        payload = {"action": "math.add", "params": {"a": 1}, "ver": "4", "sender": "n1"}
        data = serializer.serialize(payload, packet_type="REQ")
        # No packet_type → bruteforce returns something (may be wrong type!)
        result = serializer.deserialize(data)
        # Bruteforce at least preserves ver/sender which are at same field numbers
        assert result.get("ver") == "4"
        assert result.get("sender") == "n1"

    def test_bruteforce_correct_when_explicit_type_passed(
        self, serializer: ProtoBufSerializer
    ) -> None:
        """With explicit packet_type, decoding is reliable."""
        payload = {"action": "math.add", "params": {"a": 1}, "ver": "4", "sender": "n1"}
        data = serializer.serialize(payload, packet_type="REQ")
        result = serializer.deserialize(data, packet_type="REQ")
        assert result["action"] == "math.add"
        assert result["params"] == {"a": 1}

    def test_bruteforce_fails_on_garbage(self, serializer: ProtoBufSerializer) -> None:
        """Random bytes should fail bruteforce gracefully."""
        from moleculerpy.errors import SerializationError

        with pytest.raises(SerializationError):
            serializer.deserialize(b"\xff\xff\xff\xff\xff\xff\xff\xff")


class TestPacketTypeValidation:
    """Test to_packet_type() runtime validation (type-C2)."""

    def test_valid_packet_type(self) -> None:
        from moleculerpy.serializers import to_packet_type

        assert to_packet_type("REQ") == "REQ"
        assert to_packet_type("EVENT") == "EVENT"
        assert to_packet_type("GOSSIP_HELLO") == "GOSSIP_HELLO"

    def test_invalid_packet_type_raises(self) -> None:
        from moleculerpy.serializers import to_packet_type

        with pytest.raises(ValueError, match="Invalid packet type"):
            to_packet_type("INVALID")

    def test_empty_string_raises(self) -> None:
        from moleculerpy.serializers import to_packet_type

        with pytest.raises(ValueError, match="Invalid packet type"):
            to_packet_type("")


class TestSerializerIntegrationWithBroker:
    """Integration tests with real ServiceBroker (test-reviewer MED)."""

    @pytest.mark.asyncio
    @pytest.mark.skipif(not CBOR_AVAILABLE, reason="cbor2 not installed")
    async def test_cbor_broker_local_call(self) -> None:
        """Full broker lifecycle with CBOR serializer."""
        import asyncio as aio

        from moleculerpy.broker import ServiceBroker
        from moleculerpy.decorators import action
        from moleculerpy.service import Service
        from moleculerpy.settings import Settings

        class Svc(Service):
            name = "svc"

            def __init__(self) -> None:
                super().__init__(self.name)

            @action()
            async def echo(self, ctx: Any) -> Any:
                return ctx.params

        b = ServiceBroker(
            id="test-cbor",
            settings=Settings(transporter="memory://", serializer="cbor", log_level="ERROR"),
        )
        await b.register(Svc())
        await b.start()
        try:
            result = await b.call("svc.echo", {"hello": "world", "n": 42})
            assert result == {"hello": "world", "n": 42}
        finally:
            await aio.wait_for(b.stop(), timeout=3)

    @pytest.mark.asyncio
    @pytest.mark.skipif(not PROTOBUF_AVAILABLE, reason="protobuf not installed")
    async def test_protobuf_broker_local_call(self) -> None:
        """Full broker lifecycle with ProtoBuf serializer."""
        import asyncio as aio

        from moleculerpy.broker import ServiceBroker
        from moleculerpy.decorators import action
        from moleculerpy.service import Service
        from moleculerpy.settings import Settings

        class Svc(Service):
            name = "svc"

            def __init__(self) -> None:
                super().__init__(self.name)

            @action()
            async def add(self, ctx: Any) -> Any:
                return ctx.params["a"] + ctx.params["b"]

        b = ServiceBroker(
            id="test-protobuf",
            settings=Settings(transporter="memory://", serializer="protobuf", log_level="ERROR"),
        )
        await b.register(Svc())
        await b.start()
        try:
            result = await b.call("svc.add", {"a": 3, "b": 4})
            assert result == 7
        finally:
            await aio.wait_for(b.stop(), timeout=3)
