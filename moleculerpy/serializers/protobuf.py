"""Protocol Buffer serializer implementation for MoleculerPy framework.

Schema-based binary serialization using Google Protocol Buffers.
Each Moleculer packet type has a dedicated proto message definition.
Wire-compatible with Node.js Moleculer ProtoBufSerializer.

Requires: pip install moleculerpy[protobuf]

Reference: sources/reference-implementations/moleculer/src/serializers/protobuf.js
Proto schema: sources/reference-implementations/moleculer/src/serializers/proto/packets.proto
"""

from __future__ import annotations

import json
from typing import Any

from ..errors import SerializationError
from .base import BaseSerializer

try:
    from google.protobuf.json_format import MessageToDict, ParseDict

    PROTOBUF_AVAILABLE = True
except ImportError:
    PROTOBUF_AVAILABLE = False

# Packet type string → proto message class name mapping
# Matches Node.js protobuf.js getPacketFromType()
_PACKET_TYPE_MAP: dict[str, str] = {
    "EVENT": "PacketEvent",
    "REQ": "PacketRequest",
    "RES": "PacketResponse",
    "DISCOVER": "PacketDiscover",
    "INFO": "PacketInfo",
    "DISCONNECT": "PacketDisconnect",
    "HEARTBEAT": "PacketHeartbeat",
    "PING": "PacketPing",
    "PONG": "PacketPong",
    "GOSSIP_HELLO": "PacketGossipHello",
    "GOSSIP_REQ": "PacketGossipRequest",
    "GOSSIP_RES": "PacketGossipResponse",
}

# Fields that Node.js base.js serializeCustomFields() converts to JSON strings
# These are nested objects that protobuf3 cannot represent as dynamic maps,
# so they are JSON.stringify'd before proto-encode.
# Ordered list for type prefix byte encoding
_PACKET_TYPE_ORDER: list[str] = [
    "EVENT",
    "REQ",
    "RES",
    "DISCOVER",
    "INFO",
    "DISCONNECT",
    "HEARTBEAT",
    "PING",
    "PONG",
    "GOSSIP_HELLO",
    "GOSSIP_REQ",
    "GOSSIP_RES",
]

# Proto DataType enum string → int mapping (MessageToDict returns enum as string)
_DATATYPE_MAP: dict[str, int] = {
    "DATATYPE_UNDEFINED": 0,
    "DATATYPE_NULL": 1,
    "DATATYPE_JSON": 2,
    "DATATYPE_BUFFER": 3,
}

_STRINGIFY_FIELDS: dict[str, list[str]] = {
    "PacketInfo": ["services", "config", "metadata"],
    "PacketEvent": ["meta"],
    "PacketRequest": ["meta"],
    "PacketResponse": ["meta", "error"],
    "PacketGossipRequest": ["online", "offline"],
    "PacketGossipResponse": ["online", "offline"],
}


class ProtoBufSerializer(BaseSerializer):
    """Protocol Buffer serializer for Moleculer protocol.

    Uses compiled .proto schema (packets.proto) for typed serialization
    of each Moleculer packet type. Nested objects (services, meta, params)
    are JSON-stringified before proto-encoding, matching Node.js behavior.

    Wire-compatible with Node.js Moleculer ProtoBufSerializer.

    Note: Unlike JSON/MsgPack/CBOR which are schema-less, ProtoBuf requires
    knowing the packet type for serialization. The packet type is determined
    from the 'ver' + payload structure.
    """

    def __init__(self) -> None:
        """Initialize ProtoBufSerializer.

        Raises:
            ImportError: If protobuf package is not installed.
        """
        if not PROTOBUF_AVAILABLE:
            raise ImportError(
                "protobuf package required. Install: pip install moleculerpy[protobuf]"
            )

        # Import compiled proto module
        from .proto import packets_pb2  # noqa: PLC0415

        self._proto = packets_pb2
        self._message_classes: dict[str, type] = {}

        # Build message class lookup
        for packet_type, class_name in _PACKET_TYPE_MAP.items():
            msg_class = getattr(packets_pb2, class_name, None)
            if msg_class is not None:
                self._message_classes[packet_type] = msg_class

    def serialize(self, payload: dict[str, Any], packet_type: str | None = None) -> bytes:
        """Serialize payload to Protocol Buffer bytes.

        Args:
            payload: Dictionary to serialize.
            packet_type: Moleculer packet type (e.g., "REQ", "EVENT", "INFO").
                Required for correct proto message selection. If not provided,
                falls back to heuristic detection from payload fields.

        Returns:
            Raw Protocol Buffer encoded bytes (no prefix, Node.js compatible).

        Raises:
            SerializationError: If packet type cannot be determined or encoding fails.
        """
        try:
            # Use provided packet_type or fall back to heuristic detection
            resolved_type = packet_type or self._resolve_packet_type(payload)
            msg_class = self._message_classes.get(resolved_type)
            if msg_class is None:
                raise SerializationError(f"No proto message for packet type: {resolved_type}")

            # Convert nested objects to JSON strings (matches Node.js base.js)
            class_name = _PACKET_TYPE_MAP.get(resolved_type, "")
            converted = self._serialize_custom_fields(class_name, dict(payload))

            # Filter out fields not in the proto message definition
            proto_msg = msg_class()
            valid_fields = {f.name for f in proto_msg.DESCRIPTOR.fields}
            filtered = {k: v for k, v in converted.items() if k in valid_fields}

            # Raw proto-encode — no prefix byte (Node.js compatible wire format)
            msg = ParseDict(filtered, proto_msg)
            result: bytes = msg.SerializeToString()
            return result

        except Exception as e:
            if isinstance(e, SerializationError):
                raise
            raise SerializationError(f"ProtoBuf serialize failed: {e}") from e

    def deserialize(self, data: bytes, packet_type: str | None = None) -> dict[str, Any]:
        """Deserialize Protocol Buffer bytes to payload dict.

        Args:
            data: Raw Protocol Buffer encoded bytes (no prefix).
            packet_type: Moleculer packet type (e.g., "REQ", "EVENT").
                Required for correct proto message selection. If not provided,
                falls back to trying all known types (less reliable).

        Returns:
            Deserialized dictionary.

        Raises:
            SerializationError: If decoding fails.
        """
        try:
            if not data:
                raise SerializationError("Empty ProtoBuf data")

            # If no type provided, try brute-force (Python-to-Python roundtrip)
            if not packet_type:
                return self._deserialize_bruteforce(data)

            msg_class = self._message_classes.get(packet_type)
            if msg_class is None:
                raise SerializationError(f"No proto message for type: {packet_type}")

            msg = msg_class()
            msg.ParseFromString(data)
            result = MessageToDict(
                msg,
                preserving_proto_field_name=True,
                always_print_fields_with_no_presence=True,
            )

            class_name = _PACKET_TYPE_MAP.get(packet_type, "")
            return self._deserialize_custom_fields(class_name, result)

        except Exception as e:
            if isinstance(e, SerializationError):
                raise
            raise SerializationError(f"ProtoBuf deserialize failed: {e}") from e

    @staticmethod
    def _resolve_packet_type(payload: dict[str, Any]) -> str:
        """Determine Moleculer packet type from payload fields.

        Args:
            payload: The payload dictionary.

        Returns:
            Packet type string (e.g., "REQ", "EVENT", "INFO").
        """
        # Response has "success" field (may or may not have "action")
        if "success" in payload:
            return "RES"
        if "action" in payload:
            return "REQ"
        if "event" in payload:
            return "EVENT"
        if "services" in payload:
            return "INFO"
        if "arrived" in payload:
            return "PONG"
        if "time" in payload and "arrived" not in payload:
            return "PING"
        if "cpu" in payload and "sender" in payload and len(payload) <= 4:  # noqa: PLR2004
            return "HEARTBEAT"
        if "host" in payload and "port" in payload:
            return "GOSSIP_HELLO"
        if "online" in payload or "offline" in payload:
            # Distinguish REQ vs RES by context — both have same fields
            # Default to REQ (more common in gossip cycle)
            return "GOSSIP_REQ"
        # Minimal packets
        if set(payload.keys()) <= {"ver", "sender"}:
            return "DISCOVER"
        return "DISCOVER"

    @staticmethod
    def _serialize_custom_fields(class_name: str, obj: dict[str, Any]) -> dict[str, Any]:
        """Convert nested objects to JSON strings/bytes for proto encoding.

        Matches Node.js base.js serializeCustomFields() + convertDataToTransport().
        - String fields (meta, services, etc.) → JSON.stringify
        - Bytes fields (params, data) → JSON.stringify → encode to bytes
        """
        # String fields: convert dicts to JSON strings
        fields = _STRINGIFY_FIELDS.get(class_name, [])
        for field in fields:
            if field in obj and obj[field] is not None:
                if not isinstance(obj[field], str):
                    obj[field] = json.dumps(obj[field])

        # Bytes fields: convert dicts to JSON-encoded base64 strings
        # (ParseDict expects base64 for proto bytes fields)
        import base64  # noqa: PLC0415

        for bytes_field in ("params", "data"):
            if bytes_field in obj and obj[bytes_field] is not None:
                val = obj[bytes_field]
                if isinstance(val, dict | list):
                    json_bytes = json.dumps(val).encode("utf-8")
                    obj[bytes_field] = base64.b64encode(json_bytes).decode("ascii")
                    obj[f"{bytes_field}Type"] = "DATATYPE_JSON"
                elif isinstance(val, bytes):
                    obj[bytes_field] = base64.b64encode(val).decode("ascii")
                    obj[f"{bytes_field}Type"] = "DATATYPE_BUFFER"
                elif val is None:
                    obj[f"{bytes_field}Type"] = "DATATYPE_NULL"

        return obj

    @staticmethod
    def _deserialize_custom_fields(class_name: str, obj: dict[str, Any]) -> dict[str, Any]:
        """Convert JSON strings/bytes back to nested objects after proto decoding.

        Matches Node.js base.js deserializeCustomFields() + convertDataFromTransport().
        """
        # String fields: parse JSON strings back to dicts
        fields = _STRINGIFY_FIELDS.get(class_name, [])
        for field in fields:
            if field in obj and isinstance(obj[field], str) and obj[field]:
                try:
                    obj[field] = json.loads(obj[field])
                except (json.JSONDecodeError, ValueError):
                    pass

        # Bytes fields: decode JSON bytes back to dicts
        # MessageToDict returns enum as string name, not int
        for bytes_field in ("params", "data"):
            type_field = f"{bytes_field}Type"
            raw_type = obj.pop(type_field, 0)
            data_type = (
                _DATATYPE_MAP.get(raw_type, raw_type) if isinstance(raw_type, str) else raw_type
            )
            if bytes_field in obj:
                val = obj[bytes_field]
                if data_type == 2 and val:  # noqa: PLR2004
                    # DATATYPE_JSON — val may be base64 string from MessageToDict
                    import base64  # noqa: PLC0415

                    if isinstance(val, str):
                        try:
                            val = base64.b64decode(val)
                        except Exception:
                            pass
                    if isinstance(val, bytes | str):
                        obj[bytes_field] = json.loads(val)
                elif data_type == 1:  # DATATYPE_NULL
                    obj[bytes_field] = None
                elif data_type == 0:  # DATATYPE_UNDEFINED
                    obj.pop(bytes_field, None)

        return obj

    def _deserialize_bruteforce(self, data: bytes) -> dict[str, Any]:
        """Try all packet types to decode proto data (fallback when type unknown).

        Used for Python-to-Python roundtrip where packet_type is not available.
        Less reliable than type-directed deserialization.
        """
        for ptype in _PACKET_TYPE_ORDER:
            msg_class = self._message_classes.get(ptype)
            if msg_class is None:
                continue
            try:
                msg = msg_class()
                msg.ParseFromString(data)
                result = MessageToDict(
                    msg,
                    preserving_proto_field_name=True,
                    always_print_fields_with_no_presence=True,
                )
                # Check for meaningful content beyond default values
                if any(v for k, v in result.items() if k not in ("ver", "sender") and v and v != 0):
                    class_name = _PACKET_TYPE_MAP.get(ptype, "")
                    return self._deserialize_custom_fields(class_name, result)
            except Exception:
                continue
        raise SerializationError("Could not decode ProtoBuf data with any known packet type")
