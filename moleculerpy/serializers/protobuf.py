"""Protocol Buffer serializer implementation for MoleculerPy framework.

Schema-based binary serialization using Google Protocol Buffers.
Each Moleculer packet type has a dedicated proto message definition.
Wire-compatible with Node.js Moleculer ProtoBufSerializer.

Requires: pip install moleculerpy[protobuf]

Reference: sources/reference-implementations/moleculer/src/serializers/protobuf.js
Proto schema: sources/reference-implementations/moleculer/src/serializers/proto/packets.proto

Security:
- MAX_PAYLOAD_BYTES enforced by BaseSerializer template method
- Nested JSON fields (meta/services/params) parsed with depth limit protection
- Silent failures replaced with logger.warning for observability
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
from typing import Any, Final

from ..errors import SerializationError
from .base import BaseSerializer
from .types import PacketType

try:
    from google.protobuf.json_format import MessageToDict, ParseDict

    PROTOBUF_AVAILABLE = True
except ImportError:
    MessageToDict = None  # type: ignore[assignment,unused-ignore]
    ParseDict = None  # type: ignore[assignment,unused-ignore]
    PROTOBUF_AVAILABLE = False

logger = logging.getLogger(__name__)

# Max nesting depth for JSON decode of stringified nested fields.
# Prevents stack exhaustion via deeply nested attacker-controlled payload.
# Enforced explicitly via _check_json_depth() since json.loads has no depth param.
MAX_JSON_DEPTH: Final[int] = 32

# Max size for individual JSON-stringified fields inside proto message.
# Separate from BaseSerializer.MAX_PAYLOAD_BYTES which guards the whole frame.
MAX_NESTED_FIELD_BYTES: Final[int] = 1 * 1024 * 1024  # 1MB per nested field

# Heuristic constant: HEARTBEAT packet has small field count (ver, sender, cpu, +1 optional)
_HEARTBEAT_MAX_FIELDS: Final[int] = 4


def _check_json_depth(text: str, max_depth: int = MAX_JSON_DEPTH) -> bool:
    """Fast pre-scan: reject JSON strings exceeding max nesting depth.

    Counts {/[/( nesting without full parsing. O(n) single pass.
    Escaped braces inside strings are skipped via naive state tracking.

    Args:
        text: JSON string to check.
        max_depth: Maximum allowed nesting depth.

    Returns:
        True if JSON depth is within limit, False otherwise.
    """
    depth = 0
    max_seen = 0
    in_string = False
    escape_next = False

    for char in text:
        if escape_next:
            escape_next = False
            continue
        if char == "\\" and in_string:
            escape_next = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if char in "{[":
            depth += 1
            if depth > max_seen:
                max_seen = depth
                if max_seen > max_depth:
                    return False
        elif char in "}]":
            depth -= 1

    return True


# Packet type string → proto message class name mapping
# Matches Node.js protobuf.js getPacketFromType()
_PACKET_TYPE_MAP: Final[dict[str, str]] = {
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


# Proto DataType enum values (wire format)
_DATATYPE_UNDEFINED: Final[int] = 0
_DATATYPE_NULL: Final[int] = 1
_DATATYPE_JSON: Final[int] = 2
_DATATYPE_BUFFER: Final[int] = 3

# MessageToDict returns proto enum as STRING name, not int.
_DATATYPE_NAME_TO_INT: Final[dict[str, int]] = {
    "DATATYPE_UNDEFINED": _DATATYPE_UNDEFINED,
    "DATATYPE_NULL": _DATATYPE_NULL,
    "DATATYPE_JSON": _DATATYPE_JSON,
    "DATATYPE_BUFFER": _DATATYPE_BUFFER,
}

# Proto enum INT → NAME mapping for serialize path (ParseDict expects the name).
_DATATYPE_INT_TO_NAME: Final[dict[int, str]] = {v: k for k, v in _DATATYPE_NAME_TO_INT.items()}


# Fields that Node.js base.js serializeCustomFields() converts to JSON strings.
# Nested objects that protobuf3 cannot represent as dynamic maps are JSON.stringify'd.
_STRINGIFY_FIELDS: Final[dict[str, list[str]]] = {
    "PacketInfo": ["services", "config", "metadata"],
    "PacketEvent": ["meta"],
    "PacketRequest": ["meta"],
    "PacketResponse": ["meta", "error"],
    "PacketGossipRequest": ["online", "offline"],
    "PacketGossipResponse": ["online", "offline"],
}


def _safe_json_loads(value: str | bytes, field_name: str) -> Any:
    """Parse JSON with depth protection and error logging.

    Matches Node.js base.js deserializeCustomFields behavior but adds:
    - Size limit per field (MAX_NESTED_FIELD_BYTES)
    - Depth limit via sys.setrecursionlimit-aware parsing
    - logger.warning on parse failure (was silent `pass`)

    Args:
        value: Raw string or bytes containing JSON.
        field_name: Field name for error logging.

    Returns:
        Parsed value, or the original string if parsing fails.
    """
    if isinstance(value, bytes):
        if len(value) > MAX_NESTED_FIELD_BYTES:
            logger.warning(
                "ProtoBuf nested field '%s' exceeds %d bytes, skipping parse",
                field_name,
                MAX_NESTED_FIELD_BYTES,
            )
            return value
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError as e:
            logger.warning(
                "ProtoBuf nested field '%s' UTF-8 decode failed: %s",
                field_name,
                type(e).__name__,
            )
            return value

    if len(value) > MAX_NESTED_FIELD_BYTES:
        logger.warning(
            "ProtoBuf nested field '%s' exceeds %d bytes, skipping parse",
            field_name,
            MAX_NESTED_FIELD_BYTES,
        )
        return value

    # Explicit depth check BEFORE json.loads — protects against stack exhaustion
    # attacks. json.loads uses C recursion which can crash the interpreter
    # (not just raise RecursionError) on pathological inputs.
    if not _check_json_depth(value):
        logger.warning(
            "ProtoBuf nested field '%s' exceeds max JSON depth %d (possible DoS)",
            field_name,
            MAX_JSON_DEPTH,
        )
        return value

    try:
        return json.loads(value)
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning(
            "ProtoBuf nested field '%s' JSON parse failed: %s",
            field_name,
            type(e).__name__,
        )
        return value
    except RecursionError:
        logger.warning(
            "ProtoBuf nested field '%s' exceeds max JSON depth (possible DoS)",
            field_name,
        )
        return value


class ProtoBufSerializer(BaseSerializer):
    """Protocol Buffer serializer for Moleculer protocol.

    Uses compiled .proto schema (packets.proto) for typed serialization
    of each Moleculer packet type. Nested objects (services, meta, params)
    are JSON-stringified before proto-encoding, matching Node.js behavior.

    Wire-compatible with Node.js Moleculer ProtoBufSerializer.

    IMPORTANT: packet_type parameter is required for correct operation.
    Without it, serialize falls back to a heuristic resolver that CANNOT
    distinguish DISCONNECT from DISCOVER (both have only ver+sender).
    The transit layer should always pass packet_type explicitly.
    """

    def __init__(self) -> None:
        """Initialize ProtoBufSerializer.

        Raises:
            ImportError: If protobuf package is not installed.
        """
        if not PROTOBUF_AVAILABLE or ParseDict is None or MessageToDict is None:
            raise ImportError(
                "protobuf package required. Install: pip install moleculerpy[protobuf]"
            )

        # Store-on-self: mypy narrows these to non-None for the whole lifetime
        # of the instance. Eliminates runtime None checks in hot path.
        self._parse_dict = ParseDict
        self._message_to_dict = MessageToDict

        # Import compiled proto module
        from .proto import packets_pb2  # noqa: PLC0415

        self._message_classes: dict[str, type] = {}

        # Build message class lookup
        for ptype, class_name in _PACKET_TYPE_MAP.items():
            msg_class = getattr(packets_pb2, class_name, None)
            if msg_class is not None:
                self._message_classes[ptype] = msg_class

    def _serialize_impl(self, payload: dict[str, Any], packet_type: PacketType | None) -> bytes:
        """Serialize payload to Protocol Buffer bytes.

        Args:
            payload: Dictionary to serialize.
            packet_type: Moleculer packet type. REQUIRED for correct dispatch.
                If None, falls back to heuristic (unreliable for DISCONNECT).

        Returns:
            Raw Protocol Buffer encoded bytes (Node.js compatible wire format).

        Raises:
            SerializationError: If packet type cannot be determined or encoding fails.
        """
        try:
            resolved_type = packet_type or self._resolve_packet_type(payload)
            if resolved_type is None:
                raise SerializationError(
                    "ProtoBuf serialize requires packet_type — cannot infer from payload"
                )

            msg_class = self._message_classes.get(resolved_type)
            if msg_class is None:
                raise SerializationError(f"No proto message for packet type: {resolved_type}")

            # Convert nested objects to JSON strings (matches Node.js base.js)
            class_name = _PACKET_TYPE_MAP.get(resolved_type, "")
            converted = self._serialize_custom_fields(class_name, dict(payload))

            # Filter to only valid proto fields (transit may add 'id', etc.)
            proto_msg = msg_class()
            valid_fields = {f.name for f in proto_msg.DESCRIPTOR.fields}
            filtered = {k: v for k, v in converted.items() if k in valid_fields}

            # Log if fields were dropped (observability, not security)
            dropped = set(converted.keys()) - valid_fields
            if dropped:
                logger.debug(
                    "ProtoBuf dropped fields not in %s: %s",
                    class_name,
                    dropped,
                )

            # Raw proto-encode — no prefix byte (Node.js compatible)
            msg = self._parse_dict(filtered, proto_msg)
            result: bytes = msg.SerializeToString()
            return result

        except SerializationError:
            raise
        except Exception as e:
            # Don't leak payload content in error messages (security)
            raise SerializationError(f"ProtoBuf serialize failed: {type(e).__name__}") from e

    def _deserialize_impl(self, data: bytes, packet_type: PacketType | None) -> dict[str, Any]:
        """Deserialize Protocol Buffer bytes to payload dict.

        Args:
            data: Raw Protocol Buffer encoded bytes.
            packet_type: Moleculer packet type. REQUIRED for reliable decoding.
                If None, falls back to bruteforce (less reliable).

        Returns:
            Deserialized dictionary.

        Raises:
            SerializationError: On decoding failure.
        """
        try:
            if not data:
                raise SerializationError("Empty ProtoBuf data")

            # If no type provided, try brute-force (Python-to-Python roundtrip only)
            if not packet_type:
                logger.debug("ProtoBuf deserialize without packet_type — using bruteforce fallback")
                return self._deserialize_bruteforce(data)

            msg_class = self._message_classes.get(packet_type)
            if msg_class is None:
                raise SerializationError(f"No proto message for type: {packet_type}")

            msg = msg_class()
            msg.ParseFromString(data)
            result = self._message_to_dict(
                msg,
                preserving_proto_field_name=True,
                always_print_fields_with_no_presence=True,
            )

            class_name = _PACKET_TYPE_MAP.get(packet_type, "")
            return self._deserialize_custom_fields(class_name, result)

        except SerializationError:
            raise
        except Exception as e:
            raise SerializationError(f"ProtoBuf deserialize failed: {type(e).__name__}") from e

    @staticmethod
    def _resolve_packet_type(payload: dict[str, Any]) -> str | None:
        """Heuristic packet type detection from payload fields.

        WARNING: This is a fallback for Python-to-Python roundtrips only.
        It CANNOT reliably distinguish:
        - DISCONNECT from DISCOVER (both have only ver+sender)
        - GOSSIP_REQ from GOSSIP_RES (both have ver+sender+online+offline)

        The transit layer should always pass packet_type explicitly.

        Returns:
            Packet type string, or None if cannot determine.
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
        if "time" in payload:
            return "PING"
        # HEARTBEAT has cpu + small field count
        if "cpu" in payload and "sender" in payload and len(payload) <= _HEARTBEAT_MAX_FIELDS:
            return "HEARTBEAT"
        if "host" in payload and "port" in payload:
            return "GOSSIP_HELLO"
        if "online" in payload or "offline" in payload:
            # Cannot distinguish REQ vs RES from content — default to REQ
            return "GOSSIP_REQ"
        # Minimal packets: cannot distinguish DISCONNECT from DISCOVER
        if set(payload.keys()) <= {"ver", "sender"}:
            return "DISCOVER"
        return None

    @staticmethod
    def _serialize_custom_fields(class_name: str, obj: dict[str, Any]) -> dict[str, Any]:
        """Convert nested objects to JSON strings/bytes for proto encoding.

        Matches Node.js base.js serializeCustomFields() + convertDataToTransport().
        - String fields (meta, services, etc.) → JSON.stringify
        - Bytes fields (params, data) → based on type:
          - dict/list: JSON.stringify → base64(utf-8)  → DATATYPE_JSON
          - str: JSON.stringify → base64(utf-8)         → DATATYPE_JSON
          - bytes: base64(raw)                          → DATATYPE_BUFFER
          - None: (skip field)                          → DATATYPE_NULL
          - missing: (skip field)                       → DATATYPE_UNDEFINED
        """
        # String fields: convert dicts to JSON strings
        fields = _STRINGIFY_FIELDS.get(class_name, [])
        for field in fields:
            if field in obj and obj[field] is not None:
                if not isinstance(obj[field], str):
                    obj[field] = json.dumps(obj[field])

        # Bytes fields: convert based on Python type
        # (ParseDict expects base64-encoded string for proto bytes fields)
        for bytes_field in ("params", "data"):
            type_field = f"{bytes_field}Type"

            if bytes_field not in obj:
                # Field missing → DATATYPE_UNDEFINED
                obj[type_field] = _DATATYPE_INT_TO_NAME[_DATATYPE_UNDEFINED]
                continue

            val = obj[bytes_field]

            if val is None:
                obj[type_field] = _DATATYPE_INT_TO_NAME[_DATATYPE_NULL]
                # ParseDict requires the bytes field to be a valid base64 string
                # even when type is NULL. Use empty string.
                obj[bytes_field] = ""
            elif isinstance(val, dict | list):
                json_bytes = json.dumps(val).encode("utf-8")
                obj[bytes_field] = base64.b64encode(json_bytes).decode("ascii")
                obj[type_field] = _DATATYPE_INT_TO_NAME[_DATATYPE_JSON]
            elif isinstance(val, str):
                # String values: JSON-stringify first (matches Node.js)
                json_bytes = json.dumps(val).encode("utf-8")
                obj[bytes_field] = base64.b64encode(json_bytes).decode("ascii")
                obj[type_field] = _DATATYPE_INT_TO_NAME[_DATATYPE_JSON]
            elif isinstance(val, bytes):
                obj[bytes_field] = base64.b64encode(val).decode("ascii")
                obj[type_field] = _DATATYPE_INT_TO_NAME[_DATATYPE_BUFFER]
            else:
                # Scalar (int, float, bool) — treat as JSON
                json_bytes = json.dumps(val).encode("utf-8")
                obj[bytes_field] = base64.b64encode(json_bytes).decode("ascii")
                obj[type_field] = _DATATYPE_INT_TO_NAME[_DATATYPE_JSON]

        return obj

    @staticmethod
    def _deserialize_custom_fields(class_name: str, obj: dict[str, Any]) -> dict[str, Any]:
        """Convert JSON strings/bytes back to nested objects after proto decoding.

        Matches Node.js base.js deserializeCustomFields() + convertDataFromTransport().
        Handles all 4 DataType values: UNDEFINED, NULL, JSON, BUFFER.
        """
        # String fields: parse JSON strings back to dicts (with depth protection)
        fields = _STRINGIFY_FIELDS.get(class_name, [])
        for field in fields:
            if field in obj and isinstance(obj[field], str) and obj[field]:
                parsed = _safe_json_loads(obj[field], field)
                if not isinstance(parsed, str):  # parse succeeded
                    obj[field] = parsed

        # Bytes fields: decode based on DataType
        for bytes_field in ("params", "data"):
            type_field = f"{bytes_field}Type"
            raw_type = obj.pop(type_field, _DATATYPE_UNDEFINED)

            # MessageToDict returns enum as STRING name (e.g., "DATATYPE_JSON")
            if isinstance(raw_type, str):
                data_type = _DATATYPE_NAME_TO_INT.get(raw_type, _DATATYPE_UNDEFINED)
            else:
                try:
                    data_type = int(raw_type)
                except (TypeError, ValueError):
                    data_type = _DATATYPE_UNDEFINED

            if bytes_field not in obj:
                continue

            val = obj[bytes_field]

            if data_type == _DATATYPE_UNDEFINED:
                # Field was not set on sender side — remove from result
                obj.pop(bytes_field, None)
            elif data_type == _DATATYPE_NULL:
                obj[bytes_field] = None
            elif data_type == _DATATYPE_JSON and val:
                # MessageToDict returns bytes fields as base64-encoded strings
                if isinstance(val, str):
                    try:
                        decoded = base64.b64decode(val)
                    except (binascii.Error, ValueError) as e:
                        logger.warning(
                            "ProtoBuf %s base64 decode failed: %s",
                            bytes_field,
                            type(e).__name__,
                        )
                        obj.pop(bytes_field, None)
                        continue
                    obj[bytes_field] = _safe_json_loads(decoded, bytes_field)
                elif isinstance(val, bytes):
                    obj[bytes_field] = _safe_json_loads(val, bytes_field)
            elif data_type == _DATATYPE_BUFFER and val:
                # Keep as bytes (was stored as base64-encoded string by proto)
                if isinstance(val, str):
                    try:
                        obj[bytes_field] = base64.b64decode(val)
                    except (binascii.Error, ValueError) as e:
                        logger.warning(
                            "ProtoBuf %s (BUFFER) base64 decode failed: %s",
                            bytes_field,
                            type(e).__name__,
                        )
                        obj.pop(bytes_field, None)
                elif isinstance(val, bytes):
                    # Already bytes — nothing to do
                    pass

        return obj

    def _deserialize_bruteforce(self, data: bytes) -> dict[str, Any]:
        """Try all packet types to decode proto data (fallback when type unknown).

        Used for Python-to-Python roundtrip where packet_type is not available.
        UNSAFE — proto3 has no type tag, so any message can decode against any schema.
        Use only when the caller fully controls both endpoints.

        Warning: This is a CPU amplification vector for untrusted input — it tries
        up to 12 proto message classes. For untrusted data, always provide packet_type.
        """
        for ptype in _PACKET_TYPE_MAP:
            msg_class = self._message_classes.get(ptype)
            if msg_class is None:
                continue
            try:
                msg = msg_class()
                msg.ParseFromString(data)
                result = self._message_to_dict(
                    msg,
                    preserving_proto_field_name=True,
                    always_print_fields_with_no_presence=True,
                )
                # Check for meaningful content beyond default values
                has_content = any(
                    v for k, v in result.items() if k not in ("ver", "sender") and v and v != 0
                )
                # Also accept if both ver AND sender are populated (e.g., HEARTBEAT with cpu=0)
                has_metadata = bool(result.get("ver") and result.get("sender"))
                if has_content or has_metadata:
                    class_name = _PACKET_TYPE_MAP.get(ptype, "")
                    return self._deserialize_custom_fields(class_name, result)
            except Exception as e:
                logger.debug("ProtoBuf bruteforce: %s failed with %s", ptype, type(e).__name__)
                continue
        raise SerializationError("Could not decode ProtoBuf data with any known packet type")
