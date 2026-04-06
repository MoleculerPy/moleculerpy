"""Pluggable serializer package for MoleculerPy framework.

Provides serializer abstraction with four built-in implementations:
- JsonSerializer (default, stdlib) — wire-compatible with Node.js
- MsgPackSerializer (requires moleculerpy[msgpack]) — compact schema-less binary
- CborSerializer (requires moleculerpy[cbor]) — IoT-friendly, ~29% smaller than JSON
- ProtoBufSerializer (requires moleculerpy[protobuf]) — schema-based, Node.js wire-compatible

Custom serializers can be created by subclassing BaseSerializer.

Usage:
    from moleculerpy.serializers import resolve_serializer, to_packet_type

    serializer = resolve_serializer("json")      # JsonSerializer (default)
    serializer = resolve_serializer("cbor")      # CborSerializer
    serializer = resolve_serializer("protobuf")  # ProtoBufSerializer

    # Schema-less — packet_type ignored
    data = serializer.serialize({"action": "math.add", "params": {"a": 1}})

    # Schema-based (ProtoBuf) — packet_type required for reliable dispatch
    data = serializer.serialize(payload, packet_type=to_packet_type("REQ"))
    payload = serializer.deserialize(data, packet_type=to_packet_type("REQ"))
"""

from __future__ import annotations

from .base import BaseSerializer
from .cbor import CBOR_AVAILABLE, CborSerializer
from .json import JsonSerializer
from .msgpack import MsgPackSerializer
from .protobuf import PROTOBUF_AVAILABLE, ProtoBufSerializer
from .types import PacketType, to_packet_type

_SERIALIZER_REGISTRY: dict[str, type[BaseSerializer]] = {
    "JSON": JsonSerializer,
    "MSGPACK": MsgPackSerializer,
    "CBOR": CborSerializer,
    "PROTOBUF": ProtoBufSerializer,
}


def resolve_serializer(name: str) -> BaseSerializer:
    """Resolve a serializer by name.

    Args:
        name: Serializer name (case-insensitive).
            Supported: "json" (default, stdlib), "msgpack", "cbor", "protobuf".
            Optional serializers require extras: moleculerpy[msgpack|cbor|protobuf].

    Returns:
        Instantiated serializer

    Raises:
        ValueError: If serializer name is not recognized
    """
    key = name.upper()
    cls = _SERIALIZER_REGISTRY.get(key)
    if cls is None:
        raise ValueError(f"Unknown serializer: {name!r}. Available: {list(_SERIALIZER_REGISTRY)}")
    return cls()


__all__ = [
    "BaseSerializer",
    "CborSerializer",
    "JsonSerializer",
    "MsgPackSerializer",
    "PacketType",
    "ProtoBufSerializer",
    "resolve_serializer",
    "to_packet_type",
]
