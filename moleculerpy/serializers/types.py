"""Shared types for serializer package.

Provides exhaustive Literal type for Moleculer packet types, enabling
type-safe dispatch in schema-based serializers (ProtoBuf).
"""

from __future__ import annotations

from typing import Literal, cast, get_args

# Exhaustive enumeration of Moleculer packet types.
# Schema-based serializers (ProtoBuf) dispatch on this value.
# Schema-less serializers (JSON, MsgPack, CBOR) ignore it.
PacketType = Literal[
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


# Runtime validation set derived from the Literal type — single source of truth.
# Adding a new packet type only requires updating PacketType above; this set
# updates automatically via get_args().
_VALID_PACKET_TYPES: frozenset[str] = frozenset(get_args(PacketType))


def to_packet_type(value: str) -> PacketType:
    """Convert a string to PacketType with runtime validation.

    Use this at boundaries where you have a str (e.g. from Topic enum, config)
    and need to pass it to serializer APIs that require PacketType.

    Args:
        value: String value (e.g. "REQ", "EVENT").

    Returns:
        The same string, typed as PacketType.

    Raises:
        ValueError: If value is not a valid Moleculer packet type.
    """
    if value not in _VALID_PACKET_TYPES:
        raise ValueError(f"Invalid packet type: {value!r}. Valid: {sorted(_VALID_PACKET_TYPES)}")
    return cast(PacketType, value)
