"""Constants for TCP Transporter wire protocol.

Defines packet type IDs for the binary TCP frame format and mappings
between string topic names and numeric IDs used on the wire.

Reference: sources/reference-implementations/moleculer/src/transporters/tcp/constants.js

Wire format (6-byte header):
    byte[0]   = CRC  (XOR of bytes 1-5)
    bytes[1-4] = total packet length as uint32 big-endian
    byte[5]   = packet type ID (1-8)
    bytes[6..] = serialized payload
"""

from __future__ import annotations

from ...packet import Topic

# --- Packet type numeric IDs (wire protocol) ---
PACKET_EVENT_ID: int = 1
PACKET_REQUEST_ID: int = 2
PACKET_RESPONSE_ID: int = 3
PACKET_PING_ID: int = 4
PACKET_PONG_ID: int = 5
PACKET_GOSSIP_REQ_ID: int = 6
PACKET_GOSSIP_RES_ID: int = 7
PACKET_GOSSIP_HELLO_ID: int = 8

# Header size in bytes
HEADER_SIZE: int = 6

# --- Mapping: Topic → wire ID ---
_TOPIC_TO_ID: dict[Topic, int] = {
    Topic.EVENT: PACKET_EVENT_ID,
    Topic.REQUEST: PACKET_REQUEST_ID,
    Topic.RESPONSE: PACKET_RESPONSE_ID,
    Topic.PING: PACKET_PING_ID,
    Topic.PONG: PACKET_PONG_ID,
    Topic.GOSSIP_REQ: PACKET_GOSSIP_REQ_ID,
    Topic.GOSSIP_RES: PACKET_GOSSIP_RES_ID,
    Topic.GOSSIP_HELLO: PACKET_GOSSIP_HELLO_ID,
}

# --- Mapping: wire ID → Topic ---
_ID_TO_TOPIC: dict[int, Topic] = {v: k for k, v in _TOPIC_TO_ID.items()}

# TCP errors that should not be logged as warnings (expected during disconnect)
IGNORABLE_ERRORS: frozenset[str] = frozenset(
    {
        "ECONNREFUSED",
        "ECONNRESET",
        "ETIMEDOUT",
        "EHOSTUNREACH",
        "ENETUNREACH",
        "ENETDOWN",
        "EPIPE",
        "ENOENT",
        "ConnectionResetError",
        "ConnectionRefusedError",
        "TimeoutError",
        "BrokenPipeError",
        "OSError",
    }
)

# Default TCP transporter options
DEFAULT_OPTIONS: dict[str, object] = {
    # UDP discovery
    "udp_discovery": True,
    "udp_port": 4445,
    "udp_bind_address": None,
    "udp_period": 30,
    "udp_reuse_addr": True,
    "udp_max_discovery": 0,  # 0 = no limit
    # Multicast
    "udp_multicast": "239.0.0.0",
    "udp_multicast_ttl": 1,
    # Broadcast
    "udp_broadcast": False,
    # TCP server
    "max_incoming": 256,  # Max concurrent incoming TCP connections
    # TCP
    "port": None,  # None = random port
    "urls": None,  # Static node addresses (when UDP disabled)
    "use_hostname": True,
    # Gossip
    "gossip_period": 2,  # seconds
    "max_connections": 32,  # Max live outgoing TCP connections
    "max_packet_size": 1 * 1024 * 1024,  # 1 MB
}


def resolve_packet_id(topic: Topic) -> int:
    """Resolve a Topic enum to its wire protocol numeric ID.

    Args:
        topic: The packet topic type.

    Returns:
        Numeric packet ID for the TCP wire protocol.

    Raises:
        ValueError: If the topic type is not supported by TCP transport.
    """
    pid = _TOPIC_TO_ID.get(topic)
    if pid is None:
        raise ValueError(f"Unsupported packet type for TCP: {topic}")
    return pid


def resolve_packet_type(packet_id: int) -> Topic:
    """Resolve a wire protocol numeric ID to its Topic enum.

    Args:
        packet_id: Numeric ID from the TCP frame header.

    Returns:
        Corresponding Topic enum value.

    Raises:
        ValueError: If the packet ID is not recognized.
    """
    topic = _ID_TO_TOPIC.get(packet_id)
    if topic is None:
        raise ValueError(f"Unsupported packet ID: {packet_id}")
    return topic
