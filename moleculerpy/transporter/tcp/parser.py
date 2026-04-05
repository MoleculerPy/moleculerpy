"""TCP frame parser for the binary wire protocol.

Reads framed messages from an asyncio StreamReader. Each frame has a 6-byte
header followed by payload data:

    [CRC:1][LENGTH:4 BE][TYPE:1][DATA:LENGTH-6]

CRC is XOR of bytes 1-5. LENGTH is total frame size including header.

Reference: sources/reference-implementations/moleculer/src/transporters/tcp/parser.js
"""

from __future__ import annotations

import asyncio
import struct
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from .constants import HEADER_SIZE, resolve_packet_type

if TYPE_CHECKING:
    from ...packet import Topic

# Pre-compiled struct for parsing header bytes 1-5: length(uint32 BE) + type(uint8)
_HEADER_STRUCT = struct.Struct(">IB")


class FrameParser:
    """Stateless TCP frame parser using asyncio StreamReader.

    Unlike Node.js which uses a stateful Writable stream with internal buffer,
    this implementation leverages asyncio StreamReader.readexactly() which
    handles buffering internally — simpler and less error-prone.

    Usage:
        parser = FrameParser(max_packet_size=1_048_576)
        async for topic, data in parser.read_frames(reader):
            process(topic, data)
    """

    __slots__ = ("max_packet_size",)

    def __init__(self, max_packet_size: int = 1_048_576) -> None:
        """Initialize frame parser.

        Args:
            max_packet_size: Maximum allowed frame size in bytes. Frames
                exceeding this limit raise FrameError.
        """
        self.max_packet_size = max_packet_size

    async def read_frames(self, reader: asyncio.StreamReader) -> AsyncIterator[tuple[Topic, bytes]]:
        """Yield (topic, payload) tuples from a TCP stream.

        Reads frames continuously until the stream is closed or an error occurs.
        Handles partial reads via readexactly() which buffers internally.

        Args:
            reader: asyncio StreamReader connected to a TCP socket.

        Yields:
            Tuple of (Topic, payload_bytes) for each complete frame.

        Raises:
            FrameError: On CRC mismatch or oversized packet.
            asyncio.IncompleteReadError: When connection closes mid-frame.
        """
        while True:
            # Read 6-byte header
            header = await reader.readexactly(HEADER_SIZE)

            # Validate CRC: XOR of bytes 1-5 must equal byte 0
            crc = header[1] ^ header[2] ^ header[3] ^ header[4] ^ header[5]
            if crc != header[0]:
                raise FrameError(f"Invalid packet CRC! Expected {crc}, got {header[0]}")

            # Parse length and type from header bytes 1-5
            length, packet_type_id = _HEADER_STRUCT.unpack_from(header, 1)

            # Validate minimum packet length (must include at least the header)
            if length < HEADER_SIZE:
                raise FrameError(f"Invalid packet length: {length} (minimum is {HEADER_SIZE})")

            # Validate maximum packet size
            if self.max_packet_size and length > self.max_packet_size:
                raise FrameError(
                    f"Incoming packet is larger than 'max_packet_size' limit "
                    f"({length} > {self.max_packet_size})!"
                )

            # Read payload (total length minus header)
            payload_size = length - HEADER_SIZE
            if payload_size > 0:
                payload = await reader.readexactly(payload_size)
            else:
                payload = b""

            # Resolve numeric type to Topic enum
            topic = resolve_packet_type(packet_type_id)

            yield topic, payload

    # --- Async iterator protocol ---
    async def __aiter__(self) -> None:
        """Not usable standalone — use read_frames(reader) instead."""
        raise TypeError("Use parser.read_frames(reader) to iterate")


def build_frame(packet_type_id: int, data: bytes) -> bytes:
    """Build a framed TCP packet with header.

    Creates the 6-byte header + data payload ready for socket.write().

    Args:
        packet_type_id: Numeric packet type (1-8).
        data: Serialized payload bytes.

    Returns:
        Complete frame bytes including header.
    """
    total_length = HEADER_SIZE + len(data)

    # Build header: [CRC, LENGTH(4B BE), TYPE]
    header = bytearray(HEADER_SIZE)
    _HEADER_STRUCT.pack_into(header, 1, total_length, packet_type_id)

    # CRC = XOR of bytes 1-5
    header[0] = header[1] ^ header[2] ^ header[3] ^ header[4] ^ header[5]

    return bytes(header) + data


class FrameError(Exception):
    """Error in TCP frame parsing (CRC mismatch, oversized packet)."""
