"""MsgPack serializer implementation for MoleculerPy framework.

Optional serializer using the msgpack library for compact binary serialization.
Requires: pip install moleculerpy[msgpack]
"""

from __future__ import annotations

from typing import Any

from ..errors import SerializationError
from .base import BaseSerializer

try:
    import msgpack

    MSGPACK_AVAILABLE = True
except ImportError:
    msgpack = None
    MSGPACK_AVAILABLE = False


class MsgPackSerializer(BaseSerializer):
    """MsgPack binary serializer.

    Provides compact binary serialization using the msgpack library.
    Typically 20-30% smaller and faster than JSON for structured data.

    Requires the msgpack package to be installed.
    """

    def __init__(self) -> None:
        """Initialize MsgPackSerializer.

        Raises:
            ImportError: If msgpack package is not installed
        """
        if not MSGPACK_AVAILABLE:
            raise ImportError("msgpack package required. Install: pip install moleculerpy[msgpack]")

    def serialize(self, payload: dict[str, Any]) -> bytes:
        """Serialize payload to MsgPack bytes.

        Args:
            payload: Dictionary to serialize

        Returns:
            MsgPack encoded bytes

        Raises:
            SerializationError: If payload contains non-serializable types
        """
        assert msgpack is not None
        try:
            result: bytes = msgpack.packb(payload, use_bin_type=True)
            return result
        except Exception as e:
            if isinstance(e, SerializationError):
                raise
            raise SerializationError(f"MsgPack serialize failed: {e}") from e

    def deserialize(self, data: bytes) -> dict[str, Any]:
        """Deserialize MsgPack bytes to payload dict.

        Args:
            data: MsgPack encoded bytes

        Returns:
            Deserialized dictionary

        Raises:
            SerializationError: If data is not valid MsgPack or not a dict
        """
        assert msgpack is not None
        try:
            result = msgpack.unpackb(data, raw=False, strict_map_key=True)
            if not isinstance(result, dict):
                raise SerializationError(
                    f"Expected dict from MsgPack deserialization, got {type(result).__name__}"
                )
            return result
        except Exception as e:
            if isinstance(e, SerializationError):
                raise
            raise SerializationError(f"MsgPack deserialize failed: {e}") from e
