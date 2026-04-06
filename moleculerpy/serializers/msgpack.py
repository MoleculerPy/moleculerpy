"""MsgPack serializer implementation for MoleculerPy framework.

Optional serializer using the msgpack library for compact binary serialization.
Requires: pip install moleculerpy[msgpack]
"""

from __future__ import annotations

from typing import Any

from ..errors import SerializationError
from .base import BaseSerializer
from .types import PacketType

try:
    import msgpack as _msgpack

    MSGPACK_AVAILABLE = True
except ImportError:
    _msgpack = None  # type: ignore[assignment,unused-ignore]
    MSGPACK_AVAILABLE = False


class MsgPackSerializer(BaseSerializer):
    """MsgPack binary serializer.

    Provides compact binary serialization using the msgpack library.
    Typically 20-30% smaller and faster than JSON for structured data.
    Schema-less — `packet_type` parameter is ignored.

    Requires the msgpack package to be installed.
    """

    __slots__ = ("_mp",)

    def __init__(self) -> None:
        """Initialize MsgPackSerializer.

        Raises:
            ImportError: If msgpack package is not installed.
        """
        if not MSGPACK_AVAILABLE or _msgpack is None:
            raise ImportError("msgpack package required. Install: pip install moleculerpy[msgpack]")
        # Store-on-self: mypy narrows the type, no runtime None checks in hot path.
        self._mp = _msgpack

    def _serialize_impl(self, payload: dict[str, Any], packet_type: PacketType | None) -> bytes:
        """Serialize payload to MsgPack bytes (packet_type ignored)."""
        try:
            result: bytes = self._mp.packb(payload, use_bin_type=True)
            return result
        except SerializationError:
            raise
        except Exception as e:
            raise SerializationError(f"MsgPack serialize failed: {e}") from e

    def _deserialize_impl(self, data: bytes, packet_type: PacketType | None) -> dict[str, Any]:
        """Deserialize MsgPack bytes to payload dict (packet_type ignored)."""
        try:
            result = self._mp.unpackb(data, raw=False, strict_map_key=True)
            if not isinstance(result, dict):
                raise SerializationError(
                    f"Expected dict from MsgPack deserialization, got {type(result).__name__}"
                )
            return result
        except SerializationError:
            raise
        except Exception as e:
            raise SerializationError(f"MsgPack deserialize failed: {e}") from e
