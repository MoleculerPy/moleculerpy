"""CBOR serializer implementation for MoleculerPy framework.

Compact binary serialization using CBOR (Concise Binary Object Representation).
Schema-less, like JSON/MsgPack but more compact. Ideal for IoT/bandwidth-constrained.

Requires: pip install moleculerpy[cbor]

Reference: sources/reference-implementations/moleculer/src/serializers/cbor.js
Node.js uses cbor-x with options: useRecords=false, useTag259ForMaps=false

Security: tag_hook rejects all unknown CBOR tags to prevent type confusion
attacks and DoS via expensive tag parsing.

Known limitation: cbor2 hardcodes well-known tags 0/1 (datetime), 2/3 (bigint),
4/5 (decimal) BEFORE tag_hook is invoked. These tags are decoded to their native
Python types regardless of tag_hook. Downstream code must validate value types
after deserialization if operating on untrusted input.
"""

from __future__ import annotations

from typing import Any

from ..errors import SerializationError
from .base import BaseSerializer
from .types import PacketType

try:
    import cbor2 as _cbor2

    CBOR_AVAILABLE = True
except ImportError:
    _cbor2 = None  # type: ignore[assignment,unused-ignore]
    CBOR_AVAILABLE = False


def _reject_tag(decoder: Any, tag: Any, shareable_index: Any = None) -> None:
    """Tag hook that rejects all unknown CBOR tags.

    Called by cbor2 for tags NOT in its hardcoded decoder table (tags 0-5, 30, 37, etc.).
    Returns None — the tagged value becomes None in the output dict.
    """
    return None


class CborSerializer(BaseSerializer):
    """CBOR binary serializer.

    Provides compact binary serialization using the cbor2 library.
    CBOR is ~15-20% smaller than MsgPack for typical Moleculer payloads.
    Schema-less — no .proto files needed. `packet_type` parameter is ignored.

    Wire-compatible with Node.js Moleculer CborSerializer (cbor-x) for
    primitive-only payloads.
    """

    __slots__ = ("_cbor",)

    def __init__(self) -> None:
        """Initialize CborSerializer.

        Raises:
            ImportError: If cbor2 package is not installed.
        """
        if not CBOR_AVAILABLE or _cbor2 is None:
            raise ImportError("cbor2 package required. Install: pip install moleculerpy[cbor]")
        # Store-on-self: mypy narrows the type for hot path, no runtime None checks.
        self._cbor = _cbor2

    def _serialize_impl(self, payload: dict[str, Any], packet_type: PacketType | None) -> bytes:
        """Serialize payload to CBOR bytes (packet_type ignored)."""
        try:
            result: bytes = self._cbor.dumps(payload)
            return result
        except SerializationError:
            raise
        except Exception as e:
            raise SerializationError(f"CBOR serialize failed: {type(e).__name__}") from e

    def _deserialize_impl(self, data: bytes, packet_type: PacketType | None) -> dict[str, Any]:
        """Deserialize CBOR bytes to payload dict (packet_type ignored).

        Uses tag_hook=_reject_tag to reject unknown CBOR tags.
        """
        try:
            result = self._cbor.loads(data, tag_hook=_reject_tag)
            if not isinstance(result, dict):
                raise SerializationError(
                    f"Expected dict from CBOR deserialization, got {type(result).__name__}"
                )
            return result
        except SerializationError:
            raise
        except Exception as e:
            raise SerializationError(f"CBOR deserialize failed: {type(e).__name__}") from e
