"""CBOR serializer implementation for MoleculerPy framework.

Compact binary serialization using CBOR (Concise Binary Object Representation).
Schema-less, like JSON/MsgPack but more compact. Ideal for IoT/bandwidth-constrained.

Requires: pip install moleculerpy[cbor]

Reference: sources/reference-implementations/moleculer/src/serializers/cbor.js
Node.js uses cbor-x with options: useRecords=false, useTag259ForMaps=false
"""

from __future__ import annotations

from typing import Any

from ..errors import SerializationError
from .base import BaseSerializer

try:
    import cbor2

    CBOR_AVAILABLE = True
except ImportError:
    cbor2 = None  # type: ignore[assignment]
    CBOR_AVAILABLE = False


class CborSerializer(BaseSerializer):
    """CBOR binary serializer.

    Provides compact binary serialization using the cbor2 library.
    CBOR is ~15-20% smaller than MsgPack for typical Moleculer payloads.
    Schema-less — no .proto files needed.

    Wire-compatible with Node.js Moleculer CborSerializer (cbor-x).
    """

    def __init__(self) -> None:
        """Initialize CborSerializer.

        Raises:
            ImportError: If cbor2 package is not installed.
        """
        if not CBOR_AVAILABLE:
            raise ImportError("cbor2 package required. Install: pip install moleculerpy[cbor]")

    def serialize(self, payload: dict[str, Any], packet_type: str | None = None) -> bytes:
        """Serialize payload to CBOR bytes.

        Args:
            payload: Dictionary to serialize.

        Returns:
            CBOR encoded bytes.

        Raises:
            SerializationError: If payload contains non-serializable types.
        """
        assert cbor2 is not None
        try:
            return cbor2.dumps(payload)
        except Exception as e:
            if isinstance(e, SerializationError):
                raise
            raise SerializationError(f"CBOR serialize failed: {e}") from e

    def deserialize(self, data: bytes, packet_type: str | None = None) -> dict[str, Any]:
        """Deserialize CBOR bytes to payload dict.

        Args:
            data: CBOR encoded bytes.

        Returns:
            Deserialized dictionary.

        Raises:
            SerializationError: If data is not valid CBOR or not a dict.
        """
        assert cbor2 is not None
        try:
            result = cbor2.loads(data)
            if not isinstance(result, dict):
                raise SerializationError(
                    f"Expected dict from CBOR deserialization, got {type(result).__name__}"
                )
            return result
        except Exception as e:
            if isinstance(e, SerializationError):
                raise
            raise SerializationError(f"CBOR deserialize failed: {e}") from e
