"""JSON serializer implementation for MoleculerPy framework.

Default serializer using Python's built-in json module.
Compatible with Moleculer.js JSON serialization format.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from ..errors import SerializationError
from .base import BaseSerializer
from .types import PacketType

logger = logging.getLogger(__name__)


class JsonSerializer(BaseSerializer):
    """JSON serializer using Python's built-in json module.

    This is the default serializer, producing UTF-8 encoded JSON bytes.
    Compatible with Moleculer.js default serializer. Schema-less —
    `packet_type` parameter is ignored.
    """

    def _serialize_impl(self, payload: dict[str, Any], packet_type: PacketType | None) -> bytes:
        """Serialize payload to JSON bytes (packet_type ignored for JSON)."""
        try:
            return json.dumps(payload).encode("utf-8")
        except (TypeError, ValueError) as e:
            raise SerializationError(f"JSON serialize failed: {e}") from e

    def _deserialize_impl(self, data: bytes, packet_type: PacketType | None) -> dict[str, Any]:
        """Deserialize JSON bytes to payload dict (packet_type ignored)."""
        try:
            result = json.loads(data.decode("utf-8"))
        except RecursionError as e:
            # Deeply nested JSON → potential DoS. Log for observability.
            logger.warning(
                "JSON deserialize hit recursion limit (possible DoS): %d bytes", len(data)
            )
            raise SerializationError(f"JSON deserialize failed: {type(e).__name__}") from e
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise SerializationError(f"JSON deserialize failed: {e}") from e
        if not isinstance(result, dict):
            raise SerializationError(
                f"Expected dict from JSON deserialization, got {type(result).__name__}"
            )
        return result
