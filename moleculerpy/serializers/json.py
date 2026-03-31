"""JSON serializer implementation for MoleculerPy framework.

Default serializer using Python's built-in json module.
Compatible with Moleculer.js JSON serialization format.
"""

from __future__ import annotations

import json
from typing import Any

from ..errors import SerializationError
from .base import BaseSerializer


class JsonSerializer(BaseSerializer):
    """JSON serializer using Python's built-in json module.

    This is the default serializer, producing UTF-8 encoded JSON bytes.
    Compatible with Moleculer.js default serializer.
    """

    def serialize(self, payload: dict[str, Any]) -> bytes:
        """Serialize payload to JSON bytes.

        Args:
            payload: Dictionary to serialize

        Returns:
            UTF-8 encoded JSON bytes

        Raises:
            SerializationError: If payload contains non-serializable types
        """
        try:
            return json.dumps(payload).encode("utf-8")
        except (TypeError, ValueError) as e:
            raise SerializationError(f"JSON serialize failed: {e}") from e

    def deserialize(self, data: bytes) -> dict[str, Any]:
        """Deserialize JSON bytes to payload dict.

        Args:
            data: UTF-8 encoded JSON bytes

        Returns:
            Deserialized dictionary

        Raises:
            SerializationError: If data is not valid JSON or not a dict
        """
        try:
            result = json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError, RecursionError) as e:
            raise SerializationError(f"JSON deserialize failed: {e}") from e
        if not isinstance(result, dict):
            raise SerializationError(
                f"Expected dict from JSON deserialization, got {type(result).__name__}"
            )
        return result
