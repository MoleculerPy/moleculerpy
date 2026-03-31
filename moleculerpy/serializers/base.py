"""Base serializer abstraction for MoleculerPy framework.

Provides the ABC that all serializer implementations must follow.
Includes async variants with automatic thread offloading for large payloads.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any, ClassVar

from ..errors import SerializationError


class BaseSerializer(ABC):
    """Base class for all serializers.

    Subclasses must implement synchronous serialize() and deserialize() methods.
    Async variants are provided with automatic thread offloading for payloads
    exceeding THREAD_OFFLOAD_THRESHOLD.
    """

    THREAD_OFFLOAD_THRESHOLD: ClassVar[int] = 1024 * 1024  # 1MB
    MAX_PAYLOAD_BYTES: ClassVar[int] = 8 * 1024 * 1024  # 8MB default

    @abstractmethod
    def serialize(self, payload: dict[str, Any]) -> bytes:
        """Serialize payload dict to bytes.

        Args:
            payload: Dictionary to serialize

        Returns:
            Serialized bytes representation

        Raises:
            SerializationError: If serialization fails
        """
        ...

    @abstractmethod
    def deserialize(self, data: bytes) -> dict[str, Any]:
        """Deserialize bytes to payload dict.

        Args:
            data: Bytes to deserialize

        Returns:
            Deserialized dictionary

        Raises:
            SerializationError: If deserialization fails
        """
        ...

    async def serialize_async(self, payload: dict[str, Any]) -> bytes:
        """Serialize with synchronous fast path.

        For serialize, we cannot cheaply estimate size before serializing.
        JSON/MsgPack are fast enough to always run synchronously.

        Args:
            payload: Dictionary to serialize

        Returns:
            Serialized bytes representation
        """
        return self.serialize(payload)

    async def deserialize_async(self, data: bytes) -> dict[str, Any]:
        """Deserialize with thread offload for large payloads.

        Payloads larger than THREAD_OFFLOAD_THRESHOLD are deserialized
        in a thread pool to avoid blocking the event loop.

        Args:
            data: Bytes to deserialize

        Returns:
            Deserialized dictionary

        Raises:
            SerializationError: If payload exceeds MAX_PAYLOAD_BYTES
        """
        if len(data) > self.MAX_PAYLOAD_BYTES:
            raise SerializationError(
                f"Payload too large: {len(data)} bytes exceeds {self.MAX_PAYLOAD_BYTES} byte limit"
            )
        if len(data) > self.THREAD_OFFLOAD_THRESHOLD:
            return await asyncio.to_thread(self.deserialize, data)
        return self.deserialize(data)
