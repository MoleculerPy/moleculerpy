"""Base serializer abstraction for MoleculerPy framework.

Provides the ABC that all serializer implementations must follow.
Uses template method pattern: public serialize/deserialize enforce limits
and delegate to _serialize_impl/_deserialize_impl that subclasses implement.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any, ClassVar

from ..errors import SerializationError
from .types import PacketType


class BaseSerializer(ABC):
    """Base class for all serializers.

    Subclasses implement `_serialize_impl` and `_deserialize_impl` (the codec).
    Public `serialize`/`deserialize` enforce payload size limits (MAX_PAYLOAD_BYTES)
    BEFORE delegating to the subclass implementation. This ensures the limit is
    enforced in both sync and async paths.

    Async variants:
    - `deserialize_async`: offloads to thread pool for payloads above
      THREAD_OFFLOAD_THRESHOLD (prevents blocking the event loop).
    - `serialize_async`: currently runs synchronously — serialize is typically
      fast enough that thread dispatch overhead exceeds the benefit.
    """

    THREAD_OFFLOAD_THRESHOLD: ClassVar[int] = 1024 * 1024  # 1MB
    MAX_PAYLOAD_BYTES: ClassVar[int] = 8 * 1024 * 1024  # 8MB default

    # ------------------------------------------------------------------
    # Public API — enforces size limits, delegates to subclass
    # ------------------------------------------------------------------

    def serialize(self, payload: dict[str, Any], packet_type: PacketType | None = None) -> bytes:
        """Serialize payload dict to bytes.

        Args:
            payload: Dictionary to serialize.
            packet_type: Optional Moleculer packet type. Schema-based serializers
                (ProtoBuf) require this for correct dispatch. Schema-less serializers
                (JSON, MsgPack, CBOR) ignore it.

        Returns:
            Serialized bytes representation.

        Raises:
            SerializationError: If serialization fails or result exceeds MAX_PAYLOAD_BYTES.
        """
        result = self._serialize_impl(payload, packet_type)
        if len(result) > self.MAX_PAYLOAD_BYTES:
            raise SerializationError(
                f"Serialized payload too large: {len(result)} bytes "
                f"exceeds MAX_PAYLOAD_BYTES limit ({self.MAX_PAYLOAD_BYTES})"
            )
        return result

    def deserialize(self, data: bytes, packet_type: PacketType | None = None) -> dict[str, Any]:
        """Deserialize bytes to payload dict.

        Enforces MAX_PAYLOAD_BYTES limit BEFORE delegating to subclass — this prevents
        DoS via oversized input regardless of whether called from sync or async path.

        Args:
            data: Bytes to deserialize.
            packet_type: Optional packet type for schema-based deserialization.

        Returns:
            Deserialized dictionary.

        Raises:
            SerializationError: If deserialization fails or input exceeds MAX_PAYLOAD_BYTES.
        """
        if len(data) > self.MAX_PAYLOAD_BYTES:
            raise SerializationError(
                f"Payload too large: {len(data)} bytes "
                f"exceeds MAX_PAYLOAD_BYTES limit ({self.MAX_PAYLOAD_BYTES})"
            )
        return self._deserialize_impl(data, packet_type)

    # ------------------------------------------------------------------
    # Template method — subclasses implement these
    # ------------------------------------------------------------------

    @abstractmethod
    def _serialize_impl(self, payload: dict[str, Any], packet_type: PacketType | None) -> bytes:
        """Subclass-specific serialization logic.

        Args:
            payload: Dictionary to serialize.
            packet_type: Optional packet type for schema-based serializers.

        Returns:
            Serialized bytes.

        Raises:
            SerializationError: On serialization failure.
        """
        ...

    @abstractmethod
    def _deserialize_impl(self, data: bytes, packet_type: PacketType | None) -> dict[str, Any]:
        """Subclass-specific deserialization logic.

        Size limit (MAX_PAYLOAD_BYTES) is already enforced by public `deserialize`.

        Args:
            data: Bytes to deserialize.
            packet_type: Optional packet type for schema-based serializers.

        Returns:
            Deserialized dictionary.

        Raises:
            SerializationError: On deserialization failure.
        """
        ...

    # ------------------------------------------------------------------
    # Async variants — thread-offload large payloads
    # ------------------------------------------------------------------

    async def serialize_async(
        self, payload: dict[str, Any], packet_type: PacketType | None = None
    ) -> bytes:
        """Serialize asynchronously (synchronous fast-path).

        Runs synchronously — serialize() is typically fast enough (sub-10µs for
        normal Moleculer payloads) that asyncio.to_thread dispatch overhead
        (~50µs) exceeds the benefit. Size limit enforced by public serialize().

        Args:
            payload: Dictionary to serialize.
            packet_type: Optional packet type for schema-based serializers.

        Returns:
            Serialized bytes.

        Raises:
            SerializationError: If serialization fails or result exceeds MAX_PAYLOAD_BYTES.
        """
        return self.serialize(payload, packet_type)

    async def deserialize_async(
        self, data: bytes, packet_type: PacketType | None = None
    ) -> dict[str, Any]:
        """Deserialize with thread offload for large payloads.

        Payloads larger than THREAD_OFFLOAD_THRESHOLD are deserialized in a thread
        pool to avoid blocking the event loop. MAX_PAYLOAD_BYTES is enforced by
        public deserialize().

        Args:
            data: Bytes to deserialize.
            packet_type: Optional packet type for schema-based serializers.

        Returns:
            Deserialized dictionary.

        Raises:
            SerializationError: If payload exceeds MAX_PAYLOAD_BYTES.
        """
        if len(data) > self.THREAD_OFFLOAD_THRESHOLD:
            return await asyncio.to_thread(self.deserialize, data, packet_type)
        return self.deserialize(data, packet_type)
