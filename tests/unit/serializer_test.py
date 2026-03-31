"""Tests for the pluggable serializer package."""

from __future__ import annotations

import asyncio
import sys
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from moleculerpy.errors import SerializationError
from moleculerpy.serializers import (
    BaseSerializer,
    JsonSerializer,
    MsgPackSerializer,
    resolve_serializer,
)

# =============================================================================
# JSON Serializer Tests
# =============================================================================


class TestJsonSerializer:
    """Tests for JsonSerializer."""

    def setup_method(self) -> None:
        self.serializer = JsonSerializer()

    def test_roundtrip_simple(self) -> None:
        payload: dict[str, Any] = {"action": "math.add", "params": {"a": 1, "b": 2}}
        data = self.serializer.serialize(payload)
        result = self.serializer.deserialize(data)
        assert result == payload

    def test_roundtrip_unicode(self) -> None:
        payload: dict[str, Any] = {"name": "Привет мир", "emoji": "🚀", "cjk": "你好"}
        data = self.serializer.serialize(payload)
        result = self.serializer.deserialize(data)
        assert result == payload

    def test_roundtrip_nested(self) -> None:
        payload: dict[str, Any] = {
            "level1": {
                "level2": {
                    "level3": [1, 2, {"deep": True}],
                },
            },
        }
        data = self.serializer.serialize(payload)
        result = self.serializer.deserialize(data)
        assert result == payload

    def test_roundtrip_empty_dict(self) -> None:
        payload: dict[str, Any] = {}
        data = self.serializer.serialize(payload)
        result = self.serializer.deserialize(data)
        assert result == payload

    def test_serialize_returns_bytes(self) -> None:
        data = self.serializer.serialize({"key": "value"})
        assert isinstance(data, bytes)

    def test_serialize_produces_valid_utf8(self) -> None:
        data = self.serializer.serialize({"key": "value"})
        decoded = data.decode("utf-8")
        assert '"key"' in decoded
        assert '"value"' in decoded

    def test_serialize_non_serializable_raises(self) -> None:
        with pytest.raises(SerializationError, match="JSON serialize failed"):
            self.serializer.serialize({"func": lambda: None})  # type: ignore[dict-item]

    def test_deserialize_invalid_json_raises(self) -> None:
        with pytest.raises(SerializationError, match="JSON deserialize failed"):
            self.serializer.deserialize(b"not json {{{")

    def test_deserialize_invalid_utf8_raises(self) -> None:
        with pytest.raises(SerializationError, match="JSON deserialize failed"):
            self.serializer.deserialize(b"\xff\xfe")

    def test_deserialize_non_dict_raises(self) -> None:
        with pytest.raises(SerializationError, match="Expected dict"):
            self.serializer.deserialize(b"[1, 2, 3]")

    def test_deserialize_string_raises(self) -> None:
        with pytest.raises(SerializationError, match="Expected dict"):
            self.serializer.deserialize(b'"just a string"')

    def test_roundtrip_special_values(self) -> None:
        payload: dict[str, Any] = {
            "null_val": None,
            "bool_true": True,
            "bool_false": False,
            "int_val": 42,
            "float_val": 3.14,
            "list_val": [1, "two", None],
        }
        data = self.serializer.serialize(payload)
        result = self.serializer.deserialize(data)
        assert result == payload

    def test_isinstance_base_serializer(self) -> None:
        assert isinstance(self.serializer, BaseSerializer)


# =============================================================================
# MsgPack Serializer Tests
# =============================================================================


class TestMsgPackSerializer:
    """Tests for MsgPackSerializer."""

    def setup_method(self) -> None:
        pytest.importorskip("msgpack")
        self.serializer = MsgPackSerializer()

    def test_roundtrip_simple(self) -> None:
        payload: dict[str, Any] = {"action": "math.add", "params": {"a": 1, "b": 2}}
        data = self.serializer.serialize(payload)
        result = self.serializer.deserialize(data)
        assert result == payload

    def test_roundtrip_binary_data(self) -> None:
        payload: dict[str, Any] = {"data": b"\x00\x01\x02\xff"}
        data = self.serializer.serialize(payload)
        result = self.serializer.deserialize(data)
        assert result["data"] == b"\x00\x01\x02\xff"

    def test_roundtrip_nested(self) -> None:
        payload: dict[str, Any] = {
            "level1": {"level2": {"values": [1, 2, 3]}},
        }
        data = self.serializer.serialize(payload)
        result = self.serializer.deserialize(data)
        assert result == payload

    def test_roundtrip_empty_dict(self) -> None:
        payload: dict[str, Any] = {}
        data = self.serializer.serialize(payload)
        result = self.serializer.deserialize(data)
        assert result == payload

    def test_serialize_returns_bytes(self) -> None:
        data = self.serializer.serialize({"key": "value"})
        assert isinstance(data, bytes)

    def test_deserialize_invalid_data_raises(self) -> None:
        with pytest.raises(SerializationError, match="MsgPack deserialize failed"):
            self.serializer.deserialize(b"\xc1")  # invalid msgpack byte

    def test_deserialize_non_dict_raises(self) -> None:
        import msgpack

        data = msgpack.packb([1, 2, 3])
        with pytest.raises(SerializationError, match="Expected dict"):
            self.serializer.deserialize(data)

    def test_isinstance_base_serializer(self) -> None:
        assert isinstance(self.serializer, BaseSerializer)


class TestMsgPackImportError:
    """Test MsgPackSerializer behavior when msgpack is not installed."""

    def test_import_error_when_msgpack_missing(self) -> None:
        with patch.dict(sys.modules, {"msgpack": None}):
            # We need to reload to pick up the patched import
            # Instead, test via the MSGPACK_AVAILABLE flag
            from moleculerpy.serializers import msgpack as msgpack_mod

            original = msgpack_mod.MSGPACK_AVAILABLE
            try:
                msgpack_mod.MSGPACK_AVAILABLE = False
                with pytest.raises(ImportError, match="msgpack package required"):
                    MsgPackSerializer()
            finally:
                msgpack_mod.MSGPACK_AVAILABLE = original


# =============================================================================
# Factory / resolve_serializer Tests
# =============================================================================


class TestResolveSerializer:
    """Tests for the resolve_serializer factory."""

    def test_resolve_json_lowercase(self) -> None:
        s = resolve_serializer("json")
        assert isinstance(s, JsonSerializer)

    def test_resolve_json_uppercase(self) -> None:
        s = resolve_serializer("JSON")
        assert isinstance(s, JsonSerializer)

    def test_resolve_json_mixed_case(self) -> None:
        s = resolve_serializer("Json")
        assert isinstance(s, JsonSerializer)

    def test_resolve_msgpack_lowercase(self) -> None:
        pytest.importorskip("msgpack")
        s = resolve_serializer("msgpack")
        assert isinstance(s, MsgPackSerializer)

    def test_resolve_msgpack_uppercase(self) -> None:
        pytest.importorskip("msgpack")
        s = resolve_serializer("MSGPACK")
        assert isinstance(s, MsgPackSerializer)

    def test_resolve_unknown_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown serializer"):
            resolve_serializer("protobuf")

    def test_resolve_empty_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown serializer"):
            resolve_serializer("")


# =============================================================================
# Async Tests
# =============================================================================


class TestAsyncSerializer:
    """Tests for async serializer methods."""

    def test_serialize_async_returns_same_as_sync(self) -> None:
        serializer = JsonSerializer()
        payload: dict[str, Any] = {"key": "value"}
        sync_result = serializer.serialize(payload)
        async_result = asyncio.run(serializer.serialize_async(payload))
        assert sync_result == async_result

    def test_deserialize_async_small_payload_no_offload(self) -> None:
        serializer = JsonSerializer()
        data = serializer.serialize({"key": "value"})

        with patch("moleculerpy.serializers.base.asyncio.to_thread") as mock_to_thread:
            result = asyncio.run(serializer.deserialize_async(data))
            mock_to_thread.assert_not_called()
        assert result == {"key": "value"}

    def test_deserialize_async_large_payload_offloads(self) -> None:
        serializer = JsonSerializer()
        # Create payload larger than threshold
        large_payload: dict[str, Any] = {
            "data": "x" * (BaseSerializer.THREAD_OFFLOAD_THRESHOLD + 1)
        }
        data = serializer.serialize(large_payload)

        with patch(
            "moleculerpy.serializers.base.asyncio.to_thread",
            new_callable=AsyncMock,
            return_value=large_payload,
        ) as mock_to_thread:
            result = asyncio.run(serializer.deserialize_async(data))
            mock_to_thread.assert_called_once_with(serializer.deserialize, data)
        assert result == large_payload


# =============================================================================
# SerializationError Tests
# =============================================================================


class TestSerializationError:
    """Tests for SerializationError."""

    def test_is_moleculer_error(self) -> None:
        from moleculerpy.errors import MoleculerError

        err = SerializationError("test error")
        assert isinstance(err, MoleculerError)

    def test_error_attributes(self) -> None:
        err = SerializationError("test message")
        assert err.message == "test message"
        assert err.code == 500
        assert err.type == "SERIALIZATION_ERROR"
        assert err.retryable is False

    def test_error_in_registry(self) -> None:
        from moleculerpy.errors import ERROR_REGISTRY

        assert "SerializationError" in ERROR_REGISTRY

    def test_str_representation(self) -> None:
        err = SerializationError("bad data")
        assert "bad data" in str(err)


# =============================================================================
# Additional Audit Tests
# =============================================================================


class TestResolveMsgPackImportError:
    """Test resolve_serializer raises ImportError for missing msgpack."""

    def test_resolve_msgpack_raises_import_error_when_not_installed(self) -> None:
        from moleculerpy.serializers import msgpack as msgpack_mod

        original = msgpack_mod.MSGPACK_AVAILABLE
        try:
            msgpack_mod.MSGPACK_AVAILABLE = False
            with pytest.raises(ImportError, match="msgpack package required"):
                resolve_serializer("MSGPACK")
        finally:
            msgpack_mod.MSGPACK_AVAILABLE = original


class TestDeserializeEmptyBytes:
    """Test that deserializing empty bytes raises SerializationError."""

    def test_json_deserialize_empty_bytes_raises(self) -> None:
        serializer = JsonSerializer()
        with pytest.raises(SerializationError):
            serializer.deserialize(b"")

    def test_msgpack_deserialize_empty_bytes_raises(self) -> None:
        pytest.importorskip("msgpack")
        serializer = MsgPackSerializer()
        with pytest.raises(SerializationError):
            serializer.deserialize(b"")


class TestMsgPackNonSerializable:
    """Test that MsgPack raises on non-serializable types."""

    def test_msgpack_serialize_non_serializable_raises(self) -> None:
        pytest.importorskip("msgpack")
        serializer = MsgPackSerializer()
        with pytest.raises(SerializationError, match="MsgPack serialize failed"):
            serializer.serialize({"func": lambda: None})  # type: ignore[dict-item]


class TestDeserializePayloadTooLarge:
    """Test that oversized payloads are rejected by deserialize_async."""

    def test_deserialize_payload_too_large_raises(self) -> None:
        serializer = JsonSerializer()
        # Create data larger than MAX_PAYLOAD_BYTES
        oversized = b"x" * (BaseSerializer.MAX_PAYLOAD_BYTES + 1)
        with pytest.raises(SerializationError, match="Payload too large"):
            asyncio.run(serializer.deserialize_async(oversized))
