"""Pluggable serializer package for MoleculerPy framework.

Provides serializer abstraction with JSON (default) and MsgPack implementations.
Custom serializers can be created by subclassing BaseSerializer.

Usage:
    from moleculerpy.serializers import resolve_serializer

    serializer = resolve_serializer("json")     # JsonSerializer
    serializer = resolve_serializer("msgpack")  # MsgPackSerializer

    data = serializer.serialize({"action": "math.add", "params": {"a": 1}})
    payload = serializer.deserialize(data)
"""

from __future__ import annotations

from .base import BaseSerializer
from .json import JsonSerializer
from .msgpack import MsgPackSerializer

_SERIALIZER_REGISTRY: dict[str, type[BaseSerializer]] = {
    "JSON": JsonSerializer,
    "MSGPACK": MsgPackSerializer,
}


def resolve_serializer(name: str) -> BaseSerializer:
    """Resolve a serializer by name.

    Args:
        name: Serializer name (case-insensitive). Supported: "json", "msgpack".

    Returns:
        Instantiated serializer

    Raises:
        ValueError: If serializer name is not recognized
    """
    key = name.upper()
    cls = _SERIALIZER_REGISTRY.get(key)
    if cls is None:
        raise ValueError(f"Unknown serializer: {name!r}. Available: {list(_SERIALIZER_REGISTRY)}")
    return cls()


__all__ = [
    "BaseSerializer",
    "JsonSerializer",
    "MsgPackSerializer",
    "resolve_serializer",
]
