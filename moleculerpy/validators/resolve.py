"""Resolver for the pluggable validator setting.

Supports string names, classes, instances, and a registry for plugins.
"""

from __future__ import annotations

from typing import Any

from .base import BaseValidator
from .default import DefaultValidator

# Registry for named validators (plugin pattern)
_VALIDATOR_REGISTRY: dict[str, type[BaseValidator]] = {
    "default": DefaultValidator,
}


def register_validator(name: str, cls: type[BaseValidator]) -> None:
    """Register a validator class by name for string-based resolution.

    Args:
        name: Name to register (case-insensitive).
        cls: Validator class (must be BaseValidator subclass).
    """
    _VALIDATOR_REGISTRY[name.lower()] = cls


def resolve_validator(spec: Any) -> BaseValidator | None:
    """Resolve a validator specification to a BaseValidator instance.

    Args:
        spec: One of:
            - ``"default"`` or ``True`` or ``None`` → DefaultValidator()
            - ``"none"`` or ``False`` → None (validation disabled)
            - a registered string name → corresponding class instance
            - a subclass of BaseValidator → instantiate it
            - an instance of BaseValidator → pass through unchanged

    Returns:
        A BaseValidator instance, or None if validation is disabled.

    Raises:
        TypeError: If *spec* is not a recognised type.
        ValueError: If string name is not registered.
    """
    if spec is None or spec is True:
        return DefaultValidator()

    if spec is False or (isinstance(spec, str) and spec.lower() == "none"):
        return None

    if isinstance(spec, BaseValidator):
        return spec

    if isinstance(spec, type) and issubclass(spec, BaseValidator):
        return spec()

    if isinstance(spec, str):
        cls = _VALIDATOR_REGISTRY.get(spec.lower())
        if cls is None:
            raise ValueError(
                f"Unknown validator: {spec!r}. Available: {list(_VALIDATOR_REGISTRY.keys())}"
            )
        return cls()

    raise TypeError(
        f"Cannot resolve validator from {spec!r}. "
        "Expected 'default', 'none', False, a BaseValidator subclass or instance."
    )


__all__ = ["register_validator", "resolve_validator"]
