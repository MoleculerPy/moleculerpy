"""Mixin support for MoleculerPy services.

This module provides Moleculer.js compatible mixin functionality for composing
services from reusable components.

Mixins can be defined as:
1. Python classes with special attributes (__settings__, __hooks__, etc.)
2. Dictionary schemas (Moleculer.js style)

Example:
    # Class-based mixin
    class LoggerMixin:
        __settings__ = {"log_level": "debug"}
        __hooks__ = {"before": {"*": "log_action"}}

        def log_action(self, ctx):
            self.logger.debug(f"Action: {ctx.action}")

        def log(self, message: str):
            self.logger.info(message)

    # Dict-based mixin (Moleculer.js style)
    AuthMixin = {
        "settings": {"secret_key": "change-me"},
        "hooks": {
            "before": {"*": lambda self, ctx: check_auth(ctx)}
        },
        "methods": {
            "verify_token": lambda self, token: verify(token)
        }
    }

    # Using mixins in service
    class UserService(Service):
        name = "users"
        mixins = [LoggerMixin, AuthMixin]

        @action()
        async def get_user(self, ctx):
            self.log("Getting user")  # From LoggerMixin
            return {"id": 1}

Merge priorities (last wins):
    mixin1 < mixin2 < mixin3 < service (main schema)

Merge strategies by field:
    - name, version: Override (last wins)
    - settings, metadata: Deep merge
    - actions: Deep merge with hook composition
    - events: Concat handlers (all execute)
    - methods: Override (last wins)
    - hooks: Concat with order (before: [target, src], after: [src, target])
    - lifecycle (created, started, stopped): Concat (all execute)
    - dependencies, mixins: Unique array
"""

from __future__ import annotations

import copy
import logging
import types
from collections.abc import Callable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    TypedDict,
    cast,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


# Type definitions
MixinDict = dict[str, Any]
MixinClass = type
Mixin = MixinDict | MixinClass


class HooksSchema(TypedDict, total=False):
    """Schema for hooks definition."""

    before: dict[str, Callable[..., Any] | list[Callable[..., Any]] | str | list[str]]
    after: dict[str, Callable[..., Any] | list[Callable[..., Any]] | str | list[str]]
    error: dict[str, Callable[..., Any] | list[Callable[..., Any]] | str | list[str]]


class MixinSchema(TypedDict, total=False):
    """Schema for mixin definitions (Moleculer.js compatible)."""

    name: str
    version: str
    settings: dict[str, Any]
    metadata: dict[str, Any]
    actions: dict[str, Any]
    events: dict[str, Any]
    methods: dict[str, Callable[..., Any]]
    hooks: HooksSchema
    dependencies: list[str]
    mixins: list[Mixin]
    # Lifecycle handlers
    created: Callable[..., Any] | list[Callable[..., Any]]
    started: Callable[..., Any] | list[Callable[..., Any]]
    stopped: Callable[..., Any] | list[Callable[..., Any]]
    merged: Callable[..., Any] | list[Callable[..., Any]]


def _wrap_to_list(value: Any) -> list[Any]:
    """Wrap a value in a list if it's not already a list.

    Args:
        value: Value to wrap

    Returns:
        List containing the value, or the value itself if already a list
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _flatten(nested: list[Any]) -> list[Any]:
    """Flatten a nested list one level deep.

    Args:
        nested: List that may contain nested lists

    Returns:
        Flattened list
    """
    result = []
    for item in nested:
        if isinstance(item, list):
            result.extend(item)
        else:
            result.append(item)
    return result


def _compact(items: list[Any]) -> list[Any]:
    """Remove None values from a list.

    Args:
        items: List that may contain None values

    Returns:
        List without None values
    """
    return [item for item in items if item is not None]


def _unique_by_identity(items: list[Any]) -> list[Any]:
    """Remove duplicates from a list preserving order (by identity).

    Args:
        items: List that may contain duplicates

    Returns:
        List with unique items
    """
    seen = set()
    result = []
    for item in items:
        item_id = id(item)
        if item_id not in seen:
            seen.add(item_id)
            result.append(item)
    return result


def deep_merge(target: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
    """Deep merge two dictionaries (source into target).

    Args:
        target: Target dictionary (lower priority)
        source: Source dictionary (higher priority, will override)

    Returns:
        Merged dictionary
    """
    result = copy.deepcopy(target)

    for key, value in source.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)

    return result


def mixin_to_schema(mixin: Mixin) -> MixinSchema:
    """Convert a mixin (class or dict) to a schema dict.

    Args:
        mixin: Mixin class or dictionary

    Returns:
        Schema dictionary
    """
    if isinstance(mixin, dict):
        return cast(MixinSchema, dict(mixin))

    # Convert class to schema
    schema: MixinSchema = {}

    # Extract class attributes
    if hasattr(mixin, "name"):
        schema["name"] = mixin.name
    if hasattr(mixin, "version"):
        schema["version"] = mixin.version
    if hasattr(mixin, "__settings__"):
        schema["settings"] = mixin.__settings__
    if hasattr(mixin, "settings") and isinstance(mixin.settings, dict):
        schema["settings"] = mixin.settings
    if hasattr(mixin, "__metadata__"):
        schema["metadata"] = mixin.__metadata__
    if hasattr(mixin, "metadata") and isinstance(mixin.metadata, dict):
        schema["metadata"] = mixin.metadata
    if hasattr(mixin, "__hooks__"):
        schema["hooks"] = cast(HooksSchema, mixin.__hooks__)
    if hasattr(mixin, "hooks") and isinstance(mixin.hooks, dict):
        schema["hooks"] = cast(HooksSchema, mixin.hooks)
    if hasattr(mixin, "__dependencies__"):
        schema["dependencies"] = mixin.__dependencies__
    if hasattr(mixin, "dependencies") and isinstance(mixin.dependencies, list):
        schema["dependencies"] = mixin.dependencies
    if hasattr(mixin, "mixins"):
        schema["mixins"] = mixin.mixins

    # Extract methods (non-special, non-dunder)
    methods: dict[str, Callable[..., Any]] = {}
    for attr_name in dir(mixin):
        if attr_name.startswith("_"):
            continue
        if attr_name in (
            "name",
            "version",
            "settings",
            "metadata",
            "hooks",
            "dependencies",
            "mixins",
        ):
            continue

        attr = getattr(mixin, attr_name, None)
        if callable(attr) and not isinstance(attr, type):
            methods[attr_name] = attr

    if methods:
        schema["methods"] = methods

    # Extract lifecycle hooks from class
    for hook_name in ("created", "started", "stopped", "merged"):
        if hasattr(mixin, hook_name):
            hook = getattr(mixin, hook_name)
            if callable(hook):
                schema[hook_name] = hook

    return schema


def merge_settings(target: dict[str, Any] | None, source: dict[str, Any] | None) -> dict[str, Any]:
    """Merge settings with deep merge strategy.

    Args:
        target: Target settings (lower priority)
        source: Source settings (higher priority)

    Returns:
        Merged settings
    """
    if target is None:
        return source or {}
    if source is None:
        return target

    return deep_merge(target, source)


def merge_metadata(target: dict[str, Any] | None, source: dict[str, Any] | None) -> dict[str, Any]:
    """Merge metadata with deep merge strategy.

    Args:
        target: Target metadata (lower priority)
        source: Source metadata (higher priority)

    Returns:
        Merged metadata
    """
    return merge_settings(target, source)


def merge_hooks(target: HooksSchema | None, source: HooksSchema | None) -> HooksSchema:
    """Merge hooks with proper ordering.

    For 'before' hooks: [target, source] (target executes first)
    For 'after' hooks: [source, target] (source executes first)
    For 'error' hooks: [source, target]

    Args:
        target: Target hooks (lower priority but executes first in before)
        source: Source hooks (higher priority)

    Returns:
        Merged hooks
    """
    if target is None:
        return source or {}
    if source is None:
        return target

    result: HooksSchema = copy.deepcopy(target)

    for hook_type in ("before", "after", "error"):
        if hook_type not in source:
            continue

        if hook_type not in result:
            result[hook_type] = {}

        source_hooks = source[hook_type]
        target_hooks = result[hook_type]

        for action_name, handlers in source_hooks.items():
            source_list = _wrap_to_list(handlers)
            target_list = _wrap_to_list(target_hooks.get(action_name))

            # Order depends on hook type
            if hook_type == "before":
                # before: target first, then source
                merged = _compact(_flatten([target_list, source_list]))
            else:
                # after/error: source first, then target
                merged = _compact(_flatten([source_list, target_list]))

            # Unwrap single handler
            if len(merged) == 1:
                target_hooks[action_name] = merged[0]
            else:
                target_hooks[action_name] = merged

    return result


def merge_actions(target: dict[str, Any] | None, source: dict[str, Any] | None) -> dict[str, Any]:
    """Merge actions with deep merge and hook composition.

    Supports disabling actions with `action_name: False`.

    Args:
        target: Target actions (lower priority)
        source: Source actions (higher priority)

    Returns:
        Merged actions
    """
    if target is None:
        return source or {}
    if source is None:
        return target

    result = copy.deepcopy(target)

    for action_name, source_action_def in source.items():
        # Handle action disable
        if source_action_def is False:
            if action_name in result:
                del result[action_name]
            continue

        # Normalize to dict
        normalized_action_def = source_action_def
        if callable(source_action_def):
            normalized_action_def = {"handler": source_action_def}

        target_def = result.get(action_name)
        if target_def is not None and callable(target_def):
            target_def = {"handler": target_def}

        # Merge action hooks if both have them
        if (
            normalized_action_def
            and isinstance(normalized_action_def, dict)
            and "hooks" in normalized_action_def
            and isinstance(normalized_action_def["hooks"], dict)
            and target_def
            and isinstance(target_def, dict)
            and "hooks" in target_def
            and isinstance(target_def["hooks"], dict)
        ):
            normalized_action_def = dict(normalized_action_def)
            normalized_action_def["hooks"] = merge_hooks(
                target_def.get("hooks"), normalized_action_def["hooks"]
            )

        # Deep merge action definitions
        if target_def and isinstance(target_def, dict) and isinstance(normalized_action_def, dict):
            result[action_name] = deep_merge(target_def, normalized_action_def)
        else:
            result[action_name] = (
                copy.deepcopy(normalized_action_def)
                if isinstance(normalized_action_def, dict)
                else normalized_action_def
            )

    return result


def merge_events(target: dict[str, Any] | None, source: dict[str, Any] | None) -> dict[str, Any]:
    """Merge events by concatenating handlers.

    Unlike actions, event handlers are ALL executed (not overridden).

    Args:
        target: Target events (lower priority)
        source: Source events (higher priority)

    Returns:
        Merged events
    """
    if target is None:
        return source or {}
    if source is None:
        return target

    result = copy.deepcopy(target)

    for event_name, source_event_def in source.items():
        # Normalize to dict
        normalized_event_def = source_event_def
        if callable(source_event_def):
            normalized_event_def = {"handler": source_event_def}

        target_def = result.get(event_name)
        if target_def is not None and callable(target_def):
            target_def = {"handler": target_def}

        # Concat handlers
        if target_def and isinstance(target_def, dict):
            target_handlers = _wrap_to_list(target_def.get("handler"))
            source_handlers = _wrap_to_list(
                normalized_event_def.get("handler")
                if isinstance(normalized_event_def, dict)
                else normalized_event_def
            )

            merged_handlers = _compact(_flatten([target_handlers, source_handlers]))

            # Deep merge other properties
            merged_def = (
                deep_merge(target_def, normalized_event_def)
                if isinstance(normalized_event_def, dict)
                else dict(target_def)
            )

            # Set concatenated handlers
            if len(merged_handlers) == 1:
                merged_def["handler"] = merged_handlers[0]
            else:
                merged_def["handler"] = merged_handlers

            result[event_name] = merged_def
        else:
            result[event_name] = (
                copy.deepcopy(normalized_event_def)
                if isinstance(normalized_event_def, dict)
                else normalized_event_def
            )

    return result


def merge_methods(target: dict[str, Any] | None, source: dict[str, Any] | None) -> dict[str, Any]:
    """Merge methods with simple override (last wins).

    Args:
        target: Target methods (lower priority)
        source: Source methods (higher priority)

    Returns:
        Merged methods
    """
    if target is None:
        return source or {}
    if source is None:
        return target

    return {**target, **source}


def merge_lifecycle_handlers(
    target: Callable[..., Any] | list[Callable[..., Any]] | None,
    source: Callable[..., Any] | list[Callable[..., Any]] | None,
) -> list[Callable[..., Any]]:
    """Merge lifecycle handlers by concatenating them.

    Order: [target, source] (target executes first)

    Args:
        target: Target handlers (execute first)
        source: Source handlers (execute after)

    Returns:
        List of handlers to execute in order
    """
    target_list = _wrap_to_list(target)
    source_list = _wrap_to_list(source)

    return _compact(_flatten([target_list, source_list]))


def merge_unique_array(target: list[Any] | None, source: list[Any] | None) -> list[Any]:
    """Merge arrays with unique values (deduplication).

    Args:
        target: Target array
        source: Source array

    Returns:
        Merged array with unique values
    """
    target_list = target or []
    source_list = source or []

    combined = _flatten([target_list, source_list])
    return _unique_by_identity(_compact(combined))


def merge_schemas(target: MixinSchema, source: MixinSchema) -> MixinSchema:
    """Merge two schemas with Moleculer.js compatible strategies.

    Args:
        target: Target schema (lower priority)
        source: Source schema (higher priority, will override)

    Returns:
        Merged schema
    """
    result: dict[str, Any] = dict(target)

    for key, value in cast(dict[str, Any], source).items():
        if key in ("name", "version"):
            # Override
            result[key] = value

        elif key in ("settings",):
            # Deep merge
            result["settings"] = merge_settings(result.get("settings"), value)

        elif key in ("metadata",):
            # Deep merge
            result["metadata"] = merge_metadata(result.get("metadata"), value)

        elif key == "actions":
            # Merge with hook composition
            result["actions"] = merge_actions(result.get("actions"), value)

        elif key == "events":
            # Concat handlers
            result["events"] = merge_events(result.get("events"), value)

        elif key == "methods":
            # Override
            result["methods"] = merge_methods(result.get("methods"), value)

        elif key == "hooks":
            # Concat with order
            result["hooks"] = merge_hooks(result.get("hooks"), value)

        elif key in ("created", "started", "stopped", "merged"):
            # Concat lifecycle handlers
            result[key] = merge_lifecycle_handlers(
                cast(Callable[..., Any] | list[Callable[..., Any]] | None, result.get(key)),
                cast(Callable[..., Any] | list[Callable[..., Any]] | None, value),
            )

        elif key in ("dependencies", "mixins"):
            # Unique array
            result[key] = merge_unique_array(
                cast(list[Any] | None, result.get(key)),
                cast(list[Any] | None, value),
            )

        else:
            # Default: deep copy
            result[key] = copy.deepcopy(value)

    return cast(MixinSchema, result)


def apply_mixins(schema: MixinSchema, mixins: Sequence[Mixin] | None = None) -> MixinSchema:
    """Apply mixins to a schema, Moleculer.js compatible.

    Mixins are processed in reverse order (last mixin has highest priority
    among mixins, but main schema always has the highest priority).

    Order: mixin1 < mixin2 < mixin3 < main_schema

    Args:
        schema: Main schema (highest priority)
        mixins: List of mixins to apply (can be None or from schema["mixins"])

    Returns:
        Schema with all mixins merged in
    """
    # Get mixins from argument or schema
    mixin_list = mixins or schema.get("mixins", [])

    if not mixin_list:
        return schema

    # Normalize to list
    if isinstance(mixin_list, (list, tuple)):
        normalized_mixins: list[Mixin] = list(mixin_list)
    else:
        normalized_mixins = [cast(Mixin, mixin_list)]

    # Convert all mixins to schemas
    mixin_schemas = [mixin_to_schema(m) for m in normalized_mixins]

    # Process mixins in order - later mixins have higher priority
    # Order: mixin1 < mixin2 < mixin3 (mixin3 has highest priority among mixins)
    merged: MixinSchema | None = None

    for mixin_schema in mixin_schemas:
        # Recursively apply nested mixins first
        expanded_mixin = mixin_schema
        if mixin_schema.get("mixins"):
            expanded_mixin = apply_mixins(mixin_schema, mixin_schema.get("mixins"))

        if merged is None:
            merged = expanded_mixin
        else:
            # merge_schemas(target, source) - source has priority
            # So later mixin overrides earlier mixin
            merged = merge_schemas(merged, expanded_mixin)

    # Merge with main schema (main has highest priority)
    if merged is not None:
        return merge_schemas(merged, schema)

    return schema


def extract_mixin_methods(service_instance: Any, schema: MixinSchema) -> None:
    """Attach mixin methods to a service instance.

    Args:
        service_instance: Service instance to attach methods to
        schema: Merged schema containing methods
    """
    methods = schema.get("methods", {})

    for method_name, method_func in methods.items():
        if hasattr(service_instance, method_name):
            # Method already exists, skip (service definition takes priority)
            continue

        # Bind method to instance
        if callable(method_func):
            # Check if it's already a bound method or needs binding
            if isinstance(method_func, types.FunctionType):
                bound_method = types.MethodType(method_func, service_instance)
                setattr(service_instance, method_name, bound_method)
            else:
                setattr(service_instance, method_name, method_func)


__all__ = [
    "HooksSchema",
    "Mixin",
    "MixinClass",
    "MixinDict",
    "MixinSchema",
    "apply_mixins",
    "deep_merge",
    "extract_mixin_methods",
    "merge_actions",
    "merge_events",
    "merge_hooks",
    "merge_lifecycle_handlers",
    "merge_methods",
    "merge_schemas",
    "merge_settings",
    "mixin_to_schema",
]
