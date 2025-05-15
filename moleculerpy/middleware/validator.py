"""Validator Middleware for the MoleculerPy framework.

This module implements parameter validation for action handlers using
schema definitions, similar to Moleculer.js's fastest-validator integration.

Features:
    - Schema-based parameter validation
    - Type checking (string, int, float, bool, list, dict, any)
    - Required/optional fields
    - Min/max for numbers and string length
    - Pattern matching (regex)
    - Enum validation
    - Nested object validation
    - Custom validators
    - Configurable behavior (remove unknown, error on invalid)

Example usage:
    from moleculerpy import Service

    class UserService(Service):
        name = "users"

        actions = {
            "create": {
                "params": {
                    "name": {"type": "string", "min": 2, "max": 50},
                    "email": {"type": "string", "pattern": r"^[\\w.-]+@[\\w.-]+\\.\\w+$"},
                    "age": {"type": "int", "min": 0, "max": 150, "optional": True},
                    "role": {"type": "enum", "values": ["admin", "user", "guest"]},
                    "address": {
                        "type": "object",
                        "props": {
                            "city": {"type": "string"},
                            "zip": {"type": "string", "pattern": r"^\\d{5}$"},
                        },
                        "optional": True,
                    },
                },
                "handler": "create_user",
            },
        }

Configuration options:
    paramValidation: Behavior on validation
        - "error" (default): Raise ValidationError
        - "remove": Remove invalid fields, don't raise

Moleculer.js compatible concepts:
    - fastest-validator schema format
    - ValidationError with field paths
    - Nested validation support
    - Custom validator functions
"""

from __future__ import annotations

import logging
import re
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, cast

from moleculerpy.errors import ValidationError
from moleculerpy.middleware.base import Middleware

if TYPE_CHECKING:
    from moleculerpy.context import Context
    from moleculerpy.protocols import ActionProtocol

__all__ = ["ValidationRule", "ValidatorMiddleware", "validate_params"]

# Type alias for action handlers
HandlerType = Callable[["Context"], Awaitable[Any]]

# Type alias for custom validators
CustomValidatorType = Callable[[Any, dict[str, Any], str], list[str]]


class ValidationRule:
    """Represents a validation rule for a parameter.

    Attributes:
        type: The expected type (string, int, float, bool, list, dict, any)
        required: Whether the field is required (default: True)
        optional: Whether the field is optional (default: False)
        min: Minimum value (for numbers) or length (for strings/lists)
        max: Maximum value (for numbers) or length (for strings/lists)
        pattern: Regex pattern for string validation
        values: Allowed values for enum type
        props: Nested properties for object type
        items: Item schema for list type
        default: Default value if not provided
        custom: Custom validator function
    """

    __slots__ = (
        "custom",
        "default",
        "items",
        "max",
        "min",
        "optional",
        "pattern",
        "props",
        "required",
        "type",
        "values",
    )

    def __init__(
        self,
        type_: str = "any",
        required: bool = True,
        optional: bool = False,
        min_val: int | float | None = None,
        max_val: int | float | None = None,
        pattern: str | None = None,
        values: list[Any] | None = None,
        props: dict[str, Any] | None = None,
        items: dict[str, Any] | None = None,
        default: Any = None,
        custom: CustomValidatorType | None = None,
    ) -> None:
        """Initialize validation rule.

        Args:
            type_: Expected type
            required: Whether field is required
            optional: Whether field is optional (inverse of required)
            min_val: Minimum value or length
            max_val: Maximum value or length
            pattern: Regex pattern for strings
            values: Allowed values for enums
            props: Nested object properties
            items: Schema for list items
            default: Default value
            custom: Custom validator function
        """
        self.type = type_
        self.required = required and not optional
        self.optional = optional
        self.min = min_val
        self.max = max_val
        self.pattern = pattern
        self.values = values
        self.props = props
        self.items = items
        self.default = default
        self.custom = custom

    @classmethod
    def from_dict(cls, schema: dict[str, Any]) -> ValidationRule:
        """Create ValidationRule from dict schema.

        Args:
            schema: Dict with validation rules

        Returns:
            ValidationRule instance
        """
        return cls(
            type_=schema.get("type", "any"),
            required=schema.get("required", True),
            optional=schema.get("optional", False),
            min_val=schema.get("min"),
            max_val=schema.get("max"),
            pattern=schema.get("pattern"),
            values=schema.get("values"),
            props=schema.get("props"),
            items=schema.get("items"),
            default=schema.get("default"),
            custom=schema.get("custom"),
        )


def validate_params(
    params: dict[str, Any],
    schema: dict[str, Any],
    path: str = "",
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    """Validate parameters against schema.

    Args:
        params: Parameters to validate
        schema: Validation schema
        path: Current path for nested validation

    Returns:
        Tuple of (validated_params, errors)
    """
    errors: list[dict[str, str]] = []
    validated: dict[str, Any] = {}

    for field_name, field_schema in schema.items():
        field_path = f"{path}.{field_name}" if path else field_name

        # Handle string shorthand: {"name": "string"} -> {"name": {"type": "string"}}
        normalized_schema = field_schema
        if isinstance(field_schema, str):
            normalized_schema = {"type": field_schema}

        rule = ValidationRule.from_dict(normalized_schema)
        value = params.get(field_name)

        # Check required
        if value is None:
            if rule.default is not None:
                validated[field_name] = rule.default
                continue
            if rule.required and not rule.optional:
                errors.append(
                    {
                        "field": field_path,
                        "message": f"Field '{field_path}' is required",
                        "type": "required",
                    }
                )
            continue

        # Type validation
        type_errors = _validate_type(value, rule, field_path)
        if type_errors:
            errors.extend(type_errors)
            continue

        # Value constraints
        constraint_errors = _validate_constraints(value, rule, field_path)
        errors.extend(constraint_errors)

        # Nested object validation
        if rule.type == "object" and rule.props and isinstance(value, dict):
            nested_validated, nested_errors = validate_params(value, rule.props, field_path)
            errors.extend(nested_errors)
            validated[field_name] = nested_validated
        # List item validation
        elif rule.type in ("list", "array") and rule.items and isinstance(value, list):
            validated_items = []
            for i, item in enumerate(value):
                item_path = f"{field_path}[{i}]"
                if isinstance(rule.items, dict) and rule.items.get("type") == "object":
                    # Nested object in array
                    if isinstance(item, dict) and rule.items.get("props"):
                        item_validated, item_errors = validate_params(
                            item, rule.items["props"], item_path
                        )
                        errors.extend(item_errors)
                        validated_items.append(item_validated)
                    else:
                        validated_items.append(item)
                else:
                    validated_items.append(item)
            validated[field_name] = validated_items
        else:
            validated[field_name] = value

        # Custom validator
        if rule.custom:
            custom_errors = rule.custom(value, params, field_path)
            for err in custom_errors:
                errors.append(
                    {
                        "field": field_path,
                        "message": err,
                        "type": "custom",
                    }
                )

    return validated, errors


def _validate_type(
    value: Any,
    rule: ValidationRule,
    field_path: str,
) -> list[dict[str, str]]:
    """Validate value type.

    Args:
        value: Value to validate
        rule: Validation rule
        field_path: Field path for error messages

    Returns:
        List of type errors
    """
    errors: list[dict[str, str]] = []
    type_map: dict[str, tuple[type, ...]] = {
        "string": (str,),
        "str": (str,),
        "int": (int,),
        "integer": (int,),
        "float": (float, int),
        "number": (float, int),
        "bool": (bool,),
        "boolean": (bool,),
        "list": (list,),
        "array": (list,),
        "dict": (dict,),
        "object": (dict,),
        "any": (),  # Any type allowed
    }

    expected_types = type_map.get(rule.type, ())

    # "any" type accepts everything
    if rule.type == "any":
        return errors

    # Enum type
    if rule.type == "enum":
        if rule.values and value not in rule.values:
            errors.append(
                {
                    "field": field_path,
                    "message": f"Field '{field_path}' must be one of: {rule.values}",
                    "type": "enum",
                    "actual": str(value),
                    "expected": str(rule.values),
                }
            )
        return errors

    # Regular type checking
    if expected_types and not isinstance(value, expected_types):
        errors.append(
            {
                "field": field_path,
                "message": f"Field '{field_path}' must be {rule.type}, got {type(value).__name__}",
                "type": "type",
                "actual": type(value).__name__,
                "expected": rule.type,
            }
        )

    return errors


def _validate_constraints(
    value: Any,
    rule: ValidationRule,
    field_path: str,
) -> list[dict[str, str]]:
    """Validate value constraints (min, max, pattern).

    Args:
        value: Value to validate
        rule: Validation rule
        field_path: Field path for error messages

    Returns:
        List of constraint errors
    """
    errors: list[dict[str, str]] = []

    # Min/max for numbers
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if rule.min is not None and value < rule.min:
            errors.append(
                {
                    "field": field_path,
                    "message": f"Field '{field_path}' must be >= {rule.min}",
                    "type": "min",
                    "actual": str(value),
                    "expected": str(rule.min),
                }
            )
        if rule.max is not None and value > rule.max:
            errors.append(
                {
                    "field": field_path,
                    "message": f"Field '{field_path}' must be <= {rule.max}",
                    "type": "max",
                    "actual": str(value),
                    "expected": str(rule.max),
                }
            )

    # Min/max for strings (length)
    if isinstance(value, str):
        if rule.min is not None and len(value) < rule.min:
            errors.append(
                {
                    "field": field_path,
                    "message": f"Field '{field_path}' must have at least {rule.min} characters",
                    "type": "minLength",
                    "actual": str(len(value)),
                    "expected": str(rule.min),
                }
            )
        if rule.max is not None and len(value) > rule.max:
            errors.append(
                {
                    "field": field_path,
                    "message": f"Field '{field_path}' must have at most {rule.max} characters",
                    "type": "maxLength",
                    "actual": str(len(value)),
                    "expected": str(rule.max),
                }
            )

        # Pattern validation
        if rule.pattern:
            if not re.match(rule.pattern, value):
                errors.append(
                    {
                        "field": field_path,
                        "message": f"Field '{field_path}' does not match pattern",
                        "type": "pattern",
                        "actual": value,
                        "expected": rule.pattern,
                    }
                )

    # Min/max for lists (length)
    if isinstance(value, list):
        if rule.min is not None and len(value) < rule.min:
            errors.append(
                {
                    "field": field_path,
                    "message": f"Field '{field_path}' must have at least {rule.min} items",
                    "type": "minItems",
                    "actual": str(len(value)),
                    "expected": str(rule.min),
                }
            )
        if rule.max is not None and len(value) > rule.max:
            errors.append(
                {
                    "field": field_path,
                    "message": f"Field '{field_path}' must have at most {rule.max} items",
                    "type": "maxItems",
                    "actual": str(len(value)),
                    "expected": str(rule.max),
                }
            )

    return errors


class ValidatorMiddleware(Middleware):
    """Middleware for validating action parameters.

    Validates incoming action parameters against schemas defined in
    action definitions. Supports multiple validation modes.

    Attributes:
        logger: Logger for validation events
        mode: Validation mode ("error" or "remove")
    """

    __slots__ = ("logger", "mode")

    def __init__(
        self,
        mode: str = "error",
        logger: logging.Logger | None = None,
    ) -> None:
        """Initialize the ValidatorMiddleware.

        Args:
            mode: Validation mode
                - "error": Raise ValidationError on invalid params
                - "remove": Remove invalid fields, continue execution
            logger: Optional logger for validation events
        """
        super().__init__()
        self.logger = logger or logging.getLogger("moleculerpy.middleware.validator")
        self.mode = mode

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return f"ValidatorMiddleware(mode={self.mode!r})"

    async def local_action(
        self,
        next_handler: HandlerType,
        action: ActionProtocol,
    ) -> HandlerType:
        """Wrap local action with validation.

        Args:
            next_handler: The next handler in the chain
            action: The action being registered

        Returns:
            Wrapped handler with validation
        """
        # Get params schema from action
        params_schema = getattr(action, "params", None)

        # No schema defined, return original handler
        if not params_schema:
            return next_handler

        action_name = getattr(action, "name", str(action))
        mode = self.mode

        self.logger.debug(f"Validator enabled for action '{action_name}'")

        async def validate_wrapper(ctx: Context) -> Any:
            """Wrapper that validates params before calling handler."""
            params = ctx.params or {}

            # Validate parameters
            validated, errors = validate_params(params, params_schema)

            if errors:
                if mode == "error":
                    # Raise validation error
                    error_messages = [e["message"] for e in errors]
                    self.logger.warning(f"Validation failed for '{action_name}': {error_messages}")
                    raise ValidationError(
                        data=cast(dict[str, Any], errors),
                        message=f"Validation failed: {', '.join(error_messages)}",
                    )
                elif mode == "remove":
                    # Log and continue with validated params
                    self.logger.debug(f"Removed invalid fields from '{action_name}': {errors}")

            # Update context params with validated values
            # Note: This modifies ctx.params in place or creates new dict
            if hasattr(ctx, "_params"):
                ctx._params = validated
            elif hasattr(ctx, "params") and isinstance(ctx.params, dict):
                ctx.params.clear()
                ctx.params.update(validated)

            return await next_handler(ctx)

        return validate_wrapper

    async def remote_action(
        self,
        next_handler: HandlerType,
        action: ActionProtocol,
    ) -> HandlerType:
        """Wrap remote action with validation.

        Remote actions also need validation for incoming requests.

        Args:
            next_handler: The next handler in the chain
            action: The action being registered

        Returns:
            Wrapped handler with validation
        """
        return await self.local_action(next_handler, action)
