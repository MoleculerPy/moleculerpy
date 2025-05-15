"""Unit tests for the Validator Middleware module.

Tests for ValidatorMiddleware that provides parameter validation
for action handlers using schema definitions.
"""

from unittest.mock import MagicMock

import pytest

from moleculerpy.errors import ValidationError
from moleculerpy.middleware.validator import (
    ValidationRule,
    ValidatorMiddleware,
    validate_params,
)


class TestValidationRule:
    """Tests for ValidationRule class."""

    def test_from_dict_defaults(self):
        """Test ValidationRule.from_dict with defaults."""
        rule = ValidationRule.from_dict({})

        assert rule.type == "any"
        assert rule.required is True
        assert rule.optional is False
        assert rule.min is None
        assert rule.max is None

    def test_from_dict_full(self):
        """Test ValidationRule.from_dict with all options."""
        rule = ValidationRule.from_dict(
            {
                "type": "string",
                "required": False,
                "optional": True,
                "min": 2,
                "max": 50,
                "pattern": r"^\w+$",
                "default": "unknown",
            }
        )

        assert rule.type == "string"
        assert rule.required is False
        assert rule.optional is True
        assert rule.min == 2
        assert rule.max == 50
        assert rule.pattern == r"^\w+$"
        assert rule.default == "unknown"

    def test_optional_overrides_required(self):
        """Test that optional=True makes required=False."""
        rule = ValidationRule.from_dict(
            {
                "type": "string",
                "required": True,
                "optional": True,
            }
        )

        assert rule.required is False
        assert rule.optional is True


class TestValidateParams:
    """Tests for validate_params function."""

    def test_validate_required_field_present(self):
        """Test required field validation when present."""
        schema = {"name": {"type": "string"}}
        params = {"name": "John"}

        validated, errors = validate_params(params, schema)

        assert errors == []
        assert validated == {"name": "John"}

    def test_validate_required_field_missing(self):
        """Test required field validation when missing."""
        schema = {"name": {"type": "string"}}
        params = {}

        _validated, errors = validate_params(params, schema)

        assert len(errors) == 1
        assert errors[0]["type"] == "required"
        assert errors[0]["field"] == "name"

    def test_validate_optional_field_missing(self):
        """Test optional field validation when missing."""
        schema = {"name": {"type": "string", "optional": True}}
        params = {}

        validated, errors = validate_params(params, schema)

        assert errors == []
        assert validated == {}

    def test_validate_default_value(self):
        """Test default value is used when field missing."""
        schema = {"status": {"type": "string", "default": "pending"}}
        params = {}

        validated, errors = validate_params(params, schema)

        assert errors == []
        assert validated == {"status": "pending"}

    def test_validate_string_type(self):
        """Test string type validation."""
        schema = {"name": {"type": "string"}}

        # Valid
        validated, errors = validate_params({"name": "John"}, schema)
        assert errors == []

        # Invalid
        _validated, errors = validate_params({"name": 123}, schema)
        assert len(errors) == 1
        assert errors[0]["type"] == "type"

    def test_validate_int_type(self):
        """Test int type validation."""
        schema = {"age": {"type": "int"}}

        # Valid
        validated, errors = validate_params({"age": 25}, schema)
        assert errors == []

        # Invalid
        _validated, errors = validate_params({"age": "25"}, schema)
        assert len(errors) == 1
        assert errors[0]["type"] == "type"

    def test_validate_float_type(self):
        """Test float type validation (accepts int too)."""
        schema = {"price": {"type": "float"}}

        # Valid float
        validated, errors = validate_params({"price": 19.99}, schema)
        assert errors == []

        # Valid int (accepted as float)
        validated, errors = validate_params({"price": 20}, schema)
        assert errors == []

        # Invalid
        _validated, errors = validate_params({"price": "19.99"}, schema)
        assert len(errors) == 1

    def test_validate_bool_type(self):
        """Test bool type validation."""
        schema = {"active": {"type": "bool"}}

        # Valid
        validated, errors = validate_params({"active": True}, schema)
        assert errors == []

        validated, errors = validate_params({"active": False}, schema)
        assert errors == []

        # Invalid
        _validated, errors = validate_params({"active": "true"}, schema)
        assert len(errors) == 1

    def test_validate_list_type(self):
        """Test list type validation."""
        schema = {"tags": {"type": "list"}}

        # Valid
        validated, errors = validate_params({"tags": ["a", "b"]}, schema)
        assert errors == []

        # Invalid
        _validated, errors = validate_params({"tags": "a,b"}, schema)
        assert len(errors) == 1

    def test_validate_dict_type(self):
        """Test dict type validation."""
        schema = {"data": {"type": "dict"}}

        # Valid
        validated, errors = validate_params({"data": {"key": "value"}}, schema)
        assert errors == []

        # Invalid
        _validated, errors = validate_params({"data": [1, 2, 3]}, schema)
        assert len(errors) == 1

    def test_validate_any_type(self):
        """Test any type accepts all values."""
        schema = {"data": {"type": "any"}}

        validated, errors = validate_params({"data": "string"}, schema)
        assert errors == []

        validated, errors = validate_params({"data": 123}, schema)
        assert errors == []

        _validated, errors = validate_params({"data": [1, 2, 3]}, schema)
        assert errors == []

    def test_validate_enum_type(self):
        """Test enum type validation."""
        schema = {"role": {"type": "enum", "values": ["admin", "user", "guest"]}}

        # Valid
        validated, errors = validate_params({"role": "admin"}, schema)
        assert errors == []

        # Invalid
        _validated, errors = validate_params({"role": "superuser"}, schema)
        assert len(errors) == 1
        assert errors[0]["type"] == "enum"

    def test_validate_min_max_number(self):
        """Test min/max validation for numbers."""
        schema = {"age": {"type": "int", "min": 0, "max": 150}}

        # Valid
        validated, errors = validate_params({"age": 25}, schema)
        assert errors == []

        # Too low
        validated, errors = validate_params({"age": -1}, schema)
        assert len(errors) == 1
        assert errors[0]["type"] == "min"

        # Too high
        _validated, errors = validate_params({"age": 200}, schema)
        assert len(errors) == 1
        assert errors[0]["type"] == "max"

    def test_validate_min_max_string(self):
        """Test min/max validation for string length."""
        schema = {"name": {"type": "string", "min": 2, "max": 10}}

        # Valid
        validated, errors = validate_params({"name": "John"}, schema)
        assert errors == []

        # Too short
        validated, errors = validate_params({"name": "J"}, schema)
        assert len(errors) == 1
        assert errors[0]["type"] == "minLength"

        # Too long
        _validated, errors = validate_params({"name": "VeryLongName"}, schema)
        assert len(errors) == 1
        assert errors[0]["type"] == "maxLength"

    def test_validate_min_max_list(self):
        """Test min/max validation for list length."""
        schema = {"tags": {"type": "list", "min": 1, "max": 5}}

        # Valid
        validated, errors = validate_params({"tags": ["a", "b"]}, schema)
        assert errors == []

        # Too few
        validated, errors = validate_params({"tags": []}, schema)
        assert len(errors) == 1
        assert errors[0]["type"] == "minItems"

        # Too many
        _validated, errors = validate_params({"tags": ["a", "b", "c", "d", "e", "f"]}, schema)
        assert len(errors) == 1
        assert errors[0]["type"] == "maxItems"

    def test_validate_pattern(self):
        """Test pattern validation for strings."""
        schema = {"email": {"type": "string", "pattern": r"^[\w.-]+@[\w.-]+\.\w+$"}}

        # Valid
        validated, errors = validate_params({"email": "test@example.com"}, schema)
        assert errors == []

        # Invalid
        _validated, errors = validate_params({"email": "invalid-email"}, schema)
        assert len(errors) == 1
        assert errors[0]["type"] == "pattern"

    def test_validate_nested_object(self):
        """Test nested object validation."""
        schema = {
            "address": {
                "type": "object",
                "props": {
                    "city": {"type": "string"},
                    "zip": {"type": "string", "pattern": r"^\d{5}$"},
                },
            }
        }

        # Valid
        validated, errors = validate_params(
            {"address": {"city": "NYC", "zip": "10001"}},
            schema,
        )
        assert errors == []
        assert validated["address"]["city"] == "NYC"

        # Invalid nested field
        validated, errors = validate_params(
            {"address": {"city": "NYC", "zip": "invalid"}},
            schema,
        )
        assert len(errors) == 1
        assert "address.zip" in errors[0]["field"]

    def test_validate_nested_required_missing(self):
        """Test nested required field missing."""
        schema = {
            "address": {
                "type": "object",
                "props": {
                    "city": {"type": "string"},
                    "zip": {"type": "string"},
                },
            }
        }

        _validated, errors = validate_params(
            {"address": {"city": "NYC"}},  # zip missing
            schema,
        )
        assert len(errors) == 1
        assert "address.zip" in errors[0]["field"]

    def test_validate_string_shorthand(self):
        """Test string shorthand for type definition."""
        schema = {"name": "string", "age": "int"}

        _validated, errors = validate_params({"name": "John", "age": 25}, schema)
        assert errors == []

    def test_validate_custom_validator(self):
        """Test custom validator function."""

        def validate_even(value, params, path):
            if value % 2 != 0:
                return ["Value must be even"]
            return []

        schema = {"number": {"type": "int", "custom": validate_even}}

        # Valid
        validated, errors = validate_params({"number": 4}, schema)
        assert errors == []

        # Invalid
        _validated, errors = validate_params({"number": 3}, schema)
        assert len(errors) == 1
        assert errors[0]["type"] == "custom"

    def test_validate_multiple_errors(self):
        """Test multiple validation errors."""
        schema = {
            "name": {"type": "string", "min": 2},
            "age": {"type": "int", "min": 0},
        }

        _validated, errors = validate_params({"name": "J", "age": -5}, schema)
        assert len(errors) == 2


class TestValidatorMiddlewareInit:
    """Tests for ValidatorMiddleware initialization."""

    def test_init_defaults(self):
        """Test middleware initializes with defaults."""
        mw = ValidatorMiddleware()

        assert mw.mode == "error"
        assert mw.logger is not None

    def test_init_custom_mode(self):
        """Test middleware with custom mode."""
        mw = ValidatorMiddleware(mode="remove")

        assert mw.mode == "remove"

    def test_init_custom_logger(self):
        """Test middleware with custom logger."""
        custom_logger = MagicMock()
        mw = ValidatorMiddleware(logger=custom_logger)

        assert mw.logger is custom_logger

    def test_repr(self):
        """Test __repr__ returns readable string."""
        mw = ValidatorMiddleware(mode="error")

        assert repr(mw) == "ValidatorMiddleware(mode='error')"


class TestValidatorMiddlewareLocalAction:
    """Tests for ValidatorMiddleware local_action hook."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return ValidatorMiddleware(mode="error")

    @pytest.mark.asyncio
    async def test_no_schema_passthrough(self, middleware):
        """Test handler returned as-is when no schema."""

        async def handler(ctx):
            return "result"

        action = MagicMock()
        action.params = None

        wrapped = await middleware.local_action(handler, action)

        assert wrapped is handler

    @pytest.mark.asyncio
    async def test_valid_params(self, middleware):
        """Test valid params pass through."""

        async def handler(ctx):
            return ctx.params["name"]

        action = MagicMock()
        action.name = "test"
        action.params = {"name": {"type": "string"}}

        ctx = MagicMock()
        ctx.params = {"name": "John"}

        wrapped = await middleware.local_action(handler, action)
        result = await wrapped(ctx)

        assert result == "John"

    @pytest.mark.asyncio
    async def test_invalid_params_error_mode(self, middleware):
        """Test invalid params raise ValidationError in error mode."""

        async def handler(ctx):
            return "result"

        action = MagicMock()
        action.name = "test"
        action.params = {"name": {"type": "string"}}

        ctx = MagicMock()
        ctx.params = {"name": 123}  # Invalid type

        wrapped = await middleware.local_action(handler, action)

        with pytest.raises(ValidationError) as exc_info:
            await wrapped(ctx)

        assert "Validation failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_invalid_params_remove_mode(self):
        """Test invalid params removed in remove mode."""
        middleware = ValidatorMiddleware(mode="remove")

        call_count = 0

        async def handler(ctx):
            nonlocal call_count
            call_count += 1
            return "result"

        action = MagicMock()
        action.name = "test"
        action.params = {"name": {"type": "string", "optional": True}}

        ctx = MagicMock()
        ctx.params = {"name": 123}  # Invalid type - will be removed

        wrapped = await middleware.local_action(handler, action)
        result = await wrapped(ctx)

        # Handler was called despite validation errors
        assert call_count == 1
        assert result == "result"

    @pytest.mark.asyncio
    async def test_missing_required_field(self, middleware):
        """Test missing required field raises error."""

        async def handler(ctx):
            return "result"

        action = MagicMock()
        action.name = "test"
        action.params = {"name": {"type": "string"}}

        ctx = MagicMock()
        ctx.params = {}  # Missing required field

        wrapped = await middleware.local_action(handler, action)

        with pytest.raises(ValidationError):
            await wrapped(ctx)

    @pytest.mark.asyncio
    async def test_params_updated_with_defaults(self, middleware):
        """Test params updated with default values."""
        received_params = {}

        async def handler(ctx):
            received_params.update(ctx.params)
            return "result"

        action = MagicMock()
        action.name = "test"
        action.params = {
            "name": {"type": "string"},
            "status": {"type": "string", "default": "active"},
        }

        # Use spec to prevent auto-creation of _params
        ctx = MagicMock(spec=["params"])
        ctx.params = {"name": "John"}

        wrapped = await middleware.local_action(handler, action)
        await wrapped(ctx)

        # Default value should be added
        assert received_params.get("status") == "active"


class TestValidatorMiddlewareRemoteAction:
    """Tests for ValidatorMiddleware remote_action hook."""

    @pytest.mark.asyncio
    async def test_remote_action_validates(self):
        """Test remote_action also validates params."""
        middleware = ValidatorMiddleware()

        async def handler(ctx):
            return "result"

        action = MagicMock()
        action.name = "test"
        action.params = {"name": {"type": "string"}}

        ctx = MagicMock()
        ctx.params = {"name": 123}

        wrapped = await middleware.remote_action(handler, action)

        with pytest.raises(ValidationError):
            await wrapped(ctx)


class TestValidatorIntegration:
    """Integration-style tests for Validator."""

    @pytest.mark.asyncio
    async def test_full_schema_validation(self):
        """Test full schema validation with nested objects."""
        middleware = ValidatorMiddleware()

        async def create_user(ctx):
            return {"id": 1, **ctx.params}

        action = MagicMock()
        action.name = "users.create"
        action.params = {
            "name": {"type": "string", "min": 2, "max": 50},
            "email": {"type": "string", "pattern": r"^[\w.-]+@[\w.-]+\.\w+$"},
            "age": {"type": "int", "min": 0, "max": 150, "optional": True},
            "role": {"type": "enum", "values": ["admin", "user", "guest"]},
            "address": {
                "type": "object",
                "props": {
                    "city": {"type": "string"},
                    "zip": {"type": "string"},
                },
                "optional": True,
            },
        }

        # Valid request
        ctx = MagicMock()
        ctx.params = {
            "name": "John Doe",
            "email": "john@example.com",
            "role": "user",
        }

        wrapped = await middleware.local_action(create_user, action)
        result = await wrapped(ctx)

        assert result["name"] == "John Doe"

    @pytest.mark.asyncio
    async def test_validation_error_details(self):
        """Test validation error contains details."""
        middleware = ValidatorMiddleware()

        async def handler(ctx):
            return "result"

        action = MagicMock()
        action.name = "test"
        action.params = {
            "name": {"type": "string", "min": 3},
            "email": {"type": "string", "pattern": r"^[\w]+@[\w]+\.\w+$"},
        }

        ctx = MagicMock()
        ctx.params = {
            "name": "Jo",  # Too short
            "email": "invalid",  # Bad pattern
        }

        wrapped = await middleware.local_action(handler, action)

        with pytest.raises(ValidationError) as exc_info:
            await wrapped(ctx)

        error = exc_info.value
        assert error.data is not None
        assert len(error.data) == 2  # Two validation errors

    @pytest.mark.asyncio
    async def test_array_of_objects_validation(self):
        """Test validation of array containing objects."""
        schema = {
            "items": {
                "type": "list",
                "min": 1,
                "items": {
                    "type": "object",
                    "props": {
                        "id": {"type": "int"},
                        "name": {"type": "string"},
                    },
                },
            }
        }

        # Valid
        params = {
            "items": [
                {"id": 1, "name": "Item 1"},
                {"id": 2, "name": "Item 2"},
            ]
        }
        validated, errors = validate_params(params, schema)
        assert errors == []

        # Invalid - missing required field in nested object
        params = {
            "items": [
                {"id": 1},  # Missing name
            ]
        }
        _validated, errors = validate_params(params, schema)
        assert len(errors) == 1
        assert "items[0].name" in errors[0]["field"]
