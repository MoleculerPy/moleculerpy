"""Unit tests for the pluggable validator system (PRD-013, v0.14.9).

Tests cover BaseValidator ABC, DefaultValidator, resolve_validator,
backward compatibility imports.
"""

from __future__ import annotations

import pytest

from moleculerpy.errors import ValidationError
from moleculerpy.validators import BaseValidator, DefaultValidator, resolve_validator
from moleculerpy.validators.base import CheckFunction

# ---------------------------------------------------------------------------
# 1. BaseValidator is abstract — can't instantiate
# ---------------------------------------------------------------------------


def test_base_validator_is_abstract():
    with pytest.raises(TypeError):
        BaseValidator()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# 2-12. DefaultValidator tests
# ---------------------------------------------------------------------------


class TestDefaultValidatorValidate:
    """Tests for DefaultValidator.validate()."""

    def setup_method(self):
        self.v = DefaultValidator()

    # 2. Valid params pass
    def test_valid_params_pass(self):
        schema = {"name": {"type": "string", "required": True}}
        result = self.v.validate({"name": "Alice"}, schema)
        assert result is True

    # 3. Missing required field raises ValidationError
    def test_missing_required_field_raises(self):
        schema = {"name": {"type": "string", "required": True}}
        with pytest.raises(ValidationError) as exc_info:
            self.v.validate({}, schema)
        assert "name" in str(exc_info.value)

    # 4. Wrong type raises ValidationError
    def test_wrong_type_raises(self):
        schema = {"age": {"type": "number"}}
        with pytest.raises(ValidationError):
            self.v.validate({"age": "not-a-number"}, schema)

    # 5. String minLength / maxLength
    def test_string_min_length(self):
        schema = {"code": {"type": "string", "minLength": 3}}
        with pytest.raises(ValidationError):
            self.v.validate({"code": "ab"}, schema)

    def test_string_max_length(self):
        schema = {"code": {"type": "string", "maxLength": 5}}
        with pytest.raises(ValidationError):
            self.v.validate({"code": "toolong"}, schema)

    def test_string_length_valid(self):
        schema = {"code": {"type": "string", "minLength": 2, "maxLength": 6}}
        assert self.v.validate({"code": "abc"}, schema) is True

    # 6. Number min / max
    def test_number_min(self):
        schema = {"age": {"type": "number", "min": 18}}
        with pytest.raises(ValidationError):
            self.v.validate({"age": 17}, schema)

    def test_number_max(self):
        schema = {"age": {"type": "number", "max": 65}}
        with pytest.raises(ValidationError):
            self.v.validate({"age": 66}, schema)

    def test_number_range_valid(self):
        schema = {"age": {"type": "number", "min": 0, "max": 120}}
        assert self.v.validate({"age": 30}, schema) is True

    # 7. Pattern regex
    def test_pattern_valid(self):
        schema = {"email": {"type": "string", "pattern": r"^[\w.-]+@[\w.-]+\.\w+$"}}
        assert self.v.validate({"email": "user@example.com"}, schema) is True

    def test_pattern_invalid(self):
        schema = {"email": {"type": "string", "pattern": r"^[\w.-]+@[\w.-]+\.\w+$"}}
        with pytest.raises(ValidationError):
            self.v.validate({"email": "not-an-email"}, schema)

    # 8. Enum values
    def test_enum_valid(self):
        schema = {"role": {"type": "string", "enum": ["admin", "user", "guest"]}}
        assert self.v.validate({"role": "admin"}, schema) is True

    def test_enum_invalid(self):
        schema = {"role": {"type": "string", "enum": ["admin", "user", "guest"]}}
        with pytest.raises(ValidationError):
            self.v.validate({"role": "superadmin"}, schema)

    # 9. Optional fields — absent optional field passes
    def test_optional_field_absent_passes(self):
        schema = {"name": {"type": "string", "required": True}, "nickname": {"type": "string"}}
        # nickname is not in params and not required — should pass
        assert self.v.validate({"name": "Alice"}, schema) is True

    # 10. Nested object (props) — use validate_params with dict schema for nested
    def test_nested_object_valid(self):
        # We test the underlying validate_params behaviour via DefaultValidator
        schema = {
            "user": {
                "type": "object",
                "required": True,
            }
        }
        assert self.v.validate({"user": {"id": 1, "name": "Alice"}}, schema) is True

    def test_nested_object_wrong_type_raises(self):
        schema = {"user": {"type": "object", "required": True}}
        with pytest.raises(ValidationError):
            self.v.validate({"user": "not-an-object"}, schema)


# ---------------------------------------------------------------------------
# 11-12. DefaultValidator.compile()
# ---------------------------------------------------------------------------


class TestDefaultValidatorCompile:
    def setup_method(self):
        self.v = DefaultValidator()

    # 11. compile() returns callable
    def test_compile_returns_callable(self):
        schema = {"name": {"type": "string"}}
        checker = self.v.compile(schema)
        assert callable(checker)

    # 12. compiled checker works
    def test_compiled_checker_works(self):
        schema = {"name": {"type": "string", "required": True}}
        checker = self.v.compile(schema)
        assert checker({"name": "Bob"}) is True

    def test_compiled_checker_raises_on_invalid(self):
        schema = {"name": {"type": "string", "required": True}}
        checker = self.v.compile(schema)
        with pytest.raises(ValidationError):
            checker({})


# ---------------------------------------------------------------------------
# 13-18. resolve_validator()
# ---------------------------------------------------------------------------


class TestResolveValidator:
    # 13. "default" → DefaultValidator
    def test_resolve_default_string(self):
        v = resolve_validator("default")
        assert isinstance(v, DefaultValidator)

    # 14. "none" → None
    def test_resolve_none_string(self):
        v = resolve_validator("none")
        assert v is None

    # 15. False → None
    def test_resolve_false(self):
        v = resolve_validator(False)
        assert v is None

    # 16. Class (subclass of BaseValidator) → instance
    def test_resolve_class(self):
        v = resolve_validator(DefaultValidator)
        assert isinstance(v, DefaultValidator)

    # 17. Instance → passthrough
    def test_resolve_instance_passthrough(self):
        instance = DefaultValidator()
        v = resolve_validator(instance)
        assert v is instance

    # 18. Unknown string → ValueError/TypeError
    def test_resolve_unknown_raises(self):
        with pytest.raises((ValueError, TypeError)):
            resolve_validator("unknown_validator")


# ---------------------------------------------------------------------------
# 19. Custom validator class works
# ---------------------------------------------------------------------------


class AlwaysPassValidator(BaseValidator):
    """Custom validator that always passes without checking."""

    def compile(self, schema: dict) -> CheckFunction:
        def checker(params: dict) -> bool:
            return True

        return checker

    def validate(self, params: dict, schema: dict) -> bool:
        return True


class AlwaysFailValidator(BaseValidator):
    """Custom validator that always raises ValidationError."""

    def compile(self, schema: dict) -> CheckFunction:
        def checker(params: dict) -> bool:
            raise ValidationError("always fails", field="*")

        return checker

    def validate(self, params: dict, schema: dict) -> bool:
        raise ValidationError("always fails", field="*")


def test_custom_validator_always_pass():
    v = AlwaysPassValidator()
    result = v.validate({}, {"name": {"type": "string", "required": True}})
    assert result is True


def test_custom_validator_always_fail():
    v = AlwaysFailValidator()
    with pytest.raises(ValidationError):
        v.validate({"name": "Alice"}, {})


def test_resolve_custom_validator_class():
    v = resolve_validator(AlwaysPassValidator)
    assert isinstance(v, AlwaysPassValidator)


# ---------------------------------------------------------------------------
# 20. Backward compat: import from moleculerpy.validator still works
# ---------------------------------------------------------------------------


def test_backward_compat_moleculerpy_validator():
    from moleculerpy.validator import ValidationError as CompatValidationError
    from moleculerpy.validator import validate_param_rule, validate_params, validate_type

    assert CompatValidationError is ValidationError
    assert callable(validate_params)
    assert callable(validate_param_rule)
    assert callable(validate_type)


# ---------------------------------------------------------------------------
# 21. Backward compat: import from moleculerpy.middleware.validator still works
# ---------------------------------------------------------------------------


def test_backward_compat_middleware_validator():
    # The middleware validator module should be importable
    import moleculerpy.middleware.validator as mw_validator

    assert mw_validator is not None
