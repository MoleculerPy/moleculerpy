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


# ---------------------------------------------------------------------------
# 22-30. middleware() method coverage
# ---------------------------------------------------------------------------


class ConcreteValidator(BaseValidator):
    """Concrete validator for testing middleware hooks."""

    def compile(self, schema: dict) -> CheckFunction:
        def checker(params: dict) -> bool:
            for key, _rule in schema.items():
                if key not in params:
                    raise ValidationError(f"Missing: {key}")
            return True

        return checker

    def validate(self, params: dict, schema: dict) -> bool:
        return self.compile(schema)(params)


def test_middleware_returns_dict_with_hooks():
    """middleware() returns dict with local_action and local_event."""
    v = ConcreteValidator()
    mw = v.middleware()
    assert "local_action" in mw
    assert "local_event" in mw
    assert callable(mw["local_action"])
    assert callable(mw["local_event"])


@pytest.mark.asyncio
async def test_middleware_local_action_with_schema():
    """local_action wraps handler when params_schema exists."""
    v = ConcreteValidator()
    mw = v.middleware()

    class FakeAction:
        params_schema = {"name": "string"}

    call_count = 0

    async def handler(ctx):
        nonlocal call_count
        call_count += 1
        return "ok"

    wrapped = await mw["local_action"](handler, FakeAction())
    assert wrapped is not handler  # wrapped

    class FakeCtx:
        params = {"name": "Alice"}

    result = await wrapped(FakeCtx())
    assert result == "ok"
    assert call_count == 1


@pytest.mark.asyncio
async def test_middleware_local_action_without_schema():
    """local_action returns handler unchanged when no schema."""
    v = ConcreteValidator()
    mw = v.middleware()

    class FakeAction:
        params_schema = None
        params = None

    async def handler(ctx):
        return "ok"

    wrapped = await mw["local_action"](handler, FakeAction())
    assert wrapped is handler  # not wrapped


@pytest.mark.asyncio
async def test_middleware_local_action_validation_fails():
    """local_action raises ValidationError on invalid params."""
    v = ConcreteValidator()
    mw = v.middleware()

    class FakeAction:
        params_schema = {"name": "string"}

    async def handler(ctx):
        return "ok"

    wrapped = await mw["local_action"](handler, FakeAction())

    class FakeCtx:
        params = {}  # missing 'name'

    with pytest.raises(ValidationError):
        await wrapped(FakeCtx())


@pytest.mark.asyncio
async def test_middleware_local_event_with_schema():
    """local_event wraps handler when params_schema exists."""
    v = ConcreteValidator()
    mw = v.middleware()

    class FakeEvent:
        params_schema = {"user_id": "string"}

    call_count = 0

    async def handler(ctx):
        nonlocal call_count
        call_count += 1

    wrapped = await mw["local_event"](handler, FakeEvent())
    assert wrapped is not handler

    class FakeCtx:
        params = {"user_id": "123"}

    await wrapped(FakeCtx())
    assert call_count == 1


@pytest.mark.asyncio
async def test_middleware_local_event_without_schema():
    """local_event returns handler unchanged when no schema."""
    v = ConcreteValidator()
    mw = v.middleware()

    class FakeEvent:
        params_schema = None
        params = None

    async def handler(ctx):
        pass

    wrapped = await mw["local_event"](handler, FakeEvent())
    assert wrapped is handler


@pytest.mark.asyncio
async def test_middleware_local_action_none_params():
    """local_action handles ctx.params=None gracefully."""
    v = ConcreteValidator()
    mw = v.middleware()

    class FakeAction:
        params_schema = {"name": "string"}

    async def handler(ctx):
        return "ok"

    wrapped = await mw["local_action"](handler, FakeAction())

    class FakeCtx:
        params = None

    with pytest.raises(ValidationError):
        await wrapped(FakeCtx())


def test_convert_schema_to_moleculer_default():
    """convert_schema_to_moleculer returns schema unchanged by default."""
    v = DefaultValidator()
    schema = {"name": "string", "age": "number"}
    result = v.convert_schema_to_moleculer(schema)
    assert result is schema


# ---------------------------------------------------------------------------
# 31-33. resolve_validator edge cases
# ---------------------------------------------------------------------------


def test_resolve_validator_registered_string():
    """resolve_validator with registered custom name."""
    from moleculerpy.validators import register_validator

    class MyValidator(BaseValidator):
        def compile(self, schema):
            return lambda p: True

        def validate(self, params, schema):
            return True

    register_validator("my_custom", MyValidator)
    v = resolve_validator("my_custom")
    assert isinstance(v, MyValidator)


def test_resolve_validator_case_insensitive():
    """resolve_validator is case-insensitive for strings."""
    v1 = resolve_validator("DEFAULT")
    v2 = resolve_validator("Default")
    assert isinstance(v1, DefaultValidator)
    assert isinstance(v2, DefaultValidator)


def test_resolve_validator_invalid_type():
    """resolve_validator raises TypeError for invalid types."""
    with pytest.raises(TypeError):
        resolve_validator(42)


def test_resolve_validator_unknown_string():
    """resolve_validator raises ValueError for unknown string."""
    with pytest.raises(ValueError, match="Unknown validator"):
        resolve_validator("nonexistent_validator")
