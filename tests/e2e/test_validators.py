"""E2E tests for the pluggable validator system (PRD-013, v0.14.9).

Tests run with real ServiceBroker + MemoryTransporter, no mocks.
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action
from moleculerpy.errors import ValidationError
from moleculerpy.service import Service
from moleculerpy.settings import Settings
from moleculerpy.validators import BaseValidator, DefaultValidator
from moleculerpy.validators.base import CheckFunction

# ---------------------------------------------------------------------------
# Test services
# ---------------------------------------------------------------------------


class UserService(Service):
    """Service with validated actions for testing."""

    name = "users"

    def __init__(self):
        super().__init__(self.name)

    @action(
        params={
            "name": {"type": "string", "required": True},
            "age": {"type": "number", "min": 0},
        }
    )
    async def create(self, ctx) -> dict:
        # Use .get() so handler doesn't crash when validator is disabled
        return {"name": ctx.params.get("name"), "age": ctx.params.get("age")}

    @action(
        params={
            "id": {"type": "number", "required": True},
        }
    )
    async def get(self, ctx) -> dict:
        return {"id": ctx.params.get("id")}

    @action()
    async def ping(self, ctx) -> str:
        return "pong"


class CustomAlwaysPassValidator(BaseValidator):
    """Custom validator that never rejects params."""

    def compile(self, schema: dict) -> CheckFunction:
        def checker(params: dict) -> bool:
            return True

        return checker

    def validate(self, params: dict, schema: dict) -> bool:
        return True


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def broker_default():
    """Broker with default validator (explicit)."""
    settings = Settings(transporter="memory://", validator="default")
    broker = ServiceBroker(id="validator-node", settings=settings)
    await broker.register(UserService())
    await broker.start()
    yield broker
    await broker.stop()


@pytest_asyncio.fixture
async def broker_none_validator():
    """Broker with validator disabled."""
    settings = Settings(transporter="memory://", validator="none")
    broker = ServiceBroker(id="novalidator-node", settings=settings)
    await broker.register(UserService())
    await broker.start()
    yield broker
    await broker.stop()


@pytest_asyncio.fixture
async def broker_custom_validator():
    """Broker with custom validator class."""
    settings = Settings(transporter="memory://", validator=CustomAlwaysPassValidator)
    broker = ServiceBroker(id="custom-validator-node", settings=settings)
    await broker.register(UserService())
    await broker.start()
    yield broker
    await broker.stop()


@pytest_asyncio.fixture
async def broker_no_explicit_validator():
    """Broker created without explicit validator setting (backward compat)."""
    settings = Settings(transporter="memory://")
    broker = ServiceBroker(id="default-compat-node", settings=settings)
    await broker.register(UserService())
    await broker.start()
    yield broker
    await broker.stop()


# ---------------------------------------------------------------------------
# E2E tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_default_validator_valid_params_pass(broker_default):
    """Broker with default validator — valid params pass without error."""
    result = await broker_default.call("users.create", {"name": "Alice", "age": 30})
    assert result["name"] == "Alice"
    assert result["age"] == 30


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_default_validator_invalid_params_raise(broker_default):
    """Broker with default validator — invalid params raise ValidationError."""
    with pytest.raises(ValidationError):
        await broker_default.call("users.create", {})  # missing required 'name'


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_default_validator_wrong_type_raises(broker_default):
    """Broker with default validator — wrong type raises ValidationError."""
    with pytest.raises(ValidationError):
        await broker_default.call("users.create", {"name": "Alice", "age": "not-a-number"})


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_default_validator_number_min_raises(broker_default):
    """Broker with default validator — number below min raises ValidationError."""
    with pytest.raises(ValidationError):
        await broker_default.call("users.create", {"name": "Alice", "age": -1})


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_none_validator_any_params_pass(broker_none_validator):
    """Broker with validator='none' — no validation, any params pass."""
    # Missing required field should NOT raise when validator is disabled
    result = await broker_none_validator.call("users.create", {})
    # Handler runs with empty params — no ValidationError raised
    assert result is not None


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_none_validator_wrong_type_passes(broker_none_validator):
    """Broker with validator='none' — wrong types pass through."""
    result = await broker_none_validator.call("users.get", {"id": "not-a-number"})
    assert result is not None


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_custom_validator_class(broker_custom_validator):
    """Broker with custom validator class — validator is used and always passes."""
    # Should pass even with missing required fields (custom validator ignores schema)
    result = await broker_custom_validator.call("users.create", {})
    assert result is not None


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_custom_validator_is_initialized(broker_custom_validator):
    """Custom validator receives broker reference via init()."""
    assert broker_custom_validator._validator is not None
    assert broker_custom_validator._validator.broker is broker_custom_validator


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_backward_compat_no_explicit_validator(broker_no_explicit_validator):
    """Broker without explicit validator setting works same as 'default'."""
    # Valid call passes
    result = await broker_no_explicit_validator.call("users.create", {"name": "Bob", "age": 25})
    assert result["name"] == "Bob"

    # Invalid call raises
    with pytest.raises(ValidationError):
        await broker_no_explicit_validator.call("users.create", {})


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_action_without_params_schema_always_passes(broker_default):
    """Actions without params schema are never validated."""
    result = await broker_default.call("users.ping", {"anything": True})
    assert result == "pong"


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_validator_false_disables_validation():
    """Settings(validator=False) disables validation (same as 'none')."""
    settings = Settings(transporter="memory://", validator=False)
    broker = ServiceBroker(id="false-validator-node", settings=settings)
    await broker.register(UserService())
    await broker.start()
    try:
        # Should not raise even with missing required params
        result = await broker.call("users.create", {})
        assert result is not None
    finally:
        await broker.stop()


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_validator_instance_passed_directly():
    """Passing a validator instance directly to Settings works."""
    instance = DefaultValidator()
    settings = Settings(transporter="memory://", validator=instance)
    broker = ServiceBroker(id="instance-validator-node", settings=settings)
    await broker.register(UserService())
    await broker.start()
    try:
        result = await broker.call("users.create", {"name": "Charlie", "age": 20})
        assert result["name"] == "Charlie"

        with pytest.raises(ValidationError):
            await broker.call("users.create", {})
    finally:
        await broker.stop()
