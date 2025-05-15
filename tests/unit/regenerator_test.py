"""Tests for Error Regenerator.

Phase 3C: Architecture - Extensible error restoration pattern.
"""

from typing import Any
from unittest.mock import Mock

import pytest

from moleculerpy.errors import (
    MoleculerError,
    Regenerator,
    RequestTimeoutError,
    ServiceNotFoundError,
    ValidationError,
    recreate_error,
    resolve_regenerator,
)


@pytest.fixture
def mock_broker():
    """Create a mock broker with node ID."""
    broker = Mock()
    broker.nodeID = "test-node-1"
    return broker


@pytest.fixture
def regenerator(mock_broker):
    """Create an initialized regenerator."""
    regen = Regenerator()
    regen.init(mock_broker)
    return regen


class TestRegeneratorInit:
    """Tests for Regenerator initialization."""

    def test_init_sets_broker(self, mock_broker) -> None:
        """init() should set broker reference."""
        regen = Regenerator()
        regen.init(mock_broker)
        assert regen.broker is mock_broker
        assert regen.node_id == "test-node-1"

    def test_init_without_broker(self) -> None:
        """Regenerator should work without broker init."""
        regen = Regenerator()
        assert regen.broker is None
        assert regen.node_id is None


class TestExtractPlainError:
    """Tests for extract_plain_error()."""

    def test_extract_moleculer_error(self, regenerator) -> None:
        """extract_plain_error should serialize MoleculerError."""
        err = ServiceNotFoundError("math.add", node_id="node-2")
        plain = regenerator.extract_plain_error(err)

        assert plain["name"] == "ServiceNotFoundError"
        assert plain["message"] == "Service 'math.add' not found"
        assert plain["code"] == 404
        assert plain["type"] == "SERVICE_NOT_FOUND"
        assert plain["retryable"] is True
        assert plain["data"]["action"] == "math.add"
        assert plain["nodeID"] == "node-2"  # From error data
        assert "stack" in plain

    def test_extract_moleculer_error_uses_broker_node_id(self, regenerator) -> None:
        """extract_plain_error should use broker nodeID if not in error."""
        err = ValidationError("Invalid input", field="name")
        plain = regenerator.extract_plain_error(err)

        # ValidationError doesn't have nodeID in data, should use broker's
        assert plain["nodeID"] == "test-node-1"

    def test_extract_non_moleculer_error(self, regenerator) -> None:
        """extract_plain_error should wrap non-Moleculer errors."""
        err = ValueError("Something went wrong")
        plain = regenerator.extract_plain_error(err)

        assert plain["name"] == "ValueError"
        assert plain["message"] == "Something went wrong"
        assert plain["code"] == 500
        assert plain["type"] == "INTERNAL_ERROR"
        assert plain["retryable"] is False
        assert plain["nodeID"] == "test-node-1"
        assert "stack" in plain


class TestRestore:
    """Tests for restore()."""

    def test_restore_service_not_found_error(self, regenerator) -> None:
        """restore should reconstruct ServiceNotFoundError."""
        plain = {
            "name": "ServiceNotFoundError",
            "message": "Service 'math.add' not found",
            "code": 404,
            "type": "SERVICE_NOT_FOUND",
            "data": {"action": "math.add", "nodeID": "node-2"},
            "retryable": True,
        }

        err = regenerator.restore(plain, {"sender": "node-3"})

        assert isinstance(err, ServiceNotFoundError)
        assert err.code == 404
        assert err.action_name == "math.add"
        assert err.retryable is True

    def test_restore_validation_error(self, regenerator) -> None:
        """restore should reconstruct ValidationError."""
        plain = {
            "name": "ValidationError",
            "message": "Field 'email' is required",
            "code": 422,
            "type": "VALIDATION_ERROR",
            "data": {"field": "email"},
            "retryable": False,
        }

        err = regenerator.restore(plain)

        assert isinstance(err, ValidationError)
        assert err.code == 422
        assert err.field == "email"
        assert err.retryable is False

    def test_restore_unknown_error_creates_default(self, regenerator) -> None:
        """restore should create default error for unknown types."""
        plain = {
            "name": "CustomServiceError",
            "message": "Something custom happened",
            "code": 418,
            "type": "TEAPOT",
            "retryable": True,
            "data": {"custom": "data"},
        }

        err = regenerator.restore(plain)

        # Unknown error becomes generic MoleculerError
        assert isinstance(err, MoleculerError)
        assert err.code == 418
        assert err.type == "TEAPOT"
        assert err.retryable is True

    def test_restore_sets_node_id_from_payload_sender(self, regenerator) -> None:
        """restore should set nodeID from payload.sender if not in error."""
        plain = {
            "name": "MoleculerError",
            "message": "Test error",
            "code": 500,
            "type": "TEST",
            "retryable": False,
            "data": {},
        }

        err = regenerator.restore(plain, {"sender": "remote-node"})

        assert hasattr(err, "nodeID")
        assert err.nodeID == "remote-node"

    def test_restore_preserves_stack_trace(self, regenerator) -> None:
        """restore should preserve stack trace as remote_stack."""
        plain = {
            "name": "ServiceNotFoundError",
            "message": "Not found",
            "code": 404,
            "type": "SERVICE_NOT_FOUND",
            "data": {"action": "test"},
            "retryable": True,
            "stack": "Traceback:\n  File test.py line 10\n  ...",
        }

        err = regenerator.restore(plain)

        assert hasattr(err, "remote_stack")
        assert "Traceback" in err.remote_stack


class TestRestoreCustomError:
    """Tests for custom error hook."""

    def test_restore_custom_error_returns_none_by_default(self, regenerator) -> None:
        """Default restore_custom_error() should return None."""
        result = regenerator.restore_custom_error({"name": "Test"}, {})
        assert result is None

    def test_custom_regenerator_can_handle_custom_errors(self, mock_broker) -> None:
        """Subclass can override restore_custom_error() for custom types."""

        class MyCustomError(Exception):
            def __init__(self, message: str, severity: str):
                super().__init__(message)
                self.severity = severity

        class CustomRegenerator(Regenerator):
            def restore_custom_error(
                self, plain_error: dict[str, Any], payload: dict[str, Any]
            ) -> Exception | None:
                if plain_error.get("name") == "MyCustomError":
                    return MyCustomError(
                        plain_error.get("message", ""),
                        plain_error.get("data", {}).get("severity", "low"),
                    )
                return None

        regen = CustomRegenerator()
        regen.init(mock_broker)

        plain = {
            "name": "MyCustomError",
            "message": "Custom error occurred",
            "code": 500,
            "data": {"severity": "critical"},
        }

        err = regen.restore(plain)

        assert isinstance(err, MyCustomError)
        assert err.severity == "critical"


class TestRecreateError:
    """Tests for recreate_error() function."""

    def test_recreate_known_error(self) -> None:
        """recreate_error should restore known error types."""
        plain = {
            "name": "RequestTimeoutError",
            "message": "Timeout",
            "code": 504,
            "type": "REQUEST_TIMEOUT",
            "data": {"action": "slow.action", "timeout": 5.0, "elapsed": 5.5},
            "retryable": True,
        }

        err = recreate_error(plain)

        assert isinstance(err, RequestTimeoutError)
        assert err.action_name == "slow.action"
        assert err.timeout == 5.0

    def test_recreate_unknown_returns_base_error(self) -> None:
        """recreate_error should return MoleculerError for unknown types."""
        plain = {
            "name": "UnknownError",
            "message": "Unknown",
            "code": 500,
            "type": "UNKNOWN",
            "data": {},
            "retryable": False,
        }

        err = recreate_error(plain)

        # Falls back to base MoleculerError
        assert isinstance(err, MoleculerError)
        assert err.message == "Unknown"


class TestResolveRegenerator:
    """Tests for resolve_regenerator() function."""

    def test_resolve_none_returns_default(self) -> None:
        """resolve_regenerator(None) should return default Regenerator."""
        regen = resolve_regenerator(None)
        assert isinstance(regen, Regenerator)

    def test_resolve_existing_returns_same(self) -> None:
        """resolve_regenerator should return existing instance."""
        existing = Regenerator()
        result = resolve_regenerator(existing)
        assert result is existing

    def test_resolve_custom_class_returns_same(self) -> None:
        """resolve_regenerator should work with custom subclass."""

        class CustomRegenerator(Regenerator):
            pass

        custom = CustomRegenerator()
        result = resolve_regenerator(custom)
        assert result is custom
        assert isinstance(result, Regenerator)


class TestRoundTrip:
    """Tests for full serialize/deserialize round-trip."""

    def test_round_trip_service_not_found(self, regenerator) -> None:
        """Full round-trip should preserve error data."""
        original = ServiceNotFoundError("users.get", node_id="node-1")

        # Serialize
        plain = regenerator.extract_plain_error(original)

        # Deserialize (simulating receive on another node)
        restored = regenerator.restore(plain, {"sender": "node-1"})

        assert isinstance(restored, ServiceNotFoundError)
        assert restored.action_name == original.action_name
        assert restored.code == original.code
        assert restored.type == original.type
        assert restored.retryable == original.retryable

    def test_round_trip_validation_error(self, regenerator) -> None:
        """Full round-trip for ValidationError."""
        original = ValidationError(
            "Age must be positive",
            field="age",
            type="min_value",
            expected=0,
            got=-5,
        )

        plain = regenerator.extract_plain_error(original)
        restored = regenerator.restore(plain)

        assert isinstance(restored, ValidationError)
        assert restored.field == "age"
        assert restored.message == original.message

    def test_round_trip_preserves_custom_data(self, regenerator) -> None:
        """Round-trip should preserve custom data in error."""
        original = MoleculerError(
            message="Custom error",
            code=500,
            error_type="CUSTOM",
            data={"custom_field": "custom_value", "nested": {"a": 1}},
        )

        plain = regenerator.extract_plain_error(original)
        restored = regenerator.restore(plain)

        assert isinstance(restored, MoleculerError)
        assert restored.data["custom_field"] == "custom_value"
        assert restored.data["nested"]["a"] == 1
