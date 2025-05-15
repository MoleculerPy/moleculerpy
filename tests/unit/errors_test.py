"""Tests for MoleculerError hierarchy.

Phase 3A: Core Protocol Compliance - Error hierarchy tests.
"""

from moleculerpy.errors import (
    ERROR_REGISTRY,
    BrokerDisconnectedError,
    BrokerOptionsError,
    GracefulStopTimeoutError,
    InvalidPacketDataError,
    MaxCallLevelError,
    MoleculerClientError,
    MoleculerError,
    MoleculerRetryableError,
    ProtocolVersionMismatchError,
    QueueIsFullError,
    RemoteCallError,
    RequestRejectedError,
    RequestTimeoutError,
    ServiceNotAvailableError,
    ServiceNotFoundError,
    ServiceSchemaError,
    ValidationError,
)


class TestMoleculerErrorBase:
    """Tests for MoleculerError base class."""

    def test_base_error_has_all_attributes(self) -> None:
        """Base error should have code, type, data, retryable attributes."""
        err = MoleculerError("test message", code=500, type="TEST_ERROR")
        assert err.message == "test message"
        assert err.code == 500
        assert err.type == "TEST_ERROR"
        assert err.data == {}
        assert err.retryable is False

    def test_base_error_with_data(self) -> None:
        """Base error should accept and store data dict."""
        data = {"action": "math.add", "nodeID": "node-1"}
        err = MoleculerError("test", data=data)
        assert err.data == data

    def test_base_error_default_values(self) -> None:
        """Base error should have sensible defaults."""
        err = MoleculerError("test")
        assert err.code == 500
        assert err.type == "MOLECULER_ERROR"
        assert err.retryable is False

    def test_error_is_exception(self) -> None:
        """MoleculerError should be a proper Exception."""
        err = MoleculerError("test message")
        assert isinstance(err, Exception)
        assert str(err) == "test message"


class TestMoleculerRetryableError:
    """Tests for MoleculerRetryableError base class."""

    def test_retryable_error_has_flag(self) -> None:
        """Retryable errors should have retryable=True."""
        err = MoleculerRetryableError("test")
        assert err.retryable is True

    def test_retryable_error_default_code(self) -> None:
        """Retryable errors default to 500."""
        err = MoleculerRetryableError("test")
        assert err.code == 500

    def test_retryable_inherits_from_base(self) -> None:
        """Retryable errors should inherit from MoleculerError."""
        err = MoleculerRetryableError("test")
        assert isinstance(err, MoleculerError)


class TestMoleculerClientError:
    """Tests for MoleculerClientError base class."""

    def test_client_error_not_retryable(self) -> None:
        """Client errors should not be retryable."""
        err = MoleculerClientError("bad request")
        assert err.retryable is False

    def test_client_error_default_code(self) -> None:
        """Client errors default to 400."""
        err = MoleculerClientError("bad request")
        assert err.code == 400

    def test_client_error_inherits_from_base(self) -> None:
        """Client errors should inherit from MoleculerError."""
        err = MoleculerClientError("test")
        assert isinstance(err, MoleculerError)


class TestConcreteRetryableErrors:
    """Tests for concrete retryable error types."""

    def test_service_not_found_error(self) -> None:
        """ServiceNotFoundError should have correct attributes."""
        err = ServiceNotFoundError("math.add", node_id="node-1")
        assert err.code == 404
        assert err.type == "SERVICE_NOT_FOUND"
        assert err.retryable is True
        assert err.data["action"] == "math.add"
        assert err.data["nodeID"] == "node-1"
        assert "math.add" in err.message

    def test_service_not_available_error(self) -> None:
        """ServiceNotAvailableError should have correct attributes."""
        err = ServiceNotAvailableError("users.get")
        assert err.code == 404
        assert err.type == "SERVICE_NOT_AVAILABLE"
        assert err.retryable is True
        assert "users.get" in err.message

    def test_request_timeout_error(self) -> None:
        """RequestTimeoutError should have correct attributes."""
        err = RequestTimeoutError("slow.action", timeout=5.0, elapsed=5.1)
        assert err.code == 504
        assert err.type == "REQUEST_TIMEOUT"
        assert err.retryable is True
        assert err.data["timeout"] == 5.0
        assert err.data["elapsed"] == 5.1
        assert "slow.action" in err.message
        assert "5.0s" in err.message or "5.10s" in err.message

    def test_request_rejected_error(self) -> None:
        """RequestRejectedError should have correct attributes."""
        err = RequestRejectedError("api.call", reason="rate limited")
        assert err.code == 503
        assert err.type == "REQUEST_REJECTED"
        assert err.retryable is True
        assert "rate limited" in err.message

    def test_queue_is_full_error(self) -> None:
        """QueueIsFullError should have correct attributes."""
        err = QueueIsFullError("heavy.task", queue_size=1000)
        assert err.code == 429
        assert err.type == "QUEUE_FULL"  # Matches Moleculer.js
        assert err.retryable is True
        assert err.data["queueSize"] == 1000


class TestConcreteClientErrors:
    """Tests for concrete client error types."""

    def test_validation_error(self) -> None:
        """ValidationError should have correct attributes."""
        err = ValidationError("Invalid email format", field="email")
        assert err.code == 422
        assert err.type == "VALIDATION_ERROR"
        assert err.retryable is False
        assert err.data["field"] == "email"

    def test_validation_error_with_data(self) -> None:
        """ValidationError should merge field with data."""
        err = ValidationError(
            "Value out of range",
            field="age",
            data={"expected": "18-100", "got": 150},
        )
        assert err.data["field"] == "age"
        assert err.data["expected"] == "18-100"
        assert err.data["got"] == 150


class TestMaxCallLevelError:
    """Tests for MaxCallLevelError."""

    def test_max_call_level_error(self) -> None:
        """MaxCallLevelError should have correct attributes."""
        err = MaxCallLevelError(node_id="node-1", level=11)
        assert err.code == 500
        assert err.type == "MAX_CALL_LEVEL"
        assert err.retryable is False
        assert err.data["nodeID"] == "node-1"
        assert err.data["level"] == 11
        assert "11" in err.message
        assert "node-1" in err.message


class TestErrorSerialization:
    """Tests for error serialization/deserialization."""

    def test_to_dict_includes_all_fields(self) -> None:
        """to_dict should include all required fields."""
        err = ServiceNotFoundError("math.add", node_id="node-1")
        d = err.to_dict()

        assert d["name"] == "ServiceNotFoundError"
        assert d["message"] == err.message
        assert d["code"] == 404
        assert d["type"] == "SERVICE_NOT_FOUND"
        assert d["retryable"] is True
        assert d["data"]["action"] == "math.add"

    def test_from_dict_restores_service_not_found(self) -> None:
        """from_dict should restore ServiceNotFoundError."""
        original = ServiceNotFoundError("math.add", node_id="node-1")
        d = original.to_dict()
        restored = MoleculerError.from_dict(d)

        assert isinstance(restored, ServiceNotFoundError)
        assert restored.code == 404
        assert restored.type == "SERVICE_NOT_FOUND"
        assert restored.action_name == "math.add"

    def test_from_dict_restores_validation_error(self) -> None:
        """from_dict should restore ValidationError."""
        original = ValidationError("Bad input", field="name")
        d = original.to_dict()
        restored = MoleculerError.from_dict(d)

        assert isinstance(restored, ValidationError)
        assert restored.code == 422
        assert restored.type == "VALIDATION_ERROR"

    def test_from_dict_restores_max_call_level(self) -> None:
        """from_dict should restore MaxCallLevelError."""
        original = MaxCallLevelError("node-1", 10)
        d = original.to_dict()
        restored = MoleculerError.from_dict(d)

        assert isinstance(restored, MaxCallLevelError)
        assert restored.level == 10
        assert restored.node_id == "node-1"

    def test_from_dict_unknown_error_type(self) -> None:
        """from_dict should handle unknown error types gracefully."""
        d = {
            "name": "UnknownError",
            "message": "Something went wrong",
            "code": 418,
            "type": "TEAPOT",
            "retryable": False,
        }
        restored = MoleculerError.from_dict(d)

        assert isinstance(restored, MoleculerError)
        assert restored.code == 418
        assert restored.type == "TEAPOT"
        assert "Something went wrong" in restored.message


class TestErrorRegistry:
    """Tests for ERROR_REGISTRY."""

    def test_all_errors_in_registry(self) -> None:
        """All error classes should be in the registry."""
        expected = [
            "MoleculerError",
            "MoleculerRetryableError",
            "MoleculerClientError",
            "ServiceNotFoundError",
            "ServiceNotAvailableError",
            "RequestTimeoutError",
            "RequestRejectedError",
            "QueueIsFullError",
            "ValidationError",
            "MaxCallLevelError",
            # Infrastructure errors
            "ServiceSchemaError",
            "BrokerOptionsError",
            "GracefulStopTimeoutError",
            "ProtocolVersionMismatchError",
            "InvalidPacketDataError",
            "BrokerDisconnectedError",
        ]
        for name in expected:
            assert name in ERROR_REGISTRY, f"{name} missing from ERROR_REGISTRY"

    def test_registry_maps_to_classes(self) -> None:
        """Registry should map names to actual classes."""
        assert ERROR_REGISTRY["ServiceNotFoundError"] is ServiceNotFoundError
        assert ERROR_REGISTRY["ValidationError"] is ValidationError
        assert ERROR_REGISTRY["MaxCallLevelError"] is MaxCallLevelError

    def test_remote_call_error_in_registry(self) -> None:
        """RemoteCallError should be present in ERROR_REGISTRY."""
        assert ERROR_REGISTRY["RemoteCallError"] is RemoteCallError

    def test_from_dict_restores_remote_call_error(self) -> None:
        """from_dict should restore RemoteCallError with metadata."""
        data = {
            "name": "RemoteCallError",
            "message": "Remote call failed",
            "code": 500,
            "type": "REMOTE_ERROR",
            "retryable": True,
            "data": {"name": "ValidationError", "stack": "Traceback..."},
        }
        restored = MoleculerError.from_dict(data)
        assert isinstance(restored, RemoteCallError)
        assert restored.error_name == "ValidationError"
        assert restored.stack == "Traceback..."


class TestErrorRepr:
    """Tests for error string representation."""

    def test_repr_includes_key_info(self) -> None:
        """__repr__ should include key information."""
        err = ServiceNotFoundError("math.add")
        repr_str = repr(err)

        assert "ServiceNotFoundError" in repr_str
        assert "404" in repr_str
        assert "SERVICE_NOT_FOUND" in repr_str
        assert "retryable=True" in repr_str


class TestInfrastructureErrors:
    """Tests for infrastructure error types."""

    def test_service_schema_error(self) -> None:
        """ServiceSchemaError should have correct attributes."""
        err = ServiceSchemaError("Invalid schema for 'users' service", {"field": "actions"})
        assert err.code == 500
        assert err.type == "SERVICE_SCHEMA_ERROR"
        assert err.retryable is False
        assert err.data["field"] == "actions"
        assert "Invalid schema" in err.message

    def test_broker_options_error(self) -> None:
        """BrokerOptionsError should have correct attributes."""
        err = BrokerOptionsError("Invalid transporter type 'xyz'", {"type": "xyz"})
        assert err.code == 500
        assert err.type == "BROKER_OPTIONS_ERROR"
        assert err.retryable is False
        assert err.data["type"] == "xyz"

    def test_graceful_stop_timeout_with_service(self) -> None:
        """GracefulStopTimeoutError should include service info when provided."""
        err = GracefulStopTimeoutError(service_name="users", service_version="1.0.0")
        assert err.code == 500
        assert err.type == "GRACEFUL_STOP_TIMEOUT"
        assert err.retryable is False
        assert "users" in err.message
        assert err.data["name"] == "users"
        assert err.data["version"] == "1.0.0"

    def test_graceful_stop_timeout_broker(self) -> None:
        """GracefulStopTimeoutError should work for broker-level timeout."""
        err = GracefulStopTimeoutError()
        assert err.code == 500
        assert err.type == "GRACEFUL_STOP_TIMEOUT"
        assert "ServiceBroker" in err.message
        assert err.service_name is None

    def test_protocol_version_mismatch_error(self) -> None:
        """ProtocolVersionMismatchError should have correct attributes."""
        err = ProtocolVersionMismatchError(actual="4", received="3", node_id="node-2")
        assert err.code == 500
        assert err.type == "PROTOCOL_VERSION_MISMATCH"
        assert err.retryable is False
        assert err.data["actual"] == "4"
        assert err.data["received"] == "3"
        assert err.data["nodeID"] == "node-2"

    def test_invalid_packet_data_error(self) -> None:
        """InvalidPacketDataError should have correct attributes."""
        err = InvalidPacketDataError(packet_type="REQ", data={"nodeID": "node-1"})
        assert err.code == 500
        assert err.type == "INVALID_PACKET_DATA"
        assert err.retryable is False
        assert err.data["type"] == "REQ"
        assert err.data["nodeID"] == "node-1"

    def test_broker_disconnected_error(self) -> None:
        """BrokerDisconnectedError should be retryable."""
        err = BrokerDisconnectedError()
        assert err.code == 502
        assert err.type == "BAD_GATEWAY"
        assert err.retryable is True  # This is retryable!
        assert "disconnected" in err.message


class TestInfrastructureErrorInheritance:
    """Tests for infrastructure error inheritance."""

    def test_broker_disconnected_is_retryable_error(self) -> None:
        """BrokerDisconnectedError should inherit from MoleculerRetryableError."""
        err = BrokerDisconnectedError()
        assert isinstance(err, MoleculerRetryableError)
        assert isinstance(err, MoleculerError)

    def test_service_schema_error_is_base_error(self) -> None:
        """ServiceSchemaError should inherit from MoleculerError directly."""
        err = ServiceSchemaError("Bad schema")
        assert isinstance(err, MoleculerError)
        assert not isinstance(err, MoleculerRetryableError)

    def test_broker_options_error_is_base_error(self) -> None:
        """BrokerOptionsError should inherit from MoleculerError directly."""
        err = BrokerOptionsError("Bad options")
        assert isinstance(err, MoleculerError)
        assert not isinstance(err, MoleculerRetryableError)
