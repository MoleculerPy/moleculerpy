"""Moleculer-compatible error hierarchy for MoleculerPy framework.

This module provides a structured error hierarchy following Moleculer.js conventions.
All errors include code, type, data, and retryable attributes for proper handling
in distributed systems.

Error hierarchy:
    MoleculerError (base)
    ├── MoleculerRetryableError (retryable=True)
    │   ├── MoleculerServerError (500, SERVER_ERROR)
    │   ├── ServiceNotFoundError (404, SERVICE_NOT_FOUND)
    │   ├── ServiceNotAvailableError (404, SERVICE_NOT_AVAILABLE)
    │   ├── RequestTimeoutError (504, REQUEST_TIMEOUT)
    │   ├── RequestRejectedError (503, REQUEST_REJECTED)
    │   ├── QueueIsFullError (429, QUEUE_FULL)
    │   └── BrokerDisconnectedError (502, BAD_GATEWAY)
    ├── MoleculerClientError (code=400, retryable=False)
    │   └── ValidationError (422, VALIDATION_ERROR)
    ├── RemoteCallError (500, REMOTE_ERROR, retryable=True)
    ├── RequestSkippedError (514, REQUEST_SKIPPED, retryable=False)
    ├── MaxCallLevelError (500, MAX_CALL_LEVEL, retryable=False)
    ├── ServiceSchemaError (500, SERVICE_SCHEMA_ERROR, retryable=False)
    ├── BrokerOptionsError (500, BROKER_OPTIONS_ERROR, retryable=False)
    ├── GracefulStopTimeoutError (500, GRACEFUL_STOP_TIMEOUT, retryable=False)
    ├── ProtocolVersionMismatchError (500, PROTOCOL_VERSION_MISMATCH, retryable=False)
    └── InvalidPacketDataError (500, INVALID_PACKET_DATA, retryable=False)

Example usage:
    from moleculerpy.errors import ServiceNotFoundError, ValidationError

    # Raise structured error
    raise ServiceNotFoundError("math.add", node_id="node-1")

    # Check if retryable
    try:
        await broker.call("service.action")
    except MoleculerError as e:
        if e.retryable:
            # Retry logic
            pass
        else:
            # Fail fast
            raise
"""

import traceback
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from .broker import ServiceBroker


class MoleculerError(Exception):
    """Base class for all Moleculer-compatible errors.

    Attributes:
        message: Human-readable error message
        code: HTTP-like status code (e.g., 404, 500)
        type: Error type identifier (e.g., SERVICE_NOT_FOUND)
        data: Additional error data
        retryable: Whether the operation should be retried
    """

    def __init__(
        self,
        message: str,
        code: int = 500,
        error_type: str = "MOLECULER_ERROR",
        data: dict[str, Any] | None = None,
        retryable: bool = False,
        # Backwards compatibility alias
        type: str | None = None,
    ) -> None:
        """Initialize a MoleculerError.

        Args:
            message: Human-readable error description
            code: HTTP-like status code
            error_type: Error type identifier for categorization
            data: Additional context data (nodeID, action, etc.)
            retryable: Whether the caller should retry this operation
            type: Deprecated alias for error_type (backwards compatibility)
        """
        super().__init__(message)
        self.message = message
        self.code = code
        # Support both 'type' (legacy) and 'error_type' (new)
        self.type = type if type is not None else error_type
        self.data = data or {}
        self.retryable = retryable

    def to_dict(self) -> dict[str, Any]:
        """Serialize error for transit/logging.

        Returns:
            Dictionary representation suitable for JSON serialization
        """
        return {
            "name": self.__class__.__name__,
            "message": self.message,
            "nodeID": self.data.get("nodeID"),  # Include nodeID for distributed tracing
            "code": self.code,
            "type": self.type,
            "data": self.data,
            "retryable": self.retryable,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MoleculerError":
        """Deserialize error from transit packet.

        Args:
            data: Dictionary containing error data

        Returns:
            Appropriate error instance based on error name
        """
        error_name = data.get("name", "MoleculerError")
        error_class = ERROR_REGISTRY.get(error_name, cls)

        # Handle special cases for known error types
        if error_class is ServiceNotFoundError:
            return ServiceNotFoundError(
                action_name=data.get("data", {}).get("action", "unknown"),
                node_id=data.get("data", {}).get("nodeID"),
            )
        elif error_class is ServiceNotAvailableError:
            return ServiceNotAvailableError(
                action_name=data.get("data", {}).get("action", "unknown"),
                node_id=data.get("data", {}).get("nodeID"),
            )
        elif error_class is RequestTimeoutError:
            return RequestTimeoutError(
                action_name=data.get("data", {}).get("action", "unknown"),
                timeout=data.get("data", {}).get("timeout", 0),
                elapsed=data.get("data", {}).get("elapsed"),
            )
        elif error_class is MaxCallLevelError:
            return MaxCallLevelError(
                node_id=data.get("data", {}).get("nodeID", "unknown"),
                level=data.get("data", {}).get("level", 0),
            )
        elif error_class is ValidationError:
            err_data = data.get("data", {})
            return ValidationError(
                message=data.get("message", "Validation error"),
                field=err_data.get("field"),
                data=err_data,
                # Restore validation-specific attributes
                type=err_data.get("validationType"),
                expected=err_data.get("expected"),
                got=err_data.get("got"),
            )
        elif error_class is RequestRejectedError:
            return RequestRejectedError(
                action_name=data.get("data", {}).get("action", "unknown"),
                reason=data.get("data", {}).get("reason"),
            )
        elif error_class is QueueIsFullError:
            return QueueIsFullError(
                action_name=data.get("data", {}).get("action", "unknown"),
                queue_size=data.get("data", {}).get("queueSize"),
            )
        elif error_class is RequestSkippedError:
            return RequestSkippedError(
                action_name=data.get("data", {}).get("action", "unknown"),
                node_id=data.get("data", {}).get("nodeID"),
            )
        elif error_class is RemoteCallError:
            return RemoteCallError(
                message=data.get("message", "Remote call failed"),
                error_name=data.get("data", {}).get("name", "RemoteError"),
                stack=data.get("data", {}).get("stack"),
                code=data.get("code", 500),
                error_type=data.get("type", "REMOTE_ERROR"),
                retryable=data.get("retryable", True),
            )
        # Note: ServiceNotAvailableError is handled above (lines 113-117)

        # Generic fallback
        return cls(
            message=data.get("message", "Unknown error"),
            code=data.get("code", 500),
            error_type=data.get("type", "MOLECULER_ERROR"),
            data=data.get("data", {}),
            retryable=data.get("retryable", False),
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"message={self.message!r}, "
            f"code={self.code}, "
            f"type={self.type!r}, "
            f"retryable={self.retryable})"
        )

    def __eq__(self, other: object) -> bool:
        """Compare errors by their content, not identity.

        Two MoleculerErrors are equal if they have the same class,
        message, code, type, and retryable flag.
        """
        if not isinstance(other, MoleculerError):
            return NotImplemented
        return (
            self.__class__ == other.__class__
            and self.message == other.message
            and self.code == other.code
            and self.type == other.type
            and self.retryable == other.retryable
        )

    def __hash__(self) -> int:
        """Hash based on error content for use in sets/dicts."""
        return hash((self.__class__.__name__, self.message, self.code, self.type, self.retryable))


class MoleculerRetryableError(MoleculerError):
    """Base class for retryable errors.

    These errors indicate transient failures that may succeed on retry.
    Examples: network timeouts, service temporarily unavailable.
    """

    def __init__(
        self,
        message: str,
        code: int = 500,
        error_type: str = "RETRYABLE_ERROR",
        data: dict[str, Any] | None = None,
        type: str | None = None,
    ) -> None:
        """Initialize a retryable error.

        Args:
            message: Human-readable error description
            code: HTTP-like status code
            error_type: Error type identifier
            data: Additional context data
            type: Deprecated alias for error_type
        """
        super().__init__(message, code, error_type, data, retryable=True, type=type)


class MoleculerServerError(MoleculerRetryableError):
    """Base class for server-side retryable errors.

    These errors indicate server-side issues that are typically transient.
    Extends MoleculerRetryableError with server-specific defaults.
    """

    def __init__(
        self,
        message: str,
        code: int = 500,
        error_type: str = "SERVER_ERROR",
        data: dict[str, Any] | None = None,
        type: str | None = None,
    ) -> None:
        """Initialize a server error.

        Args:
            message: Human-readable error description
            code: HTTP-like status code (default 500)
            error_type: Error type identifier
            data: Additional context data
            type: Deprecated alias for error_type
        """
        super().__init__(message, code, error_type, data, type=type)


class MoleculerClientError(MoleculerError):
    """Base class for client errors (not retryable).

    These errors indicate problems with the request itself.
    Examples: validation errors, bad parameters.
    """

    def __init__(
        self,
        message: str,
        code: int = 400,
        error_type: str = "CLIENT_ERROR",
        data: dict[str, Any] | None = None,
        type: str | None = None,
    ) -> None:
        """Initialize a client error.

        Args:
            message: Human-readable error description
            code: HTTP-like status code
            error_type: Error type identifier
            data: Additional context data
            type: Deprecated alias for error_type
        """
        super().__init__(message, code, error_type, data, retryable=False, type=type)


# =============================================================================
# Concrete Retryable Errors
# =============================================================================


class ServiceNotFoundError(MoleculerRetryableError):
    """Raised when a service action cannot be found.

    This is retryable because the service may be starting up or
    temporarily unregistered during deployment.
    """

    def __init__(self, action_name: str, node_id: str | None = None) -> None:
        """Initialize ServiceNotFoundError.

        Args:
            action_name: Name of the action that was not found
            node_id: ID of the node where lookup was attempted
        """
        super().__init__(
            message=f"Service '{action_name}' not found",
            code=404,
            type="SERVICE_NOT_FOUND",
            data={"action": action_name, "nodeID": node_id},
        )
        self.action_name = action_name
        self.node_id = node_id


class ServiceNotAvailableError(MoleculerRetryableError):
    """Raised when a service exists but is not currently available.

    This typically happens when all nodes providing the service are
    overloaded or in maintenance mode.
    """

    def __init__(self, action_name: str, node_id: str | None = None) -> None:
        """Initialize ServiceNotAvailableError.

        Args:
            action_name: Name of the unavailable action
            node_id: ID of the node that reported unavailability
        """
        super().__init__(
            message=f"Service '{action_name}' not available",
            code=404,
            type="SERVICE_NOT_AVAILABLE",
            data={"action": action_name, "nodeID": node_id},
        )
        self.action_name = action_name
        self.node_id = node_id


class RequestTimeoutError(MoleculerRetryableError):
    """Raised when a request exceeds its timeout limit.

    Retryable because timeout may be due to temporary network issues
    or transient service slowness.
    """

    def __init__(
        self,
        action_name: str,
        timeout: float,
        elapsed: float | None = None,
    ) -> None:
        """Initialize RequestTimeoutError.

        Args:
            action_name: Name of the action that timed out
            timeout: Configured timeout in seconds
            elapsed: Actual elapsed time before timeout (if known)
        """
        msg = f"Request timeout for '{action_name}' after {timeout}s"
        if elapsed is not None:
            msg = f"Request timeout for '{action_name}' after {elapsed:.2f}s (limit: {timeout}s)"

        super().__init__(
            message=msg,
            code=504,
            type="REQUEST_TIMEOUT",
            data={"action": action_name, "timeout": timeout, "elapsed": elapsed},
        )
        self.action_name = action_name
        self.timeout = timeout
        self.elapsed = elapsed


class RequestRejectedError(MoleculerRetryableError):
    """Raised when a request is rejected by the service.

    This can happen due to rate limiting, circuit breaker, or other
    protective mechanisms.
    """

    def __init__(
        self,
        action_name: str,
        reason: str | None = None,
    ) -> None:
        """Initialize RequestRejectedError.

        Args:
            action_name: Name of the action that rejected the request
            reason: Human-readable rejection reason
        """
        msg = f"Request rejected for '{action_name}'"
        if reason:
            msg = f"{msg}: {reason}"

        super().__init__(
            message=msg,
            code=503,
            type="REQUEST_REJECTED",
            data={"action": action_name, "reason": reason},
        )
        self.action_name = action_name
        self.reason = reason


class QueueIsFullError(MoleculerRetryableError):
    """Raised when a service's request queue is full.

    Indicates backpressure - the caller should implement
    exponential backoff before retrying.
    """

    def __init__(
        self,
        action_name: str,
        queue_size: int | None = None,
    ) -> None:
        """Initialize QueueIsFullError.

        Args:
            action_name: Name of the action with full queue
            queue_size: Current queue size (if known)
        """
        super().__init__(
            message=f"Queue is full for '{action_name}'",
            code=429,
            type="QUEUE_FULL",  # Matches Moleculer.js
            data={"action": action_name, "queueSize": queue_size},
        )
        self.action_name = action_name
        self.queue_size = queue_size


class RequestSkippedError(MoleculerError):
    """Raised when a request is skipped because timeout was reached before execution.

    This error is NOT retryable - it indicates the request was never attempted
    because it was already past its deadline when picked up for execution.
    """

    def __init__(
        self,
        action_name: str,
        node_id: str | None = None,
    ) -> None:
        """Initialize RequestSkippedError.

        Args:
            action_name: Name of the action that was skipped
            node_id: ID of the node where the skip occurred
        """
        super().__init__(
            message=f"Calling '{action_name}' is skipped because timeout reached on '{node_id}' node.",
            code=514,
            error_type="REQUEST_SKIPPED",
            data={"action": action_name, "nodeID": node_id},
            retryable=False,
        )
        self.action_name = action_name
        self.node_id = node_id


# =============================================================================
# Concrete Client Errors
# =============================================================================


class ValidationError(MoleculerClientError):
    """Raised when parameter validation fails.

    Not retryable - the request itself is malformed and must be fixed.

    This class is backwards-compatible with the old validator.ValidationError,
    supporting additional attributes: type (validation type), expected, got.
    """

    def __init__(
        self,
        message: str,
        field: str | None = None,
        data: dict[str, Any] | None = None,
        # Backwards compatibility with validator.ValidationError
        type: str | None = None,
        expected: Any = None,
        got: Any = None,
    ) -> None:
        """Initialize ValidationError.

        Args:
            message: Validation error description
            field: Name of the field that failed validation
            data: Additional validation context
            type: Type of validation error (e.g., 'type_mismatch', 'min_value')
            expected: Expected value or type
            got: Actual value or type received
        """
        err_data = data.copy() if data else {}
        if field:
            err_data["field"] = field
        if type:
            err_data["validationType"] = type
        if expected is not None:
            err_data["expected"] = expected
        if got is not None:
            err_data["got"] = got

        super().__init__(
            message=message,
            code=422,
            type="VALIDATION_ERROR",
            data=err_data,
        )
        self.field = field
        # Backwards compatibility attributes
        self.validation_type = type
        self.expected = expected
        self.got = got


class RemoteCallError(MoleculerError):
    """Raised when a remote service call fails."""

    def __init__(
        self,
        message: str,
        error_name: str = "RemoteError",
        stack: str | None = None,
        code: int = 500,
        error_type: str = "REMOTE_ERROR",
        retryable: bool = True,
    ) -> None:
        super().__init__(
            message=message,
            code=code,
            type=error_type,
            data={"name": error_name, "stack": stack},
            retryable=retryable,
        )
        self.error_name = error_name
        self.stack = stack


# =============================================================================
# Server Errors
# =============================================================================


class MaxCallLevelError(MoleculerError):
    """Raised when maximum call level (recursion depth) is exceeded.

    This protects against infinite recursion in cyclic service calls.
    Not retryable - indicates a bug in service design.
    """

    def __init__(self, node_id: str, level: int) -> None:
        """Initialize MaxCallLevelError.

        Args:
            node_id: ID of the node where limit was exceeded
            level: Current call level that exceeded the limit
        """
        super().__init__(
            message=f"Max call level ({level}) exceeded on node {node_id}",
            code=500,
            type="MAX_CALL_LEVEL",
            data={"nodeID": node_id, "level": level},
            retryable=False,
        )
        self.node_id = node_id
        self.level = level


# =============================================================================
# Infrastructure Errors
# =============================================================================


class ServiceSchemaError(MoleculerError):
    """Raised when a service schema is invalid.

    This indicates a programming error in service definition.
    Not retryable - the schema must be fixed.
    """

    def __init__(self, message: str, data: dict[str, Any] | None = None) -> None:
        """Initialize ServiceSchemaError.

        Args:
            message: Description of the schema error
            data: Additional context about the schema problem
        """
        super().__init__(
            message=message,
            code=500,
            error_type="SERVICE_SCHEMA_ERROR",
            data=data,
            retryable=False,
        )


class BrokerOptionsError(MoleculerError):
    """Raised when broker configuration is invalid.

    This indicates invalid broker options or configuration.
    Not retryable - the configuration must be fixed.
    """

    def __init__(self, message: str, data: dict[str, Any] | None = None) -> None:
        """Initialize BrokerOptionsError.

        Args:
            message: Description of the configuration error
            data: Additional context about the invalid options
        """
        super().__init__(
            message=message,
            code=500,
            error_type="BROKER_OPTIONS_ERROR",
            data=data,
            retryable=False,
        )


class GracefulStopTimeoutError(MoleculerError):
    """Raised when graceful shutdown times out.

    This occurs when a service or broker cannot stop within the allowed time.
    Not retryable - indicates stuck requests or connections.
    """

    def __init__(
        self,
        service_name: str | None = None,
        service_version: str | None = None,
    ) -> None:
        """Initialize GracefulStopTimeoutError.

        Args:
            service_name: Name of the service that failed to stop (if applicable)
            service_version: Version of the service (if applicable)
        """
        if service_name:
            message = f"Unable to stop '{service_name}' service gracefully."
            data = {"name": service_name, "version": service_version}
        else:
            message = "Unable to stop ServiceBroker gracefully."
            data = {}

        super().__init__(
            message=message,
            code=500,
            error_type="GRACEFUL_STOP_TIMEOUT",
            data=data,
            retryable=False,
        )
        self.service_name = service_name
        self.service_version = service_version


class ProtocolVersionMismatchError(MoleculerError):
    """Raised when protocol versions don't match between nodes.

    This occurs when nodes with incompatible protocol versions
    try to communicate. Not retryable - requires node upgrade.
    """

    def __init__(
        self,
        actual: str | None = None,
        received: str | None = None,
        node_id: str | None = None,
    ) -> None:
        """Initialize ProtocolVersionMismatchError.

        Args:
            actual: Protocol version of this node
            received: Protocol version received from remote node
            node_id: ID of the remote node with mismatched version
        """
        super().__init__(
            message="Protocol version mismatch.",
            code=500,
            error_type="PROTOCOL_VERSION_MISMATCH",
            data={"actual": actual, "received": received, "nodeID": node_id},
            retryable=False,
        )
        self.actual = actual
        self.received = received
        self.node_id = node_id


class InvalidPacketDataError(MoleculerError):
    """Raised when a transit packet has invalid format or data.

    This indicates corrupted or malformed network packets.
    Not retryable - the packet itself is broken.
    """

    def __init__(
        self,
        packet_type: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        """Initialize InvalidPacketDataError.

        Args:
            packet_type: Type of the invalid packet
            data: Additional context about the invalid packet
        """
        err_data = data.copy() if data else {}
        if packet_type:
            err_data["type"] = packet_type

        super().__init__(
            message="Invalid packet data.",
            code=500,
            error_type="INVALID_PACKET_DATA",
            data=err_data,
            retryable=False,
        )
        self.packet_type = packet_type


class BrokerDisconnectedError(MoleculerRetryableError):
    """Raised when the broker's transporter is disconnected.

    This is retryable because the connection may be re-established.
    """

    def __init__(self) -> None:
        """Initialize BrokerDisconnectedError."""
        super().__init__(
            message=(
                "The broker's transporter has disconnected. "
                "Please try again when a connection is reestablished."
            ),
            code=502,
            type="BAD_GATEWAY",
            data={},
        )


class SerializationError(MoleculerError):
    """Raised when serialization or deserialization fails.

    This indicates a problem with encoding/decoding message payloads.
    Not retryable - the payload itself is malformed or incompatible.
    """

    def __init__(self, message: str) -> None:
        """Initialize SerializationError.

        Args:
            message: Description of the serialization failure
        """
        super().__init__(
            message=message,
            code=500,
            error_type="SERIALIZATION_ERROR",
            retryable=False,
        )


# =============================================================================
# Error Regenerator
# =============================================================================


class Regenerator:
    """Error regenerator for extensible error serialization/deserialization.

    This class follows the Moleculer.js Regenerator pattern, providing hooks
    for custom error handling in distributed systems.

    The regeneration process:
    1. Try restore_custom_error() hook for custom error types
    2. Try recreate_error() for known Moleculer errors
    3. Fall back to _create_default_error() for unknown errors
    4. Restore external fields (retryable, nodeID) and stack trace

    Example usage - extending with custom errors:

        class MyRegenerator(Regenerator):
            def restore_custom_error(self, plain_error, payload):
                if plain_error.get("name") == "MyCustomError":
                    return MyCustomError(plain_error["message"])
                return None

        broker.settings.regenerator = MyRegenerator()
    """

    def __init__(self) -> None:
        """Initialize the Regenerator."""
        self.broker: ServiceBroker | None = None
        self.node_id: str | None = None

    def init(self, broker: "ServiceBroker") -> None:
        """Initialize regenerator with broker reference.

        Args:
            broker: ServiceBroker instance for access to nodeID
        """
        self.broker = broker
        self.node_id = getattr(broker, "nodeID", None)

    def extract_plain_error(self, err: Exception, include_stack: bool = True) -> dict[str, Any]:
        """Extract a plain error object from an Exception for transit.

        This method serializes an error for transmission over the network.

        Args:
            err: The exception to serialize
            include_stack: Whether to include stack trace (default True).
                          Set to False for performance when stack is not needed.

        Returns:
            Dictionary containing error fields ready for JSON serialization
        """
        # Get stack trace only if needed (expensive operation)
        stack = self._get_stack_trace(err) if include_stack else ""

        if isinstance(err, MoleculerError):
            plain = err.to_dict()
            # Ensure nodeID is set (from error data or broker)
            if not plain.get("nodeID"):
                plain["nodeID"] = err.data.get("nodeID") or self.node_id
            plain["stack"] = stack
            return plain

        # Non-Moleculer error: wrap it
        return {
            "name": err.__class__.__name__,
            "message": str(err),
            "nodeID": self.node_id,
            "code": 500,
            "type": "INTERNAL_ERROR",
            "retryable": False,
            "stack": stack,
            "data": {},
        }

    def _get_stack_trace(self, err: Exception) -> str:
        """Get formatted stack trace for an exception.

        Uses err.__traceback__ if available (from except block),
        otherwise falls back to format_exc().

        Args:
            err: The exception to get stack trace for

        Returns:
            Formatted stack trace string
        """
        if err.__traceback__ is not None:
            # Use exception's own traceback (more accurate)
            return "".join(traceback.format_exception(type(err), err, err.__traceback__))
        # Fallback to current exception context
        return traceback.format_exc()

    def restore(
        self, plain_error: dict[str, Any], payload: dict[str, Any] | None = None
    ) -> Exception:
        """Restore an Error object from a plain error dictionary.

        This method deserializes an error received over the network.

        The restoration order:
        1. restore_custom_error() - hook for custom errors (can be overridden)
        2. recreate_error() - known Moleculer errors via ERROR_REGISTRY
        3. _create_default_error() - fallback for unknown errors

        Args:
            plain_error: Dictionary containing error fields
            payload: Optional full packet payload (for sender info)

        Returns:
            Reconstructed exception instance
        """
        payload = payload or {}

        # 1. Try custom error hook
        err = self.restore_custom_error(plain_error, payload)

        # 2. Try standard recreation via from_dict
        if err is None:
            err = recreate_error(plain_error)

        # 3. Fallback to default error
        if err is None:
            err = self._create_default_error(plain_error)

        # 4. Restore external fields
        self._restore_external_fields(plain_error, err, payload)

        # 5. Restore stack trace
        self._restore_stack(plain_error, err)

        return err

    def restore_custom_error(
        self, plain_error: dict[str, Any], payload: dict[str, Any]
    ) -> Exception | None:
        """Hook for restoring custom error types.

        Override this method in subclasses to handle custom error types
        that are not part of the standard Moleculer error hierarchy.

        Args:
            plain_error: Dictionary containing error fields
            payload: Full packet payload

        Returns:
            Custom error instance, or None to continue with standard restoration
        """
        return None

    def _create_default_error(self, plain_error: dict[str, Any]) -> Exception:
        """Create a default error for unknown error types.

        Args:
            plain_error: Dictionary containing error fields

        Returns:
            Generic Exception with error fields attached
        """
        err = Exception(plain_error.get("message", "Unknown error"))
        dynamic_err = cast(Any, err)
        # Attach error fields for inspection
        dynamic_err.name = plain_error.get("name", "Error")
        dynamic_err.code = plain_error.get("code", 500)
        dynamic_err.type = plain_error.get("type", "UNKNOWN")
        dynamic_err.data = plain_error.get("data", {})
        dynamic_err.retryable = plain_error.get("retryable", False)
        return err

    def _restore_external_fields(
        self,
        plain_error: dict[str, Any],
        err: Exception,
        payload: dict[str, Any],
    ) -> None:
        """Restore external error fields (retryable, nodeID).

        Args:
            plain_error: Dictionary containing error fields
            err: The error object being restored
            payload: Full packet payload
        """
        # Set retryable flag
        if hasattr(err, "retryable"):
            err.retryable = plain_error.get("retryable", False)

        # Set nodeID from plain_error or payload.sender
        node_id = plain_error.get("nodeID") or payload.get("sender")
        data_obj = getattr(err, "data", None)
        if isinstance(data_obj, dict) and node_id and "nodeID" not in data_obj:
            data_obj["nodeID"] = node_id
        cast(Any, err).nodeID = node_id

    def _restore_stack(self, plain_error: dict[str, Any], err: Exception) -> None:
        """Restore stack trace from remote error.

        Args:
            plain_error: Dictionary containing error fields
            err: The error object being restored
        """
        if plain_error.get("stack"):
            # Store remote stack as attribute (don't override local stack)
            cast(Any, err).remote_stack = plain_error["stack"]


def recreate_error(plain_error: dict[str, Any]) -> MoleculerError | None:
    """Recreate a MoleculerError from a plain error dictionary.

    This function uses ERROR_REGISTRY to find the appropriate error class
    and reconstructs it with the correct arguments.

    Args:
        plain_error: Dictionary containing error fields

    Returns:
        Reconstructed MoleculerError, or None if error type is unknown
    """
    return MoleculerError.from_dict(plain_error)


def resolve_regenerator(opt: Any) -> Regenerator:
    """Resolve a regenerator option to a Regenerator instance.

    Args:
        opt: Either a Regenerator instance or None

    Returns:
        The provided Regenerator or a new default instance
    """
    if isinstance(opt, Regenerator):
        return opt
    return Regenerator()


# =============================================================================
# Error Registry for Deserialization
# =============================================================================

ERROR_REGISTRY: dict[str, type[MoleculerError]] = {
    # Base classes
    "MoleculerError": MoleculerError,
    "MoleculerRetryableError": MoleculerRetryableError,
    "MoleculerServerError": MoleculerServerError,
    "MoleculerClientError": MoleculerClientError,
    # Retryable errors
    "ServiceNotFoundError": ServiceNotFoundError,
    "ServiceNotAvailableError": ServiceNotAvailableError,
    "RequestTimeoutError": RequestTimeoutError,
    "RequestRejectedError": RequestRejectedError,
    "RequestSkippedError": RequestSkippedError,
    "QueueIsFullError": QueueIsFullError,
    "BrokerDisconnectedError": BrokerDisconnectedError,
    # Client errors
    "ValidationError": ValidationError,
    "RemoteCallError": RemoteCallError,
    # Server/Infrastructure errors
    "MaxCallLevelError": MaxCallLevelError,
    "ServiceSchemaError": ServiceSchemaError,
    "BrokerOptionsError": BrokerOptionsError,
    "GracefulStopTimeoutError": GracefulStopTimeoutError,
    "ProtocolVersionMismatchError": ProtocolVersionMismatchError,
    "InvalidPacketDataError": InvalidPacketDataError,
    "SerializationError": SerializationError,
}


__all__ = [  # noqa: RUF022
    # Base classes
    "MoleculerError",
    "MoleculerRetryableError",
    "MoleculerServerError",
    "MoleculerClientError",
    # Retryable errors
    "ServiceNotFoundError",
    "ServiceNotAvailableError",
    "RequestTimeoutError",
    "RequestRejectedError",
    "QueueIsFullError",
    "BrokerDisconnectedError",
    # Non-retryable special errors
    "RequestSkippedError",
    # Client errors
    "ValidationError",
    "RemoteCallError",
    # Server/Infrastructure errors
    "MaxCallLevelError",
    "ServiceSchemaError",
    "BrokerOptionsError",
    "GracefulStopTimeoutError",
    "ProtocolVersionMismatchError",
    "InvalidPacketDataError",
    "SerializationError",
    # Registry
    "ERROR_REGISTRY",
    # Regenerator
    "Regenerator",
    "recreate_error",
    "resolve_regenerator",
]
