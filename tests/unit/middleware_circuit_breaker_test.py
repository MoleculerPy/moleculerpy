"""Unit tests for the CircuitBreakerMiddleware."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from moleculerpy.errors import MoleculerClientError
from moleculerpy.middleware.circuit_breaker import (
    CircuitBreakerError,
    CircuitBreakerMiddleware,
    CircuitState,
    CircuitStatus,
)
from moleculerpy.transit import RemoteCallError


class TestCircuitBreakerMiddlewareInit:
    """Test CircuitBreakerMiddleware initialization."""

    def test_default_initialization(self):
        """Test CircuitBreakerMiddleware with default values."""
        middleware = CircuitBreakerMiddleware()

        assert middleware.threshold == 0.5
        assert middleware.window_time == 60
        assert middleware.min_requests == 10
        assert middleware.half_open_time == 10

    def test_custom_initialization(self):
        """Test CircuitBreakerMiddleware with custom values."""
        middleware = CircuitBreakerMiddleware(
            threshold=0.3,
            window_time=120,
            min_requests=5,
            half_open_time=30,
        )

        assert middleware.threshold == 0.3
        assert middleware.window_time == 120
        assert middleware.min_requests == 5
        assert middleware.half_open_time == 30

    def test_invalid_threshold_raises_error(self):
        """Test that invalid threshold raises ValueError."""
        with pytest.raises(ValueError, match="threshold must be between"):
            CircuitBreakerMiddleware(threshold=1.5)

        with pytest.raises(ValueError, match="threshold must be between"):
            CircuitBreakerMiddleware(threshold=-0.1)

    def test_custom_trip_errors(self):
        """Test CircuitBreakerMiddleware with custom trip errors."""
        custom_errors = {ValueError, KeyError}
        middleware = CircuitBreakerMiddleware(trip_errors=custom_errors)

        assert middleware.trip_errors == custom_errors


class TestCircuitBreakerTripErrorCheck:
    """Test trip error checking."""

    def test_timeout_error_is_trip_error(self):
        """Test that TimeoutError is a trip error by default."""
        middleware = CircuitBreakerMiddleware()

        assert middleware._is_trip_error(TimeoutError())

    def test_remote_call_error_is_trip_error(self):
        """Test that RemoteCallError is a trip error by default."""
        middleware = CircuitBreakerMiddleware()

        assert middleware._is_trip_error(RemoteCallError("test error"))

    def test_value_error_is_not_trip_error(self):
        """Test that ValueError is not a trip error by default."""
        middleware = CircuitBreakerMiddleware()

        assert not middleware._is_trip_error(ValueError("test"))

    def test_circuit_breaker_error_is_client_error(self):
        """CircuitBreakerError should inherit MoleculerClientError for proper hierarchy."""
        error = CircuitBreakerError("test.action", CircuitState.OPEN, 1.5)
        assert isinstance(error, MoleculerClientError)

    def test_custom_check_failure_logs_warning_and_falls_back(self):
        """A failing custom check should warn and then use default trip_errors logic."""

        def broken_check(_: Exception) -> bool:
            raise RuntimeError("check failed")

        middleware = CircuitBreakerMiddleware(check=broken_check)
        middleware.logger = MagicMock()

        # TimeoutError is in DEFAULT_TRIP_ERRORS, so fallback should return True.
        assert middleware._is_trip_error(TimeoutError())
        middleware.logger.warning.assert_called_once()


class TestCircuitBreakerFailureRateCalculation:
    """Test failure rate calculation."""

    def test_calculate_failure_rate_empty(self):
        """Test failure rate with no results."""
        middleware = CircuitBreakerMiddleware()
        circuit = CircuitStatus()

        assert middleware._calculate_failure_rate(circuit) == 0.0

    def test_calculate_failure_rate_all_success(self):
        """Test failure rate with all successes."""
        middleware = CircuitBreakerMiddleware()
        circuit = CircuitStatus()
        circuit.success_count = 10
        circuit.failure_count = 0

        assert middleware._calculate_failure_rate(circuit) == 0.0

    def test_calculate_failure_rate_all_failure(self):
        """Test failure rate with all failures."""
        middleware = CircuitBreakerMiddleware()
        circuit = CircuitStatus()
        circuit.success_count = 0
        circuit.failure_count = 10

        assert middleware._calculate_failure_rate(circuit) == 1.0

    def test_calculate_failure_rate_mixed(self):
        """Test failure rate with mixed results."""
        middleware = CircuitBreakerMiddleware()
        circuit = CircuitStatus()
        circuit.success_count = 7
        circuit.failure_count = 3

        assert middleware._calculate_failure_rate(circuit) == 0.3


class TestCircuitBreakerTripping:
    """Test circuit tripping logic."""

    def test_should_not_trip_below_min_requests(self):
        """Test that circuit doesn't trip below min_requests."""
        middleware = CircuitBreakerMiddleware(min_requests=10, threshold=0.5)
        circuit = CircuitStatus()
        circuit.success_count = 0
        circuit.failure_count = 5  # 100% failure but only 5 requests

        assert not middleware._should_trip(circuit)

    def test_should_trip_above_threshold(self):
        """Test that circuit trips above threshold."""
        middleware = CircuitBreakerMiddleware(min_requests=10, threshold=0.5)
        circuit = CircuitStatus()
        circuit.success_count = 4
        circuit.failure_count = 6  # 60% failure rate

        assert middleware._should_trip(circuit)

    def test_should_not_trip_below_threshold(self):
        """Test that circuit doesn't trip below threshold."""
        middleware = CircuitBreakerMiddleware(min_requests=10, threshold=0.5)
        circuit = CircuitStatus()
        circuit.success_count = 7
        circuit.failure_count = 3  # 30% failure rate

        assert not middleware._should_trip(circuit)

    def test_trip_circuit_changes_state(self):
        """Test that _trip_circuit changes state to OPEN."""
        middleware = CircuitBreakerMiddleware()
        circuit = CircuitStatus()

        middleware._trip_circuit(circuit, "test.action")

        assert circuit.state == CircuitState.OPEN
        assert circuit.opened_at is not None

    def test_close_circuit_changes_state(self):
        """Test that _close_circuit changes state to CLOSED."""
        middleware = CircuitBreakerMiddleware()
        circuit = CircuitStatus()
        circuit.state = CircuitState.OPEN
        circuit.opened_at = time.time()
        circuit.failure_count = 5
        circuit.success_count = 5

        middleware._close_circuit(circuit, "test.action")

        assert circuit.state == CircuitState.CLOSED
        assert circuit.opened_at is None
        assert circuit.failure_count == 0
        assert circuit.success_count == 0


class TestCircuitBreakerRecoveryCheck:
    """Test recovery checking logic."""

    def test_should_test_recovery_after_half_open_time(self):
        """Test that circuit tests recovery after half_open_time."""
        middleware = CircuitBreakerMiddleware(half_open_time=10)
        circuit = CircuitStatus()
        circuit.state = CircuitState.OPEN
        circuit.opened_at = time.time() - 15  # 15 seconds ago

        assert middleware._should_test_recovery(circuit)

    def test_should_not_test_recovery_before_half_open_time(self):
        """Test that circuit doesn't test recovery before half_open_time."""
        middleware = CircuitBreakerMiddleware(half_open_time=10)
        circuit = CircuitStatus()
        circuit.state = CircuitState.OPEN
        circuit.opened_at = time.time() - 5  # 5 seconds ago

        assert not middleware._should_test_recovery(circuit)

    def test_should_not_test_recovery_if_closed(self):
        """Test that recovery check returns false if circuit is CLOSED."""
        middleware = CircuitBreakerMiddleware(half_open_time=10)
        circuit = CircuitStatus()
        circuit.state = CircuitState.CLOSED

        assert not middleware._should_test_recovery(circuit)


class TestCircuitBreakerMonitoring:
    """Test monitoring methods."""

    def test_get_circuit_state_existing(self):
        """Test getting state of existing circuit."""
        middleware = CircuitBreakerMiddleware()
        middleware._circuits["test.action"] = CircuitStatus(state=CircuitState.OPEN)

        assert middleware.get_circuit_state("test.action") == CircuitState.OPEN

    def test_get_circuit_state_nonexistent(self):
        """Test getting state of nonexistent circuit."""
        middleware = CircuitBreakerMiddleware()

        assert middleware.get_circuit_state("nonexistent") is None

    def test_get_circuit_stats(self):
        """Test getting circuit statistics."""
        middleware = CircuitBreakerMiddleware()
        circuit = CircuitStatus()
        circuit.state = CircuitState.OPEN
        circuit.failure_count = 5
        circuit.success_count = 5
        circuit.opened_at = time.time()
        middleware._circuits["test.action"] = circuit

        stats = middleware.get_circuit_stats("test.action")

        assert stats["state"] == "open"
        assert stats["failure_count"] == 5
        assert stats["success_count"] == 5
        assert stats["total_requests"] == 10
        assert stats["failure_rate"] == 0.5


@pytest.mark.asyncio
class TestCircuitBreakerRemoteAction:
    """Test remote_action hook with circuit breaker logic."""

    async def test_successful_call_records_success(self):
        """Test that successful calls are recorded."""
        middleware = CircuitBreakerMiddleware()

        action = MagicMock()
        action.name = "test.action"

        call_count = 0

        async def mock_handler(ctx):
            nonlocal call_count
            call_count += 1
            return "success"

        handler = await middleware.remote_action(mock_handler, action)

        ctx = MagicMock()
        result = await handler(ctx)

        assert result == "success"
        assert call_count == 1

        # Check that success was recorded
        circuit = middleware._circuits.get("test.action")
        assert circuit is not None
        assert circuit.success_count == 1

    async def test_failed_call_records_failure(self):
        """Test that failed calls are recorded."""
        middleware = CircuitBreakerMiddleware()

        action = MagicMock()
        action.name = "test.action"

        async def mock_handler(ctx):
            raise TimeoutError("timeout")

        handler = await middleware.remote_action(mock_handler, action)

        ctx = MagicMock()

        with pytest.raises(asyncio.TimeoutError):
            await handler(ctx)

        # Check that failure was recorded
        circuit = middleware._circuits.get("test.action")
        assert circuit is not None
        assert circuit.failure_count == 1

    async def test_circuit_trips_after_threshold(self):
        """Test that circuit trips after reaching threshold."""
        middleware = CircuitBreakerMiddleware(threshold=0.5, min_requests=4, half_open_time=100)

        action = MagicMock()
        action.name = "test.action"

        async def mock_handler(ctx):
            raise TimeoutError("timeout")

        handler = await middleware.remote_action(mock_handler, action)

        ctx = MagicMock()
        ctx.broker = MagicMock()
        ctx.broker.logger = MagicMock()

        # Make failures to trip circuit
        for _ in range(4):
            try:
                await handler(ctx)
            except TimeoutError:
                pass

        # Circuit should be open now
        assert middleware.get_circuit_state("test.action") == CircuitState.OPEN

        # Next call should fail fast
        with pytest.raises(CircuitBreakerError) as exc_info:
            await handler(ctx)

        assert exc_info.value.state == CircuitState.OPEN

    async def test_open_circuit_fails_fast(self):
        """Test that open circuit fails fast without calling handler."""
        middleware = CircuitBreakerMiddleware(half_open_time=100)

        action = MagicMock()
        action.name = "test.action"

        # Manually set circuit to OPEN
        circuit = CircuitStatus()
        circuit.state = CircuitState.OPEN
        circuit.opened_at = time.time()
        middleware._circuits["test.action"] = circuit

        call_count = 0

        async def mock_handler(ctx):
            nonlocal call_count
            call_count += 1
            return "success"

        handler = await middleware.remote_action(mock_handler, action)

        ctx = MagicMock()

        with pytest.raises(CircuitBreakerError):
            await handler(ctx)

        # Handler should not have been called
        assert call_count == 0

    async def test_half_open_recovery_success(self):
        """Test that successful call in HALF_OPEN closes circuit."""
        middleware = CircuitBreakerMiddleware(half_open_time=0)  # Immediate recovery

        action = MagicMock()
        action.name = "test.action"

        # Set circuit to OPEN with expired timeout
        circuit = CircuitStatus()
        circuit.state = CircuitState.OPEN
        circuit.opened_at = time.time() - 1
        middleware._circuits["test.action"] = circuit

        async def mock_handler(ctx):
            return "success"

        handler = await middleware.remote_action(mock_handler, action)

        ctx = MagicMock()
        result = await handler(ctx)

        assert result == "success"
        assert middleware.get_circuit_state("test.action") == CircuitState.CLOSED

    async def test_half_open_recovery_failure(self):
        """Test that failed call in HALF_OPEN reopens circuit."""
        middleware = CircuitBreakerMiddleware(half_open_time=0)  # Immediate recovery

        action = MagicMock()
        action.name = "test.action"

        # Set circuit to OPEN with expired timeout
        circuit = CircuitStatus()
        circuit.state = CircuitState.OPEN
        circuit.opened_at = time.time() - 1
        middleware._circuits["test.action"] = circuit

        async def mock_handler(ctx):
            raise TimeoutError("still failing")

        handler = await middleware.remote_action(mock_handler, action)

        ctx = MagicMock()

        with pytest.raises(asyncio.TimeoutError):
            await handler(ctx)

        # Circuit should be back to OPEN
        assert middleware.get_circuit_state("test.action") == CircuitState.OPEN

    async def test_non_trip_error_not_recorded(self):
        """Test that non-trip errors don't count toward failure rate."""
        middleware = CircuitBreakerMiddleware()

        action = MagicMock()
        action.name = "test.action"

        async def mock_handler(ctx):
            raise ValueError("not a trip error")

        handler = await middleware.remote_action(mock_handler, action)

        ctx = MagicMock()

        with pytest.raises(ValueError):
            await handler(ctx)

        # Failure should not be recorded
        circuit = middleware._circuits.get("test.action")
        assert circuit is None or circuit.failure_count == 0


class TestCircuitBreakerError:
    """Test CircuitBreakerError exception."""

    def test_error_message_open_state(self):
        """Test error message for OPEN state."""
        error = CircuitBreakerError("test.action", CircuitState.OPEN, time_until_half_open=5.5)

        assert "test.action" in str(error)
        assert "open" in str(error)
        assert "5.5s" in str(error)

    def test_error_message_no_time(self):
        """Test error message without time remaining."""
        error = CircuitBreakerError("test.action", CircuitState.OPEN)

        assert "test.action" in str(error)
        assert "open" in str(error)


@pytest.mark.asyncio
class TestCircuitBreakerEvents:
    """Test circuit breaker internal event emission."""

    async def test_emits_opened_event_on_trip(self):
        """Test that $circuit-breaker.opened is emitted when circuit trips."""
        middleware = CircuitBreakerMiddleware(threshold=0.5, min_requests=2, half_open_time=100)

        action = MagicMock()
        action.name = "test.action"

        async def mock_handler(ctx):
            raise TimeoutError("timeout")

        handler = await middleware.remote_action(mock_handler, action)

        # Create ctx with mocked broker
        ctx = MagicMock()
        ctx.broker = MagicMock()
        ctx.broker.node_id = "test-node"
        ctx.broker.broadcast_local = AsyncMock()
        ctx.broker.logger = MagicMock()

        # Make failures to trip circuit
        for _ in range(2):
            try:
                await handler(ctx)
            except TimeoutError:
                pass

        # Verify $circuit-breaker.opened was called
        ctx.broker.broadcast_local.assert_called()
        calls = ctx.broker.broadcast_local.call_args_list
        event_names = [call[0][0] for call in calls]
        assert "$circuit-breaker.opened" in event_names

    async def test_emits_half_opened_event_on_recovery_test(self):
        """Test that $circuit-breaker.half-opened is emitted when testing recovery."""
        middleware = CircuitBreakerMiddleware(half_open_time=0)

        action = MagicMock()
        action.name = "test.action"

        call_count = 0

        async def mock_handler(ctx):
            nonlocal call_count
            call_count += 1
            return "success"

        handler = await middleware.remote_action(mock_handler, action)

        # Manually set circuit to OPEN (ready for recovery test)
        circuit = CircuitStatus()
        circuit.state = CircuitState.OPEN
        circuit.opened_at = time.time() - 10  # 10 seconds ago
        middleware._circuits["test.action"] = circuit

        # Create ctx with mocked broker
        ctx = MagicMock()
        ctx.broker = MagicMock()
        ctx.broker.node_id = "test-node"
        ctx.broker.broadcast_local = AsyncMock()

        # Call to trigger recovery test
        await handler(ctx)

        # Verify $circuit-breaker.half-opened was called
        ctx.broker.broadcast_local.assert_called()
        calls = ctx.broker.broadcast_local.call_args_list
        event_names = [call[0][0] for call in calls]
        assert "$circuit-breaker.half-opened" in event_names

    async def test_emits_closed_event_on_recovery_success(self):
        """Test that $circuit-breaker.closed is emitted on successful recovery."""
        middleware = CircuitBreakerMiddleware(half_open_time=0)

        action = MagicMock()
        action.name = "test.action"

        async def mock_handler(ctx):
            return "success"

        handler = await middleware.remote_action(mock_handler, action)

        # Manually set circuit to OPEN
        circuit = CircuitStatus()
        circuit.state = CircuitState.OPEN
        circuit.opened_at = time.time() - 10
        middleware._circuits["test.action"] = circuit

        # Create ctx with mocked broker
        ctx = MagicMock()
        ctx.broker = MagicMock()
        ctx.broker.node_id = "test-node"
        ctx.broker.broadcast_local = AsyncMock()

        # Call to recover circuit
        await handler(ctx)

        # Verify $circuit-breaker.closed was called
        ctx.broker.broadcast_local.assert_called()
        calls = ctx.broker.broadcast_local.call_args_list
        event_names = [call[0][0] for call in calls]
        assert "$circuit-breaker.closed" in event_names

    async def test_emits_opened_event_on_recovery_failure(self):
        """Test that $circuit-breaker.opened is emitted when recovery fails."""
        middleware = CircuitBreakerMiddleware(half_open_time=0)

        action = MagicMock()
        action.name = "test.action"

        async def mock_handler(ctx):
            raise TimeoutError("timeout")

        handler = await middleware.remote_action(mock_handler, action)

        # Manually set circuit to OPEN
        circuit = CircuitStatus()
        circuit.state = CircuitState.OPEN
        circuit.opened_at = time.time() - 10
        middleware._circuits["test.action"] = circuit

        # Create ctx with mocked broker
        ctx = MagicMock()
        ctx.broker = MagicMock()
        ctx.broker.node_id = "test-node"
        ctx.broker.broadcast_local = AsyncMock()

        # Call that will fail recovery
        with pytest.raises(asyncio.TimeoutError):
            await handler(ctx)

        # Verify $circuit-breaker.opened was called (after half-opened)
        ctx.broker.broadcast_local.assert_called()
        calls = ctx.broker.broadcast_local.call_args_list
        event_names = [call[0][0] for call in calls]
        assert "$circuit-breaker.half-opened" in event_names
        assert "$circuit-breaker.opened" in event_names

    async def test_event_payload_contains_action_info(self):
        """Test that event payload contains action and node info."""
        middleware = CircuitBreakerMiddleware(threshold=0.5, min_requests=2, half_open_time=100)

        action = MagicMock()
        action.name = "users.create"

        async def mock_handler(ctx):
            raise TimeoutError("timeout")

        handler = await middleware.remote_action(mock_handler, action)

        ctx = MagicMock()
        ctx.broker = MagicMock()
        ctx.broker.node_id = "node-123"
        ctx.broker.broadcast_local = AsyncMock()
        ctx.broker.logger = MagicMock()

        # Trip the circuit
        for _ in range(2):
            try:
                await handler(ctx)
            except TimeoutError:
                pass

        # Find the opened event call
        for call in ctx.broker.broadcast_local.call_args_list:
            if call[0][0] == "$circuit-breaker.opened":
                payload = call[0][1]
                assert payload["action"] == "users.create"
                assert payload["nodeID"] == "node-123"
                assert "state" in payload
                break
        else:
            pytest.fail("$circuit-breaker.opened event not found")

    async def test_no_event_emission_without_broker(self):
        """Test that missing broker doesn't cause errors."""
        middleware = CircuitBreakerMiddleware(threshold=0.5, min_requests=2, half_open_time=100)

        action = MagicMock()
        action.name = "test.action"

        async def mock_handler(ctx):
            raise TimeoutError("timeout")

        handler = await middleware.remote_action(mock_handler, action)

        # Create ctx without broker
        ctx = MagicMock(spec=["params"])  # No broker attribute

        # Trip the circuit - should not raise
        for _ in range(2):
            try:
                await handler(ctx)
            except TimeoutError:
                pass

        # Circuit should still trip
        assert middleware.get_circuit_state("test.action") == CircuitState.OPEN

    async def test_event_emission_error_doesnt_break_circuit_breaker(self):
        """Test that event emission errors don't break circuit breaker logic."""
        middleware = CircuitBreakerMiddleware(threshold=0.5, min_requests=2, half_open_time=100)

        action = MagicMock()
        action.name = "test.action"

        async def mock_handler(ctx):
            raise TimeoutError("timeout")

        handler = await middleware.remote_action(mock_handler, action)

        ctx = MagicMock()
        ctx.broker = MagicMock()
        ctx.broker.node_id = "test-node"
        ctx.broker.broadcast_local = AsyncMock(side_effect=RuntimeError("emit failed"))
        ctx.broker.logger = MagicMock()

        # Trip the circuit - should not raise RuntimeError
        for _ in range(2):
            try:
                await handler(ctx)
            except TimeoutError:
                pass

        # Circuit should still trip despite event emission error
        assert middleware.get_circuit_state("test.action") == CircuitState.OPEN
