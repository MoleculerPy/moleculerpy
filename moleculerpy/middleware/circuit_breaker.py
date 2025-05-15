"""Circuit Breaker Middleware for the MoleculerPy framework.

This module implements the Circuit Breaker pattern to prevent cascade failures
in distributed systems. When a service is failing, the circuit breaker "trips"
and fails fast, allowing the service time to recover.

States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Tripped state, requests fail immediately
    - HALF_OPEN: Recovery testing, allows one request through

Example usage:
    from moleculerpy import ServiceBroker, Settings
    from moleculerpy.middleware import CircuitBreakerMiddleware

    settings = Settings(
        middlewares=[
            CircuitBreakerMiddleware(
                threshold=0.5,      # 50% failure rate to trip
                window_time=60,     # 60 second sliding window
                min_requests=10,    # Minimum calls before checking
                half_open_time=10,  # Seconds before testing recovery
            )
        ]
    )

    broker = ServiceBroker(id="my-node", settings=settings)
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar

from moleculerpy.errors import MoleculerClientError, RemoteCallError
from moleculerpy.middleware.base import HandlerType, Middleware

if TYPE_CHECKING:
    from moleculerpy.middleware.metrics import Counter, Gauge, MetricRegistry
    from moleculerpy.protocols import ContextProtocol


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing fast
    HALF_OPEN = "half_open"  # Testing recovery
    HALF_OPEN_WAIT = "half_open_wait"  # Testing in progress (anti-stick)


@dataclass
class CallResult:
    """Record of a single call result."""

    timestamp: float
    success: bool


@dataclass
class CircuitStatus:
    """Status of a circuit for a specific action.

    Attributes:
        state: Current circuit state (CLOSED, OPEN, HALF_OPEN, HALF_OPEN_WAIT)
        results: Sliding window of recent call results
        opened_at: Timestamp when circuit was opened (tripped)
        state_lock: Lock for atomic state transitions (replaces half_open_lock)
        timeout_task: Task for anti-stick timeout in HALF_OPEN_WAIT state
        failure_count: Number of failures in current window
        success_count: Number of successes in current window
    """

    state: CircuitState = CircuitState.CLOSED
    results: deque[CallResult] = field(default_factory=lambda: deque(maxlen=1000))
    opened_at: float | None = None
    state_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    timeout_task: asyncio.Task[None] | None = None
    failure_count: int = 0
    success_count: int = 0


class CircuitBreakerError(MoleculerClientError):
    """Exception raised when circuit breaker is open.

    Attributes:
        action_name: Name of the action that was blocked
        state: Current circuit state
        time_until_half_open: Seconds until circuit enters HALF_OPEN state
    """

    def __init__(
        self,
        action_name: str,
        state: CircuitState,
        time_until_half_open: float | None = None,
    ) -> None:
        self.action_name = action_name
        self.state = state
        self.time_until_half_open = time_until_half_open

        message = f"Circuit breaker is {state.value} for action '{action_name}'"
        if time_until_half_open is not None and time_until_half_open > 0:
            message += f" (testing in {time_until_half_open:.1f}s)"

        super().__init__(
            message=message,
            code=503,
            type="CIRCUIT_BREAKER_OPEN",
            data={"action": action_name, "state": state.value},
        )


class CircuitBreakerMiddleware(Middleware):
    """Middleware that implements the Circuit Breaker pattern.

    Monitors failure rates and "trips" when failures exceed the threshold,
    preventing cascade failures by failing fast.

    Emits internal events (Moleculer.js compatible):
        - $circuit-breaker.opened: When circuit trips to OPEN state
        - $circuit-breaker.closed: When circuit recovers to CLOSED state
        - $circuit-breaker.half-opened: When circuit transitions to HALF_OPEN

    Attributes:
        threshold: Failure rate threshold to trip circuit (0.0 to 1.0)
        window_time: Time window for calculating failure rate (seconds)
        min_requests: Minimum requests before considering tripping
        half_open_time: Time to wait before testing recovery (seconds)
        trip_errors: Set of exception types that count as failures
    """

    DEFAULT_TRIP_ERRORS: ClassVar[set[type[Exception]]] = {
        asyncio.TimeoutError,
        RemoteCallError,
        ConnectionError,
        OSError,
    }

    def __init__(
        self,
        threshold: float = 0.5,
        window_time: int = 60,
        min_requests: int = 10,
        half_open_time: int = 10,
        trip_errors: set[type[Exception]] | None = None,
        logger: logging.Logger | None = None,
        enabled: bool = True,
        check: Callable[[Exception], bool] | None = None,
        registry: MetricRegistry | None = None,
    ) -> None:
        """Initialize the CircuitBreakerMiddleware.

        Args:
            threshold: Failure rate (0.0-1.0) at which to trip the circuit
            window_time: Sliding window size in seconds for failure rate calculation
            min_requests: Minimum number of requests before considering tripping
            half_open_time: Seconds to wait in OPEN state before testing
            trip_errors: Set of exception types that count as failures
            logger: Optional logger for circuit breaker events
            enabled: Whether circuit breaker is enabled (default True)
            check: Custom callback to determine if error counts as failure.
                   Receives the exception, returns True if it should count.
                   If None, uses trip_errors type checking (default).
            registry: Optional MetricRegistry for collecting circuit breaker metrics
        """
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("threshold must be between 0.0 and 1.0")

        self.enabled = enabled
        self.threshold = threshold
        self.window_time = window_time
        self.min_requests = min_requests
        self.half_open_time = half_open_time
        self.trip_errors = trip_errors or self.DEFAULT_TRIP_ERRORS
        self.check = check
        self.logger = logger or logging.getLogger("moleculerpy.middleware.circuit_breaker")
        self.registry = registry

        # Per-action circuit status
        self._circuits: dict[str, CircuitStatus] = {}
        self._circuits_lock = asyncio.Lock()

        # Metrics (created when broker starts)
        self._circuit_state: Gauge | None = None
        self._circuit_opened_total: Counter | None = None
        self._circuit_closed_total: Counter | None = None
        self._circuit_half_opened_total: Counter | None = None

    def broker_created(self, broker: Any) -> None:
        """Initialize metrics when broker is created.

        Args:
            broker: The broker instance
        """
        if self.registry is None:
            return

        # Create circuit breaker metrics
        self._circuit_state = self.registry.gauge(
            "moleculer_circuit_breaker_state",
            "Circuit breaker state (0=closed, 1=half_open, 2=open)",
            label_names=("action",),
        )

        self._circuit_opened_total = self.registry.counter(
            "moleculer_circuit_breaker_opened_total",
            "Total number of times circuit breaker opened",
            label_names=("action",),
        )

        self._circuit_closed_total = self.registry.counter(
            "moleculer_circuit_breaker_closed_total",
            "Total number of times circuit breaker closed",
            label_names=("action",),
        )

        self._circuit_half_opened_total = self.registry.counter(
            "moleculer_circuit_breaker_half_opened_total",
            "Total number of times circuit breaker entered half-open state",
            label_names=("action",),
        )

    async def _get_circuit(self, action_name: str) -> CircuitStatus:
        """Get or create circuit status for an action.

        Args:
            action_name: Name of the action

        Returns:
            CircuitStatus for the action
        """
        if action_name not in self._circuits:
            async with self._circuits_lock:
                if action_name not in self._circuits:
                    self._circuits[action_name] = CircuitStatus()
        return self._circuits[action_name]

    def _is_trip_error(self, error: Exception) -> bool:
        """Check if an error should count toward circuit tripping.

        Uses custom check callback if provided, otherwise falls back to
        type-based checking via trip_errors.

        Args:
            error: The exception that was raised

        Returns:
            True if this error counts as a failure
        """
        # If custom check callback is provided, use it
        if self.check is not None:
            try:
                return self.check(error)
            except Exception as check_error:
                # If check callback fails, fall through to default behavior
                self.logger.warning(
                    "Custom circuit-breaker check failed for %s: %s. Falling back to trip_errors.",
                    type(error).__name__,
                    check_error,
                )

        # Default: check against trip_errors types
        return any(isinstance(error, err_type) for err_type in self.trip_errors)

    def _cleanup_old_results(self, circuit: CircuitStatus) -> None:
        """Remove results older than window_time from the sliding window.

        Args:
            circuit: Circuit status to clean up
        """
        cutoff = time.time() - self.window_time

        # Remove old results from the deque
        while circuit.results and circuit.results[0].timestamp < cutoff:
            old_result = circuit.results.popleft()
            if old_result.success:
                circuit.success_count -= 1
            else:
                circuit.failure_count -= 1

    def _calculate_failure_rate(self, circuit: CircuitStatus) -> float:
        """Calculate current failure rate for a circuit.

        Args:
            circuit: Circuit status to calculate rate for

        Returns:
            Failure rate as a float between 0.0 and 1.0
        """
        total = circuit.failure_count + circuit.success_count
        if total == 0:
            return 0.0
        return circuit.failure_count / total

    def _record_result(self, circuit: CircuitStatus, success: bool) -> None:
        """Record a call result in the sliding window.

        Args:
            circuit: Circuit status to update
            success: Whether the call succeeded
        """
        result = CallResult(timestamp=time.time(), success=success)
        circuit.results.append(result)

        if success:
            circuit.success_count += 1
        else:
            circuit.failure_count += 1

    def _should_trip(self, circuit: CircuitStatus) -> bool:
        """Determine if circuit should trip based on failure rate.

        Args:
            circuit: Circuit status to check

        Returns:
            True if circuit should trip to OPEN state
        """
        total = circuit.failure_count + circuit.success_count

        # Not enough data to make a decision
        if total < self.min_requests:
            return False

        failure_rate = self._calculate_failure_rate(circuit)
        return failure_rate >= self.threshold

    def _should_test_recovery(self, circuit: CircuitStatus) -> bool:
        """Check if circuit should transition to HALF_OPEN for testing.

        Args:
            circuit: Circuit status to check

        Returns:
            True if enough time has passed to test recovery
        """
        if circuit.state != CircuitState.OPEN:
            return False

        if circuit.opened_at is None:
            return True

        elapsed = time.time() - circuit.opened_at
        return elapsed >= self.half_open_time

    def _update_circuit_state_metric(self, action_name: str, state: CircuitState) -> None:
        """Update circuit state gauge metric.

        Args:
            action_name: Name of the action
            state: New circuit state
        """
        if self._circuit_state is None:
            return

        # Map state to numeric value
        state_value = {
            CircuitState.CLOSED: 0,
            CircuitState.HALF_OPEN: 1,
            CircuitState.HALF_OPEN_WAIT: 1,  # Same as HALF_OPEN
            CircuitState.OPEN: 2,
        }.get(state, 0)

        self._circuit_state.set(
            state_value,
            labels={"action": action_name},
        )

    def _trip_circuit(self, circuit: CircuitStatus, action_name: str) -> None:
        """Trip the circuit to OPEN state.

        Args:
            circuit: Circuit status to trip
            action_name: Name of the action (for metrics)
        """
        circuit.state = CircuitState.OPEN
        circuit.opened_at = time.time()

        # Update metrics
        if self._circuit_opened_total:
            self._circuit_opened_total.inc(labels={"action": action_name})
        self._update_circuit_state_metric(action_name, CircuitState.OPEN)

    def _close_circuit(self, circuit: CircuitStatus, action_name: str) -> None:
        """Close the circuit after successful recovery.

        Args:
            circuit: Circuit status to close
            action_name: Name of the action (for metrics)
        """
        circuit.state = CircuitState.CLOSED
        circuit.opened_at = None
        # Clear old results to start fresh
        circuit.results.clear()
        circuit.failure_count = 0
        circuit.success_count = 0

        # Update metrics
        if self._circuit_closed_total:
            self._circuit_closed_total.inc(labels={"action": action_name})
        self._update_circuit_state_metric(action_name, CircuitState.CLOSED)

    def get_circuit_state(self, action_name: str) -> CircuitState | None:
        """Get the current state of a circuit (for monitoring).

        Args:
            action_name: Name of the action

        Returns:
            Current CircuitState or None if no circuit exists
        """
        circuit = self._circuits.get(action_name)
        return circuit.state if circuit else None

    def get_circuit_stats(self, action_name: str) -> dict[str, Any] | None:
        """Get statistics for a circuit (for monitoring).

        Args:
            action_name: Name of the action

        Returns:
            Dictionary with circuit statistics or None
        """
        circuit = self._circuits.get(action_name)
        if not circuit:
            return None

        total = circuit.failure_count + circuit.success_count
        failure_rate = self._calculate_failure_rate(circuit) if total > 0 else 0.0

        return {
            "state": circuit.state.value,
            "failure_count": circuit.failure_count,
            "success_count": circuit.success_count,
            "total_requests": total,
            "failure_rate": failure_rate,
            "opened_at": circuit.opened_at,
        }

    async def _half_open_timeout(
        self, circuit: CircuitStatus, action_name: str, timeout: float = 30.0
    ) -> None:
        """Anti-stick protection: reset to OPEN if HALF_OPEN_WAIT takes too long.

        This prevents the circuit from being stuck in HALF_OPEN_WAIT if the
        request hangs or never completes.

        Args:
            circuit: Circuit status being monitored
            action_name: Name of the action (for logging)
            timeout: Maximum time to wait in HALF_OPEN_WAIT state
        """
        try:
            await asyncio.sleep(timeout)
            async with circuit.state_lock:
                if circuit.state == CircuitState.HALF_OPEN_WAIT:
                    # Request took too long, trip back to OPEN
                    self._trip_circuit(circuit, action_name)
        except asyncio.CancelledError:
            # Normal cancellation when request completes
            pass

    def _cancel_timeout_task(self, circuit: CircuitStatus) -> None:
        """Cancel the anti-stick timeout task if running.

        Args:
            circuit: Circuit status with potential timeout task
        """
        if circuit.timeout_task and not circuit.timeout_task.done():
            circuit.timeout_task.cancel()
        circuit.timeout_task = None

    async def _emit_circuit_event(
        self, ctx: ContextProtocol, event: str, action_name: str, circuit: CircuitStatus
    ) -> None:
        """Emit circuit breaker internal event.

        Args:
            ctx: Request context with broker reference
            event: Event name ($circuit-breaker.opened/closed/half-opened)
            action_name: Name of the action
            circuit: Circuit status for stats
        """
        # Safely get broker - handle missing attribute (for testing with spec mocks)
        broker = getattr(ctx, "broker", None)
        if broker is None:
            return

        if not hasattr(broker, "broadcast_local"):
            return

        # Calculate stats for Moleculer.js compatible payload
        total_count = circuit.failure_count + circuit.success_count
        failure_rate = self._calculate_failure_rate(circuit) if total_count > 0 else 0.0

        # Extract service name from action (e.g., "posts.create" -> "posts")
        service_name = action_name.rsplit(".", 1)[0] if "." in action_name else action_name

        payload = {
            "action": action_name,
            "nodeID": getattr(broker, "node_id", None),
            "service": service_name,
            "state": circuit.state.value,
            "failures": circuit.failure_count,
            "count": total_count,
            "rate": failure_rate,
        }

        try:
            await broker.broadcast_local(event, payload)
        except Exception as e:
            # Don't let event emission break circuit breaker logic
            self.logger.debug(f"Failed to emit {event}: {e}")

    async def remote_action(self, next_handler: HandlerType[Any], action: Any) -> HandlerType[Any]:
        """Hook for remote action middleware processing with circuit breaker.

        Implements the circuit breaker pattern for remote calls with atomic
        state transitions to prevent race conditions.

        Emits internal events on state transitions:
            - $circuit-breaker.half-opened: OPEN → HALF_OPEN
            - $circuit-breaker.closed: HALF_OPEN_WAIT → CLOSED (on success)
            - $circuit-breaker.opened: CLOSED → OPEN or HALF_OPEN_WAIT → OPEN

        Args:
            next_handler: The next handler in the chain
            action: The action object being called

        Returns:
            A wrapped handler that implements circuit breaker logic
        """
        # If disabled, pass through to next handler without any circuit breaker logic
        if not self.enabled:
            return next_handler

        action_name = getattr(action, "name", str(action))

        async def handler(ctx: ContextProtocol) -> Any:
            circuit = await self._get_circuit(action_name)

            # Clean up old results from sliding window
            self._cleanup_old_results(circuit)

            # Track events to emit outside lock
            emit_half_opened = False

            # Atomic state check and transition under single lock
            start_timeout_task = False
            async with circuit.state_lock:
                if circuit.state == CircuitState.OPEN:
                    if not self._should_test_recovery(circuit):
                        # Still in cooldown period
                        time_remaining = None
                        if circuit.opened_at:
                            time_remaining = max(
                                0, self.half_open_time - (time.time() - circuit.opened_at)
                            )
                        raise CircuitBreakerError(action_name, circuit.state, time_remaining)

                    # Transition to HALF_OPEN (ready to test)
                    circuit.state = CircuitState.HALF_OPEN
                    emit_half_opened = True

                    # Update metrics for half-open transition
                    if self._circuit_half_opened_total:
                        self._circuit_half_opened_total.inc(labels={"action": action_name})
                    self._update_circuit_state_metric(action_name, CircuitState.HALF_OPEN)

                if circuit.state == CircuitState.HALF_OPEN:
                    # Atomically transition to HALF_OPEN_WAIT (test in progress)
                    circuit.state = CircuitState.HALF_OPEN_WAIT
                    # Mark that we need to start timeout task (outside lock)
                    start_timeout_task = True

                elif circuit.state == CircuitState.HALF_OPEN_WAIT:
                    # Another request is already testing, fail fast
                    time_remaining = None
                    if circuit.opened_at:
                        time_remaining = max(
                            0, self.half_open_time - (time.time() - circuit.opened_at)
                        )
                    raise CircuitBreakerError(action_name, CircuitState.OPEN, time_remaining)

            # Emit half-opened event outside lock
            if emit_half_opened:
                await self._emit_circuit_event(
                    ctx, "$circuit-breaker.half-opened", action_name, circuit
                )

            # Start anti-stick timeout OUTSIDE lock (create_task is relatively heavy)
            if start_timeout_task:
                circuit.timeout_task = asyncio.create_task(
                    self._half_open_timeout(circuit, action_name),
                    name=f"moleculerpy:cb-timeout:{action_name}",
                )

            # Track if we need to cleanup timeout task in finally
            should_cleanup_timeout = circuit.state == CircuitState.HALF_OPEN_WAIT

            try:
                result = await next_handler(ctx)

                # Handle success - track if we closed the circuit
                emit_closed = False
                async with circuit.state_lock:
                    if circuit.state == CircuitState.HALF_OPEN_WAIT:
                        # Recovery test succeeded, close circuit
                        self._cancel_timeout_task(circuit)
                        self._close_circuit(circuit, action_name)
                        emit_closed = True
                    else:
                        # Normal CLOSED operation
                        self._record_result(circuit, success=True)

                # Emit closed event outside lock
                if emit_closed:
                    await self._emit_circuit_event(
                        ctx, "$circuit-breaker.closed", action_name, circuit
                    )

                return result

            except Exception as e:
                # Track if we need to emit opened event
                emit_opened = False
                async with circuit.state_lock:
                    if self._is_trip_error(e):
                        if circuit.state == CircuitState.HALF_OPEN_WAIT:
                            # Recovery test failed, go back to OPEN
                            self._cancel_timeout_task(circuit)
                            self._trip_circuit(circuit, action_name)
                            emit_opened = True
                        else:
                            # Normal CLOSED operation - record failure
                            self._record_result(circuit, success=False)

                            # Check if we should trip the circuit
                            if self._should_trip(circuit):
                                self._trip_circuit(circuit, action_name)
                                emit_opened = True

                                # Log the trip
                                failure_rate = self._calculate_failure_rate(circuit)
                                self.logger.warning(
                                    f"Circuit breaker tripped for {action_name} "
                                    f"(failure rate: {failure_rate:.1%})"
                                )

                # Emit opened event outside lock (before re-raising)
                if emit_opened:
                    await self._emit_circuit_event(
                        ctx, "$circuit-breaker.opened", action_name, circuit
                    )

                raise
            finally:
                # Always cleanup timeout task if we were in HALF_OPEN_WAIT
                if should_cleanup_timeout:
                    self._cancel_timeout_task(circuit)

        return handler
