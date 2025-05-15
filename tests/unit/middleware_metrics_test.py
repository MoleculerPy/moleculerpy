"""Unit tests for middleware metrics integration.

Tests RetryMiddleware, CircuitBreakerMiddleware, and BulkheadMiddleware
metrics collection.
"""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from moleculerpy.errors import QueueIsFullError
from moleculerpy.middleware.bulkhead import BulkheadMiddleware
from moleculerpy.middleware.circuit_breaker import CircuitBreakerMiddleware
from moleculerpy.middleware.metrics import MetricRegistry
from moleculerpy.middleware.retry import RetryMiddleware


class TestRetryMiddlewareMetrics:
    """Test RetryMiddleware metrics collection."""

    @pytest.fixture
    def registry(self):
        """Create metric registry."""
        return MetricRegistry()

    @pytest.fixture
    def middleware(self, registry):
        """Create RetryMiddleware with metrics."""
        return RetryMiddleware(
            max_retries=3,
            base_delay=0.01,
            registry=registry,
        )

    @pytest.fixture
    def mock_action(self):
        """Create mock action."""
        action = Mock(spec=["name"])
        action.name = "test.retry"
        return action

    @pytest.fixture
    def mock_ctx(self):
        """Create mock context."""
        return Mock()

    @pytest.mark.asyncio
    async def test_retry_metrics_on_success_after_retries(
        self, middleware, registry, mock_action, mock_ctx
    ):
        """Test that retry metrics are recorded on eventual success."""
        # Initialize middleware
        middleware.broker_created(Mock())

        # Handler that fails twice then succeeds
        call_count = 0

        async def handler(ctx):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise ConnectionError("Transient error")
            return "success"

        # Wrap handler
        wrapped = await middleware.remote_action(handler, mock_action)

        # Execute
        result = await wrapped(mock_ctx)
        assert result == "success"

        # Check retry_total counter (2 retry attempts, not counting initial)
        retry_total = registry._metrics["moleculer_retry_total"]
        assert retry_total.get(labels={"action": "test.retry", "attempt": "1"}) == 1.0
        assert retry_total.get(labels={"action": "test.retry", "attempt": "2"}) == 1.0

        # Check retry_attempts histogram (total 3 attempts)
        retry_attempts = registry._metrics["moleculer_retry_attempts"]
        assert retry_attempts.get_count(labels={"action": "test.retry"}) == 1
        assert retry_attempts.get_sum(labels={"action": "test.retry"}) == 3.0

    @pytest.mark.asyncio
    async def test_retry_metrics_on_final_failure(
        self, middleware, registry, mock_action, mock_ctx
    ):
        """Test that retry metrics are recorded on final failure."""
        # Initialize middleware
        middleware.broker_created(Mock())

        # Handler that always fails
        async def handler(ctx):
            raise ConnectionError("Permanent error")

        # Wrap handler
        wrapped = await middleware.remote_action(handler, mock_action)

        # Execute (should raise after max retries)
        with pytest.raises(ConnectionError):
            await wrapped(mock_ctx)

        # Check retry_total counter (3 retry attempts)
        retry_total = registry._metrics["moleculer_retry_total"]
        assert retry_total.get(labels={"action": "test.retry", "attempt": "1"}) == 1.0
        assert retry_total.get(labels={"action": "test.retry", "attempt": "2"}) == 1.0
        assert retry_total.get(labels={"action": "test.retry", "attempt": "3"}) == 1.0

        # Check retry_attempts histogram (total 4 attempts: initial + 3 retries)
        retry_attempts = registry._metrics["moleculer_retry_attempts"]
        assert retry_attempts.get_count(labels={"action": "test.retry"}) == 1
        assert retry_attempts.get_sum(labels={"action": "test.retry"}) == 4.0

    @pytest.mark.asyncio
    async def test_no_metrics_without_registry(self, mock_action, mock_ctx):
        """Test that middleware works without metrics registry."""
        middleware = RetryMiddleware(max_retries=2, base_delay=0.01)

        # No broker_created call, metrics should be None
        assert middleware._retry_total is None
        assert middleware._retry_attempts is None

        # Handler that fails once
        call_count = 0

        async def handler(ctx):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("Error")
            return "success"

        wrapped = await middleware.remote_action(handler, mock_action)
        result = await wrapped(mock_ctx)
        assert result == "success"


class TestCircuitBreakerMiddlewareMetrics:
    """Test CircuitBreakerMiddleware metrics collection."""

    @pytest.fixture
    def registry(self):
        """Create metric registry."""
        return MetricRegistry()

    @pytest.fixture
    def middleware(self, registry):
        """Create CircuitBreakerMiddleware with metrics."""
        return CircuitBreakerMiddleware(
            threshold=0.5,
            window_time=10,
            min_requests=2,
            half_open_time=1,
            registry=registry,
        )

    @pytest.fixture
    def mock_action(self):
        """Create mock action."""
        action = Mock(spec=["name"])
        action.name = "test.circuit"
        return action

    @pytest.fixture
    def mock_ctx(self):
        """Create mock context."""
        ctx = Mock()
        ctx.broker = Mock()
        ctx.broker.node_id = "test-node"
        ctx.broker.broadcast_local = AsyncMock()
        return ctx

    @pytest.mark.asyncio
    async def test_circuit_breaker_state_metric(self, middleware, registry, mock_action, mock_ctx):
        """Test circuit state gauge updates."""
        # Initialize middleware
        middleware.broker_created(Mock())

        # Handler that fails
        async def failing_handler(ctx):
            raise ConnectionError("Service down")

        wrapped = await middleware.remote_action(failing_handler, mock_action)

        # Initial state: CLOSED (0)
        state_gauge = registry._metrics["moleculer_circuit_breaker_state"]
        assert state_gauge.get(labels={"action": "test.circuit"}) == 0.0

        # Trip circuit (2 failures)
        with pytest.raises(ConnectionError):
            await wrapped(mock_ctx)
        with pytest.raises(ConnectionError):
            await wrapped(mock_ctx)

        # State should be OPEN (2)
        assert state_gauge.get(labels={"action": "test.circuit"}) == 2.0

    @pytest.mark.asyncio
    async def test_circuit_opened_counter(self, middleware, registry, mock_action, mock_ctx):
        """Test circuit opened counter increments."""
        # Initialize middleware
        middleware.broker_created(Mock())

        async def failing_handler(ctx):
            raise ConnectionError("Service down")

        wrapped = await middleware.remote_action(failing_handler, mock_action)

        # Trip circuit
        with pytest.raises(ConnectionError):
            await wrapped(mock_ctx)
        with pytest.raises(ConnectionError):
            await wrapped(mock_ctx)

        # Check opened counter
        opened_counter = registry._metrics["moleculer_circuit_breaker_opened_total"]
        assert opened_counter.get(labels={"action": "test.circuit"}) == 1.0

    @pytest.mark.asyncio
    async def test_circuit_closed_counter(self, middleware, registry, mock_action, mock_ctx):
        """Test circuit closed counter increments on recovery."""
        # Initialize middleware
        middleware.broker_created(Mock())

        # Handler that fails then succeeds
        fail_next = True

        async def handler(ctx):
            nonlocal fail_next
            if fail_next:
                raise ConnectionError("Error")
            return "success"

        wrapped = await middleware.remote_action(handler, mock_action)

        # Trip circuit (2 failures)
        with pytest.raises(ConnectionError):
            await wrapped(mock_ctx)
        with pytest.raises(ConnectionError):
            await wrapped(mock_ctx)

        # Wait for half-open
        await asyncio.sleep(1.1)

        # Recover
        fail_next = False
        result = await wrapped(mock_ctx)
        assert result == "success"

        # Check closed counter
        closed_counter = registry._metrics["moleculer_circuit_breaker_closed_total"]
        assert closed_counter.get(labels={"action": "test.circuit"}) == 1.0


class TestBulkheadMiddlewareMetrics:
    """Test BulkheadMiddleware metrics collection."""

    @pytest.fixture
    def registry(self):
        """Create metric registry."""
        return MetricRegistry()

    @pytest.fixture
    def middleware(self, registry):
        """Create BulkheadMiddleware with metrics."""
        return BulkheadMiddleware(
            concurrency=2,
            max_queue_size=3,
            registry=registry,
        )

    @pytest.fixture
    def mock_action(self):
        """Create mock action."""
        action = Mock(spec=["name"])
        action.name = "test.bulkhead"
        return action

    @pytest.fixture
    def mock_ctx(self):
        """Create mock context."""
        return Mock()

    @pytest.mark.asyncio
    async def test_bulkhead_in_flight_metric(self, middleware, registry, mock_action, mock_ctx):
        """Test in_flight gauge updates."""
        # Initialize middleware
        middleware.broker_created(Mock())

        # Slow handler
        async def slow_handler(ctx):
            await asyncio.sleep(0.1)
            return "done"

        wrapped = await middleware.local_action(slow_handler, mock_action)

        # Start 2 concurrent requests (at limit)
        task1 = asyncio.create_task(wrapped(mock_ctx))
        task2 = asyncio.create_task(wrapped(mock_ctx))

        # Give them time to start
        await asyncio.sleep(0.01)

        # Check in_flight gauge
        in_flight_gauge = registry._metrics["moleculer_bulkhead_in_flight"]
        assert in_flight_gauge.get(labels={"action": "test.bulkhead"}) == 2.0

        # Wait for completion
        await task1
        await task2

        # Should be back to 0
        assert in_flight_gauge.get(labels={"action": "test.bulkhead"}) == 0.0

    @pytest.mark.asyncio
    async def test_bulkhead_queue_size_metric(self, middleware, registry, mock_action, mock_ctx):
        """Test queue_size gauge updates."""
        # Initialize middleware
        middleware.broker_created(Mock())

        # Block handler execution
        blocker = asyncio.Event()

        async def blocked_handler(ctx):
            await blocker.wait()
            return "done"

        wrapped = await middleware.local_action(blocked_handler, mock_action)

        # Start 2 requests (fills concurrency)
        task1 = asyncio.create_task(wrapped(mock_ctx))
        task2 = asyncio.create_task(wrapped(mock_ctx))

        # Give them time to start
        await asyncio.sleep(0.01)

        # Start 2 more (should queue)
        task3 = asyncio.create_task(wrapped(mock_ctx))
        task4 = asyncio.create_task(wrapped(mock_ctx))

        await asyncio.sleep(0.01)

        # Check queue size
        queue_gauge = registry._metrics["moleculer_bulkhead_queue_size"]
        assert queue_gauge.get(labels={"action": "test.bulkhead"}) == 2.0

        # Unblock and wait
        blocker.set()
        await asyncio.gather(task1, task2, task3, task4)

        # Queue should be empty
        assert queue_gauge.get(labels={"action": "test.bulkhead"}) == 0.0

    @pytest.mark.asyncio
    async def test_bulkhead_rejected_counter(self, middleware, registry, mock_action, mock_ctx):
        """Test rejected counter increments on queue overflow."""
        # Initialize middleware
        middleware.broker_created(Mock())

        # Block handler
        blocker = asyncio.Event()

        async def blocked_handler(ctx):
            await blocker.wait()
            return "done"

        wrapped = await middleware.local_action(blocked_handler, mock_action)

        # Fill concurrency (2) + queue (3) = 5 requests
        tasks = [asyncio.create_task(wrapped(mock_ctx)) for _ in range(5)]

        await asyncio.sleep(0.01)

        # 6th request should be rejected
        with pytest.raises(QueueIsFullError):
            await wrapped(mock_ctx)

        # Check rejected counter
        rejected_counter = registry._metrics["moleculer_bulkhead_rejected_total"]
        assert rejected_counter.get(labels={"action": "test.bulkhead"}) == 1.0

        # Cleanup
        blocker.set()
        await asyncio.gather(*tasks)
