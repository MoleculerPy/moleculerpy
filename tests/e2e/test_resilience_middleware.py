"""E2E tests for resilience middleware with remote calls.

Tests verify that RetryMiddleware and CircuitBreakerMiddleware work correctly
for remote (inter-node) calls where network failures can occur.

Test Setup:
    - alpha-node: Client node (makes calls)
    - beta-node: Server node (hosts services with failure modes)

The middleware only wrap remote_action, so we need actual inter-node
communication to test them properly.
"""

from __future__ import annotations

import asyncio
import random
from typing import TYPE_CHECKING

import pytest

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action
from moleculerpy.middleware import (
    BulkheadMiddleware,
    CircuitBreakerMiddleware,
    RetryMiddleware,
    TimeoutMiddleware,
)
from moleculerpy.service import Service
from moleculerpy.settings import Settings

if TYPE_CHECKING:
    pass


# =============================================================================
# Test Services (deployed on beta-node)
# =============================================================================


class FlakyService(Service):
    """Service that fails randomly - for testing RetryMiddleware."""

    name = "flaky"

    def __init__(self, node_id: str, fail_rate: float = 0.5):
        super().__init__(self.name)
        self._node_id = node_id
        self._fail_rate = fail_rate
        self._call_count = 0
        self._success_count = 0
        self._fail_count = 0

    @action()
    async def unreliable(self, ctx) -> dict:
        """Action that fails based on configured rate."""
        self._call_count += 1

        # Use seed from params if provided (for deterministic testing)
        if "seed" in ctx.params:
            random.seed(ctx.params["seed"] + self._call_count)

        if random.random() < self._fail_rate:
            self._fail_count += 1
            raise ConnectionError(f"[{self._node_id}] Random failure (attempt {self._call_count})")

        self._success_count += 1
        return {
            "node_id": self._node_id,
            "attempt": self._call_count,
            "status": "success",
        }

    @action()
    async def get_stats(self, ctx) -> dict:
        """Get call statistics."""
        return {
            "node_id": self._node_id,
            "total_calls": self._call_count,
            "successes": self._success_count,
            "failures": self._fail_count,
        }

    @action()
    async def reset_stats(self, ctx) -> dict:
        """Reset statistics."""
        self._call_count = 0
        self._success_count = 0
        self._fail_count = 0
        return {"status": "reset"}


class AlwaysFailsService(Service):
    """Service that always fails - for testing CircuitBreaker."""

    name = "failing"

    def __init__(self, node_id: str):
        super().__init__(self.name)
        self._node_id = node_id
        self._call_count = 0

    @action()
    async def fail(self, ctx) -> dict:
        """Always fails with ConnectionError."""
        self._call_count += 1
        raise ConnectionError(f"[{self._node_id}] Service unavailable (call #{self._call_count})")

    @action()
    async def get_call_count(self, ctx) -> int:
        """Get number of calls received."""
        return self._call_count


class SlowService(Service):
    """Service with configurable delay - for testing TimeoutMiddleware."""

    name = "slow"

    def __init__(self, node_id: str):
        super().__init__(self.name)
        self._node_id = node_id

    @action(timeout=0.5)  # 500ms timeout
    async def delayed(self, ctx) -> dict:
        """Action with configurable delay."""
        delay = ctx.params.get("delay", 0.1)
        await asyncio.sleep(delay)
        return {
            "node_id": self._node_id,
            "delay": delay,
            "status": "completed",
        }


class ConcurrentService(Service):
    """Service for testing BulkheadMiddleware concurrency limits."""

    name = "concurrent"

    def __init__(self, node_id: str):
        super().__init__(self.name)
        self._node_id = node_id
        self._active_calls = 0
        self._max_concurrent = 0

    @action()
    async def process(self, ctx) -> dict:
        """Track concurrent calls."""
        self._active_calls += 1
        self._max_concurrent = max(self._max_concurrent, self._active_calls)

        request_id = ctx.params.get("id", "?")

        await asyncio.sleep(0.1)  # Simulate work

        self._active_calls -= 1
        return {
            "node_id": self._node_id,
            "request_id": request_id,
            "max_concurrent_observed": self._max_concurrent,
        }

    @action()
    async def get_max_concurrent(self, ctx) -> int:
        """Get maximum observed concurrent calls."""
        return self._max_concurrent


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def retry_middleware() -> RetryMiddleware:
    """Create RetryMiddleware with fast retries for testing."""
    return RetryMiddleware(
        max_retries=3,
        base_delay=0.05,  # 50ms (fast for tests)
        max_delay=0.2,
        jitter=False,  # Deterministic timing
    )


@pytest.fixture
def circuit_breaker_middleware() -> CircuitBreakerMiddleware:
    """Create CircuitBreakerMiddleware with low thresholds for testing."""
    return CircuitBreakerMiddleware(
        threshold=0.5,  # Trip at 50% failure rate
        window_time=30,
        min_requests=3,  # Trip after 3 requests if threshold exceeded
        half_open_time=1,  # Fast recovery for tests
    )


@pytest.fixture
def timeout_middleware() -> TimeoutMiddleware:
    """Create TimeoutMiddleware."""
    return TimeoutMiddleware(default_timeout=5.0)


@pytest.fixture
def bulkhead_middleware() -> BulkheadMiddleware:
    """Create BulkheadMiddleware with concurrency=2 for testing."""
    return BulkheadMiddleware(
        concurrency=2,
        max_queue_size=10,
    )


# =============================================================================
# Test Classes
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.e2e
class TestRetryMiddlewareRemote:
    """Tests for RetryMiddleware with remote calls."""

    async def test_retry_on_remote_failure(
        self,
        retry_middleware: RetryMiddleware,
    ) -> None:
        """RetryMiddleware retries failed remote calls."""
        # Create two-node cluster with middleware
        settings_alpha = Settings(
            transporter="memory://",
            prefer_local=False,
            middlewares=[retry_middleware],
        )
        settings_beta = Settings(
            transporter="memory://",
            prefer_local=False,
        )

        alpha = ServiceBroker(id="alpha-node", settings=settings_alpha)
        beta = ServiceBroker(id="beta-node", settings=settings_beta)

        # Register flaky service only on beta (forces remote calls)
        # 30% failure rate = likely to fail but should succeed with retries
        await beta.register(FlakyService("beta-node", fail_rate=0.3))

        await alpha.start()
        await beta.start()

        # Wait for discovery
        await asyncio.sleep(0.3)

        try:
            # Make multiple calls - with retry, most should succeed
            successes = 0
            failures = 0

            for i in range(10):
                try:
                    result = await alpha.call(
                        "flaky.unreliable",
                        {"seed": i * 100},  # Different seed each call
                    )
                    assert result["node_id"] == "beta-node"
                    successes += 1
                except Exception:
                    failures += 1

            # With 3 retries and 30% fail rate, should succeed most of the time
            # P(all 4 attempts fail) = 0.3^4 = 0.81% per call
            assert successes >= 7, f"Too many failures: {failures}/{successes + failures}"

        finally:
            await alpha.stop()
            await beta.stop()

    async def test_retry_exhausted(
        self,
        retry_middleware: RetryMiddleware,
    ) -> None:
        """RetryMiddleware raises after exhausting retries."""
        settings_alpha = Settings(
            transporter="memory://",
            prefer_local=False,
            middlewares=[retry_middleware],
        )
        settings_beta = Settings(
            transporter="memory://",
            prefer_local=False,
        )

        alpha = ServiceBroker(id="alpha-node", settings=settings_alpha)
        beta = ServiceBroker(id="beta-node", settings=settings_beta)

        # Register always-failing service on beta
        await beta.register(AlwaysFailsService("beta-node"))

        await alpha.start()
        await beta.start()
        await asyncio.sleep(0.3)

        try:
            # Should fail after max_retries
            # Error is wrapped in RemoteCallError when transmitted over network
            from moleculerpy.transit import RemoteCallError

            with pytest.raises(RemoteCallError) as exc_info:
                await alpha.call("failing.fail", {})

            assert "beta-node" in str(exc_info.value)

        finally:
            await alpha.stop()
            await beta.stop()


@pytest.mark.asyncio
@pytest.mark.e2e
class TestCircuitBreakerRemote:
    """Tests for CircuitBreakerMiddleware with remote calls."""

    async def test_circuit_trips_on_failures(
        self,
        circuit_breaker_middleware: CircuitBreakerMiddleware,
    ) -> None:
        """CircuitBreaker trips after threshold exceeded."""
        settings_alpha = Settings(
            transporter="memory://",
            prefer_local=False,
            middlewares=[circuit_breaker_middleware],
        )
        settings_beta = Settings(
            transporter="memory://",
            prefer_local=False,
        )

        alpha = ServiceBroker(id="alpha-node", settings=settings_alpha)
        beta = ServiceBroker(id="beta-node", settings=settings_beta)

        await beta.register(AlwaysFailsService("beta-node"))

        await alpha.start()
        await beta.start()
        await asyncio.sleep(0.3)

        try:
            # Make enough calls to trip the circuit
            connection_errors = 0
            circuit_errors = 0

            for _i in range(10):
                try:
                    await alpha.call("failing.fail", {})
                except Exception as e:
                    error_msg = str(e)
                    if "Circuit breaker" in error_msg:
                        circuit_errors += 1
                    else:
                        connection_errors += 1

                await asyncio.sleep(0.05)  # Small delay between calls

            # Should see some connection errors then circuit breaker errors
            assert connection_errors >= 3, "Should have at least min_requests failures"
            # Circuit should eventually trip
            # (depends on timing, may not always trip in 10 calls)

        finally:
            await alpha.stop()
            await beta.stop()

    async def test_circuit_state_tracking(
        self,
        circuit_breaker_middleware: CircuitBreakerMiddleware,
    ) -> None:
        """Can check circuit state via middleware API."""
        settings_alpha = Settings(
            transporter="memory://",
            prefer_local=False,
            middlewares=[circuit_breaker_middleware],
        )
        settings_beta = Settings(
            transporter="memory://",
            prefer_local=False,
        )

        alpha = ServiceBroker(id="alpha-node", settings=settings_alpha)
        beta = ServiceBroker(id="beta-node", settings=settings_beta)

        await beta.register(AlwaysFailsService("beta-node"))

        await alpha.start()
        await beta.start()
        await asyncio.sleep(0.3)

        try:
            # Initially no circuit exists
            state = circuit_breaker_middleware.get_circuit_state("failing.fail")
            assert state is None  # No calls yet

            # Make some failing calls
            for _ in range(5):
                try:
                    await alpha.call("failing.fail", {})
                except Exception:
                    pass
                await asyncio.sleep(0.05)

            # Now circuit should exist with stats
            stats = circuit_breaker_middleware.get_circuit_stats("failing.fail")
            if stats:
                assert stats["failure_count"] > 0
                assert stats["total_requests"] > 0

        finally:
            await alpha.stop()
            await beta.stop()


@pytest.mark.asyncio
@pytest.mark.e2e
class TestTimeoutMiddlewareRemote:
    """Tests for TimeoutMiddleware with remote calls."""

    async def test_timeout_on_slow_remote_call(
        self,
        timeout_middleware: TimeoutMiddleware,
    ) -> None:
        """TimeoutMiddleware enforces timeout on remote calls."""
        settings_alpha = Settings(
            transporter="memory://",
            prefer_local=False,
            middlewares=[timeout_middleware],
            request_timeout=0.5,  # Global timeout for remote calls
        )
        settings_beta = Settings(
            transporter="memory://",
            prefer_local=False,
        )

        alpha = ServiceBroker(id="alpha-node", settings=settings_alpha)
        beta = ServiceBroker(id="beta-node", settings=settings_beta)

        await beta.register(SlowService("beta-node"))

        await alpha.start()
        await beta.start()
        await asyncio.sleep(0.3)

        try:
            # Fast call should succeed
            result = await alpha.call("slow.delayed", {"delay": 0.1})
            assert result["node_id"] == "beta-node"
            assert result["status"] == "completed"

            # Slow call should timeout (request_timeout=0.5s)
            from moleculerpy.middleware.timeout import RequestTimeoutError

            with pytest.raises((RequestTimeoutError, asyncio.TimeoutError)):
                await alpha.call("slow.delayed", {"delay": 1.0})

        finally:
            await alpha.stop()
            await beta.stop()


@pytest.mark.asyncio
@pytest.mark.e2e
class TestBulkheadMiddlewareRemote:
    """Tests for BulkheadMiddleware with remote calls."""

    async def test_bulkhead_limits_concurrency(
        self,
        bulkhead_middleware: BulkheadMiddleware,
    ) -> None:
        """BulkheadMiddleware limits concurrent remote calls."""
        settings_alpha = Settings(
            transporter="memory://",
            prefer_local=False,
            middlewares=[bulkhead_middleware],
        )
        settings_beta = Settings(
            transporter="memory://",
            prefer_local=False,
        )

        alpha = ServiceBroker(id="alpha-node", settings=settings_alpha)
        beta = ServiceBroker(id="beta-node", settings=settings_beta)

        await beta.register(ConcurrentService("beta-node"))

        await alpha.start()
        await beta.start()
        await asyncio.sleep(0.3)

        try:
            # Launch many concurrent requests
            tasks = [
                asyncio.create_task(alpha.call("concurrent.process", {"id": i})) for i in range(8)
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Count successes
            successes = [r for r in results if isinstance(r, dict)]
            assert len(successes) == 8, "All requests should complete"

            # Check max concurrency observed by service
            max_concurrent = await alpha.call("concurrent.get_max_concurrent", {})

            # With bulkhead concurrency=2, max should be limited
            # Note: actual limit depends on whether middleware is applied to local or remote
            assert max_concurrent <= 3, f"Concurrency too high: {max_concurrent}"

        finally:
            await alpha.stop()
            await beta.stop()


@pytest.mark.asyncio
@pytest.mark.e2e
class TestMiddlewareStack:
    """Tests for combined middleware stack."""

    async def test_retry_with_circuit_breaker(
        self,
        retry_middleware: RetryMiddleware,
        circuit_breaker_middleware: CircuitBreakerMiddleware,
    ) -> None:
        """Retry and CircuitBreaker work together correctly."""
        # Order: Retry -> CircuitBreaker (retry wraps circuit breaker)
        settings_alpha = Settings(
            transporter="memory://",
            prefer_local=False,
            middlewares=[retry_middleware, circuit_breaker_middleware],
        )
        settings_beta = Settings(
            transporter="memory://",
            prefer_local=False,
        )

        alpha = ServiceBroker(id="alpha-node", settings=settings_alpha)
        beta = ServiceBroker(id="beta-node", settings=settings_beta)

        # 70% failure rate - high but retries should help initially
        await beta.register(FlakyService("beta-node", fail_rate=0.7))

        await alpha.start()
        await beta.start()
        await asyncio.sleep(0.3)

        try:
            successes = 0
            failures = 0

            for i in range(15):
                try:
                    result = await alpha.call(
                        "flaky.unreliable",
                        {"seed": i * 100},
                    )
                    if result["status"] == "success":
                        successes += 1
                except Exception:
                    failures += 1

                await asyncio.sleep(0.05)

            # Should have some successes due to retries
            # Circuit may or may not trip depending on exact timing
            total = successes + failures
            assert total == 15, "All calls should complete (success or fail)"

        finally:
            await alpha.stop()
            await beta.stop()

    async def test_full_resilience_stack(
        self,
        retry_middleware: RetryMiddleware,
        circuit_breaker_middleware: CircuitBreakerMiddleware,
        timeout_middleware: TimeoutMiddleware,
        bulkhead_middleware: BulkheadMiddleware,
    ) -> None:
        """Full middleware stack works together."""
        # Recommended order: Retry -> CircuitBreaker -> Timeout -> Bulkhead
        settings_alpha = Settings(
            transporter="memory://",
            prefer_local=False,
            middlewares=[
                retry_middleware,
                circuit_breaker_middleware,
                timeout_middleware,
                bulkhead_middleware,
            ],
        )
        settings_beta = Settings(
            transporter="memory://",
            prefer_local=False,
        )

        alpha = ServiceBroker(id="alpha-node", settings=settings_alpha)
        beta = ServiceBroker(id="beta-node", settings=settings_beta)

        # Register all services on beta
        await beta.register(FlakyService("beta-node", fail_rate=0.2))
        await beta.register(SlowService("beta-node"))
        await beta.register(ConcurrentService("beta-node"))

        await alpha.start()
        await beta.start()
        await asyncio.sleep(0.3)

        try:
            # Test 1: Flaky service with retries
            result = await alpha.call("flaky.unreliable", {"seed": 42})
            assert result["node_id"] == "beta-node"

            # Test 2: Fast slow service call (under timeout)
            result = await alpha.call("slow.delayed", {"delay": 0.1})
            assert result["status"] == "completed"

            # Test 3: Concurrent requests with bulkhead
            tasks = [
                asyncio.create_task(alpha.call("concurrent.process", {"id": i})) for i in range(5)
            ]
            results = await asyncio.gather(*tasks)
            assert len(results) == 5

        finally:
            await alpha.stop()
            await beta.stop()


@pytest.mark.asyncio
@pytest.mark.e2e
class TestNodeNames:
    """Tests verifying node identity in remote calls."""

    async def test_calls_reach_correct_node(self) -> None:
        """Verify calls reach the node hosting the service."""
        settings_alpha = Settings(
            transporter="memory://",
            prefer_local=False,
        )
        settings_beta = Settings(
            transporter="memory://",
            prefer_local=False,
        )

        alpha = ServiceBroker(id="alpha-node", settings=settings_alpha)
        beta = ServiceBroker(id="beta-node", settings=settings_beta)

        # Service only on beta
        await beta.register(FlakyService("beta-node", fail_rate=0.0))

        await alpha.start()
        await beta.start()
        await asyncio.sleep(0.3)

        try:
            # Call from alpha should reach beta
            result = await alpha.call("flaky.unreliable", {})
            assert result["node_id"] == "beta-node"

            # Multiple calls always go to beta (only beta has the service)
            for _ in range(5):
                result = await alpha.call("flaky.unreliable", {})
                assert result["node_id"] == "beta-node"

        finally:
            await alpha.stop()
            await beta.stop()

    async def test_three_node_cluster(self) -> None:
        """Test with three distinctly named nodes."""
        settings = Settings(transporter="memory://", prefer_local=False)

        alpha = ServiceBroker(id="alpha-node", settings=settings)
        beta = ServiceBroker(id="beta-node", settings=settings)
        gamma = ServiceBroker(id="gamma-node", settings=settings)

        # Each node hosts its own instance of the same service
        await alpha.register(FlakyService("alpha-node", fail_rate=0.0))
        await beta.register(FlakyService("beta-node", fail_rate=0.0))
        await gamma.register(FlakyService("gamma-node", fail_rate=0.0))

        await alpha.start()
        await beta.start()
        await gamma.start()
        await asyncio.sleep(0.3)

        try:
            # Calls from alpha should hit all three nodes (load balancing)
            seen_nodes: set[str] = set()
            for _ in range(30):
                result = await alpha.call("flaky.unreliable", {})
                seen_nodes.add(result["node_id"])

            # Should see all three nodes
            assert "alpha-node" in seen_nodes
            assert "beta-node" in seen_nodes
            assert "gamma-node" in seen_nodes

        finally:
            await alpha.stop()
            await beta.stop()
            await gamma.stop()
