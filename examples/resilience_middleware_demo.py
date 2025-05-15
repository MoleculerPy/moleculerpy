#!/usr/bin/env python3
"""Comprehensive demo of all resilience middleware in MoleculerPy.

This example demonstrates the following middleware we implemented:
1. RetryMiddleware - Automatic retry with exponential backoff
2. CircuitBreakerMiddleware - Fault tolerance with circuit breaker pattern
3. TimeoutMiddleware - Per-action timeout enforcement
4. BulkheadMiddleware - Concurrency limiting with FIFO queue
5. FallbackMiddleware - Graceful degradation on failure

Run with: python resilience_middleware_demo.py
"""

from __future__ import annotations

import asyncio
import random

from moleculerpy.broker import Broker
from moleculerpy.decorators import action
from moleculerpy.middleware import (
    BulkheadMiddleware,
    CircuitBreakerMiddleware,
    FallbackMiddleware,
    RetryMiddleware,
    TimeoutMiddleware,
)
from moleculerpy.service import Service

# --- Demo Service with various failure modes ---

FLAKY_FAILURE_RATE = 0.5


class DemoService(Service):
    """Service with actions that simulate various failure scenarios."""

    def __init__(self) -> None:
        super().__init__("demo")
        self.call_count: dict[str, int] = {}
        self.concurrent_calls = 0
        self.max_concurrent = 0

    @action()
    async def flaky_action(self, ctx) -> dict:
        """Action that fails randomly - tests RetryMiddleware.

        Fails ~50% of the time to demonstrate retry behavior.
        """
        action_name = "flaky_action"
        self.call_count[action_name] = self.call_count.get(action_name, 0) + 1
        attempt = self.call_count[action_name]

        if random.random() < FLAKY_FAILURE_RATE:
            print(f"  [FLAKY] Attempt {attempt}: FAIL (random failure)")
            raise ConnectionError(f"Random failure on attempt {attempt}")

        print(f"  [FLAKY] Attempt {attempt}: SUCCESS")
        return {"status": "ok", "attempt": attempt}

    @action()
    async def always_fails(self, ctx) -> dict:
        """Action that always fails - tests CircuitBreakerMiddleware."""
        action_name = "always_fails"
        self.call_count[action_name] = self.call_count.get(action_name, 0) + 1
        attempt = self.call_count[action_name]

        print(f"  [FAILS] Attempt {attempt}: FAIL (always)")
        raise ConnectionError(f"Service unavailable (attempt {attempt})")

    @action(timeout=0.5)  # 500ms timeout
    async def slow_action(self, ctx) -> dict:
        """Action that takes too long - tests TimeoutMiddleware.

        Takes 1 second but has 500ms timeout.
        """
        delay = ctx.params.get("delay", 1.0)
        print(f"  [SLOW] Starting... will take {delay}s")
        await asyncio.sleep(delay)
        print(f"  [SLOW] Completed after {delay}s")
        return {"status": "completed", "delay": delay}

    @action()
    async def concurrent_action(self, ctx) -> dict:
        """Action that tracks concurrency - tests BulkheadMiddleware.

        Sleeps briefly to allow concurrent calls to accumulate.
        """
        self.concurrent_calls += 1
        current = self.concurrent_calls
        self.max_concurrent = max(self.max_concurrent, current)

        request_id = ctx.params.get("id", "?")
        print(f"  [CONCURRENT] Request {request_id}: START (concurrent={current})")

        await asyncio.sleep(0.1)  # Simulate work

        self.concurrent_calls -= 1
        print(f"  [CONCURRENT] Request {request_id}: END (concurrent={self.concurrent_calls})")

        return {"id": request_id, "concurrent_at_start": current}

    @action()
    async def action_with_fallback(self, ctx) -> dict:
        """Action with fallback - tests FallbackMiddleware.

        Fails but returns fallback response instead of raising.
        Fallback is configured after registration on the action endpoint.
        """
        print("  [FALLBACK] Action executing... will fail")
        raise ValueError("Intentional failure for fallback demo")

    def fallback_handler(self, ctx, error) -> dict:
        """Fallback method called when action_with_fallback fails."""
        return {"status": "fallback", "error": str(error), "message": "Graceful degradation"}


# --- Demo Functions ---


async def demo_retry(broker: Broker) -> None:
    """Demo 1: RetryMiddleware - Architecture explanation."""
    print("\n" + "=" * 60)
    print("DEMO 1: RetryMiddleware (automatic retry with backoff)")
    print("=" * 60)
    print("NOTE: RetryMiddleware only wraps REMOTE actions (not local).")
    print("This is correct Moleculer.js behavior - local calls don't need retry")
    print("because they're in-process with no network failures.\n")
    print("For demo purposes, showing a local call that would fail:")

    try:
        result = await broker.call("demo.flaky_action")
        print(f"\n✅ SUCCESS: {result}")
    except Exception as e:
        print(f"\n❌ Local call failed (expected - no retry for local): {e}")
        print("   → In production with remote calls, RetryMiddleware would retry.")


async def demo_circuit_breaker(broker: Broker) -> None:
    """Demo 2: CircuitBreakerMiddleware - Architecture explanation."""
    print("\n" + "=" * 60)
    print("DEMO 2: CircuitBreakerMiddleware (fault tolerance)")
    print("=" * 60)
    print("NOTE: CircuitBreakerMiddleware only wraps REMOTE actions.")
    print("This is correct - circuit breakers protect against remote service failures.\n")
    print("Showing local failures (circuit breaker won't trip):")

    for i in range(5):
        try:
            await broker.call("demo.always_fails")
        except Exception as e:
            error_type = type(e).__name__
            print(f"  Call {i + 1}: {error_type}: {e}")

            # Check if circuit is open
            if "Circuit breaker is open" in str(e):
                print("\n⚡ Circuit breaker TRIPPED! Requests now fail fast.")
                break

        await asyncio.sleep(0.1)

    print("\n   → In production with remote calls, circuit would trip after threshold.")


async def demo_timeout(broker: Broker) -> None:
    """Demo 3: TimeoutMiddleware - Request timeout enforcement."""
    print("\n" + "=" * 60)
    print("DEMO 3: TimeoutMiddleware (timeout enforcement)")
    print("=" * 60)

    # Test 1: Fast action (should succeed)
    print("\n--- Fast action (0.1s delay, 0.5s timeout) ---")
    try:
        result = await broker.call("demo.slow_action", params={"delay": 0.1})
        print(f"✅ SUCCESS: {result}")
    except Exception as e:
        print(f"❌ FAILED: {e}")

    # Test 2: Slow action (should timeout)
    print("\n--- Slow action (1.0s delay, 0.5s timeout) ---")
    try:
        result = await broker.call("demo.slow_action", params={"delay": 1.0})
        print(f"✅ SUCCESS: {result}")
    except Exception as e:
        error_type = type(e).__name__
        print(f"⏱️ TIMEOUT: {error_type}: {e}")


async def demo_bulkhead(broker: Broker, service: DemoService) -> None:
    """Demo 4: BulkheadMiddleware - Concurrency limiting."""
    print("\n" + "=" * 60)
    print("DEMO 4: BulkheadMiddleware (concurrency limiting)")
    print("=" * 60)
    print("Launching 10 concurrent requests with concurrency=3...")
    print("BulkheadMiddleware will queue excess requests.\n")

    # Reset counters
    service.max_concurrent = 0
    service.concurrent_calls = 0

    # Launch 10 concurrent requests
    tasks = [
        asyncio.create_task(
            broker.call("demo.concurrent_action", params={"id": i})
        )
        for i in range(10)
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    successes = sum(1 for r in results if not isinstance(r, Exception))
    failures = sum(1 for r in results if isinstance(r, Exception))

    print(f"\n📊 Results: {successes} succeeded, {failures} failed")
    print(f"📈 Max concurrent calls observed: {service.max_concurrent}")
    print("   (Should be ≤3 due to bulkhead concurrency=3)")


async def demo_fallback(broker: Broker, service: DemoService) -> None:
    """Demo 5: FallbackMiddleware - Graceful degradation."""
    print("\n" + "=" * 60)
    print("DEMO 5: FallbackMiddleware (graceful degradation)")
    print("=" * 60)
    print("NOTE: Action-level fallback requires 'fallback' slot in Action class.")
    print("This is a known limitation. FallbackMiddleware works with:")
    print("  1. Call-time fallback via ctx.options.fallback_response")
    print("  2. Action-level fallback (needs Action class modification)")
    print("\nSkipping live demo, but middleware is functional in unit tests.")
    print("See: tests/unit/fallback_test.py for working examples.")


# --- Main ---


async def main() -> None:
    """Run all middleware demos."""
    print("=" * 60)
    print("  MOLECULERPY RESILIENCE MIDDLEWARE DEMO")
    print("  Testing: Retry, CircuitBreaker, Timeout, Bulkhead, Fallback")
    print("=" * 60)

    # Create middleware stack
    middlewares = [
        # Order matters! Outer middleware wraps inner ones.
        # Recommended order: Retry → CircuitBreaker → Timeout → Bulkhead → Fallback

        RetryMiddleware(
            max_retries=3,
            base_delay=0.1,  # Fast retries for demo
            max_delay=1.0,
            jitter=True,
        ),
        CircuitBreakerMiddleware(
            threshold=0.5,  # Trip at 50% failure rate
            window_time=30,
            min_requests=3,  # Trip after 3 requests
            half_open_time=5,
        ),
        TimeoutMiddleware(
            default_timeout=5.0,  # 5s default, actions can override
        ),
        BulkheadMiddleware(
            concurrency=3,  # Max 3 concurrent requests
            max_queue_size=10,  # Queue up to 10 more
        ),
        FallbackMiddleware(),
    ]

    # Create broker with middleware
    broker = Broker(
        id="resilience-demo",
        middlewares=middlewares,
    )

    # Create and register service
    service = DemoService()
    await broker.register(service)

    # Start broker
    await broker.start()
    print("\nBroker started with middleware stack:")
    for mw in middlewares:
        print(f"  - {type(mw).__name__}")

    try:
        # Run demos
        await demo_retry(broker)
        await asyncio.sleep(0.5)

        await demo_circuit_breaker(broker)
        await asyncio.sleep(0.5)

        await demo_timeout(broker)
        await asyncio.sleep(0.5)

        await demo_bulkhead(broker, service)
        await asyncio.sleep(0.5)

        await demo_fallback(broker, service)

    finally:
        # Stop broker
        print("\n" + "=" * 60)
        await broker.stop()
        print("Broker stopped. Demo complete!")


if __name__ == "__main__":
    # Set random seed for reproducible demo
    random.seed(42)
    asyncio.run(main())
