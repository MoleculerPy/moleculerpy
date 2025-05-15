#!/usr/bin/env python3
"""Alpha Node - Client with interactive menu and load testing.

Run this AFTER server_beta.py in terminal 2:
    python examples/cluster/client_alpha.py

Features:
    - Interactive menu for testing different middleware
    - Load testing mode with concurrent requests
    - Real-time statistics
    - Colored console output
"""

from __future__ import annotations

import asyncio
import statistics
import time
from dataclasses import dataclass, field

# Import colored logger for beautiful console output
from colored_logger import LoggerFactory, LogLevel

from moleculerpy.broker import ServiceBroker
from moleculerpy.middleware import (
    BulkheadMiddleware,
    CircuitBreakerMiddleware,
    RetryMiddleware,
    TimeoutMiddleware,
)
from moleculerpy.settings import Settings

# =============================================================================
# Statistics Collector
# =============================================================================


@dataclass
class Stats:
    """Statistics collector for load testing."""

    total: int = 0
    successes: int = 0
    failures: int = 0
    retries: int = 0
    timeouts: int = 0
    circuit_breaks: int = 0
    response_times: list[float] = field(default_factory=list)
    _P95_QUANTILE_BUCKETS = 20
    _P95_QUANTILE_INDEX = 18
    _P99_QUANTILE_BUCKETS = 100
    _P99_QUANTILE_INDEX = 98

    def record_success(self, duration: float) -> None:
        self.total += 1
        self.successes += 1
        self.response_times.append(duration)

    def record_failure(self, error: Exception, duration: float) -> None:
        self.total += 1
        self.failures += 1
        self.response_times.append(duration)

        error_msg = str(error).lower()
        if "timeout" in error_msg:
            self.timeouts += 1
        elif "circuit" in error_msg:
            self.circuit_breaks += 1

    def report(self) -> str:
        """Generate statistics report."""
        if not self.response_times:
            return "No data collected"

        avg = statistics.mean(self.response_times) * 1000
        p50 = statistics.median(self.response_times) * 1000
        p95 = (
            statistics.quantiles(self.response_times, n=self._P95_QUANTILE_BUCKETS)[
                self._P95_QUANTILE_INDEX
            ]
            * 1000
            if len(self.response_times) >= self._P95_QUANTILE_BUCKETS
            else max(self.response_times) * 1000
        )
        p99 = (
            statistics.quantiles(self.response_times, n=self._P99_QUANTILE_BUCKETS)[
                self._P99_QUANTILE_INDEX
            ]
            * 1000
            if len(self.response_times) >= self._P99_QUANTILE_BUCKETS
            else max(self.response_times) * 1000
        )

        success_rate = (self.successes / self.total * 100) if self.total > 0 else 0

        return f"""
╔════════════════════════════════════════════════════════╗
║                   LOAD TEST RESULTS                     ║
╠════════════════════════════════════════════════════════╣
║  Total Requests:     {self.total:>8}                        ║
║  Successes:          {self.successes:>8}  ({success_rate:.1f}%)               ║
║  Failures:           {self.failures:>8}                        ║
║    - Timeouts:       {self.timeouts:>8}                        ║
║    - Circuit Breaks: {self.circuit_breaks:>8}                        ║
╠════════════════════════════════════════════════════════╣
║  Response Times (ms):                                   ║
║    - Average:        {avg:>8.2f}                        ║
║    - P50 (Median):   {p50:>8.2f}                        ║
║    - P95:            {p95:>8.2f}                        ║
║    - P99:            {p99:>8.2f}                        ║
║    - Min:            {min(self.response_times)*1000:>8.2f}                        ║
║    - Max:            {max(self.response_times)*1000:>8.2f}                        ║
╚════════════════════════════════════════════════════════╝
"""


# =============================================================================
# Interactive Client
# =============================================================================


class InteractiveClient:
    """Interactive client for testing MoleculerPy cluster."""

    def __init__(self, broker: ServiceBroker):
        self.broker = broker
        self.stats = Stats()

    async def test_echo(self) -> None:
        """Test simple echo service."""
        print("\n--- Testing echo.ping ---")
        try:
            result = await self.broker.call("echo.ping", {"message": "Hello from alpha!"})
            print(f"Response: {result}")
        except Exception as e:
            print(f"Error: {e}")

    async def test_retry(self, count: int = 5) -> None:
        """Test retry middleware with flaky service."""
        print(f"\n--- Testing RetryMiddleware ({count} calls to flaky.unreliable) ---")
        successes = 0
        failures = 0

        for i in range(count):
            try:
                start = time.perf_counter()
                result = await self.broker.call("flaky.unreliable", {})
                duration = time.perf_counter() - start
                successes += 1
                print(f"  [{i+1}] SUCCESS in {duration*1000:.1f}ms: {result['node_id']}")
            except Exception as e:
                failures += 1
                print(f"  [{i+1}] FAILED: {type(e).__name__}: {e}")

        print(f"\nResults: {successes} successes, {failures} failures")

    async def test_circuit_breaker(self, count: int = 10) -> None:
        """Test circuit breaker with always-failing service."""
        print(f"\n--- Testing CircuitBreakerMiddleware ({count} calls to flaky.always_fail) ---")

        for i in range(count):
            try:
                await self.broker.call("flaky.always_fail", {})
                print(f"  [{i+1}] SUCCESS (unexpected!)")
            except Exception as e:
                error_type = "CIRCUIT OPEN" if "circuit" in str(e).lower() else "SERVICE ERROR"
                print(f"  [{i+1}] {error_type}: {type(e).__name__}")
            await asyncio.sleep(0.1)

    async def test_timeout(self) -> None:
        """Test timeout middleware."""
        print("\n--- Testing TimeoutMiddleware ---")

        # Fast call
        print("\n1. Fast call (delay=0.1s, timeout=2s):")
        try:
            start = time.perf_counter()
            result = await self.broker.call("slow.process", {"delay": 0.1})
            duration = time.perf_counter() - start
            print(f"   SUCCESS in {duration*1000:.1f}ms: {result}")
        except Exception as e:
            print(f"   FAILED: {e}")

        # Slow call (should timeout if timeout < delay)
        print("\n2. Slow call (delay=3s, timeout=2s):")
        try:
            start = time.perf_counter()
            result = await self.broker.call("slow.process", {"delay": 3.0})
            duration = time.perf_counter() - start
            print(f"   SUCCESS in {duration*1000:.1f}ms: {result}")
        except Exception as e:
            duration = time.perf_counter() - start
            print(f"   TIMEOUT after {duration*1000:.1f}ms: {type(e).__name__}")

    async def test_counter(self, count: int = 10) -> None:
        """Test counter service (shows load balancing if multiple servers)."""
        print(f"\n--- Testing counter.increment ({count} calls) ---")

        node_counts: dict[str, int] = {}

        for i in range(count):
            try:
                result = await self.broker.call("counter.increment", {})
                node_id = result["node_id"]
                node_counts[node_id] = node_counts.get(node_id, 0) + 1
                print(f"  [{i+1}] {node_id}: count={result['count']}")
            except Exception as e:
                print(f"  [{i+1}] FAILED: {e}")

        print(f"\nLoad distribution: {node_counts}")

    async def load_test(
        self,
        action: str,
        params: dict,
        total_requests: int = 100,
        concurrency: int = 10,
    ) -> None:
        """Run load test with concurrent requests."""
        print(f"\n{'='*60}")
        print(f"  LOAD TEST: {action}")
        print(f"  Requests: {total_requests}, Concurrency: {concurrency}")
        print(f"{'='*60}")

        self.stats = Stats()
        semaphore = asyncio.Semaphore(concurrency)
        start_time = time.perf_counter()

        async def make_request(request_id: int) -> None:
            async with semaphore:
                req_start = time.perf_counter()
                try:
                    await self.broker.call(action, params)
                    duration = time.perf_counter() - req_start
                    self.stats.record_success(duration)
                    if request_id % 10 == 0:
                        print(f"  Request {request_id}: OK ({duration*1000:.1f}ms)")
                except Exception as e:
                    duration = time.perf_counter() - req_start
                    self.stats.record_failure(e, duration)
                    if request_id % 10 == 0:
                        print(f"  Request {request_id}: FAIL ({type(e).__name__})")

        # Run all requests concurrently
        tasks = [make_request(i) for i in range(total_requests)]
        await asyncio.gather(*tasks)

        total_time = time.perf_counter() - start_time
        rps = total_requests / total_time

        print(f"\nCompleted in {total_time:.2f}s ({rps:.1f} req/s)")
        print(self.stats.report())

    async def interactive_menu(self) -> None:
        """Show interactive menu."""
        while True:
            print("\n" + "=" * 60)
            print("  MOLECULERPY CLIENT - Interactive Menu")
            print("=" * 60)
            print()
            print("  1. Test echo.ping (simple call)")
            print("  2. Test retry (flaky.unreliable)")
            print("  3. Test circuit breaker (flaky.always_fail)")
            print("  4. Test timeout (slow.process)")
            print("  5. Test counter (load balancing)")
            print()
            print("  Load Tests:")
            print("  6. Load test echo (100 req, 10 concurrent)")
            print("  7. Load test flaky (100 req, 10 concurrent)")
            print("  8. Load test counter (500 req, 50 concurrent)")
            print("  9. Custom load test")
            print()
            print("  0. Exit")
            print()

            try:
                choice = input("Select option: ").strip()
            except (KeyboardInterrupt, EOFError):
                print("\nExiting...")
                break

            if choice == "0":
                break
            elif choice == "1":
                await self.test_echo()
            elif choice == "2":
                await self.test_retry()
            elif choice == "3":
                await self.test_circuit_breaker()
            elif choice == "4":
                await self.test_timeout()
            elif choice == "5":
                await self.test_counter()
            elif choice == "6":
                await self.load_test("echo.ping", {"message": "load test"}, 100, 10)
            elif choice == "7":
                await self.load_test("flaky.unreliable", {}, 100, 10)
            elif choice == "8":
                await self.load_test("counter.increment", {}, 500, 50)
            elif choice == "9":
                await self.custom_load_test()
            else:
                print("Invalid option")

            input("\nPress Enter to continue...")

    async def custom_load_test(self) -> None:
        """Configure and run custom load test."""
        print("\n--- Custom Load Test Configuration ---")

        print("\nAvailable actions:")
        print("  1. echo.ping")
        print("  2. flaky.unreliable")
        print("  3. counter.increment")
        print("  4. slow.process")

        try:
            action_choice = input("Select action (1-4): ").strip()
            actions = {
                "1": ("echo.ping", {"message": "custom test"}),
                "2": ("flaky.unreliable", {}),
                "3": ("counter.increment", {}),
                "4": ("slow.process", {"delay": 0.1}),
            }

            if action_choice not in actions:
                print("Invalid action")
                return

            action, params = actions[action_choice]

            total = int(input("Total requests [100]: ").strip() or "100")
            concurrency = int(input("Concurrency [10]: ").strip() or "10")

            await self.load_test(action, params, total, concurrency)

        except ValueError as e:
            print(f"Invalid input: {e}")


# =============================================================================
# Main
# =============================================================================


async def main():
    """Run alpha node client."""
    NODE_ID = "alpha-node"

    # Create colored logger factory
    logger_factory = LoggerFactory(
        node_id=NODE_ID,
        level=LogLevel.INFO,  # Show INFO for middleware retries, etc.
        colors=True,
        timestamp_format="short",  # Shorter timestamps for client
    )

    # Get logger for startup messages
    startup_log = logger_factory.get_logger("STARTUP")

    startup_log.info("=" * 50)
    startup_log.info(f"MOLECULERPY CLIENT NODE: {NODE_ID}")
    startup_log.info("=" * 50)
    startup_log.info("Connecting to NATS at localhost:4222...")

    # Create middleware stack
    middlewares = [
        RetryMiddleware(
            max_retries=3,
            base_delay=0.1,
            max_delay=2.0,
            jitter=True,
        ),
        CircuitBreakerMiddleware(
            threshold=0.5,
            window_time=30,
            min_requests=5,
            half_open_time=5,
        ),
        TimeoutMiddleware(
            default_timeout=2.0,
        ),
        BulkheadMiddleware(
            concurrency=20,
            max_queue_size=100,
        ),
    ]

    # Create settings with colored logger
    settings = Settings(
        transporter="nats://localhost:4222",
        log_level="INFO",
        prefer_local=False,
        middlewares=middlewares,
        request_timeout=5.0,
        logger_factory=logger_factory,
    )

    # Create broker (no services - this is a client)
    broker = ServiceBroker(id=NODE_ID, settings=settings)

    try:
        # Start broker
        await broker.start()
        print(f"[{NODE_ID}] Connected!")
        print()

        # Wait for service discovery
        await asyncio.sleep(0.5)

        # Run interactive menu
        client = InteractiveClient(broker)
        await client.interactive_menu()

    finally:
        await broker.stop()
        print(f"\n[{NODE_ID}] Disconnected.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted")
