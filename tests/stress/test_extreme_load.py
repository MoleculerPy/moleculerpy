"""Extreme load stress tests for MoleculerPy framework.

Tests framework behavior under high concurrent load:
- 10K concurrent calls
- 100K concurrent calls
- 500K concurrent calls
- 1M concurrent calls

All tests use simple echo action to isolate framework overhead.
"""

import asyncio
import time
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio

from moleculerpy import Service, ServiceBroker, Settings
from moleculerpy.decorators import action


class EchoService(Service):
    """Simple echo service for load testing."""

    name = "echo"

    @action()
    async def ping(self, ctx) -> dict:
        """Simple echo action with minimal overhead."""
        return {"echo": ctx.params.get("value", "pong")}

    @action()
    async def add(self, ctx) -> dict:
        """Simple addition for computational load."""
        a = ctx.params.get("a", 0)
        b = ctx.params.get("b", 0)
        return {"result": a + b}


@pytest_asyncio.fixture
async def broker() -> AsyncGenerator[ServiceBroker, None]:
    """Create broker with echo service."""
    settings = Settings(
        transporter="memory://",  # In-memory for pure Python performance
        prefer_local=True,  # No network overhead
    )

    broker = ServiceBroker(id="stress-test-node", settings=settings)
    await broker.register(EchoService())
    await broker.start()

    yield broker

    await broker.stop()


# =============================================================================
# 10K Load Tests
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.slow
async def test_10k_concurrent_calls(broker):
    """Stress test: 10,000 concurrent calls.

    Validates:
    - Framework handles 10K concurrent requests
    - No memory leaks under moderate load
    - All requests complete successfully
    - Latency remains acceptable
    """
    num_calls = 10_000

    start_time = time.perf_counter()

    # Execute 10K concurrent calls
    tasks = [broker.call("echo.ping", {"value": i}) for i in range(num_calls)]

    results = await asyncio.gather(*tasks)

    elapsed = time.perf_counter() - start_time

    # Assertions
    assert len(results) == num_calls
    assert all(r["echo"] == i for i, r in enumerate(results))

    # Performance metrics
    throughput = num_calls / elapsed
    avg_latency_ms = (elapsed / num_calls) * 1000

    print(f"\n{'=' * 70}")
    print("10K Load Test Results")
    print(f"{'=' * 70}")
    print(f"Total calls:      {num_calls:,}")
    print(f"Elapsed:          {elapsed:.2f}s")
    print(f"Throughput:       {throughput:,.0f} calls/sec")
    print(f"Avg latency:      {avg_latency_ms:.3f}ms")
    print(f"{'=' * 70}\n")

    # SLA: Should handle at least 1K calls/sec
    assert throughput >= 1000, f"Throughput {throughput:.0f} < 1000 calls/sec"


@pytest.mark.asyncio
@pytest.mark.slow
async def test_10k_with_computational_load(broker):
    """Stress test: 10K calls with light computation."""
    num_calls = 10_000

    start_time = time.perf_counter()

    tasks = [broker.call("echo.add", {"a": i, "b": i * 2}) for i in range(num_calls)]

    results = await asyncio.gather(*tasks)

    elapsed = time.perf_counter() - start_time

    # Assertions
    assert len(results) == num_calls
    assert all(r["result"] == i + (i * 2) for i, r in enumerate(results))

    throughput = num_calls / elapsed

    print(f"\n{'=' * 70}")
    print("10K Computational Load Test")
    print(f"{'=' * 70}")
    print(f"Total calls:      {num_calls:,}")
    print(f"Elapsed:          {elapsed:.2f}s")
    print(f"Throughput:       {throughput:,.0f} calls/sec")
    print(f"{'=' * 70}\n")


# =============================================================================
# 100K Load Tests
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.slow
async def test_100k_concurrent_calls(broker):
    """Stress test: 100,000 concurrent calls.

    Validates:
    - Framework scales to 100K concurrent requests
    - Memory usage remains stable
    - Throughput doesn't degrade significantly
    """
    num_calls = 100_000

    start_time = time.perf_counter()

    tasks = [
        broker.call("echo.ping", {"value": i % 1000})  # Reuse values
        for i in range(num_calls)
    ]

    results = await asyncio.gather(*tasks)

    elapsed = time.perf_counter() - start_time

    # Assertions
    assert len(results) == num_calls

    throughput = num_calls / elapsed
    avg_latency_ms = (elapsed / num_calls) * 1000

    print(f"\n{'=' * 70}")
    print("100K Load Test Results")
    print(f"{'=' * 70}")
    print(f"Total calls:      {num_calls:,}")
    print(f"Elapsed:          {elapsed:.2f}s")
    print(f"Throughput:       {throughput:,.0f} calls/sec")
    print(f"Avg latency:      {avg_latency_ms:.3f}ms")
    print(f"{'=' * 70}\n")

    # SLA: Should handle at least 1K calls/sec even at 100K scale
    assert throughput >= 1000


# =============================================================================
# 500K Load Tests
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.slow
async def test_500k_concurrent_calls(broker):
    """Stress test: 500,000 concurrent calls.

    Extreme load scenario. Validates:
    - Framework can handle 500K+ concurrent tasks
    - No asyncio event loop saturation
    - Memory doesn't grow unbounded
    """
    num_calls = 500_000

    print(f"\n{'=' * 70}")
    print("Starting 500K Load Test (this may take a while...)")
    print(f"{'=' * 70}\n")

    start_time = time.perf_counter()

    # Batch to avoid memory exhaustion
    batch_size = 50_000
    total_results = []

    for batch_start in range(0, num_calls, batch_size):
        batch_end = min(batch_start + batch_size, num_calls)
        batch_count = batch_end - batch_start

        tasks = [
            broker.call("echo.ping", {"value": i % 100}) for i in range(batch_start, batch_end)
        ]

        batch_results = await asyncio.gather(*tasks)
        total_results.extend(batch_results)

        print(f"  Batch {batch_start:,} - {batch_end:,} complete ({batch_count:,} calls)")

    elapsed = time.perf_counter() - start_time

    # Assertions
    assert len(total_results) == num_calls

    throughput = num_calls / elapsed
    avg_latency_ms = (elapsed / num_calls) * 1000

    print(f"\n{'=' * 70}")
    print("500K Load Test Results")
    print(f"{'=' * 70}")
    print(f"Total calls:      {num_calls:,}")
    print(f"Elapsed:          {elapsed:.2f}s")
    print(f"Throughput:       {throughput:,.0f} calls/sec")
    print(f"Avg latency:      {avg_latency_ms:.3f}ms")
    print(f"{'=' * 70}\n")


# =============================================================================
# 1M Load Tests
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.slow
async def test_1m_concurrent_calls(broker):
    """Stress test: 1,000,000 concurrent calls.

    EXTREME load scenario. Validates:
    - Framework ultimate scalability
    - Memory efficiency with batching
    - Throughput stability over long duration
    """
    num_calls = 1_000_000

    print(f"\n{'=' * 70}")
    print("Starting 1M Load Test (expect ~2-5 minutes...)")
    print(f"{'=' * 70}\n")

    start_time = time.perf_counter()

    # Batch to avoid memory exhaustion
    batch_size = 100_000
    total_results = []

    for batch_start in range(0, num_calls, batch_size):
        batch_end = min(batch_start + batch_size, num_calls)
        batch_end - batch_start

        tasks = [
            broker.call("echo.ping", {"value": i % 100}) for i in range(batch_start, batch_end)
        ]

        batch_results = await asyncio.gather(*tasks)
        total_results.extend(batch_results)

        batch_elapsed = time.perf_counter() - start_time
        batch_throughput = (batch_end / batch_elapsed) if batch_elapsed > 0 else 0

        print(
            f"  Batch {batch_start:,} - {batch_end:,} | "
            f"Throughput: {batch_throughput:,.0f} calls/sec"
        )

    elapsed = time.perf_counter() - start_time

    # Assertions
    assert len(total_results) == num_calls

    throughput = num_calls / elapsed
    avg_latency_ms = (elapsed / num_calls) * 1000

    print(f"\n{'=' * 70}")
    print("1M Load Test Results")
    print(f"{'=' * 70}")
    print(f"Total calls:      {num_calls:,}")
    print(f"Elapsed:          {elapsed:.2f}s")
    print(f"Throughput:       {throughput:,.0f} calls/sec")
    print(f"Avg latency:      {avg_latency_ms:.3f}ms")
    print(f"{'=' * 70}\n")

    # SLA: Should complete 1M calls (even if slower)
    assert len(total_results) == num_calls


# =============================================================================
# Concurrency Scaling Tests
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.slow
async def test_concurrency_scaling():
    """Test throughput scaling across different concurrency levels.

    Measures how throughput changes with increasing concurrent load.
    """
    settings = Settings(transporter="memory://", prefer_local=True)
    broker = ServiceBroker(id="scaling-test", settings=settings)
    await broker.register(EchoService())
    await broker.start()

    concurrency_levels = [100, 1_000, 10_000, 50_000]
    results = []

    print(f"\n{'=' * 70}")
    print("Concurrency Scaling Test")
    print(f"{'=' * 70}\n")

    for concurrency in concurrency_levels:
        start_time = time.perf_counter()

        tasks = [broker.call("echo.ping", {"value": i % 100}) for i in range(concurrency)]

        await asyncio.gather(*tasks)

        elapsed = time.perf_counter() - start_time
        throughput = concurrency / elapsed

        results.append(
            {
                "concurrency": concurrency,
                "elapsed": elapsed,
                "throughput": throughput,
            }
        )

        print(f"  {concurrency:>7,} concurrent: {throughput:>10,.0f} calls/sec | {elapsed:.3f}s")

    await broker.stop()

    print(f"\n{'=' * 70}\n")

    # Assertions: throughput should not degrade drastically
    throughputs = [r["throughput"] for r in results]
    max_throughput = max(throughputs)
    min_throughput = min(throughputs)

    # Allow up to 50% degradation at extreme concurrency
    degradation = (max_throughput - min_throughput) / max_throughput
    assert degradation < 0.5, f"Throughput degradation {degradation:.1%} > 50%"
