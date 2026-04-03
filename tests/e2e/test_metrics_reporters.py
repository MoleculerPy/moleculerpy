"""E2E tests for pluggable metrics reporter system (PRD-012).

Tests validate that ConsoleReporter and PrometheusReporter work correctly
with real ServiceBroker instances using MemoryTransporter.

Test Setup:
    - Single broker with MemoryTransporter
    - MetricsMiddleware using extended MetricRegistry from metric_reporters
    - Real action calls to generate metrics
"""

from __future__ import annotations

import asyncio

import pytest

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action
from moleculerpy.metric_reporters import ConsoleReporter, MetricRegistry, PrometheusReporter
from moleculerpy.middleware.metrics import MetricsMiddleware
from moleculerpy.service import Service
from moleculerpy.settings import Settings

# =============================================================================
# Test Services
# =============================================================================


class CalculatorService(Service):
    """Simple calculator service for metric generation."""

    name = "calculator"

    @action()
    async def add(self, ctx) -> int:  # type: ignore[no-untyped-def]
        """Add two numbers."""
        return ctx.params["a"] + ctx.params["b"]

    @action()
    async def divide(self, ctx) -> float:  # type: ignore[no-untyped-def]
        """Divide a by b. Raises ZeroDivisionError when b=0."""
        b = ctx.params["b"]
        if b == 0:
            raise ZeroDivisionError("division by zero")
        return ctx.params["a"] / b


# =============================================================================
# Test 1: Broker with MetricsMiddleware + PrometheusReporter
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_prometheus_reporter_collects_action_metrics() -> None:
    """PrometheusReporter captures action metrics after real broker calls."""
    registry = MetricRegistry()
    prometheus = PrometheusReporter()
    registry.add_reporter(prometheus)

    metrics_mw = MetricsMiddleware(registry=registry)
    settings = Settings(transporter="memory://", middlewares=[metrics_mw])
    broker = ServiceBroker(id="prom-node", settings=settings)

    await broker.register(CalculatorService())
    await broker.start()
    await asyncio.sleep(0.1)

    try:
        result = await broker.call("calculator.add", {"a": 3, "b": 7})
        assert result == 10

        output = prometheus.get_text()

        # Prometheus output must contain standard moleculer metric names
        assert "moleculer_request_total" in output
        assert "moleculer_request_duration_seconds" in output

        # The action label must appear in the output
        assert "calculator.add" in output

        # At least one successful request recorded
        assert 'status="success"' in output

    finally:
        await registry.stop_reporters()
        await broker.stop()


# =============================================================================
# Test 2: ConsoleReporter tracks changed metric names via _notify_reporters
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_console_reporter_tracks_changed_metrics() -> None:
    """ConsoleReporter._changed_names is populated via _notify_reporters API.

    The extended MetricRegistry exposes _notify_reporters() for callers that
    want to push metric changes to reporters.  ConsoleReporter accumulates
    changed names and clears them on each _print_metrics() cycle.
    """
    registry = MetricRegistry()
    console = ConsoleReporter(only_changes=True, interval=60.0)  # long interval — no auto-print
    registry.add_reporter(console)

    metrics_mw = MetricsMiddleware(registry=registry)
    settings = Settings(transporter="memory://", middlewares=[metrics_mw])
    broker = ServiceBroker(id="console-node", settings=settings)

    await broker.register(CalculatorService())
    await broker.start()
    await asyncio.sleep(0.1)

    try:
        assert len(console._changed_names) == 0

        # Perform a real broker call so metrics are recorded in the registry
        await broker.call("calculator.add", {"a": 1, "b": 2})

        # Manually notify reporters for each changed metric (simulates an
        # integration point that would call _notify_reporters after each update)
        metrics_list = registry.list()
        assert len(metrics_list) > 0

        for metric in metrics_list:
            snap = metric.snapshot()
            values = snap.get("values", {})
            if values:  # only notify when metric has data
                registry._notify_reporters(metric, values)

        # ConsoleReporter must have tracked the notified metrics
        assert len(console._changed_names) > 0
        changed = console._changed_names
        assert any("moleculer_request" in name for name in changed)

    finally:
        await registry.stop_reporters()
        await broker.stop()


# =============================================================================
# Test 3: Multiple reporters simultaneously
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_multiple_reporters_work_simultaneously() -> None:
    """Console and Prometheus reporters coexist without conflict."""
    registry = MetricRegistry()
    console = ConsoleReporter(only_changes=True, interval=60.0)
    prometheus = PrometheusReporter()
    registry.add_reporter(console)
    registry.add_reporter(prometheus)

    metrics_mw = MetricsMiddleware(registry=registry)
    settings = Settings(transporter="memory://", middlewares=[metrics_mw])
    broker = ServiceBroker(id="multi-reporter-node", settings=settings)

    await broker.register(CalculatorService())
    await broker.start()
    await asyncio.sleep(0.1)

    try:
        result = await broker.call("calculator.add", {"a": 10, "b": 20})
        assert result == 30

        # Prometheus works via pull — get_text() reads registry directly
        prom_output = prometheus.get_text()
        assert "moleculer_request_total" in prom_output
        assert "calculator.add" in prom_output

        # Manually notify all reporters (push path) and verify ConsoleReporter
        metrics_list = registry.list()
        assert len(metrics_list) > 0

        for metric in metrics_list:
            snap = metric.snapshot()
            values = snap.get("values", {})
            if values:
                registry._notify_reporters(metric, values)

        assert len(console._changed_names) > 0

        # list() must return all registered metrics
        metric_names = {m.name for m in metrics_list}
        assert "moleculer_request_total" in metric_names
        assert "moleculer_request_duration_seconds" in metric_names

    finally:
        await registry.stop_reporters()
        await broker.stop()


# =============================================================================
# Test 4: MetricsMiddleware backward compatibility (no reporters)
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_metrics_middleware_backward_compat_no_reporters() -> None:
    """MetricsMiddleware with default (base) registry works without reporters."""
    # Use the original MetricsMiddleware with its default MetricRegistry —
    # no reporter involved.  This validates backward compatibility.
    metrics_mw = MetricsMiddleware()
    settings = Settings(transporter="memory://", middlewares=[metrics_mw])
    broker = ServiceBroker(id="compat-node", settings=settings)

    await broker.register(CalculatorService())
    await broker.start()
    await asyncio.sleep(0.1)

    try:
        result = await broker.call("calculator.add", {"a": 5, "b": 5})
        assert result == 10

        # Prometheus output still works via base registry
        output = metrics_mw.registry.to_prometheus()
        assert "moleculer_request_total" in output
        assert "calculator.add" in output

    finally:
        await broker.stop()
