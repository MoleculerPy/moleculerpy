#!/usr/bin/env python3
"""Observability demo stand — Logging + Metrics + Tracing.

Verifies the three observability pillars of MoleculerPy against real,
captured in-process outputs. No external services required (memory
transport only).

Pillars & tests (9 total):

  LOGGING
    1. test_structured_log_capture  — custom logger receives records
                                       with node + service context
    2. test_log_level_filter        — log_level=ERROR filters out INFO
    3. test_service_scoped_logger   — self.logger inside a service
                                       carries the service name

  METRICS
    4. test_console_reporter        — MetricsMiddleware counter is
                                       incremented on every call
    5. test_prometheus_format       — to_prometheus() emits valid
                                       exposition format
    6. test_custom_metric           — user gauge set(42) visible in
                                       registry

  TRACING
    7. test_console_trace_export    — cross-service call produces
                                       nested spans via ConsoleExporter
    8. test_event_trace_export      — EventExporter broadcasts spans
                                       on $tracing.spans
    9. test_span_attributes         — span.tags contain action/service

Usage:
    .venv/bin/python moleculerpy/examples/demo_observability.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
from dataclasses import dataclass, field
from typing import Any, ClassVar

# Silence default root logging noise from structlog/basicConfig — demo
# captures everything via custom LoggerProtocol implementations instead.
logging.basicConfig(level=logging.CRITICAL)

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action, event
from moleculerpy.metric_reporters import ConsoleReporter, PrometheusReporter
from moleculerpy.middleware.metrics import MetricsMiddleware
from moleculerpy.middleware.tracing import TracingMiddleware
from moleculerpy.service import Service
from moleculerpy.settings import Settings
from moleculerpy.tracing import (
    BaseTraceExporter,
    ConsoleExporter,
    EventExporter,
    Span,
    TracerOptions,
)

EXPECTED_CALLS = 100
EXPECTED_GAUGE = 42
EXPECTED_CHAIN_TOTAL = 5


# ---------------------------------------------------------------------------
# ANSI colors (demo output only)
# ---------------------------------------------------------------------------

GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
BOLD = "\033[1m"
RESET = "\033[0m"


def _c(text: str, color: str) -> str:
    return f"{color}{text}{RESET}"


# ---------------------------------------------------------------------------
# Capturing logger (LoggerProtocol-compatible)
# ---------------------------------------------------------------------------


@dataclass
class LogRecord:
    level: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)


class CapturingLogger:
    """LoggerProtocol implementation that captures every emitted record.

    Mirrors the minimal surface MoleculerPy's LoggerAdapter expects and
    also implements ``bind`` so contextual fields (node, service, …)
    attach to captured records for later assertions.
    """

    LEVEL_ORDER: ClassVar[dict[str, int]] = {
        "DEBUG": 10,
        "INFO": 20,
        "WARN": 30,
        "WARNING": 30,
        "ERROR": 40,
        "FATAL": 50,
    }

    def __init__(
        self,
        records: list[LogRecord] | None = None,
        context: dict[str, Any] | None = None,
        min_level: str = "DEBUG",
    ) -> None:
        self.records = records if records is not None else []
        self.context = context or {}
        self.min_level = min_level.upper()

    def bind(self, **kwargs: Any) -> CapturingLogger:
        return CapturingLogger(self.records, {**self.context, **kwargs}, self.min_level)

    def _emit(self, level: str, msg: str, **kwargs: Any) -> None:
        if self.LEVEL_ORDER.get(level, 0) < self.LEVEL_ORDER.get(self.min_level, 0):
            return
        ctx = {**self.context, **kwargs}
        self.records.append(LogRecord(level=level, message=str(msg), context=ctx))

    def debug(self, msg: str, **kwargs: Any) -> None:
        self._emit("DEBUG", msg, **kwargs)

    def info(self, msg: str, **kwargs: Any) -> None:
        self._emit("INFO", msg, **kwargs)

    def warn(self, msg: str, **kwargs: Any) -> None:
        self._emit("WARN", msg, **kwargs)

    def warning(self, msg: str, **kwargs: Any) -> None:
        self._emit("WARN", msg, **kwargs)

    def error(self, msg: str, **kwargs: Any) -> None:
        self._emit("ERROR", msg, **kwargs)

    def fatal(self, msg: str, **kwargs: Any) -> None:
        self._emit("FATAL", msg, **kwargs)

    def trace(self, msg: str, **kwargs: Any) -> None:
        self._emit("DEBUG", msg, **kwargs)


# ---------------------------------------------------------------------------
# Sample services
# ---------------------------------------------------------------------------


class MathService(Service):
    name = "math"

    def __init__(self) -> None:
        super().__init__(self.name)

    @action()
    async def add(self, ctx: Any) -> int:
        return int(ctx.params["a"]) + int(ctx.params["b"])


class GreeterService(Service):
    name = "greeter"

    def __init__(self) -> None:
        super().__init__(self.name)

    @action()
    async def hello(self, ctx: Any) -> str:
        self.logger.info("greeter.hello called", name=ctx.params.get("name"))
        return f"hello {ctx.params.get('name', 'world')}"

    @action()
    async def chain(self, ctx: Any) -> dict[str, Any]:
        # Cross-service call so tracing builds a parent/child span tree.
        total = await ctx.call("math.add", {"a": 2, "b": 3})
        return {"greeting": f"hi {ctx.params.get('name', 'x')}", "total": total}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _make_broker(
    *,
    node_id: str,
    logger: CapturingLogger | None = None,
    log_level: str = "INFO",
    middlewares: list[Any] | None = None,
    services: list[Service] | None = None,
) -> ServiceBroker:
    settings = Settings(
        transporter="memory://",
        log_level=log_level,
        logger=logger,
        middlewares=middlewares or [],
    )
    broker = ServiceBroker(id=node_id, settings=settings)
    for svc in services or []:
        await broker.register(svc)
    await asyncio.wait_for(broker.start(), timeout=5.0)
    return broker


async def _safe_stop(broker: ServiceBroker) -> None:
    try:
        await asyncio.wait_for(broker.stop(), timeout=5.0)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Test result plumbing
# ---------------------------------------------------------------------------


@dataclass
class TestResult:
    pillar: str
    name: str
    passed: bool
    detail: str = ""


async def _run_test(
    pillar: str,
    name: str,
    coro: Any,
    results: list[TestResult],
) -> None:
    try:
        detail = await coro
        results.append(TestResult(pillar, name, True, detail or ""))
        print(f"  {_c('PASS', GREEN)} {name} {_c(detail or '', CYAN)}")
    except AssertionError as e:
        results.append(TestResult(pillar, name, False, f"assert: {e}"))
        print(f"  {_c('FAIL', RED)} {name} — assert: {e}")
    except Exception as e:
        results.append(TestResult(pillar, name, False, f"{type(e).__name__}: {e}"))
        print(f"  {_c('FAIL', RED)} {name} — {type(e).__name__}: {e}")


# ===========================================================================
# LOGGING TESTS
# ===========================================================================


async def test_structured_log_capture() -> str:
    logger = CapturingLogger()
    broker = await _make_broker(
        node_id="obs-log-1",
        logger=logger,
        services=[MathService()],
    )
    try:
        await broker.call("math.add", {"a": 1, "b": 2})
    finally:
        await _safe_stop(broker)

    assert logger.records, "no log records captured"
    # At least one record should carry node context (bound by broker).
    with_node = [r for r in logger.records if "node" in r.context]
    assert with_node, "no records have bound node context"
    sample = with_node[0]
    assert sample.context.get("node") == "obs-log-1", f"unexpected node: {sample.context}"
    return f"{len(logger.records)} records, node ctx ok"


async def test_log_level_filter() -> str:
    logger = CapturingLogger(min_level="ERROR")
    broker = await _make_broker(
        node_id="obs-log-2",
        logger=logger,
        log_level="ERROR",
        services=[MathService()],
    )
    try:
        await broker.call("math.add", {"a": 1, "b": 1})
        # Emit noisy INFO/DEBUG manually through the bound broker logger.
        broker.logger.info("should be filtered")
        broker.logger.debug("should also be filtered")
        broker.logger.error("kept")
    finally:
        await _safe_stop(broker)

    levels = {r.level for r in logger.records}
    assert "INFO" not in levels, f"INFO leaked through ERROR filter: {levels}"
    assert "DEBUG" not in levels, f"DEBUG leaked through ERROR filter: {levels}"
    assert any(r.level == "ERROR" for r in logger.records), "no ERROR record captured"
    return f"levels={sorted(levels)}"


async def test_service_scoped_logger() -> str:
    logger = CapturingLogger()
    broker = await _make_broker(
        node_id="obs-log-3",
        logger=logger,
        services=[GreeterService(), MathService()],
    )
    try:
        await broker.call("greeter.hello", {"name": "claude"})
    finally:
        await _safe_stop(broker)

    svc_records = [r for r in logger.records if r.context.get("service") == "greeter"]
    assert svc_records, (
        f"no records tagged service=greeter; "
        f"services seen: {sorted({r.context.get('service') for r in logger.records})}"
    )
    # The explicit self.logger.info call emits "greeter.hello called".
    assert any("greeter.hello called" in r.message for r in svc_records), (
        "service.logger.info output not captured"
    )
    return f"{len(svc_records)} service-scoped records"


# ===========================================================================
# METRICS TESTS
# ===========================================================================


async def test_console_reporter() -> str:
    mw = MetricsMiddleware()
    # Attach both reporters; ConsoleReporter is the requested one, and
    # we also exercise PrometheusReporter side-by-side per task spec.
    console = ConsoleReporter({"interval": 0})
    prom = PrometheusReporter()
    console.init(mw.registry)
    prom.init(mw.registry)

    broker = await _make_broker(
        node_id="obs-metrics-1",
        middlewares=[mw],
        services=[MathService()],
    )
    try:
        for i in range(EXPECTED_CALLS):
            await broker.call("math.add", {"a": i, "b": 1})
    finally:
        await _safe_stop(broker)

    counter = mw._request_total  # type: ignore[attr-defined]
    total = 0.0
    # Counter stores per-label-set values — sum all success entries.
    for labels, value in counter._values.items():  # type: ignore[attr-defined]
        if "status=success" in str(labels) or any("success" in str(v) for v in labels):
            total += float(value)
        else:
            total += float(value)
    assert total >= EXPECTED_CALLS, f"counter only saw {total} requests (expected ≥100)"
    return f"request_total={int(total)}"


async def test_prometheus_format() -> str:
    mw = MetricsMiddleware()
    broker = await _make_broker(
        node_id="obs-metrics-2",
        middlewares=[mw],
        services=[MathService()],
    )
    try:
        for _ in range(5):
            await broker.call("math.add", {"a": 10, "b": 20})
    finally:
        await _safe_stop(broker)

    text = mw.registry.to_prometheus()
    assert "# HELP" in text, "no # HELP lines in Prometheus output"
    assert "# TYPE" in text, "no # TYPE lines in Prometheus output"
    assert "moleculer_request_total" in text, "standard counter missing from exposition"
    # Metric line shape: name{labels} value
    body_lines = [
        line
        for line in text.splitlines()
        if line and not line.startswith("#") and "moleculer_request_total" in line
    ]
    assert body_lines, "no metric sample lines for moleculer_request_total"
    sample = body_lines[0]
    assert "{" in sample and "}" in sample, f"expected labels in {sample!r}"
    return f"{len(body_lines)} request_total samples"


async def test_custom_metric() -> str:
    mw = MetricsMiddleware()
    gauge = mw.registry.gauge("my_gauge", "Test gauge")
    gauge.set(42)

    found = mw.registry._metrics.get("my_gauge")  # type: ignore[attr-defined]
    assert found is gauge, "gauge not registered under its name"
    assert gauge.get() == EXPECTED_GAUGE, f"gauge value is {gauge.get()}"

    # And confirm it surfaces in Prometheus export too.
    text = mw.registry.to_prometheus()
    assert "my_gauge" in text, "custom gauge missing from Prometheus output"
    return "my_gauge=42"


# ===========================================================================
# TRACING TESTS
# ===========================================================================


class CapturingExporter(BaseTraceExporter):
    """Test exporter — stores every finished span for assertions."""

    def __init__(self, opts: dict[str, Any] | None = None) -> None:
        super().__init__(opts)
        self.finished: list[Span] = []

    def span_finished(self, span: Span) -> None:
        self.finished.append(span)


async def test_console_trace_export() -> str:
    capture = CapturingExporter()
    console = ConsoleExporter({"colors": False})
    tracing_mw = TracingMiddleware(TracerOptions(enabled=True, exporter=[console, capture]))

    broker = await _make_broker(
        node_id="obs-trace-1",
        middlewares=[tracing_mw],
        services=[GreeterService(), MathService()],
    )
    try:
        result = await broker.call("greeter.chain", {"name": "ada"})
        assert result.get("total") == EXPECTED_CHAIN_TOTAL, f"chain result wrong: {result}"
    finally:
        await _safe_stop(broker)

    names = [s.name for s in capture.finished]
    assert any("greeter.chain" in n for n in names), f"parent span missing: {names}"
    assert any("math.add" in n for n in names), f"child span missing: {names}"

    # Nested relationship: math.add must have a parent id set.
    child = next(s for s in capture.finished if "math.add" in s.name)
    parent = next(s for s in capture.finished if "greeter.chain" in s.name)
    assert child.parent_id == parent.id, (
        f"math.add parent={child.parent_id!r} expected {parent.id!r}"
    )
    return f"{len(capture.finished)} spans, nested ok"


async def test_event_trace_export() -> str:
    tracing_mw = TracingMiddleware(
        TracerOptions(enabled=True, exporter=[EventExporter({"send_finished_span": True})])
    )
    received: list[dict[str, Any]] = []

    class TraceSink(Service):
        name = "trace-sink"

        def __init__(self) -> None:
            super().__init__(self.name)

        @event("$tracing.spans")
        async def on_spans(self, ctx: Any) -> None:
            payload = ctx.params
            if isinstance(payload, dict) and "spans" in payload:
                received.extend(payload["spans"])

    broker = await _make_broker(
        node_id="obs-trace-2",
        middlewares=[tracing_mw],
        services=[MathService(), TraceSink()],
    )

    try:
        await broker.call("math.add", {"a": 7, "b": 8})
        # EventExporter schedules broadcast as a task — yield to the loop.
        for _ in range(20):
            await asyncio.sleep(0.05)
            if received:
                break
    finally:
        await _safe_stop(broker)

    assert received, "no span payload delivered over $tracing.spans"
    first = received[0]
    assert "name" in first and "id" in first, f"span dict missing core fields: {first}"
    return f"{len(received)} spans via event bus"


async def test_span_attributes() -> str:
    capture = CapturingExporter()
    tracing_mw = TracingMiddleware(TracerOptions(enabled=True, exporter=[capture]))

    broker = await _make_broker(
        node_id="obs-trace-3",
        middlewares=[tracing_mw],
        services=[MathService()],
    )
    try:
        await broker.call("math.add", {"a": 40, "b": 2})
    finally:
        await _safe_stop(broker)

    assert capture.finished, "no spans captured"
    span = next((s for s in capture.finished if "math.add" in s.name), capture.finished[0])
    tags = span.tags or {}
    # TracingMiddleware populates action + action_type tags from the action object.
    expected_keys = ("action", "action_type")
    missing = [k for k in expected_keys if k not in tags]
    assert not missing, f"span.tags missing {missing}; have keys {sorted(tags)}"
    assert tags.get("action") == "math.add", f"action tag={tags.get('action')}"
    return f"tags={sorted(tags)[:4]}"


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


async def main() -> int:
    print(_c(f"{BOLD}MoleculerPy Observability Demo Stand{RESET}", CYAN))
    print(_c("=" * 56, CYAN))

    results: list[TestResult] = []

    print(_c("\n[LOGGING]", BOLD))
    await _run_test(
        "LOGGING", "test_structured_log_capture", test_structured_log_capture(), results
    )
    await _run_test("LOGGING", "test_log_level_filter", test_log_level_filter(), results)
    await _run_test("LOGGING", "test_service_scoped_logger", test_service_scoped_logger(), results)

    print(_c("\n[METRICS]", BOLD))
    await _run_test("METRICS", "test_console_reporter", test_console_reporter(), results)
    await _run_test("METRICS", "test_prometheus_format", test_prometheus_format(), results)
    await _run_test("METRICS", "test_custom_metric", test_custom_metric(), results)

    print(_c("\n[TRACING]", BOLD))
    await _run_test("TRACING", "test_console_trace_export", test_console_trace_export(), results)
    await _run_test("TRACING", "test_event_trace_export", test_event_trace_export(), results)
    await _run_test("TRACING", "test_span_attributes", test_span_attributes(), results)

    # --- Summary table -----------------------------------------------------
    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed

    print()
    print(_c("=" * 56, CYAN))
    print(_c(f"{BOLD}Summary{RESET}", CYAN))
    print(_c("=" * 56, CYAN))
    print(f"{'Pillar':<10} {'Test':<34} {'Status':<6}")
    print("-" * 56)
    for r in results:
        status = _c("PASS", GREEN) if r.passed else _c("FAIL", RED)
        print(f"{r.pillar:<10} {r.name:<34} {status}")
    print("-" * 56)
    color = GREEN if failed == 0 else RED
    print(_c(f"Total: {passed}/{len(results)} passed, {failed} failed", color))

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    try:
        rc = asyncio.run(main())
    except KeyboardInterrupt:
        rc = 130
    sys.exit(rc)
