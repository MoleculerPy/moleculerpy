"""E2E tests for ZipkinExporter and JaegerExporter with real ServiceBroker.

Validates that exporters collect spans during actual action calls
and that ConsoleExporter remains backward-compatible.

HTTP backend is mocked via unittest.mock.patch on urllib.request.urlopen.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action
from moleculerpy.middleware.tracing import TracingMiddleware
from moleculerpy.service import Service
from moleculerpy.settings import Settings
from moleculerpy.tracing import (
    ConsoleExporter,
    JaegerExporter,
    ZipkinExporter,
)

# =============================================================================
# Test Service
# =============================================================================


class MathService(Service):
    """Simple math service for tracing tests."""

    name = "math"

    @action()
    async def add(self, ctx) -> int:  # type: ignore[no-untyped-def]
        return ctx.params["a"] + ctx.params["b"]


# =============================================================================
# Helpers
# =============================================================================


def make_fake_urlopen(collected_payloads: list) -> object:
    """Return a fake urlopen callable that captures POSTed JSON."""

    def fake_urlopen(req, timeout=None):
        body = req.data
        if body:
            data = json.loads(body.decode())
            collected_payloads.append(data)
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        return ctx

    return fake_urlopen


# =============================================================================
# Test 1: ZipkinExporter — spans collected after action call
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_zipkin_exporter_collects_spans_on_action_call() -> None:
    """ZipkinExporter receives spans when a broker action is called."""
    collected_payloads: list[list[dict]] = []

    zipkin = ZipkinExporter(base_url="http://localhost:9411", interval=999)
    tracing_mw = TracingMiddleware(
        {
            "enabled": True,
            "exporter": [zipkin],
            "sampling_rate": 1.0,
        }
    )
    settings = Settings(transporter="memory://", middlewares=[tracing_mw])
    broker = ServiceBroker(id="zipkin-test-node", settings=settings)
    await broker.register(MathService())
    await broker.start()
    await asyncio.sleep(0.05)

    try:
        result = await broker.call("math.add", {"a": 3, "b": 7})
        assert result == 10

        # Manually flush — no need to wait for periodic timer
        with patch("urllib.request.urlopen", side_effect=make_fake_urlopen(collected_payloads)):
            await zipkin.flush()

        # At least one span batch should have been POSTed
        assert len(collected_payloads) >= 1
        all_spans = [span for batch in collected_payloads for span in batch]
        assert len(all_spans) >= 1

        # Verify span has required Zipkin v2 fields
        span = all_spans[0]
        assert "traceId" in span
        assert "id" in span
        assert "name" in span
        assert "timestamp" in span
        assert "duration" in span

    finally:
        await broker.stop()


# =============================================================================
# Test 2: JaegerExporter — spans collected after action call
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_jaeger_exporter_collects_spans_on_action_call() -> None:
    """JaegerExporter receives spans when a broker action is called."""
    collected_payloads: list[list[dict]] = []

    jaeger = JaegerExporter(endpoint="http://localhost:14268/api/traces", interval=999)
    tracing_mw = TracingMiddleware(
        {
            "enabled": True,
            "exporter": [jaeger],
            "sampling_rate": 1.0,
        }
    )
    settings = Settings(transporter="memory://", middlewares=[tracing_mw])
    broker = ServiceBroker(id="jaeger-test-node", settings=settings)
    await broker.register(MathService())
    await broker.start()
    await asyncio.sleep(0.05)

    try:
        result = await broker.call("math.add", {"a": 10, "b": 20})
        assert result == 30

        with patch("urllib.request.urlopen", side_effect=make_fake_urlopen(collected_payloads)):
            await jaeger.flush()

        assert len(collected_payloads) >= 1
        all_spans = [span for batch in collected_payloads for span in batch]
        assert len(all_spans) >= 1

        span = all_spans[0]
        # Same Zipkin v2 format — Jaeger accepts it
        assert "traceId" in span
        assert "id" in span
        assert "name" in span
        assert "timestamp" in span
        assert "duration" in span

    finally:
        await broker.stop()


# =============================================================================
# Test 3: ConsoleExporter — backward compatibility
# =============================================================================


class _CapturingConsoleExporter(ConsoleExporter):
    """ConsoleExporter subclass that records spans before printing."""

    def __init__(self, captured: list, opts: dict | None = None) -> None:
        super().__init__(opts)
        self._captured = captured

    def span_finished(self, span: object) -> None:
        self._captured.append(span)
        super().span_finished(span)  # type: ignore[arg-type]


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_console_exporter_still_works() -> None:
    """ConsoleExporter remains functional after adding Zipkin/Jaeger exporters."""
    finished_spans: list = []

    console = _CapturingConsoleExporter(finished_spans, {"colors": False})

    tracing_mw = TracingMiddleware(
        {
            "enabled": True,
            "exporter": [console],
            "sampling_rate": 1.0,
        }
    )
    settings = Settings(transporter="memory://", middlewares=[tracing_mw])
    broker = ServiceBroker(id="console-test-node", settings=settings)
    await broker.register(MathService())
    await broker.start()
    await asyncio.sleep(0.05)

    try:
        result = await broker.call("math.add", {"a": 1, "b": 2})
        assert result == 3

        # At least one span should have been delivered to the console exporter
        assert len(finished_spans) >= 1
        span = finished_spans[0]
        assert span.name is not None
        assert span.duration >= 0

    finally:
        await broker.stop()
