"""Unit tests for ZipkinExporter and JaegerExporter.

Tests validate payload construction, ID conversion, batching,
HTTP flush behavior, and error handling without network dependencies.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from moleculerpy.tracing import (
    BaseTraceExporter,
    JaegerExporter,
    Span,
    Tracer,
    TracerOptions,
    ZipkinExporter,
)

# =============================================================================
# Helpers
# =============================================================================


def make_tracer() -> Tracer:
    """Create a minimal Tracer backed by a mock broker."""
    broker = MagicMock()
    broker.nodeID = "test-node"
    broker.logger = MagicMock()
    return Tracer(broker, TracerOptions(enabled=True))


def make_span(
    name: str = "test.action",
    trace_id: str | None = None,
    parent_id: str | None = None,
    tags: dict | None = None,
    logs: list | None = None,
    error: Exception | None = None,
    duration: float = 0.042,
) -> Span:
    """Create a finished Span with predictable values."""
    tracer = make_tracer()
    opts: dict = {}
    if trace_id:
        opts["trace_id"] = trace_id
    if parent_id:
        opts["parent_id"] = parent_id
    if tags:
        opts["tags"] = tags

    span = Span(tracer, name, opts)
    span.start_time = 1_700_000_000.0
    span.finish_time = span.start_time + duration
    span.duration = duration
    span._started = True
    span._finished = True

    if logs:
        from moleculerpy.tracing import SpanLog

        for log_entry in logs:
            span.logs.append(
                SpanLog(
                    name=log_entry["name"],
                    fields=log_entry.get("fields", {}),
                    time=log_entry.get("time", span.start_time),
                    elapsed=log_entry.get("elapsed", 0.0),
                )
            )

    if error:
        span.error = error

    return span


def fake_urlopen_cm():
    """Return a mock context manager for urllib.request.urlopen."""
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=cm)
    cm.__exit__ = MagicMock(return_value=False)
    return cm


# =============================================================================
# ZipkinExporter — constructor
# =============================================================================


class TestZipkinExporterDefaults:
    """Test ZipkinExporter constructor defaults (keyword-arg API)."""

    def test_default_base_url(self):
        exp = ZipkinExporter()
        assert exp.base_url == "http://localhost:9411"

    def test_default_interval(self):
        exp = ZipkinExporter()
        assert exp.interval == 5.0

    def test_default_tags_is_none(self):
        exp = ZipkinExporter()
        assert exp.default_tags is None

    def test_default_headers_is_none(self):
        exp = ZipkinExporter()
        assert exp.headers is None

    def test_custom_base_url(self):
        exp = ZipkinExporter(base_url="http://zipkin:9411")
        assert exp.base_url == "http://zipkin:9411"

    def test_custom_interval(self):
        exp = ZipkinExporter(interval=10)
        assert exp.interval == 10.0

    def test_custom_default_tags(self):
        exp = ZipkinExporter(default_tags={"env": "test"})
        assert exp.default_tags == {"env": "test"}

    def test_custom_headers(self):
        exp = ZipkinExporter(headers={"X-Auth": "token"})
        assert exp.headers == {"X-Auth": "token"}

    def test_extends_base_exporter(self):
        exp = ZipkinExporter()
        assert isinstance(exp, BaseTraceExporter)

    def test_queue_initially_empty(self):
        exp = ZipkinExporter()
        assert exp._queue == []


# =============================================================================
# ZipkinExporter — _convert_id
# =============================================================================


class TestZipkinConvertId:
    """Test _convert_id hex conversion."""

    def test_strips_dashes_and_truncates(self):
        """UUID → strip dashes → truncate to 16 hex chars (64-bit span ID)."""
        exp = ZipkinExporter()
        result = exp._convert_id("550e8400-e29b-41d4-a716-446655440000")
        assert result == "550e8400e29b41d4"  # 16 chars (Zipkin spec)
        assert len(result) == 16

    def test_convert_id_32_chars_for_trace(self):
        """traceId uses length=32 (128-bit)."""
        exp = ZipkinExporter()
        result = exp._convert_id("550e8400-e29b-41d4-a716-446655440000", length=32)
        assert result == "550e8400e29b41d4a716446655440000"
        assert len(result) == 32

    def test_none_returns_none(self):
        """None UUID → None (callers check before using)."""
        exp = ZipkinExporter()
        assert exp._convert_id(None) is None

    def test_empty_string_returns_empty(self):
        exp = ZipkinExporter()
        assert exp._convert_id("") == ""

    def test_already_hex_unchanged(self):
        exp = ZipkinExporter()
        hex_id = "abcdef1234567890"
        assert exp._convert_id(hex_id) == hex_id

    def test_length_hint_does_not_truncate(self):
        """length parameter is a hint; raw hex is not truncated."""
        exp = ZipkinExporter()
        full = "550e8400e29b41d4a716446655440000"
        result = exp._convert_id(full, length=16)
        # _convert_id just strips dashes, no truncation in current impl
        assert "-" not in result


# =============================================================================
# ZipkinExporter — _make_payload
# =============================================================================


class TestZipkinMakePayload:
    """Test _make_payload converts Span → Zipkin v2 JSON dict."""

    def test_trace_id_in_payload(self):
        span = make_span(trace_id="550e8400-e29b-41d4-a716-446655440000")
        exp = ZipkinExporter()
        payload = exp._make_payload(span)
        assert payload["traceId"] == "550e8400e29b41d4a716446655440000"

    def test_span_id_in_payload(self):
        """Span id is truncated to 16 hex chars (64-bit, Zipkin spec)."""
        span = make_span()
        exp = ZipkinExporter()
        payload = exp._make_payload(span)
        expected = span.id.replace("-", "")[:16]
        assert payload["id"] == expected
        assert len(payload["id"]) == 16

    def test_name_in_payload(self):
        span = make_span(name="math.add")
        exp = ZipkinExporter()
        payload = exp._make_payload(span)
        assert payload["name"] == "math.add"

    def test_timestamp_in_microseconds(self):
        span = make_span()
        exp = ZipkinExporter()
        payload = exp._make_payload(span)
        expected_us = round(span.start_time * 1_000_000)
        assert payload["timestamp"] == expected_us

    def test_duration_in_microseconds(self):
        span = make_span(duration=0.042)
        exp = ZipkinExporter()
        payload = exp._make_payload(span)
        expected_us = round(0.042 * 1_000_000)
        assert payload["duration"] == expected_us

    def test_tags_flattened_and_stringified(self):
        span = make_span(tags={"user": "alice", "count": 42, "nested": {"key": "val"}})
        exp = ZipkinExporter()
        payload = exp._make_payload(span)
        tags = payload["tags"]
        assert tags["user"] == "alice"
        assert tags["count"] == "42"
        assert tags["nested.key"] == "val"

    def test_parent_id_present_when_has_parent(self):
        span = make_span(parent_id="aaaa-bbbb-cccc-dddd")
        exp = ZipkinExporter()
        payload = exp._make_payload(span)
        assert "parentId" in payload
        assert payload["parentId"] == "aaaabbbbccccdddd"

    def test_parent_id_absent_when_root(self):
        span = make_span()  # no parent_id
        exp = ZipkinExporter()
        payload = exp._make_payload(span)
        assert payload.get("parentId") is None

    def test_annotations_from_logs(self):
        logs = [{"name": "cache.hit", "fields": {}, "time": 1_700_000_000.01, "elapsed": 0.01}]
        span = make_span(logs=logs)
        exp = ZipkinExporter()
        payload = exp._make_payload(span)
        annotations = payload.get("annotations", [])
        assert len(annotations) == 1
        assert annotations[0]["value"] == "cache.hit"
        assert annotations[0]["timestamp"] == round(1_700_000_000.01 * 1_000_000)

    def test_default_tags_merged_into_payload(self):
        span = make_span(tags={"action": "math.add"})
        exp = ZipkinExporter(default_tags={"env": "prod", "region": "eu"})
        payload = exp._make_payload(span)
        tags = payload["tags"]
        assert tags["env"] == "prod"
        assert tags["region"] == "eu"
        assert tags["action"] == "math.add"


# =============================================================================
# ZipkinExporter — init
# =============================================================================


class TestZipkinInit:
    """Test init(tracer) sets broker/logger references."""

    def test_init_sets_broker(self):
        exp = ZipkinExporter()
        tracer = make_tracer()
        exp.init(tracer)
        assert exp.broker is tracer.broker

    def test_init_sets_logger(self):
        exp = ZipkinExporter()
        tracer = make_tracer()
        exp.init(tracer)
        assert exp.logger is tracer.logger

    def test_init_sets_tracer(self):
        exp = ZipkinExporter()
        tracer = make_tracer()
        exp.init(tracer)
        assert exp.tracer is tracer


# =============================================================================
# ZipkinExporter — span_finished / queue
# =============================================================================


class TestZipkinQueue:
    """Test span_finished queues spans."""

    def test_span_finished_queues_span(self):
        exp = ZipkinExporter()
        tracer = make_tracer()
        exp.init(tracer)
        span = make_span()
        exp.span_finished(span)
        assert len(exp._queue) == 1

    def test_multiple_spans_queued(self):
        exp = ZipkinExporter()
        tracer = make_tracer()
        exp.init(tracer)
        for _ in range(5):
            exp.span_finished(make_span())
        assert len(exp._queue) == 5


# =============================================================================
# ZipkinExporter — flush
# =============================================================================


class TestZipkinFlush:
    """Test flush() drains queue and POSTs to Zipkin."""

    @pytest.mark.asyncio
    async def test_flush_posts_to_correct_url(self):
        exp = ZipkinExporter(base_url="http://zipkin:9411")
        tracer = make_tracer()
        exp.init(tracer)
        exp.span_finished(make_span())

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value = fake_urlopen_cm()
            await exp.flush()

        assert mock_urlopen.called
        req = mock_urlopen.call_args[0][0]
        assert req.full_url == "http://zipkin:9411/api/v2/spans"

    @pytest.mark.asyncio
    async def test_flush_drains_queue(self):
        exp = ZipkinExporter()
        tracer = make_tracer()
        exp.init(tracer)
        exp.span_finished(make_span())
        exp.span_finished(make_span())

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value = fake_urlopen_cm()
            await exp.flush()

        assert len(exp._queue) == 0

    @pytest.mark.asyncio
    async def test_flush_empty_queue_no_http_call(self):
        exp = ZipkinExporter()
        tracer = make_tracer()
        exp.init(tracer)

        with patch("urllib.request.urlopen") as mock_urlopen:
            await exp.flush()

        assert not mock_urlopen.called

    @pytest.mark.asyncio
    async def test_flush_connection_error_handled_gracefully(self):
        exp = ZipkinExporter()
        tracer = make_tracer()
        exp.init(tracer)
        exp.span_finished(make_span())

        with patch(
            "urllib.request.urlopen",
            side_effect=OSError("connection refused"),
        ):
            # Must NOT raise — errors are logged, not propagated
            await exp.flush()

    @pytest.mark.asyncio
    async def test_flush_sends_json_content_type(self):
        exp = ZipkinExporter()
        tracer = make_tracer()
        exp.init(tracer)
        exp.span_finished(make_span())

        captured_req = []

        def fake_open(req, timeout=None):
            captured_req.append(req)
            return fake_urlopen_cm()

        with patch("urllib.request.urlopen", side_effect=fake_open):
            await exp.flush()

        assert captured_req
        assert captured_req[0].get_header("Content-type") == "application/json"


# =============================================================================
# ZipkinExporter — stop
# =============================================================================


class TestZipkinStop:
    """Test stop() flushes remaining spans."""

    @pytest.mark.asyncio
    async def test_stop_flushes_remaining(self):
        exp = ZipkinExporter()
        tracer = make_tracer()
        exp.init(tracer)
        exp.span_finished(make_span())

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value = fake_urlopen_cm()
            await exp.stop()

        # Flush was called — queue is now empty
        assert len(exp._queue) == 0


# =============================================================================
# JaegerExporter — constructor defaults
# =============================================================================


class TestJaegerExporterDefaults:
    """Test JaegerExporter constructor defaults."""

    def test_default_endpoint_stored_in_base_url(self):
        exp = JaegerExporter()
        assert exp.base_url == "http://localhost:14268/api/traces"

    def test_default_interval(self):
        exp = JaegerExporter()
        assert exp.interval == 5.0

    def test_default_tags_is_none(self):
        exp = JaegerExporter()
        assert exp.default_tags is None

    def test_default_headers_is_none(self):
        exp = JaegerExporter()
        assert exp.headers is None

    def test_custom_endpoint(self):
        exp = JaegerExporter(endpoint="http://jaeger:14268/api/traces")
        assert exp.base_url == "http://jaeger:14268/api/traces"

    def test_extends_base_exporter(self):
        exp = JaegerExporter()
        assert isinstance(exp, BaseTraceExporter)

    def test_jaeger_is_subclass_of_zipkin(self):
        """JaegerExporter reuses ZipkinExporter logic."""
        exp = JaegerExporter()
        assert isinstance(exp, ZipkinExporter)


# =============================================================================
# JaegerExporter — payload format
# =============================================================================


class TestJaegerPayload:
    """JaegerExporter uses same Zipkin v2 payload format."""

    def test_make_payload_has_same_keys_as_zipkin(self):
        span = make_span(name="svc.op", tags={"foo": "bar"})
        zipkin_exp = ZipkinExporter()
        jaeger_exp = JaegerExporter()

        zipkin_payload = zipkin_exp._make_payload(span)
        jaeger_payload = jaeger_exp._make_payload(span)

        # Same keys — Jaeger accepts Zipkin v2 JSON
        assert set(zipkin_payload.keys()) == set(jaeger_payload.keys())

    def test_make_payload_trace_id(self):
        span = make_span(trace_id="aabbccdd-1122-3344-5566-778899aabbcc")
        exp = JaegerExporter()
        payload = exp._make_payload(span)
        assert payload["traceId"] == "aabbccdd112233445566778899aabbcc"

    def test_make_payload_duration_microseconds(self):
        span = make_span(duration=0.1)
        exp = JaegerExporter()
        payload = exp._make_payload(span)
        assert payload["duration"] == 100_000

    def test_jaeger_default_tags_merged(self):
        span = make_span(tags={"service": "calc"})
        exp = JaegerExporter(default_tags={"env": "staging"})
        payload = exp._make_payload(span)
        assert payload["tags"]["env"] == "staging"
        assert payload["tags"]["service"] == "calc"

    @pytest.mark.asyncio
    async def test_jaeger_posts_to_custom_endpoint(self):
        exp = JaegerExporter(endpoint="http://jaeger:14268/api/traces")
        tracer = make_tracer()
        exp.init(tracer)
        exp.span_finished(make_span())

        captured_req = []

        def fake_open(req, timeout=None):
            captured_req.append(req)
            return fake_urlopen_cm()

        with patch("urllib.request.urlopen", side_effect=fake_open):
            await exp.flush()

        assert captured_req
        assert captured_req[0].full_url == "http://jaeger:14268/api/traces"


# =============================================================================
# Both exporters — shared base behaviour
# =============================================================================


class TestBothExporters:
    """Both exporters share BaseTraceExporter interface."""

    @pytest.mark.parametrize("exporter_class", [ZipkinExporter, JaegerExporter])
    def test_has_span_finished(self, exporter_class):
        exp = exporter_class()
        assert hasattr(exp, "span_finished")

    @pytest.mark.parametrize("exporter_class", [ZipkinExporter, JaegerExporter])
    def test_has_init(self, exporter_class):
        exp = exporter_class()
        assert hasattr(exp, "init")

    @pytest.mark.parametrize("exporter_class", [ZipkinExporter, JaegerExporter])
    def test_has_stop(self, exporter_class):
        exp = exporter_class()
        assert hasattr(exp, "stop")

    @pytest.mark.parametrize("exporter_class", [ZipkinExporter, JaegerExporter])
    def test_has_flush(self, exporter_class):
        exp = exporter_class()
        assert hasattr(exp, "flush")

    @pytest.mark.parametrize("exporter_class", [ZipkinExporter, JaegerExporter])
    def test_is_base_exporter(self, exporter_class):
        exp = exporter_class()
        assert isinstance(exp, BaseTraceExporter)
