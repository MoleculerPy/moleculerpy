"""Tests for MoleculerPy distributed tracing system.

Tests cover:
- Span lifecycle (create, start, finish, tags, logs, errors)
- Tracer (sampling, exporters, span management)
- Exporters (ConsoleExporter, EventExporter)
- TracingMiddleware integration
- Context.start_span/finish_span methods
"""

import asyncio
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from moleculerpy.context import Context
from moleculerpy.middleware.tracing import TracingMiddleware
from moleculerpy.tracing import (
    BaseTraceExporter,
    ConsoleExporter,
    EventExporter,
    Span,
    SpanLog,
    Tracer,
    TracerOptions,
)

# =============================================================================
# SpanLog Tests
# =============================================================================


class TestSpanLog:
    """Tests for SpanLog dataclass."""

    def test_spanlog_creation(self):
        """Test SpanLog creation with defaults."""
        log = SpanLog(name="test.event")
        assert log.name == "test.event"
        assert log.fields == {}
        assert log.time == 0.0
        assert log.elapsed == 0.0

    def test_spanlog_with_fields(self):
        """Test SpanLog with custom fields."""
        log = SpanLog(
            name="db.query",
            fields={"query": "SELECT *", "rows": 10},
            time=1234567890.0,
            elapsed=0.042,
        )
        assert log.name == "db.query"
        assert log.fields == {"query": "SELECT *", "rows": 10}
        assert log.time == 1234567890.0
        assert log.elapsed == 0.042


# =============================================================================
# Span Tests
# =============================================================================


class TestSpan:
    """Tests for Span class."""

    @pytest.fixture
    def mock_tracer(self):
        """Create mock tracer."""
        tracer = MagicMock()
        tracer.span_started = MagicMock()
        tracer.span_finished = MagicMock()
        return tracer

    def test_span_creation(self, mock_tracer):
        """Test basic span creation."""
        span = Span(mock_tracer, "test-operation")
        assert span.name == "test-operation"
        assert span.tracer is mock_tracer
        assert len(span.id) == 36  # UUID format
        assert span.trace_id == span.id  # Default trace_id = id
        assert span.parent_id is None
        assert span.type == "custom"
        assert span.sampled is True
        assert span.priority == 5
        assert span.tags == {}
        assert span.logs == []
        assert span.error is None
        assert span._started is False
        assert span._finished is False

    def test_span_creation_with_opts(self, mock_tracer):
        """Test span creation with options."""
        span = Span(
            mock_tracer,
            "db.query",
            {
                "id": "custom-id",
                "trace_id": "trace-123",
                "parent_id": "parent-456",
                "type": "action",
                "sampled": False,
                "priority": 8,
                "tags": {"db": "postgres"},
                "service": {"name": "users"},
            },
        )
        assert span.id == "custom-id"
        assert span.trace_id == "trace-123"
        assert span.parent_id == "parent-456"
        assert span.type == "action"
        assert span.sampled is False
        assert span.priority == 8
        assert span.tags == {"db": "postgres"}
        assert span.service == {"name": "users"}

    def test_span_start(self, mock_tracer):
        """Test span start."""
        span = Span(mock_tracer, "test")
        assert span._started is False

        result = span.start()

        assert result is span  # Returns self
        assert span._started is True
        assert span.start_time > 0
        mock_tracer.span_started.assert_called_once_with(span)

    def test_span_start_idempotent(self, mock_tracer):
        """Test span start is idempotent."""
        span = Span(mock_tracer, "test")
        span.start()
        start_time = span.start_time

        span.start()  # Second call

        assert span.start_time == start_time  # Unchanged
        assert mock_tracer.span_started.call_count == 1

    def test_span_start_with_custom_time(self, mock_tracer):
        """Test span start with custom time."""
        span = Span(mock_tracer, "test")
        custom_time = 1234567890.123

        span.start(time_=custom_time)

        assert span.start_time == custom_time

    def test_span_finish(self, mock_tracer):
        """Test span finish."""
        span = Span(mock_tracer, "test")
        span.start()
        time.sleep(0.01)  # Small delay

        result = span.finish()

        assert result is span
        assert span._finished is True
        assert span.finish_time > span.start_time
        assert span.duration > 0
        mock_tracer.span_finished.assert_called_once_with(span)

    def test_span_finish_idempotent(self, mock_tracer):
        """Test span finish is idempotent."""
        span = Span(mock_tracer, "test")
        span.start()
        span.finish()
        finish_time = span.finish_time

        span.finish()  # Second call

        assert span.finish_time == finish_time
        assert mock_tracer.span_finished.call_count == 1

    def test_span_add_tags(self, mock_tracer):
        """Test adding tags."""
        span = Span(mock_tracer, "test")

        result = span.add_tags({"key1": "value1"})
        assert result is span  # Chaining
        assert span.tags == {"key1": "value1"}

        span.add_tags({"key2": "value2", "key3": 42})
        assert span.tags == {"key1": "value1", "key2": "value2", "key3": 42}

    def test_span_log(self, mock_tracer):
        """Test adding log entries."""
        span = Span(mock_tracer, "test")
        span.start()

        result = span.log("cache.hit", {"key": "user:123"})

        assert result is span  # Chaining
        assert len(span.logs) == 1
        assert span.logs[0].name == "cache.hit"
        assert span.logs[0].fields == {"key": "user:123"}
        assert span.logs[0].elapsed >= 0

    def test_span_set_error(self, mock_tracer):
        """Test setting error."""
        span = Span(mock_tracer, "test")
        error = ValueError("Something went wrong")

        result = span.set_error(error)

        assert result is span
        assert span.error is error

    def test_span_is_active(self, mock_tracer):
        """Test is_active check."""
        span = Span(mock_tracer, "test")
        assert span.is_active() is False

        span.start()
        assert span.is_active() is True

        span.finish()
        assert span.is_active() is False

    def test_span_start_span_child(self, mock_tracer):
        """Test creating child span."""
        parent = Span(mock_tracer, "parent", {"trace_id": "trace-123"})
        parent.start()

        # Mock tracer.start_span to return a new Span
        child_span = Span(mock_tracer, "child")
        mock_tracer.start_span = MagicMock(return_value=child_span)

        parent.start_span("child", {"tags": {"extra": True}})

        mock_tracer.start_span.assert_called_once()
        call_opts = mock_tracer.start_span.call_args[0][1]
        assert call_opts["trace_id"] == "trace-123"
        assert call_opts["parent_id"] == parent.id
        assert call_opts["sampled"] == parent.sampled
        assert call_opts["tags"] == {"extra": True}

    def test_span_to_dict(self, mock_tracer):
        """Test span serialization."""
        span = Span(
            mock_tracer,
            "test",
            {
                "trace_id": "trace-123",
                "parent_id": "parent-456",
                "tags": {"action": "users.get"},
            },
        )
        span.start()
        span.log("checkpoint", {"data": "test"})
        span.finish()

        data = span.to_dict()

        assert data["name"] == "test"
        assert data["traceID"] == "trace-123"
        assert data["parentID"] == "parent-456"
        assert data["tags"] == {"action": "users.get"}
        assert len(data["logs"]) == 1
        assert data["duration"] > 0

    def test_span_to_dict_with_error(self, mock_tracer):
        """Test span serialization with error."""
        span = Span(mock_tracer, "test")
        span.start()
        span.set_error(ValueError("test error"))
        span.finish()

        data = span.to_dict()

        assert data["error"] is not None
        assert data["error"]["name"] == "ValueError"
        assert data["error"]["message"] == "test error"

    def test_span_repr(self, mock_tracer):
        """Test span string representation."""
        span = Span(mock_tracer, "test-operation")
        assert "test-operation" in repr(span)
        assert "created" in repr(span)

        span.start()
        assert "active" in repr(span)

        span.finish()
        assert "finished" in repr(span)


# =============================================================================
# Tracer Tests
# =============================================================================


class TestTracer:
    """Tests for Tracer class."""

    @pytest.fixture
    def mock_broker(self):
        """Create mock broker."""
        broker = MagicMock()
        broker.nodeID = "test-node"
        broker.logger = MagicMock()
        return broker

    def test_tracer_creation_defaults(self, mock_broker):
        """Test tracer creation with defaults."""
        tracer = Tracer(mock_broker)

        assert tracer.broker is mock_broker
        assert tracer.opts.enabled is True
        assert tracer.opts.sampling_rate == 1.0
        assert tracer.opts.actions is True
        assert tracer.opts.events is False
        assert tracer.exporters == []

    def test_tracer_creation_with_opts_dict(self, mock_broker):
        """Test tracer creation with dict options."""
        tracer = Tracer(
            mock_broker,
            {
                "enabled": True,
                "sampling_rate": 0.5,
                "events": True,
            },
        )

        assert tracer.opts.sampling_rate == 0.5
        assert tracer.opts.events is True

    def test_tracer_creation_with_tracer_options(self, mock_broker):
        """Test tracer creation with TracerOptions."""
        opts = TracerOptions(enabled=True, sampling_rate=0.25)
        tracer = Tracer(mock_broker, opts)

        assert tracer.opts.sampling_rate == 0.25

    def test_tracer_init_with_exporters(self, mock_broker):
        """Test tracer init initializes exporters."""
        exporter1 = MagicMock(spec=BaseTraceExporter)
        exporter2 = MagicMock(spec=BaseTraceExporter)

        tracer = Tracer(
            mock_broker,
            TracerOptions(
                exporter=[exporter1, exporter2],
            ),
        )
        tracer.init()

        exporter1.init.assert_called_once_with(tracer)
        exporter2.init.assert_called_once_with(tracer)
        assert len(tracer.exporters) == 2

    def test_tracer_init_disabled(self, mock_broker):
        """Test tracer init when disabled."""
        exporter = MagicMock(spec=BaseTraceExporter)
        tracer = Tracer(
            mock_broker,
            TracerOptions(
                enabled=False,
                exporter=[exporter],
            ),
        )

        tracer.init()

        exporter.init.assert_not_called()
        assert len(tracer.exporters) == 0

    @pytest.mark.asyncio
    async def test_tracer_stop(self, mock_broker):
        """Test tracer stop (async — supports both sync and async exporters)."""
        exporter = MagicMock(spec=BaseTraceExporter)
        tracer = Tracer(mock_broker, TracerOptions(exporter=[exporter]))
        tracer.init()

        await tracer.stop()

        exporter.stop.assert_called_once()
        assert len(tracer.exporters) == 0

    def test_tracer_is_enabled(self, mock_broker):
        """Test is_enabled check."""
        tracer_enabled = Tracer(mock_broker, TracerOptions(enabled=True))
        tracer_disabled = Tracer(mock_broker, TracerOptions(enabled=False))

        assert tracer_enabled.is_enabled() is True
        assert tracer_disabled.is_enabled() is False

    def test_tracer_start_span(self, mock_broker):
        """Test starting a span."""
        tracer = Tracer(mock_broker)
        tracer.init()

        span = tracer.start_span(
            "test-operation",
            {
                "tags": {"key": "value"},
            },
        )

        assert span.name == "test-operation"
        assert span._started is True
        assert span.tags["key"] == "value"

    def test_tracer_start_span_with_default_tags(self, mock_broker):
        """Test start_span applies default tags."""
        tracer = Tracer(
            mock_broker,
            TracerOptions(
                default_tags={"env": "test", "version": "1.0"},
            ),
        )
        tracer.init()

        span = tracer.start_span("test", {"tags": {"custom": "tag"}})

        assert span.tags["env"] == "test"
        assert span.tags["version"] == "1.0"
        assert span.tags["custom"] == "tag"

    def test_tracer_start_span_with_callable_default_tags(self, mock_broker):
        """Test start_span with callable default tags."""
        tracer = Tracer(
            mock_broker,
            TracerOptions(
                default_tags=lambda: {"timestamp": time.time()},
            ),
        )
        tracer.init()

        span = tracer.start_span("test")

        assert "timestamp" in span.tags

    def test_tracer_should_sample_all(self, mock_broker):
        """Test sampling with rate=1.0."""
        tracer = Tracer(mock_broker, TracerOptions(sampling_rate=1.0))
        span = Span(tracer, "test")

        # All should pass
        for _ in range(10):
            assert tracer.should_sample(span) is True

    def test_tracer_should_sample_none(self, mock_broker):
        """Test sampling with rate=0.0."""
        tracer = Tracer(mock_broker, TracerOptions(sampling_rate=0.0))
        span = Span(tracer, "test")

        # None should pass
        for _ in range(10):
            assert tracer.should_sample(span) is False

    def test_tracer_should_sample_probabilistic(self, mock_broker):
        """Test probabilistic sampling."""
        tracer = Tracer(mock_broker, TracerOptions(sampling_rate=0.5))
        span = Span(tracer, "test")

        # With 0.5 rate, roughly half should pass
        sampled = sum(1 for _ in range(100) if tracer.should_sample(span))
        assert 30 <= sampled <= 70  # Allow some variance

    def test_tracer_should_sample_priority(self, mock_broker):
        """Test priority-based sampling."""
        tracer = Tracer(mock_broker, TracerOptions(sampling_min_priority=5))

        low_priority = Span(tracer, "test")
        low_priority.priority = 3
        assert tracer.should_sample(low_priority) is False

        high_priority = Span(tracer, "test")
        high_priority.priority = 7
        assert tracer.should_sample(high_priority) is True

    def test_tracer_should_sample_rate_limiting(self, mock_broker):
        """Test rate-limiting sampling."""
        tracer = Tracer(
            mock_broker,
            TracerOptions(
                sampling_traces_per_second=10,
            ),
        )
        span = Span(tracer, "test")

        # First few should pass, then be limited
        passed = 0
        for _ in range(50):
            if tracer.should_sample(span):
                passed += 1

        # Should be limited to ~10/sec initially
        assert passed < 50

    def test_tracer_span_started_notifies_exporters(self, mock_broker):
        """Test span_started notifies exporters."""
        exporter = MagicMock(spec=BaseTraceExporter)
        tracer = Tracer(mock_broker, TracerOptions(exporter=[exporter]))
        tracer.init()

        span = Span(tracer, "test")
        span.sampled = True
        tracer.span_started(span)

        exporter.span_started.assert_called_once_with(span)

    def test_tracer_span_started_skips_unsampled(self, mock_broker):
        """Test span_started skips unsampled spans."""
        exporter = MagicMock(spec=BaseTraceExporter)
        tracer = Tracer(mock_broker, TracerOptions(exporter=[exporter]))
        tracer.init()

        span = Span(tracer, "test")
        span.sampled = False
        tracer.span_started(span)

        exporter.span_started.assert_not_called()

    def test_tracer_span_finished_notifies_exporters(self, mock_broker):
        """Test span_finished notifies exporters."""
        exporter = MagicMock(spec=BaseTraceExporter)
        tracer = Tracer(mock_broker, TracerOptions(exporter=[exporter]))
        tracer.init()

        span = Span(tracer, "test")
        span.sampled = True
        tracer.span_finished(span)

        exporter.span_finished.assert_called_once_with(span)

    def test_tracer_repr(self, mock_broker):
        """Test tracer string representation."""
        tracer = Tracer(
            mock_broker,
            TracerOptions(
                enabled=True,
                sampling_rate=0.5,
            ),
        )
        repr_str = repr(tracer)

        assert "enabled=True" in repr_str
        assert "sampling_rate=0.5" in repr_str


# =============================================================================
# Exporter Tests
# =============================================================================


class TestConsoleExporter:
    """Tests for ConsoleExporter."""

    @pytest.fixture
    def mock_tracer(self):
        """Create mock tracer."""
        broker = MagicMock()
        broker.logger = MagicMock()
        tracer = Tracer(broker)
        return tracer

    def test_console_exporter_creation(self):
        """Test console exporter creation."""
        exporter = ConsoleExporter()
        assert exporter.opts == {}

    def test_console_exporter_with_options(self):
        """Test console exporter with options."""
        exporter = ConsoleExporter(
            {
                "colors": False,
                "width": 100,
            }
        )
        assert exporter._colors is False
        assert exporter._width == 100

    def test_console_exporter_span_finished(self, mock_tracer, capsys):
        """Test console exporter prints span."""
        exporter = ConsoleExporter({"colors": False})
        exporter.init(mock_tracer)

        span = Span(mock_tracer, "test-operation")
        span.start()
        span.add_tags({"key": "value"})
        span.finish()

        exporter.span_finished(span)

        captured = capsys.readouterr()
        assert "test-operation" in captured.out

    def test_console_exporter_span_with_error(self, mock_tracer, capsys):
        """Test console exporter shows error."""
        exporter = ConsoleExporter({"colors": False})
        exporter.init(mock_tracer)

        span = Span(mock_tracer, "failing-op")
        span.start()
        span.set_error(ValueError("test error"))
        span.finish()

        exporter.span_finished(span)

        captured = capsys.readouterr()
        assert "ERROR" in captured.out
        assert "ValueError" in captured.out


class TestEventExporter:
    """Tests for EventExporter."""

    @pytest.fixture
    def mock_tracer(self):
        """Create mock tracer with mock broker."""
        broker = MagicMock()
        broker.broadcast = AsyncMock()
        broker.logger = MagicMock()
        tracer = Tracer(broker)
        return tracer

    def test_event_exporter_creation(self):
        """Test event exporter creation."""
        exporter = EventExporter()
        assert exporter._event_name == "$tracing.spans"
        assert exporter._send_immediately is True

    def test_event_exporter_with_options(self):
        """Test event exporter with options."""
        exporter = EventExporter(
            {
                "event_name": "custom.spans",
                "send_finished_span": False,
                "batch_size": 50,
            }
        )
        assert exporter._event_name == "custom.spans"
        assert exporter._send_immediately is False
        assert exporter._batch_size == 50


class TestBaseTraceExporter:
    """Tests for BaseTraceExporter helper methods."""

    class DummyExporter(BaseTraceExporter):
        """Dummy exporter for testing."""

        def span_finished(self, span: Span) -> None:
            pass

    def test_flatten_tags_simple(self):
        """Test flattening simple tags."""
        exporter = self.DummyExporter()
        result = exporter.flatten_tags({"a": 1, "b": "two"})

        assert result == {"a": 1, "b": "two"}

    def test_flatten_tags_nested(self):
        """Test flattening nested tags."""
        exporter = self.DummyExporter()
        result = exporter.flatten_tags(
            {
                "error": {
                    "name": "ValueError",
                    "message": "test",
                },
                "simple": "value",
            }
        )

        assert result == {
            "error.name": "ValueError",
            "error.message": "test",
            "simple": "value",
        }

    def test_flatten_tags_convert_to_string(self):
        """Test flattening with string conversion."""
        exporter = self.DummyExporter()
        result = exporter.flatten_tags(
            {"num": 42, "none": None},
            convert_to_string=True,
        )

        assert result == {"num": "42", "none": ""}

    def test_error_to_object(self):
        """Test error extraction."""
        exporter = self.DummyExporter()

        class CustomError(Exception):
            code = 500

        error = CustomError("test message")
        result = exporter.error_to_object(error)

        assert result["name"] == "CustomError"
        assert result["message"] == "test message"
        assert result["code"] == 500

    def test_error_to_object_with_stack(self):
        """Test error extraction with stack trace."""
        exporter = self.DummyExporter()
        error = ValueError("test")

        result = exporter.error_to_object(
            error,
            fields=("name", "message", "stack"),
        )

        assert "name" in result
        assert "stack" in result


# =============================================================================
# TracingMiddleware Tests
# =============================================================================


class TestTracingMiddleware:
    """Tests for TracingMiddleware."""

    @pytest.fixture
    def mock_broker(self):
        """Create mock broker."""
        broker = MagicMock()
        broker.nodeID = "test-node"
        broker.logger = MagicMock()
        broker.tracer = None  # Will be set by middleware
        return broker

    def test_middleware_creation_defaults(self):
        """Test middleware creation with defaults."""
        mw = TracingMiddleware()

        assert mw.opts.enabled is True
        assert mw.opts.sampling_rate == 1.0
        assert mw.opts.actions is True
        assert mw.opts.events is False

    def test_middleware_creation_with_dict(self):
        """Test middleware creation with dict options."""
        mw = TracingMiddleware(
            {
                "enabled": True,
                "sampling": {"rate": 0.5},
                "events": True,
            }
        )

        assert mw.opts.sampling_rate == 0.5
        assert mw.opts.events is True

    def test_middleware_flatten_opts(self):
        """Test options flattening."""
        mw = TracingMiddleware()
        result = mw._flatten_opts(
            {
                "sampling": {
                    "rate": 0.5,
                    "tracesPerSecond": 100,
                    "minPriority": 3,
                },
            }
        )

        assert result["sampling_rate"] == 0.5
        assert result["sampling_traces_per_second"] == 100
        assert result["sampling_min_priority"] == 3

    def test_middleware_broker_created(self, mock_broker):
        """Test broker_created hook."""
        mw = TracingMiddleware()

        mw.broker_created(mock_broker)

        assert mw._broker is mock_broker
        assert mw.tracer is not None
        assert mock_broker.tracer is mw.tracer

    @pytest.mark.asyncio
    async def test_middleware_broker_starting(self, mock_broker):
        """Test broker_starting hook initializes tracer."""
        exporter = MagicMock(spec=BaseTraceExporter)
        mw = TracingMiddleware(TracerOptions(exporter=[exporter]))
        mw.broker_created(mock_broker)

        await mw.broker_starting(mock_broker)

        exporter.init.assert_called_once()

    @pytest.mark.asyncio
    async def test_middleware_broker_stopped(self, mock_broker):
        """Test broker_stopped hook stops tracer."""
        exporter = MagicMock(spec=BaseTraceExporter)
        mw = TracingMiddleware(TracerOptions(exporter=[exporter]))
        mw.broker_created(mock_broker)
        await mw.broker_starting(mock_broker)

        await mw.broker_stopped(mock_broker)

        exporter.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_middleware_local_action_creates_span(self, mock_broker):
        """Test local_action creates span."""
        mw = TracingMiddleware()
        mw.broker_created(mock_broker)
        await mw.broker_starting(mock_broker)

        action = MagicMock()
        action.name = "users.get"

        ctx = Context(
            id="ctx-123",
            action="users.get",
            request_id="req-456",
            broker=mock_broker,
        )

        async def handler(ctx: Any) -> str:
            return "result"

        wrapped = await mw.local_action(handler, action)
        result = await wrapped(ctx)

        assert result == "result"
        assert ctx.span is not None
        assert ctx.span._finished is True

    @pytest.mark.asyncio
    async def test_middleware_local_action_with_error(self, mock_broker):
        """Test local_action captures errors in span."""
        mw = TracingMiddleware()
        mw.broker_created(mock_broker)
        await mw.broker_starting(mock_broker)

        action = MagicMock()
        action.name = "users.get"

        ctx = Context(
            id="ctx-123",
            action="users.get",
            request_id="req-456",
            broker=mock_broker,
        )

        async def failing_handler(ctx: Any) -> str:
            raise ValueError("test error")

        wrapped = await mw.local_action(failing_handler, action)

        with pytest.raises(ValueError):
            await wrapped(ctx)

        assert ctx.span is not None
        assert ctx.span.error is not None
        assert ctx.span._finished is True

    @pytest.mark.asyncio
    async def test_middleware_local_action_disabled(self, mock_broker):
        """Test local_action bypasses when disabled."""
        mw = TracingMiddleware({"enabled": False})
        mw.broker_created(mock_broker)

        action = MagicMock()
        action.name = "users.get"

        async def handler(ctx: Any) -> str:
            return "result"

        wrapped = await mw.local_action(handler, action)

        # Should return the original handler (or equivalent)
        ctx = Context(id="ctx-123", broker=mock_broker)
        result = await wrapped(ctx)
        assert result == "result"

    @pytest.mark.asyncio
    async def test_middleware_local_action_tracing_false(self, mock_broker):
        """Test local_action skips when ctx.tracing=False."""
        mw = TracingMiddleware()
        mw.broker_created(mock_broker)
        await mw.broker_starting(mock_broker)

        action = MagicMock()
        action.name = "users.get"

        ctx = Context(
            id="ctx-123",
            action="users.get",
            tracing=False,  # Explicitly disabled
            broker=mock_broker,
        )

        async def handler(ctx: Any) -> str:
            return "result"

        wrapped = await mw.local_action(handler, action)
        result = await wrapped(ctx)

        assert result == "result"
        assert ctx.span is None  # No span created

    @pytest.mark.asyncio
    async def test_middleware_remote_action_creates_span(self, mock_broker):
        """Test remote_action creates span."""
        mw = TracingMiddleware()
        mw.broker_created(mock_broker)
        await mw.broker_starting(mock_broker)

        action = MagicMock()
        action.name = "users.get"

        ctx = Context(
            id="ctx-123",
            action="users.get",
            request_id="req-456",
            broker=mock_broker,
        )

        async def handler(ctx: Any) -> str:
            return "remote result"

        wrapped = await mw.remote_action(handler, action)
        result = await wrapped(ctx)

        assert result == "remote result"
        assert ctx.span is not None

    @pytest.mark.asyncio
    async def test_middleware_local_event_disabled_by_default(self, mock_broker):
        """Test local_event does not trace by default."""
        mw = TracingMiddleware()  # events=False by default
        mw.broker_created(mock_broker)
        await mw.broker_starting(mock_broker)

        event = MagicMock()
        event.name = "user.created"

        async def handler(ctx: Any) -> None:
            pass

        wrapped = await mw.local_event(handler, event)

        ctx = Context(id="ctx-123", event="user.created", broker=mock_broker)
        await wrapped(ctx)

        # No span should be created
        assert ctx.span is None

    @pytest.mark.asyncio
    async def test_middleware_local_event_enabled(self, mock_broker):
        """Test local_event traces when events=True."""
        mw = TracingMiddleware({"events": True})
        mw.broker_created(mock_broker)
        await mw.broker_starting(mock_broker)

        event = MagicMock()
        event.name = "user.created"

        ctx = Context(
            id="ctx-123",
            event="user.created",
            request_id="req-456",
            broker=mock_broker,
        )

        async def handler(ctx: Any) -> None:
            pass

        wrapped = await mw.local_event(handler, event)
        await wrapped(ctx)

        assert ctx.span is not None
        assert ctx.span._finished is True


# =============================================================================
# Context Integration Tests
# =============================================================================


class TestContextTracing:
    """Tests for Context tracing methods."""

    @pytest.fixture
    def mock_broker_with_tracer(self):
        """Create mock broker with working tracer."""
        broker = MagicMock()
        broker.nodeID = "test-node"
        broker.logger = MagicMock()

        # Create real tracer
        tracer = Tracer(broker, TracerOptions(enabled=True))
        tracer.init()
        broker.tracer = tracer

        return broker

    def test_context_start_span_no_broker(self):
        """Test start_span returns None without broker."""
        ctx = Context(id="ctx-123")

        result = ctx.start_span("test")

        assert result is None
        assert ctx.span is None

    def test_context_start_span_no_tracer(self):
        """Test start_span returns None without tracer."""
        broker = MagicMock()
        broker.tracer = None
        ctx = Context(id="ctx-123", broker=broker)

        result = ctx.start_span("test")

        assert result is None

    def test_context_start_span(self, mock_broker_with_tracer):
        """Test start_span creates and sets span."""
        ctx = Context(
            id="ctx-123",
            request_id="req-456",
            broker=mock_broker_with_tracer,
        )

        span = ctx.start_span("db.query", {"tags": {"table": "users"}})

        assert span is not None
        assert ctx.span is span
        assert span.name == "db.query"
        assert span.trace_id == "req-456"
        assert span.tags.get("table") == "users"

    def test_context_start_span_nested(self, mock_broker_with_tracer):
        """Test nested spans with stack."""
        ctx = Context(
            id="ctx-123",
            request_id="req-456",
            broker=mock_broker_with_tracer,
        )

        span1 = ctx.start_span("outer")
        assert ctx.span is span1
        assert len(ctx._span_stack) == 0

        span2 = ctx.start_span("inner")
        assert ctx.span is span2
        assert len(ctx._span_stack) == 1
        assert ctx._span_stack[0] is span1

        # span2 should have span1 as parent
        assert span2.parent_id == span1.id

    def test_context_finish_span(self, mock_broker_with_tracer):
        """Test finish_span finishes and restores."""
        ctx = Context(
            id="ctx-123",
            request_id="req-456",
            broker=mock_broker_with_tracer,
        )

        span = ctx.start_span("operation")
        finished = ctx.finish_span()

        assert finished is span
        assert span._finished is True
        assert ctx.span is None

    def test_context_finish_span_nested(self, mock_broker_with_tracer):
        """Test finish_span restores parent span."""
        ctx = Context(
            id="ctx-123",
            request_id="req-456",
            broker=mock_broker_with_tracer,
        )

        span1 = ctx.start_span("outer")
        span2 = ctx.start_span("inner")

        # Finish inner
        finished2 = ctx.finish_span()
        assert finished2 is span2
        assert ctx.span is span1

        # Finish outer
        finished1 = ctx.finish_span()
        assert finished1 is span1
        assert ctx.span is None

    def test_context_finish_span_no_active(self, mock_broker_with_tracer):
        """Test finish_span with no active span."""
        ctx = Context(
            id="ctx-123",
            broker=mock_broker_with_tracer,
        )

        result = ctx.finish_span()

        assert result is None

    def test_context_current_span(self, mock_broker_with_tracer):
        """Test current_span getter."""
        ctx = Context(
            id="ctx-123",
            request_id="req-456",
            broker=mock_broker_with_tracer,
        )

        assert ctx.current_span() is None

        span = ctx.start_span("test")
        assert ctx.current_span() is span

        ctx.finish_span()
        assert ctx.current_span() is None


# =============================================================================
# TracerOptions Tests
# =============================================================================


class TestTracerOptions:
    """Tests for TracerOptions dataclass."""

    def test_tracer_options_defaults(self):
        """Test TracerOptions default values."""
        opts = TracerOptions()

        assert opts.enabled is True
        assert opts.exporter == []
        assert opts.sampling_rate == 1.0
        assert opts.sampling_traces_per_second is None
        assert opts.sampling_min_priority is None
        assert opts.actions is True
        assert opts.events is False
        assert opts.error_fields == ("name", "message", "code")
        assert opts.stack_trace is False
        assert opts.default_tags is None

    def test_tracer_options_custom(self):
        """Test TracerOptions with custom values."""
        exporter = MagicMock(spec=BaseTraceExporter)
        opts = TracerOptions(
            enabled=False,
            exporter=[exporter],
            sampling_rate=0.5,
            sampling_traces_per_second=100.0,
            sampling_min_priority=5,
            actions=False,
            events=True,
            stack_trace=True,
            default_tags={"env": "test"},
        )

        assert opts.enabled is False
        assert len(opts.exporter) == 1
        assert opts.sampling_rate == 0.5
        assert opts.sampling_traces_per_second == 100.0
        assert opts.sampling_min_priority == 5
        assert opts.actions is False
        assert opts.events is True
        assert opts.stack_trace is True
        assert opts.default_tags == {"env": "test"}


# =============================================================================
# Additional Tests for Full Coverage
# =============================================================================


class TestTracingMiddlewareEdgeCases:
    """Edge case tests for TracingMiddleware."""

    @pytest.fixture
    def mock_broker(self):
        """Create mock broker."""
        broker = MagicMock()
        broker.nodeID = "test-node"
        broker.logger = MagicMock()
        broker.tracer = None
        return broker

    def test_middleware_repr(self):
        """Test middleware string representation."""
        mw = TracingMiddleware({"enabled": True, "events": True})
        repr_str = repr(mw)

        assert "TracingMiddleware" in repr_str
        assert "enabled=True" in repr_str
        assert "events=True" in repr_str

    @pytest.mark.asyncio
    async def test_middleware_local_action_cancelled_error(self, mock_broker):
        """Test CancelledError is properly handled and re-raised."""
        mw = TracingMiddleware()
        mw.broker_created(mock_broker)
        await mw.broker_starting(mock_broker)

        action = MagicMock()
        action.name = "users.get"

        ctx = Context(
            id="ctx-123",
            action="users.get",
            request_id="req-456",
            broker=mock_broker,
        )

        async def cancelling_handler(ctx: Any) -> str:
            raise asyncio.CancelledError()

        wrapped = await mw.local_action(cancelling_handler, action)

        with pytest.raises(asyncio.CancelledError):
            await wrapped(ctx)

        # Span should be finished with cancelled status
        assert ctx.span is not None
        assert ctx.span._finished is True
        assert ctx.span.tags.get("status") == "cancelled"

    @pytest.mark.asyncio
    async def test_middleware_remote_action_cancelled_error(self, mock_broker):
        """Test remote action CancelledError handling."""
        mw = TracingMiddleware()
        mw.broker_created(mock_broker)
        await mw.broker_starting(mock_broker)

        action = MagicMock()
        action.name = "users.get"

        ctx = Context(
            id="ctx-123",
            action="users.get",
            request_id="req-456",
            broker=mock_broker,
        )

        async def cancelling_handler(ctx: Any) -> str:
            raise asyncio.CancelledError()

        wrapped = await mw.remote_action(cancelling_handler, action)

        with pytest.raises(asyncio.CancelledError):
            await wrapped(ctx)

        assert ctx.span.tags.get("status") == "cancelled"

    @pytest.mark.asyncio
    async def test_middleware_local_event_cancelled_error(self, mock_broker):
        """Test event handler CancelledError handling."""
        mw = TracingMiddleware({"events": True})
        mw.broker_created(mock_broker)
        await mw.broker_starting(mock_broker)

        event = MagicMock()
        event.name = "user.created"

        ctx = Context(
            id="ctx-123",
            event="user.created",
            request_id="req-456",
            broker=mock_broker,
        )

        async def cancelling_handler(ctx: Any) -> None:
            raise asyncio.CancelledError()

        wrapped = await mw.local_event(cancelling_handler, event)

        with pytest.raises(asyncio.CancelledError):
            await wrapped(ctx)

        assert ctx.span.tags.get("status") == "cancelled"


class TestEventExporterAsync:
    """Async-specific tests for EventExporter."""

    @pytest.fixture
    def mock_tracer(self):
        """Create mock tracer with mock broker."""
        broker = MagicMock()
        broker.broadcast = AsyncMock()
        broker.logger = MagicMock()
        tracer = Tracer(broker)
        return tracer

    def test_event_exporter_handle_emit_result_success(self, mock_tracer):
        """Test _handle_emit_result with successful task."""
        exporter = EventExporter()
        exporter.init(mock_tracer)

        # Create a completed task
        task = MagicMock()
        task.exception.return_value = None

        # Should not raise
        exporter._handle_emit_result(task)

    def test_event_exporter_handle_emit_result_with_error(self, mock_tracer):
        """Test _handle_emit_result logs errors."""
        exporter = EventExporter()
        exporter.init(mock_tracer)

        # Create a task that failed
        task = MagicMock()
        task.exception.return_value = RuntimeError("broadcast failed")

        # Should log error but not raise
        exporter._handle_emit_result(task)

        # Verify warning was logged
        mock_tracer.broker.logger.warn.assert_called_once()
        call_args = mock_tracer.broker.logger.warn.call_args[0][0]
        assert "Span emission failed" in call_args

    def test_event_exporter_handle_emit_result_cancelled(self, mock_tracer):
        """Test _handle_emit_result handles cancelled task."""
        exporter = EventExporter()
        exporter.init(mock_tracer)

        # Create a cancelled task
        task = MagicMock()
        task.exception.side_effect = asyncio.CancelledError()

        # Should not raise
        exporter._handle_emit_result(task)

    def test_event_exporter_handle_emit_result_invalid_state(self, mock_tracer):
        """Test _handle_emit_result handles invalid state."""
        exporter = EventExporter()
        exporter.init(mock_tracer)

        # Create a task with invalid state
        task = MagicMock()
        task.exception.side_effect = asyncio.InvalidStateError()

        # Should not raise
        exporter._handle_emit_result(task)

    @pytest.mark.asyncio
    async def test_event_exporter_emit_spans_no_broker(self):
        """Test _emit_spans with no broker does nothing."""
        exporter = EventExporter()
        # Don't init, so broker is None

        # Should not raise
        exporter._emit_spans([{"id": "span-1"}])


class TestParametrizedSampling:
    """Parametrized tests for sampling strategies."""

    @pytest.fixture
    def mock_broker(self):
        """Create mock broker."""
        broker = MagicMock()
        broker.logger = MagicMock()
        return broker

    @pytest.mark.parametrize(
        "rate,expected_range",
        [
            (0.0, (0, 5)),  # None should pass
            (0.25, (15, 35)),  # ~25% should pass
            (0.5, (40, 60)),  # ~50% should pass
            (0.75, (65, 85)),  # ~75% should pass
            (1.0, (100, 100)),  # All should pass
        ],
    )
    def test_probabilistic_sampling_rates(self, mock_broker, rate, expected_range):
        """Test probabilistic sampling at various rates."""
        tracer = Tracer(mock_broker, TracerOptions(sampling_rate=rate))
        span = Span(tracer, "test")

        # Sample 100 times
        passed = sum(1 for _ in range(100) if tracer.should_sample(span))

        # Check within expected range (allowing variance)
        min_expected, max_expected = expected_range
        assert min_expected <= passed <= max_expected, (
            f"Expected {min_expected}-{max_expected} samples, got {passed} for rate={rate}"
        )

    @pytest.mark.parametrize(
        "priority,min_priority,expected",
        [
            (1, 5, False),  # Low priority blocked
            (5, 5, True),  # Equal passes
            (10, 5, True),  # High priority passes
            (3, None, True),  # No min = always pass
        ],
    )
    def test_priority_sampling(self, mock_broker, priority, min_priority, expected):
        """Test priority-based sampling."""
        tracer = Tracer(
            mock_broker,
            TracerOptions(
                sampling_min_priority=min_priority,
            ),
        )
        span = Span(tracer, "test")
        span.priority = priority

        assert tracer.should_sample(span) == expected
