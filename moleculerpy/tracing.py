"""Distributed tracing system for MoleculerPy framework.

Provides Moleculer.js-compatible distributed tracing with:
- Span-based trace collection with parent-child relationships
- Pluggable exporters (Console, Event, Jaeger, Zipkin compatible)
- Sampling strategies (probabilistic, rate-limiting, priority-based)
- Context propagation through requestID and parentID

Based on Moleculer.js tracing module design patterns.

Usage:
    from moleculerpy.tracing import Tracer, ConsoleExporter

    # Configure broker with tracing
    settings = Settings(
        tracing={
            'enabled': True,
            'exporter': [ConsoleExporter()],
            'sampling': {'rate': 1.0},
        }
    )

    broker = ServiceBroker("my-node", settings=settings)

    # Spans are automatically created for action calls
    # Or create custom spans:
    span = ctx.start_span("my-operation")
    span.add_tags({"key": "value"})
    span.finish()
"""

from __future__ import annotations

import asyncio
import threading
import time
import traceback
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from moleculerpy.broker import ServiceBroker


# =============================================================================
# Span Log Entry
# =============================================================================


@dataclass(slots=True)
class SpanLog:
    """Log entry within a span.

    Attributes:
        name: Event name (e.g., "cache.hit", "db.query")
        fields: Additional data for this event
        time: Absolute timestamp (seconds since epoch)
        elapsed: Time since span start (seconds)
    """

    name: str
    fields: dict[str, Any] = field(default_factory=dict)
    time: float = 0.0
    elapsed: float = 0.0


# =============================================================================
# Span Class
# =============================================================================


class Span:
    """Represents a single trace span (unit of work).

    A span tracks the timing and metadata of an operation. Spans can have
    parent-child relationships to form a trace tree.

    Attributes:
        tracer: Reference to the Tracer instance
        name: Human-readable name for this operation
        id: Unique identifier for this span
        trace_id: ID shared by all spans in the same trace
        parent_id: ID of the parent span (if any)
        type: Span type ("action", "event", "custom")
        service: Service metadata dict
        sampled: Whether this span should be exported
        priority: Sampling priority (1-10, default 5)
        tags: Key-value metadata
        logs: List of SpanLog entries
        error: Exception if operation failed
        start_time: Start timestamp (seconds)
        finish_time: Finish timestamp (seconds)
        duration: Duration in seconds

    Example:
        span = tracer.start_span("db.query", {
            "type": "custom",
            "tags": {"query": "SELECT * FROM users"}
        })
        try:
            result = execute_query()
            span.add_tags({"rows": len(result)})
        except Exception as e:
            span.set_error(e)
        finally:
            span.finish()
    """

    __slots__ = (
        "_finished",
        "_started",
        "duration",
        "error",
        "finish_time",
        "id",
        "logs",
        "name",
        "parent_id",
        "priority",
        "sampled",
        "service",
        "start_time",
        "tags",
        "trace_id",
        "tracer",
        "type",
    )

    def __init__(
        self,
        tracer: Tracer,
        name: str,
        opts: dict[str, Any] | None = None,
    ) -> None:
        """Initialize a span.

        Args:
            tracer: The Tracer instance managing this span
            name: Operation name
            opts: Optional configuration:
                - id: Span ID (auto-generated if not provided)
                - trace_id: Trace ID (inherits from parent or auto-generated)
                - parent_id: Parent span ID
                - type: Span type ("action", "event", "custom")
                - service: Service metadata dict
                - sampled: Whether to export this span
                - priority: Sampling priority (1-10)
                - tags: Initial tags dict
        """
        opts = opts or {}

        self.tracer = tracer
        self.name = name

        # Identity
        self.id: str = opts.get("id") or str(uuid.uuid4())
        self.trace_id: str = opts.get("trace_id") or self.id
        self.parent_id: str | None = opts.get("parent_id")

        # Metadata
        self.type: str = opts.get("type", "custom")
        self.service: dict[str, Any] = opts.get("service") or {}
        self.sampled: bool = opts.get("sampled", True)
        self.priority: int = opts.get("priority", 5)

        # Data collection
        self.tags: dict[str, Any] = dict(opts.get("tags") or {})
        self.logs: list[SpanLog] = []
        self.error: Exception | None = None

        # Timing
        self.start_time: float = 0.0
        self.finish_time: float = 0.0
        self.duration: float = 0.0

        # State
        self._started: bool = False
        self._finished: bool = False

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        status = "finished" if self._finished else "active" if self._started else "created"
        return (
            f"Span(name={self.name!r}, id={self.id[:8]}..., "
            f"status={status}, duration={self.duration:.4f}s)"
        )

    def start(self, time_: float | None = None) -> Span:
        """Start timing the span.

        Args:
            time_: Optional start timestamp (defaults to now)

        Returns:
            Self for chaining
        """
        if self._started:
            return self

        self.start_time = time_ if time_ is not None else time.time()
        self._started = True

        # Notify tracer
        self.tracer.span_started(self)

        return self

    def finish(self, time_: float | None = None) -> Span:
        """Finish timing the span.

        Args:
            time_: Optional finish timestamp (defaults to now)

        Returns:
            Self for chaining
        """
        if self._finished:
            return self

        self.finish_time = time_ if time_ is not None else time.time()
        self.duration = self.finish_time - self.start_time
        self._finished = True

        # Notify tracer
        self.tracer.span_finished(self)

        return self

    def add_tags(self, tags: dict[str, Any]) -> Span:
        """Add metadata tags to the span.

        Args:
            tags: Key-value pairs to merge into span tags

        Returns:
            Self for chaining
        """
        self.tags.update(tags)
        return self

    def log(
        self,
        name: str,
        fields: dict[str, Any] | None = None,
        time_: float | None = None,
    ) -> Span:
        """Add a log event to the span.

        Args:
            name: Event name (e.g., "cache.hit")
            fields: Additional event data
            time_: Optional timestamp (defaults to now)

        Returns:
            Self for chaining
        """
        now = time_ if time_ is not None else time.time()
        elapsed = now - self.start_time if self._started else 0.0

        self.logs.append(
            SpanLog(
                name=name,
                fields=fields or {},
                time=now,
                elapsed=elapsed,
            )
        )
        return self

    def set_error(self, error: Exception) -> Span:
        """Mark the span as failed with an error.

        Args:
            error: The exception that occurred

        Returns:
            Self for chaining
        """
        self.error = error
        return self

    def is_active(self) -> bool:
        """Check if span is still active (started but not finished).

        Returns:
            True if span is active
        """
        return self._started and not self._finished

    def start_span(self, name: str, opts: dict[str, Any] | None = None) -> Span:
        """Create a child span.

        The child span inherits trace_id and uses this span's id as parent_id.

        Args:
            name: Child span name
            opts: Additional options (merged with inherited values)

        Returns:
            New child Span
        """
        child_opts = {
            "trace_id": self.trace_id,
            "parent_id": self.id,
            "sampled": self.sampled,
            "service": self.service,
            **(opts or {}),
        }
        return self.tracer.start_span(name, child_opts)

    def to_dict(self) -> dict[str, Any]:
        """Convert span to dictionary for export.

        Returns:
            Dictionary representation of the span
        """
        return {
            "id": self.id,
            "traceID": self.trace_id,
            "parentID": self.parent_id,
            "name": self.name,
            "type": self.type,
            "service": self.service,
            "sampled": self.sampled,
            "priority": self.priority,
            "tags": self.tags,
            "logs": [
                {
                    "name": log.name,
                    "fields": log.fields,
                    "time": log.time,
                    "elapsed": log.elapsed,
                }
                for log in self.logs
            ],
            "error": self._error_to_dict(self.error) if self.error else None,
            "startTime": self.start_time,
            "finishTime": self.finish_time,
            "duration": self.duration,
        }

    def _error_to_dict(self, error: Exception) -> dict[str, Any]:
        """Convert exception to dictionary.

        Args:
            error: Exception to convert

        Returns:
            Dictionary with error details
        """
        return {
            "name": type(error).__name__,
            "message": str(error),
            "code": getattr(error, "code", None),
        }


# =============================================================================
# Tracer Class
# =============================================================================


@dataclass(slots=True)
class TracerOptions:
    """Configuration options for Tracer.

    Attributes:
        enabled: Enable/disable tracing
        exporter: List of exporters (or single exporter)
        sampling_rate: Probabilistic sampling rate (0.0-1.0)
        sampling_traces_per_second: Rate-limiting (max traces/sec)
        sampling_min_priority: Minimum priority to sample
        actions: Trace action calls
        events: Trace event handlers
        error_fields: Error fields to capture
        stack_trace: Include stack traces in errors
        default_tags: Default tags for all spans
    """

    enabled: bool = True
    exporter: list[BaseTraceExporter] = field(default_factory=list)
    sampling_rate: float = 1.0
    sampling_traces_per_second: float | None = None
    sampling_min_priority: int | None = None
    actions: bool = True
    events: bool = False
    error_fields: tuple[str, ...] = ("name", "message", "code")
    stack_trace: bool = False
    default_tags: dict[str, Any] | Callable[[], dict[str, Any]] | None = None


class Tracer:
    """Central tracing orchestrator.

    Manages span lifecycle, sampling decisions, and exporter coordination.

    Attributes:
        broker: Reference to ServiceBroker
        opts: Tracer configuration
        exporters: List of active exporters
        logger: Logger instance

    Example:
        tracer = Tracer(broker, TracerOptions(
            enabled=True,
            exporter=[ConsoleExporter()],
            sampling_rate=0.5,
        ))

        span = tracer.start_span("my-operation")
        span.add_tags({"key": "value"})
        span.finish()
    """

    __slots__ = (
        "_lock",
        "_rate_limiter_balance",
        "_rate_limiter_last_time",
        "_sample_counter",
        "broker",
        "exporters",
        "logger",
        "opts",
    )

    def __init__(
        self,
        broker: ServiceBroker,
        opts: TracerOptions | dict[str, Any] | None = None,
    ) -> None:
        """Initialize the tracer.

        Args:
            broker: ServiceBroker instance
            opts: Tracer options (TracerOptions or dict)
        """
        self.broker = broker

        # Parse options
        if opts is None:
            self.opts = TracerOptions()
        elif isinstance(opts, dict):
            self.opts = TracerOptions(**opts)
        else:
            self.opts = opts

        self.exporters: list[BaseTraceExporter] = []
        self.logger = broker.logger if hasattr(broker, "logger") else None

        # Sampling state
        self._lock = threading.Lock()
        self._sample_counter: float = 0.0
        self._rate_limiter_balance: float = 1.0
        self._rate_limiter_last_time: float = time.time()

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return (
            f"Tracer(enabled={self.opts.enabled}, "
            f"exporters={len(self.exporters)}, "
            f"sampling_rate={self.opts.sampling_rate})"
        )

    def init(self) -> None:
        """Initialize the tracer and exporters.

        Called during broker startup.
        """
        if not self.opts.enabled:
            return

        # Initialize exporters
        exporter_config = self.opts.exporter
        if not isinstance(exporter_config, list):
            exporter_config = [exporter_config] if exporter_config else []

        for exp in exporter_config:
            if exp is not None:
                exp.init(self)
                self.exporters.append(exp)

        if self.logger:
            self.logger.info(f"Tracer initialized with {len(self.exporters)} exporter(s)")

    def stop(self) -> None:
        """Stop the tracer and cleanup exporters.

        Called during broker shutdown.
        """
        for exp in self.exporters:
            exp.stop()

        self.exporters.clear()

        if self.logger:
            self.logger.info("Tracer stopped")

    def is_enabled(self) -> bool:
        """Check if tracing is enabled.

        Returns:
            True if tracing is enabled
        """
        return self.opts.enabled

    def should_sample(self, span: Span) -> bool:
        """Determine if a span should be sampled.

        Checks priority, rate-limiting, and probabilistic sampling.

        Args:
            span: The span to check

        Returns:
            True if span should be exported
        """
        if not self.opts.enabled:
            return False

        # Priority check
        if self.opts.sampling_min_priority is not None:
            if span.priority < self.opts.sampling_min_priority:
                return False

        # Rate-limiting check
        if self.opts.sampling_traces_per_second is not None:
            if not self._rate_limiter_check():
                return False

        # Probabilistic sampling
        if self.opts.sampling_rate < 1.0:
            return self._probabilistic_sample()

        return True

    def _probabilistic_sample(self) -> bool:
        """Perform probabilistic sampling check.

        Returns:
            True if this sample should be included
        """
        with self._lock:
            self._sample_counter += self.opts.sampling_rate
            if self._sample_counter >= 1.0:
                self._sample_counter -= 1.0
                return True
            return False

    def _rate_limiter_check(self, cost: float = 1.0) -> bool:
        """Check rate limiter (token bucket algorithm).

        Args:
            cost: Cost of this operation

        Returns:
            True if within rate limit
        """
        if self.opts.sampling_traces_per_second is None:
            return True

        with self._lock:
            now = time.time()
            elapsed = now - self._rate_limiter_last_time
            self._rate_limiter_last_time = now

            # Refill tokens
            max_balance = max(1.0, self.opts.sampling_traces_per_second)
            self._rate_limiter_balance += elapsed * self.opts.sampling_traces_per_second
            self._rate_limiter_balance = min(self._rate_limiter_balance, max_balance)

            # Check if we can afford the cost
            if self._rate_limiter_balance >= cost:
                self._rate_limiter_balance -= cost
                return True
            return False

    def start_span(self, name: str, opts: dict[str, Any] | None = None) -> Span:
        """Create and start a new span.

        Args:
            name: Span name
            opts: Span options

        Returns:
            Started Span instance
        """
        opts = opts or {}

        # Apply default tags
        if self.opts.default_tags:
            if callable(self.opts.default_tags):
                default_tags = self.opts.default_tags()
            else:
                default_tags = self.opts.default_tags

            opts["tags"] = {**default_tags, **(opts.get("tags") or {})}

        span = Span(self, name, opts)

        # Determine sampling
        if "sampled" not in opts:
            span.sampled = self.should_sample(span)

        span.start()
        return span

    def span_started(self, span: Span) -> None:
        """Called when a span starts.

        Notifies exporters that may want to track active spans.

        Args:
            span: The started span
        """
        if not span.sampled:
            return

        for exp in self.exporters:
            try:
                exp.span_started(span)
            except Exception as e:
                if self.logger:
                    self.logger.warn(f"Exporter error in span_started: {e}")

    def span_finished(self, span: Span) -> None:
        """Called when a span finishes.

        Sends span data to all exporters.

        Args:
            span: The finished span
        """
        if not span.sampled:
            return

        for exp in self.exporters:
            try:
                exp.span_finished(span)
            except Exception as e:
                if self.logger:
                    self.logger.warn(f"Exporter error in span_finished: {e}")


# =============================================================================
# Exporter Base Class
# =============================================================================


class BaseTraceExporter(ABC):
    """Abstract base class for trace exporters.

    Exporters receive finished spans and send them to backends
    (console, Jaeger, Zipkin, etc.).

    Subclasses must implement span_finished().

    Example:
        class MyExporter(BaseTraceExporter):
            def span_finished(self, span: Span) -> None:
                data = span.to_dict()
                self.send_to_backend(data)
    """

    __slots__ = ("broker", "logger", "opts", "tracer")

    def __init__(self, opts: dict[str, Any] | None = None) -> None:
        """Initialize exporter.

        Args:
            opts: Exporter-specific options
        """
        self.tracer: Tracer | None = None
        self.broker: ServiceBroker | None = None
        self.logger: Any = None
        self.opts: dict[str, Any] = opts or {}

    def init(self, tracer: Tracer) -> None:
        """Initialize exporter with tracer reference.

        Args:
            tracer: The Tracer instance
        """
        self.tracer = tracer
        self.broker = tracer.broker
        self.logger = tracer.logger

    def stop(self) -> None:
        """Cleanup exporter resources.

        Override in subclass if needed.
        """
        return None

    def span_started(self, span: Span) -> None:
        """Called when a span starts.

        Override in subclass if needed for tracking active spans.

        Args:
            span: The started span
        """
        return None

    @abstractmethod
    def span_finished(self, span: Span) -> None:
        """Called when a span finishes.

        Must be implemented by subclasses.

        Args:
            span: The finished span
        """
        ...

    def flatten_tags(
        self,
        obj: dict[str, Any],
        prefix: str = "",
        convert_to_string: bool = False,
    ) -> dict[str, Any]:
        """Flatten nested tags for export.

        Converts {"error": {"name": "X"}} to {"error.name": "X"}.

        Args:
            obj: Nested dictionary
            prefix: Key prefix
            convert_to_string: Convert all values to strings

        Returns:
            Flattened dictionary
        """
        result: dict[str, Any] = {}

        for key, value in obj.items():
            full_key = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict):
                result.update(self.flatten_tags(value, full_key, convert_to_string))
            elif convert_to_string:
                result[full_key] = str(value) if value is not None else ""
            else:
                result[full_key] = value

        return result

    def error_to_object(
        self,
        error: Exception,
        fields: tuple[str, ...] = ("name", "message", "code"),
    ) -> dict[str, Any]:
        """Extract error information to dictionary.

        Args:
            error: Exception to extract
            fields: Fields to include

        Returns:
            Error dictionary
        """
        result: dict[str, Any] = {}

        if "name" in fields:
            result["name"] = type(error).__name__
        if "message" in fields:
            result["message"] = str(error)
        if "code" in fields:
            result["code"] = getattr(error, "code", None)
        if "stack" in fields:
            result["stack"] = traceback.format_exc()

        return result


# =============================================================================
# Console Exporter
# =============================================================================


class ConsoleExporter(BaseTraceExporter):
    """Exporter that prints spans to console.

    Useful for debugging and development.

    Options:
        colors: Enable ANSI colors (default: True)
        width: Output width (default: 80)
        gauge_width: Timeline gauge width (default: 40)

    Example:
        tracer = Tracer(broker, TracerOptions(
            exporter=[ConsoleExporter({"colors": True})]
        ))
    """

    __slots__ = ("_colors", "_gauge_width", "_width")
    _MICROSECONDS_PER_MILLISECOND = 1000
    _MILLISECONDS_PER_SECOND = 1000
    _MAX_NAME_LENGTH = 40
    _MAX_TAGS_PREVIEW = 3

    def __init__(self, opts: dict[str, Any] | None = None) -> None:
        super().__init__(opts)
        self._colors = self.opts.get("colors", True)
        self._width = self.opts.get("width", 80)
        self._gauge_width = self.opts.get("gauge_width", 40)

    def span_finished(self, span: Span) -> None:
        """Print span to console.

        Args:
            span: The finished span
        """
        self._print_span(span)

    def _print_span(self, span: Span, indent: int = 0) -> None:
        """Print a single span.

        Args:
            span: Span to print
            indent: Indentation level
        """
        prefix = "  " * indent

        # Format duration
        duration_ms = span.duration * self._MILLISECONDS_PER_SECOND
        if duration_ms < 1:
            duration_str = f"{duration_ms * self._MICROSECONDS_PER_MILLISECOND:.1f}μs"
        elif duration_ms < self._MILLISECONDS_PER_SECOND:
            duration_str = f"{duration_ms:.2f}ms"
        else:
            duration_str = f"{duration_ms / self._MILLISECONDS_PER_SECOND:.2f}s"

        # Color codes
        if self._colors:
            reset = "\033[0m"
            if span.error:
                color = "\033[31m"  # Red
            elif span.type == "action":
                color = "\033[36m"  # Cyan
            elif span.type == "event":
                color = "\033[33m"  # Yellow
            else:
                color = "\033[37m"  # White
        else:
            reset = color = ""

        # Print span line
        name = span.name
        if len(name) > self._MAX_NAME_LENGTH:
            name = name[:37] + "..."

        line = f"{prefix}{color}{name}{reset} ({duration_str})"
        if span.error:
            line += f" [ERROR: {type(span.error).__name__}]"

        print(line)

        # Print tags summary
        if span.tags:
            tags_str = ", ".join(
                f"{k}={v}" for k, v in list(span.tags.items())[: self._MAX_TAGS_PREVIEW]
            )
            if len(span.tags) > self._MAX_TAGS_PREVIEW:
                tags_str += f", ... (+{len(span.tags) - self._MAX_TAGS_PREVIEW})"
            print(f"{prefix}  tags: {tags_str}")


# =============================================================================
# Event Exporter
# =============================================================================


class EventExporter(BaseTraceExporter):
    """Exporter that emits spans as Moleculer events.

    Useful for centralized trace collection within the cluster.

    Options:
        event_name: Event name to emit (default: "$tracing.spans")
        send_finished_span: Emit immediately on finish (default: True)
        batch_size: Max batch size when send_finished_span=False (default: 100)

    Note:
        Batch mode (send_finished_span=False) only flushes when batch_size
        is reached or on shutdown. Time-based flushing is not yet implemented.

    Example:
        tracer = Tracer(broker, TracerOptions(
            exporter=[EventExporter({
                "event_name": "$tracing.spans",
                "send_finished_span": True  # Recommended
            })]
        ))
    """

    __slots__ = (
        "_batch_size",
        "_batch_time",
        "_event_name",
        "_lock",
        "_queue",
        "_send_immediately",
    )

    def __init__(self, opts: dict[str, Any] | None = None) -> None:
        super().__init__(opts)
        self._event_name = self.opts.get("event_name", "$tracing.spans")
        self._send_immediately = self.opts.get("send_finished_span", True)
        self._batch_time = self.opts.get("batch_time", 5.0)
        self._batch_size = self.opts.get("batch_size", 100)
        self._queue: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def span_finished(self, span: Span) -> None:
        """Queue or emit span.

        Args:
            span: The finished span
        """
        span_data = span.to_dict()

        if self._send_immediately:
            self._emit_spans([span_data])
        else:
            with self._lock:
                self._queue.append(span_data)
                if len(self._queue) >= self._batch_size:
                    self._flush()

    def _flush(self) -> None:
        """Flush queued spans."""
        with self._lock:
            if not self._queue:
                return
            spans = self._queue.copy()
            self._queue.clear()

        self._emit_spans(spans)

    def _emit_spans(self, spans: list[dict[str, Any]]) -> None:
        """Emit spans as event.

        Args:
            spans: List of span dictionaries
        """
        if not self.broker:
            return

        try:
            # Use broadcast to reach all trace collectors
            try:
                loop = asyncio.get_running_loop()
                # Schedule task with error handling callback
                task = loop.create_task(self.broker.broadcast(self._event_name, {"spans": spans}))
                # Add done callback to handle errors without losing them
                task.add_done_callback(self._handle_emit_result)
            except RuntimeError:
                # No running loop - this shouldn't happen in normal operation
                # but handle gracefully
                if self.logger:
                    self.logger.warn("No running event loop for span emission")
        except (OSError, RuntimeError) as e:
            if self.logger:
                self.logger.warn(f"Failed to emit trace spans: {e}")

    def _handle_emit_result(self, task: Any) -> None:
        """Handle result of emit task.

        Logs any exceptions that occurred during span emission.

        Args:
            task: The completed asyncio Task
        """
        try:
            # Check if task raised an exception
            exc = task.exception()
            if exc and self.logger:
                self.logger.warn(f"Span emission failed: {exc}")
        except asyncio.CancelledError:
            # Task was cancelled, this is fine
            pass
        except asyncio.InvalidStateError:
            # Task not done yet (shouldn't happen in done callback)
            pass

    def stop(self) -> None:
        """Flush remaining spans on shutdown."""
        self._flush()


# =============================================================================
# Exports
# =============================================================================

__all__ = [  # noqa: RUF022
    # Core
    "Span",
    "SpanLog",
    "Tracer",
    "TracerOptions",
    # Exporters
    "BaseTraceExporter",
    "ConsoleExporter",
    "EventExporter",
]
