"""Metrics middleware for MoleculerPy framework.

Provides Prometheus-compatible application metrics collection:
- Request counts (total, errors)
- Request latencies (histogram)
- Active requests (gauge)
- Event counts

Based on Moleculer.js metrics module design patterns.

Usage:
    from moleculerpy.middleware import MetricsMiddleware

    metrics = MetricsMiddleware()
    settings = Settings(middlewares=[metrics])
    broker = ServiceBroker("my-node", settings=settings)

    # Get Prometheus metrics
    prometheus_output = metrics.registry.to_prometheus()
"""

from __future__ import annotations

import asyncio
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

from moleculerpy.middleware.base import Middleware


def _escape_label_value(value: str) -> str:
    """Escape label value for Prometheus text format.

    Prometheus requires escaping of backslash, double quote, and newline
    in label values.

    Args:
        value: Raw label value

    Returns:
        Escaped label value safe for Prometheus format
    """
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


T = TypeVar("T")
HandlerType = Callable[[Any], Awaitable[T]]

# Default histogram buckets (Prometheus standard for HTTP latencies)
DEFAULT_BUCKETS: tuple[float, ...] = (
    0.001,  # 1ms
    0.005,  # 5ms
    0.01,  # 10ms
    0.025,  # 25ms
    0.05,  # 50ms
    0.1,  # 100ms
    0.25,  # 250ms
    0.5,  # 500ms
    1.0,  # 1s
    2.5,  # 2.5s
    5.0,  # 5s
    10.0,  # 10s
)


# =============================================================================
# Metric Base Class
# =============================================================================


class BaseMetric(ABC):
    """Base class for all metric types.

    Attributes:
        name: Metric name (prometheus format: lowercase, underscores)
        description: Human-readable description
        label_names: Tuple of label names for this metric
    """

    __slots__ = ("_label_names_set", "_lock", "description", "label_names", "name")

    def __init__(
        self,
        name: str,
        description: str,
        label_names: tuple[str, ...] | None = None,
    ) -> None:
        self.name = name
        self.description = description
        self.label_names = label_names or ()
        self._label_names_set = frozenset(self.label_names)  # Cache for O(1) lookups
        self._lock = threading.Lock()

    def _validate_labels(self, labels: dict[str, str] | None) -> tuple[str, ...]:
        """Validate and convert labels dict to tuple of values.

        Args:
            labels: Dictionary of label name -> value

        Returns:
            Tuple of label values in order of label_names

        Raises:
            ValueError: If labels don't match label_names
        """
        if not self.label_names:
            if labels:
                raise ValueError(f"Metric {self.name} has no labels, but got {labels}")
            return ()

        if not labels:
            raise ValueError(f"Metric {self.name} requires labels: {self.label_names}")

        # Use cached frozenset for O(1) subset check
        if not self._label_names_set <= labels.keys():
            missing = self._label_names_set - labels.keys()
            raise ValueError(f"Missing labels for {self.name}: {missing}")

        return tuple(labels[name] for name in self.label_names)

    @abstractmethod
    def _to_prometheus_lines(self) -> list[str]:
        """Generate Prometheus text format lines."""
        ...


# =============================================================================
# Counter
# =============================================================================


class Counter(BaseMetric):
    """Monotonically increasing counter metric.

    Use for: request counts, error counts, events processed.

    Example:
        counter = registry.counter(
            "requests_total",
            "Total requests",
            label_names=("action", "status"),
        )
        counter.inc(labels={"action": "users.get", "status": "success"})
    """

    __slots__ = ("_values",)

    def __init__(
        self,
        name: str,
        description: str,
        label_names: tuple[str, ...] | None = None,
    ) -> None:
        super().__init__(name, description, label_names)
        self._values: dict[tuple[str, ...], float] = {}

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return f"Counter(name={self.name!r}, labels={self.label_names}, series={len(self._values)})"

    def inc(
        self,
        value: float = 1.0,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Increment counter by value.

        Args:
            value: Amount to increment (must be positive)
            labels: Label values as dict
        """
        if value < 0:
            raise ValueError("Counter can only be incremented")

        key = self._validate_labels(labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + value

    def get(self, labels: dict[str, str] | None = None) -> float:
        """Get current counter value.

        Args:
            labels: Label values as dict

        Returns:
            Current counter value
        """
        key = self._validate_labels(labels)
        with self._lock:
            return self._values.get(key, 0.0)

    def _to_prometheus_lines(self) -> list[str]:
        """Generate Prometheus text format."""
        lines = [
            f"# HELP {self.name} {self.description}",
            f"# TYPE {self.name} counter",
        ]

        with self._lock:
            for key, value in sorted(self._values.items()):
                if self.label_names:
                    label_pairs = [
                        f'{name}="{_escape_label_value(val)}"'
                        for name, val in zip(self.label_names, key, strict=False)
                    ]
                    label_str = "{" + ",".join(label_pairs) + "}"
                    lines.append(f"{self.name}{label_str} {value}")
                else:
                    lines.append(f"{self.name} {value}")

        return lines


# =============================================================================
# Gauge
# =============================================================================


class Gauge(BaseMetric):
    """Point-in-time value metric that can go up or down.

    Use for: active requests, queue size, memory usage.

    Example:
        gauge = registry.gauge(
            "active_requests",
            "Currently active requests",
            label_names=("action",),
        )
        gauge.inc(labels={"action": "users.get"})  # Request started
        gauge.dec(labels={"action": "users.get"})  # Request finished
    """

    __slots__ = ("_values",)

    def __init__(
        self,
        name: str,
        description: str,
        label_names: tuple[str, ...] | None = None,
    ) -> None:
        super().__init__(name, description, label_names)
        self._values: dict[tuple[str, ...], float] = {}

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return f"Gauge(name={self.name!r}, labels={self.label_names}, series={len(self._values)})"

    def set(
        self,
        value: float,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Set gauge to absolute value.

        Args:
            value: Value to set
            labels: Label values as dict
        """
        key = self._validate_labels(labels)
        with self._lock:
            self._values[key] = value

    def inc(
        self,
        value: float = 1.0,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Increment gauge by value.

        Args:
            value: Amount to increment
            labels: Label values as dict
        """
        key = self._validate_labels(labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + value

    def dec(
        self,
        value: float = 1.0,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Decrement gauge by value.

        Args:
            value: Amount to decrement
            labels: Label values as dict
        """
        key = self._validate_labels(labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) - value

    def get(self, labels: dict[str, str] | None = None) -> float:
        """Get current gauge value.

        Args:
            labels: Label values as dict

        Returns:
            Current gauge value
        """
        key = self._validate_labels(labels)
        with self._lock:
            return self._values.get(key, 0.0)

    def _to_prometheus_lines(self) -> list[str]:
        """Generate Prometheus text format."""
        lines = [
            f"# HELP {self.name} {self.description}",
            f"# TYPE {self.name} gauge",
        ]

        with self._lock:
            for key, value in sorted(self._values.items()):
                if self.label_names:
                    label_pairs = [
                        f'{name}="{_escape_label_value(val)}"'
                        for name, val in zip(self.label_names, key, strict=False)
                    ]
                    label_str = "{" + ",".join(label_pairs) + "}"
                    lines.append(f"{self.name}{label_str} {value}")
                else:
                    lines.append(f"{self.name} {value}")

        return lines


# =============================================================================
# Histogram
# =============================================================================


@dataclass(slots=True)
class HistogramData:
    """Internal data for a single histogram series.

    Attributes:
        buckets: Dict of bucket upper bound -> count
        sum: Sum of all observed values
        count: Total number of observations
    """

    buckets: dict[float, int] = field(default_factory=dict)
    sum: float = 0.0
    count: int = 0


class Histogram(BaseMetric):
    """Distribution metric with configurable buckets.

    Use for: request latencies, response sizes.

    Example:
        histogram = registry.histogram(
            "request_duration_seconds",
            "Request latency",
            label_names=("action",),
            buckets=(0.01, 0.05, 0.1, 0.5, 1.0),
        )
        histogram.observe(0.042, labels={"action": "users.get"})
    """

    __slots__ = ("_buckets", "_data")

    def __init__(
        self,
        name: str,
        description: str,
        label_names: tuple[str, ...] | None = None,
        buckets: tuple[float, ...] | None = None,
    ) -> None:
        super().__init__(name, description, label_names)
        self._buckets = tuple(sorted(buckets or DEFAULT_BUCKETS))
        self._data: dict[tuple[str, ...], HistogramData] = {}

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return (
            f"Histogram(name={self.name!r}, labels={self.label_names}, "
            f"buckets={len(self._buckets)}, series={len(self._data)})"
        )

    def observe(
        self,
        value: float,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Observe a value (add to histogram).

        Args:
            value: Value to observe (e.g., latency in seconds)
            labels: Label values as dict
        """
        key = self._validate_labels(labels)

        with self._lock:
            if key not in self._data:
                self._data[key] = HistogramData(buckets={b: 0 for b in self._buckets})

            data = self._data[key]
            data.count += 1
            data.sum += value

            # Increment all buckets where value <= bucket boundary
            for bucket in self._buckets:
                if value <= bucket:
                    data.buckets[bucket] += 1

    def get_count(self, labels: dict[str, str] | None = None) -> int:
        """Get total observation count.

        Args:
            labels: Label values as dict

        Returns:
            Number of observations
        """
        key = self._validate_labels(labels)
        with self._lock:
            data = self._data.get(key)
            return data.count if data else 0

    def get_sum(self, labels: dict[str, str] | None = None) -> float:
        """Get sum of all observations.

        Args:
            labels: Label values as dict

        Returns:
            Sum of observed values
        """
        key = self._validate_labels(labels)
        with self._lock:
            data = self._data.get(key)
            return data.sum if data else 0.0

    def _to_prometheus_lines(self) -> list[str]:
        """Generate Prometheus text format."""
        lines = [
            f"# HELP {self.name} {self.description}",
            f"# TYPE {self.name} histogram",
        ]

        with self._lock:
            for key, data in sorted(self._data.items()):
                # Build base label string
                if self.label_names:
                    base_labels = [
                        f'{name}="{_escape_label_value(val)}"'
                        for name, val in zip(self.label_names, key, strict=False)
                    ]
                else:
                    base_labels = []

                # Bucket lines
                for bucket, count in sorted(data.buckets.items()):
                    bucket_labels = [*base_labels, f'le="{bucket}"']
                    label_str = "{" + ",".join(bucket_labels) + "}"
                    lines.append(f"{self.name}_bucket{label_str} {count}")

                # +Inf bucket (always equals _count)
                inf_labels = [*base_labels, 'le="+Inf"']
                label_str = "{" + ",".join(inf_labels) + "}"
                lines.append(f"{self.name}_bucket{label_str} {data.count}")

                # Sum and count
                if base_labels:
                    label_str = "{" + ",".join(base_labels) + "}"
                    lines.append(f"{self.name}_sum{label_str} {data.sum}")
                    lines.append(f"{self.name}_count{label_str} {data.count}")
                else:
                    lines.append(f"{self.name}_sum {data.sum}")
                    lines.append(f"{self.name}_count {data.count}")

        return lines


# =============================================================================
# Metric Registry
# =============================================================================


class MetricRegistry:
    """Central registry for all application metrics.

    Provides factory methods for creating metrics and Prometheus export.

    Example:
        registry = MetricRegistry()

        requests = registry.counter(
            "requests_total",
            "Total requests",
            label_names=("action", "status"),
        )
        latency = registry.histogram(
            "request_duration_seconds",
            "Request latency",
            label_names=("action",),
        )

        # Export to Prometheus format
        print(registry.to_prometheus())
    """

    __slots__ = ("_lock", "_metrics")

    def __init__(self) -> None:
        self._metrics: dict[str, BaseMetric] = {}
        self._lock = threading.Lock()

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return f"MetricRegistry(metrics={len(self._metrics)})"

    def counter(
        self,
        name: str,
        description: str,
        label_names: tuple[str, ...] | None = None,
    ) -> Counter:
        """Create or get a counter metric.

        Args:
            name: Metric name (prometheus format)
            description: Human-readable description
            label_names: Tuple of label names

        Returns:
            Counter instance
        """
        with self._lock:
            if name in self._metrics:
                metric = self._metrics[name]
                if not isinstance(metric, Counter):
                    raise TypeError(f"Metric {name} exists but is not a Counter")
                return metric

            counter = Counter(name, description, label_names)
            self._metrics[name] = counter
            return counter

    def gauge(
        self,
        name: str,
        description: str,
        label_names: tuple[str, ...] | None = None,
    ) -> Gauge:
        """Create or get a gauge metric.

        Args:
            name: Metric name (prometheus format)
            description: Human-readable description
            label_names: Tuple of label names

        Returns:
            Gauge instance
        """
        with self._lock:
            if name in self._metrics:
                metric = self._metrics[name]
                if not isinstance(metric, Gauge):
                    raise TypeError(f"Metric {name} exists but is not a Gauge")
                return metric

            gauge = Gauge(name, description, label_names)
            self._metrics[name] = gauge
            return gauge

    def histogram(
        self,
        name: str,
        description: str,
        label_names: tuple[str, ...] | None = None,
        buckets: tuple[float, ...] | None = None,
    ) -> Histogram:
        """Create or get a histogram metric.

        Args:
            name: Metric name (prometheus format)
            description: Human-readable description
            label_names: Tuple of label names
            buckets: Custom bucket boundaries

        Returns:
            Histogram instance
        """
        with self._lock:
            if name in self._metrics:
                metric = self._metrics[name]
                if not isinstance(metric, Histogram):
                    raise TypeError(f"Metric {name} exists but is not a Histogram")
                return metric

            histogram = Histogram(name, description, label_names, buckets)
            self._metrics[name] = histogram
            return histogram

    def to_prometheus(self) -> str:
        """Export all metrics in Prometheus text format.

        Returns:
            Multi-line string in Prometheus exposition format
        """
        lines: list[str] = []

        with self._lock:
            for name in sorted(self._metrics.keys()):
                metric = self._metrics[name]
                lines.extend(metric._to_prometheus_lines())
                lines.append("")  # Blank line between metrics

        return "\n".join(lines)

    def clear(self) -> None:
        """Clear all metrics (useful for testing)."""
        with self._lock:
            self._metrics.clear()


# =============================================================================
# Metrics Middleware
# =============================================================================


class MetricsMiddleware(Middleware):
    """Middleware for collecting request/event metrics.

    Automatically tracks:
    - moleculer_request_total: Counter of requests by action/service/status
    - moleculer_request_active: Gauge of currently active requests
    - moleculer_request_duration_seconds: Histogram of request latencies
    - moleculer_event_total: Counter of events by name/group

    Usage:
        from moleculerpy.middleware import MetricsMiddleware

        metrics_middleware = MetricsMiddleware()
        settings = Settings(middlewares=[metrics_middleware])

        # Later: get Prometheus metrics
        print(metrics_middleware.registry.to_prometheus())

        # Or create custom metrics
        my_counter = metrics_middleware.registry.counter(
            "my_custom_counter",
            "My custom counter",
        )
        my_counter.inc()
    """

    def __init__(
        self,
        registry: MetricRegistry | None = None,
        buckets: tuple[float, ...] | None = None,
    ) -> None:
        """Initialize metrics middleware.

        Args:
            registry: Custom metric registry (creates new one if None)
            buckets: Custom histogram buckets for latency
        """
        self.registry = registry or MetricRegistry()

        # Pre-register standard metrics
        self._request_total = self.registry.counter(
            "moleculer_request_total",
            "Total number of requests",
            label_names=("action", "service", "caller", "status"),
        )

        self._request_active = self.registry.gauge(
            "moleculer_request_active",
            "Number of currently active requests",
            label_names=("action", "service"),
        )

        self._request_duration = self.registry.histogram(
            "moleculer_request_duration_seconds",
            "Request latency in seconds",
            label_names=("action", "service"),
            buckets=buckets,
        )

        self._event_total = self.registry.counter(
            "moleculer_event_total",
            "Total number of events processed",
            label_names=("event", "group"),
        )

        self._event_emitted_total = self.registry.counter(
            "moleculer_event_emitted_total",
            "Total number of events emitted via emit()",
            label_names=("event",),
        )

        self._event_broadcast_total = self.registry.counter(
            "moleculer_event_broadcast_total",
            "Total number of events broadcast via broadcast()",
            label_names=("event",),
        )

    async def local_action(self, next_handler: HandlerType[Any], action: Any) -> HandlerType[Any]:
        """Wrap local action with metrics collection.

        Args:
            next_handler: The next handler in the chain
            action: The action object

        Returns:
            Wrapped handler with metrics
        """
        action_name = getattr(action, "name", "unknown")
        service_name = getattr(action, "service", "unknown")

        async def handler(ctx: Any) -> Any:
            caller = getattr(ctx, "caller", "") or "unknown"
            labels = {
                "action": action_name,
                "service": service_name,
            }

            # Increment active requests
            self._request_active.inc(labels=labels)

            start_time = time.perf_counter()
            status = "success"

            try:
                result = await next_handler(ctx)
                return result
            except asyncio.CancelledError:
                # Handle cancellation separately (BaseException, not Exception)
                status = "cancelled"
                raise
            except Exception:
                status = "error"
                raise
            finally:
                # Record latency
                duration = time.perf_counter() - start_time
                self._request_duration.observe(duration, labels=labels)

                # Decrement active requests
                self._request_active.dec(labels=labels)

                # Increment request counter
                self._request_total.inc(
                    labels={
                        "action": action_name,
                        "service": service_name,
                        "caller": caller,
                        "status": status,
                    }
                )

        return handler

    async def remote_action(self, next_handler: HandlerType[Any], action: Any) -> HandlerType[Any]:
        """Wrap remote action with metrics collection.

        Uses same metrics as local_action for consistent tracking.

        Args:
            next_handler: The next handler in the chain
            action: The action object

        Returns:
            Wrapped handler with metrics
        """
        # Reuse local_action logic
        return await self.local_action(next_handler, action)

    async def local_event(self, next_handler: HandlerType[Any], event: Any) -> HandlerType[Any]:
        """Wrap event handler with metrics collection.

        Args:
            next_handler: The next handler in the chain
            event: The event object

        Returns:
            Wrapped handler with metrics
        """
        event_name = getattr(event, "name", "unknown")
        event_group = getattr(event, "group", "") or "default"

        async def handler(ctx: Any) -> Any:
            try:
                result = await next_handler(ctx)
                return result
            finally:
                # Always count event (even on error)
                self._event_total.inc(
                    labels={
                        "event": event_name,
                        "group": event_group,
                    }
                )

        return handler

    async def emit(
        self, next_emit: Any, event_name: str, params: dict[str, Any], opts: dict[str, Any]
    ) -> Any:
        """Wrap emit() with metrics collection.

        Args:
            next_emit: Next emit in the chain
            event_name: Event name
            params: Event parameters
            opts: Emit options

        Returns:
            Result from emit
        """
        try:
            return await next_emit(event_name, params, opts)
        finally:
            # Count emitted event
            self._event_emitted_total.inc(labels={"event": event_name})

    async def broadcast(
        self, next_broadcast: Any, event_name: str, params: dict[str, Any], opts: dict[str, Any]
    ) -> Any:
        """Wrap broadcast() with metrics collection.

        Args:
            next_broadcast: Next broadcast in the chain
            event_name: Event name
            params: Event parameters
            opts: Broadcast options

        Returns:
            List of results from handlers
        """
        try:
            return await next_broadcast(event_name, params, opts)
        finally:
            # Count broadcast event
            self._event_broadcast_total.inc(labels={"event": event_name})


# =============================================================================
# Timer Context Manager
# =============================================================================


class Timer:
    """Context manager for timing code blocks.

    Supports both sync and async context managers.

    Example:
        histogram = registry.histogram("operation_seconds", "Operation time")

        # Sync usage
        with Timer(histogram, labels={"operation": "process"}):
            do_work()

        # Async usage
        async with Timer(histogram, labels={"operation": "fetch"}):
            await async_work()

        # Or without labels
        with Timer(histogram):
            do_work()
    """

    __slots__ = ("_histogram", "_labels", "_start")

    def __init__(
        self,
        histogram: Histogram,
        labels: dict[str, str] | None = None,
    ) -> None:
        self._histogram = histogram
        self._labels = labels
        self._start: float = 0.0

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return f"Timer(histogram={self._histogram.name!r}, labels={self._labels})"

    # Sync context manager
    def __enter__(self) -> Timer:
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_: Any) -> None:
        duration = time.perf_counter() - self._start
        self._histogram.observe(duration, labels=self._labels)

    # Async context manager
    async def __aenter__(self) -> Timer:
        self._start = time.perf_counter()
        return self

    async def __aexit__(self, *_: Any) -> None:
        duration = time.perf_counter() - self._start
        self._histogram.observe(duration, labels=self._labels)
