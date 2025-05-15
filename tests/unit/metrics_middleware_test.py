"""Unit tests for the MetricsMiddleware and metric types."""

import asyncio
from unittest.mock import MagicMock

import pytest

from moleculerpy.middleware.metrics import (
    DEFAULT_BUCKETS,
    Counter,
    Gauge,
    Histogram,
    MetricRegistry,
    MetricsMiddleware,
    Timer,
)

# =============================================================================
# Counter Tests
# =============================================================================


class TestCounter:
    """Test Counter metric type."""

    def test_counter_without_labels(self):
        """Test basic counter without labels."""
        counter = Counter("test_counter", "Test counter")

        counter.inc()
        assert counter.get() == 1.0

        counter.inc(5)
        assert counter.get() == 6.0

    def test_counter_with_labels(self):
        """Test counter with labels."""
        counter = Counter(
            "test_counter",
            "Test counter",
            label_names=("action", "status"),
        )

        counter.inc(labels={"action": "get", "status": "success"})
        counter.inc(2, labels={"action": "get", "status": "error"})
        counter.inc(labels={"action": "get", "status": "success"})

        assert counter.get(labels={"action": "get", "status": "success"}) == 2.0
        assert counter.get(labels={"action": "get", "status": "error"}) == 2.0

    def test_counter_negative_increment_raises(self):
        """Test that negative increment raises ValueError."""
        counter = Counter("test_counter", "Test counter")

        with pytest.raises(ValueError, match="can only be incremented"):
            counter.inc(-1)

    def test_counter_missing_labels_raises(self):
        """Test that missing labels raise ValueError."""
        counter = Counter(
            "test_counter",
            "Test counter",
            label_names=("action",),
        )

        with pytest.raises(ValueError, match="requires labels"):
            counter.inc()

    def test_counter_extra_labels_ok(self):
        """Test that extra labels are ignored (only required labels used)."""
        counter = Counter(
            "test_counter",
            "Test counter",
            label_names=("action",),
        )

        # Extra labels are fine - we only validate required ones
        counter.inc(labels={"action": "get", "extra": "ignored"})
        assert counter.get(labels={"action": "get"}) == 1.0

    def test_counter_prometheus_format(self):
        """Test Prometheus text format output."""
        counter = Counter(
            "requests_total",
            "Total requests",
            label_names=("action",),
        )

        counter.inc(labels={"action": "get"})
        counter.inc(2, labels={"action": "post"})

        lines = counter._to_prometheus_lines()

        assert lines[0] == "# HELP requests_total Total requests"
        assert lines[1] == "# TYPE requests_total counter"
        assert 'requests_total{action="get"} 1.0' in lines
        assert 'requests_total{action="post"} 2.0' in lines

    def test_counter_prometheus_no_labels(self):
        """Test Prometheus format without labels."""
        counter = Counter("simple_counter", "Simple counter")
        counter.inc(5)

        lines = counter._to_prometheus_lines()

        assert "simple_counter 5.0" in lines


# =============================================================================
# Gauge Tests
# =============================================================================


class TestGauge:
    """Test Gauge metric type."""

    def test_gauge_set(self):
        """Test gauge set operation."""
        gauge = Gauge("test_gauge", "Test gauge")

        gauge.set(42)
        assert gauge.get() == 42.0

        gauge.set(0)
        assert gauge.get() == 0.0

    def test_gauge_inc_dec(self):
        """Test gauge increment and decrement."""
        gauge = Gauge("test_gauge", "Test gauge")

        gauge.inc()
        assert gauge.get() == 1.0

        gauge.inc(5)
        assert gauge.get() == 6.0

        gauge.dec()
        assert gauge.get() == 5.0

        gauge.dec(3)
        assert gauge.get() == 2.0

    def test_gauge_with_labels(self):
        """Test gauge with labels."""
        gauge = Gauge(
            "active_requests",
            "Active requests",
            label_names=("action",),
        )

        gauge.inc(labels={"action": "get"})
        gauge.inc(2, labels={"action": "post"})

        assert gauge.get(labels={"action": "get"}) == 1.0
        assert gauge.get(labels={"action": "post"}) == 2.0

    def test_gauge_prometheus_format(self):
        """Test Prometheus text format output."""
        gauge = Gauge(
            "active_connections",
            "Active connections",
            label_names=("service",),
        )

        gauge.set(10, labels={"service": "users"})
        gauge.set(5, labels={"service": "orders"})

        lines = gauge._to_prometheus_lines()

        assert lines[0] == "# HELP active_connections Active connections"
        assert lines[1] == "# TYPE active_connections gauge"


# =============================================================================
# Histogram Tests
# =============================================================================


class TestHistogram:
    """Test Histogram metric type."""

    def test_histogram_default_buckets(self):
        """Test histogram uses default buckets."""
        histogram = Histogram("test_histogram", "Test histogram")
        assert histogram._buckets == DEFAULT_BUCKETS

    def test_histogram_custom_buckets(self):
        """Test histogram with custom buckets."""
        buckets = (0.1, 0.5, 1.0, 5.0)
        histogram = Histogram(
            "test_histogram",
            "Test histogram",
            buckets=buckets,
        )
        assert histogram._buckets == buckets

    def test_histogram_observe(self):
        """Test histogram observe operation."""
        histogram = Histogram(
            "test_histogram",
            "Test histogram",
            buckets=(0.1, 0.5, 1.0),
        )

        histogram.observe(0.05)  # < 0.1
        histogram.observe(0.3)  # < 0.5
        histogram.observe(0.8)  # < 1.0
        histogram.observe(2.0)  # > 1.0 (only in +Inf)

        assert histogram.get_count() == 4
        assert histogram.get_sum() == pytest.approx(3.15, rel=1e-6)

    def test_histogram_with_labels(self):
        """Test histogram with labels."""
        histogram = Histogram(
            "request_duration",
            "Request duration",
            label_names=("action",),
            buckets=(0.1, 0.5, 1.0),
        )

        histogram.observe(0.2, labels={"action": "get"})
        histogram.observe(0.3, labels={"action": "get"})
        histogram.observe(0.8, labels={"action": "post"})

        assert histogram.get_count(labels={"action": "get"}) == 2
        assert histogram.get_sum(labels={"action": "get"}) == pytest.approx(0.5, rel=1e-6)
        assert histogram.get_count(labels={"action": "post"}) == 1

    def test_histogram_bucket_counting(self):
        """Test that buckets count correctly (cumulative)."""
        histogram = Histogram(
            "test_histogram",
            "Test histogram",
            buckets=(0.1, 0.5, 1.0),
        )

        # Observe values in different buckets
        histogram.observe(0.05)  # In 0.1, 0.5, 1.0
        histogram.observe(0.3)  # In 0.5, 1.0
        histogram.observe(0.8)  # In 1.0
        histogram.observe(2.0)  # Only in +Inf

        lines = histogram._to_prometheus_lines()
        content = "\n".join(lines)

        # Check cumulative bucket counts
        assert 'test_histogram_bucket{le="0.1"} 1' in content
        assert 'test_histogram_bucket{le="0.5"} 2' in content
        assert 'test_histogram_bucket{le="1.0"} 3' in content
        assert 'test_histogram_bucket{le="+Inf"} 4' in content

    def test_histogram_prometheus_format(self):
        """Test Prometheus text format output."""
        histogram = Histogram(
            "request_duration_seconds",
            "Request duration",
            buckets=(0.1, 0.5),
        )

        histogram.observe(0.2)

        lines = histogram._to_prometheus_lines()

        assert lines[0] == "# HELP request_duration_seconds Request duration"
        assert lines[1] == "# TYPE request_duration_seconds histogram"
        assert any("_bucket" in line for line in lines)
        assert any("_sum" in line for line in lines)
        assert any("_count" in line for line in lines)


# =============================================================================
# MetricRegistry Tests
# =============================================================================


class TestMetricRegistry:
    """Test MetricRegistry."""

    def test_registry_create_counter(self):
        """Test creating counter through registry."""
        registry = MetricRegistry()

        counter = registry.counter(
            "test_counter",
            "Test counter",
            label_names=("action",),
        )

        assert isinstance(counter, Counter)
        assert counter.name == "test_counter"

    def test_registry_create_gauge(self):
        """Test creating gauge through registry."""
        registry = MetricRegistry()

        gauge = registry.gauge(
            "test_gauge",
            "Test gauge",
        )

        assert isinstance(gauge, Gauge)

    def test_registry_create_histogram(self):
        """Test creating histogram through registry."""
        registry = MetricRegistry()

        histogram = registry.histogram(
            "test_histogram",
            "Test histogram",
            buckets=(0.1, 0.5, 1.0),
        )

        assert isinstance(histogram, Histogram)
        assert histogram._buckets == (0.1, 0.5, 1.0)

    def test_registry_returns_same_metric(self):
        """Test that registry returns same metric for same name."""
        registry = MetricRegistry()

        counter1 = registry.counter("test", "Test")
        counter2 = registry.counter("test", "Test")

        assert counter1 is counter2

    def test_registry_type_mismatch_raises(self):
        """Test that type mismatch raises error."""
        registry = MetricRegistry()

        registry.counter("test", "Test")

        with pytest.raises(TypeError, match="not a Gauge"):
            registry.gauge("test", "Test")

    def test_registry_to_prometheus(self):
        """Test full Prometheus export."""
        registry = MetricRegistry()

        counter = registry.counter("requests_total", "Total requests")
        gauge = registry.gauge("active_connections", "Active connections")

        counter.inc()
        gauge.set(5)

        output = registry.to_prometheus()

        assert "# HELP requests_total" in output
        assert "# TYPE requests_total counter" in output
        assert "requests_total 1.0" in output

        assert "# HELP active_connections" in output
        assert "# TYPE active_connections gauge" in output
        # Gauge may output as int (5) or float (5.0) depending on value
        assert "active_connections 5" in output

    def test_registry_clear(self):
        """Test clearing registry."""
        registry = MetricRegistry()

        registry.counter("test", "Test")
        registry.clear()

        # Should create new counter (not return cached)
        counter = registry.counter("test", "Test")
        assert counter.get() == 0.0


# =============================================================================
# MetricsMiddleware Tests
# =============================================================================


class TestMetricsMiddleware:
    """Test MetricsMiddleware."""

    def test_middleware_creates_standard_metrics(self):
        """Test that middleware creates standard metrics."""
        middleware = MetricsMiddleware()

        # Check that standard metrics exist
        output = middleware.registry.to_prometheus()

        assert "moleculer_request_total" in output
        assert "moleculer_request_active" in output
        assert "moleculer_request_duration_seconds" in output
        assert "moleculer_event_total" in output

    def test_middleware_custom_registry(self):
        """Test middleware with custom registry."""
        registry = MetricRegistry()
        middleware = MetricsMiddleware(registry=registry)

        assert middleware.registry is registry

    def test_middleware_custom_buckets(self):
        """Test middleware with custom histogram buckets."""
        buckets = (0.01, 0.1, 1.0)
        middleware = MetricsMiddleware(buckets=buckets)

        assert middleware._request_duration._buckets == buckets

    @pytest.mark.asyncio
    async def test_local_action_success(self):
        """Test metrics collection for successful local action."""
        middleware = MetricsMiddleware()

        # Create mock action
        action = MagicMock()
        action.name = "users.get"
        action.service = "users"

        # Create mock context
        ctx = MagicMock()
        ctx.caller = "test-caller"

        # Create mock handler
        async def mock_handler(ctx):
            return {"id": 1, "name": "John"}

        # Wrap handler
        wrapped = await middleware.local_action(mock_handler, action)

        # Execute
        result = await wrapped(ctx)

        assert result == {"id": 1, "name": "John"}

        # Check metrics
        labels = {
            "action": "users.get",
            "service": "users",
            "caller": "test-caller",
            "status": "success",
        }
        assert middleware._request_total.get(labels=labels) == 1.0

        # Active should be 0 (request finished)
        active_labels = {"action": "users.get", "service": "users"}
        assert middleware._request_active.get(labels=active_labels) == 0.0

        # Duration should have 1 observation
        assert middleware._request_duration.get_count(labels=active_labels) == 1

    @pytest.mark.asyncio
    async def test_local_action_error(self):
        """Test metrics collection for failed local action."""
        middleware = MetricsMiddleware()

        action = MagicMock()
        action.name = "users.get"
        action.service = "users"

        ctx = MagicMock()
        ctx.caller = "test-caller"

        async def mock_handler(ctx):
            raise ValueError("Test error")

        wrapped = await middleware.local_action(mock_handler, action)

        with pytest.raises(ValueError):
            await wrapped(ctx)

        # Check error counter
        labels = {
            "action": "users.get",
            "service": "users",
            "caller": "test-caller",
            "status": "error",
        }
        assert middleware._request_total.get(labels=labels) == 1.0

        # Active should be 0 (request finished even on error)
        active_labels = {"action": "users.get", "service": "users"}
        assert middleware._request_active.get(labels=active_labels) == 0.0

    @pytest.mark.asyncio
    async def test_remote_action_uses_same_metrics(self):
        """Test that remote action uses same metrics as local."""
        middleware = MetricsMiddleware()

        action = MagicMock()
        action.name = "users.get"
        action.service = "users"

        ctx = MagicMock()
        ctx.caller = "remote-node"

        async def mock_handler(ctx):
            return {"result": "ok"}

        wrapped = await middleware.remote_action(mock_handler, action)
        await wrapped(ctx)

        # Should use same counter
        labels = {
            "action": "users.get",
            "service": "users",
            "caller": "remote-node",
            "status": "success",
        }
        assert middleware._request_total.get(labels=labels) == 1.0

    @pytest.mark.asyncio
    async def test_local_event(self):
        """Test metrics collection for events."""
        middleware = MetricsMiddleware()

        event = MagicMock()
        event.name = "user.created"
        event.group = "notifications"

        ctx = MagicMock()

        async def mock_handler(ctx):
            pass

        wrapped = await middleware.local_event(mock_handler, event)
        await wrapped(ctx)

        labels = {"event": "user.created", "group": "notifications"}
        assert middleware._event_total.get(labels=labels) == 1.0

    @pytest.mark.asyncio
    async def test_local_event_default_group(self):
        """Test event with default group."""
        middleware = MetricsMiddleware()

        event = MagicMock()
        event.name = "test.event"
        event.group = ""  # Empty group

        ctx = MagicMock()

        async def mock_handler(ctx):
            pass

        wrapped = await middleware.local_event(mock_handler, event)
        await wrapped(ctx)

        labels = {"event": "test.event", "group": "default"}
        assert middleware._event_total.get(labels=labels) == 1.0

    @pytest.mark.asyncio
    async def test_concurrent_requests(self):
        """Test metrics with concurrent requests."""
        middleware = MetricsMiddleware()

        action = MagicMock()
        action.name = "slow.action"
        action.service = "slow"

        active_counts = []

        async def mock_handler(ctx):
            # Record active count during execution
            labels = {"action": "slow.action", "service": "slow"}
            active_counts.append(middleware._request_active.get(labels=labels))
            await asyncio.sleep(0.01)
            return "done"

        wrapped = await middleware.local_action(mock_handler, action)

        # Run 3 concurrent requests
        ctx1 = MagicMock()
        ctx1.caller = "caller1"
        ctx2 = MagicMock()
        ctx2.caller = "caller2"
        ctx3 = MagicMock()
        ctx3.caller = "caller3"

        await asyncio.gather(
            wrapped(ctx1),
            wrapped(ctx2),
            wrapped(ctx3),
        )

        # All should have finished
        labels = {"action": "slow.action", "service": "slow"}
        assert middleware._request_active.get(labels=labels) == 0.0

        # Total should be 3
        # Note: each caller has its own counter entry
        assert middleware._request_duration.get_count(labels=labels) == 3


# =============================================================================
# Timer Tests
# =============================================================================


class TestTimer:
    """Test Timer context manager."""

    def test_timer_basic(self):
        """Test basic timer usage."""
        registry = MetricRegistry()
        histogram = registry.histogram(
            "operation_seconds",
            "Operation time",
        )

        with Timer(histogram):
            pass  # Instant operation

        assert histogram.get_count() == 1
        assert histogram.get_sum() >= 0

    def test_timer_with_labels(self):
        """Test timer with labels."""
        registry = MetricRegistry()
        histogram = registry.histogram(
            "operation_seconds",
            "Operation time",
            label_names=("operation",),
        )

        with Timer(histogram, labels={"operation": "process"}):
            pass

        assert histogram.get_count(labels={"operation": "process"}) == 1

    def test_timer_measures_duration(self):
        """Test that timer measures actual duration."""
        import time

        registry = MetricRegistry()
        histogram = registry.histogram(
            "sleep_seconds",
            "Sleep time",
        )

        with Timer(histogram):
            time.sleep(0.05)  # 50ms

        # Should be at least 0.05 seconds
        assert histogram.get_sum() >= 0.05


# =============================================================================
# Thread Safety Tests
# =============================================================================


class TestThreadSafety:
    """Test thread safety of metrics."""

    def test_counter_thread_safety(self):
        """Test counter is thread-safe."""
        import threading

        counter = Counter("test_counter", "Test")

        def increment():
            for _ in range(1000):
                counter.inc()

        threads = [threading.Thread(target=increment) for _ in range(10)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert counter.get() == 10000.0

    def test_gauge_thread_safety(self):
        """Test gauge is thread-safe."""
        import threading

        gauge = Gauge("test_gauge", "Test")

        def modify():
            for _ in range(1000):
                gauge.inc()
                gauge.dec()

        threads = [threading.Thread(target=modify) for _ in range(10)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Should be 0 (equal inc and dec)
        assert gauge.get() == 0.0

    def test_histogram_thread_safety(self):
        """Test histogram is thread-safe."""
        import threading

        histogram = Histogram("test_histogram", "Test")

        def observe():
            for i in range(1000):
                histogram.observe(i / 1000.0)

        threads = [threading.Thread(target=observe) for _ in range(10)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert histogram.get_count() == 10000


# =============================================================================
# Prometheus Format Validation Tests
# =============================================================================


class TestPrometheusFormat:
    """Test Prometheus format compliance."""

    def test_prometheus_format_structure(self):
        """Test overall Prometheus format structure."""
        registry = MetricRegistry()

        counter = registry.counter("http_requests_total", "Total HTTP requests")
        counter.inc()

        output = registry.to_prometheus()
        lines = output.strip().split("\n")

        # Should have HELP, TYPE, and value
        assert any(line.startswith("# HELP") for line in lines)
        assert any(line.startswith("# TYPE") for line in lines)
        assert any(line.startswith("http_requests_total") for line in lines)

    def test_prometheus_label_escaping(self):
        """Test that label values are properly quoted."""
        registry = MetricRegistry()

        counter = registry.counter(
            "test_counter",
            "Test",
            label_names=("action",),
        )
        counter.inc(labels={"action": "users.get"})

        output = registry.to_prometheus()

        # Labels should be quoted
        assert 'action="users.get"' in output

    def test_prometheus_histogram_format(self):
        """Test histogram follows Prometheus format."""
        registry = MetricRegistry()

        histogram = registry.histogram(
            "request_duration_seconds",
            "Request duration",
            buckets=(0.1, 0.5),
        )
        histogram.observe(0.2)

        output = registry.to_prometheus()

        # Should have _bucket, _sum, _count suffixes
        assert "request_duration_seconds_bucket" in output
        assert "request_duration_seconds_sum" in output
        assert "request_duration_seconds_count" in output

        # Should have le label for buckets
        assert 'le="0.1"' in output
        assert 'le="0.5"' in output
        assert 'le="+Inf"' in output

    def test_prometheus_label_escaping_special_chars(self):
        """Test that special characters in label values are escaped."""
        registry = MetricRegistry()

        counter = registry.counter(
            "test_counter",
            "Test",
            label_names=("action",),
        )

        # Test double quote escaping
        counter.inc(labels={"action": 'my "quoted" value'})
        output = registry.to_prometheus()
        assert 'action="my \\"quoted\\" value"' in output

    def test_prometheus_label_escaping_backslash(self):
        """Test backslash escaping in labels."""
        registry = MetricRegistry()

        counter = registry.counter(
            "test_counter",
            "Test",
            label_names=("path",),
        )

        counter.inc(labels={"path": r"C:\Users\test"})
        output = registry.to_prometheus()
        assert r'path="C:\\Users\\test"' in output

    def test_prometheus_label_escaping_newline(self):
        """Test newline escaping in labels."""
        registry = MetricRegistry()

        counter = registry.counter(
            "test_counter",
            "Test",
            label_names=("msg",),
        )

        counter.inc(labels={"msg": "line1\nline2"})
        output = registry.to_prometheus()
        assert 'msg="line1\\nline2"' in output


# =============================================================================
# Repr Tests
# =============================================================================


class TestRepr:
    """Test __repr__ methods for debugging."""

    def test_counter_repr(self):
        """Test Counter repr."""
        counter = Counter("test_counter", "Test description", label_names=("action",))
        counter.inc(labels={"action": "test"})

        repr_str = repr(counter)
        assert "Counter" in repr_str
        assert "test_counter" in repr_str
        assert "series=1" in repr_str

    def test_gauge_repr(self):
        """Test Gauge repr."""
        gauge = Gauge("test_gauge", "Test description", label_names=("service",))
        gauge.set(42, labels={"service": "api"})

        repr_str = repr(gauge)
        assert "Gauge" in repr_str
        assert "test_gauge" in repr_str
        assert "series=1" in repr_str

    def test_histogram_repr(self):
        """Test Histogram repr."""
        histogram = Histogram(
            "test_histogram",
            "Test description",
            label_names=("action",),
            buckets=(0.1, 0.5, 1.0),
        )
        histogram.observe(0.2, labels={"action": "test"})

        repr_str = repr(histogram)
        assert "Histogram" in repr_str
        assert "test_histogram" in repr_str
        assert "buckets=3" in repr_str
        assert "series=1" in repr_str

    def test_registry_repr(self):
        """Test MetricRegistry repr."""
        registry = MetricRegistry()
        registry.counter("counter1", "Test")
        registry.gauge("gauge1", "Test")

        repr_str = repr(registry)
        assert "MetricRegistry" in repr_str
        assert "metrics=2" in repr_str

    def test_timer_repr(self):
        """Test Timer repr."""
        registry = MetricRegistry()
        histogram = registry.histogram("test_histogram", "Test")
        timer = Timer(histogram, labels={"op": "test"})

        repr_str = repr(timer)
        assert "Timer" in repr_str
        assert "test_histogram" in repr_str


# =============================================================================
# Async Timer Tests
# =============================================================================


class TestAsyncTimer:
    """Test async Timer context manager."""

    @pytest.mark.asyncio
    async def test_async_timer_basic(self):
        """Test async timer basic usage."""
        registry = MetricRegistry()
        histogram = registry.histogram(
            "async_operation_seconds",
            "Async operation time",
        )

        async with Timer(histogram):
            await asyncio.sleep(0.01)  # 10ms

        assert histogram.get_count() == 1
        assert histogram.get_sum() >= 0.01

    @pytest.mark.asyncio
    async def test_async_timer_with_labels(self):
        """Test async timer with labels."""
        registry = MetricRegistry()
        histogram = registry.histogram(
            "async_operation_seconds",
            "Async operation time",
            label_names=("operation",),
        )

        async with Timer(histogram, labels={"operation": "fetch"}):
            await asyncio.sleep(0.01)

        assert histogram.get_count(labels={"operation": "fetch"}) == 1

    @pytest.mark.asyncio
    async def test_async_timer_exception_handling(self):
        """Test async timer records duration even on exception."""
        registry = MetricRegistry()
        histogram = registry.histogram(
            "async_operation_seconds",
            "Async operation time",
        )

        with pytest.raises(ValueError):
            async with Timer(histogram):
                await asyncio.sleep(0.01)
                raise ValueError("test error")

        # Should still record the observation
        assert histogram.get_count() == 1


# =============================================================================
# CancelledError Handling Tests
# =============================================================================


class TestCancelledErrorHandling:
    """Test handling of asyncio.CancelledError in middleware."""

    @pytest.mark.asyncio
    async def test_cancelled_request_status(self):
        """Test that cancelled requests get status='cancelled'."""
        import asyncio

        middleware = MetricsMiddleware()

        class MockAction:
            name = "users.get"
            service = "users"

        class MockCtx:
            caller = "test-caller"

        async def slow_handler(ctx):
            await asyncio.sleep(10)  # Will be cancelled
            return "result"

        handler = await middleware.local_action(slow_handler, MockAction())

        # Create a task that will be cancelled
        task = asyncio.create_task(handler(MockCtx()))

        # Give the handler time to start
        await asyncio.sleep(0.01)

        # Cancel the task
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        # Check that the request was recorded with status="cancelled"
        output = middleware.registry.to_prometheus()
        assert 'status="cancelled"' in output
