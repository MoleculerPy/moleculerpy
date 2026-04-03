"""Unit tests for pluggable metrics reporter system (PRD-012, v0.14.8).

Covers:
- BaseReporter: match_metric_name, format_metric_name, abstract method
- resolve_reporter(): string/dict/instance/class resolution
- MetricRegistry (extended): add_reporter, list, stop_reporters
- ConsoleReporter: metric_changed tracking, only_changes, init without loop
- PrometheusReporter: get_text, metric_changed no-op
- snapshot() on Counter, Gauge, Histogram
- Backward compatibility: imports from middleware/metrics still work
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from moleculerpy.metric_reporters.console import ConsoleReporter
from moleculerpy.metric_reporters.prometheus import PrometheusReporter
from moleculerpy.metric_reporters.registry import MetricRegistry
from moleculerpy.metric_reporters.reporter import (
    _REPORTER_REGISTRY,
    BaseReporter,
    register_reporter,
    resolve_reporter,
)
from moleculerpy.middleware.metrics import Counter, Gauge, Histogram

# =============================================================================
# Helpers / Fixtures
# =============================================================================


class ConcreteReporter(BaseReporter):
    """Minimal concrete reporter for testing BaseReporter."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.changes: list[tuple[Any, Any, Any]] = []

    def metric_changed(
        self,
        metric: Any,
        value: Any,
        labels: dict[str, str] | None = None,
    ) -> None:
        self.changes.append((metric, value, labels))


def make_metric(name: str = "test_metric") -> MagicMock:
    """Return a mock BaseMetric with a .name attribute."""
    m = MagicMock()
    m.name = name
    return m


@pytest.fixture
def registry() -> MetricRegistry:
    reg = MetricRegistry()
    yield reg
    # cleanup reporters after each test
    # (no running loop needed, stop_reporters is async)


# =============================================================================
# BaseReporter — match_metric_name
# =============================================================================


class TestBaseReporterMatchMetricName:
    """Tests for BaseReporter.match_metric_name()."""

    def test_no_filter_matches_all(self) -> None:
        r = ConcreteReporter()
        assert r.match_metric_name("any.metric") is True
        assert r.match_metric_name("foo.bar.baz") is True

    def test_includes_pattern_matches(self) -> None:
        r = ConcreteReporter(includes=["req*"])
        assert r.match_metric_name("requests_total") is True

    def test_includes_pattern_excludes_non_match(self) -> None:
        r = ConcreteReporter(includes=["req*"])
        assert r.match_metric_name("internal_count") is False

    def test_excludes_pattern_filters_out(self) -> None:
        r = ConcreteReporter(excludes=["internal*"])
        assert r.match_metric_name("internal_queue") is False
        assert r.match_metric_name("requests_total") is True

    def test_includes_and_excludes_combined(self) -> None:
        r = ConcreteReporter(includes=["mol*"], excludes=["moleculer_internal*"])
        assert r.match_metric_name("moleculer_request_total") is True
        assert r.match_metric_name("moleculer_internal_queue") is False

    def test_includes_multiple_patterns(self) -> None:
        r = ConcreteReporter(includes=["req*", "err*"])
        assert r.match_metric_name("requests_total") is True
        assert r.match_metric_name("error_count") is True
        assert r.match_metric_name("latency_ms") is False

    def test_excludes_multiple_patterns(self) -> None:
        r = ConcreteReporter(excludes=["internal*", "debug*"])
        assert r.match_metric_name("internal_foo") is False
        assert r.match_metric_name("debug_bar") is False
        assert r.match_metric_name("public_metric") is True


# =============================================================================
# BaseReporter — format_metric_name
# =============================================================================


class TestBaseReporterFormatMetricName:
    """Tests for BaseReporter.format_metric_name()."""

    def test_no_prefix_no_suffix(self) -> None:
        r = ConcreteReporter()
        assert r.format_metric_name("requests_total") == "requests_total"

    def test_prefix_applied(self) -> None:
        r = ConcreteReporter(metric_name_prefix="app_")
        assert r.format_metric_name("requests_total") == "app_requests_total"

    def test_suffix_applied(self) -> None:
        r = ConcreteReporter(metric_name_suffix="_count")
        assert r.format_metric_name("requests") == "requests_count"

    def test_prefix_and_suffix(self) -> None:
        r = ConcreteReporter(metric_name_prefix="srv_", metric_name_suffix="_total")
        assert r.format_metric_name("requests") == "srv_requests_total"


# =============================================================================
# BaseReporter — abstract method enforcement
# =============================================================================


class TestBaseReporterAbstract:
    """metric_changed() is abstract and must be implemented."""

    def test_cannot_instantiate_base_reporter(self) -> None:
        with pytest.raises(TypeError):
            BaseReporter()  # type: ignore[abstract]


# =============================================================================
# resolve_reporter()
# =============================================================================


class TestResolveReporter:
    """Tests for resolve_reporter()."""

    def test_string_console_returns_console_reporter(self) -> None:
        reporter = resolve_reporter("console")
        assert isinstance(reporter, ConsoleReporter)

    def test_string_prometheus_returns_prometheus_reporter(self) -> None:
        reporter = resolve_reporter("prometheus")
        assert isinstance(reporter, PrometheusReporter)

    def test_unknown_string_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Unknown reporter"):
            resolve_reporter("nonexistent_reporter_xyz")

    def test_dict_with_type_console_and_interval(self) -> None:
        reporter = resolve_reporter({"type": "console", "interval": 10})
        assert isinstance(reporter, ConsoleReporter)
        assert reporter.interval == 10

    def test_dict_with_type_prometheus(self) -> None:
        reporter = resolve_reporter({"type": "prometheus"})
        assert isinstance(reporter, PrometheusReporter)

    def test_dict_with_unknown_type_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Unknown reporter type"):
            resolve_reporter({"type": "unknown_type_xyz"})

    def test_instance_passed_through(self) -> None:
        original = ConsoleReporter(interval=30)
        resolved = resolve_reporter(original)
        assert resolved is original

    def test_class_is_instantiated(self) -> None:
        reporter = resolve_reporter(ConsoleReporter)
        assert isinstance(reporter, ConsoleReporter)

    def test_concrete_subclass_instantiated(self) -> None:
        reporter = resolve_reporter(ConcreteReporter)
        assert isinstance(reporter, ConcreteReporter)

    def test_invalid_type_raises_value_error(self) -> None:
        with pytest.raises(ValueError):
            resolve_reporter(12345)  # type: ignore[arg-type]


# =============================================================================
# MetricRegistry (extended) — reporter management
# =============================================================================


class TestMetricRegistryReporters:
    """Tests for MetricRegistry with reporter support."""

    def test_add_reporter_adds_and_inits(self, registry: MetricRegistry) -> None:
        reporter = ConcreteReporter()
        added = registry.add_reporter(reporter)
        assert added is reporter
        assert reporter.registry is registry
        assert reporter in registry._reporters

    def test_add_reporter_by_string(self, registry: MetricRegistry) -> None:
        reporter = registry.add_reporter("console")
        assert isinstance(reporter, ConsoleReporter)
        assert reporter in registry._reporters

    def test_add_reporter_by_dict(self, registry: MetricRegistry) -> None:
        reporter = registry.add_reporter({"type": "prometheus"})
        assert isinstance(reporter, PrometheusReporter)

    def test_multiple_reporters_coexist(self, registry: MetricRegistry) -> None:
        r1 = registry.add_reporter(ConcreteReporter())
        r2 = registry.add_reporter(ConcreteReporter())
        assert len(registry._reporters) == 2
        assert r1 in registry._reporters
        assert r2 in registry._reporters

    def test_list_returns_all_metrics(self, registry: MetricRegistry) -> None:
        registry.counter("requests_total", "Total requests")
        registry.gauge("active_requests", "Active requests")
        metrics = registry.list()
        names = {m.name for m in metrics}
        assert "requests_total" in names
        assert "active_requests" in names

    def test_list_includes_glob_filter(self, registry: MetricRegistry) -> None:
        registry.counter("req_total", "Total")
        registry.counter("req_errors", "Errors")
        registry.gauge("active_requests", "Active")
        registry.counter("internal_queue", "Internal")

        metrics = registry.list(includes=["req*"])
        names = {m.name for m in metrics}
        assert "req_total" in names
        assert "req_errors" in names
        assert "internal_queue" not in names
        assert "active_requests" not in names

    def test_list_excludes_glob_filter(self, registry: MetricRegistry) -> None:
        registry.counter("internal_count", "Internal")
        registry.counter("internal_queue", "Internal queue")
        registry.counter("public_metric", "Public")

        metrics = registry.list(excludes=["internal*"])
        names = {m.name for m in metrics}
        assert "public_metric" in names
        assert "internal_count" not in names
        assert "internal_queue" not in names

    def test_list_includes_and_excludes(self, registry: MetricRegistry) -> None:
        registry.counter("mol_req", "Mol req")
        registry.counter("mol_internal", "Internal")
        registry.counter("other", "Other")

        metrics = registry.list(includes=["mol*"], excludes=["mol_internal*"])
        names = {m.name for m in metrics}
        assert "mol_req" in names
        assert "mol_internal" not in names
        assert "other" not in names

    @pytest.mark.asyncio
    async def test_stop_reporters_calls_stop_on_all(self, registry: MetricRegistry) -> None:
        stopped: list[str] = []

        class TrackingReporter(ConcreteReporter):
            def __init__(self, label: str) -> None:
                super().__init__()
                self.label = label

            async def stop(self) -> None:
                stopped.append(self.label)

        registry.add_reporter(TrackingReporter("r1"))
        registry.add_reporter(TrackingReporter("r2"))

        await registry.stop_reporters()
        assert "r1" in stopped
        assert "r2" in stopped


# =============================================================================
# ConsoleReporter
# =============================================================================


class TestConsoleReporter:
    """Tests for ConsoleReporter."""

    def test_metric_changed_tracks_changed_name(self) -> None:
        reporter = ConsoleReporter()
        metric = make_metric("requests_total")
        reporter.metric_changed(metric, 42)
        assert "requests_total" in reporter._changed_names

    def test_metric_changed_respects_includes_filter(self) -> None:
        reporter = ConsoleReporter(includes=["req*"])
        metric_included = make_metric("requests_total")
        metric_excluded = make_metric("internal_count")

        reporter.metric_changed(metric_included, 1)
        reporter.metric_changed(metric_excluded, 1)

        assert "requests_total" in reporter._changed_names
        assert "internal_count" not in reporter._changed_names

    def test_only_changes_filters_output(self) -> None:
        """only_changes=True: _print_metrics only prints changed metrics."""
        reporter = ConsoleReporter(only_changes=True)
        reg = MagicMock()

        m1 = MagicMock()
        m1.name = "changed_metric"
        m1.snapshot.return_value = {"type": "counter", "values": {}}

        m2 = MagicMock()
        m2.name = "unchanged_metric"
        m2.snapshot.return_value = {"type": "gauge", "values": {}}

        reg.list.return_value = [m1, m2]
        reporter.registry = reg
        reporter._changed_names.add("changed_metric")

        with patch.object(reporter.logger, "info") as mock_log:
            reporter._print_metrics()

        assert mock_log.called
        logged_text = mock_log.call_args[0][0]
        assert "changed_metric" in logged_text
        assert "unchanged_metric" not in logged_text

    def test_only_changes_false_prints_all(self) -> None:
        """only_changes=False: _print_metrics prints all metrics."""
        reporter = ConsoleReporter(only_changes=False)
        reg = MagicMock()

        m1 = MagicMock()
        m1.name = "metric_a"
        m1.snapshot.return_value = {"type": "counter", "values": {}}

        m2 = MagicMock()
        m2.name = "metric_b"
        m2.snapshot.return_value = {"type": "gauge", "values": {}}

        reg.list.return_value = [m1, m2]
        reporter.registry = reg

        with patch.object(reporter.logger, "info") as mock_log:
            reporter._print_metrics()

        assert mock_log.called
        logged_text = mock_log.call_args[0][0]
        assert "metric_a" in logged_text
        assert "metric_b" in logged_text

    def test_init_without_running_loop_does_not_crash(self) -> None:
        """init() outside async context must not raise."""
        reporter = ConsoleReporter()
        reg = MagicMock()
        # Should not raise RuntimeError even without a running event loop
        reporter.init(reg)
        assert reporter.registry is reg
        assert reporter._task is None

    @pytest.mark.asyncio
    async def test_init_with_running_loop_creates_task(self) -> None:
        """init() inside async context creates a background task."""
        reporter = ConsoleReporter(interval=100)  # long interval — won't fire in test
        reg = MagicMock()
        reg.list.return_value = []
        reporter.init(reg)
        assert reporter._task is not None
        assert not reporter._task.done()
        await reporter.stop()

    @pytest.mark.asyncio
    async def test_stop_cancels_task(self) -> None:
        """stop() cancels the background print task."""
        reporter = ConsoleReporter(interval=100)
        reg = MagicMock()
        reg.list.return_value = []
        reporter.init(reg)
        task = reporter._task
        await reporter.stop()
        assert task is not None
        assert task.done()
        assert reporter._task is None

    def test_print_metrics_no_registry_does_not_crash(self) -> None:
        """_print_metrics() with no registry should be silent."""
        reporter = ConsoleReporter()
        # registry is None by default
        reporter._print_metrics()  # should not raise


# =============================================================================
# PrometheusReporter
# =============================================================================


class TestPrometheusReporter:
    """Tests for PrometheusReporter."""

    def test_get_text_returns_prometheus_format(self) -> None:
        reg = MetricRegistry()
        c = reg.counter("requests_total", "Total requests")
        c.inc(5)

        reporter = PrometheusReporter()
        reporter.init(reg)

        text = reporter.get_text()
        assert "# HELP requests_total" in text
        assert "# TYPE requests_total counter" in text
        assert "requests_total 5.0" in text

    def test_get_text_without_registry_returns_empty(self) -> None:
        reporter = PrometheusReporter()
        # registry is None
        assert reporter.get_text() == ""

    def test_metric_changed_is_noop(self) -> None:
        """PrometheusReporter.metric_changed() must not raise."""
        reporter = PrometheusReporter()
        metric = make_metric("some_metric")
        reporter.metric_changed(metric, 42)  # should not raise or do anything
        reporter.metric_changed(metric, 42, labels={"key": "val"})

    def test_get_text_with_multiple_metric_types(self) -> None:
        reg = MetricRegistry()
        reg.counter("req_total", "Requests")
        reg.gauge("active", "Active")
        reg.histogram("latency", "Latency", buckets=(0.1, 0.5))

        reporter = PrometheusReporter()
        reporter.init(reg)

        text = reporter.get_text()
        assert "req_total" in text
        assert "active" in text
        assert "latency" in text


# =============================================================================
# snapshot() on metric types
# =============================================================================


class TestMetricSnapshots:
    """Tests for snapshot() method on Counter, Gauge, Histogram."""

    def test_counter_snapshot_includes_values(self) -> None:
        c = Counter("my_counter", "desc")
        c.inc(3)
        c.inc(7)
        snap = c.snapshot()
        assert snap["name"] == "my_counter"
        assert snap["type"] == "counter"
        assert () in snap["values"]
        assert snap["values"][()] == 10.0

    def test_counter_snapshot_with_labels(self) -> None:
        c = Counter("req_total", "desc", label_names=("status",))
        c.inc(2, labels={"status": "ok"})
        snap = c.snapshot()
        assert ("ok",) in snap["values"]
        assert snap["values"][("ok",)] == 2.0

    def test_gauge_snapshot_includes_values(self) -> None:
        g = Gauge("active", "desc")
        g.set(42.0)
        snap = g.snapshot()
        assert snap["name"] == "active"
        assert snap["type"] == "gauge"
        assert snap["values"][()] == 42.0

    def test_gauge_snapshot_with_labels(self) -> None:
        g = Gauge("active", "desc", label_names=("action",))
        g.inc(5, labels={"action": "get"})
        snap = g.snapshot()
        assert ("get",) in snap["values"]

    def test_histogram_snapshot_includes_count_and_sum(self) -> None:
        h = Histogram("latency", "desc", buckets=(0.1, 0.5, 1.0))
        h.observe(0.05)
        h.observe(0.3)
        snap = h.snapshot()
        assert snap["name"] == "latency"
        assert snap["type"] == "histogram"
        assert "values" in snap
        entry = snap["values"]["()"]
        assert entry["count"] == 2
        assert abs(entry["sum"] - 0.35) < 1e-9

    def test_histogram_snapshot_empty_has_values_key(self) -> None:
        h = Histogram("empty_hist", "desc")
        snap = h.snapshot()
        assert "values" in snap
        assert isinstance(snap["values"], dict)


# =============================================================================
# Backward compatibility
# =============================================================================


class TestBackwardCompatibility:
    """Imports from middleware/metrics must still work (no breaking changes)."""

    def test_import_counter_from_middleware_metrics(self) -> None:
        from moleculerpy.middleware.metrics import Counter as CompatCounter

        c = CompatCounter("compat_counter", "desc")
        c.inc(1)
        assert c.get() == 1.0

    def test_import_gauge_from_middleware_metrics(self) -> None:
        from moleculerpy.middleware.metrics import Gauge as CompatGauge

        g = CompatGauge("compat_gauge", "desc")
        g.set(5.0)
        assert g.get() == 5.0

    def test_import_histogram_from_middleware_metrics(self) -> None:
        from moleculerpy.middleware.metrics import Histogram as CompatHistogram

        h = CompatHistogram("compat_hist", "desc")
        h.observe(0.1)
        assert h.get_count() == 1

    def test_import_metric_registry_from_middleware_metrics(self) -> None:
        from moleculerpy.middleware.metrics import MetricRegistry as R

        reg = R()
        c = reg.counter("test_compat", "desc")
        c.inc()
        assert c.get() == 1.0

    def test_original_metric_registry_has_no_reporters(self) -> None:
        """The original MetricRegistry from middleware/metrics has no reporter support."""
        from moleculerpy.middleware.metrics import MetricRegistry as OrigRegistry

        reg = OrigRegistry()
        assert not hasattr(reg, "_reporters")
        assert not hasattr(reg, "add_reporter")

    def test_import_all_from_metric_reporters_package(self) -> None:
        from moleculerpy.metric_reporters import (
            BaseReporter,
            ConsoleReporter,
            MetricRegistry,
            PrometheusReporter,
            resolve_reporter,
        )

        assert BaseReporter is not None
        assert ConsoleReporter is not None
        assert MetricRegistry is not None
        assert PrometheusReporter is not None
        assert resolve_reporter is not None

    def test_metrics_middleware_still_works(self) -> None:
        """MetricsMiddleware from middleware.metrics imports and creates metrics."""
        from moleculerpy.middleware.metrics import MetricsMiddleware

        mw = MetricsMiddleware()
        text = mw.registry.to_prometheus()
        assert "moleculer_request_total" in text


# =============================================================================
# register_reporter
# =============================================================================


class TestRegisterReporter:
    """Tests for register_reporter() function."""

    def test_register_and_resolve_custom_reporter(self) -> None:
        register_reporter("my_custom", ConcreteReporter)
        resolved = resolve_reporter("my_custom")
        assert isinstance(resolved, ConcreteReporter)

    def test_register_name_is_case_insensitive(self) -> None:
        register_reporter("MyReporter", ConcreteReporter)
        resolved = resolve_reporter("myreporter")
        assert isinstance(resolved, ConcreteReporter)
