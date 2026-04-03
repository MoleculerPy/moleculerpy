"""Prometheus metrics reporter for MoleculerPy (PRD-012).

Node.js equivalent: src/metrics/reporters/prometheus.js (300 LOC)
Note: Does NOT start an HTTP server — serving is moleculerpy-web territory.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from moleculerpy.metric_reporters.reporter import BaseReporter, register_reporter

if TYPE_CHECKING:
    from moleculerpy.middleware.metrics import BaseMetric


class PrometheusReporter(BaseReporter):
    """Prometheus text format reporter.

    Generates Prometheus exposition format from the registry.
    Use get_text() to retrieve output, serve via your HTTP framework.

    Args:
        default_labels: Dict of labels added to every metric
        includes: Glob patterns to include
        excludes: Glob patterns to exclude
    """

    def __init__(
        self,
        default_labels: dict[str, str] | None = None,
        includes: list[str] | None = None,
        excludes: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(includes=includes, excludes=excludes, **kwargs)
        self.default_labels = default_labels or {}

    def metric_changed(
        self,
        metric: BaseMetric,
        value: Any,
        labels: dict[str, str] | None = None,
    ) -> None:
        """No-op — Prometheus is pull-based, metrics read at scrape time."""

    def get_text(self) -> str:
        """Generate Prometheus exposition format text.

        Returns:
            Multi-line string (Content-Type: text/plain; version=0.0.4)
        """
        if self.registry is None:
            return ""
        return self.registry.to_prometheus()


register_reporter("prometheus", PrometheusReporter)
