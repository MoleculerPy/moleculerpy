"""Extended MetricRegistry with pluggable reporter support (PRD-012).

Wraps the existing MetricRegistry from middleware/metrics.py and adds
reporter lifecycle management.
"""

from __future__ import annotations

import fnmatch
from typing import Any

from moleculerpy.metric_reporters.reporter import BaseReporter, resolve_reporter
from moleculerpy.middleware.metrics import BaseMetric, Counter, Gauge, Histogram
from moleculerpy.middleware.metrics import MetricRegistry as _BaseRegistry


class MetricRegistry(_BaseRegistry):
    """MetricRegistry with pluggable reporter support.

    Extends the base MetricRegistry with reporter management and
    metric change notifications.
    """

    __slots__ = ("_reporters",)

    def __init__(self) -> None:
        super().__init__()
        self._reporters: list[BaseReporter] = []

    def __repr__(self) -> str:
        return f"MetricRegistry(metrics={len(self._metrics)}, reporters={len(self._reporters)})"

    # -----------------------------------------------------------------
    # Reporter management
    # -----------------------------------------------------------------

    def add_reporter(self, reporter: BaseReporter | str | dict[str, Any]) -> BaseReporter:
        """Add a reporter to this registry."""
        resolved = (
            resolve_reporter(reporter) if not isinstance(reporter, BaseReporter) else reporter
        )
        resolved.init(self)
        self._reporters.append(resolved)
        return resolved

    async def stop_reporters(self) -> None:
        """Stop all reporters."""
        for reporter in self._reporters:
            await reporter.stop()

    def _notify_reporters(
        self,
        metric: Counter | Gauge | Histogram,
        value: Any,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Notify all reporters of a metric change.

        IMPORTANT: Must NOT be called while holding self._lock to avoid
        deadlock if a reporter callback registers new metrics.
        """
        for reporter in self._reporters:
            if reporter.match_metric_name(metric.name):
                reporter.metric_changed(metric, value, labels)

    # -----------------------------------------------------------------
    # Query
    # -----------------------------------------------------------------

    def list(
        self,
        includes: list[str] | None = None,
        excludes: list[str] | None = None,
    ) -> list[BaseMetric]:
        """List all metrics, optionally filtered by glob patterns."""
        with self._lock:
            metrics: list[BaseMetric] = list(self._metrics.values())

        if includes:
            metrics = [m for m in metrics if any(fnmatch.fnmatch(m.name, p) for p in includes)]
        if excludes:
            metrics = [m for m in metrics if not any(fnmatch.fnmatch(m.name, p) for p in excludes)]

        return metrics
