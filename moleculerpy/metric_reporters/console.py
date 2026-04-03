"""Console metrics reporter for MoleculerPy (PRD-012).

Node.js equivalent: src/metrics/reporters/console.js (225 LOC)
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from moleculerpy.metric_reporters.reporter import BaseReporter, register_reporter

if TYPE_CHECKING:
    from moleculerpy.metric_reporters.registry import MetricRegistry
    from moleculerpy.middleware.metrics import BaseMetric


class ConsoleReporter(BaseReporter):
    """Reports metrics to console/logger at a configurable interval.

    Args:
        interval: Seconds between metric prints (default: 5)
        only_changes: Only print metrics that changed since last print
        includes: Glob patterns to include
        excludes: Glob patterns to exclude
    """

    def __init__(
        self,
        interval: float = 5.0,
        only_changes: bool = True,
        includes: list[str] | None = None,
        excludes: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(includes=includes, excludes=excludes, **kwargs)
        self.interval = interval
        self.only_changes = only_changes
        self._changed_names: set[str] = set()
        self._task: asyncio.Task[None] | None = None

    def init(self, registry: MetricRegistry) -> None:
        """Start periodic print task."""
        super().init(registry)
        # Cancel existing task if init() called twice (prevents resource leak)
        if self._task and not self._task.done():
            self._task.cancel()
            self._task = None
        try:
            loop = asyncio.get_running_loop()
            self._task = loop.create_task(self._print_loop())
        except RuntimeError:
            self.logger.warning(
                "ConsoleReporter: no running event loop — periodic printing disabled. "
                "Call init() from within an async context (e.g., broker.start())."
            )

    async def stop(self) -> None:
        """Cancel periodic print task."""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    def metric_changed(
        self,
        metric: BaseMetric,
        value: Any,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Track which metrics have changed."""
        if self.match_metric_name(metric.name):
            self._changed_names.add(metric.name)

    async def _print_loop(self) -> None:
        """Periodically print metric summaries."""
        while True:
            await asyncio.sleep(self.interval)
            self._print_metrics()

    def _print_metrics(self) -> None:
        """Print current metrics to logger."""
        if self.registry is None:
            return

        metrics = self.registry.list(includes=self.includes, excludes=self.excludes)

        if self.only_changes:
            metrics = [m for m in metrics if m.name in self._changed_names]
            self._changed_names.clear()

        if not metrics:
            return

        lines = [f"--- Metrics ({len(metrics)} items) ---"]
        for metric in metrics:
            name = self.format_metric_name(metric.name)
            snap = metric.snapshot()
            values = snap.get("values", {})
            lines.append(f"  {name} ({snap['type']}): {values}")
        lines.append("---")

        self.logger.info("\n".join(lines))


register_reporter("console", ConsoleReporter)
