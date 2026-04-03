"""Base reporter and resolution for MoleculerPy metrics system.

Follows Moleculer.js BaseReporter pattern (src/metrics/reporters/base.js).
PRD-012: Pluggable Metrics Reporter System.
"""

from __future__ import annotations

import fnmatch
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from moleculerpy.metric_reporters.registry import MetricRegistry
    from moleculerpy.middleware.metrics import BaseMetric


class BaseReporter(ABC):
    """Abstract base class for all metric reporters.

    Node.js equivalent: src/metrics/reporters/base.js
    """

    def __init__(
        self,
        includes: list[str] | None = None,
        excludes: list[str] | None = None,
        metric_name_prefix: str | None = None,
        metric_name_suffix: str | None = None,
    ) -> None:
        self.includes = includes
        self.excludes = excludes
        self.metric_name_prefix = metric_name_prefix or ""
        self.metric_name_suffix = metric_name_suffix or ""
        self.registry: MetricRegistry | None = None
        self.logger = logging.getLogger(type(self).__name__)

    def init(self, registry: MetricRegistry) -> None:
        """Initialize reporter with registry reference."""
        self.registry = registry

    async def stop(self) -> None:
        """Stop reporter and release resources. Override for cleanup."""
        return  # Default no-op; subclasses override for cleanup

    def match_metric_name(self, name: str) -> bool:
        """Check if a metric name passes the includes/excludes filters."""
        if self.includes:
            if not any(fnmatch.fnmatch(name, pat) for pat in self.includes):
                return False
        if self.excludes:
            if any(fnmatch.fnmatch(name, pat) for pat in self.excludes):
                return False
        return True

    def format_metric_name(self, name: str) -> str:
        """Apply prefix and suffix to metric name."""
        return f"{self.metric_name_prefix}{name}{self.metric_name_suffix}"

    @abstractmethod
    def metric_changed(
        self,
        metric: BaseMetric,
        value: Any,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Called when a metric value changes."""
        ...


_REPORTER_REGISTRY: dict[str, type[BaseReporter]] = {}


def register_reporter(name: str, cls: type[BaseReporter]) -> None:
    """Register a reporter class by name for string-based resolution."""
    _REPORTER_REGISTRY[name.lower()] = cls


def _reset_reporter_registry() -> None:
    """Reset reporter registry to built-in reporters only. For testing."""
    _REPORTER_REGISTRY.clear()
    # Re-register builtins (imported modules call register_reporter at import time)
    from moleculerpy.metric_reporters.console import ConsoleReporter  # noqa: PLC0415
    from moleculerpy.metric_reporters.prometheus import PrometheusReporter  # noqa: PLC0415

    _REPORTER_REGISTRY["console"] = ConsoleReporter
    _REPORTER_REGISTRY["prometheus"] = PrometheusReporter


def resolve_reporter(
    spec: str | dict[str, Any] | BaseReporter | type[BaseReporter],
) -> BaseReporter:
    """Resolve a reporter specification to a BaseReporter instance.

    Supports: string, dict, instance, class.
    """
    if isinstance(spec, BaseReporter):
        return spec
    if isinstance(spec, type) and issubclass(spec, BaseReporter):
        return spec()
    if isinstance(spec, str):
        cls = _REPORTER_REGISTRY.get(spec.lower())
        if cls is None:
            raise ValueError(
                f"Unknown reporter: {spec!r}. Available: {list(_REPORTER_REGISTRY.keys())}"
            )
        return cls()
    if isinstance(spec, dict):
        reporter_type = spec.get("type", "")
        cls = _REPORTER_REGISTRY.get(str(reporter_type).lower())
        if cls is None:
            raise ValueError(f"Unknown reporter type: {reporter_type!r}")
        opts = {k: v for k, v in spec.items() if k != "type"}
        return cls(**opts)
    raise ValueError(f"Cannot resolve reporter from: {spec!r}")
