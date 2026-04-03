"""Pluggable metrics reporter system for MoleculerPy (PRD-012).

Metric types (Counter, Gauge, Histogram) live in middleware/metrics.py.
This package adds the pluggable reporter layer on top.

Usage:
    from moleculerpy.metric_reporters import BaseReporter, ConsoleReporter, PrometheusReporter
    from moleculerpy.metric_reporters import MetricRegistry

    registry = MetricRegistry()
    registry.add_reporter(ConsoleReporter(interval=5))
"""

from moleculerpy.metric_reporters.console import ConsoleReporter
from moleculerpy.metric_reporters.prometheus import PrometheusReporter
from moleculerpy.metric_reporters.registry import MetricRegistry
from moleculerpy.metric_reporters.reporter import BaseReporter, resolve_reporter

__all__ = [
    "BaseReporter",
    "ConsoleReporter",
    "MetricRegistry",
    "PrometheusReporter",
    "resolve_reporter",
]
