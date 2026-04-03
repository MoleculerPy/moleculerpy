"""MoleculerPy - Fast, modern microservices framework for Python.

A Python port of the Moleculer.js microservices framework,
providing service discovery, load balancing, and inter-service communication.

Website: https://moleculerpy.services
Documentation: https://moleculerpy.services/docs
"""

from importlib.metadata import PackageNotFoundError, version

from .broker import Broker, ServiceBroker
from .context import Context
from .decorators import action, event
from .domain_types import ActionName, ContextID, EventName, NodeID, RequestID, ServiceName
from .errors import (
    MaxCallLevelError,
    MoleculerClientError,
    MoleculerError,
    MoleculerRetryableError,
    QueueIsFullError,
    RequestRejectedError,
    RequestTimeoutError,
    SerializationError,
    ServiceNotAvailableError,
    ServiceNotFoundError,
    ValidationError,
)
from .lifecycle import Lifecycle
from .metrics import (
    CpuMetrics,
    MemoryMetrics,
    MetricsCollector,
    SystemMetrics,
    get_cpu_usage,
    get_memory_usage,
    get_static_metrics,
    get_system_metrics,
)
from .serializers import BaseSerializer, JsonSerializer, MsgPackSerializer, resolve_serializer
from .service import Service
from .settings import Settings, SettingsValidationError
from .stream import AsyncStream, StreamError
from .tracing import (
    BaseTraceExporter,
    ConsoleExporter,
    EventExporter,
    Span,
    SpanLog,
    Tracer,
    TracerOptions,
)

try:
    __version__ = version("moleculerpy")
except PackageNotFoundError:
    __version__ = "0.14.7"

__all__ = [  # noqa: RUF022
    # Core
    "ServiceBroker",
    "Broker",
    "Service",
    "Context",
    "Lifecycle",
    "Settings",
    "SettingsValidationError",
    "NodeID",
    "ServiceName",
    "ActionName",
    "EventName",
    "ContextID",
    "RequestID",
    # Decorators
    "action",
    "event",
    # Errors
    "MoleculerError",
    "MoleculerRetryableError",
    "MoleculerClientError",
    "ServiceNotFoundError",
    "ServiceNotAvailableError",
    "RequestTimeoutError",
    "RequestRejectedError",
    "QueueIsFullError",
    "ValidationError",
    "MaxCallLevelError",
    "SerializationError",
    # Serializers
    "BaseSerializer",
    "JsonSerializer",
    "MsgPackSerializer",
    "resolve_serializer",
    # Streaming
    "AsyncStream",
    "StreamError",
    # Metrics (Phase 5.1)
    "CpuMetrics",
    "MemoryMetrics",
    "SystemMetrics",
    "MetricsCollector",
    "get_cpu_usage",
    "get_memory_usage",
    "get_system_metrics",
    "get_static_metrics",
    # Tracing
    "Span",
    "SpanLog",
    "Tracer",
    "TracerOptions",
    "BaseTraceExporter",
    "ConsoleExporter",
    "EventExporter",
]
