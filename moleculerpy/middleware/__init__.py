"""MoleculerPy middleware module.

This module provides middleware implementations for the MoleculerPy framework,
including resilience patterns like retry, circuit breaker, bulkhead, and fallback.

Available middlewares:
    - Middleware: Base middleware class with all hooks
    - RetryMiddleware: Automatic retry with exponential backoff
    - CircuitBreakerMiddleware: Circuit breaker pattern for fault tolerance
    - TimeoutMiddleware: Per-action timeout enforcement
    - BulkheadMiddleware: Concurrency limiting with queue
    - FallbackMiddleware: Graceful degradation on failure
    - MetricsMiddleware: Prometheus-compatible request/event metrics

Example usage:
    from moleculerpy import ServiceBroker, Settings
    from moleculerpy.middleware import (
        RetryMiddleware,
        CircuitBreakerMiddleware,
        BulkheadMiddleware,
        FallbackMiddleware,
        MetricsMiddleware,
    )

    # Create metrics middleware for Prometheus export
    metrics = MetricsMiddleware()

    settings = Settings(
        middlewares=[
            metrics,  # Should be first to capture all requests
            RetryMiddleware(max_retries=3, base_delay=0.5),
            CircuitBreakerMiddleware(threshold=0.5, window_time=60),
            BulkheadMiddleware(concurrency=3, max_queue_size=10),
            FallbackMiddleware(),
        ]
    )

    broker = ServiceBroker(id="my-node", settings=settings)

    # Export Prometheus metrics
    prometheus_output = metrics.registry.to_prometheus()
"""

# Action hook middleware (before/after/error hooks)
from moleculerpy.middleware.action_hook import ActionHookMiddleware

# Action logger middleware (action call debugging)
from moleculerpy.middleware.action_logger import ActionLoggerMiddleware
from moleculerpy.middleware.base import Middleware

# Bulkhead middleware (concurrency limiting)
from moleculerpy.middleware.bulkhead import BulkheadConfig, BulkheadMiddleware

# Resilience middlewares
from moleculerpy.middleware.circuit_breaker import (
    CircuitBreakerError,
    CircuitBreakerMiddleware,
    CircuitState,
)

# Compression middleware (message compression)
from moleculerpy.middleware.compression import CompressionMethod, CompressionMiddleware

# Context tracker middleware (graceful shutdown)
from moleculerpy.middleware.context_tracker import (
    ContextTrackerMiddleware,
    GracefulStopTimeoutError,
)

# Debounce middleware (event debouncing)
from moleculerpy.middleware.debounce import DebounceMiddleware

# Encryption middleware (AES encryption)
from moleculerpy.middleware.encryption import EncryptionMiddleware

# Error handler middleware (centralized error handling)
from moleculerpy.middleware.error_handler import ErrorHandlerMiddleware

# Fallback middleware (graceful degradation)
from moleculerpy.middleware.fallback import FallbackMiddleware

# Middleware handler for registration-time wrapping
from moleculerpy.middleware.handler import HandlerType, MiddlewareHandler

# Hot reload middleware (development hot reloading)
from moleculerpy.middleware.hot_reload import HotReloadMiddleware

# Metrics middleware (Prometheus-compatible)
from moleculerpy.middleware.metrics import (
    Counter,
    Gauge,
    Histogram,
    MetricRegistry,
    MetricsMiddleware,
    Timer,
)
from moleculerpy.middleware.retry import RetryError, RetryMiddleware

# Throttle middleware (event rate limiting)
from moleculerpy.middleware.throttle import ThrottleMiddleware

# Timeout middleware
from moleculerpy.middleware.timeout import RequestTimeoutError, TimeoutMiddleware

# Tracing middleware (distributed tracing)
from moleculerpy.middleware.tracing import TracingMiddleware

# Transit logger middleware (debugging)
from moleculerpy.middleware.transit_logger import TransitLoggerMiddleware

# Validator middleware (parameter validation)
from moleculerpy.middleware.validator import ValidationRule, ValidatorMiddleware, validate_params

__all__ = [  # noqa: RUF022
    # Base
    "Middleware",
    "MiddlewareHandler",
    "HandlerType",
    # Retry
    "RetryMiddleware",
    "RetryError",
    # Circuit Breaker
    "CircuitBreakerMiddleware",
    "CircuitBreakerError",
    "CircuitState",
    # Timeout
    "TimeoutMiddleware",
    "RequestTimeoutError",
    # Bulkhead
    "BulkheadMiddleware",
    "BulkheadConfig",
    # Fallback
    "FallbackMiddleware",
    # Metrics
    "MetricsMiddleware",
    "MetricRegistry",
    "Counter",
    "Gauge",
    "Histogram",
    "Timer",
    # Tracing
    "TracingMiddleware",
    # Error Handler
    "ErrorHandlerMiddleware",
    # Context Tracker
    "ContextTrackerMiddleware",
    "GracefulStopTimeoutError",
    # Action Hook
    "ActionHookMiddleware",
    # Debounce
    "DebounceMiddleware",
    # Hot Reload
    "HotReloadMiddleware",
    # Validator
    "ValidatorMiddleware",
    "ValidationRule",
    "validate_params",
    # Throttle
    "ThrottleMiddleware",
    # Compression
    "CompressionMethod",
    "CompressionMiddleware",
    # Encryption
    "EncryptionMiddleware",
    # Transit Logger
    "TransitLoggerMiddleware",
    # Action Logger
    "ActionLoggerMiddleware",
]
