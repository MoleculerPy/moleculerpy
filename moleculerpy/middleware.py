"""
Middleware Module for the MoleculerPy framework.

This module re-exports the Middleware base class from moleculerpy.middleware.base
for backwards compatibility. New code should import from moleculerpy.middleware.

For resilience middlewares (Retry, CircuitBreaker), import from:
    from moleculerpy.middleware import RetryMiddleware, CircuitBreakerMiddleware
"""

# Re-export for backwards compatibility
from moleculerpy.middleware.base import HandlerType, Middleware

__all__ = ["HandlerType", "Middleware"]
