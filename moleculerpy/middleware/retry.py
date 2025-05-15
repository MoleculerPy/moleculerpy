"""Retry Middleware for the MoleculerPy framework.

This module provides automatic retry functionality with exponential backoff
for failed remote action calls. It's designed to handle transient failures
in distributed systems.

Example usage:
    from moleculerpy import ServiceBroker, Settings
    from moleculerpy.middleware import RetryMiddleware

    settings = Settings(
        middlewares=[
            RetryMiddleware(
                max_retries=3,
                base_delay=0.5,
                max_delay=10.0,
                exponential_base=2.0,
            )
        ]
    )

    broker = ServiceBroker(id="my-node", settings=settings)
"""

from __future__ import annotations

import asyncio
import random
from typing import TYPE_CHECKING, Any, ClassVar

from moleculerpy.errors import MoleculerError, RemoteCallError
from moleculerpy.middleware.base import HandlerType, Middleware

if TYPE_CHECKING:
    from moleculerpy.middleware.metrics import Counter, Histogram, MetricRegistry


class RetryMiddleware(Middleware):
    """Middleware that automatically retries failed remote action calls.

    Uses exponential backoff with optional jitter to prevent thundering herd
    problems when multiple clients retry simultaneously.

    Attributes:
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Initial delay between retries in seconds (default: 0.5)
        max_delay: Maximum delay cap in seconds (default: 30.0)
        exponential_base: Base for exponential backoff calculation (default: 2.0)
        jitter: Whether to add random jitter to delays (default: True)
        retryable_errors: Set of exception types that should trigger retries
    """

    DEFAULT_RETRYABLE_ERRORS: ClassVar[set[type[Exception]]] = {
        asyncio.TimeoutError,
        RemoteCallError,
        ConnectionError,
        OSError,
    }

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 0.5,
        max_delay: float = 30.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retryable_errors: set[type[Exception]] | None = None,
        registry: MetricRegistry | None = None,
    ) -> None:
        """Initialize the RetryMiddleware.

        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Initial delay between retries in seconds
            max_delay: Maximum delay cap in seconds
            exponential_base: Base for exponential backoff (e.g., 2.0 means delays double)
            jitter: Whether to add random jitter (0-25%) to prevent thundering herd
            retryable_errors: Set of exception types to retry on. If None, uses defaults.
            registry: Optional MetricRegistry for collecting retry metrics
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retryable_errors = retryable_errors or self.DEFAULT_RETRYABLE_ERRORS
        self.registry = registry

        # Metrics (created when broker starts)
        self._retry_total: Counter | None = None
        self._retry_attempts: Histogram | None = None

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate the delay before the next retry attempt.

        Uses exponential backoff: delay = base_delay * (exponential_base ^ attempt)
        With optional jitter to prevent thundering herd.

        Args:
            attempt: The current attempt number (0-indexed)

        Returns:
            Delay in seconds before next retry
        """
        # Exponential backoff
        delay = self.base_delay * (self.exponential_base**attempt)

        # Cap at max_delay
        delay = min(delay, self.max_delay)

        # Add jitter (0-25% of delay)
        if self.jitter:
            jitter_amount = delay * 0.25 * random.random()
            delay += jitter_amount

        return delay

    def _is_retryable(self, error: Exception) -> bool:
        """Check if an error should trigger a retry.

        Uses the MoleculerError.retryable flag when available, otherwise
        falls back to checking against the configured retryable error types.

        Args:
            error: The exception that was raised

        Returns:
            True if the error is retryable, False otherwise
        """
        # First, check if error has retryable flag (MoleculerError hierarchy)
        if isinstance(error, MoleculerError):
            return error.retryable

        # Fallback to type-based check for non-Moleculer errors
        return any(isinstance(error, err_type) for err_type in self.retryable_errors)

    def broker_created(self, broker: Any) -> None:
        """Initialize metrics when broker is created.

        Args:
            broker: The broker instance
        """
        if self.registry is None:
            return

        # Create retry metrics
        self._retry_total = self.registry.counter(
            "moleculer_retry_total",
            "Total number of retry attempts",
            label_names=("action", "attempt"),
        )

        self._retry_attempts = self.registry.histogram(
            "moleculer_retry_attempts",
            "Distribution of retry attempts per request",
            label_names=("action",),
            buckets=(1, 2, 3, 4, 5, 10),
        )

    def _get_max_retries_for_action(self, action: Any) -> int:
        """Get the max retries for a specific action.

        Per-action retry settings take precedence over middleware defaults.

        Args:
            action: The action endpoint object

        Returns:
            Maximum number of retries for this action
        """
        # Check if action has custom retries setting
        action_retries = getattr(action, "retries", None)
        if action_retries is not None:
            return int(action_retries)
        return self.max_retries

    async def remote_action(self, next_handler: HandlerType[Any], action: Any) -> HandlerType[Any]:
        """Hook for remote action middleware processing with retry logic.

        Wraps the remote action handler to automatically retry on transient failures.

        Args:
            next_handler: The next handler in the chain
            action: The action object being called

        Returns:
            A wrapped handler that implements retry logic
        """
        max_retries = self._get_max_retries_for_action(action)

        async def handler(ctx: Any) -> Any:
            action_name = getattr(action, "name", "unknown")
            last_error: Exception | None = None
            total_attempts = 0

            for attempt in range(max_retries + 1):
                total_attempts = attempt + 1

                try:
                    result = await next_handler(ctx)

                    # Record successful attempt in histogram
                    if self._retry_attempts and attempt > 0:
                        self._retry_attempts.observe(
                            total_attempts,
                            labels={"action": action_name},
                        )

                    return result

                except asyncio.CancelledError:
                    # Never retry on cancellation - propagate immediately
                    raise

                except Exception as e:
                    last_error = e

                    # Check if we should retry
                    if not self._is_retryable(e):
                        raise

                    # Check if we have retries left
                    if attempt >= max_retries:
                        # Record final failed attempt
                        if self._retry_attempts:
                            self._retry_attempts.observe(
                                total_attempts,
                                labels={"action": action_name},
                            )
                        raise

                    # Record retry attempt in counter
                    if self._retry_total:
                        self._retry_total.inc(
                            labels={
                                "action": action_name,
                                "attempt": str(attempt + 1),
                            }
                        )

                    # Calculate delay and wait
                    delay = self._calculate_delay(attempt)

                    # Log retry attempt (if logger available in context)
                    if hasattr(ctx, "broker") and hasattr(ctx.broker, "logger"):
                        ctx.broker.logger.warning(
                            f"Retry {attempt + 1}/{max_retries} for {action.name} "
                            f"after {delay:.2f}s due to: {e}"
                        )

                    await asyncio.sleep(delay)

            # Should not reach here, but just in case
            if last_error:
                raise last_error
            raise RuntimeError("Retry loop exited unexpectedly")

        return handler


class RetryError(Exception):
    """Exception raised when all retry attempts have been exhausted.

    Attributes:
        original_error: The last error that caused the retry to fail
        attempts: Number of attempts made before giving up
        action_name: Name of the action that failed
    """

    def __init__(
        self,
        message: str,
        original_error: Exception,
        attempts: int,
        action_name: str,
    ) -> None:
        super().__init__(message)
        self.original_error = original_error
        self.attempts = attempts
        self.action_name = action_name

    def __str__(self) -> str:
        return (
            f"RetryError: {self.action_name} failed after {self.attempts} attempts. "
            f"Last error: {self.original_error}"
        )
