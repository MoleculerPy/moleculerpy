"""Logging configuration and utilities for the MoleculerPy framework.

This module provides structured logging capabilities using structlog, with support
for both JSON and plain text formatting compatible with Moleculer.js conventions.

Custom Logger Support:
    MoleculerPy supports custom loggers via Settings.logger or Settings.logger_factory.
    Custom loggers must implement LoggerProtocol (info, warn, error, debug, bind methods).

    Example:
        class ColoredLogger:
            def info(self, msg, **kw): print(f"\\033[32mINFO\\033[0m {msg}")
            def bind(self, **kw): return self

        settings = Settings(logger=ColoredLogger())
        broker = ServiceBroker("my-node", settings=settings)
"""

from __future__ import annotations

import datetime
import logging
import sys
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from .protocols import LoggerProtocol

# Map string log levels to logging constants
LOG_LEVEL_MAP: dict[str, int] = {
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def get_parsed_log_level(log_level: str) -> int:
    """Parse a string log level and return the corresponding logging constant.

    Args:
        log_level: String representation of log level (e.g., "INFO", "DEBUG")

    Returns:
        The corresponding logging level constant, defaults to INFO if not found
    """
    return LOG_LEVEL_MAP.get(log_level.upper(), logging.INFO)


def moleculer_format_renderer(_logger: Any, _method_name: str, event_dict: dict[str, Any]) -> str:
    """Custom log formatter that renders logs in Moleculer.js compatible format.

    Args:
        _logger: The logger instance (unused but required by structlog)
        _method_name: The log method name (unused but required by structlog)
        event_dict: Dictionary containing log event data

    Returns:
        Formatted log string in Moleculer format
    """
    # Get current time and format it
    now = datetime.datetime.now(datetime.UTC)
    timestamp = now.isoformat(timespec="milliseconds") + "Z"
    level = event_dict.pop("level", "INFO").upper()
    node = event_dict.pop("node", "<unknown>")
    service = event_dict.pop("service", "<unspecified>")
    message = event_dict.pop("event", "")
    return f"[{timestamp}] {level:<5} {node}/{service}: {message}"


class LoggerAdapter:
    """Adapter that wraps custom logger to provide bind() support.

    This adapter ensures custom loggers work seamlessly with MoleculerPy's
    logger.bind(node=..., service=...) pattern even if they don't have
    native bind() support.

    Attributes:
        _logger: The wrapped custom logger
        _context: Bound context values (node, service, etc.)
    """

    def __init__(
        self,
        logger: LoggerProtocol,
        context: dict[str, Any] | None = None,
    ) -> None:
        self._logger = logger
        self._context = context or {}

    def bind(self, **kwargs: Any) -> LoggerAdapter:
        """Create new adapter with additional context bound.

        Args:
            **kwargs: Context values to bind

        Returns:
            New LoggerAdapter with merged context
        """
        # If the wrapped logger has bind(), try to use it
        if hasattr(self._logger, "bind"):
            try:
                bound_logger = self._logger.bind(**kwargs)
                return LoggerAdapter(bound_logger, {**self._context, **kwargs})
            except Exception:
                pass

        # Fallback: just merge context
        return LoggerAdapter(self._logger, {**self._context, **kwargs})

    def _format_message(self, message: str) -> str:
        """Format message with bound context."""
        # Simple format: prepend node/service if bound
        node = self._context.get("node", "")
        service = self._context.get("service", "")
        if node and service:
            return f"{node}/{service}: {message}"
        elif node:
            return f"{node}: {message}"
        elif service:
            return f"{service}: {message}"
        return message

    def trace(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log trace-level message."""
        formatted = self._format_with_args(message, args)
        if hasattr(self._logger, "trace"):
            self._logger.trace(self._format_message(formatted), **kwargs)
        else:
            self.debug(formatted, **kwargs)  # Fallback to debug

    def debug(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log debug-level message."""
        formatted = self._format_with_args(message, args)
        self._logger.debug(self._format_message(formatted), **kwargs)

    def info(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log info-level message."""
        formatted = self._format_with_args(message, args)
        self._logger.info(self._format_message(formatted), **kwargs)

    def warn(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log warning-level message."""
        formatted = self._format_with_args(message, args)
        if hasattr(self._logger, "warn"):
            self._logger.warn(self._format_message(formatted), **kwargs)
        elif hasattr(self._logger, "warning"):
            self._logger.warning(self._format_message(formatted), **kwargs)

    def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log warning-level message (alias)."""
        self.warn(message, *args, **kwargs)

    def error(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log error-level message."""
        formatted = self._format_with_args(message, args)
        self._logger.error(self._format_message(formatted), **kwargs)

    def fatal(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log fatal-level message."""
        formatted = self._format_with_args(message, args)
        if hasattr(self._logger, "fatal"):
            self._logger.fatal(self._format_message(formatted), **kwargs)
        else:
            self.error(formatted, **kwargs)  # Fallback to error

    def _format_with_args(self, message: str, args: tuple[Any, ...]) -> str:
        """Format message with printf-style args."""
        if not args:
            return message
        try:
            return message % args
        except (TypeError, ValueError):
            return f"{message} {' '.join(str(a) for a in args)}"


def get_logger(
    log_level: str | int,
    log_format: str = "PLAIN",
    custom_logger: LoggerProtocol | None = None,
) -> Any:
    """Configure and return a structured logger instance.

    Args:
        log_level: Log level as string (e.g., "INFO") or logging constant
        log_format: Output format, either "JSON" or "PLAIN" (default)
        custom_logger: Optional custom logger implementation

    Returns:
        Logger instance (custom wrapped in adapter, or structlog BoundLogger)

    Example with custom logger:
        class MyLogger:
            def info(self, msg, **kw): print(f"INFO: {msg}")
            def error(self, msg, **kw): print(f"ERROR: {msg}")
            def debug(self, msg, **kw): print(f"DEBUG: {msg}")

        logger = get_logger("INFO", custom_logger=MyLogger())
        logger.bind(node="alpha").info("Hello")  # "alpha: INFO: Hello"
    """
    # Use custom logger if provided
    if custom_logger is not None:
        return LoggerAdapter(custom_logger)

    # Default: use structlog
    if isinstance(log_level, str):
        parsed_level = get_parsed_log_level(log_level)
    else:
        parsed_level = log_level

    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=parsed_level)

    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
    ]

    if log_format == "JSON":
        processors.extend(
            [
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.format_exc_info,
                structlog.processors.JSONRenderer(),
            ]
        )
    else:
        processors.append(moleculer_format_renderer)

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(parsed_level),
        processors=processors,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger()
