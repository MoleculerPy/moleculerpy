"""Colored console logger for MoleculerPy cluster examples.

Based on moleculerpy-repl logger patterns, provides beautiful colored output
for microservices debugging and monitoring.

Usage:
    from colored_logger import LoggerFactory, LogLevel

    factory = LoggerFactory(node_id="alpha-node", level=LogLevel.DEBUG)
    logger = factory.get_logger("BROKER")

    logger.info("Service started")
    logger.debug("Processing request")
    logger.warn("Connection slow")
    logger.error("Request failed")
"""

from __future__ import annotations

import datetime
import sys
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, TextIO

# =============================================================================
# Log Levels (Moleculer.js compatible)
# =============================================================================


class LogLevel(IntEnum):
    """Log levels matching Moleculer.js."""

    FATAL = 0
    ERROR = 1
    WARN = 2
    INFO = 3
    DEBUG = 4
    TRACE = 5


# =============================================================================
# ANSI Color Codes
# =============================================================================


class Colors:
    """ANSI escape codes for terminal colors."""

    # Reset
    RESET = "\033[0m"

    # Regular colors
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    GRAY = "\033[90m"

    # Bold colors
    BOLD = "\033[1m"
    BOLD_RED = "\033[1;31m"
    BOLD_GREEN = "\033[1;32m"
    BOLD_YELLOW = "\033[1;33m"
    BOLD_BLUE = "\033[1;34m"
    BOLD_MAGENTA = "\033[1;35m"
    BOLD_CYAN = "\033[1;36m"

    # Background colors
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"

    # Dim
    DIM = "\033[2m"


# Moleculer.js style level colors
LEVEL_COLORS = {
    LogLevel.TRACE: Colors.GRAY,
    LogLevel.DEBUG: Colors.MAGENTA,
    LogLevel.INFO: Colors.GREEN,
    LogLevel.WARN: Colors.YELLOW,
    LogLevel.ERROR: Colors.RED,
    LogLevel.FATAL: Colors.BG_RED + Colors.WHITE,
}

LEVEL_NAMES = {
    LogLevel.TRACE: "TRACE",
    LogLevel.DEBUG: "DEBUG",
    LogLevel.INFO: "INFO",
    LogLevel.WARN: "WARN",
    LogLevel.ERROR: "ERROR",
    LogLevel.FATAL: "FATAL",
}

# Deterministic colors for modules (based on name hash)
MODULE_COLORS = [
    Colors.YELLOW,
    Colors.BOLD_YELLOW,
    Colors.CYAN,
    Colors.BOLD_CYAN,
    Colors.GREEN,
    Colors.BOLD_GREEN,
    Colors.MAGENTA,
    Colors.BOLD_MAGENTA,
    Colors.BLUE,
    Colors.BOLD_BLUE,
]


def get_module_color(module: str) -> str:
    """Get deterministic color for a module name."""
    hash_value = sum(ord(c) for c in module)
    return MODULE_COLORS[hash_value % len(MODULE_COLORS)]


# =============================================================================
# Logger Configuration
# =============================================================================


@dataclass
class LoggerConfig:
    """Configuration for colored logger."""

    node_id: str = ""
    level: LogLevel = LogLevel.INFO
    colors: bool = True
    show_timestamp: bool = True
    timestamp_format: str = "iso"  # "iso" or "short"


# =============================================================================
# Colored Logger
# =============================================================================


class Logger:
    """Colored console logger with Moleculer.js style output.

    Features:
        - ANSI colored output by log level
        - Module-based coloring (deterministic)
        - ISO or short timestamp formats
        - Moleculer.js compatible log format

    Example output:
        [2026-01-30T12:34:56.789Z] INFO  alpha-node/BROKER: Service started
        [2026-01-30T12:34:56.790Z] DEBUG alpha-node/TRANSIT: Sending HEARTBEAT
    """

    def __init__(self, module: str, config: LoggerConfig | None = None) -> None:
        self.module = module.upper()
        self.config = config or LoggerConfig()
        self._module_color = get_module_color(self.module)

    def _get_timestamp(self) -> str:
        """Get formatted timestamp."""
        now = datetime.datetime.now(datetime.UTC)
        if self.config.timestamp_format == "short":
            return now.strftime("%H:%M:%S.%f")[:-3] + "Z"
        return now.isoformat(timespec="milliseconds") + "Z"

    def _format_message(self, level: LogLevel, message: str, **kwargs: Any) -> str:
        """Format log message with colors."""
        parts = []

        # Timestamp
        if self.config.show_timestamp:
            timestamp = self._get_timestamp()
            if self.config.colors:
                parts.append(f"{Colors.GRAY}[{timestamp}]{Colors.RESET}")
            else:
                parts.append(f"[{timestamp}]")

        # Level
        level_name = LEVEL_NAMES.get(level, "INFO")
        if self.config.colors:
            level_color = LEVEL_COLORS.get(level, Colors.WHITE)
            parts.append(f"{level_color}{level_name:<5}{Colors.RESET}")
        else:
            parts.append(f"{level_name:<5}")

        # Node/Module
        node_module = f"{self.config.node_id}/{self.module}" if self.config.node_id else self.module
        if self.config.colors:
            parts.append(f"{self._module_color}{node_module}{Colors.RESET}:")
        else:
            parts.append(f"{node_module}:")

        # Message
        parts.append(message)

        # Extra kwargs
        if kwargs:
            extras = " ".join(f"{k}={v}" for k, v in kwargs.items())
            if self.config.colors:
                parts.append(f"{Colors.DIM}{extras}{Colors.RESET}")
            else:
                parts.append(extras)

        return " ".join(parts)

    def _log(self, level: LogLevel, message: str, *args: Any, **kwargs: Any) -> None:
        """Internal log method.

        Supports both printf-style formatting (message % args) and
        keyword arguments for extra context.
        """
        if level.value > self.config.level.value:
            return

        # Support printf-style formatting: logger.info("msg %s", arg)
        if args:
            try:
                message = message % args
            except (TypeError, ValueError):
                # If formatting fails, just append args
                message = f"{message} {' '.join(str(a) for a in args)}"

        formatted = self._format_message(level, message, **kwargs)

        # Write to stderr for errors, stdout for others
        stream: TextIO = sys.stderr if level <= LogLevel.ERROR else sys.stdout
        print(formatted, file=stream, flush=True)

    def trace(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log trace-level message (most verbose)."""
        self._log(LogLevel.TRACE, message, *args, **kwargs)

    def debug(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log debug-level message."""
        self._log(LogLevel.DEBUG, message, *args, **kwargs)

    def info(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log info-level message."""
        self._log(LogLevel.INFO, message, *args, **kwargs)

    def warn(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log warning-level message."""
        self._log(LogLevel.WARN, message, *args, **kwargs)

    def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log warning-level message (alias)."""
        self.warn(message, *args, **kwargs)

    def error(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log error-level message."""
        self._log(LogLevel.ERROR, message, *args, **kwargs)

    def fatal(self, message: str, *args: Any, **kwargs: Any) -> None:
        """Log fatal-level message (most severe)."""
        self._log(LogLevel.FATAL, message, *args, **kwargs)

    def bind(self, **kwargs: Any) -> Logger:
        """Create a new logger with additional context.

        This is for compatibility with structlog-style logging.
        Creates a new logger instance with modified config.

        Args:
            **kwargs: Context values (e.g., node="alpha", service="TRANSIT")

        Returns:
            New Logger instance
        """
        new_config = LoggerConfig(
            node_id=kwargs.get("node", self.config.node_id),
            level=self.config.level,
            colors=self.config.colors,
            show_timestamp=self.config.show_timestamp,
            timestamp_format=self.config.timestamp_format,
        )
        new_module = kwargs.get("service", self.module)
        return Logger(new_module, new_config)


# =============================================================================
# Logger Factory (Moleculer.js LoggerFactory pattern)
# =============================================================================


class LoggerFactory:
    """Factory for creating colored loggers with shared config.

    Implements MoleculerPy LoggerFactoryProtocol for seamless integration.

    Usage:
        factory = LoggerFactory(node_id="alpha-node", level=LogLevel.DEBUG)
        broker_log = factory.get_logger("BROKER")
        transit_log = factory.get_logger("TRANSIT")

    With MoleculerPy Settings:
        from moleculerpy.settings import Settings
        from moleculerpy.broker import ServiceBroker

        factory = LoggerFactory(node_id="alpha-node", level=LogLevel.INFO)
        settings = Settings(logger_factory=factory)
        broker = ServiceBroker("alpha-node", settings=settings)
    """

    def __init__(
        self,
        node_id: str = "",
        level: LogLevel = LogLevel.INFO,
        colors: bool = True,
        show_timestamp: bool = True,
        timestamp_format: str = "iso",
    ) -> None:
        """Initialize logger factory.

        Args:
            node_id: Node identifier to include in logs
            level: Minimum log level to display
            colors: Enable ANSI color output
            show_timestamp: Include timestamp in logs
            timestamp_format: "iso" (full) or "short" (HH:MM:SS only)
        """
        self.config = LoggerConfig(
            node_id=node_id,
            level=level,
            colors=colors,
            show_timestamp=show_timestamp,
            timestamp_format=timestamp_format,
        )
        self._loggers: dict[str, Logger] = {}

    def get_logger(self, module: str) -> Logger:
        """Get or create a logger for a module.

        Args:
            module: Module name (e.g., "BROKER", "TRANSIT", "REGISTRY")

        Returns:
            Colored Logger instance for the module
        """
        module_upper = module.upper()
        if module_upper not in self._loggers:
            self._loggers[module_upper] = Logger(module_upper, self.config)
        return self._loggers[module_upper]


# =============================================================================
# Standalone colored logger for simple use cases
# =============================================================================


def create_colored_logger(
    node_id: str,
    module: str = "APP",
    level: LogLevel = LogLevel.INFO,
    colors: bool = True,
) -> Logger:
    """Create a standalone colored logger.

    Convenience function for simple use cases where a full factory
    is not needed.

    Args:
        node_id: Node identifier
        module: Module name
        level: Log level
        colors: Enable colors

    Returns:
        Colored Logger instance
    """
    config = LoggerConfig(node_id=node_id, level=level, colors=colors)
    return Logger(module, config)


# =============================================================================
# Demo
# =============================================================================


if __name__ == "__main__":
    # Demo all log levels
    print("\n=== Colored Logger Demo ===\n")

    factory = LoggerFactory(node_id="demo-node", level=LogLevel.TRACE)

    broker_log = factory.get_logger("BROKER")
    transit_log = factory.get_logger("TRANSIT")
    registry_log = factory.get_logger("REGISTRY")

    broker_log.info("Service broker started")
    broker_log.debug("Initializing registry")
    transit_log.info("Connected to NATS")
    transit_log.debug("Sending HEARTBEAT")
    registry_log.info("Registered action: users.create")
    registry_log.warn("Service 'users' already registered")
    transit_log.error("Connection lost to nats://localhost:4222")
    broker_log.fatal("Unrecoverable error!")

    print("\n=== With short timestamps ===\n")

    factory2 = LoggerFactory(
        node_id="alpha-node",
        level=LogLevel.DEBUG,
        timestamp_format="short",
    )

    log = factory2.get_logger("FLAKY")
    log.info("Processing request")
    log.debug("Attempt 1/3")
    log.warn("Retry 2/3 after 0.05s")
    log.info("Request succeeded", attempts=3)
