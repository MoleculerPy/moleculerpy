from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from .cacher.base import BaseCacher, CacherOptions
    from .protocols import LoggerFactoryProtocol, LoggerProtocol


class SettingsValidationError(ValueError):
    """Raised when Settings validation fails."""

    pass


class Settings:
    """Configuration settings for the MoleculerPy broker.

    Attributes:
        transporter: Transport protocol URL (e.g., 'nats://localhost:4222')
        serializer: Serialization format for messages
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Log output format (PLAIN or JSON)
        logger: Custom logger instance (overrides log_level/log_format)
        logger_factory: Custom logger factory for creating module loggers
        middlewares: List of middleware functions to apply
        request_timeout: Default timeout for remote action calls in seconds
        graceful_stop_timeout: Optional global timeout for broker shutdown.
            If None, waits indefinitely for graceful stop.
        heartbeat_interval: Interval between heartbeat messages in seconds
        heartbeat_timeout: Timeout for heartbeat responses in seconds
        max_call_level: Maximum nested call depth (0 = unlimited). Protects
            against infinite recursion in cyclic service calls.
        strategy: Load balancing strategy name ('RoundRobin', 'Random')
        prefer_local: If True, prefer local endpoints over remote ones

    Custom Logger Example:
        class ColoredLogger:
            def info(self, msg, **kw): print(f"\\033[32mINFO\\033[0m {msg}")
            def error(self, msg, **kw): print(f"\\033[31mERROR\\033[0m {msg}")
            def bind(self, **kw): return self  # Return self or new instance

        settings = Settings(logger=ColoredLogger())
        broker = ServiceBroker("my-node", settings=settings)

    Logger Factory Example:
        class ColoredLoggerFactory:
            def __init__(self, node_id: str):
                self.node_id = node_id

            def get_logger(self, module: str) -> ColoredLogger:
                return ColoredLogger(self.node_id, module)

        settings = Settings(logger_factory=ColoredLoggerFactory("alpha-node"))

    Raises:
        SettingsValidationError: If any setting has an invalid value
    """

    VALID_LOG_LEVELS: ClassVar[set[str]] = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    VALID_LOG_FORMATS: ClassVar[set[str]] = {"PLAIN", "JSON"}
    VALID_SERIALIZERS: ClassVar[set[str]] = {"JSON", "MSGPACK"}
    VALID_STRATEGIES: ClassVar[set[str]] = {"ROUNDROBIN", "RANDOM", "CPUUSAGE", "SHARD", "LATENCY"}

    def __init__(
        self,
        transporter: str = "nats://localhost:4222",
        serializer: str = "JSON",
        log_level: str = "INFO",
        log_format: str = "PLAIN",
        logger: "LoggerProtocol | None" = None,
        logger_factory: "LoggerFactoryProtocol | None" = None,
        middlewares: list[Any] | None = None,
        request_timeout: float = 30.0,
        graceful_stop_timeout: float | None = None,
        heartbeat_interval: float = 5.0,
        heartbeat_timeout: float = 15.0,
        max_call_level: int = 0,
        strategy: str = "RoundRobin",
        prefer_local: bool = True,
        cacher: "bool | str | CacherOptions | BaseCacher | None" = None,
        namespace: str | None = None,
    ) -> None:
        self.transporter = transporter
        self.serializer = serializer
        self.log_level = log_level
        self.log_format = log_format
        self.logger = logger
        self.logger_factory = logger_factory
        self.middlewares = middlewares or []
        self.request_timeout = request_timeout
        self.graceful_stop_timeout = graceful_stop_timeout
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.max_call_level = max_call_level
        self.strategy = strategy
        self.prefer_local = prefer_local
        self.cacher = cacher
        self.namespace = namespace

        # Validate all settings
        self._validate()

    def _validate(self) -> None:
        """Validate all settings values.

        Raises:
            SettingsValidationError: If any validation fails
        """
        # Validate timeouts
        if self.request_timeout <= 0:
            raise SettingsValidationError(
                f"request_timeout must be positive, got {self.request_timeout}"
            )

        if self.graceful_stop_timeout is not None and self.graceful_stop_timeout <= 0:
            raise SettingsValidationError(
                "graceful_stop_timeout must be positive when provided, "
                f"got {self.graceful_stop_timeout}"
            )

        if self.heartbeat_interval <= 0:
            raise SettingsValidationError(
                f"heartbeat_interval must be positive, got {self.heartbeat_interval}"
            )

        if self.heartbeat_timeout <= 0:
            raise SettingsValidationError(
                f"heartbeat_timeout must be positive, got {self.heartbeat_timeout}"
            )

        if self.heartbeat_timeout <= self.heartbeat_interval:
            raise SettingsValidationError(
                f"heartbeat_timeout ({self.heartbeat_timeout}) must be greater than "
                f"heartbeat_interval ({self.heartbeat_interval})"
            )

        # Validate max_call_level
        if self.max_call_level < 0:
            raise SettingsValidationError(
                f"max_call_level must be non-negative, got {self.max_call_level}"
            )

        # Validate log_level
        if self.log_level.upper() not in self.VALID_LOG_LEVELS:
            raise SettingsValidationError(
                f"log_level must be one of {self.VALID_LOG_LEVELS}, got '{self.log_level}'"
            )

        # Validate log_format
        if self.log_format.upper() not in self.VALID_LOG_FORMATS:
            raise SettingsValidationError(
                f"log_format must be one of {self.VALID_LOG_FORMATS}, got '{self.log_format}'"
            )

        # Validate serializer
        if self.serializer.upper() not in self.VALID_SERIALIZERS:
            raise SettingsValidationError(
                f"serializer must be one of {self.VALID_SERIALIZERS}, got '{self.serializer}'"
            )

        # Validate transporter URL format
        if "://" not in self.transporter:
            raise SettingsValidationError(
                f"transporter must be a valid URL (e.g., 'nats://localhost:4222'), "
                f"got '{self.transporter}'"
            )

        # Validate strategy
        if self.strategy.upper() not in self.VALID_STRATEGIES:
            raise SettingsValidationError(
                f"strategy must be one of {self.VALID_STRATEGIES}, got '{self.strategy}'"
            )

    @property
    def default_timeout(self) -> float:
        """Alias for request_timeout for backwards compatibility."""
        return self.request_timeout
