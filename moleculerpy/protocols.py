"""Centralized Protocol types for MoleculerPy framework.

This module provides all Protocol classes (structural typing interfaces)
used throughout the MoleculerPy framework. Centralizing protocols here:
    1. Avoids circular import issues
    2. Provides single source of truth for type contracts
    3. Follows Moleculer.js/Go pattern of clean abstractions

Protocol classes define the structural interface that objects must satisfy,
enabling duck typing with static type checking.

Example usage:
    from moleculerpy.protocols import ContextProtocol, ActionProtocol

    def my_middleware(ctx: ContextProtocol, action: ActionProtocol) -> Any:
        timeout = action.timeout or ctx.meta.get("timeout", 30.0)
        ...

Moleculer.js Equivalent:
    - Action interface defined in src/registry/action-catalog.js
    - Context interface defined in src/context.js
    - Broker interface defined in src/service-broker.js

Moleculer-Go Equivalent:
    - moleculer.Action interface in moleculer.go
    - moleculer.Context interface in context/context.go
    - Strategy interface in strategy/strategy.go
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Protocol, runtime_checkable

__all__ = [  # noqa: RUF022
    # Core protocols
    "SettingsProtocol",
    "BrokerProtocol",
    "ContextProtocol",
    "CallOptionsProtocol",
    # Logger protocols
    "LoggerProtocol",
    "LoggerFactoryProtocol",
    # Registry protocols
    "ActionProtocol",
    "EventProtocol",
    "ServiceProtocol",
    "NodeProtocol",
    # Middleware protocols
    "MiddlewareProtocol",
    # Registry protocols
    "RegistryProtocol",
]


# ═══════════════════════════════════════════════════════════════════════════
# Core Protocols (Settings, Broker, Context)
# ═══════════════════════════════════════════════════════════════════════════


@runtime_checkable
class SettingsProtocol(Protocol):
    """Protocol for broker settings.

    Defines the minimal interface for settings objects that contain
    broker configuration. Used by middleware to access global defaults.

    Moleculer.js: ServiceBroker.options
    Moleculer-Go: Config struct

    Attributes:
        request_timeout: Default timeout for requests in seconds
    """

    request_timeout: float


@runtime_checkable
class BrokerProtocol(Protocol):
    """Protocol for service broker.

    Defines the minimal interface for broker objects. Used by middleware
    and context to access broker functionality without circular imports.

    Moleculer.js: ServiceBroker class
    Moleculer-Go: ServiceBroker struct

    Attributes:
        settings: Broker configuration object
        node_id: Unique identifier for this broker node
    """

    settings: SettingsProtocol
    node_id: str


@runtime_checkable
class CallOptionsProtocol(Protocol):
    """Protocol for call options passed to broker.call().

    Defines per-call options that override defaults.
    Timeout resolution: options.timeout > meta.timeout > action.timeout > default

    Moleculer.js: CallingOptions in broker.call()
    Moleculer-Go: CallOptions struct

    Attributes:
        timeout: Per-call timeout override (highest priority)
        retries: Number of retry attempts
        node_id: Target specific node (bypass load balancing)
    """

    timeout: float | None
    retries: int | None
    node_id: str | None


@runtime_checkable
class LoggerProtocol(Protocol):
    """Protocol for custom logger implementations.

    Defines the minimal interface for logger objects that can be used
    with MoleculerPy. Allows plugging in custom loggers (e.g., colored
    console output, file logging, structured logging).

    Moleculer.js: BaseLogger class in src/loggers/base.js
    Moleculer-Go: Logger interface

    Required methods (all must handle variadic args like structlog):
        - trace: Log trace-level messages (most verbose)
        - debug: Log debug-level messages
        - info: Log info-level messages (default)
        - warn/warning: Log warning-level messages
        - error: Log error-level messages
        - fatal: Log fatal-level messages (least verbose)

    Example implementation:
        class MyColoredLogger:
            def info(self, message: str, **kwargs) -> None:
                print(f"\\033[32mINFO\\033[0m {message}")
            def error(self, message: str, **kwargs) -> None:
                print(f"\\033[31mERROR\\033[0m {message}")
            # ... other methods

        settings = Settings(logger=MyColoredLogger())
        broker = ServiceBroker("my-node", settings=settings)
    """

    def trace(self, message: str, **kwargs: Any) -> None:
        """Log trace-level message (most verbose)."""
        ...

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug-level message."""
        ...

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info-level message."""
        ...

    def warn(self, message: str, **kwargs: Any) -> None:
        """Log warning-level message."""
        ...

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning-level message (alias for warn)."""
        ...

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error-level message."""
        ...

    def fatal(self, message: str, **kwargs: Any) -> None:
        """Log fatal-level message (most severe)."""
        ...

    def bind(self, **kwargs: Any) -> LoggerProtocol:
        """Create a new logger with additional context bound.

        This is used to create child loggers with preset context values
        like node_id, service name, etc.

        Args:
            **kwargs: Context values to bind (e.g., node="alpha-node")

        Returns:
            New logger instance with context bound
        """
        ...


@runtime_checkable
class LoggerFactoryProtocol(Protocol):
    """Protocol for logger factory.

    Defines the interface for factory objects that create loggers.
    Similar to Moleculer.js LoggerFactory pattern.

    Moleculer.js: LoggerFactory class in src/logger-factory.js

    Example implementation:
        class ColoredLoggerFactory:
            def __init__(self, node_id: str):
                self.node_id = node_id

            def get_logger(self, module: str) -> LoggerProtocol:
                return ColoredLogger(self.node_id, module)
    """

    def get_logger(self, module: str) -> LoggerProtocol:
        """Get or create a logger for a module.

        Args:
            module: Module name (e.g., "BROKER", "TRANSIT", "REGISTRY")

        Returns:
            Logger instance for the module
        """
        ...


@runtime_checkable
class ContextProtocol(Protocol):
    """Protocol for request/event context.

    Defines the minimal interface for context objects passed to handlers.
    Context carries request metadata, parameters, and broker reference.

    Moleculer.js: Context class in src/context.js
    Moleculer-Go: BrokerContext interface

    Attributes:
        meta: Dictionary containing request metadata (timeout, caller, etc.)
        params: Request parameters
        broker: Reference to the broker (optional, may be None)
        options: Call options (optional)

    Key meta fields:
        - timeout: Per-call timeout override
        - caller: Name of the calling action
        - requestID: Distributed trace ID
        - parentID: Parent span ID for tracing
    """

    meta: dict[str, Any]

    @property
    def broker(self) -> BrokerProtocol | None:
        """The broker instance, if available."""
        ...

    @property
    def params(self) -> dict[str, Any]:
        """Request parameters."""
        ...

    @property
    def options(self) -> CallOptionsProtocol | None:
        """Call options (timeout, retries, etc.)."""
        ...


# ═══════════════════════════════════════════════════════════════════════════
# Registry Protocols (Action, Event, Service, Node)
# ═══════════════════════════════════════════════════════════════════════════


@runtime_checkable
class ActionProtocol(Protocol):
    """Protocol for action metadata.

    Defines the minimal interface for action objects in the registry.
    Used by middleware to access action-specific configuration.

    Moleculer.js: Action object in registry/action-catalog.js
    Moleculer-Go: moleculer.Action interface

    Attributes:
        name: Fully qualified action name (e.g., "users.create")
        handler: The action handler function
        timeout: Per-action timeout (seconds), None uses default
        retries: Number of retries for this action
        node_id: ID of the node hosting this action
        is_local: Whether action is local to this broker
    """

    name: str
    handler: Any  # Callable[..., Any] | None
    timeout: float | None
    retries: int | None
    node_id: str
    is_local: bool


@runtime_checkable
class EventProtocol(Protocol):
    """Protocol for event handler metadata.

    Defines the minimal interface for event objects in the registry.
    Used by middleware and event emitters.

    Moleculer.js: Event object in registry/event-catalog.js
    Moleculer-Go: Event in registry

    Attributes:
        name: Event name (e.g., "user.created")
        handler: The event handler function
        node_id: ID of the node hosting this handler
        is_local: Whether handler is local to this broker
    """

    name: str
    handler: Any  # Callable[..., Any] | None
    node_id: str
    is_local: bool


@runtime_checkable
class ServiceProtocol(Protocol):
    """Protocol for service metadata.

    Defines the minimal interface for service objects.
    Services contain actions and events.

    Moleculer.js: Service class in src/service.js
    Moleculer-Go: moleculer.Service interface

    Attributes:
        name: Service name (e.g., "users")
        version: Service version string
        node_id: ID of the node hosting this service
    """

    name: str
    version: str | None
    node_id: str


@runtime_checkable
class NodeProtocol(Protocol):
    """Protocol for cluster node metadata.

    Defines the minimal interface for node objects in the node catalog.
    Used by strategies for load balancing decisions.

    Moleculer.js: Node in registry/node.js
    Moleculer-Go: Node struct in registry/node.go

    Attributes:
        id: Unique node identifier
        available: Whether node is currently available
        cpu: Current CPU usage percentage (0-100)
        latency: Network latency to this node in milliseconds
        lastHeartbeatTime: Timestamp of last heartbeat
    """

    id: str
    available: bool
    cpu: int
    latency: float
    lastHeartbeatTime: float  # noqa: N815 - Moleculer protocol field name


# ═══════════════════════════════════════════════════════════════════════════
# Middleware Protocol
# ═══════════════════════════════════════════════════════════════════════════


@runtime_checkable
class MiddlewareProtocol(Protocol):
    """Protocol for middleware classes.

    Defines the interface for middleware that wraps action/event handlers.
    Middleware hooks are called at registration time (not call time).

    Moleculer.js: Middleware object with hook methods
    Moleculer-Go: moleculer.Middlewares map

    Hook methods (all optional):
        - local_action: Wrap local action handlers
        - remote_action: Wrap remote action handlers
        - local_event: Wrap local event handlers
    """

    async def local_action(
        self,
        next_handler: Callable[..., Awaitable[Any]],
        action: ActionProtocol,
    ) -> Callable[..., Awaitable[Any]]:
        """Wrap a local action handler.

        Args:
            next_handler: The next handler in the middleware chain
            action: Action metadata

        Returns:
            Wrapped handler function
        """
        ...

    async def remote_action(
        self,
        next_handler: Callable[..., Awaitable[Any]],
        action: ActionProtocol,
    ) -> Callable[..., Awaitable[Any]]:
        """Wrap a remote action handler.

        Args:
            next_handler: The next handler in the middleware chain
            action: Action metadata

        Returns:
            Wrapped handler function
        """
        ...

    async def local_event(
        self,
        next_handler: Callable[..., Awaitable[Any]],
        event: EventProtocol,
    ) -> Callable[..., Awaitable[Any]]:
        """Wrap a local event handler.

        Args:
            next_handler: The next handler in the middleware chain
            event: Event metadata

        Returns:
            Wrapped handler function
        """
        ...


# ═══════════════════════════════════════════════════════════════════════════
# Registry Protocol (for dependency injection)
# ═══════════════════════════════════════════════════════════════════════════


@runtime_checkable
class RegistryProtocol(Protocol):
    """Protocol for service registry.

    Defines the interface for registry objects that manage actions,
    events, and services. Used for dependency injection and testing.

    Moleculer.js: Registry class in src/registry/registry.js
    Moleculer-Go: ServiceRegistry struct

    Key methods:
        - get_action: Get action by name
        - get_all_actions: Get all endpoints for an action
        - get_all_events: Get all handlers for an event
        - remove_actions_by_node: Cascade cleanup on disconnect
        - remove_events_by_node: Cascade cleanup on disconnect
    """

    def get_action(self, name: str) -> ActionProtocol | None:
        """Get action by name."""
        ...

    def get_all_actions(self, name: str) -> list[ActionProtocol]:
        """Get all action endpoints for a given name."""
        ...

    def get_all_events(self, name: str) -> list[EventProtocol]:
        """Get all event handlers for a given name."""
        ...

    def remove_actions_by_node(self, node_id: str) -> int:
        """Remove all actions belonging to a node.

        Called during cascade cleanup when a node disconnects.

        Args:
            node_id: ID of the disconnected node

        Returns:
            Number of actions removed
        """
        ...

    def remove_events_by_node(self, node_id: str) -> int:
        """Remove all events belonging to a node.

        Called during cascade cleanup when a node disconnects.

        Args:
            node_id: ID of the disconnected node

        Returns:
            Number of events removed
        """
        ...
