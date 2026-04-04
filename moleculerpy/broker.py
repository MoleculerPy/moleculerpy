"""Service broker implementation for the MoleculerPy framework.

This module provides the main ServiceBroker class which orchestrates all aspects
of the microservices framework including service registration, discovery,
communication, and lifecycle management.
"""

import asyncio
import signal
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, cast

from .domain_types import ActionName, EventName

if TYPE_CHECKING:
    from .cacher.base import BaseCacher
    from .context import Context
    from .service import Service

from .discoverer import Discoverer
from .errors import (
    GracefulStopTimeoutError,
    MaxCallLevelError,
    MoleculerError,
    ServiceNotFoundError,
)
from .latency import LatencyMonitor
from .lifecycle import Lifecycle
from .local_bus import LocalBus
from .logger import get_logger
from .middleware.base import Middleware
from .middleware.handler import MiddlewareHandler
from .node import NodeCatalog
from .registry import Registry
from .settings import Settings
from .transit import Transit

SERVICE_WAIT_LOG_INTERVAL = 5.0


# For backwards compatibility, alias ServiceBroker as Broker
class ServiceBroker:
    """Main service broker for orchestrating microservices.

    The ServiceBroker is the central component that manages:
    - Service registration and discovery
    - Inter-service communication (calls, events, broadcasts)
    - Node topology and clustering
    - Middleware pipeline execution
    - Lifecycle management
    """

    def __init__(
        self,
        id: str,
        settings: Settings | None = None,
        version: str = "0.14.1",
        namespace: str = "default",
        lifecycle: Lifecycle | None = None,
        registry: Registry | None = None,
        node_catalog: NodeCatalog | None = None,
        transit: Transit | None = None,
        discoverer: Discoverer | None = None,
        middlewares: list["Middleware"] | None = None,
        cacher: "BaseCacher | None" = None,
    ) -> None:
        """Initialize the service broker.

        Args:
            id: Unique identifier for this node (Moleculer.js: nodeID)
            settings: Configuration settings
            version: MoleculerPy framework version
            namespace: Logical namespace for service isolation
            lifecycle: Custom lifecycle manager
            registry: Custom service registry
            node_catalog: Custom node catalog
            transit: Custom transit layer
            discoverer: Custom service discoverer
            middlewares: List of middleware instances
            cacher: Cache backend instance (MemoryCacher, RedisCacher, etc.)
        """
        # Moleculer.js compatibility: use nodeID as primary attribute name
        self.nodeID = id
        self.settings = settings or Settings()
        self.version = version
        self.namespace = namespace

        # Initialize local event bus for internal $ events (Moleculer.js compatible)
        self.local_bus = LocalBus()

        # Initialize middleware system
        self.middlewares = self._initialize_middlewares(middlewares)
        self.middleware_handler = MiddlewareHandler(self)

        # Initialize logger (supports custom loggers via Settings)
        self.logger = self._create_logger("BROKER")

        # Initialize core components
        self.lifecycle = lifecycle or Lifecycle(broker=self)
        self.registry = registry or Registry(
            node_id=self.nodeID,
            logger=self.logger,
            strategy=self.settings.strategy,
            prefer_local=self.settings.prefer_local,
        )
        self.node_catalog = node_catalog or NodeCatalog(
            logger=self.logger, node_id=self.nodeID, registry=self.registry, broker=self
        )
        self.transit = transit or Transit(
            settings=self.settings,
            node_id=self.nodeID,
            registry=self.registry,
            node_catalog=self.node_catalog,
            lifecycle=self.lifecycle,
            logger=self.logger,
        )
        self.discoverer = discoverer or Discoverer(broker=self)

        # Phase 5.2: Initialize latency monitor for PING/PONG protocol
        # Default: this node is master if it uses LatencyStrategy
        is_latency_master = self._is_latency_strategy()
        self.latency_monitor = LatencyMonitor(
            broker=self,
            is_master=is_latency_master,
            ping_interval=getattr(self.settings, "latency_ping_interval", 10.0),
            sample_count=getattr(self.settings, "latency_sample_count", 5),
        )

        # Wire latency monitor to transit for PING/PONG handling
        self.transit._latency_monitor = self.latency_monitor

        # Initialize cacher (from parameter or settings)
        self.cacher: BaseCacher | None = self._initialize_cacher(cacher)

        # Initialize pluggable validator
        from .validators import resolve_validator  # noqa: PLC0415

        self._validator = resolve_validator(getattr(self.settings, "validator", "default"))

        # Wrapped event methods (set during start() by middleware)
        self._wrapped_emit: (
            Callable[[str, dict[str, Any], dict[str, Any]], Awaitable[Any]] | None
        ) = None
        self._wrapped_broadcast: (
            Callable[[str, dict[str, Any], dict[str, Any]], Awaitable[list[Any]]] | None
        ) = None
        self._wrapped_broadcast_local: Callable[[str, Any], Awaitable[bool]] | None = None

        # Call middleware broker_created hooks
        self._call_middleware_hooks("broker_created", self, is_async=False)

    @property
    def serializer(self) -> Any:
        """Get the serializer from settings (Moleculer.js API compatibility).

        Returns:
            Serializer name (e.g., "JSON", "MessagePack")
        """
        return self.settings.serializer

    def _initialize_middlewares(self, middlewares: list["Middleware"] | None) -> list["Middleware"]:
        """Initialize middleware list from various sources.

        Args:
            middlewares: Directly provided middlewares

        Returns:
            List of middleware instances
        """
        if middlewares is not None:
            return middlewares
        elif hasattr(self.settings, "middlewares") and self.settings.middlewares:
            return self.settings.middlewares
        else:
            return []

    def _create_logger(self, module: str) -> Any:
        """Create a logger for a module using custom or default logger.

        Supports three logger sources (in priority order):
        1. Settings.logger_factory.get_logger(module) - Factory creates module loggers
        2. Settings.logger.bind(node=..., service=module) - Single custom logger
        3. Default structlog logger via get_logger()

        Args:
            module: Module name (e.g., "BROKER", "TRANSIT", "REGISTRY")

        Returns:
            Logger instance with context bound
        """
        # Priority 1: Use logger factory if available
        if hasattr(self.settings, "logger_factory") and self.settings.logger_factory:
            return self.settings.logger_factory.get_logger(module)

        # Priority 2: Use custom logger if available
        if hasattr(self.settings, "logger") and self.settings.logger:
            return get_logger(
                self.settings.log_level,
                self.settings.log_format,
                custom_logger=self.settings.logger,
            ).bind(node=self.nodeID, service=module, level=self.settings.log_level)

        # Priority 3: Default structlog logger
        return get_logger(self.settings.log_level, self.settings.log_format).bind(
            node=self.nodeID, service=module, level=self.settings.log_level
        )

    def _initialize_cacher(self, cacher: "BaseCacher | None") -> "BaseCacher | None":
        """Initialize cacher from parameter or settings.

        Resolves cacher configuration and initializes it with broker reference.
        Also registers caching middleware automatically.

        Args:
            cacher: Direct cacher instance or None

        Returns:
            Initialized cacher or None
        """
        from .cacher import BaseCacher  # noqa: PLC0415
        from .cacher import resolve as resolve_cacher  # noqa: PLC0415

        # Priority: direct parameter > settings.cacher
        cacher_config: Any = cacher
        if cacher_config is None and hasattr(self.settings, "cacher"):
            cacher_config = self.settings.cacher

        if cacher_config is None or cacher_config is False:
            return None

        # Resolve cacher from config
        resolved_cacher: BaseCacher | None
        if isinstance(cacher_config, BaseCacher):
            resolved_cacher = cacher_config
        else:
            # Resolve from string/dict/bool config
            resolved_cacher = resolve_cacher(cacher_config)
            if resolved_cacher is None:
                return None

        # Initialize cacher with broker reference
        resolved_cacher.init(self)

        # Auto-register caching middleware
        from .middleware.caching import CachingMiddleware  # noqa: PLC0415

        caching_mw = CachingMiddleware(resolved_cacher)
        self.middlewares.append(caching_mw)

        self.logger.info(f"Cacher initialized: {resolved_cacher.__class__.__name__}")

        return resolved_cacher

    def _call_middleware_hooks(
        self, hook_name: str, *args: Any, is_async: bool = True
    ) -> list[Any] | None:
        """Call middleware hooks with proper async handling.

        Args:
            hook_name: Name of the hook method to call
            *args: Arguments to pass to the hook
            is_async: Whether to handle async hooks

        Returns:
            List of coroutines if is_async=True, None otherwise
        """
        if is_async:
            coroutines = []
            for middleware in self.middlewares:
                hook = getattr(middleware, hook_name, None)
                if hook and callable(hook):
                    result = hook(*args)
                    if asyncio.iscoroutine(result):
                        coroutines.append(result)
            return coroutines if coroutines else None
        else:
            # Synchronous hooks
            for middleware in self.middlewares:
                hook = getattr(middleware, hook_name, None)
                if hook and callable(hook):
                    hook(*args)
            return None

    async def _execute_middleware_hooks(
        self, hook_name: str, *args: Any, reverse: bool = False
    ) -> None:
        """Execute middleware hooks.

        This method handles the common pattern of calling middleware hooks
        and preserves hook errors so lifecycle failures are visible to callers.

        Args:
            hook_name: Name of the hook method to call
            *args: Arguments to pass to the hook
            reverse: If True, execute hooks in reverse order (LIFO).
                    Use for stop/cleanup hooks to ensure proper dependency cleanup.
        """
        coroutines = self._call_middleware_hooks(hook_name, *args)
        if coroutines:
            if reverse:
                coroutines = list(reversed(coroutines))
            # Sequential execution — matches Moleculer.js callHandlers() which uses
            # reduce((p, fn) => p.then(() => fn(...))). Order matters because
            # middleware hooks have dependencies (e.g., ContextTracker must finish
            # before transport stops, HotReload starts watchers only after full init).
            for coro in coroutines:
                await coro

    def _is_latency_strategy(self) -> bool:
        """Check if this broker uses LatencyStrategy.

        Used to determine if this node should be latency master.

        Returns:
            True if registry uses LatencyStrategy
        """
        from .strategy.latency import LatencyStrategy  # noqa: PLC0415

        strategy = getattr(self.registry, "_strategy", None)
        return isinstance(strategy, LatencyStrategy)

    async def _wrap_event_methods(self) -> None:
        """Wrap emit and broadcast methods with middleware hooks.

        This method is called during broker start to wrap the core event
        emission methods with any middleware that defines emit/broadcast hooks.

        The wrapping creates a chain:
            MW1.emit -> MW2.emit -> ... -> _emit_impl

        This allows middleware to intercept, modify, or log event emissions.
        """

        # Create the base implementations
        async def emit_impl(event_name: str, params: dict[str, Any], opts: dict[str, Any]) -> Any:
            """Internal emit implementation."""
            meta = opts.get("meta", {})
            return await self._emit_core(event_name, params, meta)

        async def broadcast_impl(
            event_name: str, params: dict[str, Any], opts: dict[str, Any]
        ) -> list[Any]:
            """Internal broadcast implementation."""
            meta = opts.get("meta", {})
            groups = opts.get("groups")
            return await self._broadcast_core(event_name, params, meta, groups)

        async def broadcast_local_impl(event_name: str, payload: Any) -> bool:
            """Internal broadcast_local implementation."""
            return await self.local_bus.emit(event_name, payload)

        # Wrap with middleware in reverse order (last middleware wraps first)
        current_emit = emit_impl
        current_broadcast = broadcast_impl
        current_broadcast_local = broadcast_local_impl

        for middleware in reversed(self.middlewares):
            # Check if middleware has emit hook
            emit_hook = getattr(middleware, "emit", None)
            if emit_hook is not None and callable(emit_hook):
                # Check if it's not the base Middleware default
                if type(middleware).emit is not Middleware.emit:
                    outer_emit = current_emit
                    mw = middleware

                    async def wrapped_emit(
                        event_name: str,
                        params: dict[str, Any],
                        opts: dict[str, Any],
                        _next: Any = outer_emit,
                        _mw: Any = mw,
                    ) -> Any:
                        return await _mw.emit(_next, event_name, params, opts)

                    current_emit = wrapped_emit

            # Check if middleware has broadcast hook
            broadcast_hook = getattr(middleware, "broadcast", None)
            if broadcast_hook is not None and callable(broadcast_hook):
                # Check if it's not the base Middleware default
                if type(middleware).broadcast is not Middleware.broadcast:
                    outer_broadcast = current_broadcast
                    mw = middleware

                    async def wrapped_broadcast(
                        event_name: str,
                        params: dict[str, Any],
                        opts: dict[str, Any],
                        _next: Any = outer_broadcast,
                        _mw: Any = mw,
                    ) -> list[Any]:
                        return cast(list[Any], await _mw.broadcast(_next, event_name, params, opts))

                    current_broadcast = wrapped_broadcast

            # Check if middleware has broadcast_local hook
            broadcast_local_hook = getattr(middleware, "broadcast_local", None)
            if broadcast_local_hook is not None and callable(broadcast_local_hook):
                # Check if it's not the base Middleware default
                if type(middleware).broadcast_local is not Middleware.broadcast_local:
                    outer_broadcast_local = current_broadcast_local
                    mw = middleware

                    async def wrapped_broadcast_local(
                        event_name: str,
                        payload: Any,
                        _next: Any = outer_broadcast_local,
                        _mw: Any = mw,
                    ) -> bool:
                        return cast(bool, await _mw.broadcast_local(_next, event_name, payload))

                    current_broadcast_local = wrapped_broadcast_local

        self._wrapped_emit = current_emit
        self._wrapped_broadcast = current_broadcast
        self._wrapped_broadcast_local = current_broadcast_local
        self.logger.debug("Event methods wrapped with middleware hooks")

    async def _start_service(self, service: "Service") -> None:
        """Start a single registered service with dependency handling."""
        dependencies = getattr(service, "dependencies", None)
        if dependencies:
            self.logger.info(
                f"Service {service.name} waiting for dependencies: {', '.join(dependencies)}"
            )
            try:
                await self.wait_for_services(dependencies)
            except TimeoutError as e:
                self.logger.error(
                    f"Service {service.name} failed to start: dependency timeout - {e}"
                )
                raise

        if hasattr(service, "_call_lifecycle_hook"):
            try:
                await service._call_lifecycle_hook("started")
            except Exception as e:
                self.logger.error(f"Error in service {service.name} started hook: {e}")

        await self._execute_middleware_hooks("service_started", service)

    async def start(self) -> None:
        """Start the service broker and establish cluster connectivity."""
        self.logger.info(f"MoleculerPy v{self.version} is starting...")
        self.logger.info(f"Namespace: {self.namespace}")
        self.logger.info(f"Node ID: {self.nodeID}")
        self.logger.info(f"Transporter: {self.transit.transporter.name}")

        # Initialize validator with broker reference
        if self._validator is not None:
            self._validator.init(self)

        # Call broker_starting hooks BEFORE connecting
        await self._execute_middleware_hooks("broker_starting", self)

        # Wrap emit/broadcast with middleware hooks
        await self._wrap_event_methods()

        # Wrap transit.publish with middleware hooks (Moleculer.js pattern)
        self.transit._wrap_methods(self)

        # Connect to the cluster
        await self.transit.connect()

        # Set up balanced subscriptions if disable_balancer is enabled
        if self.settings.disable_balancer:
            await self.transit.make_balanced_subscriptions()

        # Start discoverer after transit is connected
        if hasattr(self.discoverer, "start"):
            await self.discoverer.start()

        # Start cacher (after transit so reconnect events work)
        if self.cacher:
            await self.cacher.start()

        # Phase 5.2: Start latency monitor for PING/PONG protocol
        if self.latency_monitor:
            await self.latency_monitor.start()
            # Wire LatencyStrategy to latency provider if used
            if self._is_latency_strategy():
                from .strategy.latency import LatencyStrategy  # noqa: PLC0415

                strategy = self.registry._strategy
                if isinstance(strategy, LatencyStrategy):
                    strategy.set_latency_provider(self.latency_monitor)

        # Register $node internal service (always available, Moleculer.js compatible).
        # Registered directly into registry without middleware lifecycle hooks since
        # this is a built-in introspection service, not a user service.
        from .internals import NodeInternalService  # noqa: PLC0415

        if "$node" not in self.registry.__services__:
            _node_svc = NodeInternalService()
            _node_svc.broker = self
            _node_svc.logger = self._create_logger("$node")
            self.registry.register(_node_svc)

        service_count = len(self.registry.__services__)
        self.logger.info(f"Service broker with {service_count} services started successfully")

        # Start all services in parallel (Moleculer.js-compatible Promise.all pattern).
        # TaskGroup cancels remaining services on first failure (stricter than JS Promise.all).
        # Re-raise the first sub-exception to match Moleculer.js Promise.all behavior.
        # Internal services (name starts with "$") are excluded from lifecycle hooks —
        # they are always-available introspection services, not user services.
        services = [
            svc for svc in self.registry.__services__.values() if not svc.name.startswith("$")
        ]
        if services:
            try:
                async with asyncio.TaskGroup() as tg:
                    for service in services:
                        tg.create_task(self._start_service(service))
            except ExceptionGroup as eg:
                raise eg.exceptions[0] from eg.exceptions[0]

        # Call broker_started hooks AFTER connecting
        await self._execute_middleware_hooks("broker_started", self)

        # Emit $broker.started internal event (Moleculer.js compatible)
        await self.broadcast_local("$broker.started", {})

    async def stop(self) -> None:
        """Stop the service broker and cleanup resources."""
        self.logger.info("Stopping service broker...")

        async def _stop_core() -> None:
            # Call broker_stopping hooks BEFORE disconnecting (reverse order for LIFO cleanup)
            await self._execute_middleware_hooks("broker_stopping", self, reverse=True)

            # Call stopped() lifecycle hooks on user services (reverse order).
            # Internal services (name starts with "$") are excluded.
            services = [
                svc for svc in self.registry.__services__.values() if not svc.name.startswith("$")
            ]
            for service in reversed(services):
                # Call service_stopping middleware hooks first
                await self._execute_middleware_hooks("service_stopping", service, reverse=True)

                if hasattr(service, "_call_lifecycle_hook"):
                    try:
                        await service._call_lifecycle_hook("stopped")
                    except Exception as e:
                        self.logger.error(f"Error in service {service.name} stopped hook: {e}")

                # Call service_stopped middleware hooks
                await self._execute_middleware_hooks("service_stopped", service, reverse=True)

            # Phase 5.2: Stop latency monitor first
            if self.latency_monitor:
                await self.latency_monitor.stop()

            # Stop discoverer to cancel periodic tasks
            if hasattr(self.discoverer, "stop"):
                await self.discoverer.stop()

            # Stop cacher (cleanup background tasks, clear locks)
            if self.cacher:
                await self.cacher.stop()

            # Disconnect from the cluster
            await self.transit.disconnect()

            # Call broker_stopped hooks AFTER disconnecting (reverse order)
            await self._execute_middleware_hooks("broker_stopped", self, reverse=True)

            # Emit $broker.stopped internal event (Moleculer.js compatible)
            await self.broadcast_local("$broker.stopped", {})

            self.logger.info("Service broker stopped successfully")

        stop_timeout = getattr(self.settings, "graceful_stop_timeout", None)
        if stop_timeout is None:
            await _stop_core()
            return

        try:
            await asyncio.wait_for(_stop_core(), timeout=stop_timeout)
        except TimeoutError as err:
            self.logger.error(f"Graceful broker stop timed out after {stop_timeout:.2f}s")
            raise GracefulStopTimeoutError() from err

    async def wait_for_shutdown(self) -> None:
        """Wait for shutdown signals and stop the broker gracefully."""
        loop = asyncio.get_running_loop()  # Python 3.10+ compliant
        shutdown_event = asyncio.Event()

        def signal_handler() -> None:
            self.logger.info("Received shutdown signal")
            shutdown_event.set()

        # Register signal handlers
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)

        await shutdown_event.wait()
        await self.stop()

    async def wait_for_services(
        self,
        services: list[str],
        timeout: float = 30.0,
        interval: float = 0.5,
    ) -> None:
        """Wait for specific services to become available.

        Uses asyncio.wait_for to prevent TOCTOU race conditions between
        timeout check and missing services check.

        Args:
            services: List of service names to wait for
            timeout: Maximum time to wait in seconds (default: 30.0)
            interval: Polling interval in seconds (default: 0.5)

        Raises:
            asyncio.TimeoutError: If services are not available within timeout
        """
        if not services:
            return

        self.logger.info(f"Waiting for services: {', '.join(services)}")

        async def _wait_loop() -> None:
            """Internal loop that checks for missing services."""
            last_log_time = 0.0
            while True:
                missing = self._find_missing_services(services)
                if not missing:
                    return

                # Log progress every 5 seconds
                current_time = asyncio.get_running_loop().time()
                if current_time - last_log_time >= SERVICE_WAIT_LOG_INTERVAL:
                    self.logger.debug(f"Still waiting for services: {', '.join(missing)}")
                    last_log_time = current_time

                await asyncio.sleep(interval)

        try:
            await asyncio.wait_for(_wait_loop(), timeout=timeout)
            self.logger.info("All required services are available")
        except TimeoutError:
            # Get current missing services for the error message
            missing = self._find_missing_services(services)
            raise TimeoutError(
                f"Timeout waiting for services after {timeout}s. Missing: {', '.join(missing)}"
            ) from None

    def _find_missing_services(self, services: list[str]) -> list[str]:
        """Find services that are not yet available.

        Args:
            services: List of service names to check

        Returns:
            List of service names that are not available
        """
        unresolved_services: list[str] = []
        for service_name in services:
            if not self.registry.get_service(service_name):
                unresolved_services.append(service_name)

        if not unresolved_services:
            return []

        remote_service_names: set[str] = set()
        nodes_values = self.node_catalog.nodes.values()
        for node in nodes_values:
            if node.id == self.nodeID:
                continue
            for service_obj in node.services:
                if not isinstance(service_obj, dict):
                    continue
                remote_name = service_obj.get("name")
                if isinstance(remote_name, str):
                    remote_service_names.add(remote_name)

        return [
            service_name
            for service_name in unresolved_services
            if service_name not in remote_service_names
        ]

    async def register(self, service: "Service") -> None:
        """Register a service with the broker.

        This method follows the Moleculer pattern: middleware wrapping happens
        at registration time, not at call time. This ensures that middleware
        is applied consistently regardless of how the action is invoked.

        Args:
            service: Service instance to register
        """
        self.logger.info(f"Registering service: {service.name}")

        # Set broker reference on service
        service.broker = self
        service.logger = self.logger.bind(service=service.name)

        # Call service_creating hooks BEFORE registration
        await self._execute_middleware_hooks("service_creating", service)

        # Register with registry and update local node
        self.registry.register(service)
        self.node_catalog.ensure_local_node()

        # Wrap action handlers with middleware (Moleculer pattern)
        # This ensures middleware is applied even for direct service.action() calls
        await self._wrap_service_handlers(service)

        # Call service's own created() lifecycle hook
        if hasattr(service, "_call_lifecycle_hook"):
            try:
                await service._call_lifecycle_hook("created")
            except Exception as e:
                self.logger.error(f"Error in service {service.name} created hook: {e}")
                raise

        # Call middleware service_created hooks AFTER registration
        # NOTE: service_started hooks are intentionally NOT called here.
        # They are called in broker.start() after all services are registered
        # and the broker is fully connected to the cluster.
        await self._execute_middleware_hooks("service_created", service)

        self.logger.info(f"Service {service.name} registered successfully")

    async def _wrap_service_handlers(self, service: "Service") -> None:
        """Wrap all action and event handlers for a service with middleware.

        This implements the Moleculer pattern where handlers are wrapped at
        registration time rather than at call time.

        Args:
            service: Service instance whose handlers should be wrapped
        """
        # Wrap action handlers + compile validator checkers
        svc_key = getattr(service, "full_name", service.name)
        for action in self.registry.__actions__:
            if action.is_local and action.name.startswith(f"{svc_key}."):
                if action.handler is not None:
                    action.wrapped_handler = await self.middleware_handler.wrap_handler(
                        "local_action",
                        action.handler,
                        action,
                    )
                    # Compile validator checker once at registration (Node.js pattern)
                    if action.params_schema and self._validator is not None:
                        action._compiled_checker = self._validator.compile(action.params_schema)
                    self.logger.debug(f"Wrapped action handler: {action.name}")

        # Wrap event handlers
        for event in self.registry.__events__:
            if event.is_local and event.handler is not None:
                # Check if this event belongs to the service
                event_handler = event.handler
                if hasattr(event_handler, "__self__") and event_handler.__self__ is service:
                    event.wrapped_handler = await self.middleware_handler.wrap_handler(
                        "local_event",
                        event.handler,
                        event,
                    )
                    self.logger.debug(f"Wrapped event handler: {event.name}")

    async def _apply_middlewares(
        self, handler: Callable[..., Any], hook_name: str, metadata: Any
    ) -> Callable[..., Any]:
        """Apply middleware transformations to a handler.

        Args:
            handler: Original handler function
            hook_name: Middleware hook name to apply
            metadata: Metadata to pass to middleware hooks

        Returns:
            Transformed handler function
        """
        current_handler = handler

        # Apply middlewares in reverse order for proper wrapping
        for middleware in reversed(self.middlewares):
            hook = getattr(middleware, hook_name, None)
            if hook and callable(hook):
                result = hook(current_handler, metadata)
                if asyncio.iscoroutine(result):
                    current_handler = await result
                else:
                    current_handler = result

        return current_handler

    async def call(
        self,
        action_name: ActionName | str,
        params: dict[str, Any] | None = None,
        meta: dict[str, Any] | None = None,
        parent_ctx: "Context | None" = None,
        timeout: float | None = None,
    ) -> Any:
        """Call a service action.

        This method uses pre-wrapped handlers (Moleculer pattern) for local actions,
        ensuring consistent middleware application. For remote actions, middleware
        is applied at call time since the remote handler is created dynamically.

        Supports call chain tracking via parent_ctx:
        - Level increments for each nested call
        - Request ID propagates through the chain
        - Max call level protection against infinite recursion

        Args:
            action_name: Fully qualified action name (service.action)
            params: Parameters to pass to the action
            meta: Metadata for the call
            parent_ctx: Parent context for nested calls (enables level/requestID tracking)
            timeout: Optional per-call timeout override

        Returns:
            Result from the action

        Raises:
            ServiceNotFoundError: If action is not found
            MaxCallLevelError: If max call level is exceeded
            Exception: If execution fails
        """
        if params is None:
            params = {}
        if meta is None:
            meta = {}

        # Calculate level, request_id, caller, and tracing from parent context
        parent_span = None
        if parent_ctx is not None:
            level = parent_ctx.level + 1
            request_id = parent_ctx.request_id
            parent_id = parent_ctx.id
            # Phase 3B: Inherit tracing flag from parent
            tracing = parent_ctx.tracing
            # Phase 3B: Set caller to parent's service fullName if available
            caller = parent_ctx.service.name if parent_ctx.service else None
            # Propagate parent span for trace parent-child linking
            parent_span = getattr(parent_ctx, "span", None)
        else:
            level = 1
            request_id = None  # Will be set to context.id by Lifecycle
            parent_id = None
            tracing = None
            caller = None

        # Check max call level BEFORE creating context
        # Note: Use >= to match Moleculer.js behavior (level AT the limit is rejected)
        max_level = self.settings.max_call_level
        if max_level > 0 and level >= max_level:
            raise MaxCallLevelError(self.nodeID, level)

        context = self.lifecycle.create_context(
            action=action_name,
            params=params,
            meta=meta,
            level=level,
            request_id=request_id,
            parent_id=parent_id,
            timeout=timeout,
            # Phase 3B: New tracing fields
            caller=caller,
            tracing=tracing,
        )

        # Propagate parent span for trace parent-child linking
        if parent_span is not None:
            context.parent_span = parent_span

        # Use load-balanced endpoint selection with full context.
        # ShardStrategy relies on ctx.params to resolve shard keys.
        endpoint = self.registry.get_action_endpoint(action_name, context)
        if not endpoint:
            raise ServiceNotFoundError(action_name, self.nodeID)

        # When disable_balancer=True, remote calls delegate to transporter's
        # built-in balancer (NATS queue groups). Local calls still execute
        # locally for performance (prefer_local semantics).
        if (
            not endpoint.is_local
            and self.settings.disable_balancer
            and self.transit
            and self.transit.transporter.has_built_in_balancer
        ):
            return await self.call_without_balancer(action_name, context, timeout=timeout)

        if endpoint.is_local:
            # Handle local action call using pre-wrapped handler (Moleculer pattern)
            # Use wrapped_handler if available, otherwise fall back to raw handler
            handler = endpoint.wrapped_handler or endpoint.handler

            try:
                # Validate parameters using pre-compiled checker (Node.js pattern:
                # compile() at registration, checker at call time)
                if endpoint._compiled_checker is not None:
                    endpoint._compiled_checker(context.params or {})
                elif endpoint.params_schema and self._validator is not None:
                    # Fallback for actions registered before validator init
                    self._validator.validate(context.params or {}, endpoint.params_schema)

                if handler is None:
                    raise Exception(f"Handler for action {action_name} is None")

                return await handler(context)

            except Exception as e:
                self.logger.error(f"Error in local action {action_name}: {e}")
                raise
        else:
            # Handle remote action call
            # Remote handlers are created dynamically, so wrap at call time
            async def remote_request_handler(ctx: "Context") -> Any:
                return await self.transit.request(endpoint, ctx)

            # Apply middlewares to remote call handler
            wrapped_handler = await self._apply_middlewares(
                remote_request_handler, "remote_action", endpoint
            )

            return await wrapped_handler(context)

    async def call_without_balancer(
        self,
        action_name: ActionName | str,
        context: "Context",
        timeout: float | None = None,
    ) -> Any:
        """Call action without broker-side endpoint selection.

        Delegates balancing to transporter (e.g., NATS queue groups).
        The request is published to a balanced topic and any node with
        the action can handle it.

        Args:
            action_name: Fully qualified action name (service.action)
            context: Pre-created context for the call
            timeout: Optional per-call timeout override

        Returns:
            Result from the action

        Raises:
            ServiceNotFoundError: If action is not found anywhere
        """
        # Guard: transit must be available for balanced calls
        if not self.transit:
            raise MoleculerError(
                message="Transit not available for balanced call",
                code=500,
                type="TRANSIT_NOT_AVAILABLE",
            )

        # Validate action exists (but don't select specific endpoint)
        action = self.registry.get_action(action_name)
        if not action:
            raise ServiceNotFoundError(str(action_name), node_id=self.nodeID)

        return await self.transit.request_without_endpoint(str(action_name), context, timeout)

    async def mcall(
        self,
        definitions: list[dict[str, Any]] | dict[str, dict[str, Any]],
        settled: bool = False,
    ) -> list[Any] | dict[str, Any]:
        """Call multiple actions in parallel.

        Node.js equivalent: service-broker.js mcall()

        Args:
            definitions: Either:
                - list of {"action": str, "params": dict, "meta": dict} dicts
                  → returns list of results in same order
                - dict of {name: {"action": str, "params": dict, "meta": dict}}
                  → returns dict of {name: result}
            settled: If True, exceptions are returned as values instead of raised
                     (like Promise.allSettled). If False, first failure raises.

        Returns:
            List or dict of results matching input format.
        """
        if isinstance(definitions, list):
            tasks = [
                self.call(
                    d["action"],
                    params=d.get("params"),
                    meta=d.get("meta"),
                )
                for d in definitions
            ]
            results = await asyncio.gather(*tasks, return_exceptions=settled)
            return list(results)
        else:
            names = list(definitions.keys())
            tasks = [
                self.call(
                    definitions[name]["action"],
                    params=definitions[name].get("params"),
                    meta=definitions[name].get("meta"),
                )
                for name in names
            ]
            results = await asyncio.gather(*tasks, return_exceptions=settled)
            return dict(zip(names, results, strict=False))

    async def _emit_core(
        self,
        event_name: str,
        params: dict[str, Any],
        meta: dict[str, Any],
    ) -> Any:
        """Core emit implementation (called by wrapped emit).

        Args:
            event_name: Name of the event to emit
            params: Parameters to pass with the event
            meta: Metadata for the event

        Returns:
            Result from the event handler

        Raises:
            Exception: If event handler is not found
        """
        endpoint = self.registry.get_event(event_name)
        if not endpoint:
            raise Exception(f"Event {event_name} not found")

        context = self.lifecycle.create_context(event=event_name, params=params, meta=meta)

        if endpoint.is_local and endpoint.handler:
            # Handle local event using pre-wrapped handler (Moleculer pattern)
            handler = endpoint.wrapped_handler or endpoint.handler
            return await handler(context)
        else:
            # Handle remote event
            return await self.transit.send_event(endpoint, context)

    async def emit(
        self,
        event_name: EventName | str,
        params: dict[str, Any] | None = None,
        meta: dict[str, Any] | None = None,
    ) -> Any:
        """Emit an event to a single service.

        Uses pre-wrapped handlers (Moleculer pattern) for local events.
        If middleware defines emit hooks, they are applied to this call.

        Args:
            event_name: Name of the event to emit
            params: Parameters to pass with the event
            meta: Metadata for the event

        Returns:
            Result from the event handler

        Raises:
            Exception: If event handler is not found
        """
        if params is None:
            params = {}
        if meta is None:
            meta = {}

        # Use wrapped emit if available (set during start)
        if self._wrapped_emit is not None:
            opts = {"meta": meta}
            return await self._wrapped_emit(event_name, params, opts)

        # Fallback to core implementation (before start() or no middleware)
        return await self._emit_core(event_name, params, meta)

    async def broadcast_local(
        self,
        event: str,
        payload: Any = None,
    ) -> bool:
        """Broadcast an internal event to local handlers only.

        This method is for framework-internal events ($ prefixed) that should
        NOT be sent over the network. Examples:
        - $node.connected, $node.disconnected
        - $broker.started, $broker.stopped
        - $transporter.connected

        Compatible with Moleculer.js broadcastLocal().

        Args:
            event: Event name (should start with $)
            payload: Event data

        Returns:
            True if any handlers were called
        """
        # Use wrapped broadcast_local if available (set during start)
        if self._wrapped_broadcast_local is not None:
            return await self._wrapped_broadcast_local(event, payload)

        # Fallback to core implementation (before start() or no middleware)
        return await self.local_bus.emit(event, payload)

    async def _broadcast_core(
        self,
        event_name: str,
        params: dict[str, Any],
        meta: dict[str, Any],
        groups: list[str] | None = None,
    ) -> list[Any]:
        """Core broadcast implementation (called by wrapped broadcast).

        Args:
            event_name: Name of the event to broadcast
            params: Parameters to pass with the event
            meta: Metadata for the event
            groups: Optional list of groups to broadcast to

        Returns:
            List of results from all event handlers
        """
        endpoints = self.registry.get_all_events(event_name)
        if not endpoints:
            self.logger.warning(f"No handlers found for event: {event_name}")
            return []

        # Filter by groups if specified
        if groups:
            endpoints = [e for e in endpoints if getattr(e, "group", None) in groups]

        context = self.lifecycle.create_context(event=event_name, params=params, meta=meta)

        tasks = []
        remote_targets = []
        for endpoint in endpoints:
            if endpoint.is_local and endpoint.handler:
                # Handle local event using pre-wrapped handler (Moleculer pattern)
                handler = endpoint.wrapped_handler or endpoint.handler
                tasks.append(handler(context))
            else:
                tasks.append(None)
                remote_targets.append((len(tasks) - 1, endpoint))

        if remote_targets:
            # Reuse one marshalled payload for all remote broadcast recipients.
            marshalled_context = context.marshall()
            for index, endpoint in remote_targets:
                tasks[index] = self.transit.send_event(
                    endpoint,
                    context,
                    marshalled_context=marshalled_context,
                )

        resolved_tasks = [task for task in tasks if task is not None]
        if resolved_tasks:
            # Keep Promise.all-like behavior from Moleculer.js: reject on first failure.
            results = await asyncio.gather(*resolved_tasks)
            return list(results)

        return []

    async def broadcast(
        self,
        event_name: EventName | str,
        params: dict[str, Any] | None = None,
        meta: dict[str, Any] | None = None,
        groups: list[str] | None = None,
    ) -> list[Any]:
        """Broadcast an event to all registered handlers.

        Uses pre-wrapped handlers (Moleculer pattern) for local events.
        If middleware defines broadcast hooks, they are applied to this call.

        Args:
            event_name: Name of the event to broadcast
            params: Parameters to pass with the event
            meta: Metadata for the event
            groups: Optional list of groups to broadcast to

        Returns:
            List of results from all event handlers
        """
        if params is None:
            params = {}
        if meta is None:
            meta = {}

        # Use wrapped broadcast if available (set during start)
        if self._wrapped_broadcast is not None:
            opts = {"meta": meta, "groups": groups}
            return await self._wrapped_broadcast(event_name, params, opts)

        # Fallback to core implementation (before start() or no middleware)
        return await self._broadcast_core(event_name, params, meta, groups)

    async def ping(
        self,
        node_id: str | None = None,
        timeout: float = 2.0,
    ) -> dict[str, Any] | None:
        """Ping a remote node and measure round-trip time.

        Node.js equivalent: service-broker.js ping()

        Args:
            node_id: Target node ID to ping, or None to ping all remote nodes.
            timeout: Maximum wait time in seconds for pong response(s).

        Returns:
            For single node: dict with keys "nodeID", "time" (RTT ms), "timeDiff"
                or None if no pong received within timeout.
            For all nodes: dict of {nodeID: pong_data | None}
        """
        if node_id is not None:
            return await self._ping_node(node_id, timeout)

        # Ping all remote available nodes in parallel
        nodes = {
            nid: node
            for nid, node in self.node_catalog.nodes.items()
            if nid != self.nodeID and node.available
        }
        if not nodes:
            return {}

        tasks = {nid: self._ping_node(nid, timeout) for nid in nodes}
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        return {
            nid: (result if not isinstance(result, BaseException) else None)
            for nid, result in zip(tasks.keys(), results, strict=False)
        }

    async def _ping_node(self, node_id: str, timeout: float) -> dict[str, Any] | None:
        """Send a PING to a single node and wait for PONG.

        Follows Moleculer.js pattern: broker.ping() listens to "$node.pong" on
        localBus, which transit emits upon receiving a PONG packet.

        Args:
            node_id: Target node ID
            timeout: Wait timeout in seconds

        Returns:
            Pong data dict or None on timeout
        """
        import time as _time  # noqa: PLC0415
        import uuid  # noqa: PLC0415

        from .packet import Packet, Topic  # noqa: PLC0415

        ping_id = str(uuid.uuid4())
        sent_ms = _time.time() * 1000
        pong_event = asyncio.Event()
        pong_result: dict[str, Any] = {}

        async def _on_pong(payload: Any) -> None:
            data = payload if isinstance(payload, dict) else {}
            # Filter by ping_id and nodeID to avoid collisions
            if data.get("id") == ping_id and data.get("nodeID") == node_id:
                pong_result["nodeID"] = node_id
                pong_result["time"] = data.get("elapsedTime", 0.0)
                pong_result["timeDiff"] = data.get("timeDiff", 0)
                pong_event.set()

        self.local_bus.on("$node.pong", _on_pong)

        try:
            await self.transit.publish(
                Packet(
                    Topic.PING,
                    node_id,
                    {"id": ping_id, "time": sent_ms, "sender": self.nodeID},
                )
            )
            try:
                await asyncio.wait_for(pong_event.wait(), timeout=timeout)
            except TimeoutError:
                return None
            return dict(pong_result)
        finally:
            self.local_bus.off("$node.pong", _on_pong)

    def get_health_status(self) -> dict[str, Any]:
        """Get current system health information.

        Node.js equivalent: health.js getHealthStatus()

        Returns:
            Dict with keys: cpu, mem, os, process, client, net, time
        """
        from .health import get_health_status as _get_health  # noqa: PLC0415

        return _get_health()


# Backwards compatibility alias
Broker = ServiceBroker
