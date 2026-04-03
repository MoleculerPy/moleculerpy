"""Service registry for managing actions and events in the MoleculerPy framework.

This module provides the registry system that tracks services, actions, and events
within a MoleculerPy node, enabling service discovery and routing capabilities.
"""

from __future__ import annotations

import re
import weakref
from collections.abc import Callable, Iterator
from functools import lru_cache
from typing import TYPE_CHECKING, Any, TypedDict

from .domain_types import ActionName, EventName, NodeID, ServiceName

if TYPE_CHECKING:
    from .context import Context
    from .node import Node
    from .service import Service
    from .strategy import BaseStrategy


@lru_cache(maxsize=256)
def _compile_event_pattern(pattern: str) -> re.Pattern[str] | None:
    """Compile wildcard event patterns to regex (cached)."""
    if "**" in pattern:
        regex = "^" + re.escape(pattern).replace(r"\*\*", ".*") + "$"
        return re.compile(regex)
    if "*" in pattern:
        regex = "^" + re.escape(pattern).replace(r"\*", "[^.]*") + "$"
        return re.compile(regex)
    return None


class CleanupResult(TypedDict):
    """Result of cascade cleanup operation.

    Moleculer.js/Go compatible cleanup result containing counts
    of removed services, actions, and events.
    """

    services: int
    actions: int
    events: int


class Action:
    """Represents a service action in the registry.

    Actions are callable methods on services that can be invoked remotely
    through the service broker.

    Attributes:
        name: Fully qualified action name (service.action)
        handler: Raw handler function (before middleware wrapping)
        wrapped_handler: Handler after middleware wrapping (used at call time)
        node_id: ID of the node hosting this action
        is_local: Whether this action is local to the current node
        params_schema: Optional parameter validation schema
        timeout: Optional timeout in seconds
        retries: Optional number of retry attempts
        cache_ttl: Optional cache TTL in seconds (legacy, prefer cache)
        cache: Cache configuration (True, False, or dict)
        node: Weak reference to the Node hosting this action (for CPU/latency strategies)
    """

    __slots__ = (
        "_node_ref",  # weakref to Node for strategies
        "cache",
        "cache_ttl",
        "handler",
        "is_local",
        "name",
        "node_id",
        "params_schema",
        "retries",
        "timeout",
        "wrapped_handler",
    )

    def __init__(
        self,
        name: ActionName | str,
        node_id: NodeID | str,
        is_local: bool,
        handler: Callable[..., Any] | None = None,
        wrapped_handler: Callable[..., Any] | None = None,
        params_schema: dict[str, Any] | None = None,
        timeout: float | None = None,
        retries: int | None = None,
        cache_ttl: int | None = None,
        cache: bool | dict[str, Any] | None = None,
    ) -> None:
        """Initialize an Action instance.

        Args:
            name: Fully qualified action name (service.action)
            node_id: ID of the node hosting this action
            is_local: Whether this action is local to the current node
            handler: Raw callable handler function for the action
            wrapped_handler: Handler wrapped with middleware (set at registration)
            params_schema: Optional parameter validation schema
            timeout: Optional timeout in seconds for this action
            retries: Optional number of retry attempts
            cache_ttl: Optional cache TTL in seconds (legacy)
            cache: Cache configuration (True, False, or dict with ttl, keys, lock)
        """
        self.name = ActionName(name)
        self.handler = handler
        self.wrapped_handler = wrapped_handler  # Set during registration
        self.node_id = NodeID(node_id)
        self.is_local = is_local
        self.params_schema = params_schema
        self.timeout = timeout
        self.retries = retries
        self.cache_ttl = cache_ttl
        self.cache = cache
        self._node_ref: weakref.ref[Node] | None = None

    @property
    def node(self) -> Node | None:
        """Get the Node hosting this action (for CPU/latency strategies).

        Returns:
            Node instance or None if not set or garbage collected
        """
        if self._node_ref is None:
            return None
        return self._node_ref()

    @node.setter
    def node(self, value: Node | None) -> None:
        """Set the Node reference (uses weakref to avoid circular refs)."""
        if value is None:
            self._node_ref = None
        else:
            self._node_ref = weakref.ref(value)

    def __repr__(self) -> str:
        return f"Action(name={self.name!r}, node_id={self.node_id!r}, is_local={self.is_local})"


class Event:
    """Represents an event handler in the registry.

    Events are methods that can be triggered by other services and
    can have multiple handlers across different services.

    Attributes:
        name: Event name
        node_id: ID of the node hosting this event handler
        handler: Raw handler function (before middleware wrapping)
        wrapped_handler: Handler after middleware wrapping (used at emit time)
        is_local: Whether this event handler is local to the current node
    """

    __slots__ = ("handler", "is_local", "name", "node_id", "wrapped_handler")

    def __init__(
        self,
        name: EventName | str,
        node_id: NodeID | str,
        is_local: bool = False,
        handler: Callable[..., Any] | None = None,
        wrapped_handler: Callable[..., Any] | None = None,
    ) -> None:
        """Initialize an Event instance.

        Args:
            name: Event name
            node_id: ID of the node hosting this event handler
            is_local: Whether this event handler is local to the current node
            handler: Raw callable handler function for the event
            wrapped_handler: Handler wrapped with middleware (set at registration)
        """
        self.name = EventName(name)
        self.node_id = NodeID(node_id)
        self.handler = handler
        self.wrapped_handler = wrapped_handler  # Set during registration
        self.is_local = is_local

    def __repr__(self) -> str:
        return f"Event(name={self.name!r}, node_id={self.node_id!r}, is_local={self.is_local})"


class Registry:
    """Central registry for services, actions, and events.

    The Registry maintains catalogs of all available services, actions, and events
    within the MoleculerPy cluster, enabling service discovery and routing.

    Supports load balancing strategies for distributing requests across
    multiple action endpoints.
    """

    __slots__ = (
        "__actions__",
        "__events__",
        "__logger__",
        "__node_id__",
        "__services__",
        "_actions_by_name",
        "_event_wildcards",
        "_events_by_name",
        "_extra",
        "_prefer_local",
        "_strategy",
    )

    def __init__(
        self,
        node_id: NodeID | str | None = None,
        logger: Any | None = None,
        strategy: str = "RoundRobin",
        prefer_local: bool = True,
    ) -> None:
        """Initialize a new Registry instance.

        Args:
            node_id: Unique identifier for this node
            logger: Logger instance for registry operations
            strategy: Load balancing strategy name ('RoundRobin', 'Random')
            prefer_local: If True, prefer local endpoints over remote ones
        """
        self.__services__: dict[str, Service] = {}
        self.__actions__: list[Action] = []
        self.__events__: list[Event] = []
        self._actions_by_name: dict[str, list[Action]] = {}
        self._events_by_name: dict[str, list[Event]] = {}
        self._event_wildcards: dict[str, list[Event]] = {}
        self._extra: dict[str, Any] | None = None
        self.__node_id__ = NodeID(node_id) if node_id is not None else None
        self.__logger__ = logger

        # Load balancing
        self._prefer_local = prefer_local
        self._strategy: BaseStrategy = self._create_strategy(strategy)

    def __setattr__(self, name: str, value: Any) -> None:
        """Allow extension attributes while keeping fixed slots for core fields."""
        if name in self.__slots__:
            object.__setattr__(self, name, value)
            return

        extra = object.__getattribute__(self, "_extra")
        if extra is None:
            extra = {}
            object.__setattr__(self, "_extra", extra)
        extra[name] = value

    def __getattr__(self, name: str) -> Any:
        """Resolve extension attributes stored in the extra dictionary."""
        extra = object.__getattribute__(self, "_extra")
        if extra is not None and name in extra:
            return extra[name]
        raise AttributeError(f"{self.__class__.__name__!s} has no attribute {name!r}")

    def _create_strategy(self, name: str) -> BaseStrategy:
        """Create a strategy instance by name.

        Args:
            name: Strategy name ('RoundRobin', 'Random')

        Returns:
            Strategy instance
        """
        # Import here to avoid circular imports
        from .strategy import resolve  # noqa: PLC0415

        return resolve(name)

    def register(self, service: Service) -> None:
        """Register a service and its actions/events in the registry.

        Args:
            service: Service instance to register
        """
        # Use full_name as key to support versioned services (e.g. "v2.users")
        full = getattr(service, "full_name", None)
        service_key = full if isinstance(full, str) else service.name
        self.__services__[service_key] = service
        local_node_id = self.__node_id__ if self.__node_id__ is not None else NodeID("unknown")

        # Register service actions using full_name for versioned action names
        # Node.js: $noServiceNamePrefix skips the "fullName." prefix
        svc_settings = getattr(service, "settings", None)
        no_svc_prefix = (
            svc_settings.get("$noServiceNamePrefix", False)
            if isinstance(svc_settings, dict)
            else False
        )
        service_actions = []
        for action_name in service.actions():
            handler = getattr(service, action_name)
            raw_name = getattr(handler, "_name", action_name)
            qualified_name = raw_name if no_svc_prefix else f"{service_key}.{raw_name}"
            action_obj = Action(
                name=qualified_name,
                node_id=local_node_id,
                is_local=True,
                handler=handler,
                params_schema=getattr(handler, "_params", None),
                timeout=getattr(handler, "_timeout", None),
                retries=getattr(handler, "_retries", None),
                cache_ttl=getattr(handler, "_cache_ttl", None),
                cache=getattr(handler, "_cache", None),
            )
            service_actions.append(action_obj)
        for action_obj in service_actions:
            self.add_action(action_obj)

        # Register service events
        service_events = [
            Event(
                name=getattr(getattr(service, event), "_name", event),
                node_id=local_node_id,
                is_local=True,
                handler=getattr(service, event),
            )
            for event in service.events()
        ]
        for event_obj in service_events:
            self.add_event_obj(event_obj)

        # Log registered events for debugging
        if self.__logger__:
            for event in service_events:
                self.__logger__.debug(
                    f"Event {event.name} from node {event.node_id} (local={event.is_local})"
                )

    def get_service(self, name: ServiceName | str) -> Service | None:
        """Get a service by name.

        Args:
            name: Service name to look up

        Returns:
            Service instance if found, None otherwise
        """
        return self.__services__.get(name)

    def add_action(self, action_obj: Action) -> None:
        """Add an action to the registry.

        Args:
            action_obj: Action object to add
        """
        self.__actions__.append(action_obj)
        self._actions_by_name.setdefault(action_obj.name, []).append(action_obj)

    def add_event(self, name: EventName | str, node_id: NodeID | str) -> None:
        """Add an event to the registry.

        Args:
            name: Event name
            node_id: Node ID hosting the event
        """
        event = Event(name, node_id, is_local=False)
        self.add_event_obj(event)

    def add_event_obj(self, event_obj: Event) -> None:
        """Add an event object to the registry.

        Args:
            event_obj: Event object to add
        """
        self.__events__.append(event_obj)
        self._events_by_name.setdefault(event_obj.name, []).append(event_obj)
        if "*" in event_obj.name:
            self._event_wildcards.setdefault(event_obj.name, []).append(event_obj)

    def get_action(self, name: ActionName | str) -> Action | None:
        """Get an action by name (returns first match).

        Note: For load-balanced selection, use `get_action_endpoint()` instead.

        Args:
            name: Fully qualified action name to look up

        Returns:
            First matching Action instance, or None if not found
        """
        actions = self._actions_by_name.get(name)
        return actions[0] if actions else None

    def get_all_actions(self, name: ActionName | str) -> list[Action]:
        """Get all action endpoints for a given action name.

        Returns all actions matching the name across all nodes.
        Used by load balancing strategies to select from available endpoints.

        Args:
            name: Fully qualified action name to look up

        Returns:
            List of all Action instances matching the name
        """
        actions = self._actions_by_name.get(name)
        return list(actions) if actions else []

    def get_action_endpoint(
        self,
        name: ActionName | str,
        ctx: Context | None = None,
    ) -> Action | None:
        """Get action endpoint using load balancing strategy.

        If `prefer_local` is True and a local endpoint exists, returns it.
        Otherwise, uses the configured strategy to select from all endpoints.

        Args:
            name: Fully qualified action name to look up
            ctx: Optional context for context-aware strategies (e.g., Shard)

        Returns:
            Selected action endpoint, or None if not found
        """
        endpoints = self._actions_by_name.get(name)
        if not endpoints:
            return None

        # Prefer local endpoint if configured
        if self._prefer_local:
            for endpoint in endpoints:
                if endpoint.is_local:
                    # For local, always return first (there should be only one)
                    return endpoint

        # Use strategy for remote or when prefer_local is False
        return self._strategy.select(endpoints, ctx)

    @property
    def strategy(self) -> BaseStrategy:
        """Current load balancing strategy."""
        return self._strategy

    @property
    def prefer_local(self) -> bool:
        """Whether local endpoints are preferred."""
        return self._prefer_local

    def get_all_events(self, name: EventName | str) -> list[Event]:
        """Get all event handlers for a given event name.

        Args:
            name: Event name to look up

        Returns:
            List of Event instances matching the name
        """
        return list(self._iter_matching_events(name))

    def get_event(self, name: EventName | str) -> Event | None:
        """Get the first event handler for a given event name.

        Args:
            name: Event name to look up

        Returns:
            First matching Event instance, or None if not found
        """
        return next(self._iter_matching_events(name), None)

    def _iter_matching_events(self, name: EventName | str) -> Iterator[Event]:
        """Yield matching events with exact handlers before wildcards."""
        exact_events = self._events_by_name.get(name)
        if exact_events:
            yield from exact_events

        if not self._event_wildcards:
            return

        event_name = str(name)
        for pattern, pattern_events in self._event_wildcards.items():
            compiled = _compile_event_pattern(pattern)
            if compiled is not None and compiled.match(event_name):
                yield from pattern_events

    # ═══════════════════════════════════════════════════════════════════════
    # Cascade Cleanup Methods (Moleculer.js/Go compatible)
    # ═══════════════════════════════════════════════════════════════════════

    def remove_actions_by_node(self, node_id: NodeID | str) -> int:
        """Remove all actions belonging to a disconnected node.

        This is part of the cascade cleanup pattern from Moleculer.js/Go:
            disconnect_node() → remove_services_by_node()
                             → remove_actions_by_node()
                             → remove_events_by_node()

        Moleculer.js: ServiceCatalog.removeByNode() + ActionCatalog.removeByNode()
        Moleculer-Go: ActionCatalog.RemoveByNode()

        Args:
            node_id: ID of the disconnected node

        Returns:
            Number of actions removed
        """
        original_count = len(self.__actions__)
        self.__actions__ = [action for action in self.__actions__ if action.node_id != node_id]
        self._rebuild_action_index()
        removed = original_count - len(self.__actions__)

        if removed > 0 and self.__logger__:
            self.__logger__.debug(f"Removed {removed} actions from disconnected node '{node_id}'")

        return removed

    def remove_events_by_node(self, node_id: NodeID | str) -> int:
        """Remove all event handlers belonging to a disconnected node.

        This is part of the cascade cleanup pattern from Moleculer.js/Go.

        Moleculer.js: EventCatalog.removeByNode()
        Moleculer-Go: EventCatalog.RemoveByNode()

        Args:
            node_id: ID of the disconnected node

        Returns:
            Number of event handlers removed
        """
        original_count = len(self.__events__)
        self.__events__ = [event for event in self.__events__ if event.node_id != node_id]
        self._rebuild_event_index()
        removed = original_count - len(self.__events__)

        if removed > 0 and self.__logger__:
            self.__logger__.debug(f"Removed {removed} events from disconnected node '{node_id}'")

        return removed

    def remove_services_by_node(self, node_id: NodeID | str) -> int:
        """Remove all services belonging to a disconnected node.

        Note: This removes services from __services__ dict.
        Services added via register() use local node_id which is stored
        in the Service object itself (service.node_id or via __node_id__).

        For remote services, they may be stored differently.
        This method handles both local and remote service cleanup.

        Moleculer.js: ServiceCatalog.removeAllByNodeID()
        Moleculer-Go: ServiceCatalog.RemoveByNode()

        Args:
            node_id: ID of the disconnected node

        Returns:
            Number of services removed
        """
        # Services are stored by name, need to check node_id attribute
        removed_names: list[str] = []

        for name, service in list(self.__services__.items()):
            # Check if service belongs to the disconnected node
            service_node_id = getattr(service, "node_id", self.__node_id__)
            if service_node_id == node_id:
                removed_names.append(name)

        for name in removed_names:
            del self.__services__[name]

        if removed_names and self.__logger__:
            self.__logger__.debug(
                f"Removed {len(removed_names)} services from disconnected node '{node_id}': {removed_names}"
            )

        return len(removed_names)

    def cleanup_node(self, node_id: NodeID | str) -> CleanupResult:
        """Perform full cascade cleanup when a node disconnects.

        This is the main entry point for cascade cleanup, following
        the Moleculer.js/Go pattern:

        Moleculer.js disconnectNode():
            services.removeAllByNodeID(nodeID)
            actions.removeByService(service)  // for each service
            events.removeByService(service)   // for each service
            this.registry.nodes.delete(nodeID)

        Moleculer-Go DisconnectNode():
            services.RemoveByNode(nodeID)
            actions.RemoveByNode(nodeID)
            events.RemoveByNode(nodeID)
            node.Unavailable()

        Args:
            node_id: ID of the disconnected node

        Returns:
            CleanupResult with counts: {"services": N, "actions": N, "events": N}
        """
        # Order matters: services first (may contain metadata), then actions/events
        services_removed = self.remove_services_by_node(node_id)
        actions_removed = self.remove_actions_by_node(node_id)
        events_removed = self.remove_events_by_node(node_id)

        if self.__logger__ and (services_removed or actions_removed or events_removed):
            self.__logger__.info(
                f"Cascade cleanup for node '{node_id}': "
                f"{services_removed} services, {actions_removed} actions, {events_removed} events"
            )

        return CleanupResult(
            services=services_removed,
            actions=actions_removed,
            events=events_removed,
        )

    def _rebuild_action_index(self) -> None:
        """Rebuild action name index from the canonical action list."""
        self._actions_by_name = {}
        for action in self.__actions__:
            self._actions_by_name.setdefault(action.name, []).append(action)

    def _rebuild_event_index(self) -> None:
        """Rebuild event name index from the canonical event list."""
        self._events_by_name = {}
        self._event_wildcards = {}
        for event in self.__events__:
            self._events_by_name.setdefault(event.name, []).append(event)
            if "*" in event.name:
                self._event_wildcards.setdefault(event.name, []).append(event)
