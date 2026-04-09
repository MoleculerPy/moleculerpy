"""Node management and catalog for the MoleculerPy framework.

This module provides node representation and catalog management for tracking
nodes in a MoleculerPy cluster, including their services, actions, and events.

Internal events emitted (Moleculer.js compatible):
- $node.connected: When a node connects or reconnects
- $node.disconnected: When a node disconnects (expected or unexpected)
- $node.updated: When node info is updated (not new, not reconnect)
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .broker import ServiceBroker
    from .registry import CleanupResult, Registry

from .domain_types import NodeID
from .registry import Action, Event

_module_logger = logging.getLogger(__name__)


def _is_wire_scalar(value: Any) -> bool:
    """True iff ``value`` is a leaf that every supported wire serializer
    (JSON / MsgPack / CBOR / ProtoBuf) will happily encode.

    ``json.dumps`` alone is not enough because it has two quirks that downstream
    binary serializers reject:

    * ``allow_nan`` defaults to ``True``, so ``float('nan')`` / ``inf`` slip
      through as the literal strings ``'NaN'`` / ``'Infinity'`` — which are
      not valid JSON per RFC 8259 §6 and which MsgPack's ``msgpack.packb``
      rejects with ``PackException``. On the wire this crashes the INFO
      packet mid-handshake on any non-JSON transporter.
    * It accepts non-str dict keys (via ``sort_keys``), which CBOR does
      preserve but which breaks interop with strictly-keyed consumers.

    We probe with ``allow_nan=False`` to reject IEEE 754 edge cases upfront.
    """
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError):
        return False
    return True


def _sanitize_wire_value(value: Any) -> tuple[Any, bool]:
    """Recursively strip non-serialisable leaves from ``value``.

    Returns ``(cleaned, dropped_anything)``. ``cleaned`` preserves the shape of
    the input (dict stays dict, list stays list) but replaces / removes any
    leaf that fails the wire-scalar probe. Unlike a naive top-level filter
    this keeps the valid siblings of a bad leaf — e.g. a ``routes`` list
    containing ``{"path": "/api", "hook": <callable>}`` becomes
    ``[{"path": "/api"}]`` rather than being dropped wholesale. That matters
    for ``ApiGatewayService.settings`` where the whole point of shipping
    ``routes`` to other nodes is so they can see route *structure* even if
    they cannot execute the hooks.

    ``dropped_anything`` is ``True`` when at least one value was stripped or
    a leaf failed the probe, so the caller can decide whether to emit a
    WARNING. We do not enumerate dropped *paths* to keep the helper cheap on
    hot paths; the caller gets a single boolean signal plus the top-level
    names of keys whose subtree changed.
    """
    if isinstance(value, dict):
        cleaned_dict: dict[Any, Any] = {}
        dropped = False
        for k, v in value.items():
            # Dict keys must themselves be wire-scalars (JSON only accepts
            # str keys, but we tolerate ints by stringifying downstream).
            if not isinstance(k, (str, int, float, bool)) or isinstance(k, bool):
                # Unlikely in real configs, but defensive — drop the entry.
                dropped = True
                continue
            cleaned_v, sub_dropped = _sanitize_wire_value(v)
            if cleaned_v is _DROPPED:
                dropped = True
                continue
            cleaned_dict[k] = cleaned_v
            dropped = dropped or sub_dropped
        return cleaned_dict, dropped
    if isinstance(value, (list, tuple)):
        cleaned_list: list[Any] = []
        dropped = False
        for item in value:
            cleaned_item, sub_dropped = _sanitize_wire_value(item)
            if cleaned_item is _DROPPED:
                dropped = True
                continue
            cleaned_list.append(cleaned_item)
            dropped = dropped or sub_dropped
        return cleaned_list, dropped
    if _is_wire_scalar(value):
        return value, False
    return _DROPPED, True


# Sentinel used by _sanitize_wire_value to signal "drop this value entirely"
# without confusing it with a legitimate ``None``.
_DROPPED: Any = object()


def _serializable_settings(
    settings: Any,
    *,
    service_name: str | None = None,
) -> dict[str, Any]:
    """Return a deep-cleaned copy of ``settings`` safe for the wire.

    Service settings (e.g. ``ApiGatewayService``) may contain callables such as
    route hooks (``onBeforeCall``, ``authorization``) or other non-serialisable
    objects nested inside lists / dicts. Including those verbatim in the INFO
    packet crashes or hangs the wire serializer (json/msgpack/cbor).

    The helper walks the tree recursively: each leaf is probed via
    ``json.dumps(value, allow_nan=False)`` (strict JSON, rejects NaN/inf so
    the binary serializers downstream don't crash) and non-scalar leaves are
    dropped while their valid siblings are preserved. Nested dicts and lists
    retain their shape — e.g. a ``routes`` list with one bad element keeps
    its other elements, only stripping the offending leaf.

    Dropped entries are signalled by a single WARNING log naming the
    top-level setting keys whose subtree was rewritten. This is not a full
    path list (would be expensive) but is enough for an operator to know
    which part of the config lost data.

    Args:
        settings: Raw service settings value. Must be a dict at the top
            level (anything else returns ``{}``).
        service_name: Optional service full name used to annotate warnings
            so operators can correlate dropped keys with a specific service.

    Returns:
        A new dict with non-serialisable leaves stripped and all other
        structure preserved. Empty dict if ``settings`` is not a dict.
    """
    if not isinstance(settings, dict):
        return {}
    result: dict[str, Any] = {}
    keys_with_changes: list[str] = []
    for key, value in settings.items():
        cleaned, sub_dropped = _sanitize_wire_value(value)
        if cleaned is _DROPPED:
            keys_with_changes.append(str(key))
            continue
        if sub_dropped:
            keys_with_changes.append(str(key))
        result[key] = cleaned
    if keys_with_changes:
        who = service_name or "<unknown service>"
        _module_logger.warning(
            "Stripped non-wire-safe leaves from service %s settings before "
            "INFO broadcast (affected top-level keys: %s). Callables, open "
            "files, NaN/inf and other non-JSON-serialisable leaves are "
            "local-only and will not be visible on other nodes. If peer "
            "services require these values they must be passed through "
            "action params or metadata instead.",
            who,
            ", ".join(keys_with_changes),
        )
    return result


def _suppress_task_exception(task: asyncio.Task[Any]) -> None:
    """Callback to suppress unhandled exceptions in fire-and-forget tasks.

    This marks the exception as handled to prevent 'Task exception was never
    retrieved' warnings while keeping the fire-and-forget pattern.
    """
    if not task.cancelled():
        task.exception()  # Mark as retrieved


class Node:
    """Represents a node in the MoleculerPy cluster.

    A node contains information about its services, capabilities, and current
    status within the distributed system.
    """

    __slots__ = (
        "__weakref__",
        "available",
        "client",
        "config",
        "cpu",
        "cpuCoresLogical",
        "cpuCoresPhysical",
        "cpuSeq",
        "hostname",
        "id",
        "instanceID",
        "ipList",
        "lastHeartbeatTime",
        "latency",
        "local",
        "memory",
        "metadata",
        "port",
        "sender",
        "seq",
        "services",
        "udp_address",
        "ver",
    )

    def __init__(
        self,
        node_id: NodeID | str,
        available: bool = True,
        local: bool = False,
        services: list[dict[str, Any]] | None = None,
        cpu: int = 0,
        client: dict[str, Any] | None = None,
        ip_list: list[str] | None = None,
        hostname: str | None = None,
        config: dict[str, Any] | None = None,
        instance_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        seq: int = 0,
        ver: int = 0,
        sender: str | None = None,
        # Phase 5.1: Extended metrics (Moleculer.js compatible + Python extensions)
        cpu_seq: int = 0,
        memory: float = 0.0,
        last_heartbeat_time: float | None = None,
        cpu_cores_logical: int = 1,
        cpu_cores_physical: int = 1,
        latency: float = 0.0,
    ) -> None:
        """Initialize a new Node instance.

        Args:
            node_id: Unique identifier for this node
            available: Whether the node is currently available
            local: Whether this is the local node
            services: List of services running on this node
            cpu: CPU usage percentage (0-100, int like Moleculer.js)
            client: Client information (type, version, etc.)
            ip_list: List of IP addresses for this node
            hostname: Hostname of the node
            config: Node configuration
            instance_id: Unique instance identifier
            metadata: Additional node metadata
            seq: Sequence number for ordering
            ver: Version number
            sender: Sender identifier for the node info
            cpu_seq: Moleculer.js compatible - increments when CPU changes
            memory: Memory usage percentage (Python extension)
            last_heartbeat_time: Timestamp of last heartbeat received
            cpu_cores_logical: Number of logical CPU cores (Python extension)
            cpu_cores_physical: Number of physical CPU cores (Python extension)
            latency: Network latency to this node in milliseconds
        """
        self.id = NodeID(node_id)
        self.available = available
        self.local = local
        self.services = services or []
        self.cpu = cpu
        self.client = client
        self.ipList = ip_list or []  # Keep original name for compatibility
        self.hostname = hostname
        self.config = config or {}
        self.instanceID = instance_id  # Keep original name for compatibility
        self.metadata = metadata or {}
        self.seq = seq
        self.ver = ver
        self.sender = sender

        # TCP transporter fields (Moleculer.js Node compatible)
        self.port: int = 0  # TCP listening port
        self.udp_address: str | None = None  # UDP discovery address

        # Phase 5.1: Extended metrics
        self.cpuSeq = cpu_seq  # Moleculer.js compatible name
        self.memory = memory
        self.lastHeartbeatTime = last_heartbeat_time or time.time()
        self.cpuCoresLogical = cpu_cores_logical
        self.cpuCoresPhysical = cpu_cores_physical
        self.latency = latency

    def get_info(self) -> dict[str, Any]:
        """Get node information as a dictionary.

        Returns:
            Dictionary containing all node attributes
        """
        return {
            "id": self.id,
            "available": self.available,
            "local": self.local,
            "services": self.services,
            "cpu": self.cpu,
            "client": self.client,
            "ipList": self.ipList,
            "hostname": self.hostname,
            "config": self.config,
            "instanceID": self.instanceID,
            "metadata": self.metadata,
            "seq": self.seq,
            "ver": self.ver,
            "sender": self.sender,
            "cpuSeq": self.cpuSeq,
            "memory": self.memory,
            "lastHeartbeatTime": self.lastHeartbeatTime,
            "cpuCoresLogical": self.cpuCoresLogical,
            "cpuCoresPhysical": self.cpuCoresPhysical,
            "latency": self.latency,
        }


class NodeCatalog:
    """Manages the catalog of nodes in the MoleculerPy cluster.

    The NodeCatalog tracks all nodes in the cluster, manages their lifecycle,
    and maintains the registry of services, actions, and events they provide.

    Emits internal events (via broker.broadcast_local):
    - $node.connected: {node: Node, reconnected: bool}
    - $node.disconnected: {node: Node, unexpected: bool}
    - $node.updated: {node: Node}
    """

    def __init__(
        self,
        registry: Registry,
        logger: Any,
        node_id: str,
        broker: ServiceBroker | None = None,
    ) -> None:
        """Initialize a new NodeCatalog.

        Args:
            registry: Registry instance for tracking services
            logger: Logger instance for catalog operations
            node_id: ID of the local node
            broker: Broker instance for emitting internal events
        """
        self.nodes: dict[str, Node] = {}
        self.registry = registry
        self.logger = logger
        self.node_id = node_id
        self.broker: ServiceBroker | None = broker
        self.local_node: Node | None = None
        self.ensure_local_node()

    def _emit_internal_event(
        self, event: str, payload: dict[str, Any]
    ) -> Coroutine[Any, Any, bool] | None:
        """Emit an internal event via broker.broadcast_local.

        This is a helper that schedules the async emit if broker is available.
        Returns the coroutine so caller can await if needed.

        Args:
            event: Event name (e.g., "$node.connected")
            payload: Event payload

        Returns:
            Coroutine if broker is available, None otherwise
        """
        if self.broker is not None:
            return self.broker.broadcast_local(event, payload)
        return None

    def _schedule_emit(self, event: str, payload: dict[str, Any]) -> None:
        """Schedule an internal event emission (fire-and-forget).

        Args:
            event: Event name
            payload: Event payload
        """
        coro = self._emit_internal_event(event, payload)
        if coro is not None:
            # Schedule as background task with name for debugging
            task = asyncio.create_task(coro, name=f"moleculerpy:{event}")
            # Suppress unhandled exception warnings (fire-and-forget pattern)
            task.add_done_callback(_suppress_task_exception)

    def add_node(self, node_id: str, node: Node) -> None:
        """Add a node to the catalog.

        Args:
            node_id: Unique identifier for the node
            node: Node instance to add
        """
        self.nodes[node_id] = node

        # Register node's services, actions, and events
        self._register_node_endpoints(node_id, node)

        self.logger.info(f'Node "{node_id}" added.')

    def _register_node_endpoints(self, node_id: str, node: Node) -> None:
        """Register all action and event endpoints for a node."""
        if not self.registry or not hasattr(node, "services"):
            return

        for service in node.services:
            # Reconstruct fullName if missing (Node.js registry.js:198 compat)
            if not service.get("fullName"):
                from moleculerpy.service import Service  # noqa: PLC0415

                service["fullName"] = Service.get_versioned_full_name(
                    service.get("name", ""),
                    service.get("version"),
                )
            actions = service.get("actions", {})
            for action_name in actions:
                action_obj = Action(name=action_name, node_id=node_id, is_local=False)
                # Link action to node for CPU/latency strategies
                action_obj.node = node
                self.registry.add_action(action_obj)

            events = service.get("events", {})
            for event_name in events:
                event_obj = Event(name=event_name, node_id=node_id, is_local=False)
                self.registry.add_event_obj(event_obj)

    def _sync_node_endpoints(self, node_id: str, node: Node) -> None:
        """Replace remote node endpoints to keep registry in sync with INFO updates."""
        if not self.registry or node_id == self.node_id:
            return

        self.registry.remove_actions_by_node(node_id)
        self.registry.remove_events_by_node(node_id)
        self._register_node_endpoints(node_id, node)

    def get_node(self, node_id: str) -> Node | None:
        """Get a node by its ID.

        Args:
            node_id: ID of the node to retrieve

        Returns:
            Node instance if found, None otherwise
        """
        return self.nodes.get(node_id)

    def remove_node(self, node_id: str) -> None:
        """Remove a node from the catalog.

        Args:
            node_id: ID of the node to remove
        """
        if node_id in self.nodes:
            del self.nodes[node_id]
            self.logger.info(f'Node "{node_id}" removed.')

    def disconnect_node(self, node_id: str, unexpected: bool = True) -> None:
        """Mark a node as disconnected and remove it with cascade cleanup.

        Performs cascade cleanup (Moleculer.js/Go compatible):
            1. Mark node as unavailable
            2. Cleanup registry: services → actions → events
            3. Emit $node.disconnected internal event
            4. Remove node from catalog

        Moleculer.js pattern (registry/registry.js:disconnectNode):
            services.removeAllByNodeID(nodeID)
            actions.removeByService(service)
            events.removeByService(service)
            transit.removePendingRequestByNodeID(nodeID)
            broker.broadcastLocal("$node.disconnected", {...})

        Moleculer-Go pattern (registry/registry.go:DisconnectNode):
            services.RemoveByNode(nodeID)
            actions.RemoveByNode(nodeID)
            events.RemoveByNode(nodeID)
            node.Unavailable()
            bus.EmitAsync("$node.disconnected", [...])

        Args:
            node_id: ID of the node to disconnect
            unexpected: Whether the disconnection was unexpected (e.g., heartbeat timeout)
                       vs expected (graceful shutdown). Default True.
        """
        node = self.get_node(node_id)
        if node is None:
            self.logger.debug(f'Node "{node_id}" not found, skipping disconnect')
            return

        # 1. Mark node as unavailable first
        node.available = False

        # 2. Cascade cleanup: remove services, actions, events from registry
        cleanup_result: CleanupResult = {"services": 0, "actions": 0, "events": 0}
        if self.registry is not None and hasattr(self.registry, "cleanup_node"):
            result = self.registry.cleanup_node(node_id)
            # Handle case where registry.cleanup_node returns dict
            if isinstance(result, dict):
                cleanup_result = result

        # 3. Emit $node.disconnected internal event
        self._schedule_emit(
            "$node.disconnected",
            {
                "node": node,
                "unexpected": unexpected,
            },
        )

        # 4. Log with cleanup details
        if unexpected:
            self.logger.info(
                f'Node "{node_id}" disconnected unexpectedly. '
                f"Cleaned up: {cleanup_result['services']} services, "
                f"{cleanup_result['actions']} actions, "
                f"{cleanup_result['events']} events."
            )
        else:
            self.logger.info(
                f'Node "{node_id}" disconnected. '
                f"Cleaned up: {cleanup_result['services']} services, "
                f"{cleanup_result['actions']} actions, "
                f"{cleanup_result['events']} events."
            )

        # 5. Remove node from catalog
        self.remove_node(node_id)

    def process_node_info(self, node_id: str, payload: dict[str, Any]) -> None:
        """Process node information update.

        Emits internal events (Moleculer.js compatible):
        - $node.connected (reconnected=False) for new nodes
        - $node.connected (reconnected=True) for reconnected nodes
        - $node.updated for existing available nodes

        Args:
            node_id: ID of the node being updated
            payload: Dictionary containing node information
        """
        existing_node = self.get_node(node_id)
        is_new = existing_node is None
        is_reconnected = existing_node is not None and not existing_node.available

        if is_new:
            node = Node(node_id)
            self.add_node(node_id, node)
        else:
            assert existing_node is not None
            node = existing_node

        # Update node information
        node.available = True
        node.cpu = payload.get("cpu", 0)
        node.services = payload.get("services", [])
        node.hostname = payload.get("hostname")
        node.ipList = payload.get("ipList", [])
        node.client = payload.get("client")
        node.metadata = payload.get("metadata", {})
        node.seq = payload.get("seq", 0)
        node.instanceID = payload.get("instanceID", node.instanceID)
        node.port = payload.get("port", 0)

        # Phase 5.1: Update extended metrics
        node.cpuSeq = payload.get("cpuSeq", 0)
        node.memory = payload.get("memory", 0.0)
        node.lastHeartbeatTime = payload.get("lastHeartbeatTime") or time.time()
        if "cpuCoresLogical" in payload:
            node.cpuCoresLogical = payload["cpuCoresLogical"]
        if "cpuCoresPhysical" in payload:
            node.cpuCoresPhysical = payload["cpuCoresPhysical"]

        # Sync registry endpoints with the latest service list from INFO payload.
        self._sync_node_endpoints(node_id, node)

        # Emit internal events based on state
        if is_new:
            self._schedule_emit(
                "$node.connected",
                {
                    "node": node,
                    "reconnected": False,
                },
            )
            self.logger.info(f'Node "{node_id}" connected.')
        elif is_reconnected:
            self._schedule_emit(
                "$node.connected",
                {
                    "node": node,
                    "reconnected": True,
                },
            )
            self.logger.info(f'Node "{node_id}" reconnected.')
        else:
            # Existing available node - just an update
            self._schedule_emit("$node.updated", {"node": node})
            self.logger.debug(f'Node "{node_id}" info updated.')

    def ensure_local_node(self) -> None:
        """Ensure the local node exists and is properly configured."""
        if not self.local_node:
            node = Node(self.node_id)
            self.local_node = node
            self.add_node(self.node_id, node)

        # Configure local node properties
        self.local_node.local = True
        self.local_node.client = {
            "type": "python",
            "langVersion": sys.version,
        }

        # Build service definitions from registry
        self.local_node.services = []
        for service in self.registry.__services__.values():
            _fn = getattr(service, "full_name", None)
            svc_full_name = _fn if isinstance(_fn, str) else service.name
            service_definition: dict[str, Any] = {
                "name": service.name,
                "version": getattr(service, "version", None),
                "fullName": svc_full_name,
                "settings": _serializable_settings(service.settings, service_name=svc_full_name),
                "metadata": service.metadata,
                "actions": {},
                "events": {},
            }

            # Add actions using full_name for versioned action names
            for action_method in service.actions():
                handler = getattr(service, action_method)
                raw_name = getattr(handler, "_name", action_method)
                action_name = f"{svc_full_name}.{raw_name}"
                service_definition["actions"][action_name] = {
                    "rawName": raw_name,
                    "name": action_name,
                }

            # Add events
            for event in service.events():
                event_name = getattr(getattr(service, event), "_name", event)
                service_definition["events"][event_name] = {"name": event_name}

            self.local_node.services.append(service_definition)
