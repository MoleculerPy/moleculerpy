"""$node internal service for MoleculerPy framework.

Port of Moleculer.js src/internals.js (177 LOC).
Provides node introspection actions for the service mesh.
"""

from __future__ import annotations

import asyncio
import json
import os
import platform
import socket
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from moleculerpy.decorators import action
from moleculerpy.service import Service

if TYPE_CHECKING:
    from moleculerpy.broker import ServiceBroker
    from moleculerpy.context import Context


class NodeInternalService(Service):
    """Internal service providing node introspection ($node.* actions).

    Registered automatically by the broker. Provides actions to inspect
    nodes, services, actions, events, health, options, and metrics.

    All actions have cache=False to always return fresh data.
    """

    name = "$node"

    @property
    def _broker(self) -> ServiceBroker:
        """Return broker as ServiceBroker (always set before any action is called)."""
        from moleculerpy.broker import ServiceBroker  # noqa: PLC0415

        return cast(ServiceBroker, self.broker)

    @action(name="list", cache=False)
    async def node_list(self, ctx: Context) -> list[dict[str, Any]]:
        """Return list of all known nodes.

        Params:
            onlyAvailable (bool): If True, return only available nodes.
            withServices (bool): If True, include services list in each node.

        Returns:
            List of node info dicts.
        """
        only_available: bool = bool(ctx.params.get("onlyAvailable", False))
        with_services: bool = bool(ctx.params.get("withServices", False))

        result: list[dict[str, Any]] = []
        for node in self._broker.node_catalog.nodes.values():
            if only_available and not node.available:
                continue
            info = node.get_info()
            if not with_services:
                info.pop("services", None)
            result.append(info)
        return result

    @action(name="services", cache=False)
    async def node_services(self, ctx: Context) -> list[dict[str, Any]]:
        """Return list of all registered services.

        Params:
            onlyLocal (bool): If True, return only local services.
            skipInternal (bool): If True, skip internal services (name starts with "$").
            withActions (bool): If True, include actions in each service.
            withEvents (bool): If True, include events in each service.
            onlyAvailable (bool): If True, return only services on available nodes.

        Returns:
            List of service info dicts.
        """
        only_local: bool = bool(ctx.params.get("onlyLocal", False))
        skip_internal: bool = bool(ctx.params.get("skipInternal", False))
        with_actions: bool = bool(ctx.params.get("withActions", False))
        with_events: bool = bool(ctx.params.get("withEvents", False))
        only_available: bool = bool(ctx.params.get("onlyAvailable", False))

        broker = self._broker
        registry = broker.registry
        node_catalog = broker.node_catalog
        local_node_id = broker.nodeID

        result: list[dict[str, Any]] = []

        for svc_name, svc in registry.__services__.items():
            if skip_internal and svc_name.startswith("$"):
                continue

            # All registered local services belong to the local node
            node_id = local_node_id

            if only_local and node_id != local_node_id:
                continue

            if only_available:
                node = node_catalog.nodes.get(node_id)
                if node is None or not node.available:
                    continue

            svc_info: dict[str, Any] = {
                "name": svc.name,
                "version": svc.version,
                "fullName": svc.full_name,
                "settings": svc.settings,
                "metadata": svc.metadata,
                "nodeID": node_id,
                "local": node_id == local_node_id,
            }

            if with_actions:
                actions_dict: dict[str, Any] = {}
                svc_key = svc.full_name
                for act in registry.__actions__:
                    if act.node_id == local_node_id and act.name.startswith(f"{svc_key}."):
                        act_short = act.name[len(svc_key) + 1 :]
                        actions_dict[act_short] = {
                            "name": act.name,
                            "params": act.params_schema,
                            "cache": act.cache,
                        }
                svc_info["actions"] = actions_dict

            if with_events:
                events_dict: dict[str, Any] = {}
                for evt in registry.__events__:
                    if evt.node_id == local_node_id and evt.is_local:
                        handler = evt.handler
                        if (
                            handler is not None
                            and hasattr(handler, "__self__")
                            and handler.__self__ is svc
                        ):
                            events_dict[evt.name] = {"name": evt.name}
                svc_info["events"] = events_dict

            result.append(svc_info)

        return result

    @action(name="actions", cache=False)
    async def node_actions(self, ctx: Context) -> list[dict[str, Any]]:
        """Return list of all registered actions.

        Params:
            onlyLocal (bool): If True, return only local actions.
            skipInternal (bool): If True, skip internal actions (name starts with "$").
            withEndpoints (bool): If True, include endpoint details.
            onlyAvailable (bool): If True, return only actions on available nodes.

        Returns:
            List of action info dicts.
        """
        only_local: bool = bool(ctx.params.get("onlyLocal", False))
        skip_internal: bool = bool(ctx.params.get("skipInternal", False))
        with_endpoints: bool = bool(ctx.params.get("withEndpoints", False))
        only_available: bool = bool(ctx.params.get("onlyAvailable", False))

        broker = self._broker
        registry = broker.registry
        node_catalog = broker.node_catalog
        local_node_id = broker.nodeID

        # Group actions by name for endpoint aggregation
        actions_map: dict[str, dict[str, Any]] = {}

        for act in registry.__actions__:
            if only_local and not act.is_local:
                continue

            if skip_internal and str(act.name).startswith("$"):
                continue

            if only_available:
                node = node_catalog.nodes.get(str(act.node_id))
                if node is None or not node.available:
                    continue

            act_name = str(act.name)
            if act_name not in actions_map:
                actions_map[act_name] = {
                    "name": act_name,
                    "params": act.params_schema,
                    "cache": act.cache,
                    "hasLocal": act.is_local,
                    "count": 0,
                    "endpoints": [],
                }

            actions_map[act_name]["count"] = actions_map[act_name]["count"] + 1
            if act.is_local:
                actions_map[act_name]["hasLocal"] = True

            if with_endpoints:
                endpoint: dict[str, Any] = {
                    "nodeID": str(act.node_id),
                    "state": "available",
                    "local": act.node_id == local_node_id,
                }
                actions_map[act_name]["endpoints"].append(endpoint)

        result = list(actions_map.values())
        if not with_endpoints:
            for item in result:
                item.pop("endpoints", None)

        return result

    @action(name="events", cache=False)
    async def node_events(self, ctx: Context) -> list[dict[str, Any]]:
        """Return list of all registered event listeners.

        Params:
            onlyLocal (bool): If True, return only local events.
            skipInternal (bool): If True, skip internal events (name starts with "$").
            withEndpoints (bool): If True, include endpoint details.
            onlyAvailable (bool): If True, return only events on available nodes.

        Returns:
            List of event info dicts.
        """
        only_local: bool = bool(ctx.params.get("onlyLocal", False))
        skip_internal: bool = bool(ctx.params.get("skipInternal", False))
        with_endpoints: bool = bool(ctx.params.get("withEndpoints", False))
        only_available: bool = bool(ctx.params.get("onlyAvailable", False))

        broker = self._broker
        registry = broker.registry
        node_catalog = broker.node_catalog
        local_node_id = broker.nodeID

        events_map: dict[str, dict[str, Any]] = {}

        for evt in registry.__events__:
            if only_local and not evt.is_local:
                continue

            if skip_internal and str(evt.name).startswith("$"):
                continue

            if only_available:
                node = node_catalog.nodes.get(str(evt.node_id))
                if node is None or not node.available:
                    continue

            evt_name = str(evt.name)
            if evt_name not in events_map:
                events_map[evt_name] = {
                    "name": evt_name,
                    "hasLocal": evt.is_local,
                    "count": 0,
                    "endpoints": [],
                }

            events_map[evt_name]["count"] = events_map[evt_name]["count"] + 1
            if evt.is_local:
                events_map[evt_name]["hasLocal"] = True

            if with_endpoints:
                endpoint = {
                    "nodeID": str(evt.node_id),
                    "state": "available",
                    "local": evt.node_id == local_node_id,
                }
                events_map[evt_name]["endpoints"].append(endpoint)

        result = list(events_map.values())
        if not with_endpoints:
            for item in result:
                item.pop("endpoints", None)

        return result

    @action(name="health", cache=False)
    async def node_health(self, ctx: Context) -> dict[str, Any]:
        """Return system health status.

        Calls broker.get_health_status() if available, otherwise returns
        a basic health dict with cpu/memory placeholders.

        Returns:
            Health status dict compatible with Moleculer.js.
        """
        broker = self._broker
        if hasattr(broker, "get_health_status") and callable(broker.get_health_status):
            result = broker.get_health_status()
            if asyncio.iscoroutine(result):
                return dict(await result)
            return dict(result)

        # Fallback: basic health info using Python stdlib
        cpu_info: dict[str, Any] = {"cores": os.cpu_count() or 1}

        mem_info: dict[str, Any] = {}
        try:
            import psutil  # noqa: PLC0415

            cpu_info["load"] = list(psutil.getloadavg())
            mem = psutil.virtual_memory()
            mem_info = {
                "total": mem.total,
                "free": mem.available,
                "used": mem.used,
                "percent": mem.percent,
            }
        except ImportError:
            cpu_info["load"] = []

        hostname = "unknown"
        try:
            hostname = socket.gethostname()
        except OSError:
            pass

        return {
            "cpu": cpu_info,
            "mem": mem_info,
            "os": {
                "hostname": hostname,
                "platform": platform.system(),
            },
            "process": {
                "pid": os.getpid(),
            },
            "client": {
                "type": "python",
                "version": broker.version,
            },
            "net": {
                "ip": [],
            },
            "time": {
                "now": datetime.now(tz=UTC).isoformat(),
            },
        }

    @action(name="options", cache=False)
    async def node_options(self, ctx: Context) -> dict[str, Any]:
        """Return a safe copy of the broker settings.

        Returns:
            Dict representation of broker settings.
        """
        settings = self._broker.settings
        if hasattr(settings, "__dict__"):
            raw: dict[str, Any] = vars(settings)
        elif hasattr(settings, "_asdict"):
            raw = settings._asdict()
        else:
            raw = {}

        # Shallow copy excluding non-serializable values
        safe: dict[str, Any] = {}
        for key, val in raw.items():
            try:
                json.dumps(val)
                safe[key] = val
            except (TypeError, ValueError):
                safe[key] = repr(val)

        return safe

    @action(name="metrics", cache=False)
    async def node_metrics(self, ctx: Context) -> list[dict[str, Any]]:
        """Return list of collected metrics.

        Checks if MetricsMiddleware is present and returns its registry listing.
        Returns empty list if metrics are not enabled.

        Returns:
            List of metric info dicts.
        """
        from moleculerpy.middleware.metrics import MetricsMiddleware  # noqa: PLC0415

        for mw in self._broker.middlewares:
            if isinstance(mw, MetricsMiddleware):
                reg = mw.registry
                metrics_list: list[dict[str, Any]] = []
                with reg._lock:
                    for mname, metric in list(reg._metrics.items()):
                        metrics_list.append(
                            {
                                "name": mname,
                                "type": type(metric).__name__,
                            }
                        )
                return metrics_list

        return []
