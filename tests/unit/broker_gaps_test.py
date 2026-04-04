"""Unit tests for v0.14.12 broker features: mcall, health, $node internal service.

Tests cover:
- broker.mcall() — list and dict formats, settled/non-settled
- health.get_health_status() — all required keys and sub-keys
- NodeInternalService ($node.*) — all actions
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
import pytest_asyncio

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action, event
from moleculerpy.errors import ServiceNotFoundError
from moleculerpy.health import get_health_status
from moleculerpy.internals import NodeInternalService
from moleculerpy.node import Node, NodeCatalog
from moleculerpy.registry import Registry
from moleculerpy.service import Service
from moleculerpy.settings import Settings

# =============================================================================
# Helper services for testing
# =============================================================================


class MathService(Service):
    """Simple math service for mcall tests."""

    __test__ = False
    name = "math"

    @action()
    async def add(self, ctx) -> int:
        return ctx.params["a"] + ctx.params["b"]

    @action()
    async def mul(self, ctx) -> int:
        return ctx.params["a"] * ctx.params["b"]


class FailingService(Service):
    """Service that always raises."""

    __test__ = False
    name = "failing"

    @action()
    async def boom(self, ctx) -> None:
        raise ValueError("intentional failure")


# =============================================================================
# Fixtures
# =============================================================================


@pytest_asyncio.fixture
async def broker_with_services():
    """Real broker with MathService and FailingService registered."""
    settings = Settings(transporter="memory://")
    broker = ServiceBroker(id="unit-test-node", settings=settings)
    await broker.register(MathService())
    await broker.register(FailingService())
    await broker.start()
    await asyncio.sleep(0.05)
    yield broker
    await broker.stop()


# =============================================================================
# mcall — list format
# =============================================================================


@pytest.mark.asyncio
async def test_mcall_list_returns_list_of_results(broker_with_services):
    """mcall with list format must return list of results in order."""
    broker = broker_with_services
    results = await broker.mcall(
        [
            {"action": "math.add", "params": {"a": 1, "b": 2}},
            {"action": "math.mul", "params": {"a": 3, "b": 4}},
        ]
    )
    assert isinstance(results, list)
    assert results[0] == 3
    assert results[1] == 12


@pytest.mark.asyncio
async def test_mcall_list_empty_returns_empty_list(broker_with_services):
    """mcall with empty list must return empty list."""
    broker = broker_with_services
    results = await broker.mcall([])
    assert results == []


@pytest.mark.asyncio
async def test_mcall_list_settled_false_raises_on_error(broker_with_services):
    """mcall settled=False (default) must raise on first error."""
    broker = broker_with_services
    with pytest.raises(ValueError, match="intentional failure"):
        await broker.mcall(
            [
                {"action": "math.add", "params": {"a": 1, "b": 2}},
                {"action": "failing.boom", "params": {}},
            ],
            settled=False,
        )


@pytest.mark.asyncio
async def test_mcall_list_settled_true_returns_exceptions_as_values(broker_with_services):
    """mcall settled=True must return exceptions as values, not raise."""
    broker = broker_with_services
    results = await broker.mcall(
        [
            {"action": "math.add", "params": {"a": 5, "b": 5}},
            {"action": "failing.boom", "params": {}},
        ],
        settled=True,
    )
    assert isinstance(results, list)
    assert results[0] == 10
    assert isinstance(results[1], ValueError)


@pytest.mark.asyncio
async def test_mcall_list_order_preserved(broker_with_services):
    """mcall list results must preserve input order."""
    broker = broker_with_services
    results = await broker.mcall(
        [
            {"action": "math.add", "params": {"a": 10, "b": 0}},
            {"action": "math.add", "params": {"a": 20, "b": 0}},
            {"action": "math.add", "params": {"a": 30, "b": 0}},
        ]
    )
    assert results == [10, 20, 30]


# =============================================================================
# mcall — dict format
# =============================================================================


@pytest.mark.asyncio
async def test_mcall_dict_returns_dict_of_results(broker_with_services):
    """mcall with dict format must return dict with matching keys."""
    broker = broker_with_services
    results = await broker.mcall(
        {
            "sum": {"action": "math.add", "params": {"a": 1, "b": 2}},
            "product": {"action": "math.mul", "params": {"a": 3, "b": 4}},
        }
    )
    assert isinstance(results, dict)
    assert results["sum"] == 3
    assert results["product"] == 12


@pytest.mark.asyncio
async def test_mcall_dict_empty_returns_empty_dict(broker_with_services):
    """mcall with empty dict must return empty dict."""
    broker = broker_with_services
    results = await broker.mcall({})
    assert results == {}


@pytest.mark.asyncio
async def test_mcall_dict_settled_true_partial_failure(broker_with_services):
    """mcall dict settled=True: successful key has result, failed has exception."""
    broker = broker_with_services
    results = await broker.mcall(
        {
            "ok": {"action": "math.add", "params": {"a": 7, "b": 3}},
            "err": {"action": "failing.boom", "params": {}},
        },
        settled=True,
    )
    assert isinstance(results, dict)
    assert results["ok"] == 10
    assert isinstance(results["err"], ValueError)


@pytest.mark.asyncio
async def test_mcall_dict_settled_false_raises_on_failure(broker_with_services):
    """mcall dict settled=False must raise when any call fails."""
    broker = broker_with_services
    with pytest.raises(ValueError):
        await broker.mcall(
            {
                "bad": {"action": "failing.boom", "params": {}},
            },
            settled=False,
        )


# =============================================================================
# health.get_health_status()
# =============================================================================


def test_health_returns_all_top_level_keys():
    """get_health_status() must return all required top-level keys."""
    health = get_health_status()
    for key in ("cpu", "mem", "os", "process", "client", "net", "time"):
        assert key in health, f"Missing key: {key}"


def test_health_cpu_has_cores():
    """health.cpu must contain 'cores' key."""
    health = get_health_status()
    assert "cores" in health["cpu"]
    assert isinstance(health["cpu"]["cores"], int)
    assert health["cpu"]["cores"] >= 1


def test_health_mem_has_required_keys():
    """health.mem must have 'free', 'total', 'percent' keys."""
    health = get_health_status()
    mem = health["mem"]
    for key in ("free", "total", "percent"):
        assert key in mem, f"Missing mem key: {key}"


def test_health_mem_values_are_sane():
    """health.mem values must be non-negative."""
    health = get_health_status()
    mem = health["mem"]
    assert mem["total"] > 0
    assert mem["free"] >= 0
    assert 0.0 <= mem["percent"] <= 100.0


def test_health_os_has_hostname_and_platform():
    """health.os must contain 'hostname' and 'platform' keys."""
    health = get_health_status()
    os_info = health["os"]
    assert "hostname" in os_info
    assert "platform" in os_info


def test_health_process_has_pid_and_uptime():
    """health.process must contain 'pid' and 'uptime' keys."""
    import os

    health = get_health_status()
    proc = health["process"]
    assert "pid" in proc
    assert "uptime" in proc
    assert proc["pid"] == os.getpid()
    assert proc["uptime"] >= 0


def test_health_client_has_type_python():
    """health.client must have 'type': 'python'."""
    health = get_health_status()
    assert health["client"]["type"] == "python"


def test_health_time_has_now_and_iso():
    """health.time must contain 'now' and 'iso' keys."""
    health = get_health_status()
    time_info = health["time"]
    assert "now" in time_info
    assert "iso" in time_info
    assert isinstance(time_info["now"], int)
    assert isinstance(time_info["iso"], str)
    # ISO string must look like a date
    assert "T" in time_info["iso"]


def test_health_time_now_is_milliseconds():
    """health.time.now must be in milliseconds (> 1e12)."""
    import time

    health = get_health_status()
    assert health["time"]["now"] > 1_000_000_000_000  # after year 2001 in ms


# =============================================================================
# broker.get_health_status() delegates to health module
# =============================================================================


@pytest.mark.asyncio
async def test_broker_get_health_status_returns_full_structure(broker_with_services):
    """broker.get_health_status() must return same structure as health module."""
    broker = broker_with_services
    health = broker.get_health_status()
    for key in ("cpu", "mem", "os", "process", "client", "net", "time"):
        assert key in health, f"broker.get_health_status() missing key: {key}"


# =============================================================================
# $node internal service actions
# =============================================================================


class SimpleTestService(Service):
    """Simple service to verify $node service listings."""

    __test__ = False
    name = "mytest"

    @action()
    async def greet(self, ctx) -> str:
        return "hello"

    @event()
    async def on_something(self, ctx) -> None:
        pass


@pytest_asyncio.fixture
async def broker_with_node_service():
    """Broker with SimpleTestService registered for $node action tests."""
    settings = Settings(transporter="memory://")
    broker = ServiceBroker(id="node-test-node", settings=settings)
    await broker.register(SimpleTestService())
    await broker.start()
    await asyncio.sleep(0.05)
    yield broker
    await broker.stop()


@pytest.mark.asyncio
async def test_node_list_returns_at_least_one_node(broker_with_node_service):
    """$node.list must return at least the local node."""
    broker = broker_with_node_service
    result = await broker.call("$node.list", {})
    assert isinstance(result, list)
    assert len(result) >= 1
    node_ids = [n["id"] for n in result]
    assert "node-test-node" in node_ids


@pytest.mark.asyncio
async def test_node_services_includes_node_service_and_user_service(broker_with_node_service):
    """$node.services must include both $node and user services."""
    broker = broker_with_node_service
    result = await broker.call("$node.services", {})
    assert isinstance(result, list)
    names = [s["name"] for s in result]
    assert "$node" in names
    assert "mytest" in names


@pytest.mark.asyncio
async def test_node_services_skip_internal_excludes_node_service(broker_with_node_service):
    """$node.services skipInternal=True must exclude $node service."""
    broker = broker_with_node_service
    result = await broker.call("$node.services", {"skipInternal": True})
    names = [s["name"] for s in result]
    assert "$node" not in names
    assert "mytest" in names


@pytest.mark.asyncio
async def test_node_actions_returns_list_of_actions(broker_with_node_service):
    """$node.actions must return a list of action descriptors."""
    broker = broker_with_node_service
    result = await broker.call("$node.actions", {})
    assert isinstance(result, list)
    assert len(result) > 0
    action_names = [a["name"] for a in result]
    assert "mytest.greet" in action_names


@pytest.mark.asyncio
async def test_node_actions_skip_internal_excludes_node_actions(broker_with_node_service):
    """$node.actions skipInternal=True must exclude $node.* actions."""
    broker = broker_with_node_service
    result = await broker.call("$node.actions", {"skipInternal": True})
    action_names = [a["name"] for a in result]
    for name in action_names:
        assert not name.startswith("$"), f"Internal action leaked: {name}"


@pytest.mark.asyncio
async def test_node_events_returns_list(broker_with_node_service):
    """$node.events must return a list."""
    broker = broker_with_node_service
    result = await broker.call("$node.events", {})
    assert isinstance(result, list)


@pytest.mark.asyncio
async def test_node_health_returns_health_dict(broker_with_node_service):
    """$node.health must return a dict with standard health keys."""
    broker = broker_with_node_service
    result = await broker.call("$node.health", {})
    assert isinstance(result, dict)
    for key in ("cpu", "mem", "os", "process", "client", "net", "time"):
        assert key in result, f"$node.health missing key: {key}"


@pytest.mark.asyncio
async def test_node_options_returns_dict(broker_with_node_service):
    """$node.options must return a dict."""
    broker = broker_with_node_service
    result = await broker.call("$node.options", {})
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_node_metrics_returns_list(broker_with_node_service):
    """$node.metrics must return a list (may be empty without MetricsMiddleware)."""
    broker = broker_with_node_service
    result = await broker.call("$node.metrics", {})
    assert isinstance(result, list)


@pytest.mark.asyncio
async def test_node_service_actions_have_cache_false(broker_with_node_service):
    """All $node.* actions must have cache=False."""
    from moleculerpy.registry import Registry

    broker = broker_with_node_service
    registry: Registry = broker.registry
    node_actions = [a for a in registry.__actions__ if str(a.name).startswith("$node.")]
    assert len(node_actions) > 0, "No $node actions found in registry"
    for act in node_actions:
        assert act.cache is False, f"Action {act.name} has cache={act.cache!r}, expected False"


@pytest.mark.asyncio
async def test_node_list_only_available_filters_unavailable(broker_with_node_service):
    """$node.list onlyAvailable=True must return only available nodes."""
    broker = broker_with_node_service
    result = await broker.call("$node.list", {"onlyAvailable": True})
    assert isinstance(result, list)
    for node in result:
        assert node["available"] is True
