"""E2E tests for v0.14.12 broker features: mcall, $node internal service, health.

Tests use real ServiceBroker with memory:// transporter.
"""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action
from moleculerpy.service import Service
from moleculerpy.settings import Settings

# =============================================================================
# Test services
# =============================================================================


class CalcService(Service):
    """Calculator service for mcall E2E tests."""

    __test__ = False
    name = "calc"

    @action()
    async def add(self, ctx) -> int:
        return ctx.params["a"] + ctx.params["b"]

    @action()
    async def sub(self, ctx) -> int:
        return ctx.params["a"] - ctx.params["b"]

    @action()
    async def mul(self, ctx) -> int:
        return ctx.params["a"] * ctx.params["b"]


class BrokenService(Service):
    """Service that always raises for settled tests."""

    __test__ = False
    name = "broken"

    @action()
    async def fail(self, ctx) -> None:
        raise RuntimeError("broken by design")


# =============================================================================
# Fixture
# =============================================================================


@pytest_asyncio.fixture
async def e2e_broker():
    """Real broker with calc + broken services."""
    settings = Settings(transporter="memory://")
    broker = ServiceBroker(id="e2e-gaps-node", settings=settings)
    await broker.register(CalcService())
    await broker.register(BrokenService())
    await broker.start()
    await asyncio.sleep(0.05)
    yield broker
    await broker.stop()


# =============================================================================
# mcall E2E tests
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_e2e_mcall_list_parallel_calls_return_correct_results(e2e_broker):
    """mcall with list format executes parallel calls and returns correct results."""
    broker = e2e_broker
    results = await broker.mcall(
        [
            {"action": "calc.add", "params": {"a": 10, "b": 5}},
            {"action": "calc.sub", "params": {"a": 10, "b": 5}},
            {"action": "calc.mul", "params": {"a": 10, "b": 5}},
        ]
    )
    assert isinstance(results, list)
    assert len(results) == 3
    assert results[0] == 15  # add
    assert results[1] == 5  # sub
    assert results[2] == 50  # mul


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_e2e_mcall_dict_names_mapped_correctly(e2e_broker):
    """mcall dict format maps result keys to user-specified names."""
    broker = e2e_broker
    results = await broker.mcall(
        {
            "addition": {"action": "calc.add", "params": {"a": 3, "b": 7}},
            "multiplication": {"action": "calc.mul", "params": {"a": 3, "b": 7}},
        }
    )
    assert isinstance(results, dict)
    assert set(results.keys()) == {"addition", "multiplication"}
    assert results["addition"] == 10
    assert results["multiplication"] == 21


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_e2e_mcall_settled_partial_failure(e2e_broker):
    """mcall settled=True: failed call yields exception, successful call yields result."""
    broker = e2e_broker
    results = await broker.mcall(
        {
            "good": {"action": "calc.add", "params": {"a": 100, "b": 1}},
            "bad": {"action": "broken.fail", "params": {}},
        },
        settled=True,
    )
    assert isinstance(results, dict)
    assert results["good"] == 101
    assert isinstance(results["bad"], RuntimeError)
    assert "broken by design" in str(results["bad"])


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_e2e_node_list_services_actions_return_data(e2e_broker):
    """$node.list, $node.services and $node.actions return data with real services."""
    broker = e2e_broker

    node_list = await broker.call("$node.list", {})
    assert isinstance(node_list, list)
    assert len(node_list) >= 1
    local_ids = [n["id"] for n in node_list]
    assert "e2e-gaps-node" in local_ids

    services = await broker.call("$node.services", {})
    assert isinstance(services, list)
    svc_names = [s["name"] for s in services]
    assert "calc" in svc_names
    assert "broken" in svc_names

    actions = await broker.call("$node.actions", {})
    assert isinstance(actions, list)
    action_names = [a["name"] for a in actions]
    assert "calc.add" in action_names
    assert "calc.sub" in action_names
    assert "calc.mul" in action_names


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_e2e_node_health_returns_valid_health_info(e2e_broker):
    """$node.health returns valid health dict from a running broker."""
    broker = e2e_broker
    health = await broker.call("$node.health", {})

    assert isinstance(health, dict)
    for key in ("cpu", "mem", "os", "process", "client", "net", "time"):
        assert key in health, f"Missing health key: {key}"

    # Verify sensible values
    assert health["client"]["type"] == "python"
    assert health["process"]["pid"] > 0
    assert health["mem"]["total"] > 0
    assert isinstance(health["time"]["now"], int)
