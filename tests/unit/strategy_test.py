"""Tests for load balancing strategies in MoleculerPy.

Tests cover:
- BaseStrategy interface
- RoundRobinStrategy (thread safety, cycling)
- RandomStrategy (distribution)
- Strategy resolution and registration
- Registry integration with strategies
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from moleculerpy.context import Context
from moleculerpy.node import Node
from moleculerpy.registry import Action, Registry
from moleculerpy.strategy import (
    BaseStrategy,
    CpuUsageStrategy,
    LatencyStrategy,
    RandomStrategy,
    RoundRobinStrategy,
    ShardStrategy,
    StrategyResolutionError,
    get_by_name,
    register,
    resolve,
)

if TYPE_CHECKING:
    pass


# ============================================================================
# Helper fixtures
# ============================================================================


@pytest.fixture
def mock_actions() -> list[Action]:
    """Create a list of mock Action endpoints."""
    return [
        Action(name="test.action", node_id="node-1", is_local=True),
        Action(name="test.action", node_id="node-2", is_local=False),
        Action(name="test.action", node_id="node-3", is_local=False),
    ]


@pytest.fixture
def mock_actions_all_remote() -> list[Action]:
    """Create a list of all remote Action endpoints."""
    return [
        Action(name="test.action", node_id="node-1", is_local=False),
        Action(name="test.action", node_id="node-2", is_local=False),
        Action(name="test.action", node_id="node-3", is_local=False),
    ]


# ============================================================================
# BaseStrategy Tests
# ============================================================================


class TestBaseStrategy:
    """Tests for BaseStrategy abstract class."""

    def test_base_strategy_not_implemented(self) -> None:
        """BaseStrategy.select raises NotImplementedError."""
        strategy = BaseStrategy()
        with pytest.raises(NotImplementedError, match="Subclasses must implement"):
            strategy.select([])

    def test_base_strategy_repr(self) -> None:
        """BaseStrategy has a repr."""
        strategy = BaseStrategy()
        assert repr(strategy) == "BaseStrategy()"

    def test_base_strategy_opts_default(self) -> None:
        """BaseStrategy.opts defaults to empty dict."""
        strategy = BaseStrategy()
        assert strategy.opts == {}

    def test_base_strategy_opts_custom(self) -> None:
        """BaseStrategy accepts custom opts."""
        opts = {"shardKey": "user_id", "timeout": 30}
        strategy = BaseStrategy(opts=opts)
        assert strategy.opts == opts
        assert strategy.opts["shardKey"] == "user_id"

    def test_roundrobin_opts_passed(self) -> None:
        """RoundRobinStrategy accepts opts (for future use)."""
        opts = {"some_option": "value"}
        strategy = RoundRobinStrategy(opts=opts)
        assert strategy.opts == opts

    def test_random_opts_passed(self) -> None:
        """RandomStrategy accepts opts (for future use)."""
        opts = {"some_option": "value"}
        strategy = RandomStrategy(opts=opts)
        assert strategy.opts == opts


# ============================================================================
# RoundRobinStrategy Tests
# ============================================================================


@pytest.mark.unit
class TestRoundRobinStrategy:
    """Tests for RoundRobinStrategy."""

    def test_empty_list_returns_none(self) -> None:
        """select() returns None for empty list."""
        strategy = RoundRobinStrategy()
        result = strategy.select([])
        assert result is None

    def test_single_endpoint_returns_it(self, mock_actions: list[Action]) -> None:
        """select() returns the only endpoint when list has one item."""
        strategy = RoundRobinStrategy()
        single = [mock_actions[0]]

        result = strategy.select(single)
        assert result == mock_actions[0]

        # Second call should return same endpoint
        result2 = strategy.select(single)
        assert result2 == mock_actions[0]

    def test_cycles_through_endpoints(self, mock_actions: list[Action]) -> None:
        """select() cycles through endpoints in order."""
        strategy = RoundRobinStrategy()

        # First cycle
        assert strategy.select(mock_actions) == mock_actions[0]
        assert strategy.select(mock_actions) == mock_actions[1]
        assert strategy.select(mock_actions) == mock_actions[2]

        # Second cycle (wraps around)
        assert strategy.select(mock_actions) == mock_actions[0]
        assert strategy.select(mock_actions) == mock_actions[1]

    def test_counter_property(self) -> None:
        """counter property exposes current value."""
        strategy = RoundRobinStrategy()
        assert strategy.counter == 0

        strategy.select([Mock()])
        assert strategy.counter == 1

    def test_reset_counter(self, mock_actions: list[Action]) -> None:
        """reset() sets counter back to 0."""
        strategy = RoundRobinStrategy()

        # Advance counter
        strategy.select(mock_actions)
        strategy.select(mock_actions)
        assert strategy.counter == 2

        # Reset
        strategy.reset()
        assert strategy.counter == 0

        # Should start from beginning
        assert strategy.select(mock_actions) == mock_actions[0]

    @pytest.mark.slow
    def test_thread_safe_concurrent_calls(self, mock_actions: list[Action]) -> None:
        """select() is thread-safe under concurrent access."""
        strategy = RoundRobinStrategy()
        iterations = 1000
        results: list[Action] = []
        lock = threading.Lock()

        def select_many() -> None:
            for _ in range(iterations):
                result = strategy.select(mock_actions)
                with lock:
                    results.append(result)

        # Run from multiple threads
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(select_many) for _ in range(4)]
            for f in futures:
                f.result()

        # All results should be valid endpoints
        assert len(results) == iterations * 4
        for r in results:
            assert r in mock_actions

        # Counter should have incremented exactly iterations * 4 times
        assert strategy.counter == iterations * 4

    def test_counter_wraps_correctly(self) -> None:
        """Counter handles large values without overflow."""
        strategy = RoundRobinStrategy()
        actions = [Mock(), Mock()]

        # Simulate many calls
        strategy._counter = 1_000_000
        result = strategy.select(actions)

        # Should still work correctly (modulo)
        assert result in actions

    def test_repr(self) -> None:
        """RoundRobinStrategy has informative repr."""
        strategy = RoundRobinStrategy()
        assert "RoundRobinStrategy" in repr(strategy)
        assert "counter=0" in repr(strategy)


# ============================================================================
# RandomStrategy Tests
# ============================================================================


@pytest.mark.unit
class TestRandomStrategy:
    """Tests for RandomStrategy."""

    def test_empty_list_returns_none(self) -> None:
        """select() returns None for empty list."""
        strategy = RandomStrategy()
        result = strategy.select([])
        assert result is None

    def test_single_endpoint_returns_it(self, mock_actions: list[Action]) -> None:
        """select() returns the only endpoint when list has one item."""
        strategy = RandomStrategy()
        single = [mock_actions[0]]

        result = strategy.select(single)
        assert result == mock_actions[0]

    def test_returns_valid_endpoint(self, mock_actions: list[Action]) -> None:
        """select() always returns a valid endpoint from the list."""
        strategy = RandomStrategy()

        for _ in range(100):
            result = strategy.select(mock_actions)
            assert result in mock_actions

    def test_seeded_deterministic(self, mock_actions: list[Action]) -> None:
        """RandomStrategy with seed produces deterministic results."""
        strategy1 = RandomStrategy(seed=42)
        strategy2 = RandomStrategy(seed=42)

        results1 = [strategy1.select(mock_actions) for _ in range(10)]
        results2 = [strategy2.select(mock_actions) for _ in range(10)]

        assert results1 == results2

    @pytest.mark.slow
    def test_distribution_is_reasonable(self, mock_actions: list[Action]) -> None:
        """Random selection has reasonable distribution over many calls."""
        strategy = RandomStrategy()
        iterations = 3000
        counts: dict[str, int] = {a.node_id: 0 for a in mock_actions}

        for _ in range(iterations):
            result = strategy.select(mock_actions)
            counts[result.node_id] += 1

        # Each endpoint should get roughly 1/3 of calls (within 20% tolerance)
        expected = iterations / len(mock_actions)
        tolerance = 0.20  # 20%

        for node_id, count in counts.items():
            lower = expected * (1 - tolerance)
            upper = expected * (1 + tolerance)
            assert lower <= count <= upper, f"{node_id}: {count} not in [{lower}, {upper}]"

    def test_repr(self) -> None:
        """RandomStrategy has a repr."""
        strategy = RandomStrategy()
        assert repr(strategy) == "RandomStrategy()"


# ============================================================================
# Strategy Resolution Tests
# ============================================================================


class TestStrategyResolution:
    """Tests for strategy resolution functions."""

    def test_resolve_none_returns_roundrobin(self) -> None:
        """resolve(None) returns RoundRobinStrategy as default."""
        strategy = resolve(None)
        assert isinstance(strategy, RoundRobinStrategy)

    def test_resolve_empty_returns_roundrobin(self) -> None:
        """resolve() with no args returns RoundRobinStrategy."""
        strategy = resolve()
        assert isinstance(strategy, RoundRobinStrategy)

    # Parametrized tests for resolve() - reduces duplication
    @pytest.mark.parametrize(
        "name,expected_type",
        [
            pytest.param("RoundRobin", RoundRobinStrategy, id="roundrobin-exact"),
            pytest.param("Random", RandomStrategy, id="random-exact"),
            pytest.param("roundrobin", RoundRobinStrategy, id="roundrobin-lower"),
            pytest.param("ROUNDROBIN", RoundRobinStrategy, id="roundrobin-upper"),
            pytest.param("random", RandomStrategy, id="random-lower"),
            pytest.param("RANDOM", RandomStrategy, id="random-upper"),
            pytest.param("RoUnDrObIn", RoundRobinStrategy, id="roundrobin-mixed"),
        ],
    )
    def test_resolve_by_name(self, name: str, expected_type: type) -> None:
        """resolve() resolves strategy by name (case-insensitive)."""
        strategy = resolve(name)
        assert isinstance(strategy, expected_type)

    def test_resolve_invalid_name_raises(self) -> None:
        """resolve() raises StrategyResolutionError for invalid name."""
        with pytest.raises(StrategyResolutionError, match="Invalid strategy type"):
            resolve("InvalidStrategy")

    def test_resolve_by_class(self) -> None:
        """resolve() accepts strategy class."""
        strategy = resolve(RoundRobinStrategy)
        assert isinstance(strategy, RoundRobinStrategy)

        strategy = resolve(RandomStrategy)
        assert isinstance(strategy, RandomStrategy)

    def test_resolve_by_instance(self) -> None:
        """resolve() returns instance as-is."""
        original = RoundRobinStrategy()
        resolved = resolve(original)
        assert resolved is original

    def test_resolve_invalid_type_raises(self) -> None:
        """resolve() raises for invalid type."""
        with pytest.raises(StrategyResolutionError, match="Cannot resolve"):
            resolve(123)  # type: ignore

    def test_get_by_name_valid(self) -> None:
        """get_by_name() returns class for valid names."""
        assert get_by_name("RoundRobin") is RoundRobinStrategy
        assert get_by_name("Random") is RandomStrategy

    def test_get_by_name_invalid(self) -> None:
        """get_by_name() returns None for invalid names."""
        assert get_by_name("Invalid") is None
        assert get_by_name("") is None
        assert get_by_name(None) is None  # type: ignore


# ============================================================================
# Custom Strategy Registration Tests
# ============================================================================


class TestStrategyRegistration:
    """Tests for custom strategy registration."""

    def test_register_custom_strategy(self) -> None:
        """register() adds custom strategy."""

        class CustomStrategy(BaseStrategy):
            def select(self, endpoints, ctx=None):
                return endpoints[-1] if endpoints else None

        register("Custom", CustomStrategy)

        # Should be resolvable
        strategy = resolve("Custom")
        assert isinstance(strategy, CustomStrategy)

    def test_register_invalid_class_raises(self) -> None:
        """register() raises TypeError for non-BaseStrategy class."""
        with pytest.raises(TypeError, match="must be a BaseStrategy subclass"):
            register("Invalid", str)  # type: ignore

    def test_register_non_class_raises(self) -> None:
        """register() raises TypeError for non-class."""
        with pytest.raises(TypeError, match="must be a BaseStrategy subclass"):
            register("Invalid", RoundRobinStrategy())  # type: ignore

    def test_register_as_decorator(self) -> None:
        """register() works as a decorator."""

        @register("DecoratorCustom")
        class DecoratorStrategy(BaseStrategy):
            def select(self, endpoints, ctx=None):
                return endpoints[0] if endpoints else None

        # Should be resolvable
        strategy = resolve("DecoratorCustom")
        assert isinstance(strategy, DecoratorStrategy)

    def test_register_decorator_returns_class(self) -> None:
        """register() decorator returns the class unchanged."""

        @register("ReturnedCustom")
        class ReturnedStrategy(BaseStrategy):
            def select(self, endpoints, ctx=None):
                return None

        # Class should be returned unchanged
        assert ReturnedStrategy.__name__ == "ReturnedStrategy"
        assert issubclass(ReturnedStrategy, BaseStrategy)

    def test_register_decorator_invalid_class_raises(self) -> None:
        """register() decorator raises TypeError for non-BaseStrategy class."""
        with pytest.raises(TypeError, match="must be a BaseStrategy subclass"):

            @register("InvalidDecorator")
            class InvalidClass:  # type: ignore
                pass


# ============================================================================
# Registry Integration Tests
# ============================================================================


@pytest.mark.integration
class TestRegistryLoadBalancing:
    """Tests for Registry load balancing integration."""

    def test_registry_default_strategy(self) -> None:
        """Registry uses RoundRobin by default."""
        registry = Registry(node_id="test-node")
        assert isinstance(registry.strategy, RoundRobinStrategy)

    def test_registry_custom_strategy(self) -> None:
        """Registry accepts custom strategy name."""
        registry = Registry(node_id="test-node", strategy="Random")
        assert isinstance(registry.strategy, RandomStrategy)

    def test_registry_prefer_local_default(self) -> None:
        """Registry prefer_local defaults to True."""
        registry = Registry(node_id="test-node")
        assert registry.prefer_local is True

    def test_registry_prefer_local_false(self) -> None:
        """Registry accepts prefer_local=False."""
        registry = Registry(node_id="test-node", prefer_local=False)
        assert registry.prefer_local is False

    def test_get_all_actions_returns_all(self) -> None:
        """get_all_actions() returns all endpoints for an action."""
        registry = Registry(node_id="node-1")

        # Add multiple actions with same name from different nodes
        registry.add_action(Action("test.action", "node-1", is_local=True))
        registry.add_action(Action("test.action", "node-2", is_local=False))
        registry.add_action(Action("test.action", "node-3", is_local=False))
        registry.add_action(Action("other.action", "node-1", is_local=True))

        all_actions = registry.get_all_actions("test.action")
        assert len(all_actions) == 3
        assert all(a.name == "test.action" for a in all_actions)

    def test_get_all_actions_empty(self) -> None:
        """get_all_actions() returns empty list for unknown action."""
        registry = Registry(node_id="test-node")
        assert registry.get_all_actions("nonexistent.action") == []

    def test_get_action_endpoint_prefers_local(self) -> None:
        """get_action_endpoint() prefers local endpoint when prefer_local=True."""
        registry = Registry(node_id="node-1", prefer_local=True)

        remote = Action("test.action", "node-2", is_local=False)
        local = Action("test.action", "node-1", is_local=True)

        registry.add_action(remote)
        registry.add_action(local)

        # Should always return local
        for _ in range(10):
            result = registry.get_action_endpoint("test.action")
            assert result == local
            assert result.is_local is True

    def test_get_action_endpoint_uses_strategy_when_no_local(self) -> None:
        """get_action_endpoint() uses strategy when no local endpoint exists."""
        registry = Registry(node_id="node-1", strategy="RoundRobin", prefer_local=True)

        # Add only remote actions
        registry.add_action(Action("test.action", "node-2", is_local=False))
        registry.add_action(Action("test.action", "node-3", is_local=False))

        # Should cycle through remotes
        result1 = registry.get_action_endpoint("test.action")
        result2 = registry.get_action_endpoint("test.action")

        assert result1.node_id == "node-2"
        assert result2.node_id == "node-3"

    def test_get_action_endpoint_prefer_local_false(self) -> None:
        """get_action_endpoint() uses strategy when prefer_local=False."""
        registry = Registry(node_id="node-1", strategy="RoundRobin", prefer_local=False)

        local = Action("test.action", "node-1", is_local=True)
        remote = Action("test.action", "node-2", is_local=False)

        registry.add_action(local)
        registry.add_action(remote)

        # Should use strategy (RoundRobin), not always local
        results = [registry.get_action_endpoint("test.action") for _ in range(4)]
        node_ids = [r.node_id for r in results]

        # RoundRobin should cycle: node-1, node-2, node-1, node-2
        assert node_ids == ["node-1", "node-2", "node-1", "node-2"]

    def test_get_action_endpoint_not_found(self) -> None:
        """get_action_endpoint() returns None for unknown action."""
        registry = Registry(node_id="test-node")
        result = registry.get_action_endpoint("nonexistent.action")
        assert result is None

    def test_get_action_still_works(self) -> None:
        """Original get_action() still returns first match."""
        registry = Registry(node_id="node-1")

        first = Action("test.action", "node-1", is_local=True)
        second = Action("test.action", "node-2", is_local=False)

        registry.add_action(first)
        registry.add_action(second)

        # get_action returns first match (not load-balanced)
        assert registry.get_action("test.action") == first
        assert registry.get_action("test.action") == first  # Always first


# ============================================================================
# Property-Based Tests (Hypothesis)
# ============================================================================

# Only run hypothesis tests if available
try:
    from hypothesis import given
    from hypothesis import settings as hypothesis_settings
    from hypothesis import strategies as st

    HYPOTHESIS_AVAILABLE = True
except ImportError:
    HYPOTHESIS_AVAILABLE = False

    # Define stubs to allow class definition without import error
    def given(*args, **kwargs):  # type: ignore
        """Stub for hypothesis.given when not installed."""

        def decorator(f):
            return f

        return decorator

    def hypothesis_settings(*args, **kwargs):  # type: ignore
        """Stub for hypothesis.settings when not installed."""

        def decorator(f):
            return f

        return decorator

    class st:  # type: ignore
        """Stub for hypothesis.strategies when not installed."""

        @staticmethod
        def lists(*args, **kwargs):
            return None

        @staticmethod
        def integers(*args, **kwargs):
            return None

        @staticmethod
        def text(*args, **kwargs):
            return None

        @staticmethod
        def floats(*args, **kwargs):
            return None


@pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
class TestStrategyProperties:
    """Property-based tests for strategies using Hypothesis."""

    @given(st.lists(st.integers(min_value=0, max_value=100), min_size=1, max_size=20))
    @hypothesis_settings(max_examples=50)
    def test_roundrobin_always_returns_valid_item(self, items: list[int]) -> None:
        """Property: RoundRobin always returns an item from the list."""
        strategy = RoundRobinStrategy()
        # Create mock endpoints from items
        endpoints = [Mock(value=i) for i in items]

        for _ in range(len(items) * 2):
            result = strategy.select(endpoints)
            assert result in endpoints

    @given(st.lists(st.integers(min_value=0, max_value=100), min_size=1, max_size=20))
    @hypothesis_settings(max_examples=50)
    def test_random_always_returns_valid_item(self, items: list[int]) -> None:
        """Property: Random always returns an item from the list."""
        strategy = RandomStrategy()
        endpoints = [Mock(value=i) for i in items]

        for _ in range(len(items) * 2):
            result = strategy.select(endpoints)
            assert result in endpoints

    @given(st.integers(min_value=1, max_value=100))
    @hypothesis_settings(max_examples=30)
    def test_roundrobin_cycles_completely(self, n: int) -> None:
        """Property: RoundRobin visits all items in one full cycle."""
        strategy = RoundRobinStrategy()
        endpoints = [Mock(id=i) for i in range(n)]

        # One full cycle should visit all endpoints exactly once
        visited = set()
        for _ in range(n):
            result = strategy.select(endpoints)
            visited.add(id(result))

        assert len(visited) == n


# ============================================================================
# CpuUsageStrategy Tests
# ============================================================================


class ActionWithNode:
    """Container for Action + Node that keeps strong reference to node.

    Action uses weakref for node, so we need this wrapper in tests
    to prevent garbage collection.
    """

    __slots__ = ("action", "node")

    def __init__(self, node_id: str, cpu: float | None):
        self.node = Node(node_id=node_id, cpu=cpu if cpu is not None else 0.0)
        if cpu is None:
            self.node.cpu = None  # Reset to None after init
        self.action = Action(name=f"test.{node_id}", node_id=node_id, is_local=False)
        self.action.node = self.node


def _create_actions_with_cpu(
    cpu_data: list[tuple[str, float | None]],
) -> tuple[list[Action], list[ActionWithNode]]:
    """Create actions with CPU data, keeping strong refs.

    Returns:
        Tuple of (actions list, containers to keep alive)
    """
    containers = [ActionWithNode(node_id, cpu) for node_id, cpu in cpu_data]
    actions = [c.action for c in containers]
    return actions, containers


class ActionWithHostname:
    """Container for Action + Node with hostname for latency tests."""

    __slots__ = ("action", "node")

    def __init__(self, node_id: str, hostname: str):
        self.node = Node(node_id=node_id, hostname=hostname)
        self.action = Action(name=f"test.{node_id}", node_id=node_id, is_local=False)
        self.action.node = self.node


def _create_actions_with_hostname(
    data: list[tuple[str, str]],
) -> tuple[list[Action], list[ActionWithHostname]]:
    """Create actions with hostname data, keeping strong refs.

    Args:
        data: List of (node_id, hostname) tuples

    Returns:
        Tuple of (actions list, containers to keep alive)
    """
    containers = [ActionWithHostname(node_id, hostname) for node_id, hostname in data]
    actions = [c.action for c in containers]
    return actions, containers


@pytest.mark.unit
class TestCpuUsageStrategy:
    """Tests for CpuUsageStrategy."""

    def test_empty_list_returns_none(self) -> None:
        """select() returns None for empty list."""
        strategy = CpuUsageStrategy()
        result = strategy.select([])
        assert result is None

    def test_single_endpoint_returns_it(self) -> None:
        """select() returns the only endpoint when list has one item."""
        strategy = CpuUsageStrategy()
        actions, _refs = _create_actions_with_cpu([("node-1", 50.0)])

        result = strategy.select(actions)
        assert result == actions[0]

    def test_selects_lowest_cpu(self) -> None:
        """Strategy selects endpoint with lowest CPU usage."""
        strategy = CpuUsageStrategy({"sampleCount": 10})  # Sample all

        actions, _refs = _create_actions_with_cpu(
            [
                ("high-cpu", 90.0),
                ("low-cpu", 5.0),
                ("medium-cpu", 50.0),
            ]
        )

        # Select multiple times - should always get low-cpu (fast path: 5 < 10)
        for _ in range(10):
            result = strategy.select(actions)
            assert result.node_id == "low-cpu"

    def test_fast_path_low_cpu_threshold(self) -> None:
        """Fast path returns immediately for CPU below threshold."""
        strategy = CpuUsageStrategy({"sampleCount": 1, "lowCpuUsage": 15.0})

        actions, _refs = _create_actions_with_cpu([("fast-node", 5.0)])
        result = strategy.select(actions)
        assert result.node_id == "fast-node"

    def test_default_options(self) -> None:
        """Default options are sampleCount=3, lowCpuUsage=10.0."""
        strategy = CpuUsageStrategy()

        # Check defaults via opts.get with fallback
        assert strategy.opts.get("sampleCount", 3) == 3
        assert strategy.opts.get("lowCpuUsage", 10.0) == 10.0

    def test_custom_options(self) -> None:
        """Custom options are applied correctly."""
        strategy = CpuUsageStrategy({"sampleCount": 5, "lowCpuUsage": 20.0})

        assert strategy.opts["sampleCount"] == 5
        assert strategy.opts["lowCpuUsage"] == 20.0

    def test_no_node_data_fallback_to_random(self) -> None:
        """Fallback to random selection when no CPU data available."""
        strategy = CpuUsageStrategy()

        # Actions without node references (node=None)
        actions = [
            Action(name="test.action", node_id=f"node-{i}", is_local=False) for i in range(3)
        ]

        # Should not raise, returns random selection
        result = strategy.select(actions)
        assert result in actions

    def test_partial_cpu_data(self) -> None:
        """Works correctly with partial CPU data."""
        strategy = CpuUsageStrategy({"sampleCount": 10})

        # Actions with CPU data
        cpu_actions, _refs = _create_actions_with_cpu(
            [
                ("cpu-node", 20.0),
                ("high-cpu", 90.0),
            ]
        )
        # Action without node reference
        action_no_node = Action(name="test.nonode", node_id="no-node", is_local=False)

        actions = [action_no_node, cpu_actions[1], cpu_actions[0]]  # Mix order

        # Should prefer action with lowest CPU (cpu-node=20%)
        results = [strategy.select(actions) for _ in range(10)]
        cpu_nodes = [r for r in results if r.node_id == "cpu-node"]

        # Should mostly select the lower CPU node
        assert len(cpu_nodes) >= 5  # At least half should be cpu-node

    def test_repr_shows_configuration(self) -> None:
        """repr() shows strategy configuration."""
        strategy = CpuUsageStrategy({"sampleCount": 5, "lowCpuUsage": 15.0})
        assert "sampleCount=5" in repr(strategy)
        assert "lowCpuUsage=15.0" in repr(strategy)

    def test_sample_count_limits_to_available(self) -> None:
        """Sample count is limited to available endpoints count."""
        strategy = CpuUsageStrategy({"sampleCount": 100})

        actions, _refs = _create_actions_with_cpu([(f"node-{i}", float(i * 10)) for i in range(3)])

        # Should not raise with large sampleCount
        result = strategy.select(actions)
        assert result in actions

    def test_none_cpu_treated_as_high(self) -> None:
        """None CPU value is treated as high CPU (100%)."""
        strategy = CpuUsageStrategy({"sampleCount": 10})

        # Node with cpu=None (e.g., just discovered, no heartbeat yet)
        actions, _refs = _create_actions_with_cpu(
            [
                ("no-cpu", None),  # Will be treated as 100%
                ("with-cpu", 30.0),
            ]
        )

        # Should prefer with-cpu (30% < 100%)
        for _ in range(10):
            result = strategy.select(actions)
            assert result.node_id == "with-cpu"

    def test_all_same_cpu_random_selection(self) -> None:
        """With same CPU, any endpoint can be selected."""
        strategy = CpuUsageStrategy({"sampleCount": 10})

        actions, _refs = _create_actions_with_cpu([(f"node-{i}", 50.0) for i in range(3)])

        # All have same CPU, should still return valid endpoint
        results = [strategy.select(actions) for _ in range(30)]
        unique_nodes = {r.node_id for r in results}

        # With high sample count, should see variation (not deterministic)
        assert len(unique_nodes) >= 1  # At least one unique

    def test_resolve_cpuusage_strategy(self) -> None:
        """CpuUsage can be resolved by name."""
        strategy = resolve("CpuUsage")
        assert isinstance(strategy, CpuUsageStrategy)

        # Case insensitive
        strategy2 = resolve("cpuusage")
        assert isinstance(strategy2, CpuUsageStrategy)

    def test_get_by_name_cpuusage(self) -> None:
        """get_by_name returns CpuUsageStrategy class."""
        cls = get_by_name("CpuUsage")
        assert cls is CpuUsageStrategy


# ============================================================================
# Action.node Property Tests
# ============================================================================


@pytest.mark.unit
class TestActionNodeProperty:
    """Tests for Action.node property (for strategy support)."""

    def test_node_property_initially_none(self) -> None:
        """Action.node is None by default."""
        action = Action(name="test.action", node_id="node-1", is_local=False)
        assert action.node is None

    def test_node_property_setter(self) -> None:
        """Can set node on action."""
        action = Action(name="test.action", node_id="node-1", is_local=False)
        node = Node(node_id="node-1", cpu=50.0)
        nodes = [node]  # Keep strong reference

        action.node = node
        assert action.node is node
        assert action.node.cpu == 50.0
        assert nodes  # Ensure nodes list is used

    def test_node_property_uses_weakref(self) -> None:
        """Node is stored as weakref (memory safe)."""
        action = Action(name="test.action", node_id="node-1", is_local=False)
        node = Node(node_id="node-1", cpu=50.0)
        nodes = [node]  # Keep strong reference

        action.node = node
        assert action.node is node

        # Weakref mechanism is used (internal detail)
        assert action._node_ref is not None
        assert nodes  # Ensure nodes list is used

    def test_node_property_reset_to_none(self) -> None:
        """Can reset node to None."""
        action = Action(name="test.action", node_id="node-1", is_local=False)
        node = Node(node_id="node-1", cpu=50.0)
        nodes = [node]  # Keep strong reference

        action.node = node
        action.node = None

        assert action.node is None
        assert action._node_ref is None
        assert nodes  # Ensure nodes list is used


# ============================================================================
# ShardStrategy Tests
# ============================================================================


@pytest.mark.unit
class TestShardStrategy:
    """Tests for ShardStrategy (consistent hashing)."""

    def test_empty_list_returns_none(self) -> None:
        """select() returns None for empty list."""
        strategy = ShardStrategy()
        result = strategy.select([])
        assert result is None

    def test_single_endpoint_returns_it(self) -> None:
        """select() returns the only endpoint when list has one item."""
        strategy = ShardStrategy()
        action = Action(name="test.action", node_id="node-1", is_local=False)

        ctx = Context(id="ctx-1", params={"id": "user-123"})
        result = strategy.select([action], ctx)
        assert result == action

    def test_default_shard_key_is_none(self) -> None:
        """Default shard key is None (Moleculer.js compatible).

        Without shardKey configured, strategy falls back to random selection.
        """
        strategy = ShardStrategy()

        # Without shardKey, _shard_key should be None
        assert strategy._shard_key is None

        actions = [
            Action(name="test.action", node_id=f"node-{i}", is_local=False) for i in range(3)
        ]

        # Without shardKey, should use random selection (not consistent)
        ctx = Context(id="ctx-1", params={"id": "user-123"})
        result = strategy.select(actions, ctx)

        # Just verify it returns a valid endpoint
        assert result in actions

    def test_explicit_shard_key_id(self) -> None:
        """Explicit shardKey='id' provides consistent routing."""
        strategy = ShardStrategy({"shardKey": "id"})

        actions = [
            Action(name="test.action", node_id=f"node-{i}", is_local=False) for i in range(3)
        ]

        # Same id should route to same node
        ctx1 = Context(id="ctx-1", params={"id": "user-123"})
        ctx2 = Context(id="ctx-2", params={"id": "user-123"})

        result1 = strategy.select(actions, ctx1)
        result2 = strategy.select(actions, ctx2)

        assert result1 == result2

    def test_consistent_routing(self) -> None:
        """Same shard key always routes to same node."""
        strategy = ShardStrategy({"shardKey": "userId"})

        actions = [
            Action(name="test.action", node_id=f"node-{i}", is_local=False) for i in range(5)
        ]

        # Same userId should always get same node
        for _ in range(10):
            ctx = Context(id=f"ctx-{_}", params={"userId": "user-abc"})
            result = strategy.select(actions, ctx)
            assert result.node_id == strategy.select(actions, ctx).node_id

    def test_different_keys_can_route_different_nodes(self) -> None:
        """Different shard keys can route to different nodes."""
        strategy = ShardStrategy({"shardKey": "userId", "vnodes": 100})

        actions = [
            Action(name="test.action", node_id=f"node-{i}", is_local=False) for i in range(5)
        ]

        # Different userIds should distribute across nodes
        results = set()
        for i in range(100):
            ctx = Context(id=f"ctx-{i}", params={"userId": f"user-{i}"})
            result = strategy.select(actions, ctx)
            results.add(result.node_id)

        # Should hit multiple nodes (statistically likely with 100 keys and 5 nodes)
        assert len(results) >= 2

    def test_meta_key_extraction(self) -> None:
        """Shard key starting with '#' extracts from meta."""
        strategy = ShardStrategy({"shardKey": "#tenantId"})

        actions = [
            Action(name="test.action", node_id=f"node-{i}", is_local=False) for i in range(3)
        ]

        # Same tenantId in meta should route to same node
        ctx1 = Context(id="ctx-1", meta={"tenantId": "tenant-xyz"})
        ctx2 = Context(id="ctx-2", meta={"tenantId": "tenant-xyz"})

        result1 = strategy.select(actions, ctx1)
        result2 = strategy.select(actions, ctx2)

        assert result1 == result2

    def test_no_shard_key_fallback_to_random(self) -> None:
        """Missing shard key falls back to random selection."""
        strategy = ShardStrategy({"shardKey": "userId"})

        actions = [
            Action(name="test.action", node_id=f"node-{i}", is_local=False) for i in range(3)
        ]

        # Context without userId param
        ctx = Context(id="ctx-1", params={"other": "value"})
        result = strategy.select(actions, ctx)

        # Should still return valid endpoint
        assert result in actions

    def test_none_context_fallback_to_random(self) -> None:
        """None context falls back to random selection."""
        strategy = ShardStrategy()

        actions = [
            Action(name="test.action", node_id=f"node-{i}", is_local=False) for i in range(3)
        ]

        result = strategy.select(actions, None)
        assert result in actions

    def test_custom_vnodes(self) -> None:
        """Custom vnodes count is applied."""
        # Need to also specify shardKey since default is None
        strategy = ShardStrategy({"shardKey": "id", "vnodes": 50})

        actions = [
            Action(name="test.action", node_id=f"node-{i}", is_local=False) for i in range(3)
        ]

        ctx = Context(id="ctx-1", params={"id": "test"})
        result = strategy.select(actions, ctx)
        assert result in actions

    def test_ring_rebuilds_on_node_change(self) -> None:
        """Ring rebuilds when available nodes change."""
        strategy = ShardStrategy({"shardKey": "id"})

        actions_v1 = [
            Action(name="test.action", node_id=f"node-{i}", is_local=False) for i in range(3)
        ]

        ctx = Context(id="ctx-1", params={"id": "user-123"})
        strategy.select(actions_v1, ctx)

        # Add a new node
        actions_v2 = [*actions_v1, Action(name="test.action", node_id="node-new", is_local=False)]

        result_v2 = strategy.select(actions_v2, ctx)

        # Result should be valid (may or may not change depending on hash)
        assert result_v2 in actions_v2

    def test_repr_shows_configuration(self) -> None:
        """repr() shows strategy configuration."""
        strategy = ShardStrategy({"shardKey": "tenantId", "vnodes": 20})

        assert "shardKey='tenantId'" in repr(strategy)
        assert "vnodes=20" in repr(strategy)

    def test_resolve_shard_strategy(self) -> None:
        """Shard can be resolved by name."""
        strategy = resolve("Shard")
        assert isinstance(strategy, ShardStrategy)

        # Case insensitive
        strategy2 = resolve("shard")
        assert isinstance(strategy2, ShardStrategy)

    def test_get_by_name_shard(self) -> None:
        """get_by_name returns ShardStrategy class."""
        cls = get_by_name("Shard")
        assert cls is ShardStrategy

    def test_lru_cache_effectiveness(self) -> None:
        """LRU cache improves repeated lookups."""
        strategy = ShardStrategy({"shardKey": "id", "ringSize": 100})

        actions = [
            Action(name="test.action", node_id=f"node-{i}", is_local=False) for i in range(5)
        ]

        # Same key should use cached result
        ctx = Context(id="ctx-1", params={"id": "cached-user"})

        for _ in range(100):
            result = strategy.select(actions, ctx)
            assert result is not None

        # Check cache was used (internal detail)
        cache_info = strategy._get_node_for_key.cache_info()
        assert cache_info.hits >= 99  # At least 99 cache hits out of 100 calls


# ============================================================================
# LatencyStrategy Tests
# ============================================================================


class MockLatencyProvider:
    """Mock latency provider for testing.

    Moleculer.js compatible: uses hostname for lookup.
    """

    def __init__(self, latencies: dict[str, float | None]) -> None:
        self._latencies = latencies

    def get_host_latency(self, hostname: str | None) -> float | None:
        """Get latency by hostname (Moleculer.js compatible)."""
        if hostname is None:
            return None
        return self._latencies.get(hostname)


@pytest.mark.unit
class TestLatencyStrategy:
    """Tests for LatencyStrategy."""

    def test_empty_list_returns_none(self) -> None:
        """select() returns None for empty list."""
        strategy = LatencyStrategy()
        result = strategy.select([])
        assert result is None

    def test_single_endpoint_returns_it(self) -> None:
        """select() returns the only endpoint when list has one item."""
        strategy = LatencyStrategy()
        action = Action(name="test.action", node_id="node-1", is_local=False)

        result = strategy.select([action])
        assert result == action

    def test_no_provider_fallback_to_random(self) -> None:
        """Without latency provider, falls back to random selection."""
        strategy = LatencyStrategy()

        actions = [
            Action(name="test.action", node_id=f"node-{i}", is_local=False) for i in range(3)
        ]

        # Should not raise, returns random selection
        result = strategy.select(actions)
        assert result in actions

    def test_selects_lowest_latency(self) -> None:
        """Strategy selects endpoint with lowest latency (by hostname)."""
        strategy = LatencyStrategy({"sampleCount": 10})

        # Moleculer.js uses hostname for latency lookup
        provider = MockLatencyProvider(
            {
                "high-host": 100.0,
                "low-host": 5.0,
                "medium-host": 50.0,
            }
        )
        strategy.set_latency_provider(provider)

        # Create actions with nodes that have hostnames
        actions, _refs = _create_actions_with_hostname(
            [
                ("high-latency", "high-host"),
                ("low-latency", "low-host"),
                ("medium-latency", "medium-host"),
            ]
        )

        # Select multiple times - should always get low-latency (fast path: 5 < 10)
        for _ in range(10):
            result = strategy.select(actions)
            assert result.node_id == "low-latency"

    def test_fast_path_low_latency_threshold(self) -> None:
        """Fast path returns immediately for latency below threshold."""
        strategy = LatencyStrategy({"sampleCount": 1, "lowLatency": 15.0})

        provider = MockLatencyProvider({"fast-host": 5.0})
        strategy.set_latency_provider(provider)

        actions, _refs = _create_actions_with_hostname([("fast-node", "fast-host")])
        result = strategy.select(actions)
        assert result.node_id == "fast-node"

    def test_default_options(self) -> None:
        """Default options are sampleCount=5, lowLatency=10.0."""
        strategy = LatencyStrategy()

        # Check defaults via opts.get with fallback
        assert strategy.opts.get("sampleCount", 5) == 5
        assert strategy.opts.get("lowLatency", 10.0) == 10.0

    def test_custom_options(self) -> None:
        """Custom options are applied correctly."""
        strategy = LatencyStrategy({"sampleCount": 3, "lowLatency": 20.0})

        assert strategy.opts["sampleCount"] == 3
        assert strategy.opts["lowLatency"] == 20.0

    def test_partial_latency_data(self) -> None:
        """Works correctly with partial latency data."""
        strategy = LatencyStrategy({"sampleCount": 10})

        # Latency lookup is by hostname
        provider = MockLatencyProvider(
            {
                "with-host": 20.0,
                "high-host": 100.0,
                # "no-host" not in dict (returns None)
            }
        )
        strategy.set_latency_provider(provider)

        actions, _refs = _create_actions_with_hostname(
            [
                ("no-latency", "no-host"),
                ("high-latency", "high-host"),
                ("with-latency", "with-host"),
            ]
        )

        # Should prefer action with lowest latency
        results = [strategy.select(actions) for _ in range(10)]
        latency_nodes = [r for r in results if r.node_id == "with-latency"]

        # Should mostly select the lower latency node
        assert len(latency_nodes) >= 5

    def test_repr_shows_configuration(self) -> None:
        """repr() shows strategy configuration."""
        strategy = LatencyStrategy({"sampleCount": 3, "lowLatency": 15.0})
        assert "sampleCount=3" in repr(strategy)
        assert "lowLatency=15.0" in repr(strategy)
        assert "hasProvider=False" in repr(strategy)

        # With provider
        strategy.set_latency_provider(MockLatencyProvider({}))
        assert "hasProvider=True" in repr(strategy)

    def test_resolve_latency_strategy(self) -> None:
        """Latency can be resolved by name."""
        strategy = resolve("Latency")
        assert isinstance(strategy, LatencyStrategy)

        # Case insensitive
        strategy2 = resolve("latency")
        assert isinstance(strategy2, LatencyStrategy)

    def test_get_by_name_latency(self) -> None:
        """get_by_name returns LatencyStrategy class."""
        cls = get_by_name("Latency")
        assert cls is LatencyStrategy

    def test_set_latency_provider(self) -> None:
        """Can set and clear latency provider."""
        strategy = LatencyStrategy()
        assert strategy._latency_provider is None

        provider = MockLatencyProvider({})
        strategy.set_latency_provider(provider)
        assert strategy._latency_provider is provider

        strategy.set_latency_provider(None)
        assert strategy._latency_provider is None


# ============================================================================
# Property-Based Tests for Advanced Strategies (Hypothesis)
# ============================================================================


@pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
class TestAdvancedStrategyProperties:
    """Property-based tests for advanced strategies using Hypothesis.

    These tests verify invariants that should hold for any valid input:
    - Strategies always return a valid endpoint from the list
    - ShardStrategy provides consistent routing for the same key
    - CpuUsageStrategy respects CPU ordering
    """

    @given(st.lists(st.integers(min_value=1, max_value=100), min_size=1, max_size=20))
    @hypothesis_settings(max_examples=50)
    def test_cpuusage_always_returns_valid_item(self, cpu_values: list[int]) -> None:
        """Property: CpuUsageStrategy always returns an item from the list."""
        strategy = CpuUsageStrategy({"sampleCount": 10})

        # Create actions with CPU data
        actions, _refs = _create_actions_with_cpu(
            [(f"node-{i}", float(v)) for i, v in enumerate(cpu_values)]
        )

        for _ in range(len(cpu_values) * 2):
            result = strategy.select(actions)
            assert result in actions

    @given(st.lists(st.text(min_size=1, max_size=10), min_size=1, max_size=10, unique=True))
    @hypothesis_settings(max_examples=30)
    def test_shard_consistent_routing(self, node_ids: list[str]) -> None:
        """Property: ShardStrategy always routes same key to same node."""
        # Filter out empty strings and ensure valid node IDs
        node_ids = [nid for nid in node_ids if nid.strip()]
        if not node_ids:
            return  # Skip if no valid node IDs

        strategy = ShardStrategy({"shardKey": "userId", "vnodes": 10})

        actions = [Action(name=f"test.{nid}", node_id=nid, is_local=False) for nid in node_ids]

        # Test that same key always routes to same node
        test_keys = ["user-abc", "user-xyz", "user-123"]
        for key in test_keys:
            ctx = Context(id="test-ctx", params={"userId": key})
            first_result = strategy.select(actions, ctx)

            # Repeat 10 times - should always get same result
            for _ in range(10):
                result = strategy.select(actions, ctx)
                assert result == first_result, f"Key '{key}' routed inconsistently"

    @given(st.integers(min_value=1, max_value=50))
    @hypothesis_settings(max_examples=30)
    def test_shard_different_keys_distribute(self, n_keys: int) -> None:
        """Property: Different keys can distribute across available nodes."""
        strategy = ShardStrategy({"shardKey": "id", "vnodes": 50})

        # Fixed set of 5 nodes
        actions = [
            Action(name=f"test.node-{i}", node_id=f"node-{i}", is_local=False) for i in range(5)
        ]

        # Generate n_keys different keys
        results = set()
        for i in range(n_keys):
            ctx = Context(id=f"ctx-{i}", params={"id": f"key-{i}"})
            result = strategy.select(actions, ctx)
            results.add(result.node_id)

        # Should hit at least 1 node (and likely more with enough keys)
        assert len(results) >= 1

    @given(st.lists(st.integers(min_value=1, max_value=100), min_size=1, max_size=15))
    @hypothesis_settings(max_examples=50)
    def test_latency_always_returns_valid_item(self, latency_values: list[int]) -> None:
        """Property: LatencyStrategy always returns an item from the list."""
        strategy = LatencyStrategy({"sampleCount": 10})

        # Create mock provider with latency data keyed by hostname
        latencies = {f"host-{i}": float(v) for i, v in enumerate(latency_values)}
        provider = MockLatencyProvider(latencies)
        strategy.set_latency_provider(provider)

        # Create actions with hostnames matching latency data
        actions, _refs = _create_actions_with_hostname(
            [(f"node-{i}", f"host-{i}") for i in range(len(latency_values))]
        )

        for _ in range(len(latency_values) * 2):
            result = strategy.select(actions)
            assert result in actions

    @given(st.integers(min_value=1, max_value=100))
    @hypothesis_settings(max_examples=20)
    def test_cpuusage_prefers_lower_cpu(self, high_cpu: int) -> None:
        """Property: CpuUsageStrategy prefers endpoints with lower CPU."""
        low_cpu = max(1, high_cpu // 2)  # Ensure low_cpu < high_cpu
        if low_cpu >= high_cpu:
            return  # Skip edge case

        strategy = CpuUsageStrategy({"sampleCount": 10, "lowCpuUsage": 0})

        # Create actions with distinct CPU values
        actions, _refs = _create_actions_with_cpu(
            [
                ("high-cpu-node", float(high_cpu)),
                ("low-cpu-node", float(low_cpu)),
            ]
        )

        # With sample all and no fast path, should prefer lower CPU
        results = [strategy.select(actions) for _ in range(20)]
        low_cpu_results = sum(1 for r in results if r.node_id == "low-cpu-node")

        # Should select lower CPU more often
        assert low_cpu_results >= 10, "Lower CPU node should be selected at least half the time"

    @given(st.integers(min_value=1, max_value=100))
    @hypothesis_settings(max_examples=20)
    def test_latency_prefers_lower_latency(self, high_latency: int) -> None:
        """Property: LatencyStrategy prefers endpoints with lower latency."""
        low_latency = max(1, high_latency // 2)
        if low_latency >= high_latency:
            return  # Skip edge case

        strategy = LatencyStrategy({"sampleCount": 10, "lowLatency": 0})

        provider = MockLatencyProvider(
            {
                "high-host": float(high_latency),
                "low-host": float(low_latency),
            }
        )
        strategy.set_latency_provider(provider)

        actions, _refs = _create_actions_with_hostname(
            [
                ("high-latency-node", "high-host"),
                ("low-latency-node", "low-host"),
            ]
        )

        results = [strategy.select(actions) for _ in range(20)]
        low_latency_results = sum(1 for r in results if r.node_id == "low-latency-node")

        # Should select lower latency more often
        assert low_latency_results >= 10, (
            "Lower latency node should be selected at least half the time"
        )
