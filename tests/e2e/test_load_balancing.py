"""E2E tests for load balancing strategies.

Tests verify that different strategies distribute requests correctly
across multiple nodes in a cluster.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from moleculerpy.broker import ServiceBroker


@pytest.mark.asyncio
@pytest.mark.e2e
class TestRoundRobinStrategy:
    """Tests for RoundRobin load balancing."""

    async def test_distributes_evenly(self, three_broker_cluster: list[ServiceBroker]) -> None:
        """RoundRobin distributes requests evenly across nodes."""
        broker = three_broker_cluster[0]

        # Call 30 times (10 per node expected)
        node_counts: Counter[str] = Counter()
        for _ in range(30):
            result = await broker.call("counter.increment", {})
            node_counts[result["node_id"]] += 1

        # Each node should get some calls (distribution may not be perfectly even)
        counts = list(node_counts.values())
        assert len(counts) == 3, f"Expected 3 nodes, got {len(counts)}"

        # All nodes should receive at least some requests
        for count in counts:
            assert count >= 5, f"Node got too few requests: {dict(node_counts)}"

    async def test_all_nodes_receive_requests(
        self, three_broker_cluster: list[ServiceBroker]
    ) -> None:
        """All nodes in cluster receive requests."""
        broker = three_broker_cluster[0]

        seen_nodes: set[str] = set()
        for _ in range(10):
            result = await broker.call("counter.increment", {})
            seen_nodes.add(result["node_id"])

        assert len(seen_nodes) == 3, f"Not all nodes used: {seen_nodes}"


@pytest.mark.asyncio
@pytest.mark.e2e
class TestShardStrategy:
    """Tests for Shard (consistent hashing) load balancing.

    Note: Since each broker has its own strategy instance, shard consistency
    is verified within consecutive calls from the same broker after discovery.
    """

    async def test_consistent_routing(self, shard_cluster: list[ServiceBroker]) -> None:
        """Same shard key routes to same node within consecutive calls."""
        broker = shard_cluster[0]

        # Make consecutive calls with same key
        results = []
        for _ in range(5):
            result = await broker.call("shard-test.store", {"id": "user-123", "value": "data"})
            results.append(result["node_id"])

        # All consecutive calls should go to the same node
        assert len(set(results)) == 1, f"Inconsistent routing: {results}"

    async def test_different_keys_can_route_to_different_nodes(
        self, shard_cluster: list[ServiceBroker]
    ) -> None:
        """Different shard keys can route to different nodes."""
        broker = shard_cluster[0]

        # Store data with different IDs
        seen_nodes: set[str] = set()
        for i in range(50):
            result = await broker.call(
                "shard-test.store", {"id": f"key-{i}", "value": f"value-{i}"}
            )
            seen_nodes.add(result["node_id"])

        # With 50 keys and 3 nodes, should see multiple nodes
        assert len(seen_nodes) >= 2, f"All requests went to same node: {seen_nodes}"

    async def test_shard_preserves_data_locality(self, shard_cluster: list[ServiceBroker]) -> None:
        """Data stored with same key can be retrieved from same node."""
        broker = shard_cluster[0]

        # Store data
        store_result = await broker.call(
            "shard-test.store", {"id": "persistent-key", "value": "secret-data"}
        )
        store_node = store_result["node_id"]

        # Retrieve should go to same node (where data was stored)
        retrieve_result = await broker.call("shard-test.retrieve", {"id": "persistent-key"})

        assert retrieve_result["node_id"] == store_node
        assert retrieve_result["value"] == "secret-data"


@pytest.mark.asyncio
@pytest.mark.e2e
class TestRandomStrategy:
    """Tests for Random load balancing."""

    async def test_uses_multiple_nodes(self, three_broker_cluster: list[ServiceBroker]) -> None:
        """Random strategy uses multiple nodes over many requests."""
        broker = three_broker_cluster[0]

        # Change strategy to Random
        broker.registry.strategy_name = "Random"

        seen_nodes: set[str] = set()
        for _ in range(30):
            result = await broker.call("counter.increment", {})
            seen_nodes.add(result["node_id"])

        # Should use multiple nodes (statistically very likely)
        assert len(seen_nodes) >= 2, f"Random only used {len(seen_nodes)} node(s)"


@pytest.mark.asyncio
@pytest.mark.e2e
class TestClusterBasics:
    """Basic cluster operation tests."""

    async def test_call_works(self, two_broker_cluster: list[ServiceBroker]) -> None:
        """Basic call works in cluster."""
        broker = two_broker_cluster[0]

        result = await broker.call("echo.echo", {"message": "hello"})
        assert "node_id" in result
        assert result["params"]["message"] == "hello"

    async def test_all_brokers_can_call(self, two_broker_cluster: list[ServiceBroker]) -> None:
        """All brokers in cluster can make calls."""
        for broker in two_broker_cluster:
            result = await broker.call("echo.echo", {"test": "value"})
            assert result["params"]["test"] == "value"

    async def test_local_and_remote_calls(self, two_broker_cluster: list[ServiceBroker]) -> None:
        """Both local and remote calls work."""
        broker = two_broker_cluster[0]

        # Multiple calls - some should be local, some remote
        results = []
        for _ in range(10):
            result = await broker.call("counter.increment", {})
            results.append(result["node_id"])

        # Should see both nodes
        unique_nodes = set(results)
        assert len(unique_nodes) == 2, f"Expected 2 nodes, got {unique_nodes}"


@pytest.mark.asyncio
@pytest.mark.e2e
class TestConcurrentRequests:
    """Tests for concurrent request handling."""

    async def test_concurrent_calls_work(self, three_broker_cluster: list[ServiceBroker]) -> None:
        """Concurrent calls are handled correctly."""
        broker = three_broker_cluster[0]

        # Make 30 concurrent calls
        tasks = [broker.call("counter.increment", {}) for _ in range(30)]
        results = await asyncio.gather(*tasks)

        # All should succeed
        assert len(results) == 30
        for result in results:
            assert "node_id" in result
            assert "count" in result

    async def test_concurrent_calls_distributed(
        self, three_broker_cluster: list[ServiceBroker]
    ) -> None:
        """Concurrent calls are distributed across nodes."""
        broker = three_broker_cluster[0]

        tasks = [broker.call("echo.echo", {"i": i}) for i in range(30)]
        results = await asyncio.gather(*tasks)

        node_counts = Counter(r["node_id"] for r in results)

        # All nodes should receive requests
        assert len(node_counts) == 3, f"Not all nodes used: {dict(node_counts)}"
