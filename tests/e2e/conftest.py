"""E2E test fixtures for MoleculerPy.

Provides fixtures for creating multi-broker clusters with MemoryTransporter.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING

import pytest_asyncio

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action
from moleculerpy.service import Service
from moleculerpy.settings import Settings

if TYPE_CHECKING:
    pass


# =============================================================================
# Test Services
# =============================================================================


class CounterService(Service):
    """Simple counter service for testing load balancing."""

    name = "counter"

    def __init__(self, node_id: str):
        super().__init__(self.name)
        self._node_id = node_id
        self._call_count = 0

    @action()
    async def increment(self, ctx) -> dict:
        """Increment counter and return node info."""
        self._call_count += 1
        return {
            "node_id": self._node_id,
            "count": self._call_count,
        }

    @action()
    async def get_count(self, ctx) -> int:
        """Get current call count."""
        return self._call_count

    @action()
    async def get_node_id(self, ctx) -> str:
        """Get node ID for testing."""
        return self._node_id


class EchoService(Service):
    """Echo service for testing basic communication."""

    name = "echo"

    def __init__(self, node_id: str):
        super().__init__(self.name)
        self._node_id = node_id

    @action()
    async def echo(self, ctx) -> dict:
        """Echo params back with node info."""
        return {
            "node_id": self._node_id,
            "params": ctx.params,
        }

    @action()
    async def slow_echo(self, ctx) -> dict:
        """Slow echo for timeout testing."""
        delay = ctx.params.get("delay", 0.1)
        await asyncio.sleep(delay)
        return {
            "node_id": self._node_id,
            "params": ctx.params,
        }


class MathService(Service):
    """Math service for testing stateless operations."""

    name = "math"

    def __init__(self):
        super().__init__(self.name)

    @action()
    async def add(self, ctx) -> int:
        """Add two numbers."""
        return ctx.params["a"] + ctx.params["b"]

    @action()
    async def multiply(self, ctx) -> int:
        """Multiply two numbers."""
        return ctx.params["a"] * ctx.params["b"]


class ShardTestService(Service):
    """Service for testing shard strategy."""

    name = "shard-test"

    def __init__(self, node_id: str):
        super().__init__(self.name)
        self._node_id = node_id
        self._data: dict = {}

    @action()
    async def store(self, ctx) -> dict:
        """Store data by key (for shard testing)."""
        key = ctx.params["id"]
        value = ctx.params["value"]
        self._data[key] = value
        return {"node_id": self._node_id, "stored": key}

    @action()
    async def retrieve(self, ctx) -> dict:
        """Retrieve data by key."""
        key = ctx.params["id"]
        value = self._data.get(key)
        return {"node_id": self._node_id, "key": key, "value": value}


# =============================================================================
# Fixtures
# =============================================================================


@pytest_asyncio.fixture
async def single_broker() -> AsyncGenerator[ServiceBroker, None]:
    """Create a single broker for simple tests."""
    settings = Settings(transporter="memory://", prefer_local=False)
    broker = ServiceBroker(id="test-node", settings=settings)

    await broker.register(EchoService("test-node"))
    await broker.register(MathService())
    await broker.start()

    # Wait for service discovery
    await asyncio.sleep(0.1)

    yield broker

    await broker.stop()


@pytest_asyncio.fixture
async def two_broker_cluster() -> AsyncGenerator[list[ServiceBroker], None]:
    """Create a 2-broker cluster."""
    brokers: list[ServiceBroker] = []

    for i in range(2):
        node_id = f"node-{i}"
        settings = Settings(transporter="memory://", prefer_local=False)
        broker = ServiceBroker(id=node_id, settings=settings)

        await broker.register(CounterService(node_id))
        await broker.register(EchoService(node_id))

        brokers.append(broker)

    # Start all brokers
    for broker in brokers:
        await broker.start()

    # Wait for service discovery
    await asyncio.sleep(0.3)

    yield brokers

    # Stop all brokers
    for broker in brokers:
        await broker.stop()


@pytest_asyncio.fixture
async def three_broker_cluster() -> AsyncGenerator[list[ServiceBroker], None]:
    """Create a 3-broker cluster for load balancing tests."""
    brokers: list[ServiceBroker] = []

    for i in range(3):
        node_id = f"node-{i}"
        settings = Settings(transporter="memory://", prefer_local=False)
        broker = ServiceBroker(id=node_id, settings=settings)

        await broker.register(CounterService(node_id))
        await broker.register(EchoService(node_id))
        await broker.register(ShardTestService(node_id))

        brokers.append(broker)

    # Start all brokers
    for broker in brokers:
        await broker.start()

    # Wait for service discovery
    await asyncio.sleep(0.3)

    yield brokers

    # Stop all brokers
    for broker in brokers:
        await broker.stop()


@pytest_asyncio.fixture
async def shard_cluster() -> AsyncGenerator[list[ServiceBroker], None]:
    """Create a cluster configured for shard strategy testing.

    Note: Strategy is configured at registry level after broker creation
    since Settings doesn't support strategy_options.
    """
    brokers: list[ServiceBroker] = []

    for i in range(3):
        node_id = f"shard-node-{i}"
        settings = Settings(transporter="memory://", prefer_local=False)
        broker = ServiceBroker(id=node_id, settings=settings)

        # Configure shard strategy with options
        from moleculerpy.strategy import ShardStrategy

        broker.registry._strategy = ShardStrategy({"shardKey": "id", "vnodes": 100})

        await broker.register(ShardTestService(node_id))
        brokers.append(broker)

    # Start all brokers
    for broker in brokers:
        await broker.start()

    # Wait for service discovery
    await asyncio.sleep(0.3)

    yield brokers

    # Stop all brokers
    for broker in brokers:
        await broker.stop()
