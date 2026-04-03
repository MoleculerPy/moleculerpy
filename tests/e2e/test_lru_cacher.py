"""E2E tests for MemoryLRUCacher with real ServiceBroker.

Tests verify:
    - Broker starts and uses MemoryLRUCacher for action caching
    - LRU eviction happens gracefully during broker operation
    - Regular MemoryCacher still works (backward compat)
"""

from __future__ import annotations

from typing import Any

import pytest

from moleculerpy.broker import ServiceBroker
from moleculerpy.cacher import MemoryCacher
from moleculerpy.cacher.memory_lru import MemoryLRUCacher
from moleculerpy.context import Context
from moleculerpy.decorators import action
from moleculerpy.service import Service
from moleculerpy.settings import Settings


class CountingService(Service):
    """Service that counts how many times its action was called."""

    name = "counter"

    def __init__(self) -> None:
        super().__init__(self.name)
        self._call_count = 0

    @action(cache=True)
    async def get_value(self, ctx: Context) -> dict[str, Any]:
        self._call_count += 1
        return {"value": self._call_count, "id": ctx.params.get("id", 0)}


@pytest.mark.asyncio
@pytest.mark.e2e
class TestLRUCacherWithBroker:
    """E2E tests: broker + MemoryLRUCacher."""

    async def test_broker_with_lru_cacher_caches_action_result(self) -> None:
        """Action result is cached; second identical call returns cached value."""
        settings = Settings(
            transporter="memory://",
            cacher={"type": "MemoryLRU", "max": 5, "ttl": 60},
        )
        broker = ServiceBroker(id="lru-test-node", settings=settings)

        service = CountingService()
        await broker.register(service)

        # First call — executes handler
        result1 = await broker.call("counter.get_value", {"id": 1})
        assert result1["value"] == 1

        # Second call with same params — must return cached result
        result2 = await broker.call("counter.get_value", {"id": 1})
        assert result2["value"] == 1
        assert service._call_count == 1  # handler called only once

        # Verify the cacher is actually MemoryLRUCacher
        assert isinstance(broker.cacher, MemoryLRUCacher)

        await broker.stop()

    async def test_lru_eviction_during_broker_operation(self) -> None:
        """LRU eviction is handled gracefully; broker continues working after overflow."""
        settings = Settings(
            transporter="memory://",
            cacher={"type": "MemoryLRU", "max": 3, "ttl": 60},
        )
        broker = ServiceBroker(id="lru-evict-node", settings=settings)

        class MultiKeyService(Service):
            name = "multi"

            def __init__(self) -> None:
                super().__init__(self.name)

            @action(cache={"keys": ["id"]})
            async def get(self, ctx: Context) -> dict[str, Any]:
                return {"id": ctx.params["id"]}

        await broker.register(MultiKeyService())

        # Fill cache beyond max to trigger eviction
        for i in range(5):
            result = await broker.call("multi.get", {"id": i})
            assert result["id"] == i

        cacher = broker.cacher
        assert isinstance(cacher, MemoryLRUCacher)

        # After 5 calls with max=3, size must not exceed max
        assert cacher.size() <= 3

        await broker.stop()

    async def test_regular_memory_cacher_still_works(self) -> None:
        """Backward compat: broker with MemoryCacher (not LRU) still functions."""
        settings = Settings(
            transporter="memory://",
            cacher={"type": "Memory", "ttl": 60},
        )
        broker = ServiceBroker(id="mem-compat-node", settings=settings)

        service = CountingService()
        await broker.register(service)

        result1 = await broker.call("counter.get_value", {"id": 42})
        assert result1["value"] == 1

        result2 = await broker.call("counter.get_value", {"id": 42})
        assert result2["value"] == 1
        assert service._call_count == 1

        assert isinstance(broker.cacher, MemoryCacher)
        assert not isinstance(broker.cacher, MemoryLRUCacher)

        await broker.stop()
