"""Integration tests for Broker + Cacher.

Tests cover:
    - Broker initialization with cacher
    - Cacher lifecycle (start/stop)
    - Action caching via CachingMiddleware
    - Cache invalidation on reconnect
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from moleculerpy.broker import ServiceBroker
from moleculerpy.cacher import MemoryCacher, resolve
from moleculerpy.context import Context
from moleculerpy.decorators import action
from moleculerpy.middleware.caching import CachingMiddleware
from moleculerpy.service import Service
from moleculerpy.settings import Settings


class TestBrokerCacherInitialization:
    """Test broker + cacher initialization."""

    def test_broker_without_cacher(self) -> None:
        """Broker should work without cacher."""
        broker = ServiceBroker(id="test-node")

        assert broker.cacher is None
        # No CachingMiddleware in middlewares
        assert not any(isinstance(m, CachingMiddleware) for m in broker.middlewares)

    def test_broker_with_cacher_instance(self) -> None:
        """Broker should accept cacher instance directly."""
        cacher = MemoryCacher(ttl=60)
        broker = ServiceBroker(id="test-node", cacher=cacher)

        assert broker.cacher is cacher
        # CachingMiddleware auto-registered
        assert any(isinstance(m, CachingMiddleware) for m in broker.middlewares)

    def test_broker_with_cacher_from_settings_true(self) -> None:
        """settings.cacher=True should create MemoryCacher."""
        settings = Settings(cacher=True)
        broker = ServiceBroker(id="test-node", settings=settings)

        assert broker.cacher is not None
        assert isinstance(broker.cacher, MemoryCacher)

    def test_broker_with_cacher_from_settings_string(self) -> None:
        """settings.cacher='Memory' should create MemoryCacher."""
        settings = Settings(cacher="Memory")
        broker = ServiceBroker(id="test-node", settings=settings)

        assert broker.cacher is not None
        assert isinstance(broker.cacher, MemoryCacher)

    def test_broker_with_cacher_from_settings_dict(self) -> None:
        """settings.cacher={type, ttl} should create configured cacher."""
        settings = Settings(cacher={"type": "Memory", "ttl": 120, "clone": True})
        broker = ServiceBroker(id="test-node", settings=settings)

        assert broker.cacher is not None
        assert isinstance(broker.cacher, MemoryCacher)
        assert broker.cacher.ttl == 120
        assert broker.cacher.clone is True

    def test_cacher_init_called_with_broker(self) -> None:
        """Cacher.init() should be called with broker reference."""
        cacher = MemoryCacher()
        broker = ServiceBroker(id="test-node", cacher=cacher)

        assert cacher.broker is broker

    def test_cacher_namespace_prefix(self) -> None:
        """Cacher prefix should include namespace when set in settings."""
        cacher = MemoryCacher()
        settings = Settings(namespace="prod")
        ServiceBroker(id="test-node", settings=settings, cacher=cacher)

        assert cacher.prefix == "MOL-prod-"


class TestBrokerCacherLifecycle:
    """Test cacher lifecycle with broker."""

    @pytest.fixture
    def broker_with_cacher(self) -> ServiceBroker:
        """Create broker with cacher and mocked transporter."""
        cacher = MemoryCacher(ttl=60)
        settings = Settings(transporter="memory://")
        broker = ServiceBroker(id="test-node", settings=settings, cacher=cacher)
        return broker

    @pytest.mark.asyncio
    async def test_cacher_start_creates_cleanup_task(
        self, broker_with_cacher: ServiceBroker
    ) -> None:
        """Cacher.start() should create background cleanup task."""
        cacher = broker_with_cacher.cacher
        assert cacher is not None
        assert isinstance(cacher, MemoryCacher)

        # Before start: no cleanup task
        assert cacher._cleanup_task is None

        # Mock transit to avoid real connection
        broker_with_cacher.transit.connect = AsyncMock()
        broker_with_cacher.discoverer.start = AsyncMock()

        await broker_with_cacher.start()

        # After start: cleanup task should exist
        assert cacher._cleanup_task is not None
        assert not cacher._cleanup_task.done()

        # Cleanup
        await broker_with_cacher.stop()

    @pytest.mark.asyncio
    async def test_cacher_stop_cancels_cleanup_task(
        self, broker_with_cacher: ServiceBroker
    ) -> None:
        """Cacher.stop() should cancel background cleanup task."""
        cacher = broker_with_cacher.cacher
        assert cacher is not None
        assert isinstance(cacher, MemoryCacher)

        # Start to create cleanup task
        broker_with_cacher.transit.connect = AsyncMock()
        broker_with_cacher.discoverer.start = AsyncMock()
        await broker_with_cacher.start()

        cleanup_task = cacher._cleanup_task
        assert cleanup_task is not None

        # Stop broker
        broker_with_cacher.transit.disconnect = AsyncMock()
        await broker_with_cacher.stop()

        # After stop: cleanup task should be cancelled
        assert cacher._cleanup_task is None


class TestCacherResolve:
    """Test cacher resolution from config."""

    def test_resolve_true(self) -> None:
        """resolve(True) should create MemoryCacher."""
        cacher = resolve(True)
        assert isinstance(cacher, MemoryCacher)

    def test_resolve_false(self) -> None:
        """resolve(False) should return None."""
        cacher = resolve(False)
        assert cacher is None

    def test_resolve_none(self) -> None:
        """resolve(None) should return None."""
        cacher = resolve(None)
        assert cacher is None

    def test_resolve_string_memory(self) -> None:
        """resolve('Memory') should create MemoryCacher."""
        cacher = resolve("Memory")
        assert isinstance(cacher, MemoryCacher)

    def test_resolve_dict_with_type(self) -> None:
        """resolve({type: 'Memory', ttl: 60}) should create configured cacher."""
        cacher = resolve({"type": "Memory", "ttl": 60})
        assert isinstance(cacher, MemoryCacher)
        assert cacher.ttl == 60

    def test_resolve_dict_without_type(self) -> None:
        """resolve({ttl: 60}) without type should default to MemoryCacher."""
        cacher = resolve({"ttl": 60})
        assert isinstance(cacher, MemoryCacher)
        assert cacher.ttl == 60

    def test_resolve_unknown_type_raises(self) -> None:
        """resolve({type: 'Unknown'}) should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown cacher type"):
            resolve({"type": "UnknownCacher"})


class TestActionCachingIntegration:
    """Test action caching through the full middleware chain."""

    @pytest.fixture
    def cacher(self) -> MemoryCacher:
        """Create a memory cacher."""
        return MemoryCacher(ttl=60)

    @pytest.fixture
    def broker(self, cacher: MemoryCacher) -> ServiceBroker:
        """Create broker with cacher."""
        settings = Settings(transporter="memory://")
        return ServiceBroker(id="test-node", settings=settings, cacher=cacher)

    @pytest.mark.asyncio
    async def test_cached_action_returns_cached_result(
        self, broker: ServiceBroker, cacher: MemoryCacher
    ) -> None:
        """Action with cache=True should return cached result on second call."""
        call_count = 0

        class TestService(Service):
            def __init__(self) -> None:
                super().__init__("test")

            @action(cache=True)
            async def get_data(self, ctx: Context) -> dict[str, Any]:
                nonlocal call_count
                call_count += 1
                return {"value": call_count}

        await broker.register(TestService())

        # First call - should execute handler
        result1 = await broker.call("test.get_data")
        assert result1 == {"value": 1}
        assert call_count == 1

        # Second call - should return cached
        result2 = await broker.call("test.get_data")
        assert result2 == {"value": 1}  # Same as first
        assert call_count == 1  # Handler not called again

    @pytest.mark.asyncio
    async def test_cached_action_with_different_params(
        self, broker: ServiceBroker, cacher: MemoryCacher
    ) -> None:
        """Different params should create different cache entries."""

        class TestService(Service):
            def __init__(self) -> None:
                super().__init__("test")

            @action(cache={"keys": ["id"]})
            async def get_item(self, ctx: Context) -> dict[str, Any]:
                return {"id": ctx.params["id"], "fetched": True}

        await broker.register(TestService())

        # Call with id=1
        result1 = await broker.call("test.get_item", {"id": 1})
        assert result1 == {"id": 1, "fetched": True}

        # Call with id=2 (different cache key)
        result2 = await broker.call("test.get_item", {"id": 2})
        assert result2 == {"id": 2, "fetched": True}

        # Verify both cached
        assert cacher.size() == 2

    @pytest.mark.asyncio
    async def test_cache_bypass_via_meta(self, broker: ServiceBroker, cacher: MemoryCacher) -> None:
        """ctx.meta['$cache'] = False should bypass cache."""
        call_count = 0

        class TestService(Service):
            def __init__(self) -> None:
                super().__init__("test")

            @action(cache=True)
            async def get_data(self, ctx: Context) -> dict[str, Any]:
                nonlocal call_count
                call_count += 1
                return {"value": call_count}

        await broker.register(TestService())

        # First call - populates cache
        await broker.call("test.get_data")
        assert call_count == 1

        # Second call with $cache=False - bypasses cache
        result = await broker.call("test.get_data", meta={"$cache": False})
        assert result == {"value": 2}
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_action_without_cache_not_cached(
        self, broker: ServiceBroker, cacher: MemoryCacher
    ) -> None:
        """Action without cache config should not be cached."""
        call_count = 0

        class TestService(Service):
            def __init__(self) -> None:
                super().__init__("test")

            @action()  # No cache
            async def get_data(self, ctx: Context) -> dict[str, Any]:
                nonlocal call_count
                call_count += 1
                return {"value": call_count}

        await broker.register(TestService())

        await broker.call("test.get_data")
        await broker.call("test.get_data")

        assert call_count == 2
        assert cacher.size() == 0


class TestCacherCleanupOnReconnect:
    """Test cache cleanup on transporter reconnect."""

    @pytest.mark.asyncio
    async def test_cache_cleared_on_transporter_reconnect(self) -> None:
        """Cache should be cleared when transporter reconnects."""
        cacher = MemoryCacher(ttl=60)
        settings = Settings(transporter="memory://")
        broker = ServiceBroker(id="test-node", settings=settings, cacher=cacher)

        # Pre-populate cache
        await cacher.set("key1", "value1")
        await cacher.set("key2", "value2")
        assert cacher.size() == 2

        # Simulate transporter reconnect event
        await broker.broadcast_local("$transporter.connected", {"wasReconnect": True})

        # Give time for async handler
        await asyncio.sleep(0.01)

        # Cache should be cleared
        assert cacher.size() == 0
