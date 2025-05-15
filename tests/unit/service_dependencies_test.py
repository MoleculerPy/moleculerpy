"""Tests for service dependencies feature.

This module tests the automatic waiting for dependent services during broker.start().
Services can declare dependencies that must be available before their started() hook runs.
"""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from moleculerpy.broker import Broker
from moleculerpy.decorators import action
from moleculerpy.service import Service


# Automatically mock transit operations
@pytest.fixture(autouse=True)
def mock_transit_operations():
    """Mock all transit operations to avoid network connections."""
    with (
        patch("moleculerpy.transit.Transit.connect", new_callable=AsyncMock),
        patch("moleculerpy.transit.Transit.disconnect", new_callable=AsyncMock),
        patch("moleculerpy.transit.Transit.request", new_callable=AsyncMock),
        patch("moleculerpy.transit.Transit.publish", new_callable=AsyncMock),
    ):
        yield


class DatabaseService(Service):
    """Service that provides database connections."""

    def __init__(self):
        super().__init__("database")
        self.started_called = False

    @action()
    async def query(self, ctx):
        return {"status": "ok"}

    async def started(self):
        self.started_called = True


class CacheService(Service):
    """Service that depends on database."""

    def __init__(self):
        super().__init__("cache", dependencies=["database"])
        self.started_called = False
        self.started_order = None

    @action()
    async def get(self, ctx):
        return {"value": None}

    async def started(self):
        self.started_called = True


class APIService(Service):
    """Service that depends on both database and cache."""

    def __init__(self):
        super().__init__("api", dependencies=["database", "cache"])
        self.started_called = False

    @action()
    async def health(self, ctx):
        return {"status": "healthy"}

    async def started(self):
        self.started_called = True


class TestServiceDependencies:
    """Tests for service dependencies feature."""

    @pytest.mark.asyncio
    async def test_service_without_dependencies(self):
        """Service without dependencies starts normally."""
        broker = Broker(id="test-broker")
        db_service = DatabaseService()

        await broker.register(db_service)
        await broker.start()

        assert db_service.started_called is True

        await broker.stop()

    @pytest.mark.asyncio
    async def test_service_with_local_dependency(self):
        """Service waits for local dependency before starting."""
        broker = Broker(id="test-broker")
        db_service = DatabaseService()
        cache_service = CacheService()

        # Register both services
        await broker.register(db_service)
        await broker.register(cache_service)

        await broker.start()

        # Both should be started
        assert db_service.started_called is True
        assert cache_service.started_called is True

        await broker.stop()

    @pytest.mark.asyncio
    async def test_service_with_multiple_dependencies(self):
        """Service waits for all dependencies before starting."""
        broker = Broker(id="test-broker")
        db_service = DatabaseService()
        cache_service = CacheService()
        api_service = APIService()

        # Register all services
        await broker.register(db_service)
        await broker.register(cache_service)
        await broker.register(api_service)

        await broker.start()

        # All should be started
        assert db_service.started_called is True
        assert cache_service.started_called is True
        assert api_service.started_called is True

        await broker.stop()

    @pytest.mark.asyncio
    async def test_dependency_timeout_raises_error(self):
        """Missing dependency raises TimeoutError."""

        class OrphanService(Service):
            def __init__(self):
                # Depends on non-existent service
                super().__init__("orphan", dependencies=["nonexistent"])

            @action()
            async def check(self, ctx):
                return {}

        broker = Broker(id="test-broker")
        orphan = OrphanService()

        await broker.register(orphan)

        # Patch wait_for_services to use short timeout
        original_wait = broker.wait_for_services

        async def short_timeout_wait(services, timeout=30.0, interval=0.5):
            return await original_wait(services, timeout=0.1, interval=0.05)

        broker.wait_for_services = short_timeout_wait

        with pytest.raises(asyncio.TimeoutError):
            await broker.start()

        await broker.stop()

    @pytest.mark.asyncio
    async def test_dependencies_attribute_on_service(self):
        """Service correctly stores dependencies attribute."""
        service = CacheService()

        assert service.dependencies == ["database"]

    @pytest.mark.asyncio
    async def test_empty_dependencies(self):
        """Service with empty dependencies list starts normally."""

        class NoDepsService(Service):
            def __init__(self):
                super().__init__("nodeps", dependencies=[])
                self.started_called = False

            async def started(self):
                self.started_called = True

        broker = Broker(id="test-broker")
        service = NoDepsService()

        await broker.register(service)
        await broker.start()

        assert service.started_called is True

        await broker.stop()


class TestServiceDependencyOrder:
    """Tests for dependency resolution order."""

    @pytest.mark.asyncio
    async def test_started_hooks_respect_registration_order(self):
        """Services start in registration order when dependencies are satisfied."""
        started_order = []

        class TrackedService(Service):
            def __init__(self, name, deps=None):
                super().__init__(name, dependencies=deps)

            async def started(self):
                started_order.append(self.name)

        broker = Broker(id="test-broker")

        svc_a = TrackedService("a")
        svc_b = TrackedService("b", deps=["a"])
        svc_c = TrackedService("c", deps=["a", "b"])

        # Register in order
        await broker.register(svc_a)
        await broker.register(svc_b)
        await broker.register(svc_c)

        await broker.start()

        # All should start
        assert "a" in started_order
        assert "b" in started_order
        assert "c" in started_order

        await broker.stop()

    @pytest.mark.asyncio
    async def test_independent_services_start_concurrently(self):
        """Independent services should enter started() concurrently."""
        entered_count = 0
        both_entered = asyncio.Event()
        release = asyncio.Event()

        class BlockingStartService(Service):
            def __init__(self, name: str):
                super().__init__(name)

            async def started(self):
                nonlocal entered_count
                entered_count += 1
                if entered_count == 2:
                    both_entered.set()
                await release.wait()

        broker = Broker(id="parallel-start-test")
        await broker.register(BlockingStartService("s1"))
        await broker.register(BlockingStartService("s2"))

        start_task = asyncio.create_task(broker.start())
        await asyncio.wait_for(both_entered.wait(), timeout=0.3)
        release.set()
        await start_task
        await broker.stop()


class TestServiceDependencyEdgeCases:
    """Edge case tests for dependencies."""

    @pytest.mark.asyncio
    async def test_self_dependency_handled(self):
        """Service depending on itself doesn't deadlock."""

        class SelfDepService(Service):
            def __init__(self):
                # This is a bad pattern but shouldn't deadlock
                super().__init__("self-dep", dependencies=["self-dep"])
                self.started_called = False

            async def started(self):
                self.started_called = True

        broker = Broker(id="test-broker")
        service = SelfDepService()

        await broker.register(service)

        # Should find itself as local service and proceed
        await broker.start()

        assert service.started_called is True

        await broker.stop()

    @pytest.mark.asyncio
    async def test_none_dependencies_handled(self):
        """Service with None dependencies (legacy) starts normally."""
        broker = Broker(id="test-broker")

        # Create service and manually set dependencies to None
        service = DatabaseService()
        service.dependencies = None

        await broker.register(service)
        await broker.start()

        assert service.started_called is True

        await broker.stop()
