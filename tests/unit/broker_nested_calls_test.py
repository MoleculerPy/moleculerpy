"""Tests for broker nested calls with level and request_id propagation.

Phase 3A: Core Protocol Compliance - Broker nested call tests.
"""

from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio

from moleculerpy.broker import ServiceBroker
from moleculerpy.context import Context
from moleculerpy.decorators import action
from moleculerpy.errors import MaxCallLevelError, ServiceNotFoundError
from moleculerpy.service import Service
from moleculerpy.settings import Settings
from moleculerpy.transit import Transit


class RecursiveService(Service):
    """Service that calls itself recursively for testing."""

    def __init__(self) -> None:
        super().__init__(name="recursive")

    @action()
    async def call_self(self, ctx: Context) -> dict:
        """Call self with level tracking."""
        if ctx.level < 5:
            # Recursive call through context
            result = await ctx.call("recursive.call_self")
            return {"final_level": result["final_level"], "my_level": ctx.level}
        return {"final_level": ctx.level}

    @action()
    async def get_context_info(self, ctx: Context) -> dict:
        """Return context tracing info."""
        return {
            "id": ctx.id,
            "level": ctx.level,
            "request_id": ctx.request_id,
            "parent_id": ctx.parent_id,
        }


class CallerService(Service):
    """Service that calls another service."""

    def __init__(self) -> None:
        super().__init__(name="caller")

    @action()
    async def call_target(self, ctx: Context) -> dict:
        """Call target service and return level info."""
        result = await ctx.call("target.get_info")
        return {
            "caller_level": ctx.level,
            "caller_request_id": ctx.request_id,
            "target_result": result,
        }


class TargetService(Service):
    """Target service for nested call tests."""

    def __init__(self) -> None:
        super().__init__(name="target")

    @action()
    async def get_info(self, ctx: Context) -> dict:
        """Return context info."""
        return {
            "target_level": ctx.level,
            "target_request_id": ctx.request_id,
        }


@pytest.fixture(scope="function")
def mock_transit():
    """Create mock transit for local-only testing."""
    transit = AsyncMock(spec=Transit)
    transit.connect = AsyncMock()
    transit.disconnect = AsyncMock()
    transit.transporter = Mock(name="mock_nats")
    return transit


@pytest_asyncio.fixture
async def broker_with_recursive(mock_transit):
    """Create broker with recursive service."""
    settings = Settings(transporter="mock://localhost:4222")
    broker = ServiceBroker(id="test-node", settings=settings, transit=mock_transit)
    await broker.register(RecursiveService())
    await broker.start()
    yield broker
    await broker.stop()


@pytest_asyncio.fixture
async def broker_with_caller_target(mock_transit):
    """Create broker with caller and target services."""
    settings = Settings(transporter="mock://localhost:4222")
    broker = ServiceBroker(id="test-node", settings=settings, transit=mock_transit)
    await broker.register(CallerService())
    await broker.register(TargetService())
    await broker.start()
    yield broker
    await broker.stop()


class TestBrokerCallLevel:
    """Tests for broker.call level tracking."""

    @pytest.mark.asyncio
    async def test_root_call_has_level_1(self, broker_with_recursive) -> None:
        """Root call should create context with level=1."""
        result = await broker_with_recursive.call("recursive.get_context_info")
        assert result["level"] == 1

    @pytest.mark.asyncio
    async def test_nested_call_increments_level(self, broker_with_caller_target) -> None:
        """Nested call should increment level."""
        result = await broker_with_caller_target.call("caller.call_target")

        assert result["caller_level"] == 1
        assert result["target_result"]["target_level"] == 2

    @pytest.mark.asyncio
    async def test_request_id_propagates(self, broker_with_caller_target) -> None:
        """Request ID should propagate through nested calls."""
        result = await broker_with_caller_target.call("caller.call_target")

        # Both should have same request_id
        assert result["caller_request_id"] == result["target_result"]["target_request_id"]


class TestMaxCallLevelProtection:
    """Tests for max call level protection."""

    @pytest.mark.asyncio
    async def test_max_call_level_exceeded_raises_error(self, mock_transit) -> None:
        """Exceeding max call level should raise MaxCallLevelError.

        With max_call_level=3 and >= comparison (Moleculer.js behavior):
        - Level 1: OK
        - Level 2: OK
        - Level 3: REJECTED (3 >= 3)
        """
        settings = Settings(max_call_level=3, transporter="mock://localhost:4222")
        broker = ServiceBroker(id="test-node", settings=settings, transit=mock_transit)
        await broker.register(RecursiveService())
        await broker.start()

        try:
            # This will try to recurse to level 5, but max is 3
            with pytest.raises(MaxCallLevelError) as exc_info:
                await broker.call("recursive.call_self")

            # Level 3 is rejected (>= comparison matches Moleculer.js)
            assert exc_info.value.level == 3
            assert "test-node" in exc_info.value.message
        finally:
            await broker.stop()

    @pytest.mark.asyncio
    async def test_max_call_level_zero_is_unlimited(self, broker_with_recursive) -> None:
        """max_call_level=0 should allow unlimited depth."""
        # Default is 0 (unlimited), so recursive call should succeed
        result = await broker_with_recursive.call("recursive.call_self")
        assert result["final_level"] == 5  # Recursion stops at level 5

    @pytest.mark.asyncio
    async def test_max_call_level_exactly_at_limit(self, mock_transit) -> None:
        """Call at exactly max level should be rejected (Moleculer.js behavior).

        With max_call_level=5, level 5 is rejected because we use >= comparison.
        To allow recursion up to level 5, set max_call_level=6.
        """
        settings = Settings(max_call_level=6, transporter="mock://localhost:4222")
        broker = ServiceBroker(id="test-node", settings=settings, transit=mock_transit)
        await broker.register(RecursiveService())
        await broker.start()

        try:
            # Recursion stops at level 5, max_call_level=6 allows it
            result = await broker.call("recursive.call_self")
            assert result["final_level"] == 5
        finally:
            await broker.stop()


class TestServiceNotFoundError:
    """Tests for ServiceNotFoundError."""

    @pytest.mark.asyncio
    async def test_call_nonexistent_action_raises_error(self) -> None:
        """Calling non-existent action should raise ServiceNotFoundError."""
        broker = ServiceBroker(id="test-node", settings=Settings(transporter="memory://"))
        await broker.start()

        try:
            with pytest.raises(ServiceNotFoundError) as exc_info:
                await broker.call("nonexistent.action")

            assert exc_info.value.action_name == "nonexistent.action"
            assert exc_info.value.code == 404
            assert exc_info.value.retryable is True
        finally:
            await broker.stop()


class TestContextCallMethod:
    """Tests for Context.call() method."""

    @pytest.mark.asyncio
    async def test_context_call_passes_parent(self, broker_with_caller_target) -> None:
        """Context.call() should pass self as parent context."""
        result = await broker_with_caller_target.call("caller.call_target")

        # Target should have level 2 (caller is 1)
        assert result["target_result"]["target_level"] == 2

    @pytest.mark.asyncio
    async def test_context_call_propagates_request_id(self, broker_with_caller_target) -> None:
        """Context.call() should propagate request_id."""
        result = await broker_with_caller_target.call("caller.call_target")

        caller_req_id = result["caller_request_id"]
        target_req_id = result["target_result"]["target_request_id"]

        assert caller_req_id == target_req_id
        assert caller_req_id is not None
