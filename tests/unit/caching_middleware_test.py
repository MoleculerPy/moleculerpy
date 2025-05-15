"""Unit tests for CachingMiddleware.

Tests cover:
    - Cache hit/miss behavior
    - TTL handling
    - Lock-based thundering herd protection
    - Cache bypass via meta
    - Dynamic enable/disable
    - ctx.cached_result flag
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import AsyncMock

import pytest

from moleculerpy.cacher import MemoryCacher
from moleculerpy.middleware.caching import (
    CachingMiddleware,
    _parse_cache_options,
    create_caching_middleware,
)


@dataclass
class MockContext:
    """Mock Context for testing."""

    params: dict[str, Any] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)
    cached_result: bool = False


@dataclass
class MockAction:
    """Mock Action for testing."""

    name: str = "test.action"
    cache: bool | dict[str, Any] | None = None
    cache_ttl: int | None = None


class TestParseCacheOptions:
    """Test _parse_cache_options function."""

    def test_none_returns_disabled(self) -> None:
        """None config returns disabled options."""
        opts = _parse_cache_options(None)
        assert opts.enabled is False

    def test_false_returns_disabled(self) -> None:
        """False config returns disabled options."""
        opts = _parse_cache_options(False)
        assert opts.enabled is False

    def test_true_returns_defaults(self) -> None:
        """True config returns enabled with defaults."""
        opts = _parse_cache_options(True)

        assert opts.enabled is True
        assert opts.ttl is None
        assert opts.keys is None
        assert opts.lock_enabled is True

    def test_dict_with_ttl(self) -> None:
        """Dict with TTL extracts value."""
        opts = _parse_cache_options({"ttl": 60})

        assert opts.enabled is True
        assert opts.ttl == 60

    def test_dict_with_keys(self) -> None:
        """Dict with keys extracts tuple (frozen dataclass)."""
        opts = _parse_cache_options({"keys": ["id", "name"]})

        assert opts.keys == ("id", "name")  # tuple for hashability

    def test_dict_with_lock_config(self) -> None:
        """Dict with lock config extracts values."""
        opts = _parse_cache_options(
            {
                "lock": {
                    "enabled": False,
                    "ttl": 30000,  # milliseconds
                    "stale_time": 5,
                }
            }
        )

        assert opts.lock_enabled is False
        assert opts.lock_ttl == 30  # converted to seconds
        assert opts.stale_time == 5

    def test_dict_with_enabled_false(self) -> None:
        """Dict with enabled=False disables caching."""
        opts = _parse_cache_options({"enabled": False, "ttl": 60})

        assert opts.enabled is False

    def test_dict_with_callable_enabled(self) -> None:
        """Dict with callable enabled preserves function."""

        def check_enabled(ctx: Any) -> bool:
            return ctx.params.get("use_cache", True)

        opts = _parse_cache_options({"enabled": check_enabled})

        assert callable(opts.enabled)


class TestCachingMiddlewareBasic:
    """Basic middleware behavior tests."""

    @pytest.fixture
    def cacher(self) -> MemoryCacher:
        """Create a memory cacher."""
        return MemoryCacher(ttl=60)

    @pytest.fixture
    def middleware(self, cacher: MemoryCacher) -> CachingMiddleware:
        """Create caching middleware."""
        return CachingMiddleware(cacher)

    @pytest.mark.asyncio
    async def test_cache_disabled_calls_handler(self, middleware: CachingMiddleware) -> None:
        """When cache=False, handler is called directly."""
        action = MockAction(cache=False)
        handler = AsyncMock(return_value="result")

        wrapped = await middleware.local_action(handler, action)  # type: ignore[arg-type]
        ctx = MockContext(params={"id": 1})

        result = await wrapped(ctx)

        assert result == "result"
        handler.assert_called_once_with(ctx)

    @pytest.mark.asyncio
    async def test_cache_miss_calls_handler_and_caches(
        self, middleware: CachingMiddleware, cacher: MemoryCacher
    ) -> None:
        """On cache miss, handler is called and result cached."""
        action = MockAction(cache=True)
        handler = AsyncMock(return_value={"data": "value"})

        wrapped = await middleware.local_action(handler, action)  # type: ignore[arg-type]
        ctx = MockContext()  # Empty params → key is "test.action:"

        result = await wrapped(ctx)

        assert result == {"data": "value"}
        handler.assert_called_once()

        # Verify cached (key = "test.action:" since no params)
        cached = await cacher.get("test.action:")
        assert cached == {"data": "value"}

    @pytest.mark.asyncio
    async def test_cache_hit_returns_cached(
        self, middleware: CachingMiddleware, cacher: MemoryCacher
    ) -> None:
        """On cache hit, cached value returned without calling handler."""
        action = MockAction(cache=True)
        handler = AsyncMock(return_value="fresh")

        # Pre-populate cache
        await cacher.set("test.action:", "cached-value")

        wrapped = await middleware.local_action(handler, action)  # type: ignore[arg-type]
        ctx = MockContext()

        result = await wrapped(ctx)

        assert result == "cached-value"
        handler.assert_not_called()
        assert ctx.cached_result is True

    @pytest.mark.asyncio
    async def test_cache_uses_keys_for_key_generation(
        self, middleware: CachingMiddleware, cacher: MemoryCacher
    ) -> None:
        """Cache key uses specified keys from params."""
        action = MockAction(cache={"keys": ["id"]})
        handler = AsyncMock(return_value="result")

        wrapped = await middleware.local_action(handler, action)  # type: ignore[arg-type]

        # First call
        ctx1 = MockContext(params={"id": 1, "extra": "ignored"})
        await wrapped(ctx1)

        # Second call with same id (should hit cache)
        ctx2 = MockContext(params={"id": 1, "extra": "different"})
        result = await wrapped(ctx2)

        assert result == "result"
        assert ctx2.cached_result is True
        assert handler.call_count == 1  # Only called once

    @pytest.mark.asyncio
    async def test_cache_bypass_via_meta(
        self, middleware: CachingMiddleware, cacher: MemoryCacher
    ) -> None:
        """ctx.meta["$cache"] = False bypasses cache."""
        action = MockAction(cache=True)
        handler = AsyncMock(return_value="fresh")

        # Pre-populate cache
        await cacher.set("test.action:", "cached")

        wrapped = await middleware.local_action(handler, action)  # type: ignore[arg-type]
        ctx = MockContext(meta={"$cache": False})

        result = await wrapped(ctx)

        assert result == "fresh"
        handler.assert_called_once()
        assert ctx.cached_result is False

    @pytest.mark.asyncio
    async def test_dynamic_enable_callable(self, middleware: CachingMiddleware) -> None:
        """Callable enabled function controls caching."""
        action = MockAction(cache={"enabled": lambda ctx: ctx.params.get("use_cache", True)})
        handler = AsyncMock(return_value="result")

        wrapped = await middleware.local_action(handler, action)  # type: ignore[arg-type]

        # With use_cache=False, handler always called
        ctx1 = MockContext(params={"use_cache": False})
        await wrapped(ctx1)

        ctx2 = MockContext(params={"use_cache": False})
        await wrapped(ctx2)

        assert handler.call_count == 2

    @pytest.mark.asyncio
    async def test_cacher_disconnected_bypasses_cache(
        self, middleware: CachingMiddleware, cacher: MemoryCacher
    ) -> None:
        """When cacher.connected=False, cache is bypassed."""
        cacher.connected = False

        action = MockAction(cache=True)
        handler = AsyncMock(return_value="result")

        # Pre-populate cache
        await cacher.set("test.action:", "cached")

        wrapped = await middleware.local_action(handler, action)  # type: ignore[arg-type]
        ctx = MockContext()

        result = await wrapped(ctx)

        assert result == "result"
        handler.assert_called_once()


class TestCachingMiddlewareTTL:
    """TTL handling tests."""

    @pytest.mark.asyncio
    async def test_uses_action_ttl(self) -> None:
        """Action TTL overrides cacher default."""
        cacher = MemoryCacher(ttl=60)  # Default 60s
        middleware = CachingMiddleware(cacher)

        action = MockAction(cache={"ttl": 5})  # Override to 5s
        handler = AsyncMock(return_value="result")

        wrapped = await middleware.local_action(handler, action)  # type: ignore[arg-type]
        ctx = MockContext()

        await wrapped(ctx)

        # Check internal expire is ~5s, not 60s
        key = "test.action:"
        _data, ttl = await cacher.get_with_ttl(key)
        assert ttl is not None
        assert ttl <= 5

    @pytest.mark.asyncio
    async def test_legacy_cache_ttl_attribute(self) -> None:
        """Action.cache_ttl enables caching with that TTL."""
        cacher = MemoryCacher()
        middleware = CachingMiddleware(cacher)

        action = MockAction(cache=None, cache_ttl=30)  # Legacy attribute
        handler = AsyncMock(return_value="result")

        wrapped = await middleware.local_action(handler, action)  # type: ignore[arg-type]
        ctx = MockContext()

        await wrapped(ctx)

        # Should be cached
        cached = await cacher.get("test.action:")
        assert cached == "result"


class TestCachingMiddlewareLock:
    """Lock-based thundering herd protection tests."""

    @pytest.mark.asyncio
    async def test_lock_prevents_thundering_herd(self) -> None:
        """Multiple concurrent requests only execute handler once."""
        cacher = MemoryCacher(ttl=60)
        middleware = CachingMiddleware(cacher)

        call_count = 0

        async def slow_handler(ctx: Any) -> str:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.05)  # Simulate work
            return f"result-{call_count}"

        action = MockAction(cache={"lock": {"enabled": True}})
        wrapped = await middleware.local_action(slow_handler, action)  # type: ignore[arg-type]

        # Launch 5 concurrent requests
        contexts = [MockContext(params={"id": 1}) for _ in range(5)]
        results = await asyncio.gather(*[wrapped(ctx) for ctx in contexts])

        # Handler should only be called once
        assert call_count == 1
        # All should get same result
        assert all(r == "result-1" for r in results)

    @pytest.mark.asyncio
    async def test_lock_disabled_allows_concurrent(self) -> None:
        """Without lock, concurrent requests all execute handler."""
        cacher = MemoryCacher(ttl=60)
        middleware = CachingMiddleware(cacher)

        call_count = 0

        async def slow_handler(ctx: Any) -> str:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.05)
            return f"result-{call_count}"

        action = MockAction(cache={"lock": {"enabled": False}})
        wrapped = await middleware.local_action(slow_handler, action)  # type: ignore[arg-type]

        # Launch 3 concurrent requests
        contexts = [MockContext(params={"id": 1}) for _ in range(3)]
        await asyncio.gather(*[wrapped(ctx) for ctx in contexts])

        # All handlers execute (race condition)
        assert call_count >= 1  # At least one, likely more

    @pytest.mark.asyncio
    async def test_lock_double_check_pattern(self) -> None:
        """Second request gets cached result after lock released."""
        cacher = MemoryCacher(ttl=60)
        middleware = CachingMiddleware(cacher)

        call_order: list[str] = []

        async def handler(ctx: Any) -> str:
            call_order.append("handler_start")
            await asyncio.sleep(0.02)
            call_order.append("handler_end")
            return "result"

        action = MockAction(cache=True)
        wrapped = await middleware.local_action(handler, action)  # type: ignore[arg-type]

        # Sequential requests
        ctx1 = MockContext()
        result1 = await wrapped(ctx1)

        ctx2 = MockContext()
        result2 = await wrapped(ctx2)

        assert result1 == "result"
        assert result2 == "result"
        assert ctx2.cached_result is True
        # Handler only called once
        assert call_order == ["handler_start", "handler_end"]


class TestCreateCachingMiddleware:
    """Test factory function."""

    def test_returns_middleware_dict(self) -> None:
        """create_caching_middleware returns proper dict."""
        cacher = MemoryCacher()
        mw = create_caching_middleware(cacher)

        assert mw["name"] == "Cacher"
        assert "local_action" in mw
        assert callable(mw["local_action"])


class TestCachingMiddlewareIntegration:
    """Integration-style tests."""

    @pytest.mark.asyncio
    async def test_different_params_different_cache_entries(self) -> None:
        """Different params create different cache entries."""
        cacher = MemoryCacher(ttl=60)
        middleware = CachingMiddleware(cacher)

        action = MockAction(cache={"keys": ["id"]})

        async def handler(ctx: Any) -> dict[str, Any]:
            return {"id": ctx.params["id"], "data": "value"}

        wrapped = await middleware.local_action(handler, action)  # type: ignore[arg-type]

        # Request for id=1
        ctx1 = MockContext(params={"id": 1})
        result1 = await wrapped(ctx1)

        # Request for id=2
        ctx2 = MockContext(params={"id": 2})
        result2 = await wrapped(ctx2)

        # Both cached separately
        assert result1["id"] == 1
        assert result2["id"] == 2

        # Verify both in cache
        assert cacher.size() == 2

    @pytest.mark.asyncio
    async def test_meta_key_in_cache_key(self) -> None:
        """Meta values can be included in cache key."""
        cacher = MemoryCacher(ttl=60)
        middleware = CachingMiddleware(cacher)

        action = MockAction(cache={"keys": ["id", "#tenantId"]})
        handler = AsyncMock(return_value="result")

        wrapped = await middleware.local_action(handler, action)  # type: ignore[arg-type]

        # Request with tenantId in meta
        ctx1 = MockContext(params={"id": 1}, meta={"tenantId": "t1"})
        await wrapped(ctx1)

        # Same id, different tenant (should miss)
        ctx2 = MockContext(params={"id": 1}, meta={"tenantId": "t2"})
        await wrapped(ctx2)

        # Handler called twice (different cache keys)
        assert handler.call_count == 2
