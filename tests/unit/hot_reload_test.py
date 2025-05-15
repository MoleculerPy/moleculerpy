"""Unit tests for the HotReload Middleware module.

Tests for HotReloadMiddleware that provides service hot reloading
during development.
"""

import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from moleculerpy.middleware.hot_reload import HotReloadMiddleware


class TestHotReloadMiddlewareInit:
    """Tests for HotReloadMiddleware initialization."""

    def test_init_defaults(self):
        """Test middleware initializes with defaults."""
        mw = HotReloadMiddleware()

        assert mw.logger is not None
        assert mw.logger.name == "moleculerpy.middleware.hot_reload"
        assert mw.watch_paths == ["./services"]
        assert mw.debounce_ms == 500
        assert mw.patterns == ["*.py"]
        assert mw.recursive is True
        assert mw._broker is None

    def test_init_custom_paths(self):
        """Test middleware with custom watch paths."""
        mw = HotReloadMiddleware(watch_paths=["./src", "./lib"])

        assert mw.watch_paths == ["./src", "./lib"]

    def test_init_custom_debounce(self):
        """Test middleware with custom debounce."""
        mw = HotReloadMiddleware(debounce_ms=1000)

        assert mw.debounce_ms == 1000

    def test_init_custom_patterns(self):
        """Test middleware with custom patterns."""
        mw = HotReloadMiddleware(patterns=["*_service.py", "*_worker.py"])

        assert mw.patterns == ["*_service.py", "*_worker.py"]

    def test_init_custom_logger(self):
        """Test middleware with custom logger."""
        custom_logger = MagicMock()
        mw = HotReloadMiddleware(logger=custom_logger)

        assert mw.logger is custom_logger

    def test_repr(self):
        """Test __repr__ returns readable string."""
        mw = HotReloadMiddleware(watch_paths=["./src"], debounce_ms=1000)

        assert "paths=['./src']" in repr(mw)
        assert "debounce=1000ms" in repr(mw)


class TestModuleHandling:
    """Tests for module handling logic."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return HotReloadMiddleware()

    def test_get_module_for_service(self, middleware):
        """Test getting module name from service."""
        service = MagicMock()
        type(service).__module__ = "my_services.user_service"

        result = middleware._get_module_for_service(service)

        assert result == "my_services.user_service"

    def test_get_module_for_service_main(self, middleware):
        """Test module name __main__ returns None."""
        service = MagicMock()
        type(service).__module__ = "__main__"

        result = middleware._get_module_for_service(service)

        assert result is None

    @patch.dict(sys.modules, {"test_module": MagicMock()})
    def test_reload_module_success(self, middleware):
        """Test successful module reload."""
        with patch("importlib.reload") as mock_reload:
            result = middleware._reload_module("test_module")

            assert result is True
            mock_reload.assert_called_once()

    def test_reload_module_not_loaded(self, middleware):
        """Test reload of unloaded module."""
        result = middleware._reload_module("nonexistent_module_xyz")

        assert result is False

    @patch.dict(sys.modules, {"error_module": MagicMock()})
    def test_reload_module_error(self, middleware):
        """Test reload error handling."""
        with patch("importlib.reload", side_effect=ImportError("Test error")):
            result = middleware._reload_module("error_module")

            assert result is False


class TestServiceReload:
    """Tests for service reload logic."""

    @pytest.fixture
    def middleware(self):
        """Create middleware with mocked broker."""
        mw = HotReloadMiddleware()
        broker = MagicMock()
        broker.services = []
        mw._broker = broker
        return mw

    @pytest.mark.asyncio
    async def test_reload_service_not_found(self, middleware):
        """Test reload of non-existent service."""
        result = await middleware._reload_service("nonexistent_service")

        assert result is False

    @pytest.mark.asyncio
    async def test_reload_service_no_broker(self):
        """Test reload without broker."""
        mw = HotReloadMiddleware()

        result = await mw._reload_service("test_service")

        assert result is False

    @pytest.mark.asyncio
    async def test_reload_service_no_module(self, middleware):
        """Test reload when module cannot be determined."""
        service = MagicMock()
        service.name = "test"
        service.full_name = "test"
        type(service).__module__ = "__main__"
        middleware._broker.services = [service]

        result = await middleware._reload_service("test")

        assert result is False


class TestDebouncing:
    """Tests for reload debouncing."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        return HotReloadMiddleware(debounce_ms=50)

    def test_schedule_reload_adds_to_pending(self, middleware):
        """Test scheduling adds service to pending."""
        middleware._schedule_reload("test_service")

        assert "test_service" in middleware._pending_reloads

    def test_schedule_reload_multiple(self, middleware):
        """Test scheduling multiple services."""
        middleware._schedule_reload("service1")
        middleware._schedule_reload("service2")
        middleware._schedule_reload("service1")  # Duplicate

        assert "service1" in middleware._pending_reloads
        assert "service2" in middleware._pending_reloads
        assert len(middleware._pending_reloads) == 2

    @pytest.mark.asyncio
    async def test_process_pending_reloads_clears_set(self, middleware):
        """Test processing clears pending set."""
        middleware._pending_reloads = {"service1", "service2"}
        middleware._broker = MagicMock()
        middleware._broker.services = []

        await middleware._process_pending_reloads()

        assert len(middleware._pending_reloads) == 0


class TestFileChangeHandling:
    """Tests for file change event handling."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance."""
        mw = HotReloadMiddleware(patterns=["*.py"])
        mw._service_modules = {}
        return mw

    def test_on_file_changed_non_matching_pattern(self, middleware):
        """Test non-matching files are ignored."""
        middleware._schedule_reload = MagicMock()

        middleware._on_file_changed("/path/to/file.txt")

        middleware._schedule_reload.assert_not_called()

    def test_on_file_changed_matching_pattern(self, middleware):
        """Test matching files trigger reload check."""
        # Create a mock module with __file__
        mock_module = MagicMock()
        mock_module.__file__ = "/path/to/service.py"

        middleware._service_modules = {"test_service": "test_module"}

        with patch.dict(sys.modules, {"test_module": mock_module}):
            middleware._schedule_reload = MagicMock()
            middleware._on_file_changed("/path/to/service.py")

            middleware._schedule_reload.assert_called_once_with("test_service")


class TestLifecycleHooks:
    """Tests for lifecycle hooks."""

    def test_broker_created_stores_reference(self):
        """Test broker_created stores broker reference."""
        mw = HotReloadMiddleware()
        broker = MagicMock()

        mw.broker_created(broker)

        assert mw._broker is broker

    @pytest.mark.asyncio
    async def test_service_started_tracks_module(self):
        """Test service_started tracks service module."""
        mw = HotReloadMiddleware()
        service = MagicMock()
        service.name = "users"
        type(service).__module__ = "services.user_service"

        await mw.service_started(service)

        assert mw._service_modules["users"] == "services.user_service"

    @pytest.mark.asyncio
    async def test_broker_started_starts_watching(self):
        """Test broker_started starts file watching."""
        mw = HotReloadMiddleware()
        mw._start_watching = MagicMock()
        broker = MagicMock()

        await mw.broker_started(broker)

        mw._start_watching.assert_called_once()

    @pytest.mark.asyncio
    async def test_broker_stopping_stops_watching(self):
        """Test broker_stopping stops file watching."""
        mw = HotReloadMiddleware()
        mw._stop_watching = MagicMock()
        broker = MagicMock()

        await mw.broker_stopping(broker)

        mw._stop_watching.assert_called_once()

    @pytest.mark.asyncio
    async def test_broker_stopping_cancels_pending_reload(self):
        """Test broker_stopping cancels pending reload."""
        mw = HotReloadMiddleware()
        mw._stop_watching = MagicMock()
        mock_task = MagicMock()
        mw._reload_task = mock_task
        broker = MagicMock()

        await mw.broker_stopping(broker)

        # Task was cancelled before being set to None
        mock_task.cancel.assert_called_once()
        assert mw._reload_task is None


class TestWatchdogIntegration:
    """Tests for watchdog integration."""

    def test_start_watching_without_watchdog(self):
        """Test graceful handling when watchdog not installed."""
        mw = HotReloadMiddleware()

        # Mock import to fail
        with patch.dict(sys.modules, {"watchdog": None}):
            with patch("builtins.__import__", side_effect=ImportError):
                # Should not raise, just log warning
                mw._start_watching()

        assert mw._observer is None

    def test_stop_watching_no_observer(self):
        """Test stop watching when no observer running."""
        mw = HotReloadMiddleware()
        mw._observer = None

        # Should not raise
        mw._stop_watching()

    def test_stop_watching_with_observer(self):
        """Test stop watching stops and joins observer."""
        mw = HotReloadMiddleware()
        mock_observer = MagicMock()
        mw._observer = mock_observer

        mw._stop_watching()

        mock_observer.stop.assert_called_once()
        mock_observer.join.assert_called_once()
        assert mw._observer is None


class TestIntegration:
    """Integration-style tests for HotReload."""

    @pytest.mark.asyncio
    async def test_full_reload_cycle(self):
        """Test full hot reload cycle (without actual file watching)."""
        mw = HotReloadMiddleware(debounce_ms=50)

        # Setup mock broker
        broker = MagicMock()
        mock_service = MagicMock()
        mock_service.name = "test"
        mock_service.full_name = "test"
        mock_service.stop = AsyncMock()
        mock_service.start = AsyncMock()
        type(mock_service).__module__ = "test_service_module"
        broker.services = [mock_service]
        broker.register = AsyncMock()

        mw.broker_created(broker)
        await mw.service_started(mock_service)

        # Schedule reload
        mw._schedule_reload("test")

        # Wait for debounce (mocked to avoid actual module reload)
        await asyncio.sleep(0.1)

        # Pending should be cleared
        assert len(mw._pending_reloads) == 0

    @pytest.mark.asyncio
    async def test_debounce_batches_reloads(self):
        """Test debouncing batches multiple file changes."""
        mw = HotReloadMiddleware(debounce_ms=100)
        mw._broker = MagicMock()
        mw._broker.services = []

        process_count = 0

        async def track_process():
            nonlocal process_count
            process_count += 1
            mw._pending_reloads.clear()

        mw._process_pending_reloads = track_process

        # Rapid schedule multiple reloads
        for i in range(5):
            mw._schedule_reload(f"service{i}")
            await asyncio.sleep(0.02)

        # Wait for debounce
        await asyncio.sleep(0.15)

        # Should process only once
        assert process_count == 1
