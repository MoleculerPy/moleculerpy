"""Unit tests for ActionLoggerMiddleware."""

import asyncio
import json
import logging
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from moleculerpy.middleware.action_logger import ActionLoggerMiddleware


class TestActionLoggerMiddleware:
    """Test ActionLoggerMiddleware functionality."""

    @pytest.fixture
    def middleware(self):
        """Create middleware instance for testing."""
        return ActionLoggerMiddleware(
            log_level="debug",
            log_params=True,
            log_response=True,
        )

    @pytest.fixture
    def mock_broker(self):
        """Create mock broker."""
        broker = Mock()
        broker.node_id = "test-node"
        return broker

    @pytest.fixture
    def mock_action(self):
        """Create mock action."""
        action = Mock()
        action.name = "users.get"
        return action

    @pytest.fixture
    def mock_ctx(self):
        """Create mock context."""
        ctx = Mock()
        ctx.id = "req-123"
        ctx.caller = "api-gateway"
        ctx.params = {"userId": 42}
        ctx.meta = {"requestId": "abc"}
        return ctx

    # ============ Initialization Tests ============

    def test_init_default_values(self):
        """Test default initialization values."""
        mw = ActionLoggerMiddleware()

        assert mw.log_level == "info"
        assert mw.log_params is False
        assert mw.log_response is False
        assert mw.log_meta is False
        assert mw.folder is None
        assert mw.whitelist == ["*"]
        assert mw.blacklist == []

    def test_init_custom_values(self):
        """Test custom initialization values."""
        mw = ActionLoggerMiddleware(
            log_level="debug",
            log_params=True,
            log_response=True,
            log_meta=True,
            whitelist=["users.*"],
            blacklist=["*.internal"],
        )

        assert mw.log_level == "debug"
        assert mw.log_params is True
        assert mw.log_response is True
        assert mw.log_meta is True
        assert mw.whitelist == ["users.*"]
        assert mw.blacklist == ["*.internal"]

    def test_init_invalid_log_level_raises(self):
        """Test that invalid log level raises ValueError."""
        with pytest.raises(ValueError, match="Invalid log_level"):
            ActionLoggerMiddleware(log_level="invalid")

    def test_init_folder_creates_path(self):
        """Test that folder is converted to Path."""
        mw = ActionLoggerMiddleware(folder="/tmp/logs")
        assert isinstance(mw.folder, Path)
        assert str(mw.folder) == "/tmp/logs"

    # ============ Whitelist/Blacklist Tests ============

    def test_should_log_matches_wildcard(self, middleware):
        """Test wildcard matching in whitelist."""
        assert middleware._should_log("users.get") is True
        assert middleware._should_log("orders.create") is True
        assert middleware._should_log("any.action") is True

    def test_should_log_specific_pattern(self):
        """Test specific pattern matching."""
        mw = ActionLoggerMiddleware(whitelist=["users.*"])

        assert mw._should_log("users.get") is True
        assert mw._should_log("users.create") is True
        assert mw._should_log("orders.get") is False

    def test_should_log_blacklist_takes_priority(self):
        """Test that blacklist overrides whitelist."""
        mw = ActionLoggerMiddleware(
            whitelist=["*"],
            blacklist=["*.internal", "health.*"],
        )

        assert mw._should_log("users.get") is True
        assert mw._should_log("users.internal") is False
        assert mw._should_log("health.check") is False

    # ============ Broker Created Tests ============

    def test_broker_created_sets_node_id(self, middleware, mock_broker):
        """Test that broker_created sets node_id."""
        middleware.broker_created(mock_broker)
        assert middleware._node_id == "test-node"

    def test_broker_created_sets_log_fn(self, middleware, mock_broker):
        """Test that broker_created sets log function."""
        middleware.broker_created(mock_broker)
        assert middleware._log_fn is not None

    def test_broker_created_creates_folder(self, mock_broker, tmp_path):
        """Test that broker_created creates output folder."""
        mw = ActionLoggerMiddleware(folder=tmp_path / "logs")
        mw.broker_created(mock_broker)

        expected_folder = tmp_path / "logs" / "test-node"
        assert expected_folder.exists()
        assert mw._target_folder == expected_folder

    # ============ Local Action Tests ============

    @pytest.mark.asyncio
    async def test_local_action_logs_request(
        self, middleware, mock_broker, mock_action, mock_ctx, caplog
    ):
        """Test that local_action logs the request."""
        middleware.broker_created(mock_broker)

        next_handler = AsyncMock(return_value={"id": 42, "name": "John"})

        with caplog.at_level(logging.DEBUG):
            wrapped = await middleware.local_action(next_handler, mock_action)
            result = await wrapped(mock_ctx)

        assert "users.get" in caplog.text
        assert "api-gateway" in caplog.text
        assert result == {"id": 42, "name": "John"}

    @pytest.mark.asyncio
    async def test_local_action_logs_error(
        self, middleware, mock_broker, mock_action, mock_ctx, caplog
    ):
        """Test that local_action logs errors."""
        middleware.broker_created(mock_broker)

        next_handler = AsyncMock(side_effect=ValueError("Invalid user"))

        with caplog.at_level(logging.DEBUG):
            wrapped = await middleware.local_action(next_handler, mock_action)
            with pytest.raises(ValueError):
                await wrapped(mock_ctx)

        assert "ERROR" in caplog.text
        assert "Invalid user" in caplog.text

    @pytest.mark.asyncio
    async def test_local_action_skips_blacklisted(self, mock_broker, mock_ctx):
        """Test that blacklisted actions are not logged."""
        mw = ActionLoggerMiddleware(
            log_level="debug",
            blacklist=["users.*"],
        )
        mw.broker_created(mock_broker)

        action = Mock()
        action.name = "users.get"

        next_handler = AsyncMock(return_value={})

        wrapped = await mw.local_action(next_handler, action)

        # Should return original handler (no wrapping)
        assert wrapped == next_handler

    # ============ Remote Action Tests ============

    @pytest.mark.asyncio
    async def test_remote_action_uses_same_logic(
        self, middleware, mock_broker, mock_action, mock_ctx, caplog
    ):
        """Test that remote_action uses same logging logic as local_action."""
        middleware.broker_created(mock_broker)

        next_handler = AsyncMock(return_value={"remote": True})

        with caplog.at_level(logging.DEBUG):
            wrapped = await middleware.remote_action(next_handler, mock_action)
            result = await wrapped(mock_ctx)

        assert "users.get" in caplog.text
        assert result == {"remote": True}

    # ============ File Saving Tests ============

    @pytest.mark.asyncio
    async def test_saves_to_file_on_success(self, mock_broker, mock_action, mock_ctx, tmp_path):
        """Test that action logs are saved to file."""
        mw = ActionLoggerMiddleware(
            log_level="debug",
            log_params=True,
            log_response=True,
            folder=tmp_path / "logs",
        )
        mw.broker_created(mock_broker)

        next_handler = AsyncMock(return_value={"success": True})

        wrapped = await mw.local_action(next_handler, mock_action)
        await wrapped(mock_ctx)

        # Wait for async file save
        await asyncio.sleep(0.1)

        # Check file was created
        log_folder = tmp_path / "logs" / "test-node"
        files = list(log_folder.glob("*.json"))
        assert len(files) == 1

        # Verify content
        with open(files[0]) as f:
            data = json.load(f)

        assert data["action"] == "users.get"
        assert data["status"] == "ok"
        assert "duration_ms" in data

    @pytest.mark.asyncio
    async def test_saves_to_file_on_error(self, mock_broker, mock_action, mock_ctx, tmp_path):
        """Test that error logs are saved to file."""
        mw = ActionLoggerMiddleware(
            log_level="debug",
            folder=tmp_path / "logs",
        )
        mw.broker_created(mock_broker)

        next_handler = AsyncMock(side_effect=ValueError("Test error"))

        wrapped = await mw.local_action(next_handler, mock_action)
        with pytest.raises(ValueError):
            await wrapped(mock_ctx)

        # Wait for async file save
        await asyncio.sleep(0.1)

        # Check file was created
        log_folder = tmp_path / "logs" / "test-node"
        files = list(log_folder.glob("*error*.json"))
        assert len(files) == 1

    # ============ Stopped Tests ============

    @pytest.mark.asyncio
    async def test_stopped_logs_message(self, middleware, caplog):
        """Test that stopped logs a debug message."""
        with caplog.at_level(logging.DEBUG):
            await middleware.stopped()

        assert "stopped" in caplog.text.lower()


class TestActionLoggerPatterns:
    """Test action name pattern matching."""

    @pytest.mark.parametrize(
        "whitelist,blacklist,action,expected",
        [
            (["*"], [], "users.get", True),
            (["users.*"], [], "users.get", True),
            (["users.*"], [], "orders.get", False),
            (["*"], ["*.internal"], "users.get", True),
            (["*"], ["*.internal"], "users.internal", False),
            (["users.get"], [], "users.get", True),
            (["users.get"], [], "users.list", False),
        ],
    )
    def test_pattern_matching(self, whitelist, blacklist, action, expected):
        """Test various pattern matching scenarios."""
        mw = ActionLoggerMiddleware(whitelist=whitelist, blacklist=blacklist)
        assert mw._should_log(action) is expected
