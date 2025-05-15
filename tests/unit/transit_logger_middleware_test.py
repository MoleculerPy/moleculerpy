"""Unit tests for the Transit Logger Middleware module.

Tests for TransitLoggerMiddleware that provides transit message logging
for debugging and monitoring.
"""

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moleculerpy.middleware.transit_logger import TransitLoggerMiddleware


class TestTransitLoggerMiddlewareInit:
    """Tests for TransitLoggerMiddleware initialization."""

    def test_init_default_values(self):
        """Test middleware initializes with default values."""
        mw = TransitLoggerMiddleware()

        assert mw.log_level == "info"
        assert mw.log_packet_data is False
        assert mw.folder is None
        assert mw.packet_filter == {"HEARTBEAT"}

    def test_init_with_debug_level(self):
        """Test middleware with debug log level."""
        mw = TransitLoggerMiddleware(log_level="debug")

        assert mw.log_level == "debug"

    def test_init_with_warning_level(self):
        """Test middleware with warning log level."""
        mw = TransitLoggerMiddleware(log_level="warning")

        assert mw.log_level == "warning"

    def test_init_case_insensitive_level(self):
        """Test log level is case insensitive."""
        mw = TransitLoggerMiddleware(log_level="DEBUG")

        assert mw.log_level == "debug"

    def test_init_with_log_packet_data(self):
        """Test middleware with packet data logging enabled."""
        mw = TransitLoggerMiddleware(log_packet_data=True)

        assert mw.log_packet_data is True

    def test_init_with_folder_string(self):
        """Test middleware with folder as string."""
        mw = TransitLoggerMiddleware(folder="/tmp/logs")

        assert mw.folder == Path("/tmp/logs")

    def test_init_with_folder_path(self):
        """Test middleware with folder as Path."""
        mw = TransitLoggerMiddleware(folder=Path("/tmp/logs"))

        assert mw.folder == Path("/tmp/logs")

    def test_init_with_custom_filter(self):
        """Test middleware with custom packet filter."""
        mw = TransitLoggerMiddleware(packet_filter=["HEARTBEAT", "PING", "PONG"])

        assert mw.packet_filter == {"HEARTBEAT", "PING", "PONG"}

    def test_init_empty_filter_uses_default(self):
        """Test empty packet filter uses default (HEARTBEAT)."""
        # Empty list is falsy, so default ["HEARTBEAT"] is used
        mw = TransitLoggerMiddleware(packet_filter=[])

        assert mw.packet_filter == {"HEARTBEAT"}

    def test_init_invalid_log_level_raises(self):
        """Test invalid log level raises ValueError."""
        with pytest.raises(ValueError, match="Invalid log_level"):
            TransitLoggerMiddleware(log_level="invalid")

    def test_init_invalid_level_shows_options(self):
        """Test error message shows valid options."""
        with pytest.raises(ValueError, match="debug, info, warning"):
            TransitLoggerMiddleware(log_level="error")


class TestBrokerCreatedHook:
    """Tests for broker_created hook."""

    def test_broker_created_sets_node_id(self):
        """Test broker_created sets node_id from broker."""
        mw = TransitLoggerMiddleware()
        broker = MagicMock()
        broker.node_id = "test-node-1"

        mw.broker_created(broker)

        assert mw._node_id == "test-node-1"

    def test_broker_created_fallback_node_id(self):
        """Test broker_created fallback when node_id missing."""
        mw = TransitLoggerMiddleware()
        broker = MagicMock(spec=[])  # No node_id attribute

        mw.broker_created(broker)

        assert mw._node_id == "unknown"

    def test_broker_created_sets_log_function_debug(self):
        """Test broker_created sets debug log function."""
        mw = TransitLoggerMiddleware(log_level="debug")
        broker = MagicMock()
        broker.node_id = "node-1"

        mw.broker_created(broker)

        # Verify log function is set (can't directly compare functions)
        assert mw._log_fn is not None

    def test_broker_created_creates_folder(self, tmp_path):
        """Test broker_created creates output folder."""
        folder = tmp_path / "transit-logs"
        mw = TransitLoggerMiddleware(folder=folder)
        broker = MagicMock()
        broker.node_id = "node-1"

        mw.broker_created(broker)

        expected_folder = folder / "node-1"
        assert expected_folder.exists()
        assert mw._target_folder == expected_folder


class TestSaveToFile:
    """Tests for file saving functionality."""

    def test_save_to_file_sync_creates_file(self, tmp_path):
        """Test _save_to_file_sync creates JSON file."""
        mw = TransitLoggerMiddleware(folder=tmp_path)
        broker = MagicMock()
        broker.node_id = "node-1"
        mw.broker_created(broker)

        payload = {"key": "value", "number": 42}
        mw._save_to_file_sync("test.json", payload)

        filepath = mw._target_folder / "test.json"
        assert filepath.exists()

        with open(filepath, encoding="utf-8") as f:
            saved = json.load(f)
        assert saved == payload

    def test_save_to_file_sync_handles_non_serializable(self, tmp_path):
        """Test _save_to_file_sync handles non-serializable objects."""
        mw = TransitLoggerMiddleware(folder=tmp_path)
        broker = MagicMock()
        broker.node_id = "node-1"
        mw.broker_created(broker)

        from datetime import datetime

        payload = {"timestamp": datetime.now()}
        mw._save_to_file_sync("test.json", payload)

        filepath = mw._target_folder / "test.json"
        assert filepath.exists()

    def test_save_to_file_sync_no_folder_noop(self):
        """Test _save_to_file_sync is noop when no folder."""
        mw = TransitLoggerMiddleware()
        broker = MagicMock()
        broker.node_id = "node-1"
        mw.broker_created(broker)

        # Should not raise
        mw._save_to_file_sync("test.json", {"key": "value"})

    @pytest.mark.asyncio
    async def test_save_to_file_async(self, tmp_path):
        """Test _save_to_file_async saves file asynchronously."""
        mw = TransitLoggerMiddleware(folder=tmp_path)
        broker = MagicMock()
        broker.node_id = "node-1"
        mw.broker_created(broker)

        payload = {"async": True}
        await mw._save_to_file_async("async_test.json", payload)

        filepath = mw._target_folder / "async_test.json"
        assert filepath.exists()

        with open(filepath, encoding="utf-8") as f:
            saved = json.load(f)
        assert saved == payload


class TestTransitPublishHook:
    """Tests for transit_publish hook."""

    @pytest.fixture
    def middleware(self) -> TransitLoggerMiddleware:
        """Create middleware for testing."""
        mw = TransitLoggerMiddleware(log_level="debug", log_packet_data=True)
        broker = MagicMock()
        broker.node_id = "test-node"
        mw.broker_created(broker)
        return mw

    @pytest.mark.asyncio
    async def test_logs_packet_info(self, middleware, caplog):
        """Test packet info is logged."""
        packet = MagicMock()
        packet.type.value = "REQ"
        packet.target = "other-node"
        packet.payload = {"action": "test.action"}

        async def next_publish(p):
            return "result"

        with caplog.at_level(logging.DEBUG):
            result = await middleware.transit_publish(next_publish, packet)

        assert result == "result"
        assert "REQ" in caplog.text
        assert "other-node" in caplog.text

    @pytest.mark.asyncio
    async def test_logs_payload_when_enabled(self, middleware, caplog):
        """Test payload is logged when enabled."""
        packet = MagicMock()
        packet.type.value = "REQ"
        packet.target = None
        packet.payload = {"action": "test.action", "params": {"x": 1}}

        async def next_publish(p):
            return "result"

        with caplog.at_level(logging.DEBUG):
            await middleware.transit_publish(next_publish, packet)

        assert "Payload" in caplog.text

    @pytest.mark.asyncio
    async def test_filters_heartbeat(self, middleware):
        """Test HEARTBEAT packets are filtered."""
        packet = MagicMock()
        packet.type.value = "HEARTBEAT"
        packet.target = None

        call_count = 0

        async def next_publish(p):
            nonlocal call_count
            call_count += 1
            return "result"

        result = await middleware.transit_publish(next_publish, packet)

        assert result == "result"
        assert call_count == 1  # Still called, just not logged

    @pytest.mark.asyncio
    async def test_filters_custom_types(self):
        """Test custom packet types are filtered."""
        mw = TransitLoggerMiddleware(packet_filter=["PING", "PONG"])
        broker = MagicMock()
        broker.node_id = "node-1"
        mw.broker_created(broker)

        packet = MagicMock()
        packet.type.value = "PING"
        packet.target = None

        async def next_publish(p):
            return "result"

        # Should not log (filtered), but still call next
        result = await mw.transit_publish(next_publish, packet)
        assert result == "result"

    @pytest.mark.asyncio
    async def test_saves_to_file(self, tmp_path):
        """Test packet is saved to file when folder configured."""
        mw = TransitLoggerMiddleware(folder=tmp_path)
        broker = MagicMock()
        broker.node_id = "node-1"
        mw.broker_created(broker)

        packet = MagicMock()
        packet.type.value = "REQ"
        packet.target = "target-node"
        packet.payload = {"action": "test.action"}

        async def next_publish(p):
            return "result"

        await mw.transit_publish(next_publish, packet)

        # Check file was created
        files = list(mw._target_folder.glob("*.json"))
        assert len(files) == 1
        assert "REQ" in files[0].name
        assert "target-node" in files[0].name

    @pytest.mark.asyncio
    async def test_handles_no_target(self, middleware, caplog):
        """Test handles packet with no target."""
        packet = MagicMock()
        packet.type.value = "EVENT"
        packet.target = None
        packet.payload = {}

        async def next_publish(p):
            return "result"

        with caplog.at_level(logging.DEBUG):
            await middleware.transit_publish(next_publish, packet)

        assert "<all nodes>" in caplog.text

    @pytest.mark.asyncio
    async def test_handles_enum_type(self, middleware):
        """Test handles packet with enum type."""
        from enum import Enum

        class PacketType(Enum):
            REQUEST = "REQ"

        packet = MagicMock()
        packet.type = PacketType.REQUEST
        packet.target = None
        packet.payload = {}

        async def next_publish(p):
            return "result"

        result = await middleware.transit_publish(next_publish, packet)
        assert result == "result"


class TestStoppedHook:
    """Tests for stopped hook."""

    @pytest.mark.asyncio
    async def test_stopped_logs_debug(self, caplog):
        """Test stopped logs debug message."""
        mw = TransitLoggerMiddleware()

        with caplog.at_level(logging.DEBUG):
            await mw.stopped()

        assert "stopped" in caplog.text


class TestLoggingLevels:
    """Tests for different logging levels."""

    @pytest.mark.asyncio
    async def test_debug_level_logs(self, caplog):
        """Test debug level logging."""
        mw = TransitLoggerMiddleware(log_level="debug")
        broker = MagicMock()
        broker.node_id = "node-1"
        mw.broker_created(broker)

        packet = MagicMock()
        packet.type.value = "REQ"
        packet.target = "node-2"
        packet.payload = {}

        async def next_publish(p):
            return "result"

        with caplog.at_level(logging.DEBUG):
            await mw.transit_publish(next_publish, packet)

        assert len(caplog.records) > 0

    @pytest.mark.asyncio
    async def test_info_level_logs(self, caplog):
        """Test info level logging."""
        mw = TransitLoggerMiddleware(log_level="info")
        broker = MagicMock()
        broker.node_id = "node-1"
        mw.broker_created(broker)

        packet = MagicMock()
        packet.type.value = "REQ"
        packet.target = "node-2"
        packet.payload = {}

        async def next_publish(p):
            return "result"

        with caplog.at_level(logging.INFO):
            await mw.transit_publish(next_publish, packet)

        # Should log at INFO level
        info_records = [r for r in caplog.records if r.levelno == logging.INFO]
        assert len(info_records) > 0

    @pytest.mark.asyncio
    async def test_warning_level_logs(self, caplog):
        """Test warning level logging."""
        mw = TransitLoggerMiddleware(log_level="warning")
        broker = MagicMock()
        broker.node_id = "node-1"
        mw.broker_created(broker)

        packet = MagicMock()
        packet.type.value = "REQ"
        packet.target = "node-2"
        packet.payload = {}

        async def next_publish(p):
            return "result"

        with caplog.at_level(logging.WARNING):
            await mw.transit_publish(next_publish, packet)

        # Should log at WARNING level
        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warning_records) > 0
