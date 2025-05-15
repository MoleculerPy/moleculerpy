"""Unit tests for the discoverer module."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from moleculerpy.discoverer import Discoverer
from moleculerpy.settings import Settings


class TestDiscoverer:
    """Test Discoverer class."""

    @pytest.fixture
    def mock_transit(self):
        """Create a mock transit."""
        transit = AsyncMock()
        transit.beat = AsyncMock()
        return transit

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings with default heartbeat interval."""
        settings = Mock(spec=Settings)
        settings.heartbeat_interval = 5.0
        return settings

    @pytest.fixture
    def mock_broker(self, mock_transit, mock_settings):
        """Create a mock broker."""
        broker = Mock()
        broker.transit = mock_transit
        broker.logger = Mock()
        broker.settings = mock_settings
        return broker

    @pytest.mark.asyncio
    async def test_discoverer_initialization(self, mock_broker):
        """Test Discoverer initialization (tasks are NOT created until start())."""
        discoverer = Discoverer(mock_broker)

        assert discoverer.broker == mock_broker
        assert discoverer.transit == mock_broker.transit
        assert isinstance(discoverer._tasks, list)
        # Tasks are NOT created in __init__ anymore - they're created in start()
        assert len(discoverer._tasks) == 0
        assert not discoverer._started

    @pytest.mark.asyncio
    async def test_discoverer_start_creates_tasks(self, mock_broker):
        """Test that start() creates the heartbeat task."""
        discoverer = Discoverer(mock_broker)

        # Before start, no tasks
        assert len(discoverer._tasks) == 0
        assert not discoverer._started

        # Start the discoverer
        await discoverer.start()

        # After start, tasks should be created
        assert len(discoverer._tasks) == 1
        assert discoverer._started
        assert discoverer._tasks[0].get_name() == "moleculerpy:discoverer-beat"

        # Clean up
        await discoverer.stop()

    @pytest.mark.asyncio
    async def test_discoverer_start_idempotent(self, mock_broker):
        """Test that calling start() multiple times is safe."""
        discoverer = Discoverer(mock_broker)

        await discoverer.start()
        first_task = discoverer._tasks[0]

        # Second start should be a no-op
        await discoverer.start()

        assert len(discoverer._tasks) == 1
        assert discoverer._tasks[0] is first_task

        await discoverer.stop()

    @pytest.mark.asyncio
    async def test_discoverer_heartbeat_interval_from_settings(self, mock_broker, mock_settings):
        """Test that discoverer uses heartbeat interval from settings."""
        mock_settings.heartbeat_interval = 10.0
        discoverer = Discoverer(mock_broker)

        assert discoverer.heartbeat_interval == 10.0

    @pytest.mark.asyncio
    async def test_discoverer_periodic_beat(self, mock_broker, mock_transit, mock_settings):
        """Test that discoverer sends periodic heartbeats."""
        # Use short interval for testing
        # Note: discoverer adds jitter of ±500ms, so actual sleep = max(0.1, interval + jitter)
        # With interval=0.1 and max jitter=+0.5, max sleep = 0.6s
        mock_settings.heartbeat_interval = 0.1

        discoverer = Discoverer(mock_broker)
        await discoverer.start()

        # Wait for multiple heartbeats - account for jitter (±500ms)
        # Need ~1.5s to ensure 2+ beats even in worst case
        await asyncio.sleep(1.5)

        # Stop the discoverer
        await discoverer.stop()

        # Check that beat was called multiple times
        assert mock_transit.beat.call_count >= 2

    @pytest.mark.asyncio
    async def test_discoverer_stop(self, mock_broker):
        """Test discoverer stop method."""
        discoverer = Discoverer(mock_broker)
        await discoverer.start()

        # Verify task is running
        assert len(discoverer._tasks) == 1
        assert not discoverer._tasks[0].done()

        # Stop discoverer
        await discoverer.stop()

        # Verify task was cancelled and list is cleared
        assert len(discoverer._tasks) == 0

    @pytest.mark.asyncio
    async def test_discoverer_stop_without_start(self, mock_broker):
        """Test discoverer stop when never started."""
        discoverer = Discoverer(mock_broker)

        # Should not raise any errors
        await discoverer.stop()

        assert len(discoverer._tasks) == 0

    @pytest.mark.asyncio
    async def test_discoverer_stop_with_completed_tasks(self, mock_broker):
        """Test discoverer stop with already completed tasks."""
        discoverer = Discoverer(mock_broker)
        await discoverer.start()

        # Cancel and wait for task to complete
        for task in discoverer._tasks:
            task.cancel()

        # Wait a bit for cancellation to take effect
        await asyncio.sleep(0.01)

        # Stop should handle completed tasks gracefully
        await discoverer.stop()

        assert len(discoverer._tasks) == 0

    @pytest.mark.asyncio
    async def test_discoverer_heartbeat_error_handling(
        self, mock_broker, mock_transit, mock_settings
    ):
        """Test that heartbeat errors are handled gracefully."""
        # Make beat method raise an error
        mock_transit.beat.side_effect = RuntimeError("Network error")
        # Use longer interval to account for jitter (±500ms added in discoverer)
        mock_settings.heartbeat_interval = 0.1

        discoverer = Discoverer(mock_broker)
        await discoverer.start()

        # Wait for heartbeat to trigger - must account for jitter (±500ms)
        # With interval=0.1 and max jitter=+0.5, max sleep = max(0.1, 0.6) = 0.6s
        # Wait enough time for at least one beat attempt
        await asyncio.sleep(0.8)

        # Stop the discoverer
        await discoverer.stop()

        # Check that error was logged
        mock_broker.logger.error.assert_called()
        error_message = str(mock_broker.logger.error.call_args[0][0])
        assert "Error in periodic beat" in error_message
        assert "Network error" in error_message

    @pytest.mark.asyncio
    async def test_discoverer_heartbeat_with_broker_without_logger(
        self, mock_transit, mock_settings
    ):
        """Test heartbeat error handling when broker has no logger."""
        # Create broker without logger attribute
        broker = Mock()
        broker.transit = mock_transit
        broker.settings = mock_settings
        # Explicitly don't set logger attribute

        # Make beat method raise an error
        mock_transit.beat.side_effect = RuntimeError("Test error")
        mock_settings.heartbeat_interval = 0.05

        discoverer = Discoverer(broker)
        await discoverer.start()

        # Wait for error to occur
        await asyncio.sleep(0.1)

        # Stop the discoverer
        await discoverer.stop()

        # Should not raise any errors even without logger

    @pytest.mark.asyncio
    async def test_discoverer_multiple_instances(self, mock_broker):
        """Test that multiple discoverer instances work independently."""
        discoverer1 = Discoverer(mock_broker)
        discoverer2 = Discoverer(mock_broker)

        await discoverer1.start()
        await discoverer2.start()

        # Each should have their own tasks
        assert len(discoverer1._tasks) == 1
        assert len(discoverer2._tasks) == 1
        assert discoverer1._tasks[0] != discoverer2._tasks[0]

        # Stop both
        await discoverer1.stop()
        await discoverer2.stop()

        # Both should be empty
        assert len(discoverer1._tasks) == 0
        assert len(discoverer2._tasks) == 0

    @pytest.mark.asyncio
    async def test_discoverer_stop_timeout_handling(self, mock_broker):
        """Test that stop handles timeout gracefully."""
        discoverer = Discoverer(mock_broker)
        await discoverer.start()

        # Create a task that doesn't respond to cancellation quickly
        async def stubborn_task():
            try:
                while True:
                    await asyncio.sleep(10)  # Long sleep
            except asyncio.CancelledError:
                # Simulate slow cleanup
                await asyncio.sleep(2)  # Longer than stop timeout
                raise

        # Replace the heartbeat task with our stubborn task
        for task in discoverer._tasks:
            task.cancel()
        await asyncio.sleep(0.01)
        discoverer._tasks.clear()

        stubborn = asyncio.create_task(stubborn_task())
        discoverer._tasks.append(stubborn)

        # Stop should complete even if task doesn't finish in timeout
        await discoverer.stop()

        # Tasks list should be cleared regardless
        assert len(discoverer._tasks) == 0

        # Clean up the stubborn task
        stubborn.cancel()
        try:
            await stubborn
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_discoverer_task_naming(self, mock_broker):
        """Test that discoverer tasks are properly named."""
        discoverer = Discoverer(mock_broker)
        await discoverer.start()

        assert len(discoverer._tasks) == 1
        assert discoverer._tasks[0].get_name() == "moleculerpy:discoverer-beat"

        await discoverer.stop()

    @pytest.mark.asyncio
    async def test_discoverer_cancellation_during_beat(
        self, mock_broker, mock_transit, mock_settings
    ):
        """Test that cancellation during beat is handled properly."""

        # Make beat take some time
        async def slow_beat():
            await asyncio.sleep(1)

        mock_transit.beat = slow_beat
        mock_settings.heartbeat_interval = 0.1

        discoverer = Discoverer(mock_broker)
        await discoverer.start()

        # Wait for first beat to start
        await asyncio.sleep(0.15)

        # Stop while beat is in progress
        await discoverer.stop()

        # Should complete without errors
        assert len(discoverer._tasks) == 0
