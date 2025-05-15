"""Unit tests for LatencyMonitor (Phase 5.2 PING/PONG Protocol).

Tests cover:
- LatencyMonitor initialization and lifecycle
- PING/PONG packet handling
- Latency calculation and sample averaging
- Master/Slave pattern
- Integration with broker
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from moleculerpy.latency import LatencyMonitor
from moleculerpy.packet import Packet, Topic


class TestLatencyMonitorInit:
    """Tests for LatencyMonitor initialization."""

    def test_latency_monitor_creation(self, mock_broker: MagicMock) -> None:
        """LatencyMonitor can be created with broker."""
        monitor = LatencyMonitor(broker=mock_broker)

        assert monitor.broker is mock_broker
        assert monitor.is_master is False
        assert monitor._ping_interval == 10.0
        assert monitor._sample_count == 5

    def test_latency_monitor_custom_options(self, mock_broker: MagicMock) -> None:
        """LatencyMonitor accepts custom ping_interval and sample_count."""
        monitor = LatencyMonitor(
            broker=mock_broker,
            is_master=True,
            ping_interval=5.0,
            sample_count=10,
        )

        assert monitor.is_master is True
        assert monitor._ping_interval == 5.0
        assert monitor._sample_count == 10


class TestLatencyMonitorLifecycle:
    """Tests for LatencyMonitor start/stop lifecycle."""

    @pytest.mark.asyncio
    async def test_start_non_master(self, mock_broker: MagicMock) -> None:
        """Non-master monitor starts without ping cycle."""
        monitor = LatencyMonitor(broker=mock_broker, is_master=False)

        await monitor.start()

        assert monitor._started is True
        assert monitor._ping_task is None  # No ping cycle for non-master

        await monitor.stop()

    @pytest.mark.asyncio
    async def test_start_master_starts_ping_cycle(self, mock_broker: MagicMock) -> None:
        """Master monitor starts ping cycle task."""
        monitor = LatencyMonitor(broker=mock_broker, is_master=True)

        await monitor.start()

        assert monitor._started is True
        assert monitor._ping_task is not None

        await monitor.stop()

        assert monitor._ping_task is None

    @pytest.mark.asyncio
    async def test_stop_clears_state(self, mock_broker: MagicMock) -> None:
        """Stop clears pending pings and stops task."""
        monitor = LatencyMonitor(broker=mock_broker)
        monitor._pending_pings["ping-1"] = (1000.0, "node-1")

        await monitor.start()
        await monitor.stop()

        assert len(monitor._pending_pings) == 0
        assert monitor._started is False


class TestPingPongProtocol:
    """Tests for PING/PONG packet handling."""

    @pytest.mark.asyncio
    async def test_send_ping(self) -> None:
        """send_ping sends PING packet with correct payload."""
        mock_broker = MagicMock()
        mock_broker.nodeID = "master-node"
        mock_broker.transit = MagicMock()
        mock_broker.transit.publish = AsyncMock()
        mock_broker.logger = MagicMock()

        monitor = LatencyMonitor(broker=mock_broker)

        ping_id = await monitor.send_ping("target-node")

        # Verify PING packet was sent
        mock_broker.transit.publish.assert_called_once()
        packet = mock_broker.transit.publish.call_args[0][0]

        assert packet.type == Topic.PING
        assert packet.target == "target-node"
        assert packet.payload["id"] == ping_id
        assert "time" in packet.payload
        assert packet.payload["sender"] == "master-node"

        # Verify pending ping is tracked
        assert ping_id in monitor._pending_pings

    @pytest.mark.asyncio
    async def test_handle_ping_sends_pong(self) -> None:
        """handle_ping responds with PONG packet."""
        mock_broker = MagicMock()
        mock_broker.nodeID = "target-node"
        mock_broker.transit = MagicMock()
        mock_broker.transit.publish = AsyncMock()
        mock_broker.logger = MagicMock()

        monitor = LatencyMonitor(broker=mock_broker)

        ping_payload = {
            "id": "ping-123",
            "time": 1000.0,
            "sender": "master-node",
        }

        await monitor.handle_ping("master-node", ping_payload)

        # Verify PONG packet was sent
        mock_broker.transit.publish.assert_called_once()
        packet = mock_broker.transit.publish.call_args[0][0]

        assert packet.type == Topic.PONG
        assert packet.target == "master-node"
        assert packet.payload["id"] == "ping-123"
        assert packet.payload["time"] == 1000.0
        assert "arrived" in packet.payload
        assert packet.payload["sender"] == "target-node"

    @pytest.mark.asyncio
    async def test_handle_pong_calculates_latency(self) -> None:
        """handle_pong calculates and stores latency."""
        mock_broker = MagicMock()
        mock_broker.nodeID = "master-node"
        mock_broker.logger = MagicMock()
        mock_node = MagicMock()
        mock_node.hostname = "target-host"
        mock_broker.node_catalog.get_node.return_value = mock_node

        monitor = LatencyMonitor(broker=mock_broker)

        # Simulate pending ping
        original_time = time.time() * 1000 - 25  # 25ms ago
        monitor._pending_pings["ping-123"] = (original_time, "target-node")

        pong_payload = {
            "id": "ping-123",
            "time": original_time,
            "arrived": original_time + 10,  # Processing time
            "sender": "target-node",
        }

        await monitor.handle_pong("target-node", pong_payload)

        # Verify latency was calculated
        assert "target-host" in monitor._host_latency
        assert monitor._host_latency["target-host"] > 0

        # Verify pending ping was removed
        assert "ping-123" not in monitor._pending_pings

    @pytest.mark.asyncio
    async def test_handle_pong_ignores_unknown_ping(self) -> None:
        """handle_pong ignores PONG for unknown ping ID."""
        mock_broker = MagicMock()
        mock_broker.logger = MagicMock()

        monitor = LatencyMonitor(broker=mock_broker)

        pong_payload = {
            "id": "unknown-ping",
            "time": 1000.0,
            "arrived": 1010.0,
        }

        await monitor.handle_pong("target-node", pong_payload)

        # No latency should be recorded
        assert len(monitor._host_latency) == 0


class TestLatencySampling:
    """Tests for latency sample collection and averaging."""

    def test_add_sample_updates_average(self, mock_broker: MagicMock) -> None:
        """_add_sample updates host average latency."""
        monitor = LatencyMonitor(broker=mock_broker, sample_count=3)

        monitor._add_sample("host-1", 10.0)
        assert monitor._host_latency["host-1"] == 10.0

        monitor._add_sample("host-1", 20.0)
        assert monitor._host_latency["host-1"] == 15.0  # (10 + 20) / 2

        monitor._add_sample("host-1", 30.0)
        assert monitor._host_latency["host-1"] == 20.0  # (10 + 20 + 30) / 3

    def test_add_sample_respects_sample_count(self, mock_broker: MagicMock) -> None:
        """_add_sample keeps only last N samples (deque optimization)."""
        monitor = LatencyMonitor(broker=mock_broker, sample_count=3)

        # Add 5 samples
        for i in range(1, 6):
            monitor._add_sample("host-1", float(i * 10))

        # Should only keep last 3 (30, 40, 50) via deque(maxlen=3)
        assert len(monitor._samples["host-1"]) == 3
        assert monitor._host_latency["host-1"] == 40.0  # (30 + 40 + 50) / 3

    def test_clear_samples_specific_host(self, mock_broker: MagicMock) -> None:
        """clear_samples clears specific host data."""
        monitor = LatencyMonitor(broker=mock_broker)

        monitor._add_sample("host-1", 10.0)
        monitor._add_sample("host-2", 20.0)

        monitor.clear_samples("host-1")

        assert "host-1" not in monitor._host_latency
        assert "host-2" in monitor._host_latency

    def test_clear_samples_all(self, mock_broker: MagicMock) -> None:
        """clear_samples with no arg clears all data."""
        monitor = LatencyMonitor(broker=mock_broker)

        monitor._add_sample("host-1", 10.0)
        monitor._add_sample("host-2", 20.0)

        monitor.clear_samples()

        assert len(monitor._host_latency) == 0
        assert len(monitor._samples) == 0


class TestLatencyLookup:
    """Tests for latency lookup methods."""

    def test_get_host_latency(self, mock_broker: MagicMock) -> None:
        """get_host_latency returns latency for hostname."""
        monitor = LatencyMonitor(broker=mock_broker)
        monitor._host_latency["host-1"] = 15.5

        assert monitor.get_host_latency("host-1") == 15.5
        assert monitor.get_host_latency("unknown") is None
        assert monitor.get_host_latency(None) is None

    def test_get_node_latency(self, mock_broker_with_node: MagicMock) -> None:
        """get_node_latency looks up by node ID using hostname."""
        monitor = LatencyMonitor(broker=mock_broker_with_node)
        monitor._host_latency["remote-host"] = 20.0

        assert monitor.get_node_latency("remote-node") == 20.0

    def test_get_node_latency_fallback_to_node_id(self, mock_broker: MagicMock) -> None:
        """get_node_latency falls back to node_id if no hostname."""
        mock_node = MagicMock()
        mock_node.hostname = None
        mock_broker.node_catalog.get_node.return_value = mock_node

        monitor = LatencyMonitor(broker=mock_broker)
        monitor._host_latency["node-1"] = 25.0

        assert monitor.get_node_latency("node-1") == 25.0

    def test_host_latency_property_returns_copy(self, mock_broker: MagicMock) -> None:
        """host_latency property returns copy of internal dict."""
        monitor = LatencyMonitor(broker=mock_broker)
        monitor._host_latency["host-1"] = 10.0

        result = monitor.host_latency

        # Modifying result should not affect internal state
        result["host-1"] = 999.0
        assert monitor._host_latency["host-1"] == 10.0


class TestMasterSlavePattern:
    """Tests for Master/Slave latency monitoring pattern."""

    def test_is_master_property(self, mock_broker: MagicMock) -> None:
        """is_master property can be get and set."""
        monitor = LatencyMonitor(broker=mock_broker, is_master=False)

        assert monitor.is_master is False

        monitor.is_master = True
        assert monitor.is_master is True

    @pytest.mark.asyncio
    async def test_setting_master_starts_ping_cycle(self, mock_broker: MagicMock) -> None:
        """Setting is_master=True after start begins ping cycle."""
        monitor = LatencyMonitor(broker=mock_broker, is_master=False)

        await monitor.start()
        assert monitor._ping_task is None

        monitor.is_master = True
        assert monitor._ping_task is not None

        await monitor.stop()

    @pytest.mark.asyncio
    async def test_unsetting_master_stops_ping_cycle(self, mock_broker: MagicMock) -> None:
        """Setting is_master=False stops ping cycle."""
        monitor = LatencyMonitor(broker=mock_broker, is_master=True)

        await monitor.start()
        assert monitor._ping_task is not None

        monitor.is_master = False
        assert monitor._ping_task is None

        await monitor.stop()


class TestBrokerIntegration:
    """Tests for LatencyMonitor integration with broker."""

    @pytest.mark.asyncio
    async def test_broker_creates_latency_monitor(self) -> None:
        """ServiceBroker creates LatencyMonitor on init."""
        from moleculerpy.broker import ServiceBroker
        from moleculerpy.settings import Settings

        settings = Settings(transporter="memory://")
        broker = ServiceBroker(id="test-node", settings=settings)

        assert hasattr(broker, "latency_monitor")
        assert isinstance(broker.latency_monitor, LatencyMonitor)
        assert broker.transit._latency_monitor is broker.latency_monitor

    @pytest.mark.asyncio
    async def test_broker_starts_latency_monitor(self) -> None:
        """Broker.start() starts latency monitor."""
        from moleculerpy.broker import ServiceBroker
        from moleculerpy.settings import Settings

        settings = Settings(transporter="memory://")
        broker = ServiceBroker(id="test-node", settings=settings)

        await broker.start()

        assert broker.latency_monitor._started is True

        await broker.stop()

        assert broker.latency_monitor._started is False

    @pytest.mark.asyncio
    async def test_transit_handles_ping_pong(self) -> None:
        """Transit properly delegates PING/PONG to latency monitor."""
        from moleculerpy.broker import ServiceBroker
        from moleculerpy.settings import Settings

        settings = Settings(transporter="memory://")
        broker = ServiceBroker(id="test-node", settings=settings)

        await broker.start()

        # Create a mock PING packet
        ping_packet = Packet(
            Topic.PING,
            "test-node",
            {"id": "ping-1", "time": time.time() * 1000, "sender": "remote-node"},
        )
        ping_packet.sender = "remote-node"

        # Handle the PING - should send PONG
        await broker.transit._handle_ping(ping_packet)

        await broker.stop()


class TestMoleculerJsCompatibility:
    """Tests for Moleculer.js protocol compatibility."""

    @pytest.mark.asyncio
    async def test_ping_packet_format(self) -> None:
        """PING packet matches Moleculer.js format."""
        mock_broker = MagicMock()
        mock_broker.nodeID = "test-node"
        mock_broker.transit = MagicMock()
        mock_broker.transit.publish = AsyncMock()
        mock_broker.logger = MagicMock()

        monitor = LatencyMonitor(broker=mock_broker)

        await monitor.send_ping("target-node")

        packet = mock_broker.transit.publish.call_args[0][0]

        # Moleculer.js PING format: {id: uuid, time: timestamp_ms, sender: nodeId}
        assert "id" in packet.payload
        assert "time" in packet.payload
        assert "sender" in packet.payload
        assert isinstance(packet.payload["time"], float)

    @pytest.mark.asyncio
    async def test_pong_packet_format(self) -> None:
        """PONG packet matches Moleculer.js format."""
        mock_broker = MagicMock()
        mock_broker.nodeID = "test-node"
        mock_broker.transit = MagicMock()
        mock_broker.transit.publish = AsyncMock()
        mock_broker.logger = MagicMock()

        monitor = LatencyMonitor(broker=mock_broker)

        await monitor.handle_ping("sender-node", {"id": "ping-1", "time": 1000.0})

        packet = mock_broker.transit.publish.call_args[0][0]

        # Moleculer.js PONG format: {id: pingId, time: originalTime, arrived: timestamp_ms, sender: nodeId}
        assert packet.payload["id"] == "ping-1"
        assert packet.payload["time"] == 1000.0
        assert "arrived" in packet.payload
        assert "sender" in packet.payload
        assert isinstance(packet.payload["arrived"], float)

    @pytest.mark.asyncio
    async def test_latency_is_round_trip(self) -> None:
        """Latency calculation is RTT (round-trip time)."""
        mock_broker = MagicMock()
        mock_broker.nodeID = "master-node"
        mock_broker.logger = MagicMock()
        mock_node = MagicMock()
        mock_node.hostname = "target-host"
        mock_broker.node_catalog.get_node.return_value = mock_node

        monitor = LatencyMonitor(broker=mock_broker)

        # Simulate: PING sent at T0=1000ms, PONG received at T4=1050ms
        # RTT = T4 - T0 = 50ms
        original_time = 1000.0
        monitor._pending_pings["ping-1"] = (original_time, "target-node")

        # Simulate receiving PONG 50ms later
        with patch("time.time", return_value=1.050):  # 1050ms
            await monitor.handle_pong(
                "target-node",
                {
                    "id": "ping-1",
                    "time": original_time,
                    "arrived": 1025.0,
                },
            )

        # Latency should be ~50ms (RTT)
        latency = monitor._host_latency["target-host"]
        assert 45 < latency < 55  # Allow some tolerance
