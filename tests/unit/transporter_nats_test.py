"""Unit tests for the NATS transporter."""

import json
import logging
from unittest.mock import AsyncMock, Mock

import pytest

from moleculerpy.packet import Packet, Topic
from moleculerpy.serializers import JsonSerializer
from moleculerpy.transporter.nats import NatsTransporter


def _mock_transit() -> Mock:
    """Create a mock transit with a real JsonSerializer."""
    transit = Mock()
    transit.serializer = JsonSerializer()
    return transit


class TestNatsTransporter:
    """Test NATS transporter functionality."""

    @pytest.mark.asyncio
    async def test_nats_transporter_sets_packet_sender_regression(self):
        """Regression test: NATS transporter should set sender attribute on packets."""
        # Previously, packets were missing the sender attribute which caused errors
        # in transit layer message handlers when accessing packet.sender

        # Mock NATS message
        mock_msg = Mock()
        mock_msg.subject = "MOL.HEARTBEAT.node123"
        mock_msg.data = b'{"sender": "remote-node", "cpu": 30.5}'

        # Create NATS transporter
        handler = AsyncMock()
        transporter = NatsTransporter(
            connection_string="nats://localhost:4222",
            transit=_mock_transit(),
            handler=handler,
            node_id="local-node",
        )

        # Process the message
        await transporter.message_handler(mock_msg)

        # Check that handler was called with packet having sender set
        assert handler.called
        packet = handler.call_args[0][0]
        assert isinstance(packet, Packet)
        assert packet.sender == "remote-node"
        assert packet.type == Topic.HEARTBEAT
        assert packet.payload["cpu"] == 30.5

    @pytest.mark.asyncio
    async def test_nats_message_handler_creates_packet_with_sender_regression(self):
        """Regression test: Message handler should create packets with sender attribute."""
        # Mock NATS message with different packet types
        test_cases = [
            ("MOL.INFO.node1", b'{"sender": "info-node", "id": "test"}', Topic.INFO),
            ("MOL.HEARTBEAT.node2", b'{"sender": "beat-node", "cpu": 25}', Topic.HEARTBEAT),
            ("MOL.EVENT.node3", b'{"sender": "event-node", "event": "test"}', Topic.EVENT),
        ]

        for subject, data, expected_topic in test_cases:
            handler = AsyncMock()
            transporter = NatsTransporter(
                connection_string="nats://localhost:4222",
                transit=_mock_transit(),
                handler=handler,
                node_id="local-node",
            )

            mock_msg = Mock()
            mock_msg.subject = subject
            mock_msg.data = data

            await transporter.message_handler(mock_msg)

            # Verify packet creation and sender attribute
            assert handler.called
            packet = handler.call_args[0][0]
            assert isinstance(packet, Packet)
            assert packet.type == expected_topic
            assert hasattr(packet, "sender")
            assert packet.sender is not None

            handler.reset_mock()

    @pytest.mark.asyncio
    async def test_message_handler_ignores_unknown_topic_with_warning(self, caplog):
        """Unknown topics should be ignored and logged, not raised."""
        handler = AsyncMock()
        transporter = NatsTransporter(
            connection_string="nats://localhost:4222",
            transit=_mock_transit(),
            handler=handler,
            node_id="local-node",
        )
        mock_msg = Mock()
        mock_msg.subject = "UNKNOWN.TOPIC"
        mock_msg.data = b'{"sender": "remote-node"}'

        with caplog.at_level(logging.WARNING):
            await transporter.message_handler(mock_msg)

        handler.assert_not_called()
        assert "unknown topic" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_disconnect_uses_close_timeout(self):
        """Disconnect should wrap nc.close() with timeout to avoid hanging forever."""
        transporter = NatsTransporter(
            connection_string="nats://localhost:4222",
            transit=_mock_transit(),
            handler=AsyncMock(),
            node_id="local-node",
        )

        nc = AsyncMock()
        transporter.nc = nc
        timeout_seen: float | None = None

        async def fake_wait_for(awaitable, timeout):
            nonlocal timeout_seen
            timeout_seen = timeout
            return await awaitable

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr("moleculerpy.transporter.nats.asyncio.wait_for", fake_wait_for)
            await transporter.disconnect()

        assert timeout_seen == 5.0
        nc.close.assert_awaited_once()
        assert transporter.nc is None

    @pytest.mark.asyncio
    async def test_disconnect_timeout_keeps_client_reference(self):
        """Timeout should keep client reference so caller can retry/inspect cleanup."""
        transporter = NatsTransporter(
            connection_string="nats://localhost:4222",
            transit=_mock_transit(),
            handler=AsyncMock(),
            node_id="local-node",
        )
        nc = AsyncMock()
        transporter.nc = nc

        async def fake_wait_for(awaitable, timeout):
            awaitable.close()
            raise TimeoutError()

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr("moleculerpy.transporter.nats.asyncio.wait_for", fake_wait_for)
            await transporter.disconnect()

        assert transporter.nc is nc

    @pytest.mark.asyncio
    async def test_disconnect_exception_keeps_client_reference(self):
        """Unexpected close exception should keep client reference."""
        transporter = NatsTransporter(
            connection_string="nats://localhost:4222",
            transit=_mock_transit(),
            handler=AsyncMock(),
            node_id="local-node",
        )
        nc = AsyncMock()
        transporter.nc = nc

        async def fake_wait_for(awaitable, timeout):
            awaitable.close()
            raise RuntimeError("close failed")

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr("moleculerpy.transporter.nats.asyncio.wait_for", fake_wait_for)
            await transporter.disconnect()

        assert transporter.nc is nc

    @pytest.mark.asyncio
    async def test_receive_offloads_large_json_deserialize(self):
        """Large payloads should deserialize via asyncio.to_thread to avoid event-loop blocking."""
        handler = AsyncMock()
        transporter = NatsTransporter(
            connection_string="nats://localhost:4222",
            transit=_mock_transit(),
            handler=handler,
            node_id="local-node",
        )
        data = json.dumps(
            {
                "sender": "remote-node",
                "blob": "x" * (1024 * 1024),
            }
        ).encode("utf-8")
        meta = {"packet_type": Topic.INFO}
        called = {"value": False}

        async def fake_to_thread(func, *args):
            called["value"] = True
            return func(*args)

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr("moleculerpy.serializers.base.asyncio.to_thread", fake_to_thread)
            await transporter.receive("INFO", data, meta)

        assert called["value"] is True
        handler.assert_awaited_once()
