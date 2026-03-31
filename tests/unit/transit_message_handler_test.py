"""Unit tests for transit_message_handler middleware hook."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from moleculerpy.middleware.base import Middleware
from moleculerpy.packet import Packet, Topic
from moleculerpy.transit import Transit


@pytest.fixture
def mock_dependencies():
    """Create mock dependencies for Transit."""
    mock_settings = MagicMock(transporter="nats://localhost:4222")
    mock_settings.request_timeout = 30.0
    mock_settings.serializer = "JSON"
    return {
        "node_id": "test-node-123",
        "registry": MagicMock(),
        "node_catalog": MagicMock(),
        "settings": mock_settings,
        "logger": MagicMock(),
        "lifecycle": MagicMock(),
    }


@pytest.fixture
def mock_transporter():
    """Create a mock transporter."""
    transporter = AsyncMock()
    transporter.connect = AsyncMock()
    transporter.disconnect = AsyncMock()
    transporter.publish = AsyncMock()
    transporter.subscribe = AsyncMock()
    return transporter


class TestTransitMessageHandlerHook:
    """Tests for the transit_message_handler middleware hook."""

    def test_hook_in_base_middleware(self):
        """Verify transit_message_handler exists in Middleware base class."""
        mw = Middleware.__new__(Middleware)
        assert hasattr(mw, "transit_message_handler")
        assert callable(mw.transit_message_handler)

    @pytest.mark.asyncio
    async def test_base_middleware_is_passthrough(self):
        """Base Middleware.transit_message_handler is a no-op passthrough."""
        mw = Middleware.__new__(Middleware)
        called = False

        async def next_handler(packet: Packet) -> None:
            nonlocal called
            called = True

        packet = Packet(Topic.REQUEST, "other-node", {"ver": "4", "action": "test"})
        await mw.transit_message_handler(next_handler, packet)
        assert called, "Base middleware should call next_handler"

    @pytest.mark.asyncio
    async def test_middleware_intercepts_packets(self, mock_dependencies, mock_transporter):
        """Custom middleware receives packet, calls next, dispatch happens."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Track calls
            intercepted_packets: list[Packet] = []

            class InterceptMiddleware(Middleware):
                async def transit_message_handler(self, next_handler, packet):
                    intercepted_packets.append(packet)
                    return await next_handler(packet)

            broker = MagicMock()
            broker.middlewares = [InterceptMiddleware()]
            transit._wrap_methods(broker)

            # Mock the handler dispatch table
            transit._handle_heartbeat = AsyncMock()
            transit._packet_handlers[Topic.HEARTBEAT] = transit._handle_heartbeat

            packet = Packet(Topic.HEARTBEAT, "other-node", {"ver": "4", "cpu": 50})
            packet.sender = "other-node"
            await transit._message_handler(packet)

            assert len(intercepted_packets) == 1
            assert intercepted_packets[0] is packet
            transit._handle_heartbeat.assert_called_once_with(packet)

    @pytest.mark.asyncio
    async def test_middleware_can_drop_packet(self, mock_dependencies, mock_transporter):
        """Middleware returns without calling next -> packet NOT dispatched."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            class DropMiddleware(Middleware):
                async def transit_message_handler(self, next_handler, packet):
                    # Drop packet — do NOT call next_handler
                    return

            broker = MagicMock()
            broker.middlewares = [DropMiddleware()]
            transit._wrap_methods(broker)

            transit._handle_request = AsyncMock()
            transit._packet_handlers[Topic.REQUEST] = transit._handle_request

            packet = Packet(Topic.REQUEST, "other-node", {"ver": "4", "action": "test"})
            packet.sender = "other-node"
            await transit._message_handler(packet)

            transit._handle_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_middleware_chain_order(self, mock_dependencies, mock_transporter):
        """Two middlewares: outer runs first, inner runs second."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            call_order: list[str] = []

            class FirstMiddleware(Middleware):
                async def transit_message_handler(self, next_handler, packet):
                    call_order.append("first_before")
                    await next_handler(packet)
                    call_order.append("first_after")

            class SecondMiddleware(Middleware):
                async def transit_message_handler(self, next_handler, packet):
                    call_order.append("second_before")
                    await next_handler(packet)
                    call_order.append("second_after")

            broker = MagicMock()
            # First added = outermost (wraps second)
            broker.middlewares = [FirstMiddleware(), SecondMiddleware()]
            transit._wrap_methods(broker)

            transit._handle_heartbeat = AsyncMock()
            transit._packet_handlers[Topic.HEARTBEAT] = transit._handle_heartbeat

            packet = Packet(Topic.HEARTBEAT, "other-node", {"ver": "4", "cpu": 50})
            packet.sender = "other-node"
            await transit._message_handler(packet)

            assert call_order == [
                "first_before",
                "second_before",
                "second_after",
                "first_after",
            ]

    @pytest.mark.asyncio
    async def test_no_middleware_passthrough(self, mock_dependencies, mock_transporter):
        """Without middleware, packets processed normally via _message_handler_core."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # No _wrap_methods called, but _wrapped_message_handler set with empty middlewares
            broker = MagicMock()
            broker.middlewares = []
            transit._wrap_methods(broker)

            transit._handle_heartbeat = AsyncMock()
            transit._packet_handlers[Topic.HEARTBEAT] = transit._handle_heartbeat

            packet = Packet(Topic.HEARTBEAT, "other-node", {"ver": "4", "cpu": 50})
            packet.sender = "other-node"
            await transit._message_handler(packet)

            transit._handle_heartbeat.assert_called_once_with(packet)

    @pytest.mark.asyncio
    async def test_backwards_compat_without_wrap(self, mock_dependencies, mock_transporter):
        """Before _wrap_methods called, _message_handler still works via _message_handler_core."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Do NOT call _wrap_methods — simulate pre-start state

            transit._handle_heartbeat = AsyncMock()
            transit._packet_handlers[Topic.HEARTBEAT] = transit._handle_heartbeat

            packet = Packet(Topic.HEARTBEAT, "other-node", {"ver": "4", "cpu": 50})
            packet.sender = "other-node"
            await transit._message_handler(packet)

            transit._handle_heartbeat.assert_called_once_with(packet)

    @pytest.mark.asyncio
    async def test_protocol_check_still_works(self, mock_dependencies, mock_transporter):
        """Protocol version check in _message_handler_core still rejects bad packets."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Wire up middleware
            broker = MagicMock()
            broker.middlewares = []
            transit._wrap_methods(broker)

            transit._handle_request = AsyncMock()
            transit._packet_handlers[Topic.REQUEST] = transit._handle_request

            packet = Packet(Topic.REQUEST, "other-node", {"ver": "3", "action": "test"})
            packet.sender = "other-node"
            await transit._message_handler(packet)

            # Should be rejected — handler NOT called
            transit._handle_request.assert_not_called()
            transit.logger.warning.assert_called_once()
            assert "Protocol version mismatch" in transit.logger.warning.call_args[0][0]

    @pytest.mark.asyncio
    async def test_node_id_conflict_still_works(self, mock_dependencies, mock_transporter):
        """NodeID conflict detection still triggers through middleware chain."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            mock_dependencies["node_id"] = "my-node"
            transit = Transit(**mock_dependencies)

            # Wire up middleware — _wrap_methods sets self._broker = broker
            broker = MagicMock()
            broker.middlewares = []
            broker.stop = AsyncMock(return_value=None)
            transit._wrap_methods(broker)

            # Use REQUEST topic — not in _SELF_ECHO_TOPICS
            packet = Packet(Topic.REQUEST, "my-node", {"action": "test", "ver": "4"})
            packet.sender = "my-node"
            await transit._message_handler(packet)

            transit.logger.critical.assert_called_once()
            assert "NodeID conflict" in transit.logger.critical.call_args[0][0]

    @pytest.mark.asyncio
    async def test_middleware_exception_propagates(
        self, mock_dependencies: dict[str, Any], mock_transporter: Any
    ) -> None:
        """Exception raised in middleware propagates out of _message_handler."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            class BrokenMiddleware(Middleware):
                async def transit_message_handler(self, next_handler: Any, packet: Any) -> None:
                    raise RuntimeError("middleware boom")

            broker = MagicMock()
            broker.middlewares = [BrokenMiddleware()]
            transit._wrap_methods(broker)

            packet = Packet(Topic.HEARTBEAT, "other-node", {"ver": "4", "cpu": 50})
            packet.sender = "other-node"

            with pytest.raises(RuntimeError, match="middleware boom"):
                await transit._message_handler(packet)

    @pytest.mark.asyncio
    async def test_middleware_can_modify_packet(
        self, mock_dependencies: dict[str, Any], mock_transporter: Any
    ) -> None:
        """Middleware can mutate packet payload before next handler sees it."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            class MutatingMiddleware(Middleware):
                async def transit_message_handler(self, next_handler: Any, packet: Any) -> None:
                    packet.payload["injected"] = "yes"
                    return await next_handler(packet)

            broker = MagicMock()
            broker.middlewares = [MutatingMiddleware()]
            transit._wrap_methods(broker)

            seen_payloads: list[dict[str, Any]] = []

            async def capturing_handler(p: Any) -> None:
                seen_payloads.append(dict(p.payload))

            transit._packet_handlers[Topic.HEARTBEAT] = capturing_handler

            packet = Packet(Topic.HEARTBEAT, "other-node", {"ver": "4", "cpu": 50})
            packet.sender = "other-node"
            await transit._message_handler(packet)

            assert seen_payloads[0].get("injected") == "yes"
