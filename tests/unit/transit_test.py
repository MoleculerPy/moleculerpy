"""Unit tests for the Transit module."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from moleculerpy.packet import Packet, Topic
from moleculerpy.transit import RemoteCallError, Transit


@pytest.fixture
def mock_dependencies():
    """Create mock dependencies for Transit."""
    mock_settings = MagicMock(transporter="nats://localhost:4222")
    # Ensure request_timeout is a float, not MagicMock
    mock_settings.request_timeout = 30.0
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


class TestRemoteCallError:
    """Test RemoteCallError exception."""

    def test_remote_call_error_initialization(self):
        """Test RemoteCallError with all parameters."""
        error = RemoteCallError("Test error", "CustomError", "Stack trace here")
        assert str(error) == "Test error"
        assert error.error_name == "CustomError"
        assert error.stack == "Stack trace here"

    def test_remote_call_error_defaults(self):
        """Test RemoteCallError with default parameters."""
        error = RemoteCallError("Test error")
        assert str(error) == "Test error"
        assert error.error_name == "RemoteError"
        assert error.stack is None


class TestTransit:
    """Test Transit class."""

    def test_transit_initialization(self, mock_dependencies, mock_transporter):
        """Test Transit initialization."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            assert transit.node_id == "test-node-123"
            assert transit.registry == mock_dependencies["registry"]
            assert transit.node_catalog == mock_dependencies["node_catalog"]
            assert transit.logger == mock_dependencies["logger"]
            assert transit.lifecycle == mock_dependencies["lifecycle"]
            assert transit.transporter == mock_transporter
            assert transit._pending_requests == {}

    @pytest.mark.asyncio
    async def test_connect(self, mock_dependencies, mock_transporter):
        """Test Transit connect method."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)
            transit.discover = AsyncMock()
            transit.send_node_info = AsyncMock()
            transit._make_subscriptions = AsyncMock()

            await transit.connect()

            mock_transporter.connect.assert_called_once()
            transit.discover.assert_called_once()
            transit.send_node_info.assert_called_once()
            transit._make_subscriptions.assert_called_once()

    @pytest.mark.asyncio
    async def test_disconnect(self, mock_dependencies, mock_transporter):
        """Test Transit disconnect method."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Add a pending request
            future = asyncio.Future()
            transit._pending_requests["test-req"] = future

            await transit.disconnect()

            # Check that disconnect packet was published
            mock_transporter.publish.assert_called()
            packet = mock_transporter.publish.call_args[0][0]
            assert packet.type == Topic.DISCONNECT

            # Check that pending requests were cancelled
            assert future.cancelled()
            assert len(transit._pending_requests) == 0

            # Check that transporter was disconnected
            mock_transporter.disconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish(self, mock_dependencies, mock_transporter):
        """Test Transit publish method."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)
            packet = Packet(Topic.INFO, None, {"test": "data"})

            await transit.publish(packet)

            mock_transporter.publish.assert_called_once_with(packet)

    @pytest.mark.asyncio
    async def test_discover(self, mock_dependencies, mock_transporter):
        """Test Transit discover method."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            await transit.discover()

            mock_transporter.publish.assert_called_once()
            packet = mock_transporter.publish.call_args[0][0]
            assert packet.type == Topic.DISCOVER
            assert packet.target is None
            assert packet.payload == {}

    @pytest.mark.asyncio
    async def test_beat(self, mock_dependencies, mock_transporter):
        """Test Transit beat method.

        Phase 5.1: Updated to test Moleculer.js compatible HEARTBEAT format:
        - cpu: int (0-100, rounded)
        - cpuSeq: int (increments when CPU changes)
        - memory: float (Python extension)
        """
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            with patch("psutil.cpu_percent", return_value=25.5):
                with patch("psutil.virtual_memory") as mock_mem:
                    mock_mem.return_value = MagicMock(
                        percent=45.0,
                        total=16 * 1024**3,
                        available=8 * 1024**3,
                        used=8 * 1024**3,
                    )
                    transit = Transit(**mock_dependencies)

                    await transit.beat()

                    mock_transporter.publish.assert_called_once()
                    packet = mock_transporter.publish.call_args[0][0]
                    assert packet.type == Topic.HEARTBEAT
                    # CPU is now rounded to int like Moleculer.js
                    assert packet.payload["cpu"] == 26  # round(25.5) = 26
                    # Phase 5.1: cpuSeq and memory added
                    assert "cpuSeq" in packet.payload
                    assert packet.payload["cpuSeq"] == 1  # First call, first increment
                    assert "memory" in packet.payload
                    assert packet.payload["memory"] == 45.0

    @pytest.mark.asyncio
    async def test_send_node_info(self, mock_dependencies, mock_transporter):
        """Test Transit send_node_info method."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Test with no local node
            transit.node_catalog.local_node = None
            await transit.send_node_info()
            mock_transporter.publish.assert_not_called()

            # Test with local node
            mock_node = MagicMock()
            mock_node.get_info.return_value = {"id": "test-node", "services": []}
            transit.node_catalog.local_node = mock_node

            await transit.send_node_info()

            mock_transporter.publish.assert_called_once()
            packet = mock_transporter.publish.call_args[0][0]
            assert packet.type == Topic.INFO
            assert packet.payload == {"id": "test-node", "services": []}

    @pytest.mark.asyncio
    async def test_make_subscriptions(self, mock_dependencies, mock_transporter):
        """Test Transit _make_subscriptions method."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            await transit._make_subscriptions()

            # Check that all necessary subscriptions were made
            expected_calls = [
                (Topic.INFO.value, None),
                (Topic.INFO.value, "test-node-123"),
                (Topic.DISCOVER.value, None),
                (Topic.HEARTBEAT.value, None),
                (Topic.REQUEST.value, "test-node-123"),
                (Topic.RESPONSE.value, "test-node-123"),
                (Topic.EVENT.value, "test-node-123"),
                (Topic.EVENT_ACK.value, "test-node-123"),  # Phase 3C: Event Ack subscription
                (Topic.DISCONNECT.value, None),
                (Topic.PING.value, "test-node-123"),  # Phase 5: Latency measurement
                (Topic.PONG.value, "test-node-123"),  # Phase 5: Latency measurement
            ]

            assert mock_transporter.subscribe.call_count == len(expected_calls)
            for i, (topic, node_id) in enumerate(expected_calls):
                call_args = mock_transporter.subscribe.call_args_list[i][0]
                assert call_args == (topic, node_id)

    @pytest.mark.asyncio
    async def test_handle_discover(self, mock_dependencies, mock_transporter):
        """Test Transit _handle_discover method."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)
            transit.send_node_info = AsyncMock()

            packet = Packet(Topic.DISCOVER, "other-node", {})
            await transit._handle_discover(packet)

            transit.send_node_info.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_heartbeat(self, mock_dependencies, mock_transporter):
        """Test Transit _handle_heartbeat method."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            mock_node = MagicMock()
            transit.node_catalog.get_node.return_value = mock_node

            packet = Packet(Topic.HEARTBEAT, "other-node", {"cpu": 50.0})
            packet.sender = "other-node"
            await transit._handle_heartbeat(packet)

            transit.node_catalog.get_node.assert_called_once_with("other-node")
            assert mock_node.cpu == 50.0

    @pytest.mark.asyncio
    async def test_handle_info(self, mock_dependencies, mock_transporter):
        """Test Transit _handle_info method."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            packet = Packet(
                Topic.INFO,
                "other-node",
                {"id": "other-node", "services": ["service1", "service2"], "ip": "192.168.1.1"},
            )
            packet.sender = "other-node"

            await transit._handle_info(packet)

            transit.node_catalog.process_node_info.assert_called_once_with(
                "other-node",
                {"id": "other-node", "services": ["service1", "service2"]},
            )

    @pytest.mark.asyncio
    async def test_handle_info_uses_node_catalog_process_node_info(
        self, mock_dependencies, mock_transporter
    ):
        """INFO packets should be routed through node_catalog.process_node_info."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            packet = Packet(
                Topic.INFO,
                "other-node",
                {
                    "id": "other-node",
                    "services": [{"name": "users"}],
                    "ipList": ["127.0.0.1"],
                    "instanceID": "inst-1",
                    "customField": "ignored",
                },
            )
            packet.sender = "other-node"

            await transit._handle_info(packet)

            transit.node_catalog.process_node_info.assert_called_once_with(
                "other-node",
                {
                    "id": "other-node",
                    "services": [{"name": "users"}],
                    "ipList": ["127.0.0.1"],
                    "instanceID": "inst-1",
                },
            )
            transit.node_catalog.add_node.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_disconnect(self, mock_dependencies, mock_transporter):
        """Test Transit _handle_disconnect method."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            packet = Packet(Topic.DISCONNECT, "other-node", {})
            packet.sender = "other-node"

            await transit._handle_disconnect(packet)

            transit.node_catalog.disconnect_node.assert_called_once_with("other-node")

    @pytest.mark.asyncio
    async def test_handle_event(self, mock_dependencies, mock_transporter):
        """Test Transit _handle_event method."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Setup mock event endpoint
            mock_endpoint = MagicMock()
            mock_endpoint.is_local = True
            mock_endpoint.wrapped_handler = None
            mock_endpoint.handler = AsyncMock()
            mock_endpoint.name = "test.event"
            transit.registry.get_event.return_value = mock_endpoint

            mock_context = MagicMock()
            transit.lifecycle.rebuild_context.return_value = mock_context

            packet = Packet(Topic.EVENT, "other-node", {"event": "test.event", "data": "test"})
            await transit._handle_event(packet)

            transit.registry.get_event.assert_called_once_with("test.event")
            transit.lifecycle.rebuild_context.assert_called_once_with(
                {"event": "test.event", "data": "test"}
            )
            mock_endpoint.handler.assert_called_once_with(mock_context)

    @pytest.mark.asyncio
    async def test_handle_event_uses_wrapped_handler(self, mock_dependencies, mock_transporter):
        """Test that _handle_event prefers wrapped_handler over raw handler."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            mock_endpoint = MagicMock()
            mock_endpoint.is_local = True
            mock_endpoint.wrapped_handler = AsyncMock()
            mock_endpoint.handler = AsyncMock()
            mock_endpoint.name = "test.event"
            transit.registry.get_event.return_value = mock_endpoint

            mock_context = MagicMock()
            transit.lifecycle.rebuild_context.return_value = mock_context

            packet = Packet(Topic.EVENT, "other-node", {"event": "test.event", "data": "test"})
            await transit._handle_event(packet)

            mock_endpoint.wrapped_handler.assert_called_once_with(mock_context)
            mock_endpoint.handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_request_success(self, mock_dependencies, mock_transporter):
        """Test Transit _handle_request method with successful execution."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Setup mock action endpoint
            mock_endpoint = MagicMock()
            mock_endpoint.is_local = True
            mock_endpoint.wrapped_handler = None
            mock_endpoint.handler = AsyncMock(return_value={"result": "success"})
            mock_endpoint.name = "test.action"
            mock_endpoint.params_schema = None
            transit.registry.get_action.return_value = mock_endpoint

            mock_context = MagicMock()
            mock_context.id = "req-123"
            mock_context.params = {}
            transit.lifecycle.rebuild_context.return_value = mock_context

            packet = Packet(Topic.REQUEST, "other-node", {"action": "test.action", "id": "req-123"})
            packet.sender = "other-node"
            await transit._handle_request(packet)

            # Check that response was sent
            mock_transporter.publish.assert_called_once()
            response_packet = mock_transporter.publish.call_args[0][0]
            assert response_packet.type == Topic.RESPONSE
            assert response_packet.target == "other-node"
            assert response_packet.payload["success"] is True
            assert response_packet.payload["data"] == {"result": "success"}

    @pytest.mark.asyncio
    async def test_handle_request_error(self, mock_dependencies, mock_transporter):
        """Test Transit _handle_request method with handler error."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Setup mock action endpoint that raises error
            mock_endpoint = MagicMock()
            mock_endpoint.is_local = True
            mock_endpoint.wrapped_handler = None
            mock_endpoint.handler = AsyncMock(side_effect=ValueError("Test error"))
            mock_endpoint.name = "test.action"
            mock_endpoint.params_schema = None
            transit.registry.get_action.return_value = mock_endpoint

            mock_context = MagicMock()
            mock_context.id = "req-123"
            mock_context.params = {}
            transit.lifecycle.rebuild_context.return_value = mock_context

            packet = Packet(Topic.REQUEST, "other-node", {"action": "test.action", "id": "req-123"})
            packet.sender = "other-node"
            await transit._handle_request(packet)

            # Check that error response was sent
            mock_transporter.publish.assert_called_once()
            response_packet = mock_transporter.publish.call_args[0][0]
            assert response_packet.type == Topic.RESPONSE
            assert response_packet.payload["success"] is False
            assert "error" in response_packet.payload
            assert response_packet.payload["error"]["name"] == "ValueError"
            assert response_packet.payload["error"]["message"] == "Test error"

    @pytest.mark.asyncio
    async def test_handle_request_uses_wrapped_handler(self, mock_dependencies, mock_transporter):
        """Test that _handle_request prefers wrapped_handler over raw handler."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            mock_endpoint = MagicMock()
            mock_endpoint.is_local = True
            mock_endpoint.wrapped_handler = AsyncMock(return_value={"result": "wrapped"})
            mock_endpoint.handler = AsyncMock(return_value={"result": "raw"})
            mock_endpoint.name = "test.action"
            mock_endpoint.params_schema = None
            transit.registry.get_action.return_value = mock_endpoint

            mock_context = MagicMock()
            mock_context.id = "req-123"
            mock_context.params = {}
            transit.lifecycle.rebuild_context.return_value = mock_context

            packet = Packet(Topic.REQUEST, "other-node", {"action": "test.action", "id": "req-123"})
            packet.sender = "other-node"
            await transit._handle_request(packet)

            mock_endpoint.wrapped_handler.assert_called_once_with(mock_context)
            mock_endpoint.handler.assert_not_called()

            # Verify wrapped_handler result was used in response
            mock_transporter.publish.assert_called_once()
            response_packet = mock_transporter.publish.call_args[0][0]
            assert response_packet.payload["success"] is True
            assert response_packet.payload["data"] == {"result": "wrapped"}

    @pytest.mark.asyncio
    async def test_handle_response(self, mock_dependencies, mock_transporter):
        """Test Transit _handle_response method."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Create a pending request
            future = asyncio.Future()
            transit._pending_requests["req-123"] = future

            packet = Packet(
                Topic.RESPONSE,
                "other-node",
                {"id": "req-123", "success": True, "data": {"result": "success"}},
            )

            await transit._handle_response(packet)

            # Check that future was resolved
            assert future.done()
            assert future.result() == {
                "id": "req-123",
                "success": True,
                "data": {"result": "success"},
            }
            assert "req-123" not in transit._pending_requests

    @pytest.mark.asyncio
    async def test_request_success(self, mock_dependencies, mock_transporter):
        """Test Transit request method with successful response."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            mock_endpoint = MagicMock()
            mock_endpoint.node_id = "remote-node"
            mock_endpoint.name = "test.action"
            mock_endpoint.timeout = None  # Use settings.request_timeout

            mock_context = MagicMock()
            mock_context.id = "req-123"
            mock_context.marshall.return_value = {"id": "req-123", "action": "test.action"}

            # Simulate response
            async def simulate_response():
                await asyncio.sleep(0.01)
                packet = Packet(
                    Topic.RESPONSE,
                    "remote-node",
                    {"id": "req-123", "success": True, "data": {"result": "success"}},
                )
                await transit._handle_response(packet)

            # Start the response simulation
            response_task = asyncio.create_task(simulate_response())

            # Make the request
            result = await transit.request(mock_endpoint, mock_context)

            # Ensure the task completes
            await response_task

            assert result == {"result": "success"}
            mock_transporter.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_request_remote_error(self, mock_dependencies, mock_transporter):
        """Test Transit request method with remote error."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            mock_endpoint = MagicMock()
            mock_endpoint.node_id = "remote-node"
            mock_endpoint.name = "test.action"
            mock_endpoint.timeout = None  # Use settings.request_timeout

            mock_context = MagicMock()
            mock_context.id = "req-123"
            mock_context.marshall.return_value = {"id": "req-123", "action": "test.action"}

            # Simulate error response
            async def simulate_error_response():
                await asyncio.sleep(0.01)
                packet = Packet(
                    Topic.RESPONSE,
                    "remote-node",
                    {
                        "id": "req-123",
                        "success": False,
                        "error": {
                            "name": "CustomError",
                            "message": "Remote error occurred",
                            "stack": "Stack trace here",
                        },
                    },
                )
                await transit._handle_response(packet)

            # Start the response simulation
            error_task = asyncio.create_task(simulate_error_response())

            # Make the request and expect error
            with pytest.raises(RemoteCallError) as exc_info:
                await transit.request(mock_endpoint, mock_context)

            # Ensure the task completes
            await error_task

            assert str(exc_info.value) == "Remote error occurred"
            assert exc_info.value.error_name == "CustomError"
            assert exc_info.value.stack == "Stack trace here"

    @pytest.mark.asyncio
    async def test_request_timeout(self, mock_dependencies, mock_transporter):
        """Test Transit request method with timeout."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            # Set short timeout for this test
            mock_dependencies["settings"].request_timeout = 0.1
            transit = Transit(**mock_dependencies)

            mock_endpoint = MagicMock()
            mock_endpoint.node_id = "remote-node"
            mock_endpoint.name = "test.action"
            mock_endpoint.timeout = None  # Use settings.request_timeout (0.1s)

            mock_context = MagicMock()
            mock_context.id = "req-123"
            mock_context.marshall.return_value = {"id": "req-123", "action": "test.action"}

            # Make the request and expect timeout
            with pytest.raises(Exception) as exc_info:
                await transit.request(mock_endpoint, mock_context)

            assert "timed out" in str(exc_info.value)
            assert "req-123" not in transit._pending_requests

    @pytest.mark.asyncio
    async def test_send_event(self, mock_dependencies, mock_transporter):
        """Test Transit send_event method."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            mock_endpoint = MagicMock()
            mock_endpoint.node_id = "remote-node"
            mock_endpoint.name = "test.event"

            mock_context = MagicMock()
            mock_context.marshall.return_value = {"event": "test.event", "data": "test"}

            await transit.send_event(mock_endpoint, mock_context)

            mock_transporter.publish.assert_called_once()
            packet = mock_transporter.publish.call_args[0][0]
            assert packet.type == Topic.EVENT
            assert packet.target == "remote-node"
            assert packet.payload == {"event": "test.event", "data": "test"}

    @pytest.mark.asyncio
    async def test_send_event_uses_pre_marshaled_payload(self, mock_dependencies, mock_transporter):
        """send_event should skip context.marshall when payload is precomputed."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            mock_endpoint = MagicMock()
            mock_endpoint.node_id = "remote-node"
            mock_endpoint.name = "test.event"

            mock_context = MagicMock()
            marshalled = {"event": "test.event", "data": "cached"}

            await transit.send_event(
                mock_endpoint,
                mock_context,
                marshalled_context=marshalled,
            )

            mock_context.marshall.assert_not_called()
            mock_transporter.publish.assert_called_once()
            packet = mock_transporter.publish.call_args[0][0]
            assert packet.payload == marshalled

    @pytest.mark.asyncio
    async def test_message_handler_routing(self, mock_dependencies, mock_transporter):
        """Test Transit _message_handler routing to correct handlers."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Mock all handlers and update the dispatch table
            # (handlers are cached in __init__ for performance)
            transit._handle_info = AsyncMock()
            transit._handle_discover = AsyncMock()
            transit._handle_heartbeat = AsyncMock()
            transit._handle_request = AsyncMock()
            transit._handle_response = AsyncMock()
            transit._handle_event = AsyncMock()
            transit._handle_disconnect = AsyncMock()

            # Update the dispatch table with mocked handlers
            transit._packet_handlers = {
                Topic.INFO: transit._handle_info,
                Topic.DISCOVER: transit._handle_discover,
                Topic.HEARTBEAT: transit._handle_heartbeat,
                Topic.REQUEST: transit._handle_request,
                Topic.RESPONSE: transit._handle_response,
                Topic.EVENT: transit._handle_event,
                Topic.DISCONNECT: transit._handle_disconnect,
            }

            # Test each packet type
            test_cases = [
                (Topic.INFO, transit._handle_info),
                (Topic.DISCOVER, transit._handle_discover),
                (Topic.HEARTBEAT, transit._handle_heartbeat),
                (Topic.REQUEST, transit._handle_request),
                (Topic.RESPONSE, transit._handle_response),
                (Topic.EVENT, transit._handle_event),
                (Topic.DISCONNECT, transit._handle_disconnect),
            ]

            for topic, handler in test_cases:
                packet = Packet(topic, None, {})
                await transit._message_handler(packet)
                handler.assert_called_once_with(packet)
                handler.reset_mock()

    @pytest.mark.asyncio
    async def test_message_handler_unknown_type(self, mock_dependencies, mock_transporter):
        """Test Transit _message_handler with unknown packet type."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Create a packet with invalid type (using a mock)
            packet = MagicMock()
            packet.type = MagicMock()
            packet.type.value = "UNKNOWN_TYPE"

            # Should log warning but not raise
            await transit._message_handler(packet)

            transit.logger.warning.assert_called_once()
            assert "Unknown packet type" in str(transit.logger.warning.call_args)

    @pytest.mark.asyncio
    async def test_message_handler_error_handling(self, mock_dependencies, mock_transporter):
        """Test Transit _message_handler error handling."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Make handler raise an error
            transit._handle_info = AsyncMock(side_effect=RuntimeError("Handler error"))
            # Update dispatch table (handlers are cached in __init__ for performance)
            transit._packet_handlers[Topic.INFO] = transit._handle_info

            packet = Packet(Topic.INFO, None, {})

            # Should log error but not raise
            await transit._message_handler(packet)

            transit.logger.error.assert_called_once()
            assert "Error handling INFO packet" in str(transit.logger.error.call_args)

    @pytest.mark.asyncio
    async def test_packet_sender_access_regression(self, mock_dependencies, mock_transporter):
        """Regression test: Transit handlers should access packet.sender without error."""
        # Previously, accessing packet.sender would raise AttributeError
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Test heartbeat handler
            packet = Packet(Topic.HEARTBEAT, "target", {"cpu": 45.0})
            packet.sender = "remote-node"

            mock_node = MagicMock()
            transit.node_catalog.get_node.return_value = mock_node

            # Should not raise AttributeError
            await transit._handle_heartbeat(packet)
            transit.node_catalog.get_node.assert_called_once_with("remote-node")
            assert mock_node.cpu == 45.0

    @pytest.mark.asyncio
    async def test_node_field_mapping_regression(self, mock_dependencies, mock_transporter):
        """Regression test: Transit should handle legacy field names in node info."""
        # Previously, Node constructor would fail with legacy field names like 'ipList'.
        # INFO packets should now be routed via process_node_info preserving these keys.
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Create packet with legacy field names (like from Moleculer.js nodes)
            packet = Packet(
                Topic.INFO,
                "target",
                {
                    "id": "remote-node",
                    "ipList": ["192.168.1.100", "10.0.0.100"],  # Legacy field name
                    "instanceID": "instance-456",  # Legacy field name
                    "services": [{"name": "test-service"}],
                    "cpu": 35.5,
                    "available": True,
                },
            )
            packet.sender = "remote-node"

            # Should not raise TypeError about unexpected keyword arguments
            await transit._handle_info(packet)

            transit.node_catalog.process_node_info.assert_called_once_with(
                "remote-node",
                {
                    "id": "remote-node",
                    "ipList": ["192.168.1.100", "10.0.0.100"],
                    "instanceID": "instance-456",
                    "services": [{"name": "test-service"}],
                    "cpu": 35.5,
                    "available": True,
                },
            )

    @pytest.mark.asyncio
    async def test_node_field_filtering_regression(self, mock_dependencies, mock_transporter):
        """Regression test: Transit should filter out unknown INFO payload fields."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Create packet with mix of valid and invalid field names
            packet = Packet(
                Topic.INFO,
                "target",
                {
                    "id": "remote-node",
                    "ipList": ["192.168.1.200"],  # Should be mapped to ip_list
                    "hostname": "remote-host",  # Should be preserved as valid field
                    "cpu": 42.0,  # Should be preserved as valid field
                    "customField": "custom-value",  # Should be filtered out (invalid)
                    "instanceID": "inst-789",  # Should be mapped to instance_id
                    "unknownProperty": "unknown-value",  # Should be filtered out (invalid)
                },
            )
            packet.sender = "remote-node"

            # Should not raise TypeError about unexpected keyword arguments
            await transit._handle_info(packet)

            transit.node_catalog.process_node_info.assert_called_once_with(
                "remote-node",
                {
                    "id": "remote-node",
                    "ipList": ["192.168.1.200"],
                    "hostname": "remote-host",
                    "cpu": 42.0,
                    "instanceID": "inst-789",
                },
            )

    @pytest.mark.asyncio
    async def test_handle_request_success_includes_metadata(
        self, mock_dependencies, mock_transporter
    ):
        """Regression test: Transit should include context metadata in successful responses."""
        # Issue: Lines 304 was returning {"meta": {}} instead of {"meta": context.meta}
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Setup mock action endpoint
            mock_endpoint = MagicMock()
            mock_endpoint.is_local = True
            mock_endpoint.wrapped_handler = None
            mock_endpoint.handler = AsyncMock(return_value={"result": "success"})
            mock_endpoint.name = "test.action"
            mock_endpoint.params_schema = None
            transit.registry.get_action.return_value = mock_endpoint

            # Create context with metadata
            mock_context = MagicMock()
            mock_context.id = "req-123"
            mock_context.params = {}
            mock_context.meta = {
                "user_id": "user-456",
                "trace_id": "trace-789",
                "request_ip": "192.168.1.100",
                "custom_field": "custom_value",
            }
            transit.lifecycle.rebuild_context.return_value = mock_context

            packet = Packet(Topic.REQUEST, "other-node", {"action": "test.action", "id": "req-123"})
            packet.sender = "other-node"
            await transit._handle_request(packet)

            # Check that response was sent with metadata
            mock_transporter.publish.assert_called_once()
            response_packet = mock_transporter.publish.call_args[0][0]
            assert response_packet.type == Topic.RESPONSE
            assert response_packet.target == "other-node"
            assert response_packet.payload["success"] is True
            assert response_packet.payload["data"] == {"result": "success"}

            # CRITICAL: Verify metadata is included in response (line 304)
            assert "meta" in response_packet.payload
            assert response_packet.payload["meta"] == mock_context.meta
            assert response_packet.payload["meta"]["user_id"] == "user-456"
            assert response_packet.payload["meta"]["trace_id"] == "trace-789"
            assert response_packet.payload["meta"]["request_ip"] == "192.168.1.100"
            assert response_packet.payload["meta"]["custom_field"] == "custom_value"

    @pytest.mark.asyncio
    async def test_handle_request_error_includes_metadata(
        self, mock_dependencies, mock_transporter
    ):
        """Regression test: Transit should include context metadata in error responses."""
        # Issue: Line 316 was returning {"meta": {}} instead of {"meta": context.meta}
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Setup mock action endpoint that raises error
            mock_endpoint = MagicMock()
            mock_endpoint.is_local = True
            mock_endpoint.wrapped_handler = None
            mock_endpoint.handler = AsyncMock(side_effect=ValueError("Test error"))
            mock_endpoint.name = "test.action"
            mock_endpoint.params_schema = None
            transit.registry.get_action.return_value = mock_endpoint

            # Create context with metadata
            mock_context = MagicMock()
            mock_context.id = "req-456"
            mock_context.params = {}
            mock_context.meta = {
                "user_id": "user-789",
                "trace_id": "trace-abc",
                "session_id": "session-xyz",
                "auth_token": "token-123",
            }
            transit.lifecycle.rebuild_context.return_value = mock_context

            packet = Packet(Topic.REQUEST, "other-node", {"action": "test.action", "id": "req-456"})
            packet.sender = "other-node"
            await transit._handle_request(packet)

            # Check that error response was sent with metadata
            mock_transporter.publish.assert_called_once()
            response_packet = mock_transporter.publish.call_args[0][0]
            assert response_packet.type == Topic.RESPONSE
            assert response_packet.payload["success"] is False
            assert "error" in response_packet.payload
            assert response_packet.payload["error"]["name"] == "ValueError"
            assert response_packet.payload["error"]["message"] == "Test error"

            # CRITICAL: Verify metadata is included in error response (line 316)
            assert "meta" in response_packet.payload
            assert response_packet.payload["meta"] == mock_context.meta
            assert response_packet.payload["meta"]["user_id"] == "user-789"
            assert response_packet.payload["meta"]["trace_id"] == "trace-abc"
            assert response_packet.payload["meta"]["session_id"] == "session-xyz"
            assert response_packet.payload["meta"]["auth_token"] == "token-123"

    @pytest.mark.asyncio
    async def test_metadata_propagation_empty_meta(self, mock_dependencies, mock_transporter):
        """Test that responses handle empty metadata correctly."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Setup mock action endpoint
            mock_endpoint = MagicMock()
            mock_endpoint.is_local = True
            mock_endpoint.wrapped_handler = None
            mock_endpoint.handler = AsyncMock(return_value={"result": "success"})
            mock_endpoint.name = "test.action"
            mock_endpoint.params_schema = None
            transit.registry.get_action.return_value = mock_endpoint

            # Create context with empty metadata
            mock_context = MagicMock()
            mock_context.id = "req-789"
            mock_context.params = {}
            mock_context.meta = {}
            transit.lifecycle.rebuild_context.return_value = mock_context

            packet = Packet(Topic.REQUEST, "other-node", {"action": "test.action", "id": "req-789"})
            packet.sender = "other-node"
            await transit._handle_request(packet)

            # Check that response includes empty meta dict
            mock_transporter.publish.assert_called_once()
            response_packet = mock_transporter.publish.call_args[0][0]
            assert response_packet.type == Topic.RESPONSE
            assert response_packet.payload["success"] is True
            assert "meta" in response_packet.payload
            assert response_packet.payload["meta"] == {}

    @pytest.mark.asyncio
    async def test_metadata_propagation_nested_meta(self, mock_dependencies, mock_transporter):
        """Test that responses handle nested metadata structures correctly."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Setup mock action endpoint
            mock_endpoint = MagicMock()
            mock_endpoint.is_local = True
            mock_endpoint.wrapped_handler = None
            mock_endpoint.handler = AsyncMock(return_value={"result": "success"})
            mock_endpoint.name = "test.action"
            mock_endpoint.params_schema = None
            transit.registry.get_action.return_value = mock_endpoint

            # Create context with nested metadata
            mock_context = MagicMock()
            mock_context.id = "req-complex"
            mock_context.params = {}
            mock_context.meta = {
                "user": {
                    "id": "user-123",
                    "name": "Test User",
                    "roles": ["admin", "user"],
                },
                "request": {
                    "ip": "192.168.1.100",
                    "headers": {"user-agent": "TestAgent/1.0"},
                },
                "tracing": {
                    "trace_id": "trace-xyz",
                    "span_id": "span-abc",
                    "parent_span_id": "span-parent",
                },
            }
            transit.lifecycle.rebuild_context.return_value = mock_context

            packet = Packet(
                Topic.REQUEST, "other-node", {"action": "test.action", "id": "req-complex"}
            )
            packet.sender = "other-node"
            await transit._handle_request(packet)

            # Check that response includes complete nested metadata
            mock_transporter.publish.assert_called_once()
            response_packet = mock_transporter.publish.call_args[0][0]
            assert response_packet.type == Topic.RESPONSE
            assert response_packet.payload["success"] is True
            assert "meta" in response_packet.payload

            # Verify nested structure is preserved
            meta = response_packet.payload["meta"]
            assert meta["user"]["id"] == "user-123"
            assert meta["user"]["name"] == "Test User"
            assert "admin" in meta["user"]["roles"]
            assert meta["request"]["ip"] == "192.168.1.100"
            assert meta["request"]["headers"]["user-agent"] == "TestAgent/1.0"
            assert meta["tracing"]["trace_id"] == "trace-xyz"
            assert meta["tracing"]["span_id"] == "span-abc"


class TestTransitCoverageGaps:
    """Tests for uncovered branches in Transit module."""

    @pytest.mark.asyncio
    async def test_beat_initializes_static_metrics(self, mock_dependencies, mock_transporter):
        """Test that beat() initializes static metrics on first call."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Setup local node with default cpuCoresLogical = 1 (triggers initialization)
            mock_local_node = MagicMock()
            mock_local_node.cpuCoresLogical = 1  # Default value triggers static metrics init
            mock_local_node.cpu = 0
            mock_local_node.cpuSeq = 0
            mock_local_node.memory = 0.0
            transit.node_catalog.local_node = mock_local_node

            # Mock _metrics_collector (MetricsCollector uses __slots__)
            mock_collector = MagicMock()
            mock_collector.collect = AsyncMock(
                return_value={
                    "cpu": 50,
                    "cpuSeq": 1,
                    "memory": 30.0,
                }
            )

            # Mock get_static_metrics to return test data
            with patch.object(transit, "_metrics_collector", mock_collector):
                with patch("moleculerpy.transit.get_static_metrics") as mock_static:
                    mock_static.return_value = {
                        "cpu_cores_logical": 8,
                        "cpu_cores_physical": 4,
                        "hostname": "test-host",
                        "ip_list": ["192.168.1.100"],
                    }

                    await transit.beat()

                    # Static metrics should be initialized
                    assert mock_local_node.cpuCoresLogical == 8
                    assert mock_local_node.cpuCoresPhysical == 4
                    assert mock_local_node.hostname == "test-host"
                    assert mock_local_node.ipList == ["192.168.1.100"]

    @pytest.mark.asyncio
    async def test_beat_skips_static_metrics_if_already_set(
        self, mock_dependencies, mock_transporter
    ):
        """Test that beat() doesn't reinitialize static metrics."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Setup local node with already initialized cpuCoresLogical
            mock_local_node = MagicMock()
            mock_local_node.cpuCoresLogical = 8  # Already initialized (> 1)
            mock_local_node.cpu = 0
            mock_local_node.cpuSeq = 0
            mock_local_node.memory = 0.0
            transit.node_catalog.local_node = mock_local_node

            # Mock _metrics_collector (MetricsCollector uses __slots__)
            mock_collector = MagicMock()
            mock_collector.collect = AsyncMock(
                return_value={
                    "cpu": 50,
                    "cpuSeq": 1,
                    "memory": 30.0,
                }
            )

            # Mock get_static_metrics - should NOT be called
            with patch.object(transit, "_metrics_collector", mock_collector):
                with patch("moleculerpy.transit.get_static_metrics") as mock_static:
                    await transit.beat()

                    # get_static_metrics should not be called (cpuCoresLogical > 1)
                    mock_static.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_heartbeat_ignores_packet_without_sender(
        self, mock_dependencies, mock_transporter
    ):
        """Test that _handle_heartbeat returns early if packet has no sender."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Packet without sender
            packet = Packet(Topic.HEARTBEAT, None, {"cpu": 50})
            packet.sender = None

            # Should return early without error
            await transit._handle_heartbeat(packet)

            # node_catalog.get_node should NOT be called
            transit.node_catalog.get_node.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_info_ignores_packet_without_sender(
        self, mock_dependencies, mock_transporter
    ):
        """Test that _handle_info returns early if packet has no sender."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Packet without sender
            packet = Packet(Topic.INFO, None, {"services": []})
            packet.sender = None

            # Should return early without error
            await transit._handle_info(packet)

            # node_catalog.process_node_info should NOT be called
            transit.node_catalog.process_node_info.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_info_ignores_packet_without_payload(
        self, mock_dependencies, mock_transporter
    ):
        """Test that _handle_info returns early if packet has no payload."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Packet without payload
            packet = Packet(Topic.INFO, None, None)
            packet.sender = "other-node"
            packet.payload = None

            # Should return early without error
            await transit._handle_info(packet)

            # node_catalog.process_node_info should NOT be called
            transit.node_catalog.process_node_info.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_ping_ignores_packet_without_sender(
        self, mock_dependencies, mock_transporter
    ):
        """Test that _handle_ping returns early if packet has no sender."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Add latency monitor mock
            mock_latency_monitor = MagicMock()
            mock_latency_monitor.handle_ping = AsyncMock()
            transit._latency_monitor = mock_latency_monitor

            # Packet without sender
            packet = Packet(Topic.PING, None, {"time": 1234567890})
            packet.sender = None

            # Should return early without error
            await transit._handle_ping(packet)

            # latency_monitor.handle_ping should NOT be called
            mock_latency_monitor.handle_ping.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_pong_ignores_packet_without_sender(
        self, mock_dependencies, mock_transporter
    ):
        """Test that _handle_pong returns early if packet has no sender."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Add latency monitor mock
            mock_latency_monitor = MagicMock()
            mock_latency_monitor.handle_pong = AsyncMock()
            transit._latency_monitor = mock_latency_monitor

            # Packet without sender
            packet = Packet(Topic.PONG, None, {"time": 1234567890, "arrived": 1234567891})
            packet.sender = None

            # Should return early without error
            await transit._handle_pong(packet)

            # latency_monitor.handle_pong should NOT be called
            mock_latency_monitor.handle_pong.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_pong_calls_latency_monitor(self, mock_dependencies, mock_transporter):
        """Test that _handle_pong calls latency_monitor.handle_pong when sender present."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Add latency monitor mock
            mock_latency_monitor = MagicMock()
            mock_latency_monitor.handle_pong = AsyncMock()
            transit._latency_monitor = mock_latency_monitor

            # Packet with sender
            payload = {"time": 1234567890, "arrived": 1234567891}
            packet = Packet(Topic.PONG, None, payload)
            packet.sender = "other-node"

            await transit._handle_pong(packet)

            # latency_monitor.handle_pong should be called
            mock_latency_monitor.handle_pong.assert_called_once_with("other-node", payload)

    @pytest.mark.asyncio
    async def test_handle_event_warns_on_missing_event_name(
        self, mock_dependencies, mock_transporter
    ):
        """Test that _handle_event logs warning if event name is missing."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Packet without event name in payload
            packet = Packet(Topic.EVENT, None, {"data": "some_data"})
            packet.sender = "other-node"

            # Should return early with warning (no exception)
            await transit._handle_event(packet)

            # Logger should have warning call
            transit.logger.warning.assert_called_once_with(
                "Received event packet without event name"
            )

    @pytest.mark.asyncio
    async def test_handle_event_ack_warns_on_missing_ack_id(
        self, mock_dependencies, mock_transporter
    ):
        """Test that _handle_event_ack logs warning if ackID is missing."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Packet without ackID in payload
            packet = Packet(Topic.EVENT_ACK, None, {"success": True})
            packet.sender = "other-node"

            # Should return early with warning (no exception)
            await transit._handle_event_ack(packet)

            # Logger should have warning call
            transit.logger.warning.assert_called_once_with("Received EVENT_ACK without ackID")

    @pytest.mark.asyncio
    async def test_handle_request_warns_on_missing_action_name(
        self, mock_dependencies, mock_transporter
    ):
        """Test that _handle_request logs warning if action name is missing."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Packet without action name in payload
            packet = Packet(Topic.REQUEST, None, {"params": {}})
            packet.sender = "other-node"

            # Should return early with warning (no exception)
            await transit._handle_request(packet)

            # Logger should have warning call
            transit.logger.warning.assert_called_once_with(
                "Received request packet without action name"
            )

    @pytest.mark.asyncio
    async def test_handle_request_warns_on_no_local_handler(
        self, mock_dependencies, mock_transporter
    ):
        """Test that _handle_request warns if action has no local handler."""
        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(**mock_dependencies)

            # Mock registry to return action that is not local
            mock_action = MagicMock()
            mock_action.is_local = False
            transit.registry.get_action.return_value = mock_action

            # Packet with action name
            packet = Packet(Topic.REQUEST, None, {"action": "remote.action", "params": {}})
            packet.sender = "other-node"

            await transit._handle_request(packet)

            # Logger should have warning call
            transit.logger.warning.assert_called_with("No local handler for action: remote.action")
