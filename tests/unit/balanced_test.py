"""Tests for PRD-009: Balanced Request/Event Handling."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from moleculerpy.errors import BrokerDisconnectedError, MoleculerError
from moleculerpy.packet import Packet, Topic
from moleculerpy.settings import Settings
from moleculerpy.transporter.base import Transporter

# =============================================================================
# TestBalancedSettings
# =============================================================================


class TestBalancedSettings:
    """Tests for disable_balancer setting."""

    def test_disable_balancer_default_false(self) -> None:
        """Default: disable_balancer is False."""
        settings = Settings()
        assert settings.disable_balancer is False

    def test_disable_balancer_true(self) -> None:
        """Can set disable_balancer=True."""
        settings = Settings(disable_balancer=True)
        assert settings.disable_balancer is True


# =============================================================================
# TestBalancedTransporterBase
# =============================================================================


class TestBalancedTransporterBase:
    """Tests for base transporter balanced methods."""

    def _make_concrete_transporter(self) -> Transporter:
        """Create a concrete transporter subclass for testing."""

        class ConcreteTransporter(Transporter):
            async def connect(self) -> None:
                pass

            async def disconnect(self) -> None:
                pass

            async def publish(self, packet: Packet) -> None:
                pass

            async def subscribe(self, command: str, topic: str | None = None) -> None:
                pass

            async def send(self, topic: str, data: bytes, meta: dict[str, Any]) -> None:
                pass

            async def receive(self, cmd: str, data: bytes, meta: dict[str, Any]) -> None:
                pass

            @classmethod
            def from_config(cls, config, transit, handler=None, node_id=None):  # type: ignore[override]
                return cls("test")

        return ConcreteTransporter("test")

    def test_has_built_in_balancer_default_false(self) -> None:
        """Base transporter: has_built_in_balancer = False."""
        transporter = self._make_concrete_transporter()
        assert transporter.has_built_in_balancer is False

    @pytest.mark.asyncio
    async def test_balanced_methods_are_noop(self) -> None:
        """Base balanced methods do nothing (no-op)."""
        transporter = self._make_concrete_transporter()
        packet = Packet(Topic.REQUEST, None, {"action": "test.hello"})

        # All should complete without error
        await transporter.subscribe_balanced_request("test.hello")
        await transporter.subscribe_balanced_event("user.created", "users")
        await transporter.publish_balanced_request(packet)
        await transporter.publish_balanced_event(packet, "users")
        await transporter.unsubscribe_from_balanced_commands()


# =============================================================================
# TestBalancedNatsTransporter
# =============================================================================


class TestBalancedNatsTransporter:
    """Tests for NATS transporter balanced methods."""

    def _make_nats_transporter(self) -> Any:
        """Create a NatsTransporter with mocked NATS connection."""
        from moleculerpy.transporter.nats import NatsTransporter

        transit = MagicMock()
        transit.serializer = MagicMock()
        transit.serializer.serialize_async = AsyncMock(return_value=b"serialized")
        t = NatsTransporter("nats://localhost:4222", transit, handler=AsyncMock(), node_id="node-1")
        t.nc = MagicMock()
        t.nc.subscribe = AsyncMock(return_value=MagicMock(unsubscribe=AsyncMock()))
        t.nc.publish = AsyncMock()
        return t

    def test_has_built_in_balancer_true(self) -> None:
        """NATS: has_built_in_balancer = True."""
        from moleculerpy.transporter.nats import NatsTransporter

        assert NatsTransporter.has_built_in_balancer is True

    @pytest.mark.asyncio
    async def test_subscribe_balanced_request(self) -> None:
        """NATS subscribes to MOL.REQB.<action> with queue group."""
        t = self._make_nats_transporter()
        await t.subscribe_balanced_request("math.add")
        t.nc.subscribe.assert_called_once_with(
            "MOL.REQB.math.add", queue="math.add", cb=t.message_handler
        )
        assert len(t._balanced_subscriptions) == 1

    @pytest.mark.asyncio
    async def test_subscribe_balanced_event(self) -> None:
        """NATS subscribes to MOL.EVENTB.<group>.<event> with queue group."""
        t = self._make_nats_transporter()
        await t.subscribe_balanced_event("user.created", "users")
        t.nc.subscribe.assert_called_once_with(
            "MOL.EVENTB.users.user.created", queue="users", cb=t.message_handler
        )
        assert len(t._balanced_subscriptions) == 1

    @pytest.mark.asyncio
    async def test_publish_balanced_request(self) -> None:
        """NATS publishes to MOL.REQB.<action>."""
        t = self._make_nats_transporter()
        packet = Packet(Topic.REQUEST, None, {"action": "math.add", "params": {"a": 1}})
        await t.publish_balanced_request(packet)
        # Verify send_with_middleware was called (which calls nc.publish internally)
        t.nc.publish.assert_called_once()
        args = t.nc.publish.call_args
        assert args[0][0] == "MOL.REQB.math.add"

    @pytest.mark.asyncio
    async def test_publish_balanced_event(self) -> None:
        """NATS publishes to MOL.EVENTB.<group>.<event>."""
        t = self._make_nats_transporter()
        packet = Packet(Topic.EVENT, None, {"event": "user.created"})
        await t.publish_balanced_event(packet, "users")
        t.nc.publish.assert_called_once()
        args = t.nc.publish.call_args
        assert args[0][0] == "MOL.EVENTB.users.user.created"

    @pytest.mark.asyncio
    async def test_unsubscribe_from_balanced_commands(self) -> None:
        """Unsubscribe clears all balanced subscriptions."""
        t = self._make_nats_transporter()
        await t.subscribe_balanced_request("math.add")
        await t.subscribe_balanced_event("user.created", "users")
        assert len(t._balanced_subscriptions) == 2

        await t.unsubscribe_from_balanced_commands()
        assert len(t._balanced_subscriptions) == 0

    @pytest.mark.asyncio
    async def test_subscribe_balanced_event_wildcard(self) -> None:
        """NATS replaces ** with > for wildcard events."""
        t = self._make_nats_transporter()
        await t.subscribe_balanced_event("user.**", "users")
        t.nc.subscribe.assert_called_once_with(
            "MOL.EVENTB.users.user.>", queue="users", cb=t.message_handler
        )

    @pytest.mark.asyncio
    async def test_subscribe_balanced_event_wildcard_with_trailing(self) -> None:
        """NATS wildcard: 'sensor.**.data' -> 'sensor.>' (removes everything after **)."""
        t = self._make_nats_transporter()
        await t.subscribe_balanced_event("sensor.**.data", "sensors")
        t.nc.subscribe.assert_called_once_with(
            "MOL.EVENTB.sensors.sensor.>", queue="sensors", cb=t.message_handler
        )


# =============================================================================
# TestPrepublish
# =============================================================================


class TestPrepublish:
    """Tests for transit.prepublish() routing logic."""

    def _make_transit(
        self, disable_balancer: bool = True, has_built_in_balancer: bool = True
    ) -> Any:
        """Create a Transit with mocked dependencies."""
        from moleculerpy.transit import Transit

        settings = MagicMock()
        settings.transporter = "nats://localhost:4222"
        settings.serializer = "JSON"
        settings.request_timeout = 30.0

        registry = MagicMock()
        node_catalog = MagicMock()
        logger = MagicMock()
        lifecycle = MagicMock()

        with patch("moleculerpy.transit.Transporter") as mock_transporter_cls:
            mock_transporter = MagicMock()
            mock_transporter.has_built_in_balancer = has_built_in_balancer
            mock_transporter.publish_balanced_request = AsyncMock()
            mock_transporter.publish_balanced_event = AsyncMock()
            mock_transporter_cls.get_by_name.return_value = mock_transporter

            transit = Transit(
                node_id="test-node",
                registry=registry,
                node_catalog=node_catalog,
                settings=settings,
                logger=logger,
                lifecycle=lifecycle,
            )

        transit.transporter = mock_transporter

        # Set up broker mock
        broker = MagicMock()
        broker.settings = MagicMock()
        broker.settings.disable_balancer = disable_balancer
        transit._broker = broker

        # Mock publish for normal path
        transit._wrapped_publish = AsyncMock()

        return transit

    @pytest.mark.asyncio
    async def test_prepublish_balanced_request(self) -> None:
        """target=None + REQUEST -> publish_balanced_request."""
        transit = self._make_transit()
        packet = Packet(Topic.REQUEST, None, {"action": "math.add"})

        await transit.prepublish(packet)

        transit.transporter.publish_balanced_request.assert_called_once_with(packet)
        transit._wrapped_publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_prepublish_balanced_event_with_groups(self) -> None:
        """target=None + EVENT + groups -> publish_balanced_event per group."""
        transit = self._make_transit()
        packet = Packet(
            Topic.EVENT, None, {"event": "user.created", "groups": ["users", "notifications"]}
        )

        await transit.prepublish(packet)

        assert transit.transporter.publish_balanced_event.call_count == 2
        calls = transit.transporter.publish_balanced_event.call_args_list
        # Each call gets a deepcopy with single group
        assert calls[0][0][1] == "users"
        assert calls[0][0][0].payload["groups"] == ["users"]
        assert calls[1][0][1] == "notifications"
        assert calls[1][0][0].payload["groups"] == ["notifications"]

    @pytest.mark.asyncio
    async def test_prepublish_normal_when_target_set(self) -> None:
        """target=nodeID -> normal publish (existing behavior)."""
        transit = self._make_transit()
        packet = Packet(Topic.REQUEST, "remote-node", {"action": "math.add"})

        await transit.prepublish(packet)

        transit.transporter.publish_balanced_request.assert_not_called()
        transit._wrapped_publish.assert_called_once_with(packet)

    @pytest.mark.asyncio
    async def test_prepublish_normal_when_balancer_not_disabled(self) -> None:
        """disable_balancer=False -> always normal publish."""
        transit = self._make_transit(disable_balancer=False)
        packet = Packet(Topic.REQUEST, None, {"action": "math.add"})

        await transit.prepublish(packet)

        transit.transporter.publish_balanced_request.assert_not_called()
        transit._wrapped_publish.assert_called_once_with(packet)

    @pytest.mark.asyncio
    async def test_prepublish_normal_when_no_built_in_balancer(self) -> None:
        """Transporter without built-in balancer -> normal publish."""
        transit = self._make_transit(has_built_in_balancer=False)
        packet = Packet(Topic.REQUEST, None, {"action": "math.add"})

        await transit.prepublish(packet)

        transit.transporter.publish_balanced_request.assert_not_called()
        transit._wrapped_publish.assert_called_once_with(packet)

    @pytest.mark.asyncio
    async def test_prepublish_event_without_groups_normal(self) -> None:
        """EVENT without groups -> normal publish even in balanced mode."""
        transit = self._make_transit()
        packet = Packet(Topic.EVENT, None, {"event": "user.created"})

        await transit.prepublish(packet)

        transit.transporter.publish_balanced_event.assert_not_called()
        transit._wrapped_publish.assert_called_once_with(packet)


# =============================================================================
# TestMakeBalancedSubscriptions
# =============================================================================


class TestMakeBalancedSubscriptions:
    """Tests for transit.make_balanced_subscriptions()."""

    @pytest.mark.asyncio
    async def test_subscribes_for_all_local_actions(self) -> None:
        """Creates balanced subscription for each local action."""
        from moleculerpy.transit import Transit

        settings = MagicMock()
        settings.transporter = "nats://localhost:4222"
        settings.serializer = "JSON"

        with patch("moleculerpy.transit.Transporter") as mock_cls:
            transporter = MagicMock()
            transporter.has_built_in_balancer = True
            transporter.unsubscribe_from_balanced_commands = AsyncMock()
            transporter.subscribe_balanced_request = AsyncMock()
            transporter.subscribe_balanced_event = AsyncMock()
            mock_cls.get_by_name.return_value = transporter

            transit = Transit(
                node_id="test-node",
                registry=MagicMock(),
                node_catalog=MagicMock(),
                settings=settings,
                logger=MagicMock(),
                lifecycle=MagicMock(),
            )

        transit.transporter = transporter

        # Create mock service with actions
        service = MagicMock()
        service.name = "math"
        action_handler = MagicMock()
        action_handler._is_action = True
        action_handler._name = "add"
        service.actions.return_value = ["add_method"]
        service.events.return_value = []
        service.add_method = action_handler

        broker = MagicMock()
        broker.registry.__services__ = {"math": service}
        transit._broker = broker

        await transit.make_balanced_subscriptions()

        transporter.unsubscribe_from_balanced_commands.assert_called_once()
        transporter.subscribe_balanced_request.assert_called_once_with("math.add")

    @pytest.mark.asyncio
    async def test_subscribes_for_all_local_events(self) -> None:
        """Creates balanced subscription for each local event with group."""
        from moleculerpy.transit import Transit

        settings = MagicMock()
        settings.transporter = "nats://localhost:4222"
        settings.serializer = "JSON"

        with patch("moleculerpy.transit.Transporter") as mock_cls:
            transporter = MagicMock()
            transporter.has_built_in_balancer = True
            transporter.unsubscribe_from_balanced_commands = AsyncMock()
            transporter.subscribe_balanced_request = AsyncMock()
            transporter.subscribe_balanced_event = AsyncMock()
            mock_cls.get_by_name.return_value = transporter

            transit = Transit(
                node_id="test-node",
                registry=MagicMock(),
                node_catalog=MagicMock(),
                settings=settings,
                logger=MagicMock(),
                lifecycle=MagicMock(),
            )

        transit.transporter = transporter

        # Create mock service with events
        service = MagicMock()
        service.name = "users"
        event_handler = MagicMock()
        event_handler._is_event = True
        event_handler._name = "user.created"
        event_handler._group = None  # Should default to service name
        service.actions.return_value = []
        service.events.return_value = ["on_user_created"]
        service.on_user_created = event_handler

        broker = MagicMock()
        broker.registry.__services__ = {"users": service}
        transit._broker = broker

        await transit.make_balanced_subscriptions()

        transporter.subscribe_balanced_event.assert_called_once_with("user.created", "users")

    @pytest.mark.asyncio
    async def test_skips_when_no_built_in_balancer(self) -> None:
        """No subscriptions when transporter lacks built-in balancer."""
        from moleculerpy.transit import Transit

        settings = MagicMock()
        settings.transporter = "nats://localhost:4222"
        settings.serializer = "JSON"

        with patch("moleculerpy.transit.Transporter") as mock_cls:
            transporter = MagicMock()
            transporter.has_built_in_balancer = False
            transporter.unsubscribe_from_balanced_commands = AsyncMock()
            mock_cls.get_by_name.return_value = transporter

            transit = Transit(
                node_id="test-node",
                registry=MagicMock(),
                node_catalog=MagicMock(),
                settings=settings,
                logger=MagicMock(),
                lifecycle=MagicMock(),
            )

        transit.transporter = transporter
        transit._broker = MagicMock()

        await transit.make_balanced_subscriptions()

        transporter.unsubscribe_from_balanced_commands.assert_not_called()

    @pytest.mark.asyncio
    async def test_event_with_custom_group(self) -> None:
        """Event with explicit group uses that group, not service name."""
        from moleculerpy.transit import Transit

        settings = MagicMock()
        settings.transporter = "nats://localhost:4222"
        settings.serializer = "JSON"

        with patch("moleculerpy.transit.Transporter") as mock_cls:
            transporter = MagicMock()
            transporter.has_built_in_balancer = True
            transporter.unsubscribe_from_balanced_commands = AsyncMock()
            transporter.subscribe_balanced_request = AsyncMock()
            transporter.subscribe_balanced_event = AsyncMock()
            mock_cls.get_by_name.return_value = transporter

            transit = Transit(
                node_id="test-node",
                registry=MagicMock(),
                node_catalog=MagicMock(),
                settings=settings,
                logger=MagicMock(),
                lifecycle=MagicMock(),
            )

        transit.transporter = transporter

        service = MagicMock()
        service.name = "payment"
        event_handler = MagicMock()
        event_handler._name = "order.paid"
        event_handler._group = "billing"
        service.actions.return_value = []
        service.events.return_value = ["on_order_paid"]
        service.on_order_paid = event_handler

        broker = MagicMock()
        broker.registry.__services__ = {"payment": service}
        transit._broker = broker

        await transit.make_balanced_subscriptions()

        transporter.subscribe_balanced_event.assert_called_once_with("order.paid", "billing")


# =============================================================================
# TestBackwardsCompatibility
# =============================================================================


class TestBackwardsCompatibility:
    """Tests ensuring backwards compatibility when balanced mode is off."""

    @pytest.mark.asyncio
    async def test_default_settings_no_balanced(self) -> None:
        """With default settings, no balanced behavior activated."""
        settings = Settings()
        assert settings.disable_balancer is False
        # Default settings should not enable balanced mode

    @pytest.mark.asyncio
    async def test_prepublish_falls_through_with_defaults(self) -> None:
        """prepublish with default settings goes to normal publish."""
        from moleculerpy.transit import Transit

        settings = MagicMock()
        settings.transporter = "nats://localhost:4222"
        settings.serializer = "JSON"

        with patch("moleculerpy.transit.Transporter") as mock_cls:
            transporter = MagicMock()
            transporter.has_built_in_balancer = True
            mock_cls.get_by_name.return_value = transporter

            transit = Transit(
                node_id="test-node",
                registry=MagicMock(),
                node_catalog=MagicMock(),
                settings=settings,
                logger=MagicMock(),
                lifecycle=MagicMock(),
            )

        # Broker with disable_balancer=False (default)
        broker = MagicMock()
        broker.settings.disable_balancer = False
        transit._broker = broker
        transit._wrapped_publish = AsyncMock()

        packet = Packet(Topic.REQUEST, None, {"action": "test.hello"})
        await transit.prepublish(packet)

        # Should use normal publish
        transit._wrapped_publish.assert_called_once_with(packet)
        transporter.publish_balanced_request.assert_not_called()


# =============================================================================
# TestRequestWithoutEndpoint
# =============================================================================


class TestRequestWithoutEndpoint:
    """Tests for transit.request_without_endpoint()."""

    @pytest.mark.asyncio
    async def test_request_without_endpoint_success(self) -> None:
        """Balanced request receives response and returns data."""
        from moleculerpy.transit import Transit

        settings = MagicMock()
        settings.transporter = "nats://localhost:4222"
        settings.serializer = "JSON"
        settings.request_timeout = 5.0

        with patch("moleculerpy.transit.Transporter") as mock_cls:
            transporter = MagicMock()
            transporter.has_built_in_balancer = True
            transporter.publish_balanced_request = AsyncMock()
            mock_cls.get_by_name.return_value = transporter

            transit = Transit(
                node_id="test-node",
                registry=MagicMock(),
                node_catalog=MagicMock(),
                settings=settings,
                logger=MagicMock(),
                lifecycle=MagicMock(),
            )

        transit.transporter = transporter
        broker = MagicMock()
        broker.settings.disable_balancer = True
        transit._broker = broker

        # Create mock context
        context = MagicMock()
        context.id = "ctx-123"
        context.marshall.return_value = {"id": "ctx-123", "params": {"a": 1}}

        # Simulate response arriving asynchronously
        async def simulate_response() -> None:
            await asyncio.sleep(0.01)
            future = transit._pending_requests.get("ctx-123")
            if future and not future.done():
                future.set_result({"success": True, "data": 42})

        task = asyncio.create_task(simulate_response())

        result = await transit.request_without_endpoint("math.add", context)
        assert result == 42
        await task

    @pytest.mark.asyncio
    async def test_request_without_endpoint_timeout(self) -> None:
        """Balanced request raises TimeoutError on timeout."""
        from moleculerpy.transit import Transit

        settings = MagicMock()
        settings.transporter = "nats://localhost:4222"
        settings.serializer = "JSON"
        settings.request_timeout = 0.05

        with patch("moleculerpy.transit.Transporter") as mock_cls:
            transporter = MagicMock()
            transporter.has_built_in_balancer = True
            transporter.publish_balanced_request = AsyncMock()
            mock_cls.get_by_name.return_value = transporter

            transit = Transit(
                node_id="test-node",
                registry=MagicMock(),
                node_catalog=MagicMock(),
                settings=settings,
                logger=MagicMock(),
                lifecycle=MagicMock(),
            )

        transit.transporter = transporter
        broker = MagicMock()
        broker.settings.disable_balancer = True
        transit._broker = broker

        context = MagicMock()
        context.id = "ctx-timeout"
        context.marshall.return_value = {"id": "ctx-timeout"}

        with pytest.raises(TimeoutError, match="timed out"):
            await transit.request_without_endpoint("math.add", context, timeout=0.05)


# =============================================================================
# TestCallWithoutBalancer
# =============================================================================


class TestCallWithoutBalancer:
    """Tests for broker.call_without_balancer()."""

    @pytest.mark.asyncio
    async def test_call_without_balancer_success(self) -> None:
        """call_without_balancer delegates to transit.request_without_endpoint."""
        from moleculerpy.broker import ServiceBroker

        broker = ServiceBroker(id="test-node")

        # Mock registry and transit
        mock_action = MagicMock()
        broker.registry = MagicMock()
        broker.registry.get_action.return_value = mock_action

        broker.transit = MagicMock()
        broker.transit.request_without_endpoint = AsyncMock(return_value=42)

        context = MagicMock()
        result = await broker.call_without_balancer("math.add", context)

        assert result == 42
        broker.transit.request_without_endpoint.assert_called_once_with("math.add", context, None)

    @pytest.mark.asyncio
    async def test_call_without_balancer_action_not_found(self) -> None:
        """call_without_balancer raises ServiceNotFoundError when action unknown."""
        from moleculerpy.broker import ServiceBroker
        from moleculerpy.errors import ServiceNotFoundError

        broker = ServiceBroker(id="test-node")
        broker.registry = MagicMock()
        broker.registry.get_action.return_value = None

        context = MagicMock()
        with pytest.raises(ServiceNotFoundError):
            await broker.call_without_balancer("unknown.action", context)


# =============================================================================
# TestNatsBalancedEdgeCases
# =============================================================================


class TestNatsBalancedEdgeCases:
    """Edge-case tests for NATS balanced methods."""

    @pytest.mark.asyncio
    async def test_nats_balanced_methods_raise_when_not_connected(self) -> None:
        """NATS balanced subscribe raises RuntimeError when nc=None."""
        from moleculerpy.transporter.nats import NatsTransporter

        transit = MagicMock()
        t = NatsTransporter("nats://localhost:4222", transit, handler=AsyncMock(), node_id="node-1")
        t.nc = None

        with pytest.raises(RuntimeError, match="Not connected"):
            await t.subscribe_balanced_request("math.add")

    @pytest.mark.asyncio
    async def test_subscribe_balanced_event_raises_when_not_connected(self) -> None:
        """NATS subscribe_balanced_event raises RuntimeError when nc=None."""
        from moleculerpy.transporter.nats import NatsTransporter

        transit = MagicMock()
        t = NatsTransporter("nats://localhost:4222", transit, handler=AsyncMock(), node_id="node-1")
        t.nc = None

        with pytest.raises(RuntimeError, match="Not connected"):
            await t.subscribe_balanced_event("user.created", "users")

    @pytest.mark.asyncio
    async def test_publish_balanced_request_raises_when_not_connected(self) -> None:
        """NATS publish_balanced_request raises RuntimeError when nc=None."""
        from moleculerpy.transporter.nats import NatsTransporter

        transit = MagicMock()
        t = NatsTransporter("nats://localhost:4222", transit, handler=AsyncMock(), node_id="node-1")
        t.nc = None

        packet = Packet(Topic.REQUEST, None, {"action": "math.add"})
        with pytest.raises(RuntimeError, match="Not connected"):
            await t.publish_balanced_request(packet)

    @pytest.mark.asyncio
    async def test_publish_balanced_event_raises_when_not_connected(self) -> None:
        """NATS publish_balanced_event raises RuntimeError when nc=None."""
        from moleculerpy.transporter.nats import NatsTransporter

        transit = MagicMock()
        t = NatsTransporter("nats://localhost:4222", transit, handler=AsyncMock(), node_id="node-1")
        t.nc = None

        packet = Packet(Topic.EVENT, None, {"event": "user.created"})
        with pytest.raises(RuntimeError, match="Not connected"):
            await t.publish_balanced_event(packet, "users")

    @pytest.mark.asyncio
    async def test_publish_balanced_request_empty_action_warns(self) -> None:
        """NATS publish_balanced_request with empty action logs warning and returns."""
        from moleculerpy.transporter.nats import NatsTransporter

        transit = MagicMock()
        transit.serializer = MagicMock()
        transit.serializer.serialize_async = AsyncMock(return_value=b"serialized")
        t = NatsTransporter("nats://localhost:4222", transit, handler=AsyncMock(), node_id="node-1")
        t.nc = MagicMock()
        t.nc.publish = AsyncMock()

        packet = Packet(Topic.REQUEST, None, {"params": {"a": 1}})  # no "action" key
        with patch("moleculerpy.transporter.nats.logger") as mock_logger:
            await t.publish_balanced_request(packet)
            mock_logger.warning.assert_called_once()
            assert "missing action" in mock_logger.warning.call_args[0][0].lower()

        # nc.publish should NOT have been called
        t.nc.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_balanced_event_empty_event_warns(self) -> None:
        """NATS publish_balanced_event with empty event logs warning and returns."""
        from moleculerpy.transporter.nats import NatsTransporter

        transit = MagicMock()
        transit.serializer = MagicMock()
        transit.serializer.serialize_async = AsyncMock(return_value=b"serialized")
        t = NatsTransporter("nats://localhost:4222", transit, handler=AsyncMock(), node_id="node-1")
        t.nc = MagicMock()
        t.nc.publish = AsyncMock()

        packet = Packet(Topic.EVENT, None, {"params": {}})  # no "event" key
        with patch("moleculerpy.transporter.nats.logger") as mock_logger:
            await t.publish_balanced_event(packet, "users")
            mock_logger.warning.assert_called_once()
            assert "missing event" in mock_logger.warning.call_args[0][0].lower()

        t.nc.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_unsubscribe_from_balanced_commands_drains(self) -> None:
        """Unsubscribe actually calls unsubscribe on each subscription object."""
        from moleculerpy.transporter.nats import NatsTransporter

        transit = MagicMock()
        t = NatsTransporter("nats://localhost:4222", transit, handler=AsyncMock(), node_id="node-1")

        sub1 = MagicMock()
        sub1.unsubscribe = AsyncMock()
        sub2 = MagicMock()
        sub2.unsubscribe = AsyncMock()
        t._balanced_subscriptions = [sub1, sub2]

        await t.unsubscribe_from_balanced_commands()

        sub1.unsubscribe.assert_called_once()
        sub2.unsubscribe.assert_called_once()
        assert len(t._balanced_subscriptions) == 0


# =============================================================================
# TestMakeBalancedSubscriptionsNoBroker
# =============================================================================


class TestMakeBalancedSubscriptionsNoBroker:
    """Test make_balanced_subscriptions when broker is None."""

    @pytest.mark.asyncio
    async def test_make_balanced_subscriptions_no_broker(self) -> None:
        """make_balanced_subscriptions returns early when _broker is None."""
        from moleculerpy.transit import Transit

        settings = MagicMock()
        settings.transporter = "nats://localhost:4222"
        settings.serializer = "JSON"

        with patch("moleculerpy.transit.Transporter") as mock_cls:
            transporter = MagicMock()
            transporter.has_built_in_balancer = True
            transporter.unsubscribe_from_balanced_commands = AsyncMock()
            mock_cls.get_by_name.return_value = transporter

            transit = Transit(
                node_id="test-node",
                registry=MagicMock(),
                node_catalog=MagicMock(),
                settings=settings,
                logger=MagicMock(),
                lifecycle=MagicMock(),
            )

        transit.transporter = transporter
        transit._broker = None

        # Should return early without error (after unsubscribe but before iterating services)
        await transit.make_balanced_subscriptions()

        # unsubscribe is called, but no subscriptions are made
        transporter.unsubscribe_from_balanced_commands.assert_called_once()
        transporter.subscribe_balanced_request.assert_not_called()
        transporter.subscribe_balanced_event.assert_not_called()


# =============================================================================
# TestRequestWithoutEndpointError
# =============================================================================


class TestRequestWithoutEndpointError:
    """Test request_without_endpoint with error response."""

    @pytest.mark.asyncio
    async def test_request_without_endpoint_error_response(self) -> None:
        """Balanced request raises RemoteCallError on error response."""
        from moleculerpy.errors import RemoteCallError
        from moleculerpy.transit import Transit

        settings = MagicMock()
        settings.transporter = "nats://localhost:4222"
        settings.serializer = "JSON"
        settings.request_timeout = 5.0

        with patch("moleculerpy.transit.Transporter") as mock_cls:
            transporter = MagicMock()
            transporter.has_built_in_balancer = True
            transporter.publish_balanced_request = AsyncMock()
            mock_cls.get_by_name.return_value = transporter

            transit = Transit(
                node_id="test-node",
                registry=MagicMock(),
                node_catalog=MagicMock(),
                settings=settings,
                logger=MagicMock(),
                lifecycle=MagicMock(),
            )

        transit.transporter = transporter
        broker = MagicMock()
        broker.settings.disable_balancer = True
        transit._broker = broker

        context = MagicMock()
        context.id = "ctx-error"
        context.marshall.return_value = {"id": "ctx-error", "params": {}}

        # Simulate error response arriving asynchronously
        async def simulate_error_response() -> None:
            await asyncio.sleep(0.01)
            future = transit._pending_requests.get("ctx-error")
            if future and not future.done():
                future.set_result(
                    {
                        "success": False,
                        "error": {
                            "message": "Service math.add is not available",
                            "name": "ServiceNotFoundError",
                            "code": 404,
                            "type": "SERVICE_NOT_FOUND",
                            "retryable": False,
                        },
                    }
                )

        task = asyncio.create_task(simulate_error_response())

        with pytest.raises(RemoteCallError, match=r"Service math\.add is not available"):
            await transit.request_without_endpoint("math.add", context)

        await task


# =============================================================================
# TestPrepublishDisconnected
# =============================================================================


class TestPrepublishDisconnected:
    """Tests for prepublish disconnected check."""

    def _make_transit(self) -> Any:
        """Create a Transit with mocked dependencies (disconnected state)."""
        from moleculerpy.transit import Transit

        settings = MagicMock()
        settings.transporter = "nats://localhost:4222"
        settings.serializer = "JSON"

        with patch("moleculerpy.transit.Transporter") as mock_cls:
            transporter = MagicMock()
            transporter.has_built_in_balancer = True
            # Simulate disconnected: nc=None and no 'connected' attr
            transporter.nc = None
            del transporter.connected  # ensure getattr falls through
            mock_cls.get_by_name.return_value = transporter

            transit = Transit(
                node_id="test-node",
                registry=MagicMock(),
                node_catalog=MagicMock(),
                settings=settings,
                logger=MagicMock(),
                lifecycle=MagicMock(),
            )

        transit.transporter = transporter
        transit._was_connected = False

        broker = MagicMock()
        broker.settings.disable_balancer = True
        transit._broker = broker
        transit._wrapped_publish = AsyncMock()

        return transit

    @pytest.mark.asyncio
    async def test_prepublish_raises_on_disconnected_request(self) -> None:
        """prepublish raises BrokerDisconnectedError for REQUEST when disconnected."""
        transit = self._make_transit()
        packet = Packet(Topic.REQUEST, None, {"action": "math.add"})

        with pytest.raises(BrokerDisconnectedError):
            await transit.prepublish(packet)

    @pytest.mark.asyncio
    async def test_prepublish_raises_on_disconnected_event(self) -> None:
        """prepublish raises BrokerDisconnectedError for EVENT when disconnected."""
        transit = self._make_transit()
        packet = Packet(Topic.EVENT, None, {"event": "user.created", "groups": ["users"]})

        with pytest.raises(BrokerDisconnectedError):
            await transit.prepublish(packet)

    @pytest.mark.asyncio
    async def test_prepublish_silently_skips_internal_when_disconnected(self) -> None:
        """prepublish silently skips INFO/HEARTBEAT packets when disconnected."""
        transit = self._make_transit()

        # INFO packet should not raise, just return silently
        packet = Packet(Topic.INFO, None, {})
        await transit.prepublish(packet)  # should not raise

        transit._wrapped_publish.assert_not_called()
        transit.transporter.publish_balanced_request.assert_not_called()


# =============================================================================
# TestSendEventBalanced
# =============================================================================


class TestSendEventBalanced:
    """Tests for send_event routing through prepublish when balanced."""

    def _make_transit(
        self, disable_balancer: bool = True, has_built_in_balancer: bool = True
    ) -> Any:
        """Create a Transit with mocked dependencies."""
        from moleculerpy.transit import Transit

        settings = MagicMock()
        settings.transporter = "nats://localhost:4222"
        settings.serializer = "JSON"

        with patch("moleculerpy.transit.Transporter") as mock_cls:
            transporter = MagicMock()
            transporter.has_built_in_balancer = has_built_in_balancer
            transporter.publish_balanced_event = AsyncMock()
            transporter.nc = MagicMock()  # Connected
            mock_cls.get_by_name.return_value = transporter

            transit = Transit(
                node_id="test-node",
                registry=MagicMock(),
                node_catalog=MagicMock(),
                settings=settings,
                logger=MagicMock(),
                lifecycle=MagicMock(),
            )

        transit.transporter = transporter

        broker = MagicMock()
        broker.settings.disable_balancer = disable_balancer
        transit._broker = broker
        transit._wrapped_publish = AsyncMock()
        transit._was_connected = True

        return transit

    @pytest.mark.asyncio
    async def test_send_event_balanced_with_groups(self) -> None:
        """send_event routes through prepublish when balanced + groups."""
        transit = self._make_transit()
        endpoint = MagicMock()
        endpoint.node_id = "remote-node"
        context = MagicMock()
        context.marshall.return_value = {"event": "user.created", "params": {}}

        await transit.send_event(endpoint, context, groups=["users", "notifications"])

        # Should route through balanced event publish, not normal publish
        assert transit.transporter.publish_balanced_event.call_count == 2
        transit._wrapped_publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_event_normal_without_groups(self) -> None:
        """send_event uses normal publish when no groups provided."""
        transit = self._make_transit()
        endpoint = MagicMock()
        endpoint.node_id = "remote-node"
        context = MagicMock()
        context.marshall.return_value = {"event": "user.created", "params": {}}

        await transit.send_event(endpoint, context)

        # Should use normal publish path (via prepublish -> publish)
        transit.transporter.publish_balanced_event.assert_not_called()
        transit._wrapped_publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_event_normal_when_balancer_not_disabled(self) -> None:
        """send_event uses normal publish when disable_balancer=False, even with groups."""
        transit = self._make_transit(disable_balancer=False)
        endpoint = MagicMock()
        endpoint.node_id = "remote-node"
        context = MagicMock()
        context.marshall.return_value = {"event": "user.created", "params": {}}

        await transit.send_event(endpoint, context, groups=["users"])

        # Should use normal publish (balanced not active)
        transit.transporter.publish_balanced_event.assert_not_called()
        transit._wrapped_publish.assert_called_once()


# =============================================================================
# TestCallWithoutBalancerNoTransit
# =============================================================================


class TestCallWithoutBalancerNoTransit:
    """Tests for call_without_balancer when transit is None."""

    @pytest.mark.asyncio
    async def test_call_without_balancer_no_transit(self) -> None:
        """call_without_balancer raises MoleculerError when transit is None."""
        from moleculerpy.broker import ServiceBroker

        broker = ServiceBroker(id="test-node")
        broker.transit = None  # type: ignore[assignment]

        context = MagicMock()
        with pytest.raises(MoleculerError, match="Transit not available"):
            await broker.call_without_balancer("math.add", context)


# =============================================================================
# TestIsTransporterConnectedFallbacks
# =============================================================================


class TestIsTransporterConnectedFallbacks:
    """Tests for transit._is_transporter_connected() fallback paths."""

    def _make_transit(self) -> Any:
        """Create a Transit with minimal mocks."""
        from moleculerpy.transit import Transit

        settings = MagicMock()
        settings.transporter = "nats://localhost:4222"
        settings.serializer = "JSON"

        with patch("moleculerpy.transit.Transporter") as mock_cls:
            transporter = MagicMock()
            mock_cls.get_by_name.return_value = transporter

            transit = Transit(
                node_id="test-node",
                registry=MagicMock(),
                node_catalog=MagicMock(),
                settings=settings,
                logger=MagicMock(),
                lifecycle=MagicMock(),
            )

        return transit

    def test_connected_attr_true(self) -> None:
        """_is_transporter_connected returns True when transporter.connected=True."""
        transit = self._make_transit()
        transit.transporter.connected = True
        assert transit._is_transporter_connected() is True

    def test_connected_attr_false(self) -> None:
        """_is_transporter_connected returns False when transporter.connected=False."""
        transit = self._make_transit()
        transit.transporter.connected = False
        assert transit._is_transporter_connected() is False

    def test_no_connected_attr_nc_present(self) -> None:
        """_is_transporter_connected returns True when nc is not None (no connected attr)."""
        transit = self._make_transit()
        # Remove 'connected' attr so getattr falls through
        del transit.transporter.connected
        transit.transporter.nc = MagicMock()
        assert transit._is_transporter_connected() is True

    def test_no_connected_no_nc_fallback_was_connected_true(self) -> None:
        """_is_transporter_connected falls back to _was_connected=True."""
        transit = self._make_transit()
        del transit.transporter.connected
        transit.transporter.nc = None
        transit._was_connected = True
        assert transit._is_transporter_connected() is True

    def test_no_connected_no_nc_fallback_was_connected_false(self) -> None:
        """_is_transporter_connected falls back to _was_connected=False."""
        transit = self._make_transit()
        del transit.transporter.connected
        transit.transporter.nc = None
        transit._was_connected = False
        assert transit._is_transporter_connected() is False

    def test_no_connected_no_nc_attr_at_all(self) -> None:
        """_is_transporter_connected falls back to _was_connected when nc attr missing."""
        transit = self._make_transit()
        del transit.transporter.connected
        del transit.transporter.nc
        transit._was_connected = False
        assert transit._is_transporter_connected() is False


# =============================================================================
# TestCallRoutingToCallWithoutBalancer
# =============================================================================


class TestCallRoutingToCallWithoutBalancer:
    """Tests for broker.call() routing to call_without_balancer for remote endpoints."""

    @pytest.mark.asyncio
    async def test_call_routes_to_call_without_balancer_for_remote(self) -> None:
        """broker.call() delegates to call_without_balancer when disable_balancer + remote."""
        from moleculerpy.broker import ServiceBroker

        broker = ServiceBroker(id="test-node")
        broker.settings.disable_balancer = True

        # Mock registry endpoint
        endpoint = MagicMock()
        endpoint.is_local = False
        broker.registry = MagicMock()
        broker.registry.get_action_endpoint.return_value = endpoint
        broker.registry.get_action.return_value = MagicMock()

        # Mock transit with built-in balancer
        broker.transit = MagicMock()
        broker.transit.transporter = MagicMock()
        broker.transit.transporter.has_built_in_balancer = True
        broker.transit.request_without_endpoint = AsyncMock(return_value=99)

        result = await broker.call("math.add", {"a": 1, "b": 2})

        assert result == 99
        broker.transit.request_without_endpoint.assert_called_once()


# =============================================================================
# TestVersionFallback
# =============================================================================


class TestVersionFallback:
    """Test that __version__ is accessible."""

    def test_version_exists(self) -> None:
        """moleculerpy.__version__ is set (either from metadata or fallback)."""
        import moleculerpy

        assert hasattr(moleculerpy, "__version__")
        assert isinstance(moleculerpy.__version__, str)
        assert len(moleculerpy.__version__) > 0
