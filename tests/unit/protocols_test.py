"""Unit tests for the Protocols module.

Tests for Protocol classes that define structural typing interfaces
for the MoleculerPy framework. These tests verify that:
1. @runtime_checkable protocols work with isinstance()
2. Actual implementations satisfy their protocols
3. Protocol contracts are correctly defined
"""

from unittest.mock import Mock

import pytest

from moleculerpy.protocols import (
    ActionProtocol,
    BrokerProtocol,
    CallOptionsProtocol,
    ContextProtocol,
    EventProtocol,
    MiddlewareProtocol,
    NodeProtocol,
    RegistryProtocol,
    ServiceProtocol,
    SettingsProtocol,
)


class TestSettingsProtocol:
    """Test SettingsProtocol runtime checkable behavior."""

    def test_settings_protocol_isinstance(self):
        """Test that objects with request_timeout satisfy SettingsProtocol."""
        # Create object satisfying protocol
        settings = Mock()
        settings.request_timeout = 30.0

        assert isinstance(settings, SettingsProtocol)

    def test_settings_protocol_missing_attribute(self):
        """Test that objects without request_timeout don't satisfy protocol."""
        # Object without request_timeout
        settings = Mock(spec=[])

        assert not isinstance(settings, SettingsProtocol)


class TestBrokerProtocol:
    """Test BrokerProtocol runtime checkable behavior."""

    def test_broker_protocol_isinstance(self):
        """Test that objects with settings and node_id satisfy BrokerProtocol."""
        broker = Mock()
        broker.settings = Mock()
        broker.settings.request_timeout = 30.0
        broker.node_id = "node-123"

        assert isinstance(broker, BrokerProtocol)

    def test_broker_protocol_missing_node_id(self):
        """Test that objects without node_id don't satisfy protocol."""
        broker = Mock(spec=["settings"])
        broker.settings = Mock()

        assert not isinstance(broker, BrokerProtocol)


class TestContextProtocol:
    """Test ContextProtocol runtime checkable behavior."""

    def test_context_protocol_mock_isinstance(self):
        """Test that mock objects with required attrs satisfy ContextProtocol."""
        # Create mock satisfying protocol
        ctx = Mock()
        ctx.meta = {"timeout": 10}
        ctx.broker = None
        ctx.params = {"key": "value"}
        ctx.options = None

        assert isinstance(ctx, ContextProtocol)

    def test_context_class_has_required_attributes(self):
        """Test that Context class has attributes used by protocol."""
        from moleculerpy.context import Context

        ctx = Context(id="ctx-123", params={"key": "value"}, meta={"timeout": 10})

        # Check attributes exist (Context has different property names)
        assert hasattr(ctx, "meta")
        assert hasattr(ctx, "broker")
        assert hasattr(ctx, "params")

        # Check meta is dict
        assert isinstance(ctx.meta, dict)
        assert ctx.params == {"key": "value"}


class TestCallOptionsProtocol:
    """Test CallOptionsProtocol runtime checkable behavior."""

    def test_call_options_protocol_isinstance(self):
        """Test that objects with timeout, retries, node_id satisfy protocol."""
        options = Mock()
        options.timeout = 5.0
        options.retries = 3
        options.node_id = None

        assert isinstance(options, CallOptionsProtocol)


class TestActionProtocol:
    """Test ActionProtocol runtime checkable behavior."""

    def test_action_class_satisfies_protocol(self):
        """Test that Action class satisfies ActionProtocol."""
        from moleculerpy.registry import Action

        action = Action(
            name="users.create",
            node_id="node-123",
            is_local=True,
            handler=lambda ctx: None,
            timeout=30.0,
        )

        assert isinstance(action, ActionProtocol)

    def test_action_protocol_required_attributes(self):
        """Test ActionProtocol required attributes."""
        from moleculerpy.registry import Action

        action = Action(
            name="users.create",
            node_id="node-123",
            is_local=True,
        )

        assert action.name == "users.create"
        assert action.node_id == "node-123"
        assert action.is_local is True
        assert action.handler is None
        assert action.timeout is None
        assert action.retries is None


class TestEventProtocol:
    """Test EventProtocol runtime checkable behavior."""

    def test_event_class_satisfies_protocol(self):
        """Test that Event class satisfies EventProtocol."""
        from moleculerpy.registry import Event

        event = Event(
            name="user.created",
            node_id="node-123",
            is_local=True,
            handler=lambda ctx: None,
        )

        assert isinstance(event, EventProtocol)

    def test_event_protocol_required_attributes(self):
        """Test EventProtocol required attributes."""
        from moleculerpy.registry import Event

        event = Event(name="user.created", node_id="node-123")

        assert event.name == "user.created"
        assert event.node_id == "node-123"
        assert event.is_local is False  # default
        assert event.handler is None


class TestServiceProtocol:
    """Test ServiceProtocol runtime checkable behavior."""

    def test_service_protocol_isinstance(self):
        """Test that objects with name, version, node_id satisfy protocol."""
        service = Mock()
        service.name = "users"
        service.version = "1.0.0"
        service.node_id = "node-123"

        assert isinstance(service, ServiceProtocol)


class TestNodeProtocol:
    """Test NodeProtocol runtime checkable behavior."""

    def test_node_class_satisfies_protocol(self):
        """Test that Node class satisfies NodeProtocol."""
        from moleculerpy.node import Node

        node = Node(
            node_id="node-123",
            available=True,
            cpu=50,
        )
        node.latency = 10.5  # Set latency

        assert isinstance(node, NodeProtocol)

    def test_node_protocol_required_attributes(self):
        """Test NodeProtocol required attributes."""
        from moleculerpy.node import Node

        node = Node(node_id="node-123")

        assert node.id == "node-123"
        assert node.available is True  # default
        assert node.cpu == 0  # default
        assert hasattr(node, "lastHeartbeatTime")


class TestRegistryProtocol:
    """Test RegistryProtocol runtime checkable behavior."""

    def test_registry_class_satisfies_protocol(self):
        """Test that Registry class satisfies RegistryProtocol."""
        from moleculerpy.registry import Registry

        registry = Registry(node_id="node-123")

        assert isinstance(registry, RegistryProtocol)

    def test_registry_protocol_methods_exist(self):
        """Test that Registry has all protocol-required methods."""
        from moleculerpy.registry import Registry

        registry = Registry()

        # Check required methods exist
        assert callable(registry.get_action)
        assert callable(registry.get_all_actions)
        assert callable(registry.get_all_events)
        assert callable(registry.remove_actions_by_node)
        assert callable(registry.remove_events_by_node)

    def test_registry_protocol_method_signatures(self):
        """Test that Registry methods have correct return types."""
        from moleculerpy.registry import Registry

        registry = Registry()

        # get_action returns Action | None
        result = registry.get_action("nonexistent")
        assert result is None

        # get_all_actions returns list[Action]
        result = registry.get_all_actions("nonexistent")
        assert isinstance(result, list)

        # get_all_events returns list[Event]
        result = registry.get_all_events("nonexistent")
        assert isinstance(result, list)

        # remove_actions_by_node returns int
        result = registry.remove_actions_by_node("node-1")
        assert isinstance(result, int)

        # remove_events_by_node returns int
        result = registry.remove_events_by_node("node-1")
        assert isinstance(result, int)


class TestMiddlewareProtocol:
    """Test MiddlewareProtocol structure."""

    def test_middleware_protocol_defines_hooks(self):
        """Test that MiddlewareProtocol defines expected hook methods."""
        # MiddlewareProtocol should define local_action, remote_action, local_event
        # Check that they exist in the protocol
        assert hasattr(MiddlewareProtocol, "local_action")
        assert hasattr(MiddlewareProtocol, "remote_action")
        assert hasattr(MiddlewareProtocol, "local_event")


class TestProtocolsImportability:
    """Test that all protocols can be imported correctly."""

    def test_all_protocols_importable(self):
        """Test that __all__ exports are importable."""
        from moleculerpy import protocols

        expected_exports = [
            "SettingsProtocol",
            "BrokerProtocol",
            "ContextProtocol",
            "CallOptionsProtocol",
            "ActionProtocol",
            "EventProtocol",
            "ServiceProtocol",
            "NodeProtocol",
            "MiddlewareProtocol",
            "RegistryProtocol",
        ]

        for name in expected_exports:
            assert hasattr(protocols, name), f"Missing export: {name}"
            assert name in protocols.__all__, f"Not in __all__: {name}"

    def test_protocols_are_runtime_checkable(self):
        """Test that all protocols are @runtime_checkable."""
        from typing import Protocol

        protocols_to_check = [
            SettingsProtocol,
            BrokerProtocol,
            ContextProtocol,
            CallOptionsProtocol,
            ActionProtocol,
            EventProtocol,
            ServiceProtocol,
            NodeProtocol,
            MiddlewareProtocol,
            RegistryProtocol,
        ]

        for protocol in protocols_to_check:
            # All protocols should be Protocol subclasses
            assert issubclass(protocol, Protocol), f"{protocol.__name__} is not a Protocol"
            # runtime_checkable protocols can be used with isinstance()
            # Test by trying to use isinstance with a mock - should not raise TypeError
            mock = Mock()
            try:
                isinstance(mock, protocol)
            except TypeError:
                pytest.fail(f"{protocol.__name__} is not @runtime_checkable")
