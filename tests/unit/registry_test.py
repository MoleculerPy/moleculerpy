"""Unit tests for the Registry module."""

from unittest.mock import MagicMock, Mock

from moleculerpy.registry import Action, Event, Registry


class TestAction:
    """Test Action class."""

    def test_action_initialization(self):
        """Test Action initialization with all parameters."""
        handler = Mock()
        params_schema = {"type": "object", "properties": {"name": {"type": "string"}}}

        action = Action(
            name="service.action",
            node_id="node-123",
            is_local=True,
            handler=handler,
            params_schema=params_schema,
        )

        assert action.name == "service.action"
        assert action.node_id == "node-123"
        assert action.is_local is True
        assert action.handler == handler
        assert action.params_schema == params_schema

    def test_action_minimal_initialization(self):
        """Test Action initialization with minimal parameters."""
        action = Action(name="service.action", node_id="node-123", is_local=False)

        assert action.name == "service.action"
        assert action.node_id == "node-123"
        assert action.is_local is False
        assert action.handler is None
        assert action.params_schema is None


class TestEvent:
    """Test Event class."""

    def test_event_initialization(self):
        """Test Event initialization with all parameters."""
        handler = Mock()

        event = Event(name="user.created", node_id="node-123", is_local=True, handler=handler)

        assert event.name == "user.created"
        assert event.node_id == "node-123"
        assert event.is_local is True
        assert event.handler == handler

    def test_event_minimal_initialization(self):
        """Test Event initialization with minimal parameters."""
        event = Event(name="user.created", node_id="node-123")

        assert event.name == "user.created"
        assert event.node_id == "node-123"
        assert event.is_local is False
        assert event.handler is None

    def test_event_default_is_local(self):
        """Test Event initialization with default is_local value."""
        event = Event(name="user.created", node_id="node-123", handler=Mock())

        assert event.is_local is False


class TestRegistry:
    """Test Registry class."""

    def test_registry_initialization(self):
        """Test Registry initialization."""
        logger = Mock()
        registry = Registry(node_id="node-123", logger=logger)

        assert registry.__node_id__ == "node-123"
        assert registry.__logger__ == logger
        assert registry.__services__ == {}
        assert registry.__actions__ == []
        assert registry.__events__ == []

    def test_registry_minimal_initialization(self):
        """Test Registry initialization without optional parameters."""
        registry = Registry()

        assert registry.__node_id__ is None
        assert registry.__logger__ is None
        assert registry.__services__ == {}
        assert registry.__actions__ == []
        assert registry.__events__ == []
        assert registry._actions_by_name == {}
        assert registry._events_by_name == {}

    def test_registry_allows_compat_dynamic_attributes(self):
        """Registry should allow dynamic attrs used by external tooling/tests."""
        registry = Registry()
        assert not hasattr(registry, "__dict__")
        registry.strategy_name = "Random"
        assert registry.strategy_name == "Random"

    def test_register_service(self):
        """Test registering a service with actions and events."""
        registry = Registry(node_id="node-123", logger=Mock())

        # Create mock service
        service = MagicMock()
        service.name = "test-service"

        # Mock actions
        action1 = Mock()
        action1._name = "customAction"
        action1._params = {"type": "object"}

        action2 = Mock()
        # No _name attribute, should use method name
        del action2._name
        del action2._params

        service.actions.return_value = ["action1", "action2"]
        service.action1 = action1
        service.action2 = action2

        # Mock events
        event1 = Mock()
        event1._name = "custom.event"

        event2 = Mock()
        # No _name attribute, should use method name
        del event2._name

        service.events.return_value = ["event1", "event2"]
        service.event1 = event1
        service.event2 = event2

        # Register the service
        registry.register(service)

        # Check service registration
        assert registry.__services__["test-service"] == service

        # Check actions registration
        assert len(registry.__actions__) == 2
        assert registry.__actions__[0].name == "test-service.customAction"
        assert registry.__actions__[0].node_id == "node-123"
        assert registry.__actions__[0].is_local is True
        assert registry.__actions__[0].handler == action1
        assert registry.__actions__[0].params_schema == {"type": "object"}

        assert registry.__actions__[1].name == "test-service.action2"
        assert registry.__actions__[1].handler == action2
        assert registry.__actions__[1].params_schema is None

        # Check events registration
        assert len(registry.__events__) == 2
        assert registry.__events__[0].name == "custom.event"
        assert registry.__events__[0].node_id == "node-123"
        assert registry.__events__[0].is_local is True
        assert registry.__events__[0].handler == event1

        assert registry.__events__[1].name == "event2"
        assert registry.__events__[1].handler == event2

    def test_get_service(self):
        """Test getting a service by name."""
        registry = Registry()

        service1 = MagicMock()
        service1.name = "service1"
        service1.actions.return_value = []
        service1.events.return_value = []

        service2 = MagicMock()
        service2.name = "service2"
        service2.actions.return_value = []
        service2.events.return_value = []

        registry.register(service1)
        registry.register(service2)

        # Test getting existing services
        assert registry.get_service("service1") == service1
        assert registry.get_service("service2") == service2

        # Test getting non-existent service
        assert registry.get_service("nonexistent") is None

    def test_add_action(self):
        """Test adding an action directly."""
        registry = Registry()

        action = Action(name="service.action", node_id="node-123", is_local=False)

        registry.add_action(action)

        assert len(registry.__actions__) == 1
        assert registry.__actions__[0] == action

    def test_add_event(self):
        """Test adding an event by name and node_id."""
        registry = Registry()

        registry.add_event("user.created", "node-123")

        assert len(registry.__events__) == 1
        assert registry.__events__[0].name == "user.created"
        assert registry.__events__[0].node_id == "node-123"
        assert registry.__events__[0].is_local is False
        assert registry.__events__[0].handler is None

    def test_add_event_obj(self):
        """Test adding an event object directly."""
        registry = Registry()

        event = Event(name="user.created", node_id="node-123", is_local=True, handler=Mock())

        registry.add_event_obj(event)

        assert len(registry.__events__) == 1
        assert registry.__events__[0] == event

    def test_get_action(self):
        """Test getting an action by name."""
        registry = Registry()

        action1 = Action("service.action1", "node-1", True)
        action2 = Action("service.action2", "node-2", False)
        action3 = Action("other.action", "node-3", True)

        registry.add_action(action1)
        registry.add_action(action2)
        registry.add_action(action3)

        # Test getting existing actions
        assert registry.get_action("service.action1") == action1
        assert registry.get_action("service.action2") == action2
        assert registry.get_action("other.action") == action3

        # Test getting non-existent action
        assert registry.get_action("nonexistent.action") is None

    def test_get_action_returns_first_match(self):
        """Test that get_action returns the first matching action."""
        registry = Registry()

        # Add multiple actions with the same name (different nodes)
        action1 = Action("service.action", "node-1", True)
        action2 = Action("service.action", "node-2", False)

        registry.add_action(action1)
        registry.add_action(action2)

        # Should return the first one
        assert registry.get_action("service.action") == action1

    def test_action_name_index_updates_on_add_and_remove(self):
        """Action name index should stay consistent after add/remove operations."""
        registry = Registry()
        action1 = Action("service.action", "node-1", True)
        action2 = Action("service.action", "node-2", False)

        registry.add_action(action1)
        registry.add_action(action2)
        assert registry._actions_by_name["service.action"] == [action1, action2]

        removed = registry.remove_actions_by_node("node-1")
        assert removed == 1
        assert registry._actions_by_name["service.action"] == [action2]

    def test_get_action_endpoint_uses_indexed_actions_without_copy(self):
        """Endpoint selection should consume indexed action list directly."""
        registry = Registry(prefer_local=False)
        action1 = Action("service.action", "node-1", False)
        action2 = Action("service.action", "node-2", False)
        registry.add_action(action1)
        registry.add_action(action2)

        selector = Mock(return_value=action1)
        registry._strategy = Mock(select=selector)

        selected = registry.get_action_endpoint("service.action")

        assert selected == action1
        assert selector.call_count == 1
        passed_endpoints, passed_ctx = selector.call_args[0]
        assert passed_endpoints is registry._actions_by_name["service.action"]
        assert passed_ctx is None

    def test_get_all_events(self):
        """Test getting all events by name."""
        registry = Registry()

        # Add multiple events with same and different names
        event1 = Event("user.created", "node-1", True)
        event2 = Event("user.created", "node-2", False)
        event3 = Event("user.updated", "node-3", True)
        event4 = Event("user.created", "node-4", False)

        registry.add_event_obj(event1)
        registry.add_event_obj(event2)
        registry.add_event_obj(event3)
        registry.add_event_obj(event4)

        # Test getting all events with name 'user.created'
        user_created_events = registry.get_all_events("user.created")
        assert len(user_created_events) == 3
        assert event1 in user_created_events
        assert event2 in user_created_events
        assert event4 in user_created_events

        # Test getting all events with name 'user.updated'
        user_updated_events = registry.get_all_events("user.updated")
        assert len(user_updated_events) == 1
        assert event3 in user_updated_events

        # Test getting events with non-existent name
        nonexistent_events = registry.get_all_events("nonexistent.event")
        assert len(nonexistent_events) == 0

    def test_event_name_index_updates_on_add_and_remove(self):
        """Event name index should stay consistent after add/remove operations."""
        registry = Registry()
        event1 = Event("user.created", "node-1", True)
        event2 = Event("user.created", "node-2", False)

        registry.add_event_obj(event1)
        registry.add_event_obj(event2)
        assert registry._events_by_name["user.created"] == [event1, event2]

        removed = registry.remove_events_by_node("node-1")
        assert removed == 1
        assert registry._events_by_name["user.created"] == [event2]

    def test_get_event(self):
        """Test getting the first event by name."""
        registry = Registry()

        # Add multiple events with same name
        event1 = Event("user.created", "node-1", True)
        event2 = Event("user.created", "node-2", False)
        event3 = Event("user.updated", "node-3", True)

        registry.add_event_obj(event1)
        registry.add_event_obj(event2)
        registry.add_event_obj(event3)

        # Should return the first matching event
        assert registry.get_event("user.created") == event1
        assert registry.get_event("user.updated") == event3

        # Test getting non-existent event
        assert registry.get_event("nonexistent.event") is None

    def test_get_all_events_matches_wildcard_patterns(self):
        """Wildcard subscriptions should match concrete event names."""
        registry = Registry()

        wildcard = Event("order.*", "node-1", True)
        exact = Event("order.created", "node-2", False)
        unrelated = Event("user.created", "node-3", False)

        registry.add_event_obj(wildcard)
        registry.add_event_obj(exact)
        registry.add_event_obj(unrelated)

        matched = registry.get_all_events("order.created")

        assert wildcard in matched
        assert exact in matched
        assert unrelated not in matched

    def test_get_event_prefers_exact_before_wildcard(self):
        """Exact event handler should be preferred over wildcard for emit semantics."""
        registry = Registry()

        wildcard = Event("order.*", "node-1", True)
        exact = Event("order.created", "node-2", False)

        registry.add_event_obj(wildcard)
        registry.add_event_obj(exact)

        assert registry.get_event("order.created") == exact

    def test_get_event_does_not_require_get_all_events_materialization(self):
        """Single event lookup should not depend on list materialization path."""
        registry = Registry()
        wildcard = Event("order.*", "node-1", True)
        registry.add_event_obj(wildcard)

        registry.get_all_events = Mock(side_effect=AssertionError("unexpected get_all_events call"))

        assert registry.get_event("order.created") == wildcard

    def test_register_service_without_logger(self):
        """Test registering a service without a logger."""
        registry = Registry(node_id="node-123")  # No logger

        service = MagicMock()
        service.name = "test-service"
        service.actions.return_value = []

        event_method = Mock()
        event_method._name = "test.event"
        service.events.return_value = ["event_method"]
        service.event_method = event_method

        # Should not raise any errors
        registry.register(service)

        assert len(registry.__events__) == 1
        assert registry.__events__[0].name == "test.event"

    def test_register_service_with_logger(self):
        """Test that events are logged when logger is present."""
        logger = Mock()
        registry = Registry(node_id="node-123", logger=logger)

        service = MagicMock()
        service.name = "test-service"
        service.actions.return_value = []

        event1 = Mock()
        event1._name = "event.one"
        event2 = Mock()
        event2._name = "event.two"

        service.events.return_value = ["event1", "event2"]
        service.event1 = event1
        service.event2 = event2

        registry.register(service)

        # Check that debug logs were made for each event
        assert logger.debug.call_count == 2

        call_args_list = [call[0][0] for call in logger.debug.call_args_list]
        assert "Event event.one from node node-123 (local=True)" in call_args_list
        assert "Event event.two from node node-123 (local=True)" in call_args_list

    def test_empty_registry_operations(self):
        """Test operations on an empty registry."""
        registry = Registry()

        # All get operations should return None or empty lists
        assert registry.get_service("any-service") is None
        assert registry.get_action("any.action") is None
        assert registry.get_event("any.event") is None
        assert registry.get_all_events("any.event") == []


class TestCascadeCleanup:
    """Test cascade cleanup methods (Moleculer.js/Go compatible).

    Tests for the cascade cleanup pattern that removes all services,
    actions, and events belonging to a disconnected node.
    """

    def test_remove_actions_by_node(self):
        """Test removing actions belonging to a disconnected node."""
        registry = Registry(node_id="local", logger=Mock())

        # Add actions from different nodes
        action1 = Action("svc.action1", "node-1", False)
        action2 = Action("svc.action2", "node-1", False)
        action3 = Action("svc.action3", "node-2", False)
        action4 = Action("svc.action4", "local", True)

        registry.add_action(action1)
        registry.add_action(action2)
        registry.add_action(action3)
        registry.add_action(action4)

        assert len(registry.__actions__) == 4

        # Remove actions from node-1
        removed = registry.remove_actions_by_node("node-1")

        assert removed == 2
        assert len(registry.__actions__) == 2
        assert all(a.node_id != "node-1" for a in registry.__actions__)

    def test_remove_actions_by_node_empty(self):
        """Test removing actions from non-existent node."""
        registry = Registry(logger=Mock())

        action = Action("svc.action", "node-1", False)
        registry.add_action(action)

        removed = registry.remove_actions_by_node("nonexistent")

        assert removed == 0
        assert len(registry.__actions__) == 1

    def test_remove_events_by_node(self):
        """Test removing events belonging to a disconnected node."""
        registry = Registry(logger=Mock())

        event1 = Event("user.created", "node-1")
        event2 = Event("user.updated", "node-1")
        event3 = Event("user.deleted", "node-2")

        registry.add_event_obj(event1)
        registry.add_event_obj(event2)
        registry.add_event_obj(event3)

        assert len(registry.__events__) == 3

        removed = registry.remove_events_by_node("node-1")

        assert removed == 2
        assert len(registry.__events__) == 1
        assert registry.__events__[0].node_id == "node-2"

    def test_remove_events_by_node_empty(self):
        """Test removing events from non-existent node."""
        registry = Registry(logger=Mock())

        event = Event("user.created", "node-1")
        registry.add_event_obj(event)

        removed = registry.remove_events_by_node("nonexistent")

        assert removed == 0
        assert len(registry.__events__) == 1

    def test_remove_services_by_node(self):
        """Test removing services belonging to a disconnected node."""
        registry = Registry(node_id="local", logger=Mock())

        # Create mock services with node_id attribute
        service1 = MagicMock()
        service1.name = "svc1"
        service1.node_id = "node-1"
        service1.actions.return_value = []
        service1.events.return_value = []

        service2 = MagicMock()
        service2.name = "svc2"
        service2.node_id = "node-2"
        service2.actions.return_value = []
        service2.events.return_value = []

        registry.register(service1)
        registry.register(service2)

        assert len(registry.__services__) == 2

        removed = registry.remove_services_by_node("node-1")

        assert removed == 1
        assert len(registry.__services__) == 1
        assert "svc2" in registry.__services__

    def test_cleanup_node_returns_typed_result(self):
        """Test cleanup_node returns CleanupResult TypedDict."""

        registry = Registry(node_id="local", logger=Mock())

        # Setup: add items from node-1
        registry.add_action(Action("svc.action1", "node-1", False))
        registry.add_action(Action("svc.action2", "node-1", False))
        registry.add_event_obj(Event("evt.event1", "node-1"))

        result = registry.cleanup_node("node-1")

        # Verify it's a proper dict with expected keys
        assert isinstance(result, dict)
        assert "services" in result
        assert "actions" in result
        assert "events" in result
        assert result["services"] == 0
        assert result["actions"] == 2
        assert result["events"] == 1

    def test_cleanup_node_nonexistent(self):
        """Test cleanup_node for non-existent node returns zeros."""
        registry = Registry(logger=Mock())

        result = registry.cleanup_node("nonexistent")

        assert result == {"services": 0, "actions": 0, "events": 0}

    def test_cleanup_node_logs_when_items_removed(self):
        """Test that cleanup_node logs when items are removed."""
        logger = Mock()
        registry = Registry(node_id="local", logger=logger)

        registry.add_action(Action("svc.action", "node-1", False))

        registry.cleanup_node("node-1")

        # Should log info about cleanup
        assert logger.info.called
        call_args = logger.info.call_args[0][0]
        assert "node-1" in call_args
        assert "1 actions" in call_args

    def test_cleanup_node_no_log_when_nothing_removed(self):
        """Test that cleanup_node doesn't log when nothing is removed."""
        logger = Mock()
        registry = Registry(logger=logger)

        registry.cleanup_node("nonexistent")

        # Should not log info (nothing removed)
        assert not logger.info.called

    def test_cleanup_node_order_services_first(self):
        """Test that cleanup removes services before actions/events."""
        # This tests the order: services → actions → events
        # Important for metadata that might be needed during cleanup
        registry = Registry(node_id="local", logger=Mock())

        service = MagicMock()
        service.name = "svc"
        service.node_id = "node-1"
        service.actions.return_value = []
        service.events.return_value = []

        registry.register(service)
        registry.add_action(Action("svc.action", "node-1", False))
        registry.add_event_obj(Event("svc.event", "node-1"))

        result = registry.cleanup_node("node-1")

        # All should be removed
        assert result["services"] == 1
        assert result["actions"] == 1
        assert result["events"] == 1
        assert len(registry.__services__) == 0
        assert len(registry.__actions__) == 0
        assert len(registry.__events__) == 0
