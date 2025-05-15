"""Tests for Phase 3B Context distributed tracing fields.

Tests for node_id, caller, tracing, and service fields added in Phase 3B.
"""

from unittest.mock import Mock

from moleculerpy.context import Context
from moleculerpy.lifecycle import Lifecycle


class TestContextNodeId:
    """Tests for Context.node_id field."""

    def test_node_id_defaults_to_broker_id(self):
        """node_id should default to broker.id when not explicitly provided."""
        broker = Mock()
        broker.nodeID = "node-abc123"

        ctx = Context(id="ctx-1", broker=broker)

        assert ctx.node_id == "node-abc123"

    def test_node_id_explicit_value(self):
        """Explicit node_id should override broker.id."""
        broker = Mock()
        broker.nodeID = "node-abc123"

        ctx = Context(id="ctx-1", broker=broker, node_id="explicit-node")

        assert ctx.node_id == "explicit-node"

    def test_node_id_none_without_broker(self):
        """node_id should be None when no broker and no explicit value."""
        ctx = Context(id="ctx-1")

        assert ctx.node_id is None

    def test_node_id_serialized_as_nodeID(self):
        """node_id should serialize as camelCase 'nodeID' for wire format."""
        broker = Mock()
        broker.nodeID = "node-xyz"

        ctx = Context(id="ctx-1", broker=broker)
        data = ctx.unmarshall()

        assert "nodeID" in data
        assert data["nodeID"] == "node-xyz"


class TestContextCaller:
    """Tests for Context.caller field."""

    def test_caller_defaults_to_none(self):
        """caller should default to None when not provided."""
        ctx = Context(id="ctx-1")

        assert ctx.caller is None

    def test_caller_explicit_value(self):
        """Explicit caller value should be preserved."""
        ctx = Context(id="ctx-1", caller="v2.users")

        assert ctx.caller == "v2.users"

    def test_caller_serialized_in_unmarshall(self):
        """caller should be included in unmarshall() output."""
        ctx = Context(id="ctx-1", caller="graph.extract")
        data = ctx.unmarshall()

        assert "caller" in data
        assert data["caller"] == "graph.extract"

    def test_caller_none_serializes_as_none(self):
        """None caller should serialize as None (not omitted)."""
        ctx = Context(id="ctx-1")
        data = ctx.unmarshall()

        assert "caller" in data
        assert data["caller"] is None


class TestContextTracing:
    """Tests for Context.tracing field."""

    def test_tracing_defaults_to_none(self):
        """tracing should default to None (inherit from parent)."""
        ctx = Context(id="ctx-1")

        assert ctx.tracing is None

    def test_tracing_explicit_true(self):
        """Explicit tracing=True should be preserved."""
        ctx = Context(id="ctx-1", tracing=True)

        assert ctx.tracing is True

    def test_tracing_explicit_false(self):
        """Explicit tracing=False should be preserved."""
        ctx = Context(id="ctx-1", tracing=False)

        assert ctx.tracing is False

    def test_tracing_serialized_in_unmarshall(self):
        """tracing should be included in unmarshall() output."""
        ctx = Context(id="ctx-1", tracing=True)
        data = ctx.unmarshall()

        assert "tracing" in data
        assert data["tracing"] is True


class TestContextService:
    """Tests for Context.service field."""

    def test_service_defaults_to_none(self):
        """service should default to None when not provided."""
        ctx = Context(id="ctx-1")

        assert ctx.service is None

    def test_service_explicit_value(self):
        """Explicit service reference should be preserved."""
        mock_service = Mock()
        mock_service.name = "test.service"

        ctx = Context(id="ctx-1", service=mock_service)

        assert ctx.service is mock_service
        assert ctx.service.name == "test.service"

    def test_service_not_serialized(self):
        """service should NOT be serialized (it's a runtime reference)."""
        mock_service = Mock()
        mock_service.name = "test.service"

        ctx = Context(id="ctx-1", service=mock_service)
        data = ctx.unmarshall()

        # service is runtime-only, not sent over the wire
        assert "service" not in data


class TestLifecycleCreateContext:
    """Tests for Lifecycle.create_context() with new fields."""

    def test_create_context_with_new_fields(self):
        """create_context should accept and pass new Phase 3B fields."""
        broker = Mock()
        broker.nodeID = "lifecycle-node"
        lifecycle = Lifecycle(broker)

        ctx = lifecycle.create_context(
            action="test.action",
            node_id="explicit-node",
            caller="parent.service",
            tracing=True,
        )

        assert ctx.node_id == "explicit-node"
        assert ctx.caller == "parent.service"
        assert ctx.tracing is True

    def test_create_context_node_id_defaults_to_broker(self):
        """create_context should default node_id to broker.id."""
        broker = Mock()
        broker.nodeID = "default-node-id"
        lifecycle = Lifecycle(broker)

        ctx = lifecycle.create_context(action="test.action")

        assert ctx.node_id == "default-node-id"


class TestLifecycleRebuildContext:
    """Tests for Lifecycle.rebuild_context() with new fields."""

    def test_rebuild_context_camel_case_nodeID(self):
        """rebuild_context should deserialize camelCase 'nodeID'."""
        broker = Mock()
        broker.nodeID = "local-node"
        lifecycle = Lifecycle(broker)

        ctx = lifecycle.rebuild_context(
            {
                "id": "ctx-123",
                "nodeID": "remote-node",
                "caller": "v1.api",
                "tracing": False,
            }
        )

        assert ctx.node_id == "remote-node"
        assert ctx.caller == "v1.api"
        assert ctx.tracing is False

    def test_rebuild_context_snake_case_node_id(self):
        """rebuild_context should also accept snake_case 'node_id'."""
        broker = Mock()
        broker.nodeID = "local-node"
        lifecycle = Lifecycle(broker)

        ctx = lifecycle.rebuild_context(
            {
                "id": "ctx-456",
                "node_id": "remote-node-snake",
            }
        )

        assert ctx.node_id == "remote-node-snake"

    def test_rebuild_context_missing_fields_defaults(self):
        """Missing fields use appropriate defaults in rebuild_context."""
        broker = Mock()
        broker.nodeID = "local-node"
        lifecycle = Lifecycle(broker)

        ctx = lifecycle.rebuild_context({"id": "ctx-789"})

        # node_id falls back to broker.id via Context.__init__ default
        assert ctx.node_id == "local-node"
        # caller and tracing default to None
        assert ctx.caller is None
        assert ctx.tracing is None


class TestContextFieldsPropagation:
    """Integration-style tests for field propagation."""

    def test_all_new_fields_in_unmarshall(self):
        """All new Phase 3B fields should appear in unmarshall()."""
        broker = Mock()
        broker.nodeID = "test-broker"

        ctx = Context(
            id="full-ctx",
            broker=broker,
            node_id="my-node",
            caller="parent.action",
            tracing=True,
        )

        data = ctx.unmarshall()

        assert data["nodeID"] == "my-node"
        assert data["caller"] == "parent.action"
        assert data["tracing"] is True

    def test_marshall_equals_unmarshall(self):
        """marshall() and unmarshall() should return identical data."""
        broker = Mock()
        broker.nodeID = "broker-node"

        ctx = Context(
            id="test-ctx",
            broker=broker,
            caller="some.service",
            tracing=False,
        )

        assert ctx.marshall() == ctx.unmarshall()
