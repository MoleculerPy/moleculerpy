"""Tests for Request ID propagation.

Phase 3A: Core Protocol Compliance - Request ID propagation tests.
"""

import pytest

from moleculerpy.context import Context


class TestRequestIdBasics:
    """Tests for basic Request ID behavior."""

    def test_root_context_generates_request_id(self) -> None:
        """Root context should use id as request_id by default."""
        ctx = Context(id="ctx-root-1")
        assert ctx.request_id == "ctx-root-1"

    def test_custom_request_id_respected(self) -> None:
        """Explicit request_id should be used instead of id."""
        ctx = Context(id="ctx-1", request_id="req-custom-xyz")
        assert ctx.request_id == "req-custom-xyz"
        assert ctx.id == "ctx-1"

    def test_request_id_in_serialization(self) -> None:
        """Request ID should be serialized as requestID (camelCase)."""
        ctx = Context(id="ctx-1", request_id="req-abc")
        data = ctx.unmarshall()
        assert "requestID" in data
        assert data["requestID"] == "req-abc"


class TestRequestIdPropagation:
    """Tests for Request ID propagation through call chain."""

    def test_child_context_inherits_request_id(self) -> None:
        """Child context should inherit parent's request_id."""
        # Simulate parent context
        parent = Context(id="ctx-parent", request_id="req-chain-1", level=1)

        # Child would be created with same request_id
        child = Context(
            id="ctx-child",
            request_id=parent.request_id,  # Propagated from parent
            level=parent.level + 1,
            parent_id=parent.id,
        )

        assert child.request_id == "req-chain-1"
        assert child.id != parent.id
        assert child.level == 2

    def test_deep_nesting_preserves_request_id(self) -> None:
        """Request ID should be preserved through deep nesting."""
        request_id = "req-original"

        # Simulate deep call chain
        contexts = []
        for i in range(10):
            ctx = Context(
                id=f"ctx-{i}",
                request_id=request_id,
                level=i + 1,
                parent_id=f"ctx-{i - 1}" if i > 0 else None,
            )
            contexts.append(ctx)

        # All contexts should have same request_id
        for ctx in contexts:
            assert ctx.request_id == request_id

        # But different context ids and levels
        assert contexts[0].level == 1
        assert contexts[9].level == 10
        assert len(set(ctx.id for ctx in contexts)) == 10


class TestRequestIdWithLifecycle:
    """Tests for Request ID with Lifecycle."""

    @pytest.fixture
    def lifecycle(self):
        """Create Lifecycle with mock broker."""
        from unittest.mock import MagicMock

        from moleculerpy.lifecycle import Lifecycle
        from moleculerpy.settings import Settings

        mock_broker = MagicMock()
        mock_broker.settings = Settings()
        return Lifecycle(broker=mock_broker)

    def test_lifecycle_propagates_request_id(self, lifecycle) -> None:
        """Lifecycle should create context with specified request_id."""
        ctx = lifecycle.create_context(
            action="test.action",
            request_id="req-from-parent",
            level=2,
        )
        assert ctx.request_id == "req-from-parent"
        assert ctx.level == 2

    def test_lifecycle_generates_request_id_for_root(self, lifecycle) -> None:
        """Lifecycle should generate request_id for root context."""
        ctx = lifecycle.create_context(action="test.action")
        assert ctx.request_id == ctx.id  # Defaults to context id
