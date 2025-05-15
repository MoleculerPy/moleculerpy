"""Tests for Context.level and max call level protection.

Phase 3A: Core Protocol Compliance - Context level tests.
"""

import pytest

from moleculerpy.context import Context
from moleculerpy.errors import MaxCallLevelError
from moleculerpy.lifecycle import Lifecycle
from moleculerpy.settings import Settings


class TestContextLevel:
    """Tests for Context.level attribute."""

    def test_root_context_has_level_1(self) -> None:
        """Root context should have level=1 by default."""
        ctx = Context(id="ctx-1")
        assert ctx.level == 1

    def test_context_with_explicit_level(self) -> None:
        """Context should accept explicit level."""
        ctx = Context(id="ctx-1", level=5)
        assert ctx.level == 5

    def test_level_serialized_in_unmarshall(self) -> None:
        """Level should be included in unmarshall output."""
        ctx = Context(id="ctx-1", level=3)
        data = ctx.unmarshall()
        assert data["level"] == 3

    def test_level_not_hardcoded_anymore(self) -> None:
        """Verify level is dynamic, not hardcoded to 1."""
        ctx1 = Context(id="ctx-1", level=1)
        ctx2 = Context(id="ctx-2", level=7)

        assert ctx1.unmarshall()["level"] == 1
        assert ctx2.unmarshall()["level"] == 7


class TestContextRequestId:
    """Tests for Context.request_id attribute."""

    def test_root_context_request_id_defaults_to_id(self) -> None:
        """Root context request_id should default to context id."""
        ctx = Context(id="ctx-abc-123")
        assert ctx.request_id == "ctx-abc-123"

    def test_context_with_explicit_request_id(self) -> None:
        """Context should accept explicit request_id."""
        ctx = Context(id="ctx-1", request_id="req-xyz")
        assert ctx.request_id == "req-xyz"
        assert ctx.id == "ctx-1"

    def test_request_id_serialized_in_unmarshall(self) -> None:
        """Request ID should be included in unmarshall output."""
        ctx = Context(id="ctx-1", request_id="req-chain-1")
        data = ctx.unmarshall()
        assert data["requestID"] == "req-chain-1"


class TestLifecycleContextCreation:
    """Tests for Lifecycle.create_context with level/request_id."""

    @pytest.fixture
    def lifecycle(self) -> Lifecycle:
        """Create Lifecycle with mock broker."""
        from unittest.mock import MagicMock

        mock_broker = MagicMock()
        mock_broker.settings = Settings()
        return Lifecycle(broker=mock_broker)

    def test_create_context_default_level(self, lifecycle: Lifecycle) -> None:
        """Created context should have level=1 by default."""
        ctx = lifecycle.create_context(action="test.action")
        assert ctx.level == 1

    def test_create_context_with_level(self, lifecycle: Lifecycle) -> None:
        """Create context should accept level parameter."""
        ctx = lifecycle.create_context(action="test.action", level=5)
        assert ctx.level == 5

    def test_create_context_default_request_id(self, lifecycle: Lifecycle) -> None:
        """Request ID should default to context id."""
        ctx = lifecycle.create_context(action="test.action")
        assert ctx.request_id == ctx.id

    def test_create_context_with_request_id(self, lifecycle: Lifecycle) -> None:
        """Create context should accept request_id parameter."""
        ctx = lifecycle.create_context(
            action="test.action",
            request_id="req-chain-1",
        )
        assert ctx.request_id == "req-chain-1"
        assert ctx.request_id != ctx.id

    def test_rebuild_context_preserves_level(self, lifecycle: Lifecycle) -> None:
        """Rebuild should preserve level from dict."""
        data = {
            "id": "ctx-1",
            "action": "test.action",
            "level": 4,
            "requestID": "req-1",
        }
        ctx = lifecycle.rebuild_context(data)
        assert ctx.level == 4

    def test_rebuild_context_preserves_request_id(self, lifecycle: Lifecycle) -> None:
        """Rebuild should preserve requestID from dict."""
        data = {
            "id": "ctx-1",
            "action": "test.action",
            "level": 2,
            "requestID": "req-chain-xyz",
        }
        ctx = lifecycle.rebuild_context(data)
        assert ctx.request_id == "req-chain-xyz"

    def test_rebuild_context_handles_snake_case(self, lifecycle: Lifecycle) -> None:
        """Rebuild should handle snake_case keys too."""
        data = {
            "id": "ctx-1",
            "action": "test.action",
            "level": 3,
            "request_id": "req-snake",
            "parent_id": "parent-1",
        }
        ctx = lifecycle.rebuild_context(data)
        assert ctx.request_id == "req-snake"
        assert ctx.parent_id == "parent-1"


class TestSettingsMaxCallLevel:
    """Tests for Settings.max_call_level."""

    def test_default_max_call_level_is_zero(self) -> None:
        """Default max_call_level should be 0 (unlimited)."""
        settings = Settings()
        assert settings.max_call_level == 0

    def test_max_call_level_configurable(self) -> None:
        """max_call_level should be configurable."""
        settings = Settings(max_call_level=10)
        assert settings.max_call_level == 10

    def test_max_call_level_validation_negative(self) -> None:
        """Negative max_call_level should raise error."""
        from moleculerpy.settings import SettingsValidationError

        with pytest.raises(SettingsValidationError):
            Settings(max_call_level=-1)

    def test_max_call_level_zero_is_valid(self) -> None:
        """Zero max_call_level should be valid (unlimited)."""
        settings = Settings(max_call_level=0)
        assert settings.max_call_level == 0


class TestMaxCallLevelError:
    """Tests for MaxCallLevelError."""

    def test_error_attributes(self) -> None:
        """MaxCallLevelError should have correct attributes."""
        err = MaxCallLevelError(node_id="node-1", level=11)
        assert err.node_id == "node-1"
        assert err.level == 11
        assert err.code == 500
        assert err.type == "MAX_CALL_LEVEL"
        assert err.retryable is False

    def test_error_message(self) -> None:
        """Error message should include level and node."""
        err = MaxCallLevelError(node_id="test-node", level=6)
        assert "6" in err.message
        assert "test-node" in err.message
