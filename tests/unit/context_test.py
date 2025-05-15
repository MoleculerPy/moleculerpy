import gc
from unittest.mock import AsyncMock, Mock

import pytest

from moleculerpy.context import Context, is_action_context, is_event_context


@pytest.fixture
def mock_broker():
    broker = Mock()
    broker.nodeID = "test-node-id"  # Phase 3B: Required for Context.node_id
    broker.call = AsyncMock(return_value="call_result")
    broker.emit = AsyncMock(return_value="emit_result")
    broker.broadcast = AsyncMock(return_value="broadcast_result")
    return broker


@pytest.fixture
def context(mock_broker):
    return Context(
        id="test-id",
        action="test.action",
        event="test.event",
        parent_id="parent-id",
        params={"key": "value"},
        meta={"meta_key": "meta_value"},
        stream=False,
        broker=mock_broker,
    )


def test_context_initialization(context):
    assert context.id == "test-id"
    assert context.action == "test.action"
    assert context.event == "test.event"
    assert context.parent_id == "parent-id"
    assert context.params == {"key": "value"}
    assert context.meta == {"meta_key": "meta_value"}
    assert context.stream is False
    assert context.broker is not None
    assert context.kind == "internal"


def test_context_initialization_with_defaults():
    ctx = Context(id="test-id")
    assert ctx.id == "test-id"
    assert ctx.action is None
    assert ctx.event is None
    assert ctx.parent_id is None
    assert ctx.params == {}
    assert ctx.meta == {}
    assert ctx.stream is False
    assert ctx.broker is None
    assert ctx.kind == "internal"


def test_context_kind_and_type_guards():
    action_ctx = Context(id="a-1", action="users.get")
    event_ctx = Context(id="e-1", event="user.created")

    assert action_ctx.kind == "action"
    assert event_ctx.kind == "event"
    assert is_action_context(action_ctx)
    assert not is_event_context(action_ctx)
    assert is_event_context(event_ctx)
    assert not is_action_context(event_ctx)


def test_context_uses_slots_for_memory_efficiency():
    """Context should use __slots__ to avoid per-instance __dict__ overhead."""
    ctx = Context(id="test-id")
    assert hasattr(Context, "__slots__")
    assert not hasattr(ctx, "__dict__")


def test_context_allows_dynamic_attrs_for_middleware_compatibility():
    """Context should still allow custom attrs used by external middleware."""
    ctx = Context(id="test-id")
    ctx.custom_trace_id = "trace-123"
    assert ctx.custom_trace_id == "trace-123"


def test_context_init_bypasses_custom_setattr():
    """Context should avoid __setattr__ overhead for core fields."""

    class CountingContext(Context):
        def __init__(self, *args, **kwargs) -> None:
            object.__setattr__(self, "setattr_calls", 0)
            super().__init__(*args, **kwargs)

        def __setattr__(self, name: str, value) -> None:
            if name != "setattr_calls":
                object.__setattr__(self, "setattr_calls", self.setattr_calls + 1)
            super().__setattr__(name, value)

    ctx = CountingContext(id="test-id")
    assert ctx.setattr_calls == 0


def test_context_broker_reference_is_weak():
    """Context should not keep broker alive through a strong reference."""

    class DummyBroker:
        def __init__(self) -> None:
            self.nodeID = "dummy-node"

    broker = DummyBroker()
    ctx = Context(id="ctx-weakref", broker=broker)
    assert ctx.broker is broker

    del broker
    gc.collect()

    assert ctx.broker is None


def test_marshall(context):
    marshalled = context.marshall()
    assert marshalled == {
        "id": "test-id",
        "action": "test.action",
        "event": "test.event",
        "params": {"key": "value"},
        "meta": {"meta_key": "meta_value"},
        "timeout": 0,
        "level": 1,
        "requestID": "test-id",  # Defaults to context id
        "tracing": None,
        "parentID": "parent-id",
        "stream": False,
        # Phase 3B: New distributed tracing fields
        "nodeID": "test-node-id",  # From mock_broker.id
        "caller": None,
        # Phase 3C: Event Acknowledgment fields
        "needAck": None,
        "ackID": None,
        # Phase 3D: Streaming
        "seq": None,
    }


def test_unmarshall(context):
    unmarshalled = context.unmarshall()
    assert unmarshalled == {
        "id": "test-id",
        "action": "test.action",
        "event": "test.event",
        "params": {"key": "value"},
        "meta": {"meta_key": "meta_value"},
        "timeout": 0,
        "level": 1,
        "requestID": "test-id",  # Defaults to context id
        "tracing": None,
        "parentID": "parent-id",
        "stream": False,
        # Phase 3B: New distributed tracing fields
        "nodeID": "test-node-id",  # From mock_broker.id
        "caller": None,
        # Phase 3C: Event Acknowledgment fields
        "needAck": None,
        "ackID": None,
        # Phase 3D: Streaming
        "seq": None,
    }


def test_prepare_meta(context):
    """Test _prepare_meta merges metadata (now sync for performance)."""
    new_meta = {"new_key": "new_value"}
    result = context._prepare_meta(new_meta)
    assert result == {"meta_key": "meta_value", "new_key": "new_value"}


def test_prepare_meta_empty(context):
    """Test _prepare_meta with no args returns copy of context meta."""
    result = context._prepare_meta()
    assert result == {"meta_key": "meta_value"}


@pytest.mark.asyncio
async def test_call(context, mock_broker):
    params = {"param_key": "param_value"}
    meta = {"meta_key": "new_meta_value"}

    result = await context.call("service.action", params, meta)

    assert result == "call_result"
    # Context.call() now passes parent_ctx=self and timeout to broker.call()
    mock_broker.call.assert_called_once_with(
        "service.action",
        {"param_key": "param_value"},
        {"meta_key": "new_meta_value"},
        parent_ctx=context,
        timeout=None,
    )


@pytest.mark.asyncio
async def test_emit(context, mock_broker):
    params = {"param_key": "param_value"}
    meta = {"meta_key": "new_meta_value"}

    result = await context.emit("event", params, meta)

    assert result == "emit_result"
    # Meta is merged: context.meta + provided meta (provided overwrites)
    mock_broker.emit.assert_called_once_with(
        "event", {"param_key": "param_value"}, {"meta_key": "new_meta_value"}
    )


@pytest.mark.asyncio
async def test_broadcast(context, mock_broker):
    params = {"param_key": "param_value"}
    meta = {"meta_key": "new_meta_value"}

    result = await context.broadcast("event", params, meta)

    assert result == "broadcast_result"
    # Meta is merged: context.meta + provided meta (provided overwrites)
    mock_broker.broadcast.assert_called_once_with(
        "event", {"param_key": "param_value"}, {"meta_key": "new_meta_value"}
    )


@pytest.mark.asyncio
async def test_call_without_broker(context):
    context._broker = None
    with pytest.raises(AttributeError):
        await context.call("service.action")


@pytest.mark.asyncio
async def test_emit_without_broker(context):
    context._broker = None
    with pytest.raises(AttributeError):
        await context.emit("service.event")


@pytest.mark.asyncio
async def test_broadcast_without_broker(context):
    context._broker = None
    with pytest.raises(AttributeError):
        await context.broadcast("service.event")
