"""Tests for domain primitive NewType aliases."""

from moleculerpy.domain_types import (
    ActionName,
    ContextID,
    EventName,
    NodeID,
    RequestID,
    ServiceName,
)


def test_domain_types_are_runtime_strings():
    node_id = NodeID("node-1")
    service = ServiceName("users")
    action = ActionName("users.get")
    event = EventName("user.created")
    context_id = ContextID("ctx-1")
    request_id = RequestID("req-1")

    assert isinstance(node_id, str)
    assert isinstance(service, str)
    assert isinstance(action, str)
    assert isinstance(event, str)
    assert isinstance(context_id, str)
    assert isinstance(request_id, str)
