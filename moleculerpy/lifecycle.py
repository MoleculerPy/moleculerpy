"""Lifecycle management for contexts in the MoleculerPy framework.

This module provides lifecycle management capabilities for request contexts,
including creation, rebuilding, and cleanup of execution contexts.
"""

import uuid
from typing import TYPE_CHECKING, Any

from .context import Context
from .domain_types import ActionName, ContextID, EventName, NodeID, RequestID, ServiceName

if TYPE_CHECKING:
    from .broker import ServiceBroker


class Lifecycle:
    """Manages the lifecycle of execution contexts within a MoleculerPy node.

    The Lifecycle class is responsible for creating and managing Context instances
    that represent the execution environment for service actions and events.
    """

    def __init__(self, broker: "ServiceBroker") -> None:
        """Initialize a new Lifecycle instance.

        Args:
            broker: Reference to the service broker
        """
        self.broker = broker

    def create_context(
        self,
        context_id: ContextID | str | uuid.UUID | None = None,
        event: EventName | str | None = None,
        action: ActionName | str | None = None,
        parent_id: str | None = None,
        params: dict[str, Any] | None = None,
        meta: dict[str, Any] | None = None,
        stream: bool = False,
        level: int = 1,
        request_id: RequestID | str | None = None,
        timeout: float | None = None,
        # Phase 3B: New fields for distributed tracing
        node_id: NodeID | str | None = None,
        caller: ServiceName | str | None = None,
        tracing: bool | None = None,
        service: Any | None = None,
        # Phase 3C: Event Acknowledgment
        need_ack: bool | None = None,
        ack_id: str | None = None,
        # Phase 3D: Streaming protocol
        seq: int | None = None,
    ) -> Context:
        """Create a new execution context.

        Args:
            context_id: Unique identifier for the context. If None, a UUID will be generated
            event: Name of the event being handled
            action: Name of the action being executed
            parent_id: ID of the parent context for nested calls
            params: Parameters for the action/event
            meta: Metadata associated with the request
            stream: Whether this is a streaming context
            level: Call depth in the call chain (default: 1 for root calls)
            request_id: Request chain ID (defaults to context id for root calls)
            timeout: Request timeout in seconds (optional)
            node_id: ID of the node where context was created
            caller: Full name of the calling service
            tracing: Whether to trace this request
            service: Reference to the service handling this context
            need_ack: Whether event requires acknowledgment from receiver
            ack_id: Unique ID for correlating ACK responses
            seq: Sequence number for streaming (0 = init, 1..N = data chunks)

        Returns:
            New Context instance with proper level and request_id tracking
        """
        if context_id is None:
            context_id = uuid.uuid4()

        ctx_id = ContextID(str(context_id))

        return Context(
            id=ctx_id,
            action=action,
            event=event,
            parent_id=parent_id,
            params=params,
            meta=meta,
            stream=stream,
            broker=self.broker,
            level=level,
            request_id=request_id if request_id is not None else RequestID(str(ctx_id)),
            timeout=timeout,
            # Phase 3B: Pass new fields
            node_id=node_id if node_id is not None else NodeID(self.broker.nodeID),
            caller=caller,
            tracing=tracing,
            service=service,
            # Phase 3C: Event Acknowledgment
            need_ack=need_ack,
            ack_id=ack_id,
            # Phase 3D: Streaming
            seq=seq,
        )

    def rebuild_context(self, context_dict: dict[str, Any]) -> Context:
        """Rebuild a context from a dictionary representation.

        This is typically used when reconstructing contexts from serialized
        data received over the network (transit packets).

        Args:
            context_dict: Dictionary containing context data

        Returns:
            Reconstructed Context instance with level and request_id preserved
        """
        ctx_id = str(context_dict.get("id", uuid.uuid4()))

        # Handle both camelCase (from transit) and snake_case
        # Use explicit None check to handle empty string values correctly
        parent_id = context_dict.get("parentID")
        if parent_id is None:
            parent_id = context_dict.get("parent_id")

        request_id = context_dict.get("requestID")
        if request_id is None:
            request_id = context_dict.get("request_id")
        # Phase 3B: New fields (support both camelCase and snake_case)
        # Use explicit None check to handle False values correctly
        node_id = context_dict.get("nodeID")
        if node_id is None:
            node_id = context_dict.get("node_id")

        # Phase 3C: Event Acknowledgment (support both camelCase and snake_case)
        # Use explicit None check to handle False values correctly
        need_ack = context_dict.get("needAck")
        if need_ack is None:
            need_ack = context_dict.get("need_ack")

        ack_id = context_dict.get("ackID")
        if ack_id is None:
            ack_id = context_dict.get("ack_id")

        # Phase 3D: Streaming - seq is same in both camelCase and snake_case
        seq = context_dict.get("seq")

        return Context(
            id=ctx_id,
            action=context_dict.get("action"),
            event=context_dict.get("event"),
            parent_id=parent_id,
            params=context_dict.get("params"),
            meta=context_dict.get("meta"),
            stream=context_dict.get("stream", False),
            broker=self.broker,
            level=context_dict.get("level", 1),
            request_id=request_id if request_id else ctx_id,
            timeout=context_dict.get("timeout"),
            # Phase 3B: New fields
            node_id=node_id,
            caller=context_dict.get("caller"),
            tracing=context_dict.get("tracing"),
            # Phase 3C: Event Acknowledgment
            need_ack=need_ack,
            ack_id=ack_id,
            # Phase 3D: Streaming
            seq=seq,
        )
