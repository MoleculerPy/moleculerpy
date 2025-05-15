"""Context class for handling service requests and events in MoleculerPy framework.

The Context class provides the execution context for service actions and events,
including metadata handling and broker communication capabilities.

Key attributes for distributed tracing:
    - level: Call depth in the call chain (starts at 1, increments on nested calls)
    - request_id: Unique ID for the entire request chain (propagated through nested calls)
    - node_id: ID of the node where context was created (updated to endpoint node on remote)
    - caller: Full name of the service that initiated this call
    - tracing: Boolean flag indicating if request should be traced/sampled
    - span: Current trace span (local only, NOT serialized over network)
    - need_ack: Whether event requires acknowledgment from receiver
    - ack_id: Unique ID for correlating ACK responses

Streaming protocol fields (Phase 3D):
    - stream: Whether this is a streaming request/response
    - seq: Sequence number for ordered chunk delivery (0 = init, 1..N = data, final with stream=False)

Note on span serialization (Moleculer.js compatible):
    The span object is NOT transmitted over the network. Only requestID (= traceID),
    parentID, and tracing flag are serialized. Each node recreates its own Span
    using these fields to maintain distributed trace correlation.
"""

from __future__ import annotations

import weakref
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypeGuard, cast
from weakref import ReferenceType

from .domain_types import (
    ActionName,
    ContextID,
    EventName,
    NodeID,
    RequestID,
    ServiceName,
)

if TYPE_CHECKING:
    from .broker import ServiceBroker
    from .service import Service
    from .tracing import Span

ContextKind = Literal["action", "event", "internal"]


class ActionContextProtocol(Protocol):
    """Typed view of a context containing an action call."""

    action: ActionName
    event: None


class EventContextProtocol(Protocol):
    """Typed view of a context containing an event emission."""

    action: None
    event: EventName


class Context:
    """Represents the execution context for a service action or event.

    The Context contains all relevant information about a request including
    parameters, metadata, tracing information, and provides methods to
    call other services or emit events.

    Attributes:
        id: Unique identifier for this specific context
        level: Call depth in the call chain (1 = root, 2 = first nested call, etc.)
        request_id: Unique ID for the entire request chain (same across nested calls)
        node_id: ID of the node where context was created/executed
        caller: Full name of the calling service (e.g., "v2.users", "graph.extract")
        tracing: Whether this request should be traced/sampled (None = inherit)
        span: Current trace span object (local only, NOT serialized)
        action: Name of the action being executed
        event: Name of the event being handled
        params: Parameters passed to the action/event
        meta: Metadata associated with the request
        parent_id: ID of the parent context (for nested calls)
        stream: Whether this is a streaming context
        seq: Sequence number for streaming (0 = init, 1..N = data chunks)
        service: Reference to the service handling this context
        need_ack: Whether event requires acknowledgment (for reliable event delivery)
        ack_id: Unique ID for correlating ACK responses with original events
        cached_result: Whether the result was served from cache (set by CachingMiddleware)
    """

    __slots__ = (
        "_broker",
        "_extra",
        "_params",
        "_span_stack",
        "ack_id",
        "action",
        "cached_result",
        "caller",
        "event",
        "fallback_result",
        "id",
        "level",
        "meta",
        "need_ack",
        "node_id",
        "options",
        "params",
        "parent_id",
        "request_id",
        "seq",
        "service",
        "span",
        "stream",
        "timeout",
        "tracing",
    )

    _broker: ReferenceType[ServiceBroker] | None
    _extra: dict[str, Any] | None
    _params: dict[str, Any] | None
    _span_stack: list[Span]
    ack_id: str | None
    action: ActionName | None
    cached_result: bool
    caller: ServiceName | None
    event: EventName | None
    fallback_result: bool
    id: ContextID
    level: int
    meta: dict[str, Any]
    need_ack: bool | None
    node_id: NodeID | None
    options: Any | None
    params: dict[str, Any]
    parent_id: str | None
    request_id: RequestID
    seq: int | None
    service: Service | None
    span: Span | None
    stream: bool
    timeout: float | None
    tracing: bool | None

    def __init__(
        self,
        id: ContextID | str,
        action: ActionName | str | None = None,
        event: EventName | str | None = None,
        parent_id: str | None = None,
        params: dict[str, Any] | None = None,
        meta: dict[str, Any] | None = None,
        stream: bool = False,
        broker: ServiceBroker | None = None,
        level: int = 1,
        request_id: RequestID | str | None = None,
        timeout: float | None = None,
        # Phase 3B: New fields for distributed tracing
        node_id: NodeID | str | None = None,
        caller: ServiceName | str | None = None,
        tracing: bool | None = None,
        service: Service | None = None,
        # Phase 3C: Event Acknowledgment fields
        need_ack: bool | None = None,
        ack_id: str | None = None,
        # Phase 3D: Streaming protocol fields
        seq: int | None = None,
        # Tracing span (local only, NOT serialized - Moleculer.js compatible)
        span: Span | None = None,
        # Caching: indicates if result was served from cache
        cached_result: bool = False,
    ) -> None:
        """Initialize a new Context instance.

        Args:
            id: Unique identifier for this context
            action: Name of the action being executed
            event: Name of the event being handled
            parent_id: ID of the parent context (for nested calls)
            params: Parameters passed to the action/event
            meta: Metadata associated with the request
            stream: Whether this is a streaming context
            broker: Reference to the service broker
            level: Call depth in the call chain (default: 1 for root calls)
            request_id: Request chain ID (defaults to context id for root calls)
            timeout: Request timeout in seconds (optional, for per-call override)
            node_id: ID of the node where context was created
            caller: Full name of the calling service
            tracing: Whether to trace this request (None = inherit from parent)
            service: Reference to the service handling this context
            need_ack: Whether event requires acknowledgment from receiver
            ack_id: Unique ID for correlating ACK responses
            seq: Sequence number for streaming (0 = init, 1..N = data chunks)
            span: Current trace span (local only, NOT serialized over network)
        """
        set_attr = object.__setattr__
        context_id = ContextID(str(id))
        set_attr(self, "id", context_id)
        # Validate level is positive (1 = root call, 2+ = nested)
        if level < 1:
            raise ValueError(f"Context level must be >= 1, got {level}")
        set_attr(self, "level", level)
        resolved_request_id = request_id if request_id is not None else context_id
        set_attr(self, "request_id", RequestID(str(resolved_request_id)))
        set_attr(self, "action", ActionName(action) if action is not None else None)
        set_attr(self, "event", EventName(event) if event is not None else None)
        set_attr(self, "params", params or {})
        set_attr(self, "meta", meta or {})
        set_attr(self, "parent_id", parent_id)
        set_attr(self, "stream", stream)
        set_attr(self, "timeout", timeout)
        set_attr(self, "_broker", weakref.ref(broker) if broker is not None else None)

        # Phase 3B: Distributed tracing fields
        # node_id defaults to broker's node ID if available
        resolved_node_id = node_id if node_id is not None else (broker.nodeID if broker else None)
        set_attr(
            self,
            "node_id",
            NodeID(resolved_node_id) if resolved_node_id is not None else None,
        )
        set_attr(self, "caller", ServiceName(caller) if caller is not None else None)
        set_attr(self, "tracing", tracing)
        set_attr(self, "service", service)

        # Phase 3C: Event Acknowledgment fields
        set_attr(self, "need_ack", need_ack)
        set_attr(self, "ack_id", ack_id)

        # Phase 3D: Streaming protocol fields
        set_attr(self, "seq", seq)

        # Tracing span fields (local only, NOT serialized - Moleculer.js compatible)
        # span: Current active span for this context
        # _span_stack: Stack for nested span support (startSpan/finishSpan)
        set_attr(self, "span", span)
        set_attr(self, "_span_stack", [])

        # Caching: flag indicating if result was served from cache
        # Set to True by CachingMiddleware on cache hit
        set_attr(self, "cached_result", cached_result)

        # Additional optional fields used by middlewares
        set_attr(self, "options", None)
        set_attr(self, "fallback_result", False)
        set_attr(self, "_params", None)
        set_attr(self, "_extra", None)

    def __setattr__(self, name: str, value: Any) -> None:
        """Allow extension attributes while keeping fixed slots for core fields."""
        if name in self.__slots__:
            object.__setattr__(self, name, value)
            return

        try:
            extra = object.__getattribute__(self, "_extra")
        except AttributeError:
            extra = None
        if extra is None:
            extra = {}
            object.__setattr__(self, "_extra", extra)
        extra[name] = value

    def __getattr__(self, name: str) -> Any:
        """Resolve extension attributes stored in the extra dictionary."""
        try:
            extra = object.__getattribute__(self, "_extra")
        except AttributeError:
            extra = None
        if extra is not None and name in extra:
            return extra[name]
        raise AttributeError(f"{self.__class__.__name__!s} has no attribute {name!r}")

    @property
    def broker(self) -> ServiceBroker | None:
        """Get the service broker instance (dereferences weakref).

        Returns:
            The service broker instance or None if not set / already GC'd
        """
        return self._broker() if self._broker is not None else None

    @property
    def kind(self) -> ContextKind:
        """Return a typed discriminator for context intent."""
        if self.action is not None and self.event is None:
            return "action"
        if self.event is not None and self.action is None:
            return "event"
        return "internal"

    def unmarshall(self) -> dict[str, Any]:
        """Convert context to dictionary format for serialization.

        Returns:
            Dictionary representation of the context with all tracing fields
        """
        return {
            "id": self.id,
            "action": self.action,
            "event": self.event,
            "params": self.params,
            "meta": self.meta,
            "timeout": self.timeout or 0,
            "level": self.level,
            "requestID": self.request_id,
            "tracing": self.tracing,
            "parentID": self.parent_id,
            "stream": self.stream,
            # Phase 3B: New fields
            "nodeID": self.node_id,
            "caller": self.caller,
            # Phase 3C: Event Acknowledgment
            "needAck": self.need_ack,
            "ackID": self.ack_id,
            # Phase 3D: Streaming
            "seq": self.seq,
        }

    def marshall(self) -> dict[str, Any]:
        """Convert context to dictionary format for serialization.

        Note: This method is identical to unmarshall() and is kept for compatibility.

        Returns:
            Dictionary representation of the context
        """
        return self.unmarshall()

    def _prepare_meta(self, meta: dict[str, Any] | None = None) -> dict[str, Any]:
        """Merge context metadata with provided metadata.

        Note: This is a sync method (no I/O) for performance.
        Matches Moleculer.js mergeMeta() which uses Object.assign().

        Args:
            meta: Additional metadata to merge

        Returns:
            Merged metadata dictionary
        """
        if meta is None:
            return self.meta.copy()  # Avoid mutation, fast path for no meta
        return {**self.meta, **meta}

    async def call(
        self,
        service_name: str,
        params: dict[str, Any] | None = None,
        meta: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Call another service action, propagating context chain.

        This method passes the current context as parent, enabling:
        - Level tracking (level increments for each nested call)
        - Request ID propagation (same request_id across the call chain)
        - Max call level protection

        Args:
            service_name: Name of the service action to call
            params: Parameters to pass to the service
            meta: Additional metadata for the call
            timeout: Optional per-call timeout override

        Returns:
            Result from the service call

        Raises:
            AttributeError: If broker is not available
            MaxCallLevelError: If max call level is exceeded
        """
        broker = self.broker
        if broker is None:
            raise AttributeError("Broker not available in context")

        if params is None:
            params = {}

        merged_meta = self._prepare_meta(meta)
        return await broker.call(
            service_name,
            params,
            merged_meta,
            parent_ctx=self,
            timeout=timeout,
        )

    async def emit(
        self,
        service_name: str,
        params: dict[str, Any] | None = None,
        meta: dict[str, Any] | None = None,
    ) -> Any:
        """Emit an event to services.

        Note: Unlike call(), emit() does not propagate level/request_id
        because events are fire-and-forget and not part of the call chain.

        Args:
            service_name: Name of the event to emit
            params: Parameters to pass with the event
            meta: Additional metadata for the event

        Returns:
            Result from the emit operation

        Raises:
            AttributeError: If broker is not available
        """
        broker = self.broker
        if broker is None:
            raise AttributeError("Broker not available in context")

        if params is None:
            params = {}

        merged_meta = self._prepare_meta(meta)
        return await broker.emit(service_name, params, merged_meta)

    async def broadcast(
        self,
        service_name: str,
        params: dict[str, Any] | None = None,
        meta: dict[str, Any] | None = None,
    ) -> Any:
        """Broadcast an event to all services.

        Note: Unlike call(), broadcast() does not propagate level/request_id
        because events are fire-and-forget and not part of the call chain.

        Args:
            service_name: Name of the event to broadcast
            params: Parameters to pass with the event
            meta: Additional metadata for the event

        Returns:
            Result from the broadcast operation

        Raises:
            AttributeError: If broker is not available
        """
        broker = self.broker
        if broker is None:
            raise AttributeError("Broker not available in context")

        if params is None:
            params = {}

        merged_meta = self._prepare_meta(meta)
        return await broker.broadcast(service_name, params, merged_meta)

    # =========================================================================
    # Tracing Methods
    # =========================================================================

    def start_span(
        self,
        name: str,
        opts: dict[str, Any] | None = None,
    ) -> Span | None:
        """Create and start a child span for this context.

        Creates a new span as a child of the current span (if any).
        Pushes the current span onto the stack and sets the new span as current.

        This enables nested span tracking:

            span1 = ctx.start_span("db.query")
            # ... do work ...
            span2 = ctx.start_span("cache.lookup")
            # ... nested work ...
            ctx.finish_span()  # finishes span2
            ctx.finish_span()  # finishes span1

        Args:
            name: Human-readable name for the operation
            opts: Optional span configuration:
                - type: Span type ("custom", "action", "event")
                - tags: Initial tags dict
                - service: Service metadata dict

        Returns:
            The started Span, or None if tracing is disabled

        Example:
            span = ctx.start_span("db.query", {
                "type": "custom",
                "tags": {"query": "SELECT * FROM users"}
            })
            try:
                result = execute_query()
                span.add_tags({"rows": len(result)})
            except Exception as e:
                span.set_error(e)
            finally:
                ctx.finish_span()
        """
        # Check if tracing is enabled and broker has tracer
        broker = self.broker
        if not broker or not hasattr(broker, "tracer"):
            return None

        tracer = getattr(broker, "tracer", None)
        if not tracer or not tracer.is_enabled():
            return None

        # Build span options with context correlation
        span_opts: dict[str, Any] = {
            "trace_id": self.request_id,  # Use request_id as trace_id
            **(opts or {}),
        }

        # If we have a current span, use it as parent
        if self.span:
            span_opts["parent_id"] = self.span.id
        elif self.parent_id:
            span_opts["parent_id"] = self.parent_id

        # Add service info from context
        if self.service and "service" not in span_opts:
            span_opts["service"] = {
                "name": self.service.name,
                "version": getattr(self.service, "version", None),
                "node_id": self.node_id,
            }

        # Create and start the span
        new_span = cast("Span", tracer.start_span(name, span_opts))

        # Push current span to stack and set new as current
        if self.span:
            self._span_stack.append(self.span)
        self.span = new_span

        return new_span

    def finish_span(self) -> Span | None:
        """Finish the current span and restore the previous one.

        Finishes the current span and pops the previous span from the stack.

        Returns:
            The finished Span, or None if no span was active

        Example:
            span = ctx.start_span("operation")
            try:
                # ... do work ...
                pass
            finally:
                ctx.finish_span()
        """
        if not self.span:
            return None

        # Finish the current span
        finished_span = self.span
        finished_span.finish()

        # Restore previous span from stack
        if self._span_stack:
            self.span = self._span_stack.pop()
        else:
            self.span = None

        return finished_span

    def current_span(self) -> Span | None:
        """Get the current active span.

        Returns:
            The current Span or None if no span is active
        """
        return self.span


def is_action_context(ctx: Context) -> TypeGuard[ActionContextProtocol]:
    """Type guard for contexts representing action calls."""
    return ctx.action is not None and ctx.event is None


def is_event_context(ctx: Context) -> TypeGuard[EventContextProtocol]:
    """Type guard for contexts representing event emissions."""
    return ctx.event is not None and ctx.action is None
