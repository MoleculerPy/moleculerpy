"""Transit layer for message handling and communication in MoleculerPy framework.

This module provides the Transit class which handles message routing, node discovery,
and inter-service communication through various transporter implementations.

Internal events emitted (Moleculer.js compatible):
- $transporter.connected: When transporter establishes connection
- $transporter.disconnected: When transporter disconnects (graceful or not)
- $transporter.error: When transporter encounters an error
"""

import asyncio
import time
import traceback
from typing import TYPE_CHECKING, Any, cast

from .metrics import MetricsCollector, get_static_metrics

if TYPE_CHECKING:
    from .context import Context
    from .lifecycle import Lifecycle
    from .node import NodeCatalog
    from .registry import Action, Event, Registry
    from .settings import Settings

from .errors import MoleculerError, RemoteCallError
from .packet import Packet, Topic
from .stream import AsyncStream, PendingStream, StreamChunk, StreamMetadata, StreamState
from .transporter.base import Transporter
from .validator import ValidationError, validate_params

# Default timeout for event acknowledgments (seconds)
DEFAULT_ACK_TIMEOUT: float = 5.0


class Transit:
    """Handles message routing and communication between MoleculerPy nodes.

    The Transit layer is responsible for:
    - Establishing connections through transporters
    - Routing messages between nodes
    - Handling service discovery and heartbeats
    - Managing request/response cycles
    - Processing events
    """

    def __init__(
        self,
        node_id: str,
        registry: "Registry",
        node_catalog: "NodeCatalog",
        settings: "Settings",
        logger: Any,
        lifecycle: "Lifecycle",
    ) -> None:
        """Initialize the Transit layer.

        Args:
            node_id: Unique identifier for this node
            registry: Service registry for action/event lookup
            node_catalog: Catalog of known nodes in the cluster
            settings: Configuration settings
            logger: Logger instance
            lifecycle: Context lifecycle manager
        """
        self.node_id = node_id
        self.registry = registry
        self.node_catalog = node_catalog
        self.settings = settings
        self.logger = logger
        self.lifecycle = lifecycle

        # Initialize transporter based on settings
        transporter_name = settings.transporter.split("://")[0]
        self.transporter: Transporter = Transporter.get_by_name(
            transporter_name,
            {"connection": settings.transporter},
            transit=self,
            handler=self._message_handler,
            node_id=node_id,
        )

        # Track pending requests for timeout handling
        self._pending_requests: dict[str, asyncio.Future[dict[str, Any]]] = {}

        # Phase 3C: Track pending event acknowledgments
        self._pending_acks: dict[str, asyncio.Future[dict[str, Any]]] = {}

        # Phase 3D: Track pending streams (for streaming responses)
        self._pending_streams: dict[str, PendingStream] = {}

        # Pre-build handler dispatch table (perf: avoid dict creation per message)
        self._packet_handlers = {
            Topic.INFO: self._handle_info,
            Topic.DISCOVER: self._handle_discover,
            Topic.HEARTBEAT: self._handle_heartbeat,
            Topic.REQUEST: self._handle_request,
            Topic.RESPONSE: self._handle_response,
            Topic.EVENT: self._handle_event,
            Topic.EVENT_ACK: self._handle_event_ack,  # Phase 3C: Event Ack handler
            Topic.DISCONNECT: self._handle_disconnect,
            # Phase 5: Latency measurement
            Topic.PING: self._handle_ping,
            Topic.PONG: self._handle_pong,
        }

        # Phase 5: Latency monitor reference (set by broker)
        self._latency_monitor: Any = None

        # Phase 5.1: Metrics collector for CPU/memory tracking
        self._metrics_collector = MetricsCollector()

        # Track connection state for $transporter.* events
        self._was_connected: bool = False

        # Broker reference for middleware wrapping (set by broker after creation)
        self._broker: Any = None

        # Wrapped publish method (set by _wrap_methods)
        self._wrapped_publish: Any = None

    def _emit_transporter_event(self, event: str, payload: dict[str, Any]) -> None:
        """Emit a transporter internal event via broker (fire-and-forget).

        Args:
            event: Event name (e.g., "$transporter.connected")
            payload: Event payload
        """
        broker = getattr(self.node_catalog, "broker", None)
        if broker is not None:
            result = broker.broadcast_local(event, payload)
            # Only schedule if it's actually a coroutine (handles mock cases in tests)
            if asyncio.iscoroutine(result):
                task = asyncio.create_task(result)
                task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)

    def _wrap_methods(self, broker: Any) -> None:
        """Wrap transit and transporter methods with middleware hooks.

        Called by broker during start() after middleware is ready.
        Follows Moleculer.js pattern: this.publish = broker.wrapMethod("transitPublish", ...)

        Args:
            broker: The ServiceBroker instance
        """
        from .middleware.base import Middleware  # noqa: PLC0415

        self._broker = broker

        # =================================================================
        # 1. Wrap transit.publish with transitPublish hooks (Packet level)
        # =================================================================
        async def publish_impl(packet: Packet) -> None:
            await self.transporter.publish(packet)

        current_publish = publish_impl
        for middleware in reversed(broker.middlewares):
            hook = getattr(middleware, "transit_publish", None)
            if hook is not None and callable(hook):
                if type(middleware).transit_publish is not Middleware.transit_publish:
                    outer_publish = current_publish
                    mw = middleware

                    async def wrapped_publish(
                        packet: Packet,
                        _next: Any = outer_publish,
                        _mw: Any = mw,
                    ) -> None:
                        await _mw.transit_publish(_next, packet)

                    current_publish = wrapped_publish

        self._wrapped_publish = current_publish
        self.logger.debug("Transit publish wrapped with middleware hooks")

        # =================================================================
        # 2. Wrap transporter.send with transporterSend hooks (byte level)
        # =================================================================
        async def send_impl(topic: str, data: bytes, meta: dict[str, Any]) -> None:
            await self.transporter.send(topic, data, meta)

        current_send = send_impl
        for middleware in reversed(broker.middlewares):
            hook = getattr(middleware, "transporter_send", None)
            if hook is not None and callable(hook):
                if type(middleware).transporter_send is not Middleware.transporter_send:
                    # Get wrapped function from middleware
                    wrapped_fn = hook(current_send)
                    current_send = wrapped_fn

        self.transporter._wrapped_send = current_send
        self.logger.debug("Transporter send wrapped with middleware hooks")

        # =================================================================
        # 3. Wrap transporter.receive with transporterReceive hooks
        #    NOTE: Moleculer.js uses { reverse: true } for receive, meaning
        #    middleware order is reversed (first added = last to process)
        # =================================================================
        async def receive_impl(cmd: str, data: bytes, meta: dict[str, Any]) -> None:
            await self.transporter.receive(cmd, data, meta)

        current_receive = receive_impl
        # Normal order (not reversed) because the hook itself is for "receive"
        # and middleware is applied in the order: decompression before decryption
        for middleware in broker.middlewares:
            hook = getattr(middleware, "transporter_receive", None)
            if hook is not None and callable(hook):
                if type(middleware).transporter_receive is not Middleware.transporter_receive:
                    # Get wrapped function from middleware
                    wrapped_fn = hook(current_receive)
                    current_receive = wrapped_fn

        self.transporter._wrapped_receive = current_receive
        self.logger.debug("Transporter receive wrapped with middleware hooks")

    async def _message_handler(self, packet: Packet) -> None:
        """Handle incoming packets based on their type.

        Args:
            packet: Incoming packet to process
        """
        handler = self._packet_handlers.get(packet.type)
        if handler:
            try:
                await handler(packet)
            except Exception as e:
                self.logger.error(f"Error handling {packet.type.value} packet: {e}")
        else:
            self.logger.warning(f"Unknown packet type: {packet.type}")

    async def _make_subscriptions(self) -> None:
        """Subscribe to all necessary topics for this node."""
        subscriptions = [
            (Topic.INFO.value, None),
            (Topic.INFO.value, self.node_id),
            (Topic.DISCOVER.value, None),
            (Topic.HEARTBEAT.value, None),
            (Topic.REQUEST.value, self.node_id),
            (Topic.RESPONSE.value, self.node_id),
            (Topic.EVENT.value, self.node_id),
            (Topic.EVENT_ACK.value, self.node_id),  # Phase 3C: Event Ack subscription
            (Topic.DISCONNECT.value, None),
            # Phase 5: Latency measurement subscriptions
            (Topic.PING.value, self.node_id),
            (Topic.PONG.value, self.node_id),
        ]

        for topic, node_id in subscriptions:
            await self.transporter.subscribe(topic, node_id)

    async def connect(self) -> None:
        """Establish connection and initialize the node in the cluster.

        Emits $transporter.connected internal event (Moleculer.js compatible).
        """
        was_reconnect = self._was_connected
        await self.transporter.connect()
        await self.discover()
        await self.send_node_info()
        await self._make_subscriptions()

        # Mark as connected and emit event
        self._was_connected = True
        self._emit_transporter_event(
            "$transporter.connected",
            {
                "wasReconnect": was_reconnect,
            },
        )

        self.logger.info(f"Transit connected for node {self.node_id}")

    async def disconnect(self) -> None:
        """Gracefully disconnect from the cluster."""
        # Notify other nodes of disconnection
        try:
            await self.publish(Packet(Topic.DISCONNECT, None, {}))
        except Exception as e:
            self.logger.warning(f"Error sending disconnect notification: {e}")

        # Cancel all pending requests
        # Copy keys to avoid "dictionary changed size during iteration"
        for req_id in list(self._pending_requests.keys()):
            future = self._pending_requests.pop(req_id, None)
            if future and not future.done():
                future.cancel()

        # Phase 3C: Cancel all pending event acks
        for ack_id in list(self._pending_acks.keys()):
            future = self._pending_acks.pop(ack_id, None)
            if future and not future.done():
                future.cancel()

        # Phase 3D: Cancel all pending streams
        for stream_id in list(self._pending_streams.keys()):
            pending = self._pending_streams.pop(stream_id, None)
            if pending and pending.state == StreamState.ACTIVE:
                pending.state = StreamState.CANCELLED
                # Signal end to any waiting consumers
                await pending.queue.put(
                    StreamChunk(
                        seq=-1, data=asyncio.CancelledError("Stream cancelled"), is_final=True
                    )
                )

        # Disconnect transporter
        await self.transporter.disconnect()

        # Emit $transporter.disconnected event (Moleculer.js compatible)
        self._emit_transporter_event(
            "$transporter.disconnected",
            {
                "graceful": True,
            },
        )

        self.logger.info(f"Transit disconnected for node {self.node_id}")

    async def publish(self, packet: Packet) -> None:
        """Publish a packet through the transporter.

        If middleware defines transit_publish hooks, they are applied.

        Args:
            packet: Packet to publish
        """
        # Use wrapped publish if available (set by _wrap_methods)
        if self._wrapped_publish is not None:
            await self._wrapped_publish(packet)
        else:
            # Fallback to direct publish (before broker.start() or no middleware)
            await self.transporter.publish(packet)

    async def discover(self) -> None:
        """Send a discovery request to find other nodes in the cluster."""
        await self.publish(Packet(Topic.DISCOVER, None, {}))

    async def beat(self) -> None:
        """Send a heartbeat with current node metrics.

        Moleculer.js compatible:
        - cpu: CPU usage percentage (0-100, int)
        - cpuSeq: Sequence number that increments when CPU changes

        Python extensions:
        - memory: Memory usage percentage
        """
        # Collect metrics using MetricsCollector (handles cpuSeq tracking)
        metrics = await self._metrics_collector.collect()

        # Update local node metrics
        local_node = self.node_catalog.local_node
        if local_node:
            local_node.cpu = int(metrics["cpu"])
            local_node.cpuSeq = int(metrics["cpuSeq"])
            local_node.memory = float(metrics["memory"])
            local_node.lastHeartbeatTime = time.time()

            # Set static metrics on first heartbeat
            if local_node.cpuCoresLogical == 1:
                static = get_static_metrics()
                local_node.cpuCoresLogical = static["cpu_cores_logical"]
                local_node.cpuCoresPhysical = static["cpu_cores_physical"]
                local_node.hostname = static["hostname"]
                local_node.ipList = static["ip_list"]

        heartbeat_data = {
            "cpu": metrics["cpu"],
            "cpuSeq": metrics["cpuSeq"],
            "memory": metrics["memory"],  # Python extension
        }
        await self.publish(Packet(Topic.HEARTBEAT, None, heartbeat_data))

    async def send_node_info(self) -> None:
        """Send current node information to the cluster."""
        if self.node_catalog.local_node is None:
            self.logger.error("Local node is not initialized")
            return

        node_info = self.node_catalog.local_node.get_info()
        await self.publish(Packet(Topic.INFO, None, node_info))

    async def _handle_discover(self, packet: Packet) -> None:
        """Handle discovery requests by sending node info.

        Args:
            packet: Discovery packet
        """
        await self.send_node_info()

    async def _handle_heartbeat(self, packet: Packet) -> None:
        """Handle heartbeat packets from other nodes.

        Updates node metrics from heartbeat:
        - cpu: CPU usage percentage (Moleculer.js compatible)
        - cpuSeq: Sequence number (Moleculer.js compatible)
        - memory: Memory usage percentage (Python extension)
        - lastHeartbeatTime: Timestamp for health monitoring

        Args:
            packet: Heartbeat packet
        """
        if not packet.sender:
            return

        node = self.node_catalog.get_node(packet.sender)
        if node:
            # Update Moleculer.js compatible metrics
            node.cpu = packet.payload.get("cpu", 0)
            node.cpuSeq = packet.payload.get("cpuSeq", 0)

            # Update Python extension metrics
            node.memory = packet.payload.get("memory", 0.0)

            # Update timestamp for health monitoring
            node.lastHeartbeatTime = time.time()

    async def _handle_info(self, packet: Packet) -> None:
        """Handle node info packets.

        Args:
            packet: Info packet containing node details
        """
        if not packet.payload or not packet.sender:
            return

        # Route INFO updates through NodeCatalog to avoid duplicate registrations.
        allowed_fields = {
            "id",
            "available",
            "local",
            "services",
            "cpu",
            "client",
            "ipList",
            "hostname",
            "config",
            "instanceID",
            "metadata",
            "seq",
            "ver",
            "sender",
            # Phase 5.1: extended metrics
            "cpuSeq",
            "memory",
            "lastHeartbeatTime",
            "cpuCoresLogical",
            "cpuCoresPhysical",
        }
        payload = {k: v for k, v in packet.payload.items() if k in allowed_fields}
        payload.setdefault("id", packet.sender)
        self.node_catalog.process_node_info(packet.sender, payload)

    async def _handle_disconnect(self, packet: Packet) -> None:
        """Handle node disconnection notifications.

        Args:
            packet: Disconnect packet
        """
        if packet.sender:
            self.node_catalog.disconnect_node(packet.sender)

    async def _handle_ping(self, packet: Packet) -> None:
        """Handle PING packets for latency measurement.

        Phase 5: Latency measurement protocol.
        Responds with PONG containing the original timestamp.

        Args:
            packet: PING packet
        """
        if not packet.sender:
            return

        if self._latency_monitor:
            await self._latency_monitor.handle_ping(packet.sender, packet.payload)

    async def _handle_pong(self, packet: Packet) -> None:
        """Handle PONG packets for latency measurement.

        Phase 5: Latency measurement protocol.
        Calculates round-trip time and updates latency records.

        Args:
            packet: PONG packet
        """
        if not packet.sender:
            return

        if self._latency_monitor:
            await self._latency_monitor.handle_pong(packet.sender, packet.payload)

    async def _handle_event(self, packet: Packet) -> None:
        """Handle event packets.

        Processes incoming events and sends ACK responses when required.

        Args:
            packet: Event packet
        """
        event_name = packet.payload.get("event")
        if not event_name:
            self.logger.warning("Received event packet without event name")
            return

        endpoint = self.registry.get_event(event_name)
        if endpoint and endpoint.is_local and endpoint.handler:
            context = self.lifecycle.rebuild_context(packet.payload)
            success = True
            error_msg: str | None = None

            try:
                await endpoint.handler(context)
            except Exception as e:
                success = False
                error_msg = str(e)
                self.logger.error(f"Failed to process event {endpoint.name}: {e}")

            # Phase 3C: Send ACK if requested
            if context.need_ack and packet.sender:
                ack_id = context.ack_id or context.id
                await self._send_event_ack(
                    target=packet.sender,
                    ack_id=ack_id,
                    success=success,
                    error=error_msg,
                )

    async def _send_event_ack(
        self,
        target: str,
        ack_id: str,
        success: bool = True,
        error: str | None = None,
    ) -> None:
        """Send an event acknowledgment response.

        Phase 3C: Event Acknowledgment implementation.

        Args:
            target: Node ID to send ACK to
            ack_id: ID of the event being acknowledged
            success: Whether the event was processed successfully
            error: Error message if processing failed
        """
        ack_payload = {
            "ackID": ack_id,
            "success": success,
            "sender": self.node_id,
        }
        if error:
            ack_payload["error"] = error

        # Use % formatting for lazy evaluation (avoid f-string overhead if DEBUG disabled)
        self.logger.debug("Sending EVENT_ACK for %s to %s (success=%s)", ack_id, target, success)
        await self.publish(Packet(Topic.EVENT_ACK, target, ack_payload))

    async def _handle_event_ack(self, packet: Packet) -> None:
        """Handle event acknowledgment packets.

        Phase 3C: Resolves pending ACK futures when ACK is received.

        Args:
            packet: EVENT_ACK packet
        """
        ack_id = packet.payload.get("ackID")
        if not ack_id:
            self.logger.warning("Received EVENT_ACK without ackID")
            return

        future = self._pending_acks.pop(ack_id, None)
        if future and not future.done():
            self.logger.debug("Received EVENT_ACK for %s from %s", ack_id, packet.sender)
            future.set_result(packet.payload)
        else:
            self.logger.debug("Received EVENT_ACK for unknown/completed %s", ack_id)

    async def _handle_request(self, packet: Packet) -> None:
        """Handle service request packets.

        Args:
            packet: Request packet
        """
        action_name = packet.payload.get("action")
        if not action_name:
            self.logger.warning("Received request packet without action name")
            return

        endpoint = self.registry.get_action(action_name)
        if not endpoint or not endpoint.is_local:
            self.logger.warning(f"No local handler for action: {action_name}")
            return

        context = self.lifecycle.rebuild_context(packet.payload)

        try:
            # Validate parameters if schema is defined
            if endpoint.params_schema:
                try:
                    validate_params(context.params, endpoint.params_schema)
                except ValidationError:
                    raise  # Re-raise validation errors

            # Execute the action handler
            if not endpoint.handler:
                raise Exception(f"No handler defined for action {action_name}")

            result = await endpoint.handler(context)
            response = {"id": context.id, "data": result, "success": True, "meta": context.meta}

        except Exception as e:
            self.logger.error(f"Failed call to {endpoint.name}: {e}")

            # Serialize error using MoleculerError format if available
            if isinstance(e, MoleculerError):
                error_data = e.to_dict()
                error_data["stack"] = traceback.format_exc()
            else:
                # Determine if error is retryable (transient network/IO errors)
                retryable = isinstance(
                    e,
                    (
                        ConnectionError,
                        OSError,
                        asyncio.TimeoutError,
                    ),
                )
                error_data = {
                    "name": e.__class__.__name__,
                    "message": str(e),
                    "code": 500,
                    "type": "INTERNAL_ERROR",
                    "retryable": retryable,
                    "stack": traceback.format_exc(),
                }

            response = {
                "id": context.id,
                "error": error_data,
                "success": False,
                "meta": context.meta,
            }

        # Send response back to the caller
        await self.publish(Packet(Topic.RESPONSE, packet.sender, response))

    async def _handle_response(self, packet: Packet) -> None:
        """Handle response packets for pending requests.

        Routes to stream handler if stream flag is present.

        Args:
            packet: Response packet
        """
        req_id = packet.payload.get("id")
        if not req_id:
            return

        # Phase 3D: Check if this is a streaming response
        if packet.payload.get("stream"):
            await self._handle_response_stream(packet)
            return

        future = self._pending_requests.pop(req_id, None)
        if future and not future.done():
            future.set_result(packet.payload)

    async def _handle_response_stream(self, packet: Packet) -> None:
        """Handle streaming response packets.

        Phase 3D: Streaming protocol implementation.
        Handles incoming stream chunks and manages out-of-order delivery.

        Protocol:
            - seq=0: Stream initialization
            - seq=1..N: Data chunks
            - stream=false: Final chunk (end marker)
            - $streamError in meta: Error during streaming

        Args:
            packet: Response packet with stream data
        """
        # Cache payload reference (perf: avoid repeated attribute access)
        payload = packet.payload
        req_id = payload.get("id")
        seq = payload.get("seq", 0)
        is_final = not payload.get("stream", True)
        data = payload.get("data")
        meta = payload.get("meta", {})

        self.logger.debug("Received stream chunk for %s: seq=%d, final=%s", req_id, seq, is_final)

        # Check for stream error
        stream_error = meta.get("$streamError")
        if stream_error:
            pending = self._pending_streams.get(req_id)
            if pending:
                pending.state = StreamState.ERROR
                error = Exception(stream_error.get("message", "Stream error"))
                await pending.queue.put(StreamChunk(seq=seq, data=error, is_final=True))
                self._pending_streams.pop(req_id, None)
            return

        # Get or create pending stream
        pending = self._pending_streams.get(req_id)
        if pending is None:
            # First chunk - check if we have a waiting request
            future = self._pending_requests.get(req_id)
            if future is None:
                self.logger.warning("Received stream chunk for unknown request: %s", req_id)
                return

            # Create pending stream
            object_mode = meta.get("$streamObjectMode", False)
            pending = PendingStream(
                metadata=StreamMetadata(context_id=req_id, object_mode=object_mode),
                state=StreamState.ACTIVE,
            )
            self._pending_streams[req_id] = pending

            # Create AsyncStream and resolve the future with it
            stream: AsyncStream[Any] = AsyncStream(
                context_id=req_id,
                queue=pending.queue,
                object_mode=object_mode,
                on_close=self._on_stream_closed,
            )
            self._pending_requests.pop(req_id, None)
            if not future.done():
                future.set_result({"stream": stream, "success": True})

        # Handle chunk delivery
        chunk = StreamChunk(seq=seq, data=data, is_final=is_final)

        # Check if this chunk is expected (in order)
        if seq == pending.next_seq:
            # Deliver this chunk and any buffered sequential chunks
            await pending.queue.put(chunk)
            pending.next_seq += 1

            # Check pool for sequential chunks
            while pending.next_seq in pending.pool:
                pooled = pending.pool.pop(pending.next_seq)
                await pending.queue.put(pooled)
                pending.next_seq += 1

        elif seq > pending.next_seq:
            # Out of order - buffer in pool (like Moleculer.js)
            self.logger.debug(
                "Buffering out-of-order chunk: expected=%d, got=%d", pending.next_seq, seq
            )
            pending.pool[seq] = chunk

        else:
            # Duplicate or stale chunk - ignore
            self.logger.debug("Ignoring stale chunk: expected=%d, got=%d", pending.next_seq, seq)

        # Clean up if final
        if is_final:
            pending.state = StreamState.COMPLETED
            self._pending_streams.pop(req_id, None)

    def _on_stream_closed(self, req_id: str) -> None:
        """Handle local stream close/cancel by cleaning pending stream buffers."""
        pending = self._pending_streams.pop(req_id, None)
        if pending is None:
            return

        if pending.state != StreamState.COMPLETED:
            pending.state = StreamState.CANCELLED
        pending.pool.clear()
        while True:
            try:
                pending.queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    async def request(
        self, endpoint: "Action", context: "Context", timeout: float | None = None
    ) -> Any:
        """Send a request to a remote service action.

        Args:
            endpoint: Action endpoint to call
            context: Request context
            timeout: Optional timeout override in seconds. If not specified,
                     uses endpoint timeout or settings.request_timeout.

        Returns:
            Response data from the remote service

        Raises:
            RemoteCallError: If the remote call fails
            asyncio.TimeoutError: If the request times out
        """
        req_id = context.id
        future = asyncio.get_running_loop().create_future()
        self._pending_requests[req_id] = future

        # Determine timeout: explicit > endpoint > settings
        request_timeout = timeout
        if request_timeout is None:
            request_timeout = getattr(endpoint, "timeout", None)
        if request_timeout is None:
            request_timeout = self.settings.request_timeout

        # Send the request
        await self.publish(Packet(Topic.REQUEST, endpoint.node_id, context.marshall()))

        try:
            response = await asyncio.wait_for(future, request_timeout)

            # Check if the response indicates an error
            if not response.get("success", True):
                error_data = response.get("error", {})
                error_msg = error_data.get("message", "Unknown error")
                error_name = error_data.get("name", "RemoteError")
                error_stack = error_data.get("stack")
                error_code = error_data.get("code", 500)
                error_type = error_data.get("type", "REMOTE_ERROR")
                error_retryable = error_data.get("retryable", True)

                if error_stack:
                    self.logger.error(f"Remote error stack: {error_stack}")

                # Try to deserialize as specific MoleculerError if possible
                if "name" in error_data and error_data["name"] != "RemoteError":
                    try:
                        deserialized = MoleculerError.from_dict(error_data)
                        # Only raise if we got a more specific error type
                        if type(deserialized) is not MoleculerError:
                            raise deserialized
                    except MoleculerError:
                        raise  # Re-raise the deserialized error
                    except Exception as deser_err:
                        # Log deserialization failure but continue to RemoteCallError
                        self.logger.debug(
                            "Failed to deserialize error as MoleculerError: %s", deser_err
                        )

                raise RemoteCallError(
                    message=error_msg,
                    error_name=error_name,
                    stack=error_stack,
                    code=error_code,
                    error_type=error_type,
                    retryable=error_retryable,
                )

            return response.get("data")

        except asyncio.CancelledError:
            # Propagate cancellation properly
            self.logger.debug("Request to %s was cancelled", endpoint.name)
            raise

        except TimeoutError:
            raise TimeoutError(
                f"Request to {endpoint.name} timed out after {request_timeout}s"
            ) from None

        finally:
            # Always clean up the pending request to prevent memory leaks
            self._pending_requests.pop(req_id, None)

    async def stream_request(
        self, endpoint: "Action", context: "Context", timeout: float | None = None
    ) -> AsyncStream[Any]:
        """Send a streaming request to a remote service action.

        Phase 3D: Streaming protocol implementation.

        This method sends a request and returns an AsyncStream that yields
        chunks as they arrive. Compatible with Moleculer.js streaming.

        Args:
            endpoint: Action endpoint to call
            context: Request context (stream flag will be set automatically)
            timeout: Optional timeout for first chunk. If not specified,
                     uses endpoint timeout or settings.request_timeout.

        Returns:
            AsyncStream that yields chunks as they arrive

        Raises:
            RemoteCallError: If the remote call fails
            asyncio.TimeoutError: If no response received within timeout

        Usage:
            stream = await transit.stream_request(endpoint, ctx)
            async for chunk in stream:
                process(chunk)
        """
        req_id = context.id
        context.stream = True  # Mark as streaming request

        future = asyncio.get_running_loop().create_future()
        self._pending_requests[req_id] = future

        # Determine timeout: explicit > endpoint > settings
        request_timeout = timeout
        if request_timeout is None:
            request_timeout = getattr(endpoint, "timeout", None)
        if request_timeout is None:
            request_timeout = self.settings.request_timeout

        # Send the request
        await self.publish(Packet(Topic.REQUEST, endpoint.node_id, context.marshall()))

        try:
            # Wait for first response (stream init or error)
            response = await asyncio.wait_for(future, request_timeout)

            # Check if we got a stream
            if "stream" in response and isinstance(response["stream"], AsyncStream):
                return response["stream"]

            # Check for error response
            if not response.get("success", True):
                error_data = response.get("error", {})
                raise RemoteCallError(
                    message=error_data.get("message", "Unknown error"),
                    error_name=error_data.get("name", "RemoteError"),
                    stack=error_data.get("stack"),
                    code=error_data.get("code", 500),
                    error_type=error_data.get("type", "REMOTE_ERROR"),
                    retryable=error_data.get("retryable", True),
                )

            # Unexpected non-stream response
            raise RemoteCallError(
                message="Expected streaming response but got regular response",
                error_name="StreamError",
                code=500,
                error_type="STREAM_ERROR",
                retryable=False,
            )

        except asyncio.CancelledError:
            self.logger.debug("Stream request to %s was cancelled", endpoint.name)
            # Clean up pending stream
            self._pending_streams.pop(req_id, None)
            raise

        except TimeoutError:
            # Clean up pending stream
            self._pending_streams.pop(req_id, None)
            raise TimeoutError(
                f"Stream request to {endpoint.name} timed out after {request_timeout}s"
            ) from None

        finally:
            # Clean up pending request (stream is tracked separately now)
            self._pending_requests.pop(req_id, None)

    async def send_event(
        self,
        endpoint: "Event",
        context: "Context",
        marshalled_context: dict[str, Any] | None = None,
    ) -> None:
        """Send an event to a remote service.

        Args:
            endpoint: Event endpoint to send to
            context: Event context
            marshalled_context: Optional pre-marshalled context payload
        """
        payload = marshalled_context if marshalled_context is not None else context.marshall()
        await self.publish(Packet(Topic.EVENT, endpoint.node_id, payload))

    async def send_event_with_ack(
        self,
        endpoint: "Event",
        context: "Context",
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Send an event and wait for acknowledgment.

        Phase 3C: Reliable event delivery with acknowledgment.

        This method sends an event with needAck=true and waits for the receiver
        to send back an EVENT_ACK packet confirming processing.

        Args:
            endpoint: Event endpoint to send to
            context: Event context (need_ack will be set automatically)
            timeout: Timeout for ACK in seconds (default: settings.ack_timeout or 5.0)

        Returns:
            ACK response payload containing:
            - ackID: ID of the acknowledged event
            - success: Whether event was processed successfully
            - error: Error message if processing failed (optional)

        Raises:
            asyncio.TimeoutError: If ACK is not received within timeout
            asyncio.CancelledError: If the operation is cancelled
        """
        # Set up ACK tracking
        ack_id = context.ack_id or context.id
        context.need_ack = True
        context.ack_id = ack_id

        future = asyncio.get_running_loop().create_future()
        self._pending_acks[ack_id] = future

        # Determine timeout
        ack_timeout = timeout
        if ack_timeout is None:
            ack_timeout = getattr(self.settings, "ack_timeout", DEFAULT_ACK_TIMEOUT)

        try:
            # Send the event
            self.logger.debug("Sending event %s with ACK (id=%s)", context.event, ack_id)
            await self.publish(Packet(Topic.EVENT, endpoint.node_id, context.marshall()))

            # Wait for ACK
            response = await asyncio.wait_for(future, ack_timeout)
            return cast(dict[str, Any], response)

        except asyncio.CancelledError:
            self.logger.debug("Event ACK for %s was cancelled", ack_id)
            raise

        except TimeoutError:
            raise TimeoutError(
                f"Event ACK for {context.event} timed out after {ack_timeout}s"
            ) from None

        finally:
            # Clean up pending ACK
            self._pending_acks.pop(ack_id, None)
