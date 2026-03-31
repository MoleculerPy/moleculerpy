"""
Middleware Module for the MoleculerPy framework.

This module defines the Middleware base class that provides hooks for intercepting and
modifying behavior at various stages of the MoleculerPy lifecycle. Middleware can be used
for logging, authentication, request/response transformation, error handling, and more.

Each middleware method provides a specific hook point in the broker or service lifecycle.
"""

from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

# Define type variables for better type annotations
T = TypeVar("T")
HandlerType = Callable[[Any], Awaitable[T]]


class Middleware:
    """
    Base Middleware class that can be extended to intercept and modify MoleculerPy behavior.

    Middleware classes can implement any of these methods to hook into the framework's lifecycle:

    Handler hooks (to intercept actions and events):
    - local_action: Called when a local action is being set up
    - remote_action: Called when a remote action is being set up
    - local_event: Called when a local event is being set up

    Event emission hooks (Moleculer.js compatible):
    - emit: Called to wrap broker.emit() calls
    - broadcast: Called to wrap broker.broadcast() calls

    Broker lifecycle hooks:
    - broker_created: Called synchronously when the broker is created
    - broker_starting: Called asynchronously BEFORE the broker connects to the transporter
    - broker_started: Called asynchronously AFTER the broker is fully started
    - broker_stopping: Called asynchronously BEFORE the broker disconnects
    - broker_stopped: Called asynchronously AFTER the broker is fully stopped
    - stopped: Called for middleware to clean up its own resources

    Service lifecycle hooks:
    - service_creating: Called asynchronously BEFORE a service is registered
    - service_created: Called asynchronously AFTER a service is registered
    - service_started: Called asynchronously when a service is started
    - service_stopping: Called asynchronously BEFORE a service is stopped
    - service_stopped: Called asynchronously AFTER a service is stopped
    """

    async def local_action(self, next_handler: HandlerType[Any], action: Any) -> HandlerType[Any]:
        """
        Hook for local action middleware processing.

        Args:
            next_handler: The next handler in the chain
            action: The action object being registered

        Returns:
            A wrapped handler that will be called when the action is invoked
        """

        async def handler(ctx: Any) -> Any:
            return await next_handler(ctx)

        return handler

    async def remote_action(self, next_handler: HandlerType[Any], action: Any) -> HandlerType[Any]:
        """
        Hook for remote action middleware processing.

        Args:
            next_handler: The next handler in the chain
            action: The action object being registered

        Returns:
            A wrapped handler that will be called when the remote action is invoked
        """

        async def handler(ctx: Any) -> Any:
            return await next_handler(ctx)

        return handler

    async def local_event(self, next_handler: HandlerType[Any], event: Any) -> HandlerType[Any]:
        """
        Hook for local event middleware processing.

        Args:
            next_handler: The next handler in the chain
            event: The event object being registered

        Returns:
            A wrapped handler that will be called when the event is emitted
        """

        async def handler(ctx: Any) -> Any:
            return await next_handler(ctx)

        return handler

    def broker_created(self, broker: Any) -> None:
        """
        Hook called synchronously when the broker is created.

        This is the only synchronous hook, as it's called from the Broker's __init__ method.

        Args:
            broker: The broker instance
        """
        pass

    async def broker_starting(self, broker: Any) -> None:
        """
        Hook called asynchronously BEFORE the broker connects to the transporter.

        Use this hook to initialize resources that need to be ready before
        the broker starts accepting requests.

        Args:
            broker: The broker instance
        """
        pass

    async def broker_started(self, broker: Any) -> None:
        """
        Hook called asynchronously AFTER the broker is fully started.

        At this point, the broker is connected to the transporter and ready
        to handle requests.

        Args:
            broker: The broker instance
        """
        pass

    async def broker_stopping(self, broker: Any) -> None:
        """
        Hook called asynchronously BEFORE the broker disconnects.

        Use this hook to gracefully finish in-flight operations before
        the broker stops.

        Args:
            broker: The broker instance
        """
        pass

    async def broker_stopped(self, broker: Any) -> None:
        """
        Hook called asynchronously AFTER the broker is fully stopped.

        Use this hook to clean up resources after the broker has stopped.

        Args:
            broker: The broker instance
        """
        pass

    async def service_creating(self, service: Any) -> None:
        """
        Hook called asynchronously BEFORE a service is registered.

        Use this hook to set up resources needed by the service.

        Args:
            service: The service instance
        """
        pass

    async def service_created(self, service: Any) -> None:
        """
        Hook called asynchronously AFTER a service is registered.

        Args:
            service: The service instance
        """
        pass

    async def service_started(self, service: Any) -> None:
        """
        Hook called asynchronously when a service is started.

        Args:
            service: The service instance
        """
        pass

    async def service_stopping(self, service: Any) -> None:
        """
        Hook called asynchronously BEFORE a service is stopped.

        Use this hook to gracefully stop ongoing operations in the service.

        Args:
            service: The service instance
        """
        pass

    async def service_stopped(self, service: Any) -> None:
        """
        Hook called asynchronously AFTER a service is stopped.

        Use this hook to clean up service resources.

        Args:
            service: The service instance
        """
        pass

    # ==========================================================================
    # Event emission hooks (Moleculer.js compatible)
    # ==========================================================================

    async def emit(
        self, next_emit: Any, event_name: str, params: dict[str, Any], opts: dict[str, Any]
    ) -> Any:
        """
        Hook for intercepting broker.emit() calls.

        This hook wraps the emit method allowing middleware to:
        - Log or trace event emissions
        - Modify event parameters
        - Prevent certain events from being emitted
        - Add metrics or timing

        Args:
            next_emit: The next emit function in the chain
            event_name: Name of the event being emitted
            params: Event parameters
            opts: Emit options (meta, etc.)

        Returns:
            Result from emit (usually None)

        Example:
            async def emit(self, next_emit, event_name, params, opts):
                self.logger.debug(f"Emitting: {event_name}")
                start = time.time()
                result = await next_emit(event_name, params, opts)
                self.logger.debug(f"Emitted {event_name} in {time.time()-start:.3f}s")
                return result
        """
        return await next_emit(event_name, params, opts)

    async def broadcast(
        self, next_broadcast: Any, event_name: str, params: dict[str, Any], opts: dict[str, Any]
    ) -> Any:
        """
        Hook for intercepting broker.broadcast() calls.

        Similar to emit hook but for broadcast (sends to all handlers).

        Args:
            next_broadcast: The next broadcast function in the chain
            event_name: Name of the event being broadcast
            params: Event parameters
            opts: Broadcast options (meta, groups, etc.)

        Returns:
            List of results from all handlers

        Example:
            async def broadcast(self, next_broadcast, event_name, params, opts):
                if event_name.startswith("sensitive."):
                    self.audit_log(event_name, params)
                return await next_broadcast(event_name, params, opts)
        """
        return await next_broadcast(event_name, params, opts)

    async def broadcast_local(
        self,
        next_broadcast_local: Callable[[str, Any], Awaitable[bool]],
        event_name: str,
        payload: Any,
    ) -> bool:
        """
        Hook for intercepting broker.broadcast_local() calls.

        Used for internal framework events ($ prefixed) that stay local to the node.
        Useful for metrics, logging, or debugging internal events like $broker.started.

        Args:
            next_broadcast_local: The next broadcast_local function in the chain
            event_name: Internal event name (e.g., "$broker.started", "$node.connected")
            payload: Event payload

        Returns:
            True if any handlers were called

        Example:
            async def broadcast_local(self, next_broadcast_local, event_name, payload):
                if event_name.startswith("$circuit-breaker."):
                    self.circuit_events.inc()
                return await next_broadcast_local(event_name, payload)
        """
        return await next_broadcast_local(event_name, payload)

    async def transit_publish(
        self,
        next_publish: Callable[[Any], Awaitable[None]],
        packet: Any,
    ) -> None:
        """
        Hook for intercepting transit.publish() calls.

        This hook wraps the transit layer's publish method, allowing middleware to:
        - Log or trace all outgoing packets
        - Collect metrics (packet size, type, destination)
        - Implement compression or encryption
        - Add custom headers or metadata

        Args:
            next_publish: The next publish function in the chain
            packet: The Packet object being published

        Example:
            async def transit_publish(self, next_publish, packet):
                self.packets_sent.inc()
                self.bytes_sent.add(len(packet.serialize()))
                self.logger.debug(f"Publishing {packet.topic} to {packet.target}")
                return await next_publish(packet)
        """
        return await next_publish(packet)

    async def transit_message_handler(
        self,
        next_handler: Callable[[Any], Awaitable[None]],
        packet: Any,
    ) -> None:
        """Hook for intercepting transit._message_handler() calls.

        Intercepts packets after deserialization but before type-specific dispatch.
        Use for: protocol-level auth, rate limiting, audit logging, packet filtering, metrics.

        Note: Node.js Moleculer passes (cmd, packet) to this hook. In Python,
        use packet.type (Topic enum) to distinguish packet types — this is the
        equivalent of the Node.js cmd parameter.

        Args:
            next_handler: The next handler function in the chain
            packet: The deserialized Packet object (has .type, .sender, .payload)

        Example:
            async def transit_message_handler(self, next_handler, packet):
                self.logger.info(f"Received {packet.type} from {packet.sender}")
                if packet.sender in BLOCKED_NODES:
                    return  # drop packet
                return await next_handler(packet)
        """
        return await next_handler(packet)

    def transporter_send(
        self, next_send: Callable[[str, bytes, dict[str, Any]], Awaitable[None]]
    ) -> Callable[[str, bytes, dict[str, Any]], Awaitable[None]]:
        """
        Hook for intercepting transporter.send() calls (byte-level).

        This hook wraps the low-level transporter send method, allowing middleware to:
        - Compress data before sending (CompressionMiddleware)
        - Encrypt data before sending (EncryptionMiddleware)
        - Collect byte-level metrics

        Unlike transit_publish (which works with Packet objects), this hook
        works with raw serialized bytes.

        Args:
            next_send: The next send function in the chain

        Returns:
            Wrapped send function: (topic: str, data: bytes, meta: dict) -> None

        Example:
            def transporter_send(self, next_send):
                async def send(topic, data, meta):
                    compressed = zlib.compress(data)
                    return await next_send(topic, compressed, meta)
                return send
        """

        async def send(topic: str, data: bytes, meta: dict[str, Any]) -> None:
            return await next_send(topic, data, meta)

        return send

    def transporter_receive(
        self, next_receive: Callable[[str, bytes, dict[str, Any]], Awaitable[None]]
    ) -> Callable[[str, bytes, dict[str, Any]], Awaitable[None]]:
        """
        Hook for intercepting transporter.receive() calls (byte-level).

        This hook wraps the low-level transporter receive method, allowing middleware to:
        - Decompress data after receiving (CompressionMiddleware)
        - Decrypt data after receiving (EncryptionMiddleware)
        - Collect byte-level metrics

        Note: The middleware order is reversed for receive compared to send.
        This ensures symmetric processing (e.g., encrypt→compress on send,
        decompress→decrypt on receive).

        Args:
            next_receive: The next receive function in the chain

        Returns:
            Wrapped receive function: (cmd: str, data: bytes, meta: dict) -> None

        Example:
            def transporter_receive(self, next_receive):
                async def receive(cmd, data, meta):
                    decompressed = zlib.decompress(data)
                    return await next_receive(cmd, decompressed, meta)
                return receive
        """

        async def receive(cmd: str, data: bytes, meta: dict[str, Any]) -> None:
            return await next_receive(cmd, data, meta)

        return receive

    async def stopped(self) -> None:
        """
        Hook called when middleware should clean up resources.

        This hook is called during broker shutdown to allow middleware
        to clean up timers, pending tasks, connections, etc.

        Unlike broker_stopped, this is called specifically for the middleware
        to release its own resources.
        """
        pass
