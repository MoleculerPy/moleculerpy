"""Memory transporter implementation for the MoleculerPy framework.

This module provides an in-memory transporter for development and testing
without requiring external message brokers like NATS or Redis.

Key features:
- No external dependencies
- Messages stay in-process via shared event bus
- Multiple brokers can communicate in the same process
- Built-in load balancer (no external balancer needed)

Usage:
    from moleculerpy import ServiceBroker, Settings

    settings = Settings(
        transporter={"type": "memory"}  # or just "memory"
    )
    broker = ServiceBroker("node-1", settings=settings)
"""

import asyncio
import json
import logging
import weakref
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, cast

from .base import Transporter

logger = logging.getLogger(__name__)
JSON_THREAD_OFFLOAD_THRESHOLD = 1024 * 1024

if TYPE_CHECKING:
    from ..packet import Packet
    from ..transit import Transit


class _MemoryBus:
    """Shared in-memory event bus for inter-broker communication.

    This class provides a simple pub/sub mechanism for brokers running
    in the same process. It uses asyncio for async message delivery.

    Note: This is a module-level singleton to allow communication between
    multiple brokers in the same process (similar to Moleculer.js global.bus).
    """

    def __init__(self) -> None:
        """Initialize the memory bus."""
        # Type: handler(topic, data) -> Awaitable[None]
        self._handlers: dict[
            str, tuple[tuple[Callable[[str, bytes], Awaitable[None]], str], ...]
        ] = {}
        # handlers[topic] = ((callback, subscriber_id), ...)
        self._lock = asyncio.Lock()
        # Track running handler tasks for proper cleanup and error logging
        self._running_tasks: weakref.WeakSet[asyncio.Task[None]] = weakref.WeakSet()
        # Track task ownership to support per-subscriber graceful shutdown.
        self._task_owners: dict[asyncio.Task[None], str] = {}

    async def subscribe(
        self,
        topic: str,
        handler: Callable[[str, bytes], Awaitable[None]],
        subscriber_id: str,
    ) -> None:
        """Subscribe to a topic.

        Args:
            topic: Topic pattern to subscribe to
            handler: Async callback function to handle messages
            subscriber_id: Unique ID of the subscriber (for cleanup)
        """
        async with self._lock:
            handlers = self._handlers.get(topic, ())
            self._handlers[topic] = (*handlers, (handler, subscriber_id))

    async def unsubscribe(self, subscriber_id: str) -> None:
        """Remove all subscriptions for a subscriber.

        Args:
            subscriber_id: ID of the subscriber to remove
        """
        async with self._lock:
            for topic in list(self._handlers.keys()):
                filtered = tuple(
                    (h, sid) for h, sid in self._handlers[topic] if sid != subscriber_id
                )
                if filtered:
                    self._handlers[topic] = filtered
                else:
                    del self._handlers[topic]

    async def emit(self, topic: str, data: bytes) -> None:
        """Emit a message to all subscribers of a topic.

        Args:
            topic: Topic to emit to
            data: Serialized message data
        """
        # Copy-on-write subscriptions let emit read an immutable snapshot
        # without taking the lock on the hot path.
        handlers = self._handlers.get(topic)
        if not handlers:
            return

        for handler, subscriber_id in handlers:
            try:

                async def invoke_handler(
                    cb: Callable[[str, bytes], Awaitable[None]] = handler,
                    msg_topic: str = topic,
                    msg_data: bytes = data,
                ) -> None:
                    await cb(msg_topic, msg_data)

                # Schedule handler as a task to avoid blocking
                task: asyncio.Task[None] = asyncio.create_task(
                    invoke_handler(),
                )
                # Track task for cleanup and error logging
                self._running_tasks.add(task)
                self._task_owners[task] = subscriber_id
                task.add_done_callback(self._on_task_done)
            except Exception as e:
                # Log but don't break other subscribers
                logger.warning(
                    "Handler error for subscriber %s on topic %s: %s",
                    subscriber_id,
                    topic,
                    e,
                )

    def _on_task_done(self, task: "asyncio.Task[None]") -> None:
        """Handle completed handler task, log any exceptions.

        Args:
            task: The completed task
        """
        self._task_owners.pop(task, None)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error(
                "Handler task %s failed with exception: %s",
                task.get_name(),
                exc,
                exc_info=exc,
            )

    def get_subscriber_count(self, topic: str) -> int:
        """Get the number of subscribers for a topic.

        Args:
            topic: Topic to check

        Returns:
            Number of subscribers
        """
        return len(self._handlers.get(topic, []))

    def get_running_task_count(self) -> int:
        """Get the number of currently running handler tasks.

        Returns:
            Number of running tasks
        """
        return len(self._running_tasks)

    async def wait_for_pending_tasks(
        self,
        timeout: float = 5.0,
        subscriber_id: str | None = None,
    ) -> None:
        """Wait for all pending handler tasks to complete.

        Useful for graceful shutdown to ensure all messages are processed.

        Args:
            timeout: Maximum time to wait in seconds
            subscriber_id: Optional subscriber to scope waiting/canceling.
        """
        if subscriber_id is None:
            tasks = list(self._running_tasks)
        else:
            tasks = [
                task for task in self._running_tasks if self._task_owners.get(task) == subscriber_id
            ]
        if tasks:
            _done, pending = await asyncio.wait(tasks, timeout=timeout)
            if pending:
                logger.warning(
                    "Timed out waiting for %d handler tasks to complete",
                    len(pending),
                )
                for task in pending:
                    task.cancel()


# Module-level singleton bus for inter-broker communication
_global_bus = _MemoryBus()


class MemoryTransporter(Transporter):
    """In-memory transporter for development and testing.

    This transporter enables communication between multiple MoleculerPy brokers
    running in the same Python process without requiring external dependencies.

    Features:
    - Zero external dependencies
    - Instant message delivery (same process)
    - Supports multiple brokers in one process
    - Built-in load balancer

    Example:
        >>> from moleculerpy import ServiceBroker, Settings
        >>> settings = Settings(transporter={"type": "memory"})
        >>> broker1 = ServiceBroker("node-1", settings=settings)
        >>> broker2 = ServiceBroker("node-2", settings=settings)
        >>> # Both brokers can now communicate via memory bus
    """

    name = "memory"

    def __init__(
        self,
        transit: "Transit",
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
        bus: _MemoryBus | None = None,
    ) -> None:
        """Initialize the memory transporter.

        Args:
            transit: Transit instance for message routing
            handler: Message handler callback
            node_id: Unique identifier for this node
            bus: Optional custom bus instance (uses global bus if not provided)
        """
        super().__init__(self.name)
        self.transit = transit
        self.handler = handler
        self.node_id = node_id or "memory-node"
        self.bus = bus or _global_bus
        self._connected = False
        self._subscriptions: list[str] = []

        # Built-in balancer means no external balancer needed
        self.has_built_in_balancer = True

    def _serialize(self, payload: dict[str, Any]) -> bytes:
        """Serialize a payload for transmission.

        Adds protocol version and sender information.
        Note: Creates a copy to avoid mutating the original payload.

        Args:
            payload: Dictionary payload to serialize

        Returns:
            Serialized payload as bytes
        """
        # Create copy to avoid mutating original
        data = {**payload, "ver": "4", "sender": self.node_id}
        return json.dumps(data).encode("utf-8")

    def _deserialize(self, data: bytes) -> dict[str, Any]:
        """Deserialize received data.

        Args:
            data: Serialized message data

        Returns:
            Deserialized payload dictionary
        """
        return cast(dict[str, Any], json.loads(data.decode("utf-8")))

    async def _deserialize_async(self, data: bytes) -> dict[str, Any]:
        """Deserialize data with optional thread offload for large payloads."""
        if len(data) > JSON_THREAD_OFFLOAD_THRESHOLD:
            return cast(
                dict[str, Any],
                await asyncio.to_thread(lambda: json.loads(data.decode("utf-8"))),
            )
        return self._deserialize(data)

    def get_topic_name(self, command: str, node_id: str | None = None) -> str:
        """Generate a topic name for a command.

        Args:
            command: Command type for the topic
            node_id: Optional specific node ID to target

        Returns:
            Formatted topic name (MOL.{command} or MOL.{command}.{node_id})
        """
        topic = f"MOL.{command}"
        if node_id:
            topic += f".{node_id}"
        return topic

    async def _message_handler(self, topic: str, data: bytes) -> None:
        """Handle incoming messages from the bus.

        Routes through middleware chain via receive_with_middleware.

        Args:
            topic: Topic the message was received on
            data: Serialized message data
        """
        if not self.handler:
            return

        try:
            # Check if it's our own message (peek at sender without full deserialize)
            # This is an optimization to avoid processing our own broadcasts
            payload_peek = await self._deserialize_async(data)
            if payload_peek.get("sender") == self.node_id:
                return

            # Import here to avoid circular imports
            from ..packet import Packet  # noqa: PLC0415

            # Parse packet type from topic
            packet_type = Packet.from_topic(topic)
            if packet_type is None:
                return

            # Pass raw bytes through middleware chain
            meta = {"topic": topic, "packet_type": packet_type}
            await self.receive_with_middleware(packet_type.value, data, meta)
        except Exception as e:
            # Log error but don't crash the message loop
            logger.exception("Error processing message on topic %s: %s", topic, e)

    async def receive(self, cmd: str, data: bytes, meta: dict[str, Any]) -> None:
        """Process received raw bytes after middleware processing.

        Deserializes the data and calls the handler with a Packet.

        Args:
            cmd: Command type (e.g., "REQ", "RES", "EVENT")
            data: Raw message bytes (potentially decompressed/decrypted)
            meta: Metadata containing packet_type, topic, etc.
        """
        if not self.handler:
            return

        # Import here to avoid circular imports
        from ..packet import Packet  # noqa: PLC0415

        try:
            payload = await self._deserialize_async(data)
        except Exception as e:
            logger.exception("Failed to deserialize message: %s", e)
            return

        packet_type = meta.get("packet_type")
        if packet_type is None:
            logger.error("packet_type missing from meta")
            return

        sender = payload.get("sender")
        packet = Packet(packet_type, str(sender) if sender is not None else "", payload)
        packet.sender = sender

        await self.handler(packet)

    async def connect(self) -> None:
        """Establish connection to the memory bus.

        For memory transporter, this simply marks the transporter as connected.
        """
        self._connected = True

    async def disconnect(self) -> None:
        """Disconnect from the memory bus.

        Removes all subscriptions for this transporter.
        """
        self._connected = False

        # Remove all subscriptions
        await self.bus.unsubscribe(self.node_id)
        await self.bus.wait_for_pending_tasks(timeout=5.0, subscriber_id=self.node_id)
        self._subscriptions.clear()

    async def publish(self, packet: "Packet") -> None:
        """Publish a packet to the memory bus.

        Serializes the packet and sends through middleware chain.

        Args:
            packet: Packet to publish

        Raises:
            RuntimeError: If not connected
        """
        if not self._connected:
            raise RuntimeError("Memory transporter is not connected")

        topic = self.get_topic_name(packet.type.value, packet.target)
        serialized = self._serialize(packet.payload)

        # Send through middleware chain
        meta = {"packet": packet}
        await self.send_with_middleware(topic, serialized, meta)

    async def send(self, topic: str, data: bytes, meta: dict[str, Any]) -> None:
        """Send raw bytes to the memory bus after middleware processing.

        This is the low-level send that actually emits to the bus.

        Args:
            topic: Topic to emit to
            data: Serialized (and possibly compressed/encrypted) bytes
            meta: Metadata (packet reference, etc.)

        Raises:
            RuntimeError: If not connected
        """
        if not self._connected:
            raise RuntimeError("Memory transporter is not connected")
        await self.bus.emit(topic, data)

    async def subscribe(self, command: str, topic: str | None = None) -> None:
        """Subscribe to messages for a specific command.

        Subscribes to both:
        - Broadcast topic: MOL.{command}
        - Node-specific topic: MOL.{command}.{node_id}

        Args:
            command: Command type to subscribe to
            topic: Optional specific topic (overrides node_id targeting)

        Raises:
            RuntimeError: If not connected
        """
        if not self._connected:
            raise RuntimeError("Memory transporter is not connected")

        # Subscribe to broadcast topic (for messages with no target)
        broadcast_topic = self.get_topic_name(command)
        if broadcast_topic not in self._subscriptions:
            self._subscriptions.append(broadcast_topic)
            await self.bus.subscribe(broadcast_topic, self._message_handler, self.node_id)

        # Subscribe to node-specific topic (for targeted messages)
        if topic:
            node_topic = self.get_topic_name(command, topic)
        else:
            node_topic = self.get_topic_name(command, self.node_id)

        if node_topic not in self._subscriptions:
            self._subscriptions.append(node_topic)
            await self.bus.subscribe(node_topic, self._message_handler, self.node_id)

    @classmethod
    def from_config(
        cls: type["MemoryTransporter"],
        config: dict[str, Any],
        transit: "Transit",
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
    ) -> "MemoryTransporter":
        """Create a memory transporter from configuration.

        Args:
            config: Configuration dictionary (mostly ignored for memory)
            transit: Transit instance for message routing
            handler: Message handler callback
            node_id: Optional node identifier

        Returns:
            Configured MemoryTransporter instance
        """
        return cls(
            transit=transit,
            handler=handler,
            node_id=node_id,
        )


def reset_global_bus() -> None:
    """Reset the global memory bus.

    Useful for test isolation to ensure clean state between tests.
    """
    global _global_bus  # noqa: PLW0603
    _global_bus = _MemoryBus()


def get_global_bus() -> _MemoryBus:
    """Get the global memory bus instance.

    Returns:
        The global _MemoryBus instance
    """
    return _global_bus
