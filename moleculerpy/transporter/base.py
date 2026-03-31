"""Base transporter abstraction for the MoleculerPy framework.

This module provides the abstract base class for all transporters, which handle
communication between MoleculerPy nodes over various messaging protocols.
"""

import importlib
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..packet import Packet
    from ..transit import Transit


class Transporter(ABC):
    """Abstract base class for all MoleculerPy transporters.

    Transporters handle the low-level communication between nodes in a MoleculerPy
    cluster. They are responsible for publishing and subscribing to messages
    using various messaging protocols (NATS, Redis, etc.).

    Architecture (Moleculer.js compatible):
    - publish(packet) → serialize → send(topic, bytes, meta)
    - send(topic, bytes, meta) → actual transport publish (can be wrapped by middleware)
    - receive(cmd, bytes, meta) → deserialize → handler(packet) (can be wrapped)
    """

    has_built_in_balancer: bool = False

    def __init__(self, name: str) -> None:
        """Initialize the transporter.

        Args:
            name: Name identifier for this transporter
        """
        self.name = name
        # Wrapped methods (set by transit._wrap_transporter_methods)
        self._wrapped_send: Callable[[str, bytes, dict[str, Any]], Any] | None = None
        self._wrapped_receive: Callable[[str, bytes, dict[str, Any]], Any] | None = None

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the messaging system.

        This method should handle the connection establishment and any
        necessary authentication or initialization procedures.
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the messaging system.

        This method should handle graceful disconnection and cleanup
        of any resources used by the transporter.
        """
        pass

    @abstractmethod
    async def publish(self, packet: "Packet") -> None:
        """Publish a packet to the messaging system.

        Args:
            packet: The packet to publish containing the message data
        """
        pass

    @abstractmethod
    async def subscribe(self, command: str, topic: str | None = None) -> None:
        """Subscribe to messages for a specific command or topic.

        Args:
            command: The command type to subscribe to
            topic: Optional specific topic to subscribe to
        """
        pass

    @abstractmethod
    async def send(self, topic: str, data: bytes, meta: dict[str, Any]) -> None:
        """Send raw bytes to the messaging system.

        This is the low-level send method that can be wrapped by middleware
        for compression, encryption, or metrics collection.

        Args:
            topic: Topic/channel to send to
            data: Serialized message bytes
            meta: Metadata (packet reference, etc.)
        """
        pass

    @abstractmethod
    async def receive(self, cmd: str, data: bytes, meta: dict[str, Any]) -> None:
        """Process received raw bytes from the messaging system.

        This is the low-level receive method that can be wrapped by middleware
        for decompression, decryption, or metrics collection.

        Args:
            cmd: Command type (e.g., "REQ", "RES", "EVENT")
            data: Raw message bytes (before deserialization)
            meta: Metadata (subject, etc.)
        """
        pass

    async def subscribe_balanced_request(self, action: str) -> None:
        """Subscribe to balanced request topic for an action. No-op in base."""
        return

    async def subscribe_balanced_event(self, event: str, group: str) -> None:
        """Subscribe to balanced event topic. No-op in base."""
        return

    async def publish_balanced_request(self, packet: "Packet") -> None:
        """Publish a balanced request packet. No-op in base."""
        return

    async def publish_balanced_event(self, packet: "Packet", group: str) -> None:
        """Publish a balanced event packet. No-op in base."""
        return

    async def unsubscribe_from_balanced_commands(self) -> None:
        """Unsubscribe all balanced subscriptions. No-op in base."""
        return

    async def send_with_middleware(self, topic: str, data: bytes, meta: dict[str, Any]) -> None:
        """Send through middleware chain if configured, else direct send.

        Args:
            topic: Topic/channel to send to
            data: Serialized message bytes
            meta: Metadata
        """
        if self._wrapped_send is not None:
            await self._wrapped_send(topic, data, meta)
        else:
            await self.send(topic, data, meta)

    async def receive_with_middleware(self, cmd: str, data: bytes, meta: dict[str, Any]) -> None:
        """Receive through middleware chain if configured, else direct receive.

        Args:
            cmd: Command type
            data: Raw message bytes
            meta: Metadata
        """
        if self._wrapped_receive is not None:
            await self._wrapped_receive(cmd, data, meta)
        else:
            await self.receive(cmd, data, meta)

    @classmethod
    def get_by_name(
        cls: type["Transporter"],
        name: str,
        config: dict[str, Any],
        transit: "Transit",
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
    ) -> "Transporter":
        """Get a transporter instance by name.

        This factory method dynamically loads and instantiates the appropriate
        transporter subclass based on the provided name.

        Args:
            name: Name of the transporter (e.g., "nats", "redis")
            config: Configuration dictionary for the transporter
            transit: Transit instance for handling message routing
            handler: Optional message handler function
            node_id: Optional node identifier

        Returns:
            Configured transporter instance

        Raises:
            ValueError: If no transporter is found for the given name
        """
        # Import known transporters to ensure they're registered
        importlib.import_module("moleculerpy.transporter.nats")
        importlib.import_module("moleculerpy.transporter.memory")
        importlib.import_module("moleculerpy.transporter.redis")

        for subclass in cls.__subclasses__():
            if subclass.__name__.lower().startswith(name.lower()):
                return subclass.from_config(config, transit, handler, node_id)

        raise ValueError(f"No transporter found for: {name}")

    @classmethod
    @abstractmethod
    def from_config(
        cls: type["Transporter"],
        config: dict[str, Any],
        transit: "Transit",
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
    ) -> "Transporter":
        """Create a transporter instance from configuration.

        Args:
            config: Configuration dictionary
            transit: Transit instance for message routing
            handler: Optional message handler function
            node_id: Optional node identifier

        Returns:
            Configured transporter instance
        """
        pass
