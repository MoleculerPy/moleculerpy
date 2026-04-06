"""Base transporter abstraction for the MoleculerPy framework.

Matches Node.js Moleculer BaseTransporter (transporters/base.js):
- serialize/deserialize in base (not per-transporter)
- publish/receive are concrete Template Methods
- subclasses only override: connect, disconnect, send, subscribe, _is_connected

Architecture (Moleculer.js compatible):
- publish(packet) → serialize → send_with_middleware(topic, bytes, meta)
- send(topic, bytes, meta) → actual wire publish (abstract, per-transport)
- receive(cmd, bytes, meta) → deserialize → handler(packet) (concrete)
"""

import importlib
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    from ..transit import Transit

from ..packet import Packet
from ..serializers import to_packet_type

PROTOCOL_VERSION: str = "4"

logger = logging.getLogger(__name__)


class SubscriptionTopic(TypedDict):
    """Type for topic subscription entries passed to make_subscriptions."""

    cmd: str
    nodeID: str | None


class Transporter(ABC):
    """Abstract base class for all MoleculerPy transporters.

    Template Method pattern (matching Node.js BaseTransporter):
    - publish() and receive() are CONCRETE — shared serialize/deserialize logic
    - send() is ABSTRACT — wire-level transport (NATS publish, Redis publish, etc.)
    - connect() and disconnect() are ABSTRACT — connection lifecycle
    - _is_connected() is ABSTRACT — connection state check

    Subclasses override send/connect/disconnect/_is_connected.
    TCP overrides publish/receive entirely (different protocol).
    """

    has_built_in_balancer: bool = False

    def __init__(
        self,
        name: str,
        transit: "Transit | None" = None,
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
        *,
        prefix: str = "MOL",
    ) -> None:
        """Initialize the transporter with common attributes.

        Args:
            name: Name identifier for this transporter
            transit: Transit instance for serialization/routing
            handler: Message handler callback
            node_id: Node identifier
            prefix: Topic prefix (default: "MOL")
        """
        self.name = name
        self.transit: Transit | None = transit
        self.handler = handler
        self.node_id = node_id
        self.prefix = prefix
        # Wrapped methods (set by transit._wrap_transporter_methods)
        self._wrapped_send: Callable[[str, bytes, dict[str, Any]], Any] | None = None
        self._wrapped_receive: Callable[[str, bytes, dict[str, Any]], Any] | None = None

    # ------------------------------------------------------------------
    # Abstract methods — subclasses MUST implement
    # ------------------------------------------------------------------

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the messaging system."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the messaging system."""
        pass

    @abstractmethod
    async def subscribe(self, command: str, topic: str | None = None) -> None:
        """Subscribe to messages for a specific command or topic."""
        pass

    @abstractmethod
    async def send(self, topic: str, data: bytes, meta: dict[str, Any]) -> None:
        """Send raw bytes over the wire. The ONLY method subclasses need for publishing."""
        pass

    @abstractmethod
    def _is_connected(self) -> bool:
        """Check if the transporter is currently connected."""
        ...

    # ------------------------------------------------------------------
    # Concrete Template Methods — shared logic (matching Node.js base.js)
    # ------------------------------------------------------------------

    def get_topic_name(self, command: str, node_id: str | None = None) -> str:
        """Generate topic name. Matches Node.js getTopicName().

        Default: prefix.command[.nodeID]
        Override in subclass for custom separators (e.g., MQTT uses /).
        """
        topic = f"{self.prefix}.{command}"
        if node_id:
            topic += f".{node_id}"
        return topic

    async def publish(self, packet: "Packet") -> None:
        """Serialize packet and send via middleware chain.

        Matches Node.js BaseTransporter.publish():
          topic = getTopicName(packet.type, packet.target)
          data = serialize(packet)
          send(topic, data, {packet})

        Subclasses normally do NOT override this. Override send() instead.
        TCP overrides because its protocol differs fundamentally.
        """
        if not self._is_connected():
            raise RuntimeError(f"{self.name} transporter is not connected")

        if self.transit is None:
            raise RuntimeError("Transit not initialized")

        topic = self.get_topic_name(packet.type.value, packet.target)
        payload = {**packet.payload, "ver": PROTOCOL_VERSION, "sender": self.node_id}
        serialized = await self.transit.serializer.serialize_async(
            payload, packet_type=to_packet_type(packet.type.value)
        )

        meta: dict[str, Any] = {"packet": packet}
        await self.send_with_middleware(topic, serialized, meta)

    async def receive(self, cmd: str, data: bytes, meta: dict[str, Any]) -> None:
        """Deserialize incoming bytes and dispatch to handler.

        Matches Node.js BaseTransporter.incomingMessage():
          packet = deserialize(cmd, msg)
          messageHandler(cmd, packet)

        Subclasses normally do NOT override this. TCP overrides.
        """
        if self.transit is None:
            raise RuntimeError("Transit not initialized")

        packet_type = meta.get("packet_type")
        if packet_type is None:
            raise ValueError("packet_type missing from meta")

        try:
            payload = await self.transit.serializer.deserialize_async(
                data, packet_type=to_packet_type(packet_type.value)
            )
        except Exception as e:
            self._on_deserialize_error(cmd, e)
            return

        sender = payload.get("sender")
        packet = Packet(packet_type, sender, payload)
        packet.sender = sender

        if self.handler:
            await self.handler(packet)
        else:
            raise ValueError("Message received but no handler is defined")

    def _on_deserialize_error(self, cmd: str, error: Exception) -> None:
        """Hook for deserialization error handling.

        Default: log warning and drop message (matches Node.js behavior).
        Override in subclass to raise instead (e.g., Redis).
        """
        logger.warning("Failed to decode %s message, dropping: %r", self.name, error)

    # ------------------------------------------------------------------
    # Subscription helpers
    # ------------------------------------------------------------------

    async def make_subscriptions(self, topics: list[SubscriptionTopic]) -> None:
        """Batch subscribe to all topics at once.

        Default calls subscribe() per topic. Kafka overrides with ConsumerGroup.
        """
        for t in topics:
            await self.subscribe(t["cmd"], t.get("nodeID"))

    async def subscribe_balanced_request(self, action: str) -> None:
        """Subscribe to balanced request topic. No-op in base."""
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

    # ------------------------------------------------------------------
    # Middleware wrappers
    # ------------------------------------------------------------------

    async def send_with_middleware(self, topic: str, data: bytes, meta: dict[str, Any]) -> None:
        """Send through middleware chain if configured, else direct send."""
        if self._wrapped_send is not None:
            await self._wrapped_send(topic, data, meta)
        else:
            await self.send(topic, data, meta)

    async def receive_with_middleware(self, cmd: str, data: bytes, meta: dict[str, Any]) -> None:
        """Receive through middleware chain if configured, else direct receive."""
        if self._wrapped_receive is not None:
            await self._wrapped_receive(cmd, data, meta)
        else:
            await self.receive(cmd, data, meta)

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def get_by_name(
        cls: type["Transporter"],
        name: str,
        config: dict[str, Any],
        transit: "Transit",
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
    ) -> "Transporter":
        """Get a transporter instance by name (factory method)."""
        # Import known transporters to ensure they're registered
        importlib.import_module("moleculerpy.transporter.nats")
        importlib.import_module("moleculerpy.transporter.memory")
        importlib.import_module("moleculerpy.transporter.redis")
        importlib.import_module("moleculerpy.transporter.mqtt")
        importlib.import_module("moleculerpy.transporter.amqp")
        importlib.import_module("moleculerpy.transporter.kafka")
        importlib.import_module("moleculerpy.transporter.tcp")

        # TCP is a subpackage — also check nested subclasses
        all_subclasses = list(cls.__subclasses__())
        for sc in cls.__subclasses__():
            all_subclasses.extend(sc.__subclasses__())

        for subclass in all_subclasses:
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
        """Create a transporter instance from configuration."""
        pass
