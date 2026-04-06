"""NATS transporter implementation for the MoleculerPy framework.

This module provides a NATS-based transporter for inter-node communication
in a MoleculerPy cluster using the NATS messaging system.
"""

import asyncio
import logging
import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import nats
from nats.aio.msg import Msg

if TYPE_CHECKING:
    from ..transit import Transit

from ..packet import Packet
from ..serializers import to_packet_type
from .base import PROTOCOL_VERSION, Transporter

logger = logging.getLogger(__name__)


class NatsTransporter(Transporter):
    """NATS transporter for MoleculerPy inter-node communication.

    This transporter uses NATS (https://nats.io/) as the messaging backbone
    for communication between MoleculerPy nodes in a distributed system.
    """

    name = "nats"
    has_built_in_balancer: bool = True

    def __init__(
        self,
        connection_string: str,
        transit: "Transit",
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
    ) -> None:
        """Initialize the NATS transporter.

        Args:
            connection_string: NATS server connection string
            transit: Transit instance for message routing
            handler: Optional message handler function
            node_id: Unique identifier for this node
        """
        super().__init__(self.name, transit=transit, handler=handler, node_id=node_id)
        self.connection_string = connection_string
        self.nc: Any | None = None
        self._balanced_subscriptions: list[Any] = []

    def _is_connected(self) -> bool:
        """Check if NATS client is connected."""
        return self.nc is not None and getattr(self.nc, "is_connected", False)

    async def message_handler(self, msg: Msg) -> None:
        """Handle incoming NATS messages.

        Routes through middleware chain via receive_with_middleware.

        Args:
            msg: NATS message object

        Raises:
            ValueError: If no handler is configured
        """
        # Extract command type from topic
        try:
            packet_type = Packet.from_topic(msg.subject)
        except ValueError:
            logger.warning("Skipping NATS message from unknown topic: %s", msg.subject)
            return
        if packet_type is None:
            logger.warning("Skipping NATS message with unresolved topic type: %s", msg.subject)
            return

        # Pass raw bytes through middleware chain
        meta = {"subject": msg.subject, "packet_type": packet_type}
        await self.receive_with_middleware(packet_type.value, msg.data, meta)

    async def send(self, topic: str, data: bytes, meta: dict[str, Any]) -> None:
        """Send raw bytes to NATS after middleware processing.

        This is the low-level send that actually publishes to NATS.

        Args:
            topic: NATS topic to publish to
            data: Serialized (and possibly compressed/encrypted) bytes
            meta: Metadata (packet reference, etc.)

        Raises:
            RuntimeError: If not connected to NATS
        """
        if not self.nc:
            raise RuntimeError("Not connected to NATS server")
        await self.nc.publish(topic, data)

    async def connect(self) -> None:
        """Establish connection to the NATS server.

        Raises:
            Exception: If connection fails
        """
        try:
            self.nc = await nats.connect(self.connection_string)
        except Exception as e:
            raise Exception(f"Failed to connect to NATS server: {e}") from e

    async def disconnect(self) -> None:
        """Disconnect from the NATS server gracefully."""
        if self.nc:
            try:
                await asyncio.wait_for(self.nc.close(), timeout=5.0)
            except TimeoutError:
                logger.warning("Timed out closing NATS connection after 5.0s")
            except Exception:
                # Log the error but don't raise to ensure cleanup continues
                logger.exception("Failed to close NATS connection cleanly")
            finally:
                # Always clear the reference — even on timeout/error, the connection
                # is effectively dead and should not be reused.
                self.nc = None

    async def subscribe(self, command: str, topic: str | None = None) -> None:
        """Subscribe to messages for a specific command.

        Args:
            command: Command type to subscribe to
            topic: Optional specific topic (uses node_id if not provided)

        Raises:
            ValueError: If handler is not configured or not async
            RuntimeError: If not connected to NATS
        """
        if not self.nc:
            raise RuntimeError("Not connected to NATS server")

        if self.handler is None:
            raise ValueError("Handler must be provided for subscription")

        # Validate that the handler is properly configured
        if not asyncio.iscoroutinefunction(self.message_handler):
            raise ValueError("Message handler must be an async function")

        topic_name = self.get_topic_name(command, topic)
        await self.nc.subscribe(topic_name, cb=self.message_handler)

    async def subscribe_balanced_request(self, action: str) -> None:
        """Subscribe to balanced request topic using NATS queue groups.

        Args:
            action: Action name to subscribe to
        """
        if not self.nc:
            raise RuntimeError("Not connected to NATS server")
        topic = f"MOL.REQB.{action}"
        sub = await self.nc.subscribe(topic, queue=action, cb=self.message_handler)
        self._balanced_subscriptions.append(sub)

    async def subscribe_balanced_event(self, event: str, group: str) -> None:
        """Subscribe to balanced event topic using NATS queue groups.

        Args:
            event: Event name to subscribe to
            group: Consumer group name
        """
        if not self.nc:
            raise RuntimeError("Not connected to NATS server")
        nats_event = re.sub(r"\*\*.*$", ">", event)
        topic = f"MOL.EVENTB.{group}.{nats_event}"
        sub = await self.nc.subscribe(topic, queue=group, cb=self.message_handler)
        self._balanced_subscriptions.append(sub)

    async def publish_balanced_request(self, packet: Packet) -> None:
        """Publish a balanced request packet via NATS.

        Args:
            packet: Packet to publish
        """
        if not self.nc:
            raise RuntimeError("Not connected to NATS server")
        action = packet.payload.get("action", "")
        if not action:
            logger.warning("Cannot publish balanced request: missing action field")
            return
        assert self.transit is not None  # guaranteed after connect
        topic = f"MOL.REQB.{action}"
        payload = {**packet.payload, "ver": PROTOCOL_VERSION, "sender": self.node_id}
        data = await self.transit.serializer.serialize_async(
            payload, packet_type=to_packet_type(packet.type.value)
        )
        await self.send_with_middleware(topic, data, {"packet": packet})

    async def publish_balanced_event(self, packet: Packet, group: str) -> None:
        """Publish a balanced event packet via NATS.

        Args:
            packet: Packet to publish
            group: Consumer group name
        """
        if not self.nc:
            raise RuntimeError("Not connected to NATS server")
        event = packet.payload.get("event", "")
        if not event:
            logger.warning("Cannot publish balanced event: missing event field")
            return
        assert self.transit is not None
        topic = f"MOL.EVENTB.{group}.{event}"
        payload = {**packet.payload, "ver": PROTOCOL_VERSION, "sender": self.node_id}
        data = await self.transit.serializer.serialize_async(
            payload, packet_type=to_packet_type(packet.type.value)
        )
        await self.send_with_middleware(topic, data, {"packet": packet})

    async def unsubscribe_from_balanced_commands(self) -> None:
        """Unsubscribe all balanced subscriptions."""
        for sub in self._balanced_subscriptions:
            await sub.unsubscribe()
        self._balanced_subscriptions.clear()

    @classmethod
    def from_config(
        cls: type["NatsTransporter"],
        config: dict[str, Any],
        transit: "Transit",
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
    ) -> "NatsTransporter":
        """Create a NATS transporter from configuration.

        Args:
            config: Configuration dictionary containing connection details
            transit: Transit instance for message routing
            handler: Optional message handler function
            node_id: Optional node identifier

        Returns:
            Configured NatsTransporter instance

        Raises:
            KeyError: If required configuration keys are missing
        """
        try:
            connection_string = config["connection"]
        except KeyError:
            raise KeyError("NATS configuration must include 'connection' key") from None

        return cls(
            connection_string=connection_string,
            transit=transit,
            handler=handler,
            node_id=node_id,
        )
