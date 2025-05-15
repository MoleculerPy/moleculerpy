"""NATS transporter implementation for the MoleculerPy framework.

This module provides a NATS-based transporter for inter-node communication
in a MoleculerPy cluster using the NATS messaging system.
"""

import asyncio
import json
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import nats
from nats.aio.msg import Msg

if TYPE_CHECKING:
    from ..transit import Transit

from ..packet import Packet
from .base import Transporter

logger = logging.getLogger(__name__)
JSON_THREAD_OFFLOAD_THRESHOLD = 1024 * 1024


class NatsTransporter(Transporter):
    """NATS transporter for MoleculerPy inter-node communication.

    This transporter uses NATS (https://nats.io/) as the messaging backbone
    for communication between MoleculerPy nodes in a distributed system.
    """

    name = "nats"

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
        super().__init__(self.name)
        self.connection_string = connection_string
        self.transit = transit
        self.handler = handler
        self.node_id = node_id
        self.nc: Any | None = None

    def _serialize(self, payload: dict[str, Any]) -> bytes:
        """Serialize a payload for transmission over NATS.

        Args:
            payload: Dictionary payload to serialize

        Returns:
            Serialized payload as bytes
        """
        # Add protocol version and sender information
        payload["ver"] = "4"
        payload["sender"] = self.node_id
        return json.dumps(payload).encode("utf-8")

    def get_topic_name(self, command: str, node_id: str | None = None) -> str:
        """Generate a NATS topic name for a command.

        Args:
            command: Command type for the topic
            node_id: Optional specific node ID to target

        Returns:
            Formatted NATS topic name
        """
        topic = f"MOL.{command}"
        if node_id:
            topic += f".{node_id}"
        return topic

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

    async def receive(self, cmd: str, data: bytes, meta: dict[str, Any]) -> None:
        """Process received raw bytes after middleware processing.

        Deserializes the data and calls the handler with a Packet.

        Args:
            cmd: Command type (e.g., "REQ", "RES", "EVENT")
            data: Raw message bytes (potentially decompressed/decrypted)
            meta: Metadata containing packet_type, subject, etc.
        """
        try:
            if len(data) > JSON_THREAD_OFFLOAD_THRESHOLD:
                payload = await asyncio.to_thread(lambda: json.loads(data.decode("utf-8")))
            else:
                payload = json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ValueError(f"Failed to decode message data: {e}") from e

        packet_type = meta.get("packet_type")
        if packet_type is None:
            raise ValueError("packet_type missing from meta")

        sender = payload.get("sender")
        packet = Packet(packet_type, sender, payload)
        packet.sender = sender

        if self.handler:
            await self.handler(packet)
        else:
            raise ValueError("Message received but no handler is defined")

    async def publish(self, packet: "Packet") -> None:
        """Publish a packet to NATS.

        Serializes the packet and sends through middleware chain.

        Args:
            packet: Packet to publish

        Raises:
            RuntimeError: If not connected to NATS
        """
        if not self.nc:
            raise RuntimeError("Not connected to NATS server")

        topic = self.get_topic_name(packet.type.value, packet.target)
        serialized_payload = self._serialize(packet.payload)

        # Send through middleware chain
        meta = {"packet": packet}
        await self.send_with_middleware(topic, serialized_payload, meta)

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
            else:
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
