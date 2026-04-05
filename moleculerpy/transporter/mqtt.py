"""MQTT transporter implementation for the MoleculerPy framework.

This module provides an MQTT-based transporter for inter-node communication
in a MoleculerPy cluster. Ideal for IoT and edge computing scenarios
where lightweight pub/sub messaging is required.

Requires: aiomqtt>=2.0.0 (install with `pip install moleculerpy[mqtt]`)

Reference: sources/reference-implementations/moleculer/src/transporters/mqtt.js
"""

import asyncio
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..transit import Transit

from ..packet import Packet
from .base import Transporter

# Moleculer protocol version (must match transit.PROTOCOL_VERSION)
PROTOCOL_VERSION: str = "4"

logger = logging.getLogger(__name__)
DISCONNECT_TIMEOUT_SECONDS = 5.0


class MqttTransporter(Transporter):
    """MQTT transporter for MoleculerPy inter-node communication.

    This transporter uses MQTT as the messaging backbone for communication
    between MoleculerPy nodes in a distributed system. Compatible with
    Node.js Moleculer MqttTransporter wire format.

    MQTT is a good choice for:
    - IoT and edge computing (low bandwidth overhead)
    - Resource-constrained environments
    - Mosquitto, HiveMQ, AWS IoT Core brokers

    Note: hasBuiltInBalancer = False (matches Node.js).
    Load balancing is handled by Moleculer Service Registry.

    Example:
        >>> broker = ServiceBroker(Settings(transporter="mqtt://localhost:1883"))
    """

    name = "mqtt"
    has_built_in_balancer: bool = False

    def __init__(
        self,
        connection_string: str,
        transit: "Transit",
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
        *,
        qos: int = 0,
        topic_separator: str = ".",
    ) -> None:
        """Initialize the MQTT transporter.

        Args:
            connection_string: MQTT broker URL (e.g., mqtt://localhost:1883)
            transit: Transit instance for message routing
            handler: Optional message handler function
            node_id: Unique identifier for this node
            qos: MQTT Quality of Service level (0, 1, or 2). Default: 0
            topic_separator: Topic name separator. Default: "." (Node.js compatible)
        """
        super().__init__(self.name)
        self.connection_string = connection_string
        self.transit = transit
        self.handler = handler
        self.node_id = node_id
        self.qos = qos
        self.topic_separator = topic_separator

        # MQTT client (set during connect)
        self._client: Any | None = None

        # Background listener task
        self._listener_task: asyncio.Task[None] | None = None

        # Shutdown flag
        self._shutting_down = False

        # Topic prefix (MOL or MOL-{namespace})
        self.prefix = "MOL"

    def get_topic_name(self, command: str, node_id: str | None = None) -> str:
        """Generate an MQTT topic name for a command.

        Format: {prefix}{sep}{cmd}[{sep}{nodeID}]
        Matches Node.js MqttTransporter.getTopicName() exactly.

        Args:
            command: Command type (e.g., REQ, RES, EVENT)
            node_id: Optional specific node ID to target

        Returns:
            Formatted MQTT topic name
        """
        topic = f"{self.prefix}{self.topic_separator}{command}"
        if node_id:
            topic += f"{self.topic_separator}{node_id}"
        return topic

    async def connect(self) -> None:
        """Establish connection to the MQTT broker.

        Lazily imports aiomqtt to fail gracefully when not installed.
        Safe to call on already-connected transporter (will disconnect first).

        Raises:
            ImportError: If aiomqtt is not installed
            Exception: If connection fails
        """
        # Guard against reconnect: clean up previous connection first
        if self._client is not None:
            await self.disconnect()

        try:
            import aiomqtt  # noqa: PLC0415 — lazy import, matches Node.js pattern
        except ImportError:
            raise ImportError(
                "The 'aiomqtt' package is missing. Install it with: pip install aiomqtt"
            ) from None

        # Parse connection string to extract host/port
        hostname, port = self._parse_connection_string(self.connection_string)

        self._client = aiomqtt.Client(
            hostname=hostname,
            port=port,
        )

        # Connect the client
        await self._client.__aenter__()

        self._shutting_down = False
        logger.info("MQTT client is connected to %s:%d.", hostname, port)

        # Start background listener
        self._listener_task = asyncio.create_task(self._listen_loop())

    async def disconnect(self) -> None:
        """Disconnect from the MQTT broker gracefully."""
        self._shutting_down = True

        # Cancel listener task
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        self._listener_task = None

        # Disconnect client
        if self._client:
            try:
                await self._client.__aexit__(None, None, None)
            except Exception:
                logger.exception("Failed to disconnect MQTT client cleanly")
            self._client = None

        logger.info("MQTT client disconnected.")

    async def _listen_loop(self) -> None:
        """Listen for incoming MQTT messages in background.

        Routes messages through the middleware chain.
        On unexpected errors, marks connection as broken to prevent zombie state.
        """
        if not self._client:
            return

        try:
            async for message in self._client.messages:
                if self._shutting_down:
                    break
                await self._handle_message(message)
        except asyncio.CancelledError:
            pass
        except Exception:
            if not self._shutting_down:
                logger.exception("MQTT listener error — connection may be broken")
                # Prevent zombie state: clear client so publish/subscribe fail fast
                self._client = None

    async def _handle_message(self, message: Any) -> None:
        """Handle a single incoming MQTT message.

        Parses topic to extract command type and routes through middleware.
        Matches Node.js: topic.substring(prefix.length + sep.length), split(sep)[0]

        Args:
            message: aiomqtt Message object (topic, payload)
        """
        raw_topic = str(message.topic)
        data = message.payload

        # Ensure data is bytes
        if isinstance(data, str):
            data = data.encode("utf-8")

        # Extract command from topic: strip prefix + separator, take first segment
        prefix_len = len(self.prefix) + len(self.topic_separator)
        if len(raw_topic) <= prefix_len:
            logger.warning("Skipping MQTT message with short topic: %s", raw_topic)
            return

        remainder = raw_topic[prefix_len:]
        cmd = remainder.split(self.topic_separator)[0]

        # Resolve packet type using standard MOL.{CMD} format for from_topic
        # Note: from_topic always uses "." separator internally (NATS convention)
        try:
            packet_type = Packet.from_topic(f"{self.prefix}.{cmd}")
        except ValueError:
            logger.warning("Skipping MQTT message from unknown topic: %s", raw_topic)
            return
        if packet_type is None:
            logger.warning("Skipping MQTT message with unresolved topic type: %s", raw_topic)
            return

        meta = {"topic": raw_topic, "packet_type": packet_type}
        await self.receive_with_middleware(packet_type.value, data, meta)

    async def receive(self, cmd: str, data: bytes, meta: dict[str, Any]) -> None:
        """Process received raw bytes after middleware processing.

        Deserializes the data and calls the handler with a Packet.

        Args:
            cmd: Command type (e.g., "REQ", "RES", "EVENT")
            data: Raw message bytes
            meta: Metadata containing packet_type, topic, etc.
        """
        try:
            payload = await self.transit.serializer.deserialize_async(data)
        except Exception as e:
            logger.warning("Failed to decode MQTT message, dropping: %r", e)
            return

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
        """Publish a packet to MQTT.

        Serializes the packet and sends through middleware chain.

        Args:
            packet: Packet to publish

        Raises:
            RuntimeError: If not connected to MQTT broker
        """
        if not self._client:
            raise RuntimeError("Not connected to MQTT broker")

        topic = self.get_topic_name(packet.type.value, packet.target)
        payload = {**packet.payload, "ver": PROTOCOL_VERSION, "sender": self.node_id}
        serialized_payload = await self.transit.serializer.serialize_async(payload)

        meta = {"packet": packet}
        await self.send_with_middleware(topic, serialized_payload, meta)

    async def send(self, topic: str, data: bytes, meta: dict[str, Any]) -> None:
        """Send raw bytes to MQTT after middleware processing.

        Matches Node.js: if (!this.client) return; (silent no-op)

        Args:
            topic: MQTT topic to publish to
            data: Serialized bytes
            meta: Metadata
        """
        if not self._client:
            return  # Silent no-op, matches Node.js behavior
        await self._client.publish(topic, data, qos=self.qos)

    async def subscribe(self, command: str, topic: str | None = None) -> None:
        """Subscribe to messages for a specific command.

        Args:
            command: Command type to subscribe to
            topic: Optional specific topic (node_id)

        Raises:
            RuntimeError: If not connected to MQTT broker
        """
        if not self._client:
            raise RuntimeError("Not connected to MQTT broker")

        topic_name = self.get_topic_name(command, topic)
        await self._client.subscribe(topic_name, qos=self.qos)
        logger.debug("MQTT subscribed to: %s (qos=%d)", topic_name, self.qos)

    @staticmethod
    def _parse_connection_string(connection_string: str) -> tuple[str, int]:
        """Parse MQTT connection URL into hostname and port.

        Supports: mqtt://host:port, mqtt://host, host:port, host

        Args:
            connection_string: MQTT broker URL or hostname

        Returns:
            Tuple of (hostname, port)
        """
        url = connection_string

        # Strip scheme
        for scheme in ("mqtt://", "mqtts://", "tcp://"):
            if url.startswith(scheme):
                url = url[len(scheme) :]
                break

        # Split host:port
        if ":" in url:
            host, port_str = url.rsplit(":", 1)
            try:
                port = int(port_str)
            except ValueError:
                host = url
                port = 1883
        else:
            host = url
            port = 1883

        return host, port

    @classmethod
    def from_config(
        cls: type["MqttTransporter"],
        config: dict[str, Any],
        transit: "Transit",
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
    ) -> "MqttTransporter":
        """Create an MQTT transporter from configuration.

        Extracts MQTT-specific options (qos, topicSeparator) from config,
        passes the rest to the MQTT client. Matches Node.js pattern.

        Args:
            config: Configuration dictionary containing connection details
            transit: Transit instance for message routing
            handler: Optional message handler function
            node_id: Optional node identifier

        Returns:
            Configured MqttTransporter instance

        Raises:
            KeyError: If required configuration keys are missing
        """
        try:
            connection_string = config["connection"]
        except KeyError:
            raise KeyError("MQTT configuration must include 'connection' key") from None

        # Extract MQTT-specific options (like Node.js: delete opts.qos, opts.topicSeparator)
        qos = config.get("qos", 0)
        topic_separator = config.get("topicSeparator", config.get("topic_separator", "."))

        return cls(
            connection_string=connection_string,
            transit=transit,
            handler=handler,
            node_id=node_id,
            qos=qos,
            topic_separator=topic_separator,
        )
