"""AMQP 0-9-1 transporter implementation for the MoleculerPy framework.

This module provides an AMQP-based transporter (RabbitMQ) for inter-node
communication with built-in load balancing through work queues, ACK/NACK
message acknowledgment, and fanout exchanges for broadcast messages.

Requires: aio-pika>=9.0.0 (install with `pip install moleculerpy[amqp]`)

Reference: sources/reference-implementations/moleculer/src/transporters/amqp.js (491 LOC)
"""

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..transit import Transit

from ..packet import Packet, Topic
from .base import Transporter

# Moleculer protocol version (must match transit.PROTOCOL_VERSION)
PROTOCOL_VERSION: str = "4"

logger = logging.getLogger(__name__)

# Default queue expiry: 2 minutes (matches Node.js)
AUTO_DELETE_QUEUES_DEFAULT_MS = 2 * 60 * 1000

# Packet types that need ACK (at-least-once delivery)
_ACK_PACKET_TYPES = frozenset({Topic.REQUEST})

# Packet types with autoDelete=True queues (ephemeral)
_AUTO_DELETE_PACKET_TYPES = frozenset(
    {
        Topic.HEARTBEAT,
        Topic.DISCOVER,
        Topic.DISCONNECT,
        Topic.INFO,
        Topic.PING,
        Topic.PONG,
    }
)


class AmqpTransporter(Transporter):
    """AMQP 0-9-1 transporter for MoleculerPy (RabbitMQ).

    Uses fanout exchanges for broadcast packets and direct queues for
    targeted/balanced packets. Supports ACK/NACK for request reliability.

    Key differences from MQTT/NATS:
    - hasBuiltInBalancer = True (RabbitMQ work queues replace software balancer)
    - Two send paths: exchange publish (broadcast) vs sendToQueue (targeted/balanced)
    - ACK/NACK for REQUEST packets (at-least-once delivery)

    Example:
        >>> broker = ServiceBroker("node-1", settings=Settings(
        ...     transporter="amqp://guest:guest@localhost:5672"
        ... ))
    """

    name = "amqp"
    has_built_in_balancer: bool = True

    def __init__(
        self,
        urls: list[str],
        transit: "Transit",
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
        *,
        prefetch: int = 1,
        event_time_to_live: int | None = None,
        heartbeat_time_to_live: int | None = None,
        auto_delete_queues: int = AUTO_DELETE_QUEUES_DEFAULT_MS,
        queue_options: dict[str, Any] | None = None,
        exchange_options: dict[str, Any] | None = None,
        message_options: dict[str, Any] | None = None,
        consume_options: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the AMQP transporter.

        Args:
            urls: List of AMQP broker URLs for cluster failover
            transit: Transit instance for message routing
            handler: Message handler function
            node_id: Unique identifier for this node
            prefetch: Messages handled concurrently per channel (default: 1)
            event_time_to_live: TTL for event messages in ms (default: None)
            heartbeat_time_to_live: TTL for heartbeat messages in ms (default: None)
            auto_delete_queues: Queue expiry in ms (default: 120000, -1 to disable)
            queue_options: Extra options merged into assertQueue calls
            exchange_options: Extra options for assertExchange calls
            message_options: Extra options for publish/sendToQueue calls
            consume_options: Extra options for channel.consume calls
        """
        super().__init__(self.name)
        self.urls = urls
        self.transit = transit
        self.handler = handler
        self.node_id = node_id

        # AMQP options (matching Node.js constructor)
        self.prefetch = prefetch
        self.event_time_to_live = event_time_to_live
        self.heartbeat_time_to_live = heartbeat_time_to_live
        self.auto_delete_queues = auto_delete_queues
        self.queue_options = queue_options or {}
        self.exchange_options = exchange_options or {}
        self.message_options = message_options or {}
        self.consume_options = consume_options or {}

        # Connection state
        self._connection: Any | None = None
        self._channel: Any | None = None
        self._bindings: list[tuple[str, str, str]] = []
        self._connection_count = 0
        self._connect_attempt = 0
        self._disconnecting = False

        # Topic prefix
        self.prefix = "MOL"

    def get_topic_name(self, command: str, node_id: str | None = None) -> str:
        """Generate topic/queue name. Matches Node.js getTopicName()."""
        topic = f"{self.prefix}.{command}"
        if node_id:
            topic += f".{node_id}"
        return topic

    def _get_queue_options(
        self, packet_type: Topic, balanced_queue: bool = False
    ) -> dict[str, Any]:
        """Get assertQueue options per packet type. Matches Node.js _getQueueOptions().

        Args:
            packet_type: The packet type (Topic enum)
            balanced_queue: Whether this is a balanced work queue

        Returns:
            Dict of queue options for assertQueue
        """
        options: dict[str, Any] = {}

        if packet_type == Topic.REQUEST:
            if self.auto_delete_queues >= 0 and not balanced_queue:
                options["expires"] = self.auto_delete_queues
        elif packet_type == Topic.RESPONSE:
            if self.auto_delete_queues >= 0:
                options["expires"] = self.auto_delete_queues
        elif packet_type in (Topic.EVENT, Topic.EVENT_ACK):
            if self.auto_delete_queues >= 0:
                options["expires"] = self.auto_delete_queues
            if self.event_time_to_live:
                options["message_ttl"] = self.event_time_to_live
        elif packet_type == Topic.HEARTBEAT:
            options["auto_delete"] = True
            if self.heartbeat_time_to_live:
                options["message_ttl"] = self.heartbeat_time_to_live
        elif packet_type in _AUTO_DELETE_PACKET_TYPES:
            options["auto_delete"] = True

        # Merge user queue options
        return {**options, **self.queue_options}

    async def connect(self) -> None:
        """Establish connection to AMQP broker.

        Lazily imports aio-pika. Supports cluster failover via round-robin URLs.

        Raises:
            ImportError: If aio-pika is not installed
            Exception: If connection fails
        """
        # Guard against reconnect
        if self._connection is not None:
            await self.disconnect()

        try:
            import aio_pika  # noqa: PLC0415
        except ImportError:
            raise ImportError(
                "The 'aio-pika' package is missing. Install it with: pip install aio-pika"
            ) from None

        # Round-robin URL selection for cluster failover
        self._connect_attempt += 1
        url_index = (self._connect_attempt - 1) % len(self.urls)
        url = self.urls[url_index]

        self._disconnecting = False

        # Connect
        self._connection = await aio_pika.connect_robust(url)
        logger.info("AMQP is connected to %s.", url)

        # Create channel with prefetch
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=self.prefetch)
        logger.info("AMQP channel created (prefetch=%d).", self.prefetch)

        self._connection_count += 1

    async def disconnect(self) -> None:
        """Disconnect from AMQP broker gracefully.

        Unbinds queues first (exchanges/queues NOT deleted — may contain messages).
        """
        self._disconnecting = True
        self._connection_count = 0

        # Unbind all fanout bindings
        if self._channel and self._bindings:
            for queue_name, exchange_name, routing_key in self._bindings:
                try:
                    queue = await self._channel.get_queue(queue_name, ensure=False)
                    await queue.unbind(exchange_name, routing_key)
                except Exception:
                    logger.debug("Failed to unbind %s from %s", queue_name, exchange_name)

        # Close channel
        if self._channel:
            try:
                await self._channel.close()
            except Exception:
                logger.debug("Failed to close AMQP channel cleanly")
            self._channel = None

        # Close connection
        if self._connection:
            try:
                await self._connection.close()
            except Exception:
                logger.debug("Failed to close AMQP connection cleanly")
            self._connection = None

        self._bindings = []
        logger.info("AMQP disconnected.")

    async def subscribe(self, command: str, topic: str | None = None) -> None:
        """Subscribe to a command. Matches Node.js subscribe() logic.

        Two patterns:
        - topic (nodeID) is set: direct queue, no exchange (targeted packets)
        - topic is None: fanout exchange + per-node queue (broadcast packets)

        Args:
            command: Command type (REQ, RES, EVENT, DISCOVER, etc.)
            topic: Optional node_id for targeted subscriptions
        """
        if not self._channel:
            raise RuntimeError("Not connected to AMQP broker")

        import aio_pika  # noqa: PLC0415

        topic_name = self.get_topic_name(command, topic)

        # Resolve packet type for queue options
        try:
            packet_type = Packet.from_topic(f"{self.prefix}.{command}")
        except (ValueError, AttributeError):
            packet_type = None

        if topic is not None:
            # Targeted: direct queue, no exchange
            need_ack = packet_type in _ACK_PACKET_TYPES if packet_type else False
            q_opts = self._get_queue_options(packet_type, False) if packet_type else {}

            queue = await self._channel.declare_queue(
                topic_name,
                durable=not q_opts.get("auto_delete", False),
                **self._filter_queue_args(q_opts),
            )
            await queue.consume(
                self._make_consumer(command, need_ack),
                no_ack=not need_ack,
            )
        else:
            # Broadcast: fanout exchange + per-node queue + bind
            queue_name = f"{self.prefix}.{command}.{self.node_id}"
            q_opts = self._get_queue_options(packet_type, False) if packet_type else {}

            exchange = await self._channel.declare_exchange(
                topic_name, aio_pika.ExchangeType.FANOUT, **self.exchange_options
            )
            queue = await self._channel.declare_queue(
                queue_name,
                durable=not q_opts.get("auto_delete", False),
                **self._filter_queue_args(q_opts),
            )
            await queue.bind(exchange)

            # Save binding for cleanup on disconnect
            self._bindings.append((queue_name, topic_name, ""))

            await queue.consume(
                self._make_consumer(command, False),
                no_ack=True,
            )

        logger.debug("AMQP subscribed to: %s", topic_name)

    async def subscribe_balanced_request(self, action: str) -> None:
        """Subscribe to balanced request queue for an action.

        Creates shared work queue: {prefix}.REQB.{action}
        Multiple nodes consume from same queue — RabbitMQ round-robins.

        Args:
            action: Action name (e.g., "math.add")
        """
        if not self._channel:
            raise RuntimeError("Not connected to AMQP broker")

        queue_name = f"{self.prefix}.REQB.{action}"
        q_opts = self._get_queue_options(Topic.REQUEST, balanced_queue=True)

        queue = await self._channel.declare_queue(
            queue_name, durable=True, **self._filter_queue_args(q_opts)
        )
        await queue.consume(
            self._make_consumer("REQ", need_ack=True),
            no_ack=False,
        )
        logger.debug("AMQP balanced request: %s", queue_name)

    async def subscribe_balanced_event(self, event: str, group: str) -> None:
        """Subscribe to balanced event queue for a group.

        Creates shared work queue: {prefix}.EVENTB.{group}.{event}

        Args:
            event: Event name
            group: Consumer group name
        """
        if not self._channel:
            raise RuntimeError("Not connected to AMQP broker")

        queue_name = f"{self.prefix}.EVENTB.{group}.{event}"
        q_opts = self._get_queue_options(Topic.EVENT, balanced_queue=True)

        queue = await self._channel.declare_queue(
            queue_name, durable=True, **self._filter_queue_args(q_opts)
        )
        await queue.consume(
            self._make_consumer("EVENT", need_ack=True),
            no_ack=False,
        )
        logger.debug("AMQP balanced event: %s", queue_name)

    async def publish(self, packet: "Packet") -> None:
        """Publish a packet. Serializes and routes through middleware."""
        if not self._channel:
            raise RuntimeError("Not connected to AMQP broker")

        topic = self.get_topic_name(packet.type.value, packet.target)
        payload = {**packet.payload, "ver": PROTOCOL_VERSION, "sender": self.node_id}
        serialized = await self.transit.serializer.serialize_async(payload)

        meta = {"packet": packet, "balanced": False}
        await self.send_with_middleware(topic, serialized, meta)

    async def publish_balanced_request(self, packet: Packet) -> None:
        """Publish balanced request to shared work queue."""
        if not self._channel:
            raise RuntimeError("Not connected to AMQP broker")

        action = packet.payload.get("action", "")
        if not action:
            logger.warning("Cannot publish balanced request: missing action")
            return

        topic = f"{self.prefix}.REQB.{action}"
        payload = {**packet.payload, "ver": PROTOCOL_VERSION, "sender": self.node_id}
        data = await self.transit.serializer.serialize_async(payload)
        await self.send_with_middleware(topic, data, {"packet": packet, "balanced": True})

    async def publish_balanced_event(self, packet: Packet, group: str) -> None:
        """Publish balanced event to shared group queue."""
        if not self._channel:
            raise RuntimeError("Not connected to AMQP broker")

        event = packet.payload.get("event", "")
        if not event:
            logger.warning("Cannot publish balanced event: missing event")
            return

        topic = f"{self.prefix}.EVENTB.{group}.{event}"
        payload = {**packet.payload, "ver": PROTOCOL_VERSION, "sender": self.node_id}
        data = await self.transit.serializer.serialize_async(payload)
        await self.send_with_middleware(topic, data, {"packet": packet, "balanced": True})

    async def unsubscribe_from_balanced_commands(self) -> None:
        """Unsubscribe balanced queues. No-op — queues persist in RabbitMQ."""
        pass

    async def send(self, topic: str, data: bytes, meta: dict[str, Any]) -> None:
        """Send raw bytes to AMQP. Matches Node.js send() routing logic.

        Two paths:
        - targeted/balanced → sendToQueue (direct queue delivery)
        - broadcast → publish to exchange (fanout to all subscribers)

        Args:
            topic: Queue or exchange name
            data: Serialized bytes
            meta: Must contain 'packet' and 'balanced' keys
        """
        if not self._channel:
            return  # Silent no-op, matches Node.js

        import aio_pika  # noqa: PLC0415

        packet = meta.get("packet")
        balanced = meta.get("balanced", False)

        message = aio_pika.Message(body=data)

        if (packet and packet.target is not None) or balanced:
            # Direct queue send (targeted or balanced)
            await self._channel.default_exchange.publish(message, routing_key=topic)
        else:
            # Fanout exchange publish (broadcast)
            # Declare exchange (idempotent) before publish — matches Node.js channel.publish()
            exchange = await self._channel.declare_exchange(
                topic, aio_pika.ExchangeType.FANOUT, **self.exchange_options
            )
            await exchange.publish(message, routing_key="")

    async def receive(self, cmd: str, data: bytes, meta: dict[str, Any]) -> None:
        """Process received bytes after middleware. Deserialize and call handler."""
        try:
            payload = await self.transit.serializer.deserialize_async(data)
        except Exception as e:
            logger.warning("Failed to decode AMQP message, dropping: %r", e)
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

    def _make_consumer(self, cmd: str, need_ack: bool = False) -> Callable[..., Any]:
        """Create a message consumer callback. Matches Node.js _consumeCB().

        Args:
            cmd: Command type for routing
            need_ack: Whether to ACK/NACK messages

        Returns:
            Async callback for queue.consume()
        """
        try:
            packet_type = Packet.from_topic(f"{self.prefix}.{cmd}")
        except (ValueError, AttributeError):
            packet_type = None

        async def consumer(message: Any) -> None:
            meta = {"packet_type": packet_type, "amqp_message": message}

            if need_ack:
                try:
                    await self.receive_with_middleware(cmd, message.body, meta)
                    if self._channel:
                        await message.ack()
                except Exception:
                    logger.exception("AMQP message handling error, nacking")
                    if self._channel:
                        await message.nack()
            else:
                await self.receive_with_middleware(cmd, message.body, meta)

        return consumer

    @staticmethod
    def _filter_queue_args(opts: dict[str, Any]) -> dict[str, Any]:
        """Filter queue options to aio-pika compatible kwargs.

        Converts Node.js style options to aio-pika declare_queue kwargs.
        """
        result: dict[str, Any] = {}
        arguments: dict[str, Any] = {}

        if "auto_delete" in opts:
            result["auto_delete"] = opts["auto_delete"]
        if "expires" in opts:
            arguments["x-expires"] = opts["expires"]
        if "message_ttl" in opts:
            arguments["x-message-ttl"] = opts["message_ttl"]

        if arguments:
            result["arguments"] = arguments

        return result

    @classmethod
    def from_config(
        cls: type["AmqpTransporter"],
        config: dict[str, Any],
        transit: "Transit",
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
    ) -> "AmqpTransporter":
        """Create AMQP transporter from configuration.

        Supports:
        - config["connection"] = "amqp://host" (single URL)
        - config["connection"] = "amqp://h1;amqp://h2" (cluster, semicolon-separated)
        - config["url"] = [...] (list of URLs)

        Args:
            config: Configuration dictionary
            transit: Transit instance
            handler: Message handler
            node_id: Node identifier

        Returns:
            Configured AmqpTransporter
        """
        # Parse URLs
        raw_url = config.get("connection", config.get("url", "amqp://localhost"))
        if isinstance(raw_url, list):
            urls = raw_url
        elif isinstance(raw_url, str):
            urls = [u.strip() for u in raw_url.split(";") if u.strip()]
        else:
            urls = ["amqp://localhost"]

        if not urls:
            urls = ["amqp://localhost"]

        return cls(
            urls=urls,
            transit=transit,
            handler=handler,
            node_id=node_id,
            prefetch=config.get("prefetch", 1),
            event_time_to_live=config.get("eventTimeToLive", config.get("event_time_to_live")),
            heartbeat_time_to_live=config.get(
                "heartbeatTimeToLive", config.get("heartbeat_time_to_live")
            ),
            auto_delete_queues=cls._parse_auto_delete(
                config.get("autoDeleteQueues", config.get("auto_delete_queues", True))
            ),
            queue_options=config.get("queueOptions", config.get("queue_options", {})),
            exchange_options=config.get("exchangeOptions", config.get("exchange_options", {})),
            message_options=config.get("messageOptions", config.get("message_options", {})),
            consume_options=config.get("consumeOptions", config.get("consume_options", {})),
        )

    @staticmethod
    def _parse_auto_delete(value: bool | int) -> int:
        """Parse autoDeleteQueues value. Matches Node.js normalization."""
        if value is True:
            return AUTO_DELETE_QUEUES_DEFAULT_MS
        if value is False:
            return -1
        if isinstance(value, int):
            return value
        return AUTO_DELETE_QUEUES_DEFAULT_MS
