"""Kafka transporter implementation for the MoleculerPy framework.

High-throughput, durable messaging for data-intensive microservices.
Uses aiokafka for async producer/consumer with consumer groups.

Requires: aiokafka>=0.10.0 (install with `pip install moleculerpy[kafka]`)

Reference: sources/reference-implementations/moleculer/src/transporters/kafka.js (298 LOC)
"""

import asyncio
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..transit import Transit

from ..packet import Packet
from .base import Transporter

PROTOCOL_VERSION: str = "4"

logger = logging.getLogger(__name__)


class KafkaTransporter(Transporter):
    """Kafka transporter for MoleculerPy.

    Uses a single ConsumerGroup for all topics (batch subscription via
    makeSubscriptions). hasBuiltInBalancer = False — Moleculer software
    balancer handles request distribution.

    Key difference from NATS/MQTT/AMQP: subscribe() is a no-op.
    All subscriptions happen in make_subscriptions() which creates
    topics + starts a single ConsumerGroup.

    Example:
        >>> broker = ServiceBroker("node-1", settings=Settings(
        ...     transporter="kafka://localhost:9092"
        ... ))
    """

    name = "kafka"
    has_built_in_balancer: bool = False

    def __init__(
        self,
        bootstrap_servers: str,
        transit: "Transit",
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
        *,
        group_id: str | None = None,
        partition: int = 0,
        producer_config: dict[str, Any] | None = None,
        consumer_config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize Kafka transporter.

        Args:
            bootstrap_servers: Kafka broker address (e.g., localhost:9092)
            transit: Transit instance
            handler: Message handler
            node_id: Node identifier
            group_id: Consumer group ID (default: node_id)
            partition: Default partition for publish (default: 0)
            producer_config: Extra AIOKafkaProducer kwargs
            consumer_config: Extra AIOKafkaConsumer kwargs
        """
        super().__init__(self.name)
        self.bootstrap_servers = bootstrap_servers
        self.transit = transit
        self.handler = handler
        self.node_id = node_id
        self.group_id = group_id or node_id or "moleculerpy"
        self.partition = partition
        self.producer_config = producer_config or {}
        self.consumer_config = consumer_config or {}

        self._producer: Any | None = None
        self._consumer: Any | None = None
        self._consume_task: Any | None = None
        self._shutting_down = False

        self.prefix = "MOL"

    def get_topic_name(self, command: str, node_id: str | None = None) -> str:
        """Generate Kafka topic name. Matches Node.js getTopicName()."""
        topic = f"{self.prefix}.{command}"
        if node_id:
            topic += f".{node_id}"
        return topic

    async def connect(self) -> None:
        """Connect to Kafka — create producer.

        Consumer is created later in make_subscriptions().
        Matches Node.js: producer.on("ready") → onConnected().
        """
        if self._producer is not None:
            await self.disconnect()

        try:
            from aiokafka import AIOKafkaProducer  # noqa: PLC0415
        except ImportError:
            raise ImportError(
                "The 'aiokafka' package is missing. Install it with: pip install aiokafka"
            ) from None

        self._shutting_down = False
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            **self.producer_config,
        )
        await self._producer.start()
        logger.info("Kafka producer connected to %s.", self.bootstrap_servers)

    async def disconnect(self) -> None:
        """Disconnect producer and consumer."""
        self._shutting_down = True

        # Stop consumer
        if self._consume_task and not self._consume_task.done():
            self._consume_task.cancel()
            try:
                await self._consume_task
            except Exception:
                pass
        self._consume_task = None

        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception:
                logger.debug("Failed to stop Kafka consumer cleanly")
            self._consumer = None

        # Stop producer
        if self._producer:
            try:
                await self._producer.stop()
            except Exception:
                logger.debug("Failed to stop Kafka producer cleanly")
            self._producer = None

        logger.info("Kafka disconnected.")

    async def subscribe(self, command: str, topic: str | None = None) -> None:
        """No-op for Kafka. Subscriptions handled by make_subscriptions().

        Kafka uses a single ConsumerGroup for all topics, created in
        make_subscriptions() after connect. Individual subscribe() calls
        are not supported by the Kafka consumer model.
        """
        pass

    async def make_subscriptions(self, topics: list[dict[str, Any]]) -> None:
        """Create all topic subscriptions at once via ConsumerGroup.

        Matches Node.js makeSubscriptions(): creates topics, then starts
        a single ConsumerGroup for all topics.

        Args:
            topics: List of {"cmd": str, "nodeID": str|None} dicts
        """
        try:
            from aiokafka import AIOKafkaConsumer  # noqa: PLC0415
        except ImportError:
            raise ImportError(
                "The 'aiokafka' package is missing. Install it with: pip install aiokafka"
            ) from None

        topic_names = [self.get_topic_name(t["cmd"], t.get("nodeID")) for t in topics]

        logger.info("Kafka subscribing to %d topics.", len(topic_names))

        # Pre-create topics (matches Node.js producer.createTopics)
        try:
            from aiokafka.admin import AIOKafkaAdminClient, NewTopic  # noqa: PLC0415

            admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            await admin.start()
            try:
                new_topics = [
                    NewTopic(name=t, num_partitions=1, replication_factor=1) for t in topic_names
                ]
                await admin.create_topics(new_topics)
            except Exception:
                pass  # Topics may already exist — safe to ignore
            finally:
                await admin.close()
        except Exception:
            logger.debug("Kafka AdminClient topic creation skipped (auto-create may handle it)")

        self._consumer = AIOKafkaConsumer(
            *topic_names,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="latest",
            value_deserializer=lambda x: x,  # raw bytes
            **self.consumer_config,
        )
        await self._consumer.start()

        # Start background consume loop
        self._consume_task = asyncio.create_task(self._consume_loop())
        logger.info(
            "Kafka consumer started (group=%s, topics=%d).", self.group_id, len(topic_names)
        )

    async def _consume_loop(self) -> None:
        """Background consume loop — routes messages to transit."""
        if not self._consumer:
            return

        try:
            async for message in self._consumer:
                if self._shutting_down:
                    break

                topic = message.topic
                # Extract command: topic.split(".")[1] — matches Node.js line 212
                parts = topic.split(".")
                if len(parts) < 2:  # noqa: PLR2004
                    logger.warning("Kafka: skipping message with short topic: %s", topic)
                    continue

                cmd = parts[1]

                # Resolve packet type
                try:
                    packet_type = Packet.from_topic(f"{self.prefix}.{cmd}")
                except (ValueError, AttributeError):
                    logger.warning("Kafka: unknown topic %s, skipping", topic)
                    continue
                if packet_type is None:
                    continue

                meta = {"topic": topic, "packet_type": packet_type, "kafka_message": message}
                await self.receive_with_middleware(cmd, message.value, meta)

        except asyncio.CancelledError:
            pass  # Normal shutdown
        except Exception:
            if not self._shutting_down:
                logger.exception("Kafka consumer error — connection may be broken")
                self._consumer = None

    async def receive(self, cmd: str, data: bytes, meta: dict[str, Any]) -> None:
        """Deserialize and call handler."""
        try:
            payload = await self.transit.serializer.deserialize_async(data)
        except Exception as e:
            logger.warning("Failed to decode Kafka message, dropping: %r", e)
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
        """Publish packet via Kafka producer."""
        if not self._producer:
            raise RuntimeError("Not connected to Kafka")

        topic = self.get_topic_name(packet.type.value, packet.target)
        payload = {**packet.payload, "ver": PROTOCOL_VERSION, "sender": self.node_id}
        serialized = await self.transit.serializer.serialize_async(payload)

        meta = {"packet": packet}
        await self.send_with_middleware(topic, serialized, meta)

    async def send(self, topic: str, data: bytes, meta: dict[str, Any]) -> None:
        """Send raw bytes via Kafka producer.

        Matches Node.js: producer.send([{topic, messages, partition}]).
        """
        if not self._producer:
            return  # Silent no-op

        await self._producer.send_and_wait(
            topic,
            value=data,
            partition=self.partition,
        )

    @classmethod
    def from_config(
        cls: type["KafkaTransporter"],
        config: dict[str, Any],
        transit: "Transit",
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
    ) -> "KafkaTransporter":
        """Create Kafka transporter from configuration.

        Supports:
        - config["connection"] = "kafka://host:9092"
        - config["host"] = "host:9092"
        """
        raw = config.get("connection", config.get("host", "localhost:9092"))
        bootstrap_servers = raw.replace("kafka://", "")

        consumer_cfg = config.get("consumer", {})
        producer_cfg = config.get("producer", {})
        publish_cfg = config.get("publish", {})

        return cls(
            bootstrap_servers=bootstrap_servers,
            transit=transit,
            handler=handler,
            node_id=node_id,
            group_id=consumer_cfg.get("group_id", consumer_cfg.get("groupId")),
            partition=publish_cfg.get("partition", 0),
            producer_config=producer_cfg,
            consumer_config=consumer_cfg,
        )
