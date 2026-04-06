"""Kafka transporter implementation for the MoleculerPy framework.

High-throughput, durable messaging for data-intensive microservices.
Uses aiokafka for async producer/consumer with consumer groups.

Requires: aiokafka>=0.10.0 (install with `pip install moleculerpy[kafka]`)

Reference: sources/reference-implementations/moleculer/src/transporters/kafka.js (298 LOC)
"""

import asyncio
import logging
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..transit import Transit

from ..packet import Packet
from .base import SubscriptionTopic, Transporter

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
        super().__init__(self.name, transit=transit, handler=handler, node_id=node_id, prefix="MOL")
        self.bootstrap_servers = bootstrap_servers
        # Per-instance unique group_id (matches Node.js moleculer kafka.js line 186:
        # `groupId: this.broker.instanceID`). Each broker start gets its own
        # consumer group — ensures every broker sees all historical DISCOVER/INFO
        # messages from other nodes (via auto_offset_reset='earliest') without
        # replaying its own old messages on restart.
        self.group_id = group_id or f"moleculerpy-{node_id or 'default'}-{uuid.uuid4().hex[:12]}"
        self.partition = partition
        self.producer_config = producer_config or {}
        self.consumer_config = consumer_config or {}

        self._producer: Any | None = None  # AIOKafkaProducer (optional dep)
        self._consumer: Any | None = None  # AIOKafkaConsumer (optional dep)
        self._consume_task: asyncio.Task[None] | None = None
        self._shutting_down = False

    def _is_connected(self) -> bool:
        """Check if Kafka producer is connected."""
        return self._producer is not None

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

    async def _teardown_consumer(self) -> None:
        """Stop consumer and consume task. Reusable by disconnect + make_subscriptions."""
        if self._consume_task and not self._consume_task.done():
            self._consume_task.cancel()
            try:
                await self._consume_task
            except (asyncio.CancelledError, Exception):
                pass  # CancelledError is BaseException in 3.9+
        self._consume_task = None

        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception:
                logger.debug("Failed to stop Kafka consumer cleanly")
            self._consumer = None

    async def disconnect(self) -> None:
        """Disconnect producer and consumer."""
        self._shutting_down = True

        await self._teardown_consumer()

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

    async def make_subscriptions(self, topics: list["SubscriptionTopic"]) -> None:
        """Create all topic subscriptions at once via ConsumerGroup.

        Matches Node.js makeSubscriptions(): creates topics, then starts
        a single ConsumerGroup for all topics. Safe to call multiple times
        (tears down existing consumer first to prevent leaks).

        Args:
            topics: List of {"cmd": str, "nodeID": str|None} dicts
        """
        # Guard: tear down existing consumer before rebuilding (prevents
        # orphaned tasks/consumers on reconnect or repeated calls).
        if self._consumer is not None or self._consume_task is not None:
            await self._teardown_consumer()

        try:
            from aiokafka import AIOKafkaConsumer  # noqa: PLC0415
        except ImportError:
            raise ImportError(
                "The 'aiokafka' package is missing. Install it with: pip install aiokafka"
            ) from None

        topic_names = [self.get_topic_name(t["cmd"], t.get("nodeID")) for t in topics]

        logger.info("Kafka subscribing to %d topics.", len(topic_names))

        # Pre-create topics (matches Node.js producer.createTopics).
        # TopicAlreadyExistsError is expected; other errors are logged but
        # not fatal (Kafka auto.create.topics.enable may handle it).
        try:
            from aiokafka.admin import AIOKafkaAdminClient, NewTopic  # noqa: PLC0415
            from aiokafka.errors import TopicAlreadyExistsError  # noqa: PLC0415

            admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            await admin.start()
            try:
                new_topics = [
                    NewTopic(name=t, num_partitions=1, replication_factor=1) for t in topic_names
                ]
                await admin.create_topics(new_topics)
            except TopicAlreadyExistsError:
                pass  # Expected — topics pre-exist
            except Exception:
                logger.warning(
                    "Kafka topic creation failed; auto-create may handle it", exc_info=True
                )
            finally:
                await admin.close()
        except ImportError:
            logger.debug("Kafka AdminClient not available — skipping topic pre-creation")
        except Exception:
            logger.debug("Kafka AdminClient topic creation skipped (auto-create may handle it)")

        self._consumer = AIOKafkaConsumer(
            *topic_names,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            # "latest" matches Node.js Moleculer kafka.js line 187 (`fromOffset: "latest"`).
            # Each broker uses a unique group_id (broker.instanceID pattern), so no
            # committed offset exists → "latest" means start from end-of-stream.
            # Initial DISCOVER may race against subscribe, but the Moleculer protocol
            # recovers via periodic HEARTBEAT: receiving a heartbeat from an unknown
            # sender triggers a fresh DISCOVER round-trip. Avoiding "earliest" prevents
            # replaying stale messages from previous broker lifetimes (which would also
            # cause cross-serializer decode errors when topics are shared).
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
        """Background consume loop — routes messages to transit.

        On unrecoverable error, logs and sets _consumer to None so that
        the transporter is visibly disconnected. Matches Node.js pattern
        where consumer 'error' event triggers $transporter.error broadcast.
        """
        if not self._consumer:
            return

        try:
            async for message in self._consumer:
                if self._shutting_down:
                    break

                topic = message.topic
                # Extract command from topic — matches Node.js line 212
                parts = topic.split(".")
                if len(parts) < 2:  # noqa: PLR2004
                    logger.warning("Kafka: skipping message with short topic: %s", topic)
                    continue

                cmd = parts[1]

                # Resolve packet type from full topic string (avoids
                # reconstructing prefix + cmd which only works by coincidence)
                try:
                    packet_type = Packet.from_topic(topic)
                except (ValueError, AttributeError):
                    logger.warning("Kafka: unknown topic %s, skipping", topic)
                    continue
                if packet_type is None:
                    continue

                meta = {"topic": topic, "packet_type": packet_type, "kafka_message": message}
                await self.receive_with_middleware(cmd, message.value, meta)

        except asyncio.CancelledError:
            raise  # Propagate cancellation properly
        except Exception:
            if not self._shutting_down:
                logger.exception("Kafka consumer error — connection broken, consumer stopped")
                self._consumer = None

    async def send(self, topic: str, data: bytes, meta: dict[str, Any]) -> None:
        """Send raw bytes via Kafka producer.

        Matches Node.js: producer.send([{topic, messages, partition}]).
        """
        if not self._producer:
            logger.warning("Kafka send called without producer — message dropped (topic=%s)", topic)
            return

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

        consumer_cfg_raw: dict[str, Any] = config.get("consumer") or {}
        producer_cfg: dict[str, Any] = config.get("producer") or {}
        publish_cfg: dict[str, Any] = config.get("publish") or {}

        # Extract group_id before passing to AIOKafkaConsumer to prevent
        # TypeError from duplicate kwarg (group_id passed explicitly + via **kwargs).
        group_id = consumer_cfg_raw.get("group_id", consumer_cfg_raw.get("groupId"))
        consumer_cfg = {
            k: v for k, v in consumer_cfg_raw.items() if k not in ("group_id", "groupId")
        }

        return cls(
            bootstrap_servers=bootstrap_servers,
            transit=transit,
            handler=handler,
            node_id=node_id,
            group_id=group_id,
            partition=publish_cfg.get("partition", 0),
            producer_config=producer_cfg,
            consumer_config=consumer_cfg,
        )
