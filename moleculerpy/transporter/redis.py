"""Redis transporter implementation for the MoleculerPy framework.

This module provides a Redis-based transporter for inter-node communication
in a MoleculerPy cluster using Redis Pub/Sub messaging system.

Requires: redis>=5.0.0 (install with `pip install moleculerpy[redis]`)
"""

import asyncio
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from ..transit import Transit

from ..packet import Packet
from .base import Transporter

logger = logging.getLogger(__name__)
DISCONNECT_TIMEOUT_SECONDS = 5.0


class RedisConnectionError(RuntimeError):
    """Raised when Redis connection fails."""

    pass


class RedisTransporter(Transporter):
    """Redis transporter for MoleculerPy inter-node communication.

    This transporter uses Redis Pub/Sub as the messaging backbone
    for communication between MoleculerPy nodes in a distributed system.

    Redis is a common choice because:
    - Often already deployed for caching
    - Simple Pub/Sub model
    - Low latency
    - High throughput

    Example:
        >>> broker = ServiceBroker({
        ...     "transporter": {
        ...         "type": "redis",
        ...         "connection": "redis://localhost:6379"
        ...     }
        ... })
    """

    name = "redis"

    def __init__(
        self,
        connection_string: str,
        transit: "Transit",
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
    ) -> None:
        """Initialize the Redis transporter.

        Args:
            connection_string: Redis connection URL (e.g., redis://localhost:6379)
            transit: Transit instance for message routing
            handler: Optional message handler function
            node_id: Unique identifier for this node
        """
        super().__init__(self.name, transit=transit, handler=handler, node_id=node_id)
        self.connection_string = connection_string

        # Redis client instances
        self._redis: Any | None = None
        self._pubsub: Any | None = None

        # Listener task for receiving messages
        self._listener_task: asyncio.Task[None] | None = None

        # Track subscribed channels for cleanup
        self._subscribed_channels: set[str] = set()

        # Shutdown flag
        self._shutting_down = False

    def _is_connected(self) -> bool:
        """Check if Redis client is connected."""
        return self._redis is not None

    def _on_deserialize_error(self, cmd: str, error: Exception) -> None:
        """Override: Redis raises on decode error instead of dropping."""
        raise ValueError(f"Failed to decode message data: {error}") from error

    async def _listen_loop(self) -> None:
        """Listen for incoming Redis Pub/Sub messages.

        This runs as a background task and processes messages
        through the middleware chain.
        """
        if not self._pubsub:
            return

        try:
            async for message in self._pubsub.listen():
                if self._shutting_down:
                    break

                if message["type"] == "message":
                    await self._handle_message(message)

        except asyncio.CancelledError:
            # Normal shutdown
            pass
        except Exception:
            # Log error but don't crash the listener
            # In production, this should use proper logging
            if not self._shutting_down:
                raise

    async def _handle_message(self, message: dict[str, Any]) -> None:
        """Handle a single Redis Pub/Sub message.

        Args:
            message: Redis message dict with 'channel', 'data', 'type' keys
        """
        channel = message["channel"]
        data = message["data"]

        # Decode channel if bytes
        if isinstance(channel, bytes):
            channel = channel.decode("utf-8")

        # Extract command type from channel (MOL.{command}[.{node}])
        try:
            packet_type = Packet.from_topic(channel)
        except ValueError:
            logger.warning("Skipping Redis message from unknown topic: %s", channel)
            return
        if packet_type is None:
            logger.warning("Skipping Redis message with unresolved topic type: %s", channel)
            return

        # Pass raw bytes through middleware chain
        meta = {"channel": channel, "packet_type": packet_type}
        await self.receive_with_middleware(packet_type.value, data, meta)

    async def _await_with_timeout(self, awaitable: Any, operation: str) -> None:
        """Await a cleanup step with a fixed timeout and warning-level diagnostics."""
        try:
            await asyncio.wait_for(awaitable, timeout=DISCONNECT_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.warning(
                "Timed out during Redis transporter %s after %.1fs",
                operation,
                DISCONNECT_TIMEOUT_SECONDS,
            )

    async def send(self, topic: str, data: bytes, meta: dict[str, Any]) -> None:
        """Send raw bytes to Redis after middleware processing.

        This is the low-level send that actually publishes to Redis.

        Args:
            topic: Redis channel to publish to
            data: Serialized (and possibly compressed/encrypted) bytes
            meta: Metadata (packet reference, etc.)

        Raises:
            RuntimeError: If not connected to Redis
        """
        if not self._redis:
            raise RuntimeError("Not connected to Redis server")
        await self._redis.publish(topic, data)

    async def connect(self) -> None:
        """Establish connection to the Redis server.

        Creates both a regular client for publishing and a pubsub
        client for subscribing.

        Raises:
            ImportError: If redis package is not installed
            Exception: If connection fails
        """
        try:
            import redis.asyncio as redis  # noqa: PLC0415
        except ImportError as e:
            raise ImportError(
                "Redis transporter requires the 'redis' package. "
                "Install it with: pip install moleculerpy[redis]"
            ) from e

        try:
            redis_from_url = cast(Callable[..., Any], redis.from_url)
            self._redis = redis_from_url(
                self.connection_string,
                decode_responses=False,  # We handle encoding ourselves
            )
            # Test connection
            await self._redis.ping()

            # Create pubsub instance for subscriptions
            self._pubsub = self._redis.pubsub()

        except Exception as e:
            self._redis = None
            self._pubsub = None
            raise RedisConnectionError(f"Failed to connect to Redis server: {e}") from e

    async def disconnect(self) -> None:
        """Disconnect from the Redis server gracefully.

        Cancels the listener task, unsubscribes from all channels,
        and closes both pubsub and redis connections.
        """
        self._shutting_down = True

        # Cancel listener task
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._await_with_timeout(self._listener_task, "listener task cancellation")
            except asyncio.CancelledError:
                pass
            self._listener_task = None

        # Close pubsub
        if self._pubsub:
            try:
                # Unsubscribe from all channels
                if self._subscribed_channels:
                    await self._await_with_timeout(
                        self._pubsub.unsubscribe(*self._subscribed_channels),
                        "pubsub unsubscribe",
                    )
                    self._subscribed_channels.clear()
                # Use aclose() instead of close() (redis 5.0.1+)
                if hasattr(self._pubsub, "aclose"):
                    await self._await_with_timeout(self._pubsub.aclose(), "pubsub close")
                else:
                    await self._await_with_timeout(self._pubsub.close(), "pubsub close")
            except Exception as e:
                logger.debug("PubSub cleanup error (ignored): %s", e)
            finally:
                self._pubsub = None

        # Close redis client
        if self._redis:
            try:
                # Use aclose() instead of close() (redis 5.0.1+)
                if hasattr(self._redis, "aclose"):
                    await self._await_with_timeout(self._redis.aclose(), "redis close")
                else:
                    await self._await_with_timeout(self._redis.close(), "redis close")
            except Exception as e:
                logger.debug("Redis client cleanup error (ignored): %s", e)
            finally:
                self._redis = None

        self._shutting_down = False

    async def subscribe(self, command: str, topic: str | None = None) -> None:
        """Subscribe to messages for a specific command.

        Subscribes to both broadcast and node-specific channels.

        Args:
            command: Command type to subscribe to (e.g., REQ, RES, EVENT)
            topic: Optional specific topic (uses node_id if not provided)

        Raises:
            ValueError: If handler is not configured
            RuntimeError: If not connected to Redis
        """
        if not self._redis or not self._pubsub:
            raise RuntimeError("Not connected to Redis server")

        if self.handler is None:
            raise ValueError("Handler must be provided for subscription")

        # Subscribe to broadcast channel (all nodes)
        broadcast_channel = self.get_topic_name(command)
        await self._pubsub.subscribe(broadcast_channel)
        self._subscribed_channels.add(broadcast_channel)

        # Subscribe to node-specific channel
        target_node = topic if topic else self.node_id
        if target_node:
            node_channel = self.get_topic_name(command, target_node)
            await self._pubsub.subscribe(node_channel)
            self._subscribed_channels.add(node_channel)

        # Start listener task if not already running
        if self._listener_task is None or self._listener_task.done():
            self._listener_task = asyncio.create_task(
                self._listen_loop(),
                name="moleculerpy:redis-listener",
            )

    @classmethod
    def from_config(
        cls: type["RedisTransporter"],
        config: dict[str, Any],
        transit: "Transit",
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
    ) -> "RedisTransporter":
        """Create a Redis transporter from configuration.

        Args:
            config: Configuration dictionary containing connection details
            transit: Transit instance for message routing
            handler: Optional message handler function
            node_id: Optional node identifier

        Returns:
            Configured RedisTransporter instance

        Example config:
            {
                "connection": "redis://localhost:6379",
                "db": 0,  # Optional, default 0
            }

        Raises:
            KeyError: If required configuration keys are missing
        """
        try:
            connection_string = config["connection"]
        except KeyError:
            raise KeyError("Redis configuration must include 'connection' key") from None

        return cls(
            connection_string=connection_string,
            transit=transit,
            handler=handler,
            node_id=node_id,
        )
