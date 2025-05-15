from enum import Enum
from functools import lru_cache
from typing import Any


class Topic(Enum):
    """Enumeration of message topic types in the MoleculerPy framework."""

    HEARTBEAT = "HEARTBEAT"
    EVENT = "EVENT"
    EVENT_ACK = "EVENTACK"  # Phase 3C: Event Acknowledgment response
    DISCONNECT = "DISCONNECT"
    DISCOVER = "DISCOVER"
    INFO = "INFO"
    REQUEST = "REQ"
    RESPONSE = "RES"
    # Phase 5: Latency measurement
    PING = "PING"
    PONG = "PONG"


class Packet:
    """Represents a message packet in the MoleculerPy framework.

    Attributes:
        type: The topic type of the packet
        target: The target node or service for the packet
        payload: The data payload of the packet
        sender: The sender node ID (set by transporter)
    """

    __slots__ = ("payload", "sender", "target", "type")

    def __init__(self, topic: Topic, target: str | None, payload: Any) -> None:
        """Initialize a new Packet.

        Args:
            topic: The topic type of the packet
            target: The target node or service
            payload: The data payload
        """
        self.type = topic
        self.target: str | None = target
        self.payload = payload
        self.sender: str | None = None  # Will be set by transporter

    @staticmethod
    def from_topic(topic: str) -> Topic | None:
        """Parse a topic string and return the corresponding Topic enum.

        Args:
            topic: The topic string to parse (format: 'prefix.TOPIC_TYPE.suffix')

        Returns:
            The corresponding Topic enum value, or None if parsing fails

        Raises:
            ValueError: If the topic string is invalid or topic type is not recognized
        """
        if not topic:
            raise ValueError("Topic string cannot be empty")

        second_dot = topic.find(".")
        if second_dot == -1:
            raise ValueError(f"Invalid topic format: {topic}")
        topic_type = topic[second_dot + 1 :].split(".", 1)[0]
        return _topic_from_type(topic_type)


@lru_cache(maxsize=32)
def _topic_from_type(topic_type: str) -> Topic:
    """Resolve topic type token to Topic enum with a tiny cache for hot paths."""
    try:
        return Topic(topic_type)
    except (ValueError, KeyError) as e:
        raise ValueError(f"Unknown topic type: {topic_type}") from e
