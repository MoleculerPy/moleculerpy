"""
Transit Logger Middleware for the MoleculerPy framework.

This middleware logs all transit messages (sent and received packets)
for debugging and monitoring purposes.

Example:
    from moleculerpy import ServiceBroker, Settings
    from moleculerpy.middleware.transit_logger import TransitLoggerMiddleware

    settings = Settings(
        middlewares=[
            TransitLoggerMiddleware(
                log_level="debug",
                log_packet_data=True,
                packet_filter=["HEARTBEAT"]
            )
        ]
    )
    broker = ServiceBroker("node-1", settings=settings)
"""

import asyncio
import json
import logging
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from .base import Middleware

logger = logging.getLogger(__name__)


class TransitLoggerMiddleware(Middleware):
    """Middleware that logs all transit messages for debugging.

    This middleware logs outgoing packets via the transit_publish hook.
    It can optionally write packet data to files for offline analysis.

    Args:
        log_level: Log level to use ("debug", "info", "warning"). Default: "info"
        log_packet_data: Whether to log full packet payload. Default: False
        folder: Optional folder path to save packet data as JSON files.
        packet_filter: List of packet types to skip (e.g., ["HEARTBEAT"]).
            Default: ["HEARTBEAT"] to reduce log noise.

    Example:
        middleware = TransitLoggerMiddleware(
            log_level="debug",
            log_packet_data=True,
            folder="/tmp/transit-logs",
            packet_filter=["HEARTBEAT", "PING", "PONG"]
        )
    """

    def __init__(
        self,
        log_level: str = "info",
        log_packet_data: bool = False,
        folder: str | Path | None = None,
        packet_filter: list[str] | None = None,
    ) -> None:
        """Initialize the transit logger middleware.

        Args:
            log_level: Logging level ("debug", "info", "warning")
            log_packet_data: Whether to include packet payload in logs
            folder: Optional folder to save packets as JSON files
            packet_filter: Packet types to exclude from logging
        """
        self.log_level = log_level.lower()
        self.log_packet_data = log_packet_data
        self.folder = Path(folder) if folder else None
        self.packet_filter: set[str] = set(packet_filter or ["HEARTBEAT"])

        self._node_id: str | None = None
        self._log_fn: Callable[..., None] | None = None
        self._target_folder: Path | None = None

        # Validate log level
        level_map = {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING}
        if self.log_level not in level_map:
            raise ValueError(f"Invalid log_level: {log_level}. Use: debug, info, warning")

        logger.info(
            "Transit logger middleware initialized: level=%s, log_data=%s, filter=%s",
            self.log_level,
            self.log_packet_data,
            list(self.packet_filter),
        )

    def broker_created(self, broker: Any) -> None:
        """Set up logging when broker is created.

        Args:
            broker: The broker instance
        """
        self._node_id = getattr(broker, "node_id", "unknown")

        # Set up log function based on level
        level_fn = {
            "debug": logger.debug,
            "info": logger.info,
            "warning": logger.warning,
        }
        self._log_fn = level_fn.get(self.log_level, logger.info)

        # Create output folder if specified
        if self.folder:
            node_folder = self.folder / self._node_id
            node_folder.mkdir(parents=True, exist_ok=True)
            self._target_folder = node_folder
            logger.info("Transit logs will be saved to: %s", self._target_folder)
        else:
            self._target_folder = None

    def _save_to_file_sync(self, filename: str, payload: dict[str, Any]) -> None:
        """Save packet payload to a JSON file (synchronous).

        This is the blocking I/O operation that runs in a thread pool.

        Args:
            filename: Name of the file (without folder path)
            payload: Packet payload to save
        """
        if not self._target_folder:
            return

        try:
            filepath = self._target_folder / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, default=str)
        except Exception as e:
            logger.warning("Failed to save transit log file: %s", e)

    async def _save_to_file_async(self, filename: str, payload: dict[str, Any]) -> None:
        """Save packet payload to a JSON file (async, non-blocking).

        Uses asyncio.to_thread to avoid blocking the event loop.

        Args:
            filename: Name of the file (without folder path)
            payload: Packet payload to save
        """
        if not self._target_folder:
            return

        await asyncio.to_thread(self._save_to_file_sync, filename, payload)

    async def transit_publish(self, next_publish: Any, packet: Any) -> Any:
        """Log outgoing packets.

        Args:
            next_publish: Next handler in the chain
            packet: The packet being published

        Returns:
            Result from next handler
        """
        # Check packet filter
        packet_type = getattr(packet, "type", None)
        type_value = str(getattr(packet_type, "value", packet_type))

        if type_value in self.packet_filter:
            return await next_publish(packet)

        # Get target
        target = getattr(packet, "target", None) or "<all nodes>"

        # Log the packet
        if self._log_fn:
            self._log_fn("=> Send %s packet to '%s'", type_value, target)

            if self.log_packet_data:
                payload = getattr(packet, "payload", {})
                self._log_fn("=> Payload: %s", payload)

        # Save to file if configured (non-blocking)
        if self._target_folder:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"{timestamp}-send-{type_value}-to-{target.replace('.', '_')}.json"
            payload = getattr(packet, "payload", {})
            await self._save_to_file_async(filename, payload)

        return await next_publish(packet)

    async def stopped(self) -> None:
        """Clean up when middleware is stopped."""
        logger.debug("Transit logger middleware stopped")
