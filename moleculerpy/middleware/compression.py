"""
Compression Middleware for the MoleculerPy framework.

This middleware compresses transit messages to reduce bandwidth usage.
Supports deflate, gzip, and zlib compression methods.

Example:
    from moleculerpy import ServiceBroker, Settings
    from moleculerpy.middleware.compression import CompressionMiddleware

    settings = Settings(
        middlewares=[
            CompressionMiddleware(method="deflate", threshold=1024)
        ]
    )
    broker = ServiceBroker("node-1", settings=settings)
"""

import asyncio
import logging
import zlib
from collections.abc import Awaitable, Callable
from enum import Enum
from typing import Any

from .base import Middleware

logger = logging.getLogger(__name__)

# Compression flag bytes
FLAG_NOT_COMPRESSED = b"\x00"
FLAG_COMPRESSED = b"\x01"


class CompressionMethod(Enum):
    """Supported compression methods."""

    DEFLATE = "deflate"
    GZIP = "gzip"
    ZLIB = "zlib"


class CompressionMiddleware(Middleware):
    """Middleware that compresses transit messages for bandwidth reduction.

    This middleware wraps the transporter send/receive methods to compress
    outgoing messages and decompress incoming messages.

    Args:
        method: Compression method - "deflate", "gzip", or "zlib". Default: "deflate"
        threshold: Minimum size in bytes to trigger compression. Default: 1024 (1KB)
            Messages smaller than threshold are sent uncompressed with a flag byte.
        level: Compression level (1-9). Default: 6 (balanced speed/ratio)

    Example:
        middleware = CompressionMiddleware(
            method="deflate",
            threshold=1024,
            level=6
        )
    """

    _EXECUTOR_THRESHOLD_BYTES = 1024 * 1024

    def __init__(
        self,
        method: str | CompressionMethod = CompressionMethod.DEFLATE,
        threshold: int = 1024,
        level: int = 6,
    ) -> None:
        """Initialize the compression middleware.

        Args:
            method: Compression method (CompressionMethod enum or string)
            threshold: Minimum size in bytes to compress (0 = always compress)
            level: Compression level 1-9 (1=fast, 9=best compression)

        Raises:
            ValueError: If method is not supported
        """
        # Normalize method to enum
        if isinstance(method, str):
            try:
                method = CompressionMethod(method)
            except ValueError:
                raise ValueError(
                    f"Unknown compression method: {method}. "
                    f"Use: {', '.join(m.value for m in CompressionMethod)}"
                ) from None

        self.method = method
        self.threshold = threshold
        self.level = level

        # Select compression/decompression functions based on method
        match method:
            case CompressionMethod.DEFLATE:
                self._wbits = -zlib.MAX_WBITS  # Raw deflate (no header)
            case CompressionMethod.GZIP:
                self._wbits = zlib.MAX_WBITS | 16  # gzip format
            case CompressionMethod.ZLIB:
                self._wbits = zlib.MAX_WBITS  # zlib format with header

        logger.info(
            "Compression middleware initialized: method=%s, threshold=%d bytes, level=%d",
            method.value,
            threshold,
            level,
        )

    def _compress(self, data: bytes) -> bytes:
        """Compress data using configured method.

        Args:
            data: Raw bytes to compress

        Returns:
            Compressed bytes
        """
        compress_obj = zlib.compressobj(self.level, zlib.DEFLATED, self._wbits)
        return compress_obj.compress(data) + compress_obj.flush()

    def _decompress(self, data: bytes) -> bytes:
        """Decompress data using configured method.

        Args:
            data: Compressed bytes

        Returns:
            Decompressed bytes

        Raises:
            zlib.error: If decompression fails
        """
        decompress_obj = zlib.decompressobj(self._wbits)
        return decompress_obj.decompress(data) + decompress_obj.flush()

    def broker_created(self, broker: Any) -> None:
        """Log compression settings when broker is created.

        Args:
            broker: The broker instance
        """
        logger.info(
            "Transmission COMPRESSION enabled: method='%s', threshold=%d bytes",
            self.method.value,
            self.threshold,
        )

    def transporter_send(
        self, next_send: Callable[[str, bytes, dict[str, Any]], Awaitable[None]]
    ) -> Callable[[str, bytes, dict[str, Any]], Awaitable[None]]:
        """Wrap transporter send to compress outgoing data.

        Compresses data if it exceeds threshold, otherwise sends with
        a "not compressed" flag byte.

        Args:
            next_send: The next send function in the chain

        Returns:
            Wrapped send function
        """

        async def send(topic: str, data: bytes, meta: dict[str, Any]) -> None:
            # Skip compression for small messages
            if self.threshold > 0 and len(data) < self.threshold:
                logger.debug(
                    "Packet '%s' too small (%d bytes), sending uncompressed",
                    topic,
                    len(data),
                )
                # Prepend "not compressed" flag
                return await next_send(topic, FLAG_NOT_COMPRESSED + data, meta)

            # Compress the data
            if len(data) >= self._EXECUTOR_THRESHOLD_BYTES:
                loop = asyncio.get_running_loop()
                compressed = await loop.run_in_executor(None, self._compress, data)
            else:
                compressed = self._compress(data)

            # Calculate compression ratio
            original_size = len(data)
            compressed_size = len(compressed)
            savings = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0.0

            logger.debug(
                "Packet '%s' compressed: %d -> %d bytes (%.0f%% savings)",
                topic,
                original_size,
                compressed_size,
                savings,
            )

            # Prepend "compressed" flag
            return await next_send(topic, FLAG_COMPRESSED + compressed, meta)

        return send

    def transporter_receive(
        self, next_receive: Callable[[str, bytes, dict[str, Any]], Awaitable[None]]
    ) -> Callable[[str, bytes, dict[str, Any]], Awaitable[None]]:
        """Wrap transporter receive to decompress incoming data.

        Checks the flag byte and decompresses if necessary.

        Args:
            next_receive: The next receive function in the chain

        Returns:
            Wrapped receive function
        """

        async def receive(cmd: str, data: bytes, meta: dict[str, Any]) -> None:
            if len(data) < 1:
                logger.warning("Received empty packet for cmd '%s'", cmd)
                return await next_receive(cmd, data, meta)

            # Check compression flag
            is_compressed = data[0]
            payload = data[1:]  # Remove flag byte

            if is_compressed == 0:
                # Not compressed
                logger.debug(
                    "Packet '%s' was not compressed (%d bytes)",
                    cmd,
                    len(payload),
                )
                return await next_receive(cmd, payload, meta)

            # Decompress
            try:
                loop = asyncio.get_running_loop()
                decompressed = await loop.run_in_executor(None, self._decompress, payload)
                compressed_size = len(payload)
                original_size = len(decompressed)
                savings = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0.0

                logger.debug(
                    "Packet '%s' decompressed: %d -> %d bytes (%.0f%% savings)",
                    cmd,
                    compressed_size,
                    original_size,
                    savings,
                )
                return await next_receive(cmd, decompressed, meta)

            except zlib.error as e:
                logger.error(
                    "Decompression failed for packet '%s': %s",
                    cmd,
                    e,
                )
                raise

        return receive
