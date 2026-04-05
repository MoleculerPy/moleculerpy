"""TCP Reader (server) for incoming connections.

Listens on a TCP port, accepts incoming connections, parses framed
messages using FrameParser, and dispatches them to the transporter.

Reference: sources/reference-implementations/moleculer/src/transporters/tcp/tcp-reader.js
"""

from __future__ import annotations

import asyncio
import logging
import socket
from typing import TYPE_CHECKING, Any, Protocol

from .parser import FrameError, FrameParser

if TYPE_CHECKING:
    from ...packet import Topic

logger = logging.getLogger(__name__)


class IncomingMessageHandler(Protocol):
    """Protocol for the transporter callback."""

    async def on_incoming_message(
        self, topic: Topic, data: bytes, socket_info: dict[str, Any]
    ) -> None: ...


class TcpReader:
    """TCP server that accepts incoming connections and parses frames.

    Each incoming connection gets its own FrameParser reading loop.
    Parsed messages are dispatched to the transporter via on_incoming_message.

    Reference: Node.js TcpReader — net.createServer() + Parser pipe.
    Python: asyncio.start_server() + FrameParser.read_frames().
    """

    __slots__ = (
        "_handler",
        "_max_incoming",
        "_max_packet_size",
        "_port",
        "_server",
        "_sockets",
        "logger",
    )

    def __init__(
        self,
        handler: IncomingMessageHandler,
        port: int | None = None,
        max_packet_size: int = 1_048_576,
        max_incoming_connections: int = 256,
    ) -> None:
        """Initialize TCP reader.

        Args:
            handler: Object with on_incoming_message() method (the transporter).
            port: TCP port to listen on. None = OS-assigned random port.
            max_packet_size: Maximum frame size for the parser.
            max_incoming_connections: Max concurrent incoming TCP connections.
        """
        self._handler = handler
        self._port = port or 0  # 0 = OS picks a random port
        self._max_packet_size = max_packet_size
        self._max_incoming = max_incoming_connections
        self._server: asyncio.Server | None = None
        self._sockets: list[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = []
        self.logger = logger

    @property
    def port(self) -> int:
        """Return the actual listening port (useful when port=0)."""
        if self._server is None:
            return self._port
        sockets = self._server.sockets
        if sockets:
            return int(sockets[0].getsockname()[1])
        return self._port

    async def listen(self) -> int:
        """Start listening for TCP connections.

        Returns:
            The actual port number (important when using random port).
        """
        self._server = await asyncio.start_server(
            self._on_client_connected,
            host="0.0.0.0",
            port=self._port,
            reuse_address=True,
        )
        actual_port = self.port
        self._port = actual_port
        self.logger.info("TCP server is listening on port %d", actual_port)
        return actual_port

    async def _on_client_connected(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle a new incoming TCP connection.

        Creates a FrameParser and reads frames until the connection closes.
        """
        peer = writer.get_extra_info("peername")
        address = peer[0] if peer else "unknown"

        # Reject if too many incoming connections (DoS protection)
        if len(self._sockets) >= self._max_incoming:
            self.logger.warning(
                "Max incoming connections reached (%d), rejecting %s", self._max_incoming, address
            )
            self._close_writer(writer)
            return

        self.logger.debug("New TCP client connected from '%s'", address)
        self._sockets.append((reader, writer))
        parser = FrameParser(max_packet_size=self._max_packet_size)

        socket_info: dict[str, Any] = {"remote_address": address}

        # Set TCP_NODELAY on the accepted socket
        sock: socket.socket | None = writer.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        try:
            async for topic, data in parser.read_frames(reader):
                await self._handler.on_incoming_message(topic, data, socket_info)
        except asyncio.IncompleteReadError:
            self.logger.debug("TCP client disconnected from '%s'", address)
        except FrameError as e:
            self.logger.warning("Packet parser error from '%s': %s", address, e)
        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            self.logger.debug("TCP client '%s' error: %s", address, e)
        finally:
            self._close_writer(writer)
            self._sockets = [(r, w) for r, w in self._sockets if w is not writer]

    def _close_writer(self, writer: asyncio.StreamWriter) -> None:
        """Safely close a StreamWriter."""
        try:
            if not writer.is_closing():
                writer.close()
        except Exception:
            pass

    async def close(self) -> None:
        """Stop the TCP server and close all client connections."""
        # Snapshot and clear to avoid concurrent modification
        sockets, self._sockets = self._sockets, []
        for _reader, writer in sockets:
            self._close_writer(writer)

        # Stop accepting new connections
        if self._server is not None:
            self._server.close()
            try:
                await asyncio.wait_for(self._server.wait_closed(), timeout=2.0)
            except TimeoutError:
                pass
            self._server = None

        self.logger.debug("TCP reader closed")
