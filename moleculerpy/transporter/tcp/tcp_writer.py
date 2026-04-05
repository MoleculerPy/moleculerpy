"""TCP Writer (client) with connection pool for outgoing messages.

Manages a pool of outgoing TCP connections keyed by nodeID.
Connections are created lazily on first send and evicted by LRU
when the pool exceeds max_connections.

Reference: sources/reference-implementations/moleculer/src/transporters/tcp/tcp-writer.js
"""

from __future__ import annotations

import asyncio
import logging
import socket
import time
from typing import TYPE_CHECKING, Any, Protocol

from .constants import (
    PACKET_GOSSIP_HELLO_ID,
    PACKET_GOSSIP_REQ_ID,
    PACKET_GOSSIP_RES_ID,
)
from .parser import build_frame

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class NodeAddressResolver(Protocol):
    """Protocol for resolving node connection info."""

    def get_node_address(self, node_id: str) -> tuple[str, int] | None: ...


class _Connection:
    """Wraps a single outgoing TCP connection."""

    __slots__ = ("last_used", "node_id", "reader", "writer")

    def __init__(
        self,
        node_id: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self.node_id = node_id
        self.reader = reader
        self.writer = writer
        self.last_used: float = time.monotonic()

    @property
    def is_alive(self) -> bool:
        """Check if the connection is still usable."""
        return not self.writer.is_closing()

    def close(self) -> None:
        """Close the connection."""
        try:
            if not self.writer.is_closing():
                self.writer.close()
        except Exception:
            pass


# Gossip packet types don't update lastUsed (same as Node.js)
_GOSSIP_TYPES: frozenset[int] = frozenset(
    {
        PACKET_GOSSIP_REQ_ID,
        PACKET_GOSSIP_RES_ID,
        PACKET_GOSSIP_HELLO_ID,
    }
)


class TcpWriter:
    """TCP connection pool for outgoing messages.

    Connections are created lazily when sending to a node. A LRU eviction
    strategy closes the oldest connections when the pool exceeds max_connections.

    On first connect, sends a GOSSIP_HELLO via the transporter's sendHello().
    """

    __slots__ = (
        "_connect_locks",
        "_connections",
        "_max_connections",
        "_on_end",
        "_on_error",
        "_on_hello",
        "_read_tasks",
        "_resolver",
        "logger",
    )

    def __init__(
        self,
        resolver: NodeAddressResolver,
        max_connections: int = 32,
        on_error: Any = None,
        on_end: Any = None,
        on_hello: Any = None,
    ) -> None:
        """Initialize TCP writer.

        Args:
            resolver: Object that resolves nodeID → (host, port).
            max_connections: Maximum number of live outgoing connections.
            on_error: Callback(node_id, error) for connection errors.
            on_end: Callback(node_id) when remote peer closes connection cleanly.
            on_hello: Async callback(node_id) called after first connection.
        """
        self._resolver = resolver
        self._max_connections = max_connections
        self._on_error = on_error
        self._on_end = on_end
        self._on_hello = on_hello
        self._connections: dict[str, _Connection] = {}
        self._connect_locks: dict[str, asyncio.Lock] = {}
        self._read_tasks: dict[str, asyncio.Task[None]] = {}
        self.logger = logger

    async def _connect(self, node_id: str) -> _Connection:
        """Establish a TCP connection to a remote node.

        Args:
            node_id: Target node identifier.

        Returns:
            A _Connection wrapper.

        Raises:
            ConnectionError: If the node address can't be resolved or connection fails.
        """
        addr = self._resolver.get_node_address(node_id)
        if addr is None:
            raise ConnectionError(f"Missing node info for '{node_id}'")

        host, port = addr
        self.logger.debug("Connecting to '%s' via %s:%d", node_id, host, port)

        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5.0)

        # TCP tuning (from Go reference: NoDelay + KeepAlive)
        sock: socket.socket | None = writer.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        conn = _Connection(node_id, reader, writer)
        self._connections[node_id] = conn

        self.logger.debug("Connected successfully to '%s'", node_id)

        # Monitor connection for EOF — detect when remote peer closes cleanly
        # (Node.js: socket.on("end", ...) → nodes.disconnected(nodeID, false))
        self._read_tasks[node_id] = asyncio.create_task(self._monitor_connection(node_id, reader))

        # Evict oldest connections if over limit
        if len(self._connections) > self._max_connections:
            self._manage_connections()

        return conn

    async def _monitor_connection(self, node_id: str, reader: asyncio.StreamReader) -> None:
        """Background task that detects when remote peer closes the connection.

        Reads until EOF, then calls on_end callback. Matches Node.js:
        socket.on("end", () => { this.emit("end", nodeID) })
        """
        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    # EOF — remote peer closed connection
                    self.logger.debug("TCP connection ended with '%s'", node_id)
                    self._remove_connection(node_id)
                    if self._on_end:
                        self._on_end(node_id)
                    break
        except (ConnectionResetError, BrokenPipeError, OSError):
            self._remove_connection(node_id)
            if self._on_end:
                self._on_end(node_id)
        except asyncio.CancelledError:
            pass

    async def send(self, node_id: str, packet_type_id: int, data: bytes) -> None:
        """Send a framed message to a remote node.

        Lazily connects if needed. Updates lastUsed for non-gossip packets.
        Sends GOSSIP_HELLO on first connection (after lock release to avoid
        deadlock: hello → publish → send → lock).

        Args:
            node_id: Target node.
            packet_type_id: Wire protocol packet type (1-8).
            data: Serialized payload bytes.

        Raises:
            ConnectionError: If connection to the node fails.
        """
        # Get or create per-node lock to prevent concurrent connect races
        if node_id not in self._connect_locks:
            self._connect_locks[node_id] = asyncio.Lock()

        is_new_connection = False
        async with self._connect_locks[node_id]:
            conn = self._connections.get(node_id)
            if conn is None or not conn.is_alive:
                conn = await self._connect(node_id)
                is_new_connection = True

            # Update lastUsed only for non-gossip packets (same as Node.js)
            if packet_type_id not in _GOSSIP_TYPES:
                conn.last_used = time.monotonic()

            # Build and send the framed packet INSIDE lock (prevents stale conn race)
            frame = build_frame(packet_type_id, data)
            try:
                conn.writer.write(frame)
                await conn.writer.drain()
            except (ConnectionResetError, BrokenPipeError, OSError) as e:
                self._remove_connection(node_id)
                if self._on_error:
                    self._on_error(node_id, e)
                raise

        # Send HELLO AFTER lock release to avoid deadlock
        # (hello → publish → send → would try to acquire same lock)
        if is_new_connection and self._on_hello:
            try:
                await self._on_hello(node_id)
            except Exception:
                self.logger.debug("Unable to send Gossip HELLO to %s", node_id)

    def _manage_connections(self) -> None:
        """Evict oldest connections when pool exceeds max_connections (LRU)."""
        excess = len(self._connections) - self._max_connections
        if excess <= 0:
            return

        # Sort by last_used ascending, close the oldest
        sorted_conns = sorted(
            self._connections.items(),
            key=lambda item: item[1].last_used,
        )
        to_remove = sorted_conns[:excess]

        self.logger.debug("Closing %d old TCP connections", len(to_remove))
        for node_id, conn in to_remove:
            conn.close()
            del self._connections[node_id]
            self._connect_locks.pop(node_id, None)
            task = self._read_tasks.pop(node_id, None)
            if task and not task.done():
                task.cancel()

    def _remove_connection(self, node_id: str) -> None:
        """Remove and close a connection by nodeID."""
        conn = self._connections.pop(node_id, None)
        if conn is not None:
            conn.close()
        self._connect_locks.pop(node_id, None)
        # Cancel EOF monitor task
        task = self._read_tasks.pop(node_id, None)
        if task and not task.done():
            task.cancel()

    async def close(self) -> None:
        """Close all outgoing TCP connections."""
        # Cancel all EOF monitor tasks
        for task in self._read_tasks.values():
            if not task.done():
                task.cancel()
        self._read_tasks.clear()

        for conn in self._connections.values():
            conn.close()
        self._connections.clear()
        self._connect_locks.clear()
        self.logger.debug("TCP writer closed")
