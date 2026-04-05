"""UDP Discovery Server for TCP Transporter.

Broadcasts/multicasts discovery messages so that TCP nodes can find
each other on the LAN without static configuration.

Message format: "namespace|nodeID|tcpPort" (plain text, pipe-delimited).

Reference: sources/reference-implementations/moleculer/src/transporters/tcp/udp-broadcaster.js
"""

from __future__ import annotations

import asyncio
import logging
import random
import socket
import struct
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


class UdpBroadcaster:
    """UDP multicast/broadcast server for zero-config node discovery.

    Supports two modes:
    1. Multicast — joins a multicast group (default: 239.0.0.0:4445)
    2. Broadcast — sends to subnet broadcast addresses

    On receiving a discovery message from another node, emits via
    on_message callback: (nodeID, address, port).
    """

    __slots__ = (
        "_counter",
        "_discover_timer",
        "_initial_discover_task",
        "_loop",
        "_namespace",
        "_node_id",
        "_on_message",
        "_opts",
        "_tcp_port",
        "_transports",
        "logger",
    )

    def __init__(
        self,
        namespace: str,
        node_id: str,
        tcp_port: int,
        opts: dict[str, Any],
        on_message: Callable[[str, str, int], Any],
    ) -> None:
        """Initialize UDP broadcaster.

        Args:
            namespace: Moleculer namespace for filtering messages.
            node_id: This node's ID.
            tcp_port: This node's TCP listening port.
            opts: UDP configuration options (from DEFAULT_OPTIONS).
            on_message: Callback(node_id, address, port) for discovered nodes.
        """
        self._namespace = namespace
        self._node_id = node_id
        self._tcp_port = tcp_port
        self._opts = opts
        self._on_message = on_message
        self._transports: list[tuple[asyncio.DatagramTransport, _UdpProtocol]] = []
        self._discover_timer: asyncio.Task[None] | None = None
        self._initial_discover_task: asyncio.Task[None] | None = None
        self._counter = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self.logger = logger

    async def bind(self) -> None:
        """Start UDP server(s) and begin discovery.

        Creates multicast and/or broadcast sockets depending on config.
        """
        if self._opts.get("udp_discovery") is False:
            self.logger.info("UDP Discovery is disabled")
            return

        self._loop = asyncio.get_running_loop()

        # Start multicast listener(s)
        multicast_addr = self._opts.get("udp_multicast")
        if multicast_addr:
            bind_addr = self._opts.get("udp_bind_address")
            if bind_addr:
                await self._start_server(
                    bind_addr,
                    self._opts.get("udp_port", 4445),
                    multicast_addr,
                    self._opts.get("udp_multicast_ttl", 1),
                )
            else:
                # Bind on all IPv4 interfaces
                for ip in self._get_interface_addresses():
                    await self._start_server(
                        ip,
                        self._opts.get("udp_port", 4445),
                        multicast_addr,
                        self._opts.get("udp_multicast_ttl", 1),
                    )

        # Start broadcast listener
        udp_broadcast = self._opts.get("udp_broadcast")
        if udp_broadcast:
            await self._start_server(
                self._opts.get("udp_bind_address") or "0.0.0.0",
                self._opts.get("udp_port", 4445),
            )

        # Send first discover after ~0.5-1s delay (use create_task to prevent GC)
        delay = random.randint(500, 1000) / 1000.0

        async def _delayed_discover() -> None:
            await asyncio.sleep(delay)
            await self._discover()

        self._initial_discover_task = asyncio.create_task(_delayed_discover())

        # Start periodic discovery
        self._start_discovering()

    async def _start_server(
        self,
        host: str,
        port: int,
        multicast_address: str | None = None,
        ttl: int = 1,
    ) -> None:
        """Start a single UDP socket (multicast or broadcast).

        Args:
            host: Local address to bind.
            port: UDP port.
            multicast_address: Multicast group to join (or None for broadcast).
            ttl: Multicast TTL.
        """
        try:
            # Pre-configure the raw socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # SO_REUSEPORT if available (Linux/macOS)
            if hasattr(socket, "SO_REUSEPORT"):
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                except OSError:
                    pass

            sock.bind((host, port))

            destinations: list[str] = []

            if multicast_address:
                # Join multicast group
                group = socket.inet_aton(multicast_address)
                local = socket.inet_aton(host)
                mreq = struct.pack("4s4s", group, local)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
                sock.setsockopt(
                    socket.IPPROTO_IP,
                    socket.IP_MULTICAST_TTL,
                    struct.pack("b", ttl),
                )
                sock.setsockopt(
                    socket.IPPROTO_IP,
                    socket.IP_MULTICAST_IF,
                    local,
                )
                destinations = [multicast_address]
                self.logger.info(
                    "UDP Multicast Server is listening on %s:%d. Membership: %s",
                    host,
                    port,
                    multicast_address,
                )
            else:
                # Broadcast mode
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                udp_broadcast = self._opts.get("udp_broadcast")
                if isinstance(udp_broadcast, str):
                    destinations = [udp_broadcast]
                elif isinstance(udp_broadcast, list):
                    destinations = udp_broadcast
                else:
                    destinations = self._get_broadcast_addresses()
                self.logger.info(
                    "UDP Broadcast Server is listening on %s:%d. Targets: %s",
                    host,
                    port,
                    ", ".join(destinations),
                )

            sock.setblocking(False)

            # Create asyncio datagram endpoint from pre-configured socket
            protocol = _UdpProtocol(self._namespace, self._node_id, self._on_message)
            protocol.destinations = destinations
            protocol.udp_port = port

            loop = asyncio.get_running_loop()
            transport, _ = await loop.create_datagram_endpoint(
                lambda: protocol,
                sock=sock,
            )

            self._transports.append((transport, protocol))

        except Exception as e:
            self.logger.warning("Unable to start UDP Discovery Server: %s", e)

    async def _discover(self) -> None:
        """Broadcast a discovery message to all destinations."""
        if not self._transports:
            return

        self._counter += 1
        message = f"{self._namespace}|{self._node_id}|{self._tcp_port}".encode()
        port = self._opts.get("udp_port", 4445)

        for transport, protocol in self._transports:
            if not protocol.destinations:
                continue
            for dest in protocol.destinations:
                try:
                    transport.sendto(message, (dest, port))
                    self.logger.debug("Discovery packet sent to '%s:%d'", dest, port)
                except OSError as e:
                    self.logger.warning("Discovery broadcast error to '%s:%d': %s", dest, port, e)

    def _start_discovering(self) -> None:
        """Start periodic discovery broadcasts."""
        if self._discover_timer is not None:
            return

        period = self._opts.get("udp_period", 30)
        max_discovery = self._opts.get("udp_max_discovery", 0)

        async def _discovery_loop() -> None:
            try:
                while True:
                    await asyncio.sleep(period)
                    await self._discover()
                    if max_discovery and self._counter >= max_discovery:
                        self.logger.info("UDP discovery stopped (max reached)")
                        break
            except asyncio.CancelledError:
                pass

        self._discover_timer = asyncio.create_task(_discovery_loop())
        self.logger.info("UDP discovery started (period: %ds)", period)

    async def close(self) -> None:
        """Stop discovery and close all UDP sockets."""
        # Cancel initial delayed discover if still pending
        if self._initial_discover_task is not None:
            self._initial_discover_task.cancel()
            try:
                await self._initial_discover_task
            except asyncio.CancelledError:
                pass
            self._initial_discover_task = None

        if self._discover_timer is not None:
            self._discover_timer.cancel()
            try:
                await self._discover_timer
            except asyncio.CancelledError:
                pass
            self._discover_timer = None

        for transport, _ in self._transports:
            transport.close()
        self._transports.clear()

        self.logger.debug("UDP broadcaster closed")

    @staticmethod
    def _get_interface_addresses() -> list[str]:
        """Get all IPv4 interface addresses on this machine.

        Uses psutil if available, falls back to socket.gethostbyname.
        No external dependency required (psutil is optional).
        """
        try:
            import psutil  # noqa: PLC0415

            addresses = []
            for _name, snics in psutil.net_if_addrs().items():
                for snic in snics:
                    if snic.family == socket.AF_INET:
                        addresses.append(snic.address)
            return addresses
        except ImportError:
            hostname = socket.gethostname()
            try:
                return [socket.gethostbyname(hostname)]
            except socket.gaierror:
                return ["127.0.0.1"]

    @staticmethod
    def _get_broadcast_addresses() -> list[str]:
        """Get IPv4 broadcast addresses for all interfaces.

        Uses psutil if available, falls back to 255.255.255.255.
        """
        try:
            import psutil  # noqa: PLC0415

            addresses = []
            for _name, snics in psutil.net_if_addrs().items():
                for snic in snics:
                    if snic.family == socket.AF_INET and snic.broadcast:
                        addresses.append(snic.broadcast)
            return addresses if addresses else ["255.255.255.255"]
        except ImportError:
            return ["255.255.255.255"]


class _UdpProtocol(asyncio.DatagramProtocol):
    """asyncio DatagramProtocol for receiving UDP discovery messages."""

    def __init__(
        self,
        namespace: str,
        local_node_id: str,
        on_message: Callable[[str, str, int], Any],
    ) -> None:
        self._namespace = namespace
        self._local_node_id = local_node_id
        self._on_message = on_message
        self.destinations: list[str] = []
        self.udp_port: int = 4445

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        """Process incoming UDP discovery message."""
        # Size cap: discovery messages are "ns|nodeid|port" — always short
        if len(data) > 512:  # noqa: PLR2004
            logger.debug("Oversized UDP packet from %s (%d bytes), dropping", addr[0], len(data))
            return
        try:
            msg = data.decode("utf-8")
            parts = msg.split("|")
            if len(parts) != 3:  # noqa: PLR2004
                logger.debug("Malformed UDP packet: %s", msg)
                return

            namespace, node_id, port_str = parts
            if namespace != self._namespace:
                return
            if node_id == self._local_node_id:
                return

            port = int(port_str)
            if not (1 <= port <= 65535):  # noqa: PLR2004
                logger.debug("Invalid port in UDP discovery: %s", port_str)
                return
            self._on_message(node_id, addr[0], port)
        except Exception as e:
            logger.debug("UDP packet processing error: %s", e)

    def error_received(self, exc: Exception) -> None:
        """Handle UDP socket errors."""
        logger.debug("UDP socket error: %s", exc)
