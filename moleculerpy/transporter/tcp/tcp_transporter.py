"""TCP Transporter with Gossip Protocol for MoleculerPy.

Peer-to-peer transporter using fault-tolerant Gossip Protocol for node
discovery without an external message broker. All nodes are equal — no
leader or controller — enabling truly horizontal scaling.

Architecture:
    - TcpReader: accepts incoming TCP connections, parses frames
    - TcpWriter: manages outgoing connection pool with LRU eviction
    - UdpBroadcaster: zero-config discovery via multicast/broadcast
    - Gossip: replaces HEARTBEAT/DISCOVER/INFO with state-exchange protocol

Reference: sources/reference-implementations/moleculer/src/transporters/tcp.js (781 LOC)
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from ...packet import Packet, Topic
from ...serializers import to_packet_type
from ..base import PROTOCOL_VERSION, Transporter
from .constants import (
    DEFAULT_OPTIONS,
    resolve_packet_id,
)
from .tcp_reader import TcpReader
from .tcp_writer import TcpWriter
from .udp_broadcaster import UdpBroadcaster

if TYPE_CHECKING:
    from ...transit import Transit


logger = logging.getLogger(__name__)


class TcpTransporter(Transporter):
    """TCP Transporter with Gossip Protocol.

    Uses direct TCP connections for messaging and Gossip Protocol for
    peer-to-peer service discovery. No external broker needed.

    Key differences from other transporters:
    - subscribe() is a no-op (no pub/sub topics)
    - publish() only sends to targeted packets (unicast)
    - Gossip replaces HEARTBEAT/DISCOVER/INFO/DISCONNECT
    - discoverer.disableHeartbeat() is called on init

    Example:
        >>> broker = ServiceBroker(Settings(transporter="tcp://host1:port/node1,host2:port/node2"))
    """

    name = "tcp"
    has_built_in_balancer: bool = False

    def __init__(
        self,
        transit: Transit,
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
        opts: dict[str, Any] | None = None,
    ) -> None:
        """Initialize TCP transporter.

        Args:
            transit: Transit instance for serialization and message handling.
            handler: Message handler callback (from Transit).
            node_id: This node's unique identifier.
            opts: Configuration options (merged with DEFAULT_OPTIONS).
        """
        super().__init__(self.name, transit=transit, handler=handler, node_id=node_id or "")
        self.logger = logger

        # Merge user options with defaults
        self.opts: dict[str, Any] = {**DEFAULT_OPTIONS, **(opts or {})}

        # Components (initialized in connect)
        self.reader: TcpReader | None = None
        self.writer: TcpWriter | None = None
        self.udp_server: UdpBroadcaster | None = None

        # Gossip timer
        self._gossip_timer: asyncio.Task[None] | None = None

        # Connection state
        self.connected: bool = False

        # Debug logging for gossip (set via opts["debug"])
        self._gossip_debug: bool = bool(self.opts.get("debug", False))

        # Node registry references (set in _init_registry)
        self._registry: Any = None
        self._nodes: Any = None
        self._discoverer: Any = None

    def _init_registry(self) -> None:
        """Initialize references to broker registry components.

        Called during connect() to get access to node catalog, discoverer, etc.
        NodeCatalog API: get_node(), add_node(), disconnect_node(), process_node_info()
        """
        # NodeCatalog is on transit directly, not on registry
        self._nodes = getattr(self.transit, "node_catalog", None)

        broker = getattr(self.transit, "_broker", None)
        if broker is not None:
            self._registry = broker.registry
            # Also try to get node_catalog from registry if not on transit
            if self._nodes is None:
                self._nodes = getattr(broker.registry, "node_catalog", None)
            self._discoverer = getattr(broker.registry, "discoverer", None)

            # Disable normal heartbeat logic — gossip replaces it
            if self._discoverer and hasattr(self._discoverer, "disable_heartbeat"):
                self._discoverer.disable_heartbeat()

    # ------------------------------------------------------------------
    # Transporter ABC implementation
    # ------------------------------------------------------------------

    def _is_connected(self) -> bool:
        """TCP is connected when writer is available."""
        return self.writer is not None

    async def connect(self) -> None:
        """Start TCP server, UDP discovery, and gossip timers."""
        self._init_registry()

        # Load static URLs if configured
        if self.opts.get("urls"):
            self._load_urls()

        # Start TCP server (reader) and client pool (writer)
        await self._start_tcp_server()

        # Start UDP discovery
        await self._start_udp_server()

        # Start gossip timer
        self._start_timers()

        self.connected = True

        # Set the TCP port on local node (may be random)
        if self._nodes and self._nodes.local_node and self.reader:
            self._nodes.local_node.port = self.reader.port
            # Increment seq to ensure gossip recognizes us as "newer" than offline default
            self._nodes.local_node.seq = max(self._nodes.local_node.seq, 1)
            # Regenerate node INFO with updated port
            self._nodes.ensure_local_node()

        self.logger.info(
            "TCP Transporter started on port %s", self.reader.port if self.reader else "?"
        )

    async def disconnect(self) -> None:
        """Stop TCP/UDP servers and close all connections."""
        self.connected = False

        # Stop gossip first — cancel timer and await completion
        await self._stop_timers()

        # Close writer first (outgoing connections) — unblocks any pending sends
        if self.writer:
            await self.writer.close()
            self.writer = None

        # Close reader (incoming server) — unblocks read loops
        if self.reader:
            await self.reader.close()
            self.reader = None

        if self.udp_server:
            await self.udp_server.close()
            self.udp_server = None

        self.logger.info("TCP Transporter stopped")

    async def subscribe(self, command: str, topic: str | None = None) -> None:
        """No-op — TCP doesn't use pub/sub topics."""
        return

    async def publish(self, packet: Packet) -> None:
        """Publish a packet via TCP.

        Only sends targeted packets (EVENT, REQUEST, RESPONSE, PING, PONG,
        GOSSIP_*). Broadcast packets without a target are silently dropped.

        Args:
            packet: Packet to publish.
        """
        # Only send targeted packets that TCP supports
        assert self.transit is not None  # guaranteed after connect
        if not packet.target:
            return

        supported = {
            Topic.EVENT,
            Topic.REQUEST,
            Topic.RESPONSE,
            Topic.PING,
            Topic.PONG,
            Topic.GOSSIP_REQ,
            Topic.GOSSIP_RES,
            Topic.GOSSIP_HELLO,
        }
        if packet.type not in supported:
            return

        # Serialize payload — pass packet_type for schema-based serializers (ProtoBuf)
        payload = {**packet.payload, "ver": PROTOCOL_VERSION, "sender": self.node_id}
        data = await self.transit.serializer.serialize_async(
            payload, packet_type=to_packet_type(packet.type.value)
        )

        # Send via middleware chain
        meta: dict[str, Any] = {"packet": packet}
        await self.send_with_middleware(packet.type.value, data, meta)

    async def send(self, topic: str, data: bytes, meta: dict[str, Any]) -> None:
        """Send framed data to a target node via TCP.

        Args:
            topic: Packet type string (used to resolve wire ID).
            data: Serialized payload bytes.
            meta: Must contain {"packet": Packet} with target nodeID.
        """
        if not self.writer:
            return

        packet: Packet | None = meta.get("packet")
        if packet is None or not packet.target:
            return

        packet_id = resolve_packet_id(packet.type)
        try:
            await self.writer.send(packet.target, packet_id, data)
        except (ConnectionError, OSError) as err:
            # Node disconnected — unexpected error (Node.js line 775: true)
            if self._nodes and hasattr(self._nodes, "disconnect_node"):
                self._nodes.disconnect_node(packet.target, unexpected=True)
            raise err

    async def receive(self, cmd: str, data: bytes, meta: dict[str, Any]) -> None:
        """Process received raw bytes — deserialize and dispatch.

        Args:
            cmd: Command type string.
            data: Raw serialized bytes.
            meta: Metadata.
        """
        assert self.transit is not None
        try:
            payload = await self.transit.serializer.deserialize_async(
                data, packet_type=to_packet_type(cmd)
            )
            packet = Packet(Topic(cmd), None, payload)
            packet.sender = payload.get("sender")

            if self.handler:
                await self.handler(packet)
        except Exception as e:
            self.logger.warning("Error processing incoming message: %s", e)

    # ------------------------------------------------------------------
    # TCP Server
    # ------------------------------------------------------------------

    async def _start_tcp_server(self) -> None:
        """Start TCP reader and writer components."""
        self.writer = TcpWriter(
            resolver=self,
            max_connections=self.opts.get("max_connections", 32),
            on_error=self._on_writer_error,
            on_end=self._on_writer_end,
            on_hello=self._send_hello,
        )

        self.reader = TcpReader(
            handler=self,
            port=self.opts.get("port"),
            max_packet_size=self.opts.get("max_packet_size", 1_048_576),
            max_incoming_connections=self.opts.get("max_incoming", 256),
        )

        await self.reader.listen()

    def _on_writer_error(self, node_id: str, error: Exception) -> None:
        """Handle TCP writer connection errors."""
        self.logger.debug("TCP client error on '%s': %s", node_id, error)
        if self._nodes and hasattr(self._nodes, "disconnect_node"):
            # Expected disconnect (Node.js line 147-148: false)
            self._nodes.disconnect_node(node_id, unexpected=False)

    def _on_writer_end(self, node_id: str) -> None:
        """Handle TCP connection closed cleanly by remote peer.

        Node.js: writer.on("end", nodeID => nodes.disconnected(nodeID, false))
        """
        self.logger.debug("TCP connection ended with '%s'", node_id)
        if self._nodes and hasattr(self._nodes, "disconnect_node"):
            self._nodes.disconnect_node(node_id, unexpected=False)

    # ------------------------------------------------------------------
    # NodeAddressResolver protocol (used by TcpWriter)
    # ------------------------------------------------------------------

    def get_node_address(self, node_id: str) -> tuple[str, int] | None:
        """Resolve a nodeID to (host, port) for TCP connection.

        Args:
            node_id: Target node identifier.

        Returns:
            Tuple of (host, port) or None if node is unknown.
        """
        if not self._nodes:
            return None

        node = self._nodes.get_node(node_id)
        if node is None:
            return None

        host = self._get_node_host(node)
        port = getattr(node, "port", None)

        if not host or not port:
            self.logger.warning("Node %s has no valid address", node_id)
            return None

        return (host, port)

    def _get_node_host(self, node: Any) -> str | None:
        """Get the best address for a node.

        Priority: udp_address > hostname (if use_hostname) > first IP in ipList.
        """
        udp_addr: str | None = getattr(node, "udp_address", None)
        if udp_addr:
            return udp_addr

        if self.opts.get("use_hostname", True):
            hostname: str | None = getattr(node, "hostname", None)
            if hostname:
                return hostname

        ip_list: list[str] | None = getattr(node, "ipList", None)
        if ip_list and len(ip_list) > 0:
            return str(ip_list[0])

        return None

    # ------------------------------------------------------------------
    # IncomingMessageHandler protocol (used by TcpReader)
    # ------------------------------------------------------------------

    async def on_incoming_message(
        self, topic: Topic, data: bytes, socket_info: dict[str, Any]
    ) -> None:
        """Process incoming TCP message — route gossip or normal packets.

        Gossip packets (HELLO/REQ/RES) are handled directly by the transporter.
        Normal packets (EVENT/REQ/RES/PING/PONG) go through the transit handler.

        Args:
            topic: Parsed packet topic from frame header.
            data: Serialized payload bytes.
            socket_info: Connection metadata (remote_address, etc.).
        """
        if topic == Topic.GOSSIP_HELLO:
            await self._process_gossip_hello(data, socket_info)
        elif topic == Topic.GOSSIP_REQ:
            await self._process_gossip_request(data)
        elif topic == Topic.GOSSIP_RES:
            await self._process_gossip_response(data)
        else:
            # Normal packet — deserialize and dispatch through transit
            await self.receive_with_middleware(topic.value, data, {})

    # ------------------------------------------------------------------
    # UDP Discovery
    # ------------------------------------------------------------------

    async def _start_udp_server(self) -> None:
        """Start UDP broadcaster for auto-discovery."""
        tcp_port = self.reader.port if self.reader else 0
        namespace = getattr(self.transit, "settings", None)
        ns = getattr(namespace, "namespace", "") if namespace else ""

        self.udp_server = UdpBroadcaster(
            namespace=ns,
            node_id=self.node_id or "",
            tcp_port=tcp_port,
            opts=self.opts,
            on_message=self._on_udp_message,
        )

        await self.udp_server.bind()

    def _on_udp_message(self, node_id: str, address: str, port: int) -> None:
        """Handle incoming UDP discovery message.

        Registers unknown nodes as offline so gossip can exchange info.

        Args:
            node_id: Remote node ID.
            address: Remote IP address.
            port: Remote TCP port.
        """
        if not node_id or node_id == self.node_id:
            return

        if not self._nodes:
            return

        node = self._nodes.get_node(node_id)
        if node is None:
            # Unknown node — register as offline
            node = self._add_offline_node(node_id, address, port)
        elif not getattr(node, "available", True):
            # Known offline node — update connection data
            node.port = port
            node.hostname = address
            ip_list = getattr(node, "ipList", [])
            if address not in ip_list:
                ip_list.insert(0, address)

        # Track UDP address for connection priority
        node.udp_address = address

    # ------------------------------------------------------------------
    # Static URL Loading
    # ------------------------------------------------------------------

    def _load_urls(self) -> None:
        """Load static node URLs from configuration.

        Supports (matching Node.js tcp.js loadUrls):
        - String: "host:port/nodeID,host2:port2/nodeID2"
        - Array: ["host:port/nodeID", ...]
        - Dict: {"nodeID": "host:port", ...}
        - File: "file:///path/to/nodes.txt" (one URL per line, or JSON)
        """
        urls = self.opts.get("urls")
        if not urls:
            return

        # file:// support (Node.js: fs.readFileSync)
        if isinstance(urls, str) and urls.startswith("file://"):
            fname = urls[7:]  # Strip "file://"
            self.logger.debug("Loading node list from file '%s'", fname)
            try:
                import json  # noqa: PLC0415
                import pathlib  # noqa: PLC0415

                content = pathlib.Path(fname).read_text().strip()
                if not content:
                    return
                if content.startswith("{") or content.startswith("["):
                    urls = json.loads(content)
                else:
                    urls = [line.strip() for line in content.splitlines() if line.strip()]
            except Exception as e:
                self.logger.warning("Failed to load URLs from file '%s': %s", fname, e)
                return

        # Dict format: {"nodeID": "host:port"} → ["host:port/nodeID"]
        if isinstance(urls, dict):
            urls = [f"{addr}/{node_id}" for node_id, addr in urls.items()]

        # Comma-separated string → list
        if isinstance(urls, str):
            urls = [u.strip() for u in urls.split(",") if u.strip()]

        for raw_url in urls:
            if not raw_url:
                continue

            # Strip tcp:// prefix
            endpoint = raw_url[6:] if raw_url.startswith("tcp://") else raw_url

            parts = endpoint.split("/")
            if len(parts) != 2:  # noqa: PLR2004
                self.logger.warning("Invalid endpoint URL (missing nodeID): %s", raw_url)
                continue

            host_port, node_id = parts
            addr_parts = host_port.rsplit(":", 1)
            if len(addr_parts) < 2:  # noqa: PLR2004
                self.logger.warning("Invalid endpoint URL (missing port): %s", raw_url)
                continue

            host = addr_parts[0]
            try:
                port = int(addr_parts[1])
            except ValueError:
                self.logger.warning("Invalid port in URL: %s", raw_url)
                continue

            if node_id == self.node_id:
                # Our own URL — use the port if not set
                if not self.opts.get("port"):
                    self.opts["port"] = port
            else:
                self._add_offline_node(node_id, host, port)

    # ------------------------------------------------------------------
    # Offline Node Management
    # ------------------------------------------------------------------

    def _add_offline_node(self, node_id: str, address: str, port: int) -> Any:
        """Register a node as offline (we know it exists but not its full INFO).

        Args:
            node_id: Remote node ID.
            address: Remote IP/hostname.
            port: Remote TCP port.

        Returns:
            The created/updated node object.
        """
        if not self._nodes:
            return None

        # Check if node already exists
        existing = self._nodes.get_node(node_id)
        if existing:
            existing.port = port
            existing.hostname = address
            return existing

        # Create new offline node via NodeCatalog API
        from ...node import Node  # noqa: PLC0415

        node = Node(node_id)
        node.local = False
        node.hostname = address
        node.ipList = [address]
        node.port = port
        node.available = False
        node.seq = 0

        self._nodes.add_node(node_id, node)
        self.logger.debug("Registered offline node: %s at %s:%d", node_id, address, port)

        return node

    # ------------------------------------------------------------------
    # Gossip Protocol
    # ------------------------------------------------------------------

    def _start_timers(self) -> None:
        """Start the gossip timer."""
        period = max(self.opts.get("gossip_period", 2), 1)

        async def _gossip_loop() -> None:
            try:
                while self.connected:
                    await asyncio.sleep(period)
                    if self.connected:
                        # Update local CPU info before gossiping (matches Node.js)
                        self._update_local_cpu()
                        await self._send_gossip_request()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.logger.warning("Gossip timer error: %s", e)

        self._gossip_timer = asyncio.ensure_future(_gossip_loop())

    async def _stop_timers(self) -> None:
        """Stop the gossip timer and await its completion."""
        if self._gossip_timer is not None:
            self._gossip_timer.cancel()
            try:
                await self._gossip_timer
            except asyncio.CancelledError:
                pass
            self._gossip_timer = None

    async def _send_hello(self, node_id: str) -> None:
        """Send a GOSSIP_HELLO to a remote node.

        Called after establishing a new TCP connection (handles racing problem).

        Args:
            node_id: Target node.
        """
        local_node = self._nodes.local_node if self._nodes else None
        if local_node is None:
            return

        host = self._get_node_host(local_node)
        port = getattr(local_node, "port", 0)

        packet = Packet(
            Topic.GOSSIP_HELLO,
            node_id,
            {
                "host": host,
                "port": port,
            },
        )

        if self._gossip_debug:
            self.logger.info("HELLO %s -> %s", self.node_id, node_id)

        try:
            await self.publish(packet)
        except Exception:
            self.logger.debug("Unable to send Gossip HELLO to %s", node_id)

    async def _process_gossip_hello(self, data: bytes, socket_info: dict[str, Any]) -> None:
        """Process incoming GOSSIP_HELLO packet.

        Registers unknown sender as offline node for future gossip exchange.

        Args:
            data: Serialized payload.
            socket_info: Connection info (remote_address).
        """
        assert self.transit is not None
        try:
            payload = await self.transit.serializer.deserialize_async(
                data, packet_type=to_packet_type("GOSSIP_HELLO")
            )
            sender = payload.get("sender", "")

            if self._gossip_debug:
                self.logger.info("HELLO %s <- %s", self.node_id, sender)

            if not sender or not self._nodes:
                return

            node = self._nodes.get_node(sender)
            if node is None:
                host = payload.get("host", socket_info.get("remote_address", ""))
                port = payload.get("port", 0)
                node = self._add_offline_node(sender, host, port)

            if node and not getattr(node, "udp_address", None):
                node.udp_address = socket_info.get("remote_address")

        except Exception as e:
            self.logger.warning("Invalid incoming GOSSIP_HELLO packet: %s", e)

    async def _send_gossip_request(self) -> None:
        """Create and send a Gossip request to random nodes.

        Builds a state summary of all known nodes and sends to:
        1. A random online node (always)
        2. A random offline node (probabilistically, based on offline ratio)
        """
        if not self._nodes:
            return

        nodes = self._get_all_nodes()
        if not nodes or len(nodes) <= 1:
            return

        packet_data: dict[str, Any] = {"online": {}, "offline": {}}
        online_list: list[Any] = []
        offline_list: list[Any] = []

        for node in nodes:
            available = getattr(node, "available", False)
            node_id = getattr(node, "id", None) or getattr(node, "node_id", None)
            if not node_id:
                continue

            seq = getattr(node, "seq", 0)

            if not available:
                if seq > 0:
                    packet_data["offline"][node_id] = seq
                offline_list.append(node)
            else:
                cpu_seq = getattr(node, "cpuSeq", 0)
                cpu = getattr(node, "cpu", 0)
                packet_data["online"][node_id] = [seq, cpu_seq, cpu]

                local = getattr(node, "local", False)
                if not local:
                    online_list.append(node)

        # Remove empty keys
        if not packet_data["offline"]:
            del packet_data["offline"]
        if not packet_data["online"]:
            del packet_data["online"]

        # Send to a random online node
        if online_list:
            await self._send_gossip_to_random(packet_data, online_list)

        # Probabilistically send to a random offline node
        if offline_list:
            ratio = len(offline_list) / (len(online_list) + 1)
            if ratio >= 1 or random.random() < ratio:
                await self._send_gossip_to_random(packet_data, offline_list)

    async def _send_gossip_to_random(self, data: dict[str, Any], endpoints: list[Any]) -> None:
        """Send a GOSSIP_REQ to a random node from the list.

        Args:
            data: Gossip state data (online/offline node states).
            endpoints: List of candidate nodes.
        """
        if not endpoints:
            return

        ep = endpoints[0] if len(endpoints) == 1 else random.choice(endpoints)
        ep_id = getattr(ep, "id", None) or getattr(ep, "node_id", None)
        if not ep_id:
            return

        packet = Packet(Topic.GOSSIP_REQ, ep_id, data)

        if self._gossip_debug:
            self.logger.info("GOSSIP_REQ %s -> %s", self.node_id, ep_id)

        try:
            await self.publish(packet)
        except Exception:
            self.logger.debug("Unable to send Gossip REQ to %s", ep_id)

    async def _process_gossip_request(self, data: bytes) -> None:
        """Process incoming GOSSIP_REQ — compare states and send response.

        This is the core gossip reconciliation. For each known node, compare
        our state with the requester's state and build a response with any
        newer information we have.

        Args:
            data: Serialized gossip request payload.
        """
        assert self.transit is not None
        response: dict[str, Any] = {"online": {}, "offline": {}}

        try:
            payload = await self.transit.serializer.deserialize_async(
                data, packet_type=to_packet_type("GOSSIP_REQ")
            )
            sender = payload.get("sender", "")

            if self._gossip_debug:
                self.logger.info("GOSSIP_REQ %s <- %s", self.node_id, sender)

            if not self._nodes:
                return

            nodes = self._get_all_nodes()
            for node in nodes:
                node_id = getattr(node, "id", None) or getattr(node, "node_id", None)
                if not node_id:
                    continue

                # Get requester's state for this node
                online_entry = payload.get("online", {}).get(node_id)
                offline_entry = payload.get("offline", {}).get(node_id)

                node_seq = getattr(node, "seq", 0)
                node_available = getattr(node, "available", False)
                node_local = getattr(node, "local", False)

                req_seq = None
                req_cpu_seq = None
                req_cpu = None

                if offline_entry is not None:
                    req_seq = offline_entry
                elif online_entry is not None:
                    if isinstance(online_entry, list) and len(online_entry) >= 1:
                        req_seq = online_entry[0]
                        req_cpu_seq = online_entry[1] if len(online_entry) > 1 else 0
                        req_cpu = online_entry[2] if len(online_entry) > 2 else 0  # noqa: PLR2004

                # Case 1: We have newer info or requester doesn't know this node
                # Match Node.js: `!seq || seq < node.seq` — falsy check includes 0
                if not req_seq or req_seq < node_seq:
                    if node_available:
                        info = self._get_node_info(node_id)
                        if info:
                            cpu_seq = getattr(node, "cpuSeq", 0)
                            cpu = getattr(node, "cpu", 0)
                            response["online"][node_id] = [info, cpu_seq, cpu]
                    else:
                        response["offline"][node_id] = node_seq
                    continue

                # Case 2: Requester says node is OFFLINE
                if offline_entry is not None:
                    if not node_available:
                        # Both agree it's offline — update seq if newer
                        if req_seq > node_seq:
                            node.seq = req_seq
                    elif not node_local:
                        # We thought it's online, requester says offline — trust requester
                        # (Node.js line 546: false = expected/gossip-driven)
                        if self._nodes and hasattr(self._nodes, "disconnect_node"):
                            self._nodes.disconnect_node(node_id, unexpected=False)
                        node.seq = req_seq
                        continue  # Node may be removed after disconnect
                    elif node_local:
                        # Requester says WE are offline! Correct them.
                        node.seq = req_seq + 1
                        info = self._get_local_node_info()
                        if info:
                            cpu_seq = getattr(node, "cpuSeq", 0)
                            cpu = getattr(node, "cpu", 0)
                            response["online"][node_id] = [info, cpu_seq, cpu]

                # Case 3: Requester says node is ONLINE
                elif online_entry is not None:
                    if node_available:
                        node_cpu_seq = getattr(node, "cpuSeq", 0)
                        if req_cpu_seq is not None and req_cpu_seq > node_cpu_seq:
                            # Requester has newer CPU info — update
                            self._heartbeat_node(node, req_cpu, req_cpu_seq)
                        elif req_cpu_seq is not None and req_cpu_seq < node_cpu_seq:
                            # We have newer CPU info — send back
                            response["online"][node_id] = [node_cpu_seq, getattr(node, "cpu", 0)]

            # Clean up empty keys
            if not response["offline"]:
                del response["offline"]
            if not response["online"]:
                del response["online"]

            # Send response if we have something to contribute
            if "online" in response or "offline" in response:
                sender_node = self._nodes.get_node(sender) if sender else None
                if sender_node:
                    sender_id = getattr(sender_node, "id", None) or getattr(
                        sender_node, "node_id", sender
                    )
                    rsp_packet = Packet(Topic.GOSSIP_RES, sender_id, response)

                    if self._gossip_debug:
                        self.logger.info("GOSSIP_RES %s -> %s", self.node_id, sender_id)

                    try:
                        await self.publish(rsp_packet)
                    except Exception:
                        pass
            elif self._gossip_debug:
                self.logger.info("EMPTY RESPONSE %s -> %s", self.node_id, sender)

        except Exception as e:
            self.logger.warning("Invalid incoming GOSSIP_REQ packet: %s", e)

    async def _process_gossip_response(self, data: bytes) -> None:
        """Process incoming GOSSIP_RES — merge newer state into our registry.

        Args:
            data: Serialized gossip response payload.
        """
        assert self.transit is not None
        try:
            payload = await self.transit.serializer.deserialize_async(
                data, packet_type=to_packet_type("GOSSIP_RES")
            )
            sender = payload.get("sender", "")

            if self._gossip_debug:
                self.logger.info("GOSSIP_RES %s <- %s", self.node_id, sender)

            if not self._nodes:
                return

            # Process online nodes
            online = payload.get("online", {})
            for node_id, row in online.items():
                if node_id == self.node_id:
                    continue  # We know our own state better

                if not isinstance(row, list):
                    continue

                info = None
                cpu_seq = None
                cpu = None

                if len(row) == 1:
                    info = row[0]
                elif len(row) == 2:  # noqa: PLR2004
                    cpu_seq, cpu = row
                elif len(row) >= 3:  # noqa: PLR2004
                    info, cpu_seq, cpu = row[0], row[1], row[2]

                node = self._nodes.get_node(node_id)

                # If we don't know it, or have older/equal seq, update with full INFO.
                # Equal seq is accepted to handle the seq=0 bootstrap case where
                # both offline node and first INFO have seq=0.
                if info and isinstance(info, dict):
                    node_seq = getattr(node, "seq", 0) if node else -1
                    info_seq = info.get("seq", 0)
                    node_available = getattr(node, "available", False) if node else False
                    if (
                        not node
                        or (not node_available and node_seq <= info_seq)
                        or node_seq < info_seq
                    ):
                        info["sender"] = node_id
                        self._process_node_info(info)
                        node = self._nodes.get_node(node_id)  # Re-fetch after update

                # Update CPU if newer
                if node and cpu_seq:  # Truthy check matches Node.js (skips 0)
                    node_cpu_seq = getattr(node, "cpuSeq", 0)
                    if cpu_seq > node_cpu_seq:
                        self._heartbeat_node(node, cpu, cpu_seq)

            # Process offline nodes
            offline = payload.get("offline", {})
            for node_id, seq in offline.items():
                if node_id == self.node_id:
                    continue

                node = self._nodes.get_node(node_id)
                if not node:
                    continue

                node_seq = getattr(node, "seq", 0)
                if node_seq < seq:
                    if getattr(node, "available", False):
                        # Gossip says offline (Node.js line 676: false = expected)
                        if self._nodes and hasattr(self._nodes, "disconnect_node"):
                            self._nodes.disconnect_node(node_id, unexpected=False)
                    node.seq = seq

        except Exception as e:
            self.logger.warning("Invalid incoming GOSSIP_RES packet: %s", e)

    # ------------------------------------------------------------------
    # Registry helpers
    # ------------------------------------------------------------------

    def _update_local_cpu(self) -> None:
        """Update local node CPU info before gossiping (matches Node.js pattern).

        Node.js: getLocalNodeInfo().updateLocalInfo(broker.getCpuUsage)
        """
        if not self._nodes:
            return
        local_node = getattr(self._nodes, "local_node", None)
        if local_node is None:
            return
        # Update CPU from metrics collector if available
        broker = getattr(self.transit, "_broker", None)
        if broker and hasattr(broker, "get_cpu_usage"):
            try:
                cpu = broker.get_cpu_usage()
                if cpu is not None:
                    local_node.cpu = cpu
                    local_node.cpuSeq = getattr(local_node, "cpuSeq", 0) + 1
            except Exception:
                pass
        elif hasattr(local_node, "update_local_info"):
            try:
                local_node.update_local_info()
            except Exception:
                pass

    def _get_all_nodes(self) -> list[Any]:
        """Get all nodes from the NodeCatalog as a list."""
        if not self._nodes:
            return []
        # NodeCatalog stores nodes in self.nodes dict
        nodes_dict = getattr(self._nodes, "nodes", None)
        if isinstance(nodes_dict, dict):
            return list(nodes_dict.values())
        return []

    def _get_node_info(self, node_id: str) -> dict[str, Any] | None:
        """Get full INFO for a node (builds payload dict from Node object)."""
        node = self._nodes.get_node(node_id) if self._nodes else None
        if node is None:
            return None
        # Build info dict matching Moleculer protocol
        return self._node_to_info(node)

    def _get_local_node_info(self) -> dict[str, Any] | None:
        """Get local node INFO (regenerated)."""
        if not self._nodes:
            return None
        # Regenerate local node info
        if hasattr(self._nodes, "ensure_local_node"):
            self._nodes.ensure_local_node()
        local = self._nodes.local_node
        if local:
            return self._node_to_info(local)
        return None

    @staticmethod
    def _node_to_info(node: Any) -> dict[str, Any]:
        """Convert a Node object to an INFO payload dict."""
        return {
            "sender": getattr(node, "id", None) or getattr(node, "node_id", ""),
            "services": getattr(node, "services", []),
            "hostname": getattr(node, "hostname", ""),
            "ipList": getattr(node, "ipList", []),
            "port": getattr(node, "port", 0),
            "client": getattr(node, "client", {}),
            "seq": getattr(node, "seq", 0),
            "cpu": getattr(node, "cpu", 0),
            "cpuSeq": getattr(node, "cpuSeq", 0),
            "metadata": getattr(node, "metadata", {}),
        }

    def _process_node_info(self, info: dict[str, Any]) -> None:
        """Process a full node INFO payload (register/update node)."""
        if self._nodes and hasattr(self._nodes, "process_node_info"):
            sender = info.get("sender", "")
            self._nodes.process_node_info(sender, info)

    def _heartbeat_node(self, node: Any, cpu: Any, cpu_seq: Any) -> None:
        """Update a node's CPU heartbeat data."""
        if hasattr(node, "heartbeat"):
            node.heartbeat({"cpu": cpu, "cpuSeq": cpu_seq})
        else:
            if cpu is not None:
                node.cpu = cpu
            if cpu_seq is not None:
                node.cpuSeq = cpu_seq

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_config(
        cls,
        config: dict[str, Any],
        transit: Transit,
        handler: Callable[..., Any] | None = None,
        node_id: str | None = None,
    ) -> TcpTransporter:
        """Create a TcpTransporter from configuration.

        Supports connection string formats:
        - "tcp://"  (auto-discovery)
        - "tcp://host1:port/node1,host2:port/node2"  (static URLs)

        Args:
            config: Configuration dict with "connection" key.
            transit: Transit instance.
            handler: Message handler callback.
            node_id: This node's ID.

        Returns:
            Configured TcpTransporter instance.
        """
        conn = config.get("connection", "")

        opts: dict[str, Any] = {}

        # Parse connection string for static URLs
        if isinstance(conn, str) and conn.startswith("tcp://"):
            url_part = conn[6:]  # Strip "tcp://"
            if url_part:
                opts["urls"] = [u.strip() for u in url_part.split(",") if u.strip()]

        # Merge any additional config options
        for key in DEFAULT_OPTIONS:
            if key in config:
                opts[key] = config[key]

        return cls(transit=transit, handler=handler, node_id=node_id, opts=opts)
