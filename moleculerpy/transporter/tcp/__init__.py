"""TCP Transporter with Gossip Protocol for MoleculerPy.

Peer-to-peer transport without external message broker.
Uses Gossip Protocol for service discovery and TCP for direct messaging.

Reference: sources/reference-implementations/moleculer/src/transporters/tcp.js
"""

from .tcp_transporter import TcpTransporter

__all__ = ["TcpTransporter"]
