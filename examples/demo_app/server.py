"""Start the multi-service demo application and wait for requests."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

DEMO_PARENT = Path(__file__).resolve().parent.parent
DEMO_PARENT_STR = str(DEMO_PARENT)
if DEMO_PARENT_STR not in sys.path:
    sys.path.insert(0, DEMO_PARENT_STR)

from demo_app.bootstrap import configure_local_paths

configure_local_paths()

from demo_app.common import create_demo_broker, get_nats_url, wait_for_cluster_settle


def default_node_id(prefix: str) -> str:
    """Create a deterministic-enough node ID for an interactive demo session."""
    return f"{prefix}-{os.getpid()}"


async def run_server(node_id: str, nats_url: str) -> None:
    """Start the demo broker and wait until interrupted."""
    broker = await create_demo_broker(node_id=node_id, nats_url=nats_url)

    print(f"Starting demo broker '{node_id}' on {nats_url}")
    print("Services: catalog, orders, payments, analytics, notifications, gateway")

    await broker.start()
    await wait_for_cluster_settle()

    print("Demo broker is ready. Press Ctrl+C to stop.")
    await broker.wait_for_shutdown()


def main() -> None:
    """CLI entrypoint for running the demo server."""
    parser = argparse.ArgumentParser(description="Run the MoleculerPy demo server")
    parser.add_argument(
        "--node-id",
        default=default_node_id("demo-app"),
        help="Broker node ID",
    )
    parser.add_argument("--nats-url", default=get_nats_url(), help="NATS connection URL")
    args = parser.parse_args()
    asyncio.run(run_server(node_id=args.node_id, nats_url=args.nats_url))


if __name__ == "__main__":
    main()
