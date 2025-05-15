"""Live REPL launcher for the multi-service demo application."""

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
from moleculerpy_repl import REPL


def default_node_id(prefix: str) -> str:
    """Create a deterministic-enough node ID for an interactive demo session."""
    return f"{prefix}-{os.getpid()}"


async def run_repl(node_id: str, nats_url: str) -> None:
    """Start the demo broker and open a live REPL against it."""
    broker = await create_demo_broker(node_id=node_id, nats_url=nats_url)

    await broker.start()
    await wait_for_cluster_settle()

    print("Demo REPL is ready.")
    print("Suggested commands:")
    print("  info")
    print("  services")
    print("  actions")
    print("  call catalog.list")
    print("  call gateway.checkout product_id=starter-kit quantity=2 customer_name=Alice")
    print("  call gateway.dashboard")

    repl = REPL(broker=broker, delimiter="demo $ ")
    await repl.run()


def main() -> None:
    """CLI entrypoint for the live REPL demo."""
    parser = argparse.ArgumentParser(description="Run the MoleculerPy demo REPL")
    parser.add_argument(
        "--node-id",
        default=default_node_id("demo-repl"),
        help="Broker node ID",
    )
    parser.add_argument("--nats-url", default=get_nats_url(), help="NATS connection URL")
    args = parser.parse_args()
    asyncio.run(run_repl(node_id=args.node_id, nats_url=args.nats_url))


if __name__ == "__main__":
    main()
