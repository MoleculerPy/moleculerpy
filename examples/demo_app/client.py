"""Scripted client for exercising the multi-service demo application."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

DEMO_PARENT = Path(__file__).resolve().parent.parent
DEMO_PARENT_STR = str(DEMO_PARENT)
if DEMO_PARENT_STR not in sys.path:
    sys.path.insert(0, DEMO_PARENT_STR)

from demo_app.bootstrap import configure_local_paths

configure_local_paths()

from demo_app.common import create_client_broker, get_nats_url, wait_for_cluster_settle


def default_node_id(prefix: str) -> str:
    """Create a deterministic-enough node ID for an interactive demo session."""
    return f"{prefix}-{os.getpid()}"


async def run_client(node_id: str, nats_url: str) -> None:
    """Connect to the running demo server and execute a checkout scenario."""
    broker = create_client_broker(node_id=node_id, nats_url=nats_url)

    print(f"Starting demo client '{node_id}' on {nats_url}")
    await broker.start()

    try:
        await broker.wait_for_services(["gateway"], timeout=15.0)

        first_checkout = await broker.call(
            "gateway.checkout",
            {
                "product_id": "starter-kit",
                "quantity": 2,
                "customer_name": "Alice",
                "payment_method": "card",
            },
        )
        second_checkout = await broker.call(
            "gateway.checkout",
            {
                "product_id": "team-bundle",
                "quantity": 1,
                "customer_name": "Bob",
                "payment_method": "invoice",
            },
        )

        await wait_for_cluster_settle(1.0)
        dashboard = await broker.call("gateway.dashboard")

        print("\nCheckout #1")
        print(json.dumps(first_checkout, indent=2, sort_keys=True))
        print("\nCheckout #2")
        print(json.dumps(second_checkout, indent=2, sort_keys=True))
        print("\nDashboard")
        print(json.dumps(dashboard, indent=2, sort_keys=True))
    finally:
        await broker.stop()


def main() -> None:
    """CLI entrypoint for running the demo client."""
    parser = argparse.ArgumentParser(description="Run the MoleculerPy demo client")
    parser.add_argument(
        "--node-id",
        default=default_node_id("demo-client"),
        help="Client broker node ID",
    )
    parser.add_argument("--nats-url", default=get_nats_url(), help="NATS connection URL")
    args = parser.parse_args()
    asyncio.run(run_client(node_id=args.node_id, nats_url=args.nats_url))


if __name__ == "__main__":
    main()
