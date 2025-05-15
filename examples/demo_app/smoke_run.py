"""Self-contained smoke runner for the multi-service demo application."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

DEMO_PARENT = Path(__file__).resolve().parent.parent
DEMO_PARENT_STR = str(DEMO_PARENT)
if DEMO_PARENT_STR not in sys.path:
    sys.path.insert(0, DEMO_PARENT_STR)

from demo_app.bootstrap import configure_local_paths

configure_local_paths()

from demo_app.common import (
    create_client_broker,
    create_demo_broker,
    get_nats_url,
    wait_for_cluster_settle,
)


async def run_smoke(nats_url: str) -> None:
    """Start demo and client brokers, then verify the full workflow."""
    app_broker = await create_demo_broker(node_id="demo-app", nats_url=nats_url)
    client_broker = create_client_broker(node_id="demo-client", nats_url=nats_url)

    try:
        await app_broker.start()
        await client_broker.start()

        await client_broker.wait_for_services(["gateway"], timeout=15.0)
        await wait_for_cluster_settle(1.0)

        result = await client_broker.call(
            "gateway.checkout",
            {
                "product_id": "pro-kit",
                "quantity": 3,
                "customer_name": "Smoke Test",
                "payment_method": "card",
            },
        )

        await wait_for_cluster_settle(1.0)
        dashboard = await client_broker.call("gateway.dashboard")

        print("Checkout")
        print(json.dumps(result, indent=2, sort_keys=True))
        print("\nDashboard")
        print(json.dumps(dashboard, indent=2, sort_keys=True))
    finally:
        await client_broker.stop()
        await app_broker.stop()


def main() -> None:
    """CLI entrypoint for smoke-running the demo application."""
    parser = argparse.ArgumentParser(description="Run the MoleculerPy demo smoke flow")
    parser.add_argument("--nats-url", default=get_nats_url(), help="NATS connection URL")
    args = parser.parse_args()
    asyncio.run(run_smoke(args.nats_url))


if __name__ == "__main__":
    main()
