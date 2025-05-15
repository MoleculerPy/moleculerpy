"""Runner config for the demo application.

Allows starting the demo via:

    moleculerpy-runner \
      -c moleculerpy/examples/demo_app/broker_config.py \
      --repl \
      moleculerpy/examples/demo_app/services
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

DEMO_PARENT = Path(__file__).resolve().parent.parent
DEMO_PARENT_STR = str(DEMO_PARENT)
if DEMO_PARENT_STR not in sys.path:
    sys.path.insert(0, DEMO_PARENT_STR)

from demo_app.bootstrap import configure_local_paths

configure_local_paths()

from demo_app.common import build_demo_settings, get_nats_url
from moleculerpy import ServiceBroker
from moleculerpy_channels import ChannelsMiddleware
from moleculerpy_channels.adapters import NatsAdapter


def create_broker(config: Any, worker_id: int | None = None) -> ServiceBroker:
    """Build a demo broker for moleculerpy-runner."""
    base_node_id = getattr(config, "node_id", None) or f"demo-runner-{os.getpid()}"
    if worker_id and worker_id > 1:
        node_id = f"{base_node_id}-{worker_id}"
    else:
        node_id = base_node_id

    nats_url = getattr(config, "transporter", None) or get_nats_url()
    return ServiceBroker(
        id=node_id,
        settings=build_demo_settings(nats_url),
        middlewares=[
            ChannelsMiddleware(
                adapter=NatsAdapter(url=nats_url),
                context=True,
            )
        ],
    )
