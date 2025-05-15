"""Shared broker/bootstrap utilities for the demo application."""

from __future__ import annotations

import asyncio
import os
from typing import Final

from .bootstrap import configure_local_paths

configure_local_paths()

from moleculerpy import ServiceBroker, Settings
from moleculerpy_channels import ChannelsMiddleware
from moleculerpy_channels.adapters import NatsAdapter

from .services import (
    AnalyticsService,
    CatalogService,
    GatewayService,
    NotificationsService,
    OrdersService,
    PaymentsService,
)

DEMO_SERVICE_TYPES: Final = (
    CatalogService,
    OrdersService,
    PaymentsService,
    AnalyticsService,
    NotificationsService,
    GatewayService,
)


def get_nats_url() -> str:
    """Return the NATS URL for the demo."""
    return os.environ.get("NATS_URL", "nats://localhost:4222")


def build_demo_settings(nats_url: str) -> Settings:
    """Create standard broker settings for the demo."""
    return Settings(
        transporter=nats_url,
        prefer_local=True,
        log_level=os.environ.get("MOLECULERPY_LOG_LEVEL", "INFO"),
    )


async def create_demo_broker(
    node_id: str = "demo-app",
    nats_url: str | None = None,
) -> ServiceBroker:
    """Create a broker with demo services and NATS-backed channels."""
    resolved_nats_url = nats_url or get_nats_url()
    broker = ServiceBroker(
        id=node_id,
        settings=build_demo_settings(resolved_nats_url),
        middlewares=[
            ChannelsMiddleware(
                adapter=NatsAdapter(url=resolved_nats_url),
                context=True,
            )
        ],
    )

    for service_type in DEMO_SERVICE_TYPES:
        await broker.register(service_type())

    return broker


def create_client_broker(
    node_id: str = "demo-client",
    nats_url: str | None = None,
) -> ServiceBroker:
    """Create a lightweight client broker for remote action calls."""
    resolved_nats_url = nats_url or get_nats_url()
    return ServiceBroker(
        id=node_id,
        settings=build_demo_settings(resolved_nats_url),
    )


async def wait_for_cluster_settle(delay: float = 0.8) -> None:
    """Allow service discovery and channel subscriptions to settle."""
    await asyncio.sleep(delay)
