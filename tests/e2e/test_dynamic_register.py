"""E2E tests for dynamic service registration after broker.start().

Verifies Node.js Moleculer parity: registering a local service after the
broker is connected must increment local_node.seq AND immediately broadcast
INFO so remote nodes detect the new service without waiting for heartbeat.
"""

from __future__ import annotations

import asyncio

import pytest

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action
from moleculerpy.service import Service
from moleculerpy.settings import Settings


class LateService(Service):
    """Service registered after broker.start()."""

    name = "late"

    @action()
    async def ping(self, ctx) -> str:
        return "pong"


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_dynamic_register_broadcasts_info() -> None:
    """After broker.start(), registering a new service triggers INFO broadcast,
    so remote brokers see the service within ~1s (well under 5s heartbeat)."""
    broker_a = ServiceBroker(
        id="node-a", settings=Settings(transporter="memory://", prefer_local=False)
    )
    broker_b = ServiceBroker(
        id="node-b", settings=Settings(transporter="memory://", prefer_local=False)
    )

    await broker_a.start()
    await broker_b.start()

    try:
        # Allow initial discovery to settle.
        await asyncio.sleep(0.3)

        # Baseline: broker A does not know about "late" service.
        assert broker_a.registry.get_action("late.ping") is None

        seq_before = broker_b.node_catalog.local_node.seq  # type: ignore[union-attr]

        # Register new service on broker B AFTER start.
        await broker_b.register(LateService())

        # seq must have been bumped.
        assert broker_b.node_catalog.local_node.seq == seq_before + 1  # type: ignore[union-attr]

        # Broker A should learn about the new service well under 5s heartbeat,
        # because broker B broadcast an INFO packet on register.
        await broker_a.wait_for_services(["late"], timeout=2.0)

        assert broker_a.registry.get_action("late.ping") is not None
    finally:
        await broker_a.stop()
        await broker_b.stop()
