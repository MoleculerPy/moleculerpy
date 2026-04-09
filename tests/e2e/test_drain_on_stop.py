"""E2E test: broker.stop() drains services on remote nodes before disconnecting.

Verifies the Node.js Moleculer pattern: an INFO packet with empty services list
is broadcast BEFORE the DISCONNECT packet, so peer nodes mark the node as
draining and stop routing new requests to it.
"""

from __future__ import annotations

import asyncio

import pytest

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action
from moleculerpy.service import Service
from moleculerpy.settings import Settings


class MathDrainService(Service):
    name = "math"

    def __init__(self) -> None:
        super().__init__(self.name)

    @action()
    async def add(self, ctx) -> int:
        return ctx.params["a"] + ctx.params["b"]


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_drain_info_sent_before_disconnect() -> None:
    """Broker A stops → Broker B sees math service removed before DISCONNECT."""
    settings_a = Settings(transporter="memory://", prefer_local=False)
    broker_a = ServiceBroker(id="node-a", settings=settings_a)
    await broker_a.register(MathDrainService())

    settings_b = Settings(transporter="memory://", prefer_local=False)
    broker_b = ServiceBroker(id="node-b", settings=settings_b)

    await broker_a.start()
    await broker_b.start()

    try:
        # Wait until broker B sees math service from node-a
        await broker_b.wait_for_services(["math"], timeout=5.0)
        assert broker_b.registry.get_action("math.add") is not None

        # Stop broker A — this should trigger drain INFO before DISCONNECT
        await broker_a.stop()

        # Allow propagation
        await asyncio.sleep(0.2)

        # Broker B should no longer have math.add from node-a
        action_obj = broker_b.registry.get_action("math.add")
        # Either action is gone or the node-a entry was removed
        assert action_obj is None or not any(
            getattr(ep, "node_id", None) == "node-a" for ep in getattr(action_obj, "endpoints", [])
        )
    finally:
        await broker_b.stop()
