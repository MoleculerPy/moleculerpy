"""E2E test: ContextTracker auto-registration drains in-flight requests on stop.

When `settings.tracking.enabled=True`, broker.stop() should wait for
in-flight contexts to complete (up to shutdown_timeout) before disconnecting.
"""

from __future__ import annotations

import asyncio

import pytest

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action
from moleculerpy.service import Service
from moleculerpy.settings import Settings, TrackingConfig


class SlowService(Service):
    name = "slow"

    def __init__(self) -> None:
        super().__init__(self.name)
        self.completed = False

    @action()
    async def work(self, ctx) -> str:
        await asyncio.sleep(1.0)
        self.completed = True
        return "done"


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_tracking_shutdown_waits_for_inflight() -> None:
    """broker.stop() should wait for in-flight tracked action to complete."""
    settings = Settings(
        transporter="memory://",
        tracking=TrackingConfig(enabled=True, shutdown_timeout=5.0),
    )
    broker = ServiceBroker(id="track-node", settings=settings)
    svc = SlowService()
    await broker.register(svc)
    await broker.start()

    # Fire request in background
    call_task = asyncio.create_task(broker.call("slow.work"))
    # Let it begin
    await asyncio.sleep(0.1)
    assert not svc.completed

    # Stop should wait for the in-flight action to finish
    await broker.stop()

    # Action should have completed before stop returned
    assert svc.completed
    assert call_task.done()
    assert await call_task == "done"
