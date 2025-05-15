#!/usr/bin/env python3
"""NATS-backed smoke demo for MoleculerPy.

This example starts two brokers against the same NATS server:
- `demo-server` hosts a few services
- `demo-client` waits for discovery and performs remote calls

Run:
    NATS_URL=nats://localhost:4222 .venv/bin/python moleculerpy/examples/nats_smoke_demo.py
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from moleculerpy import Broker, Context, Service, Settings, action, event


class MathService(Service):
    """Simple arithmetic service."""

    name = "math"

    def __init__(self) -> None:
        super().__init__(self.name)

    @action(params=["a", "b"])
    async def add(self, ctx: Context) -> float:
        return float(ctx.params["a"]) + float(ctx.params["b"])


class GreeterService(Service):
    """Greeting service that emits an event for observability."""

    name = "greeter"

    def __init__(self) -> None:
        super().__init__(self.name)

    @action(params=["name"])
    async def hello(self, ctx: Context) -> str:
        name = str(ctx.params.get("name", "World"))
        message = f"Hello, {name}!"
        await ctx.emit("greeter.greeting", {"name": name, "message": message})
        return message


class MonitorService(Service):
    """Tracks greeting events to prove event flow works."""

    name = "monitor"

    def __init__(self) -> None:
        super().__init__(self.name)
        self.greetings_seen = 0
        self.last_message: str | None = None

    @event(name="greeter.greeting")
    async def on_greeting(self, ctx: Context) -> None:
        self.greetings_seen += 1
        self.last_message = str(ctx.params.get("message"))

    @action()
    async def stats(self, ctx: Context) -> dict[str, Any]:
        return {
            "greetings_seen": self.greetings_seen,
            "last_message": self.last_message,
        }


class GatewayService(Service):
    """Shows service-to-service calls from inside an action."""

    name = "gateway"

    def __init__(self) -> None:
        super().__init__(self.name)

    @action(params=["name", "a", "b"])
    async def summary(self, ctx: Context) -> dict[str, Any]:
        total = await ctx.call("math.add", {"a": ctx.params["a"], "b": ctx.params["b"]})
        greeting = await ctx.call("greeter.hello", {"name": ctx.params["name"]})
        return {
            "greeting": greeting,
            "sum": total,
            "source": "gateway.summary",
        }


async def main() -> None:
    """Run the smoke demo against a real NATS server."""
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    settings = Settings(transporter=nats_url, log_level="INFO")

    server = Broker("demo-server", settings=settings)
    client = Broker("demo-client", settings=settings)

    await server.register(MathService())
    await server.register(GreeterService())
    await server.register(MonitorService())
    await server.register(GatewayService())

    try:
        await server.start()
        await client.start()
        await client.wait_for_services(["math", "greeter", "monitor", "gateway"], timeout=10.0)

        add_result = await client.call("math.add", {"a": 7, "b": 5})
        print(f"math.add(7, 5) -> {add_result}")

        summary = await client.call(
            "gateway.summary",
            {"name": "MoleculerPy", "a": 10, "b": 32},
        )
        print(f"gateway.summary(...) -> {summary}")

        # Allow event delivery to settle before reading monitor stats.
        await asyncio.sleep(0.2)
        stats = await client.call("monitor.stats", {})
        print(f"monitor.stats() -> {stats}")

    finally:
        await client.stop()
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
