#!/usr/bin/env python3
"""Gamma Node - Second server for load balancing testing.

Run this OPTIONALLY in terminal 3 to test load balancing:
    python examples/cluster/server_gamma.py

This node hosts the same services as beta-node to demonstrate
how calls are distributed across multiple nodes.
"""

from __future__ import annotations

import asyncio
import random
import signal

# Import colored logger for beautiful console output
from colored_logger import LoggerFactory, LogLevel

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action
from moleculerpy.service import Service
from moleculerpy.settings import Settings

# =============================================================================
# Services (same as beta, different node_id)
# =============================================================================

FLAKY_FAILURE_RATE = 0.4


class FlakyService(Service):
    name = "flaky"

    def __init__(self, node_id: str):
        super().__init__(self.name)
        self._node_id = node_id
        self._call_count = 0

    @action()
    async def unreliable(self, ctx) -> dict:
        self._call_count += 1
        if random.random() < FLAKY_FAILURE_RATE:
            print(f"  [{self._node_id}] flaky.unreliable #{self._call_count} -> FAIL")
            raise ConnectionError(f"Random failure on {self._node_id}")
        print(f"  [{self._node_id}] flaky.unreliable #{self._call_count} -> SUCCESS")
        return {"node_id": self._node_id, "attempt": self._call_count, "status": "success"}

    @action()
    async def always_fail(self, ctx) -> dict:
        self._call_count += 1
        print(f"  [{self._node_id}] flaky.always_fail #{self._call_count} -> FAIL")
        raise ConnectionError(f"Service unavailable on {self._node_id}")


class SlowService(Service):
    name = "slow"

    def __init__(self, node_id: str):
        super().__init__(self.name)
        self._node_id = node_id

    @action()
    async def process(self, ctx) -> dict:
        delay = ctx.params.get("delay", 0.5)
        print(f"  [{self._node_id}] slow.process (delay={delay}s)...")
        await asyncio.sleep(delay)
        print(f"  [{self._node_id}] slow.process completed")
        return {"node_id": self._node_id, "delay": delay, "status": "completed"}


class CounterService(Service):
    name = "counter"

    def __init__(self, node_id: str):
        super().__init__(self.name)
        self._node_id = node_id
        self._count = 0

    @action()
    async def increment(self, ctx) -> dict:
        self._count += 1
        print(f"  [{self._node_id}] counter.increment -> count={self._count}")
        return {"node_id": self._node_id, "count": self._count}

    @action()
    async def get(self, ctx) -> dict:
        return {"node_id": self._node_id, "count": self._count}


class EchoService(Service):
    name = "echo"

    def __init__(self, node_id: str):
        super().__init__(self.name)
        self._node_id = node_id

    @action()
    async def ping(self, ctx) -> dict:
        message = ctx.params.get("message", "pong")
        print(f"  [{self._node_id}] echo.ping -> {message}")
        return {"node_id": self._node_id, "message": message}


# =============================================================================
# Main
# =============================================================================


async def main():
    NODE_ID = "gamma-node"

    # Create colored logger factory
    logger_factory = LoggerFactory(
        node_id=NODE_ID,
        level=LogLevel.INFO,
        colors=True,
        timestamp_format="iso",
    )

    # Get logger for startup messages
    startup_log = logger_factory.get_logger("STARTUP")

    startup_log.info("=" * 50)
    startup_log.info(f"MOLECULERPY SERVER NODE: {NODE_ID}")
    startup_log.info("=" * 50)
    startup_log.info("Services hosted (same as beta, for load balancing):")
    startup_log.info("  - flaky.unreliable")
    startup_log.info("  - flaky.always_fail")
    startup_log.info("  - slow.process")
    startup_log.info("  - counter.increment")
    startup_log.info("  - echo.ping")

    # Create settings with colored logger
    settings = Settings(
        transporter="nats://localhost:4222",
        log_level="INFO",
        prefer_local=False,
        logger_factory=logger_factory,
    )

    broker = ServiceBroker(id=NODE_ID, settings=settings)

    await broker.register(FlakyService(NODE_ID))
    await broker.register(SlowService(NODE_ID))
    await broker.register(CounterService(NODE_ID))
    await broker.register(EchoService(NODE_ID))

    shutdown_event = asyncio.Event()

    def signal_handler():
        print(f"\n[{NODE_ID}] Shutting down...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    await broker.start()
    print()
    print(f"[{NODE_ID}] Server started. Waiting for calls...")
    print(f"[{NODE_ID}] Press Ctrl+C to stop")
    print()

    await shutdown_event.wait()
    await broker.stop()
    print(f"[{NODE_ID}] Server stopped.")


if __name__ == "__main__":
    asyncio.run(main())
