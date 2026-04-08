"""
Demo stand for moleculerpy-channels Pub/Sub on real Redis + NATS.

Tests (6):
    1. Basic publish/subscribe (Redis)
    2. Consumer groups balancing (Redis, 3 consumers)
    3. Dead Letter Queue after max_retries (Redis)
    4. Retry policy (handler succeeds on Nth attempt, Redis)
    5. Graceful shutdown drains in-flight messages (Redis)
    6. NATS adapter basic publish/subscribe

Pre-flight:
    - moleculerpy_channels must be importable
    - Redis (Valkey) must be reachable on localhost:6381
    - NATS must be reachable on localhost:4222 (optional — test 6 skips)

Run:
    .venv/bin/python examples/demo_channels.py
"""

from __future__ import annotations

import asyncio
import socket
import sys
import time
import uuid
from typing import Any

# ── ANSI colors ──────────────────────────────────────────────────────────────
G = "\033[92m"
R = "\033[91m"
Y = "\033[93m"
B = "\033[94m"
D = "\033[90m"
BOLD = "\033[1m"
RST = "\033[0m"


def _log(prefix: str, msg: str) -> None:
    print(f"{prefix} {msg}")


def _check_port(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


# ── Pre-flight ───────────────────────────────────────────────────────────────
try:
    import moleculerpy_channels  # noqa: F401
    from moleculerpy_channels import ChannelsMiddleware
    from moleculerpy_channels.adapters import RedisAdapter
except ImportError:
    print(f"{R}[ERROR]{RST} moleculerpy_channels not installed.")
    print(f"{D}Install: pip install -e moleculerpy-channels[all]{RST}")
    sys.exit(2)

try:
    from moleculerpy_channels.adapters import NatsAdapter  # type: ignore[attr-defined]

    NATS_AVAILABLE = True
except ImportError:
    NatsAdapter = None  # type: ignore[assignment,misc]
    NATS_AVAILABLE = False

from moleculerpy import Service, ServiceBroker

REDIS_HOST = "localhost"
REDIS_PORT = 6381
REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/15"
NATS_HOST = "localhost"
NATS_PORT = 4222
NATS_URL = f"nats://{NATS_HOST}:{NATS_PORT}"


# ── Broker factory ───────────────────────────────────────────────────────────
async def _make_broker(node_id: str, adapter: Any) -> ServiceBroker:
    broker = ServiceBroker(
        id=node_id,
        middlewares=[ChannelsMiddleware(adapter=adapter)],
    )
    return broker


async def _cleanup_redis_streams(*names: str) -> None:
    """Best-effort delete of Redis stream keys before a test run (db 15)."""
    try:
        import redis.asyncio as aioredis

        client = aioredis.from_url(REDIS_URL)
        if names:
            await client.delete(*names)
        else:
            await client.flushdb()
        await client.aclose()
    except Exception as e:
        print(f"{Y}[WARN]{RST} redis cleanup failed: {e}")


# ── Tests ────────────────────────────────────────────────────────────────────
async def test_basic_publish_subscribe() -> tuple[bool, str]:
    await _cleanup_redis_streams()
    ch = f"demo.basic.{uuid.uuid4().hex[:6]}"
    got = asyncio.Event()
    received: list[Any] = []

    class SubService(Service):
        name = "sub_basic"

        @property
        def schema(self) -> dict:
            return {"channels": {ch: {"group": "g1", "handler": self._handle}}}

        async def _handle(self, payload: Any, raw: Any) -> None:
            received.append(payload)
            got.set()

    pub = await _make_broker("pub-node", RedisAdapter(redis_url=REDIS_URL))
    sub = await _make_broker("sub-node", RedisAdapter(redis_url=REDIS_URL))
    await sub.register(SubService())

    await pub.start()
    await sub.start()
    try:
        await asyncio.sleep(0.3)
        await pub.send_to_channel(ch, {"hello": "world", "n": 1})
        await asyncio.wait_for(got.wait(), timeout=5.0)
        assert received and received[0]["hello"] == "world"
        return True, "1 message delivered"
    finally:
        await pub.stop()
        await sub.stop()


async def test_consumer_groups() -> tuple[bool, str]:
    await _cleanup_redis_streams()
    ch = f"demo.group.{uuid.uuid4().hex[:6]}"
    counts = [0, 0, 0]
    done = asyncio.Event()
    total = 30
    received_total = 0
    lock = asyncio.Lock()

    def make_service(idx: int) -> type:
        class _Svc(Service):
            name = f"cg_consumer_{idx}"

            @property
            def schema(self) -> dict:
                return {"channels": {ch: {"group": "shared-group", "handler": self._h}}}

            async def _h(self, payload: Any, raw: Any) -> None:
                nonlocal received_total
                async with lock:
                    counts[idx] += 1
                    received_total += 1
                    if received_total >= total:
                        done.set()

        return _Svc

    pub = await _make_broker("pub-cg", RedisAdapter(redis_url=REDIS_URL))
    consumers = []
    for i in range(3):
        b = await _make_broker(f"cg-{i}", RedisAdapter(redis_url=REDIS_URL))
        await b.register(make_service(i)())
        consumers.append(b)

    await pub.start()
    for b in consumers:
        await b.start()
    try:
        await asyncio.sleep(0.5)
        for i in range(total):
            await pub.send_to_channel(ch, {"i": i})
        await asyncio.wait_for(done.wait(), timeout=10.0)
        if sum(counts) != total:
            return False, f"expected {total}, got {sum(counts)} (distribution={counts})"
        if not all(c > 0 for c in counts):
            return False, f"not all consumers received: {counts}"
        return True, f"distribution={counts} (total={total})"
    finally:
        await pub.stop()
        for b in consumers:
            await b.stop()


async def test_dlq() -> tuple[bool, str]:
    dlq_name = f"DLQ_{uuid.uuid4().hex[:6]}"
    await _cleanup_redis_streams(dlq_name)
    ch = f"demo.dlq.{uuid.uuid4().hex[:6]}"
    attempts = {"n": 0}

    class DlqSvc(Service):
        name = "dlq_svc"

        @property
        def schema(self) -> dict:
            return {
                "channels": {
                    ch: {
                        "group": "dlq-g",
                        "max_retries": 2,
                        "redis": {
                            "min_idle_time": 300,
                            "claim_interval": 150,
                            "dlq_check_interval": 1,
                        },
                        "dead_lettering": {"enabled": True, "queue_name": dlq_name},
                        "handler": self._h,
                    }
                }
            }

        async def _h(self, payload: Any, raw: Any) -> None:
            attempts["n"] += 1
            raise ValueError(f"boom #{attempts['n']}")

    pub = await _make_broker("pub-dlq", RedisAdapter(redis_url=REDIS_URL))
    adapter = RedisAdapter(redis_url=REDIS_URL)
    sub = await _make_broker("sub-dlq", adapter)
    await sub.register(DlqSvc())

    await pub.start()
    await sub.start()
    try:
        await asyncio.sleep(0.3)
        await pub.send_to_channel(ch, {"id": 1})
        # Wait for retries to exhaust and message to land in DLQ
        deadline = time.monotonic() + 40.0
        dlq_msgs: list[Any] = []
        while time.monotonic() < deadline:
            await asyncio.sleep(0.5)
            try:
                dlq_msgs = await adapter.redis.xrange(dlq_name.encode(), b"-", b"+")
            except Exception:
                dlq_msgs = []
            if dlq_msgs:
                break
        if not dlq_msgs:
            return False, f"no DLQ message after {attempts['n']} attempts"
        return True, f"DLQ received {len(dlq_msgs)} msg after {attempts['n']} attempts"
    finally:
        await pub.stop()
        await sub.stop()


async def test_retry() -> tuple[bool, str]:
    await _cleanup_redis_streams()
    ch = f"demo.retry.{uuid.uuid4().hex[:6]}"
    dlq_name = f"DLQ_RETRY_{uuid.uuid4().hex[:6]}"
    attempts = {"n": 0}
    success_evt = asyncio.Event()

    class RetrySvc(Service):
        name = "retry_svc"

        @property
        def schema(self) -> dict:
            return {
                "channels": {
                    ch: {
                        "group": "retry-g",
                        "max_retries": 5,
                        "redis": {"min_idle_time": 500, "claim_interval": 200},
                        "dead_lettering": {"enabled": True, "queue_name": dlq_name},
                        "handler": self._h,
                    }
                }
            }

        async def _h(self, payload: Any, raw: Any) -> None:
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise ValueError(f"transient #{attempts['n']}")
            success_evt.set()

    pub = await _make_broker("pub-retry", RedisAdapter(redis_url=REDIS_URL))
    sub = await _make_broker("sub-retry", RedisAdapter(redis_url=REDIS_URL))
    await sub.register(RetrySvc())

    await pub.start()
    await sub.start()
    try:
        await asyncio.sleep(0.3)
        await pub.send_to_channel(ch, {"id": 42})
        await asyncio.wait_for(success_evt.wait(), timeout=20.0)
        if attempts["n"] < 3:
            return False, f"success on attempt {attempts['n']} (<3)"
        return True, f"succeeded on attempt {attempts['n']}"
    finally:
        await pub.stop()
        await sub.stop()


async def test_graceful_shutdown() -> tuple[bool, str]:
    await _cleanup_redis_streams()
    ch = f"demo.graceful.{uuid.uuid4().hex[:6]}"
    completed = {"n": 0}
    started = asyncio.Event()

    class SlowSvc(Service):
        name = "slow_svc"

        @property
        def schema(self) -> dict:
            return {"channels": {ch: {"group": "slow-g", "handler": self._h}}}

        async def _h(self, payload: Any, raw: Any) -> None:
            started.set()
            await asyncio.sleep(1.5)
            completed["n"] += 1

    pub = await _make_broker("pub-gr", RedisAdapter(redis_url=REDIS_URL))
    sub = await _make_broker("sub-gr", RedisAdapter(redis_url=REDIS_URL))
    await sub.register(SlowSvc())

    await pub.start()
    await sub.start()
    try:
        await asyncio.sleep(0.3)
        await pub.send_to_channel(ch, {"task": 1})
        await asyncio.wait_for(started.wait(), timeout=5.0)
        # Handler now sleeping. Stop subscriber — should wait for in-flight.
        t0 = time.monotonic()
        await sub.stop()
        elapsed = time.monotonic() - t0
        if completed["n"] != 1:
            return False, f"in-flight not drained (completed={completed['n']})"
        return True, f"drained in {elapsed:.2f}s"
    finally:
        await pub.stop()


async def test_nats_adapter() -> tuple[bool, str]:
    if not NATS_AVAILABLE:
        return False, "nats-py not installed (skipped)"
    if not _check_port(NATS_HOST, NATS_PORT):
        return False, f"NATS not reachable on {NATS_HOST}:{NATS_PORT} (skipped)"

    ch = f"demo_nats_{uuid.uuid4().hex[:6]}"
    got = asyncio.Event()
    received: list[Any] = []

    class NatsSvc(Service):
        name = "nats_svc"

        @property
        def schema(self) -> dict:
            return {"channels": {ch: {"group": "nats-g", "handler": self._h}}}

        async def _h(self, payload: Any, raw: Any) -> None:
            received.append(payload)
            got.set()

    pub_adapter = NatsAdapter(url=NATS_URL)
    sub_adapter = NatsAdapter(url=NATS_URL)
    pub = await _make_broker("pub-nats", pub_adapter)
    sub = await _make_broker("sub-nats", sub_adapter)
    await sub.register(NatsSvc())

    await pub.start()
    await sub.start()
    try:
        await asyncio.sleep(0.5)
        await pub.send_to_channel(ch, {"via": "nats"})
        await asyncio.wait_for(got.wait(), timeout=8.0)
        return True, f"NATS delivered payload={received[0]}"
    finally:
        await pub.stop()
        await sub.stop()


# ── Runner ───────────────────────────────────────────────────────────────────
TESTS: list[tuple[str, Any]] = [
    ("basic_publish_subscribe", test_basic_publish_subscribe),
    ("consumer_groups", test_consumer_groups),
    ("dlq", test_dlq),
    ("retry", test_retry),
    ("graceful_shutdown", test_graceful_shutdown),
    ("nats_adapter", test_nats_adapter),
]


async def main() -> int:
    print(f"\n{BOLD}{'=' * 70}{RST}")
    print(f"  {BOLD}MoleculerPy Channels — Demo Stand{RST}")
    print(f"{BOLD}{'=' * 70}{RST}\n")

    # Pre-flight checks
    if not _check_port(REDIS_HOST, REDIS_PORT):
        print(f"{R}[FAIL]{RST} Redis not reachable on {REDIS_HOST}:{REDIS_PORT}")
        print(f"{D}Hint: docker ps | grep valkey{RST}")
        return 2
    print(f"{G}[OK]{RST}   Redis reachable on {REDIS_HOST}:{REDIS_PORT}")

    if NATS_AVAILABLE and _check_port(NATS_HOST, NATS_PORT):
        print(f"{G}[OK]{RST}   NATS reachable on {NATS_HOST}:{NATS_PORT}")
    else:
        print(f"{Y}[WARN]{RST} NATS not reachable — test 6 will skip")
    print()

    results: list[tuple[str, bool, str, float]] = []
    for name, fn in TESTS:
        print(f"{B}>>{RST} {name} ...", flush=True)
        t0 = time.monotonic()
        try:
            ok, detail = await fn()
        except Exception as e:
            ok, detail = False, f"exception: {type(e).__name__}: {e}"
        elapsed = time.monotonic() - t0
        results.append((name, ok, detail, elapsed))
        status = f"{G}PASS{RST}" if ok else f"{R}FAIL{RST}"
        print(f"   {status} ({elapsed:.2f}s) — {detail}\n")

    # Summary table
    print(f"{BOLD}{'=' * 70}{RST}")
    print(f"  {BOLD}Summary{RST}")
    print(f"{BOLD}{'=' * 70}{RST}")
    print(f"{'Test':<30} {'Status':<10} {'Time':<10} Detail")
    print("-" * 70)
    passed = 0
    for name, ok, detail, elapsed in results:
        status = f"{G}PASS{RST}" if ok else f"{R}FAIL{RST}"
        print(f"{name:<30} {status:<19} {elapsed:>6.2f}s   {detail}")
        if ok:
            passed += 1
    print("-" * 70)
    total = len(results)
    print(f"{BOLD}{passed}/{total} passed{RST}\n")
    return 0 if passed == total else 1


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        print(f"\n{Y}interrupted{RST}")
        sys.exit(130)
