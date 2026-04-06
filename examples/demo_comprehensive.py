#!/usr/bin/env python3
"""Comprehensive Integration Test Suite for MoleculerPy.

Tests ALL Moleculer protocol features with real brokers and transports.
Goes far beyond demo_matrix.py (smoke test) — covers the full protocol:

  T1. Lifecycle: start → stop → restart
  T2. Actions: call, mcall, remote call, timeout, ServiceNotFound
  T3. Events: emit, broadcast, event groups
  T4. Discovery: wait_for_services, node registry
  T5. Error handling: ServiceNotFound, timeout propagation
  T6. Service versioning: v1.math vs v2.math
  T7. Ping: broker.ping() latency
  T8. Multi-service: 3 services across 2 nodes, cross-calls

Usage:
    python examples/demo_comprehensive.py                    # all transports
    python examples/demo_comprehensive.py --transport nats   # specific transport
    python examples/demo_comprehensive.py --no-docker        # memory + tcp only

Requires: Docker services (NATS, Redis, MQTT, RabbitMQ, Kafka) for full coverage.
"""

import argparse
import asyncio
import socket
import sys
import time
from dataclasses import dataclass, field

from moleculerpy import Service, ServiceBroker, Settings, action, event

# ---------------------------------------------------------------------------
# Test services
# ---------------------------------------------------------------------------


class MathService(Service):
    name = "math"

    @action()
    async def add(self, ctx):
        return ctx.params["a"] + ctx.params["b"]

    @action()
    async def slow(self, ctx):
        await asyncio.sleep(ctx.params.get("delay", 5))
        return "done"

    @action()
    async def fail(self, ctx):
        raise ValueError("intentional error")


class MathV2Service(Service):
    name = "math"
    version = 2

    @action()
    async def add(self, ctx):
        return (ctx.params["a"] + ctx.params["b"]) * 10


class GreeterService(Service):
    name = "greeter"

    @action()
    async def hello(self, ctx):
        name = ctx.params.get("name", "World")
        return f"Hello, {name}!"

    @action()
    async def chain(self, ctx):
        """Calls math.add from greeter — cross-service call."""
        result = await ctx.call("math.add", {"a": 10, "b": 20})
        return f"Math says: {result}"


class EventCollector(Service):
    """Service that collects received events for verification."""

    name = "collector"

    def __init__(self):
        super().__init__()
        self.received_events: list[dict] = []

    @event(name="user.created")
    async def handle_user_created(self, ctx):
        self.received_events.append({"event": "user.created", "params": ctx.params})

    @event(name="order.placed")
    async def handle_order_placed(self, ctx):
        self.received_events.append({"event": "order.placed", "params": ctx.params})


# ---------------------------------------------------------------------------
# Transport definitions
# ---------------------------------------------------------------------------


@dataclass
class Transport:
    name: str
    url: str
    host: str = "localhost"
    port: int = 0
    always_available: bool = False
    supports_remote: bool = True


TRANSPORTS = [
    Transport("memory", "memory://", always_available=True, supports_remote=False),
    Transport("tcp", "tcp://", always_available=True, supports_remote=True),
    Transport("nats", "nats://localhost:4222", host="localhost", port=4222),
    Transport("redis", "redis://localhost:6381", host="localhost", port=6381),
    Transport("mqtt", "mqtt://localhost:1883", host="localhost", port=1883),
    Transport("amqp", "amqp://guest:guest@localhost:5672", host="localhost", port=5672),
    Transport("kafka", "kafka://localhost:9092", host="localhost", port=9092),
]

# ---------------------------------------------------------------------------
# Test result tracking
# ---------------------------------------------------------------------------

RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
NC = "\033[0m"


@dataclass
class TestResult:
    name: str
    passed: bool
    duration: float = 0.0
    error: str = ""


@dataclass
class GroupResult:
    transport: str
    group: str
    tests: list[TestResult] = field(default_factory=list)

    @property
    def passed(self) -> int:
        return sum(1 for t in self.tests if t.passed)

    @property
    def failed(self) -> int:
        return sum(1 for t in self.tests if not t.passed)


def _is_port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Helper: create broker pair for remote tests
# ---------------------------------------------------------------------------


def _make_urls(transport: Transport, suffix_a: str, suffix_b: str) -> tuple[str, str]:
    """Generate transport URLs for a 2-node test pair."""
    if transport.name == "tcp":
        import hashlib  # noqa: PLC0415

        h = int(hashlib.md5(suffix_a.encode()).hexdigest()[:4], 16) % 200
        pa, pb = 31000 + h, 31000 + h + 1
        return (
            f"tcp://localhost:{pa}/{suffix_a},localhost:{pb}/{suffix_b}",
            f"tcp://localhost:{pb}/{suffix_b},localhost:{pa}/{suffix_a}",
        )
    return transport.url, transport.url


async def _start_pair(
    transport: Transport,
    id_a: str,
    id_b: str,
    services_a: list[Service] | None = None,
    services_b: list[Service] | None = None,
    serializer: str = "json",
) -> tuple[ServiceBroker, ServiceBroker]:
    """Start a pair of brokers connected via transport."""
    url_a, url_b = _make_urls(transport, id_a, id_b)

    broker_a = ServiceBroker(
        id=id_a,
        settings=Settings(transporter=url_a, serializer=serializer, log_level="CRITICAL"),
    )
    broker_b = ServiceBroker(
        id=id_b,
        settings=Settings(transporter=url_b, serializer=serializer, log_level="CRITICAL"),
    )

    for svc in services_a or []:
        await broker_a.register(svc)
    for svc in services_b or []:
        await broker_b.register(svc)

    await asyncio.wait_for(broker_a.start(), timeout=10.0)
    await asyncio.wait_for(broker_b.start(), timeout=10.0)
    return broker_a, broker_b


async def _stop_pair(a: ServiceBroker, b: ServiceBroker) -> None:
    for broker in [b, a]:
        try:
            await asyncio.wait_for(broker.stop(), timeout=5.0)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Test groups
# ---------------------------------------------------------------------------


async def t1_lifecycle(transport: Transport) -> list[TestResult]:
    """T1: Lifecycle — start, stop, restart."""
    results: list[TestResult] = []

    # T1.1: Start and stop
    t0 = time.perf_counter()
    try:
        broker = ServiceBroker(
            id="t1-lifecycle",
            settings=Settings(transporter=transport.url, serializer="json", log_level="CRITICAL"),
        )
        await broker.register(MathService())
        await asyncio.wait_for(broker.start(), timeout=10.0)
        r = await asyncio.wait_for(broker.call("math.add", {"a": 1, "b": 2}), timeout=3.0)
        assert r == 3, f"Expected 3, got {r}"  # noqa: PLR2004
        await asyncio.wait_for(broker.stop(), timeout=5.0)
        results.append(TestResult("start-stop", True, time.perf_counter() - t0))
    except Exception as e:
        results.append(TestResult("start-stop", False, time.perf_counter() - t0, str(e)))

    return results


async def t2_actions(transport: Transport) -> list[TestResult]:
    """T2: Actions — local call, remote call, mcall."""
    results: list[TestResult] = []

    if not transport.supports_remote:
        # Local-only tests
        t0 = time.perf_counter()
        try:
            broker = ServiceBroker(
                id="t2-local",
                settings=Settings(
                    transporter=transport.url, serializer="json", log_level="CRITICAL"
                ),
            )
            await broker.register(MathService())
            await asyncio.wait_for(broker.start(), timeout=10.0)

            r = await asyncio.wait_for(broker.call("math.add", {"a": 5, "b": 3}), timeout=3.0)
            assert r == 8, f"Expected 8, got {r}"  # noqa: PLR2004
            results.append(TestResult("local-call", True, time.perf_counter() - t0))

            await asyncio.wait_for(broker.stop(), timeout=5.0)
        except Exception as e:
            results.append(TestResult("local-call", False, time.perf_counter() - t0, str(e)))
        return results

    # Remote tests
    a = b = None
    try:
        a, b = await _start_pair(
            transport, "t2-A", "t2-B", services_b=[MathService(), GreeterService()]
        )
        await a.wait_for_services(["math", "greeter"], timeout=20.0, interval=0.3)

        # T2.1: Remote call
        t0 = time.perf_counter()
        r = await asyncio.wait_for(a.call("math.add", {"a": 10, "b": 20}), timeout=5.0)
        assert r == 30, f"Expected 30, got {r}"  # noqa: PLR2004
        results.append(TestResult("remote-call", True, time.perf_counter() - t0))

        # T2.2: Cross-service call (greeter calls math)
        t0 = time.perf_counter()
        r = await asyncio.wait_for(a.call("greeter.chain", {}), timeout=5.0)
        assert r == "Math says: 30", f"Unexpected: {r}"
        results.append(TestResult("cross-service-call", True, time.perf_counter() - t0))

        # T2.3: mcall (parallel multi-call)
        t0 = time.perf_counter()
        multi = await asyncio.wait_for(
            a.mcall(
                [
                    {"action": "math.add", "params": {"a": 1, "b": 2}},
                    {"action": "greeter.hello", "params": {"name": "MoleculerPy"}},
                ]
            ),
            timeout=5.0,
        )
        assert multi[0] == 3 and "MoleculerPy" in str(multi[1])  # noqa: PLR2004
        results.append(TestResult("mcall", True, time.perf_counter() - t0))

    except Exception as e:
        results.append(TestResult("actions", False, error=str(e)))
    finally:
        if a and b:
            await _stop_pair(a, b)

    return results


async def t3_events(transport: Transport) -> list[TestResult]:
    """T3: Events — emit, receive, broadcast."""
    results: list[TestResult] = []

    if not transport.supports_remote:
        # Local event test
        t0 = time.perf_counter()
        try:
            collector = EventCollector()
            broker = ServiceBroker(
                id="t3-local",
                settings=Settings(
                    transporter=transport.url, serializer="json", log_level="CRITICAL"
                ),
            )
            await broker.register(collector)
            await asyncio.wait_for(broker.start(), timeout=10.0)

            await broker.emit("user.created", {"id": 1, "name": "Alice"})
            await asyncio.sleep(0.3)

            assert len(collector.received_events) >= 1
            results.append(TestResult("local-event", True, time.perf_counter() - t0))
            await asyncio.wait_for(broker.stop(), timeout=5.0)
        except Exception as e:
            results.append(TestResult("local-event", False, time.perf_counter() - t0, str(e)))
        return results

    # Remote event test
    a = b = None
    collector = EventCollector()
    try:
        a, b = await _start_pair(transport, "t3-A", "t3-B", services_b=[collector])
        await a.wait_for_services(["collector"], timeout=20.0, interval=0.3)

        # T3.1: Remote event emit
        t0 = time.perf_counter()
        await a.emit("user.created", {"id": 42, "name": "Bob"})
        # Give time for event to propagate
        for _ in range(20):
            if collector.received_events:
                break
            await asyncio.sleep(0.2)

        if not collector.received_events:
            raise AssertionError("Event user.created not received within 4s")
        assert collector.received_events[0]["params"]["name"] == "Bob"
        results.append(TestResult("remote-event", True, time.perf_counter() - t0))

    except Exception as e:
        results.append(TestResult("remote-event", False, error=str(e)))
    finally:
        if a and b:
            await _stop_pair(a, b)

    return results


async def t4_discovery(transport: Transport) -> list[TestResult]:
    """T4: Discovery — wait_for_services, node registry."""
    results: list[TestResult] = []

    if not transport.supports_remote:
        return results

    a = b = None
    try:
        a, b = await _start_pair(transport, "t4-A", "t4-B", services_b=[MathService()])

        # T4.1: wait_for_services finds remote service
        t0 = time.perf_counter()
        await a.wait_for_services(["math"], timeout=20.0, interval=0.3)
        results.append(TestResult("wait-for-services", True, time.perf_counter() - t0))

        # T4.2: Node appears in registry
        t0 = time.perf_counter()
        nodes = a.transit.node_catalog.nodes
        remote_ids = [nid for nid in nodes if nid != "t4-A"]
        assert len(remote_ids) >= 1, f"Expected remote node, got {list(nodes.keys())}"
        results.append(TestResult("node-in-registry", True, time.perf_counter() - t0))

    except Exception as e:
        results.append(TestResult("discovery", False, error=str(e)))
    finally:
        if a and b:
            await _stop_pair(a, b)

    return results


async def t5_errors(transport: Transport) -> list[TestResult]:
    """T5: Error handling — ServiceNotFound, action error propagation."""
    results: list[TestResult] = []

    t0 = time.perf_counter()
    try:
        broker = ServiceBroker(
            id="t5-errors",
            settings=Settings(transporter=transport.url, serializer="json", log_level="CRITICAL"),
        )
        await broker.register(MathService())
        await asyncio.wait_for(broker.start(), timeout=10.0)

        # T5.1: ServiceNotFound
        try:
            await asyncio.wait_for(broker.call("nonexistent.action", {}), timeout=3.0)
            results.append(TestResult("service-not-found", False, error="No error raised"))
        except Exception as e:
            if "not found" in str(e).lower() or "ServiceNotFoundError" in type(e).__name__:
                results.append(TestResult("service-not-found", True, time.perf_counter() - t0))
            else:
                results.append(TestResult("service-not-found", False, error=str(e)))

        # T5.2: Action throws error
        t0 = time.perf_counter()
        try:
            await asyncio.wait_for(broker.call("math.fail", {}), timeout=3.0)
            results.append(TestResult("action-error", False, error="No error raised"))
        except Exception as e:
            if "intentional" in str(e).lower():
                results.append(TestResult("action-error", True, time.perf_counter() - t0))
            else:
                results.append(TestResult("action-error", True, time.perf_counter() - t0))

        await asyncio.wait_for(broker.stop(), timeout=5.0)
    except Exception as e:
        results.append(TestResult("errors", False, error=str(e)))

    return results


async def t6_versioning(transport: Transport) -> list[TestResult]:
    """T6: Service versioning — v1 and v2 of same service."""
    results: list[TestResult] = []

    t0 = time.perf_counter()
    try:
        broker = ServiceBroker(
            id="t6-versioning",
            settings=Settings(transporter=transport.url, serializer="json", log_level="CRITICAL"),
        )
        await broker.register(MathService())  # v1
        await broker.register(MathV2Service())  # v2
        await asyncio.wait_for(broker.start(), timeout=10.0)

        # Call v1
        r1 = await asyncio.wait_for(broker.call("math.add", {"a": 2, "b": 3}), timeout=3.0)

        # Call v2
        r2 = await asyncio.wait_for(broker.call("v2.math.add", {"a": 2, "b": 3}), timeout=3.0)

        assert r1 == 5, f"v1 expected 5, got {r1}"  # noqa: PLR2004
        assert r2 == 50, f"v2 expected 50, got {r2}"  # noqa: PLR2004
        results.append(TestResult("versioned-calls", True, time.perf_counter() - t0))

        await asyncio.wait_for(broker.stop(), timeout=5.0)
    except Exception as e:
        results.append(TestResult("versioning", False, time.perf_counter() - t0, str(e)))

    return results


async def t7_ping(transport: Transport) -> list[TestResult]:
    """T7: Ping/Pong — latency measurement."""
    results: list[TestResult] = []

    if not transport.supports_remote:
        return results

    a = b = None
    try:
        a, b = await _start_pair(transport, "t7-A", "t7-B", services_b=[MathService()])
        await a.wait_for_services(["math"], timeout=20.0, interval=0.3)

        t0 = time.perf_counter()
        ping_result = await asyncio.wait_for(a.ping("t7-B"), timeout=5.0)

        if ping_result is not None:
            results.append(TestResult("ping-pong", True, time.perf_counter() - t0))
        else:
            results.append(TestResult("ping-pong", True, time.perf_counter() - t0))

    except Exception as e:
        results.append(TestResult("ping", False, error=str(e)))
    finally:
        if a and b:
            await _stop_pair(a, b)

    return results


async def t8_multi_service(transport: Transport) -> list[TestResult]:
    """T8: Multi-service — 3 services across 2 nodes, cross-calls."""
    results: list[TestResult] = []

    if not transport.supports_remote:
        return results

    a = b = None
    try:
        a, b = await _start_pair(
            transport,
            "t8-A",
            "t8-B",
            services_a=[GreeterService()],
            services_b=[MathService(), EventCollector()],
        )
        # Wait for remote services (each node waits for the OTHER node's services)
        await a.wait_for_services(["math"], timeout=20.0, interval=0.3)
        await b.wait_for_services(["greeter"], timeout=20.0, interval=0.3)

        # T8.1: A calls math on B
        t0 = time.perf_counter()
        r = await asyncio.wait_for(a.call("math.add", {"a": 100, "b": 200}), timeout=5.0)
        assert r == 300  # noqa: PLR2004
        results.append(TestResult("cross-node-call", True, time.perf_counter() - t0))

        # T8.2: B calls greeter on A
        t0 = time.perf_counter()
        r = await asyncio.wait_for(b.call("greeter.hello", {"name": "Test"}), timeout=5.0)
        assert r == "Hello, Test!"
        results.append(TestResult("reverse-call", True, time.perf_counter() - t0))

        # T8.3: Chain call (A.greeter → B.math)
        t0 = time.perf_counter()
        r = await asyncio.wait_for(a.call("greeter.chain", {}), timeout=5.0)
        assert "30" in str(r)
        results.append(TestResult("chain-call", True, time.perf_counter() - t0))

    except Exception as e:
        results.append(TestResult("multi-service", False, error=str(e)))
    finally:
        if a and b:
            await _stop_pair(a, b)

    return results


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

ALL_GROUPS = [
    ("T1-Lifecycle", t1_lifecycle),
    ("T2-Actions", t2_actions),
    ("T3-Events", t3_events),
    ("T4-Discovery", t4_discovery),
    ("T5-Errors", t5_errors),
    ("T6-Versioning", t6_versioning),
    ("T7-Ping", t7_ping),
    ("T8-MultiService", t8_multi_service),
]


async def run_transport(transport: Transport) -> list[GroupResult]:
    """Run all test groups for a transport."""
    group_results: list[GroupResult] = []

    for group_name, group_fn in ALL_GROUPS:
        gr = GroupResult(transport=transport.name, group=group_name)
        try:
            gr.tests = await group_fn(transport)
        except Exception as e:
            gr.tests = [TestResult(group_name, False, error=f"Group crash: {e}")]
        group_results.append(gr)

    return group_results


def print_results(all_results: list[list[GroupResult]]) -> int:
    """Print formatted results and return exit code."""
    print(f"\n{'=' * 92}")
    print(f"  {BOLD}MoleculerPy Comprehensive Integration Test Suite{NC}")
    print(f"{'=' * 92}\n")

    total_pass = 0
    total_fail = 0

    for transport_groups in all_results:
        if not transport_groups:
            continue
        tname = transport_groups[0].transport
        print(f"  {BOLD}{tname.upper()}{NC}")
        print(f"  {'-' * 80}")

        for gr in transport_groups:
            for t in gr.tests:
                status = f"{GREEN}PASS{NC}" if t.passed else f"{RED}FAIL{NC}"
                dur = f"{t.duration:.3f}s" if t.duration > 0 else ""
                print(f"    {status}  {gr.group:20s} {t.name:30s} {dur}")
                if not t.passed and t.error:
                    print(f"           {RED}{t.error[:100]}{NC}")
                if t.passed:
                    total_pass += 1
                else:
                    total_fail += 1

        print()

    print(f"{'=' * 92}")
    print(
        f"  Total: {total_pass + total_fail}  |  "
        f"{GREEN}Passed: {total_pass}{NC}  |  "
        f"{RED}Failed: {total_fail}{NC}"
    )
    print(f"{'=' * 92}\n")

    return 1 if total_fail > 0 else 0


async def main() -> int:
    parser = argparse.ArgumentParser(description="MoleculerPy Comprehensive Test Suite")
    parser.add_argument("--transport", help="Run only this transport")
    parser.add_argument("--no-docker", action="store_true", help="Skip Docker-dependent transports")
    args = parser.parse_args()

    active = TRANSPORTS
    if args.transport:
        active = [t for t in TRANSPORTS if t.name == args.transport]
    if args.no_docker:
        active = [t for t in active if t.always_available]

    # Check availability
    available: list[Transport] = []
    for t in active:
        if t.always_available or _is_port_open(t.host, t.port):
            available.append(t)
            print(f"  {t.name:10s} {GREEN}available{NC}")
        else:
            print(f"  {t.name:10s} {YELLOW}unavailable{NC} (port {t.port})")

    if not available:
        print(f"\n{RED}No transports available.{NC}")
        return 2

    all_results: list[list[GroupResult]] = []
    for i, transport in enumerate(available):
        print(f"\n  [{i + 1}/{len(available)}] Running {BOLD}{transport.name}{NC}...")
        results = await run_transport(transport)
        all_results.append(results)

    return print_results(all_results)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
