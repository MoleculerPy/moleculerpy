"""Comprehensive Demo Matrix — all serializers x all transporters.

Integration test stand verifying every supported (serializer, transporter)
combination works end-to-end with a real ServiceBroker.

Usage:
    .venv/bin/python examples/demo_matrix.py
    .venv/bin/python examples/demo_matrix.py --transport memory
    .venv/bin/python examples/demo_matrix.py --serializer cbor

Docker services (optional, tested if reachable):
    (cd moleculerpy && docker compose up -d)    # NATS 4223 + Redis 6381
    (cd moleculerpy/tests/integration && docker compose -p integration up -d \
        mosquitto rabbitmq kafka)                # MQTT / AMQP / Kafka
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import socket
import sys
import time
from dataclasses import dataclass
from typing import Any

logging.basicConfig(level=logging.CRITICAL)

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action
from moleculerpy.service import Service
from moleculerpy.settings import Settings


class MathService(Service):
    name = "math"

    def __init__(self) -> None:
        super().__init__(self.name)

    @action()
    async def add(self, ctx: Any) -> int:
        return int(ctx.params["a"]) + int(ctx.params["b"])

    @action()
    async def complex_payload(self, ctx: Any) -> dict[str, Any]:
        return {
            "result": ctx.params.get("input", {}).get("value", 0) * 2,
            "meta": {"processed": True, "tags": ["demo", "test"]},
            "numbers": [1, 2, 3, 4, 5],
        }


def _is_port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        rc = sock.connect_ex((host, port))
        sock.close()
        return rc == 0
    except OSError:
        return False


@dataclass
class Transport:
    name: str
    url: str
    always_available: bool = False
    host: str = ""
    port: int = 0
    supports_remote: bool = True

    def is_available(self) -> bool:
        if self.always_available:
            return True
        return _is_port_open(self.host, self.port) if self.host else False


TRANSPORTS: list[Transport] = [
    Transport("memory", "memory://", always_available=True, supports_remote=False),
    Transport("tcp", "tcp://", always_available=True, supports_remote=True),
    Transport("nats", "nats://localhost:4223", host="localhost", port=4223),
    Transport("redis", "redis://localhost:6381", host="localhost", port=6381),
    Transport("mqtt", "mqtt://localhost:1883", host="localhost", port=1883),
    Transport("amqp", "amqp://guest:guest@localhost:5672", host="localhost", port=5672),
    Transport("kafka", "kafka://localhost:9092", host="localhost", port=9092),
]

SERIALIZERS: list[str] = ["json", "msgpack", "cbor", "protobuf"]


@dataclass
class CellResult:
    serializer: str
    transport: str
    status: str = "?"
    local_call: bool = False
    complex_payload: bool = False
    remote_call: bool | None = None
    benchmark_rps: float = 0.0
    error: str = ""
    duration: float = 0.0


async def _test_single_node(serializer: str, transport: Transport) -> tuple[bool, bool, float, str]:
    broker_id = f"demo-{transport.name}-{serializer}"
    try:
        settings = Settings(
            transporter=transport.url,
            serializer=serializer,
            log_level="CRITICAL",
        )
        broker = ServiceBroker(id=broker_id, settings=settings)
        await broker.register(MathService())
        await asyncio.wait_for(broker.start(), timeout=10.0)
    except Exception as e:
        # Stop partially-started broker to prevent resource leaks
        try:
            await asyncio.wait_for(broker.stop(), timeout=3.0)
        except Exception:
            pass
        return False, False, 0.0, f"start: {type(e).__name__}: {e}"

    local_ok = False
    complex_ok = False
    rps = 0.0

    try:
        result = await asyncio.wait_for(broker.call("math.add", {"a": 3, "b": 4}), timeout=3.0)
        local_ok = result == 7

        complex_result = await asyncio.wait_for(
            broker.call(
                "math.complex_payload",
                {"input": {"value": 21}, "extra": ["a", "b"]},
            ),
            timeout=3.0,
        )
        complex_ok = (
            isinstance(complex_result, dict)
            and complex_result.get("result") == 42
            and complex_result.get("meta", {}).get("processed") is True
        )

        if local_ok and complex_ok:
            n = 1000
            start = time.perf_counter()
            for _ in range(n):
                await broker.call("math.add", {"a": 1, "b": 2})
            elapsed = time.perf_counter() - start
            rps = n / elapsed if elapsed > 0 else 0.0
    except Exception as e:
        try:
            await asyncio.wait_for(broker.stop(), timeout=5.0)
        except Exception:
            pass
        return local_ok, complex_ok, rps, f"call: {type(e).__name__}: {e}"

    try:
        await asyncio.wait_for(broker.stop(), timeout=5.0)
    except Exception as e:
        return local_ok, complex_ok, rps, f"stop: {type(e).__name__}: {e}"

    return local_ok, complex_ok, rps, ""


async def _test_two_nodes(serializer: str, transport: Transport) -> tuple[bool, str]:
    if not transport.supports_remote:
        return False, "transport does not support multi-node"

    # Deterministic port offsets for TCP (avoids PYTHONHASHSEED collisions)
    _TCP_PORT_OFFSETS = {"json": 0, "msgpack": 2, "cbor": 4, "protobuf": 6}

    if transport.name == "tcp":
        offset = _TCP_PORT_OFFSETS.get(serializer, 8)
        base = 30100 + offset
        port_a, port_b = base, base + 1
        url_a = (
            f"tcp://localhost:{port_a}/demo-{serializer}-A,localhost:{port_b}/demo-{serializer}-B"
        )
        url_b = (
            f"tcp://localhost:{port_b}/demo-{serializer}-B,localhost:{port_a}/demo-{serializer}-A"
        )
    else:
        url_a = url_b = transport.url

    broker_a = ServiceBroker(
        id=f"demo-{serializer}-A",
        settings=Settings(transporter=url_a, serializer=serializer, log_level="CRITICAL"),
    )
    broker_b = ServiceBroker(
        id=f"demo-{serializer}-B",
        settings=Settings(transporter=url_b, serializer=serializer, log_level="CRITICAL"),
    )
    broker_a_started = False
    try:
        await broker_b.register(MathService())
        await asyncio.wait_for(broker_a.start(), timeout=10.0)
        broker_a_started = True
        await asyncio.wait_for(broker_b.start(), timeout=10.0)
    except Exception as e:
        # Stop partially-started broker(s) to prevent resource leaks
        if broker_a_started:
            try:
                await asyncio.wait_for(broker_a.stop(), timeout=5.0)
            except Exception:
                pass
        return False, f"two-start: {type(e).__name__}: {e}"

    remote_ok = False
    err = ""
    try:
        # Canonical Moleculer pattern: wait_for_services replaces fixed sleep.
        # Discovery is eventually-consistent (especially Kafka). wait_for_services
        # polls the registry until the remote service is reachable.
        await broker_a.wait_for_services(["math"], timeout=20.0, interval=0.3)
        result = await asyncio.wait_for(
            broker_a.call("math.add", {"a": 100, "b": 200}),
            timeout=5.0,
        )
        remote_ok = result == 300
    except Exception as e:
        err = f"remote-call: {type(e).__name__}: {e}"
    finally:
        try:
            await asyncio.wait_for(broker_b.stop(), timeout=5.0)
        except Exception:
            pass
        try:
            await asyncio.wait_for(broker_a.stop(), timeout=5.0)
        except Exception:
            pass

    return remote_ok, err


async def _test_cell(serializer: str, transport: Transport) -> CellResult:
    result = CellResult(serializer=serializer, transport=transport.name)
    start = time.perf_counter()

    if not transport.is_available():
        result.status = "SKIP"
        result.error = f"not reachable at {transport.host}:{transport.port}"
        return result

    try:
        local_ok, complex_ok, rps, err = await _test_single_node(serializer, transport)
        result.local_call = local_ok
        result.complex_payload = complex_ok
        result.benchmark_rps = rps
        if err:
            result.error = err

        if local_ok and complex_ok and transport.supports_remote:
            remote_ok, remote_err = await _test_two_nodes(serializer, transport)
            result.remote_call = remote_ok
            if remote_err and not result.error:
                result.error = remote_err

        if result.local_call and result.complex_payload:
            if result.remote_call is False:
                result.status = "PARTIAL"
            else:
                result.status = "OK"
        else:
            result.status = "FAIL"
    except Exception as e:
        result.status = "FAIL"
        result.error = f"unexpected: {type(e).__name__}: {e}"

    result.duration = time.perf_counter() - start
    return result


def _fmt_status(status: str) -> str:
    colors = {
        "OK": "\033[92mOK     \033[0m",
        "PARTIAL": "\033[93mPARTIAL\033[0m",
        "FAIL": "\033[91mFAIL   \033[0m",
        "SKIP": "\033[90mSKIP   \033[0m",
        "?": "?      ",
    }
    return colors.get(status, status)


def _fmt_bool(v: bool | None) -> str:
    if v is None:
        return " N/A"
    return "\033[92m  + \033[0m" if v else "\033[91m  - \033[0m"


def print_availability(transports: list[Transport]) -> None:
    print("\n  Transport availability:")
    for t in transports:
        if t.is_available():
            status = "\033[92mavailable\033[0m"
        else:
            status = "\033[90mNOT reachable\033[0m"
        marker = "" if t.always_available else f" ({t.host}:{t.port})"
        print(f"    {t.name:<10} {status}{marker}")
    print()


def print_matrix(results: list[CellResult]) -> None:
    print("\n" + "=" * 92)
    print("  MoleculerPy Demo Matrix")
    print("=" * 92)
    print()
    print(
        f"  {'Transport':<10} {'Serializer':<10} {'Status':<18} "
        f"{'Local':<6} {'Cplx':<6} {'Rem':<6} {'Benchmark':<15}"
    )
    print("  " + "-" * 88)

    for r in results:
        bench = f"{r.benchmark_rps:>9,.0f} rps" if r.benchmark_rps > 0 else "       --"
        print(
            f"  {r.transport:<10} {r.serializer:<10} {_fmt_status(r.status)} "
            f"{_fmt_bool(r.local_call)} {_fmt_bool(r.complex_payload)} "
            f"{_fmt_bool(r.remote_call)} {bench}"
        )
        if r.error and r.status not in ("SKIP", "OK"):
            print(f"    error: {r.error[:86]}")

    print("  " + "-" * 88)
    ok = sum(1 for r in results if r.status == "OK")
    partial = sum(1 for r in results if r.status == "PARTIAL")
    fail = sum(1 for r in results if r.status == "FAIL")
    skip = sum(1 for r in results if r.status == "SKIP")
    print(
        f"  Total: {len(results)}  |  OK: {ok}  |  PARTIAL: {partial}  |  FAIL: {fail}  |  SKIP: {skip}"
    )
    print("=" * 92 + "\n")


async def run_matrix(
    transport_filter: str | None = None,
    serializer_filter: str | None = None,
) -> int:
    active_transports = TRANSPORTS
    if transport_filter:
        active_transports = [t for t in TRANSPORTS if t.name == transport_filter]
        if not active_transports:
            print(f"Unknown transport: {transport_filter}")
            return 2

    active_serializers = SERIALIZERS
    if serializer_filter:
        if serializer_filter not in SERIALIZERS:
            print(f"Unknown serializer: {serializer_filter}")
            return 2
        active_serializers = [serializer_filter]

    print_availability(active_transports)

    results: list[CellResult] = []
    total = len(active_transports) * len(active_serializers)
    idx = 0

    for transport in active_transports:
        for serializer in active_serializers:
            idx += 1
            print(
                f"  [{idx}/{total}] Testing {transport.name} + {serializer}...",
                end=" ",
                flush=True,
            )
            result = await _test_cell(serializer, transport)
            results.append(result)
            print(f"{result.status} ({result.duration:.2f}s)")

    print_matrix(results)

    non_skip_fails = sum(1 for r in results if r.status in ("FAIL", "PARTIAL"))
    return 1 if non_skip_fails > 0 else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="MoleculerPy demo matrix")
    parser.add_argument("--transport", "-t", help="Only test this transport")
    parser.add_argument("--serializer", "-s", help="Only test this serializer")
    args = parser.parse_args()

    try:
        return asyncio.run(
            run_matrix(
                transport_filter=args.transport,
                serializer_filter=args.serializer,
            )
        )
    except KeyboardInterrupt:
        print("\nInterrupted")
        return 130


if __name__ == "__main__":
    sys.exit(main())
