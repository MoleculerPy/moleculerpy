#!/usr/bin/env python3
"""Cacher integration demo stand — Memory + LRU + Redis on real Redis.

Runs 7 integration tests against real cachers (Memory, MemoryLRU, Redis on
localhost:6381 — Valkey docker container). Prints a colored PASS/FAIL table
and exits 0 on success, 1 on any failure.

Usage:
    .venv/bin/python moleculerpy/examples/demo_cacher.py

Pre-flight: Redis must be reachable on localhost:6381.
Uses Redis DB 15 for isolation (flushed before/after each test).
"""

from __future__ import annotations

import asyncio
import logging
import socket
import sys
import time
from dataclasses import dataclass, field
from typing import Any

logging.basicConfig(level=logging.CRITICAL)

from moleculerpy.broker import ServiceBroker
from moleculerpy.cacher import MemoryCacher, MemoryLRUCacher, RedisCacher
from moleculerpy.decorators import action
from moleculerpy.service import Service

REDIS_HOST = "localhost"
REDIS_PORT = 6381
REDIS_DB = 15
REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"

# ─── ANSI colors ────────────────────────────────────────────────────────────
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
BOLD = "\033[1m"
RESET = "\033[0m"


def _port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        rc = s.connect_ex((host, port))
        s.close()
        return rc == 0
    except OSError:
        return False


@dataclass
class TestResult:
    name: str
    status: str = "?"
    duration: float = 0.0
    error: str = ""
    details: list[str] = field(default_factory=list)


# ─── Flush helper ───────────────────────────────────────────────────────────


async def _flush_db() -> None:
    """Flush isolated Redis DB 15 between tests."""
    import redis.asyncio as aioredis

    client = aioredis.Redis.from_url(REDIS_URL, decode_responses=False)
    try:
        await client.flushdb()
    finally:
        await client.aclose()


# ─── Test 1: MemoryCacher get/set/delete/TTL ────────────────────────────────


async def test_memory_get_set() -> TestResult:
    r = TestResult(name="memory_get_set")
    t0 = time.perf_counter()
    try:
        cacher = MemoryCacher(ttl=1)
        await cacher.start()

        await cacher.set("foo", {"v": 1})
        val = await cacher.get("foo")
        assert val == {"v": 1}, f"expected {{'v':1}}, got {val}"
        r.details.append("set/get ok")

        await cacher.delete("foo")
        assert await cacher.get("foo") is None, "delete failed"
        r.details.append("delete ok")

        await cacher.set("ttl_key", "value", ttl=1)
        assert await cacher.get("ttl_key") == "value"
        await asyncio.sleep(1.3)
        expired = await cacher.get("ttl_key")
        assert expired is None, f"expected expired None, got {expired}"
        r.details.append("ttl expire ok")

        await cacher.stop()
        r.status = "PASS"
    except Exception as e:
        r.status = "FAIL"
        r.error = f"{type(e).__name__}: {e}"
    r.duration = time.perf_counter() - t0
    return r


# ─── Test 2: LRU eviction ───────────────────────────────────────────────────


async def test_lru_eviction() -> TestResult:
    r = TestResult(name="lru_eviction")
    t0 = time.perf_counter()
    try:
        cacher = MemoryLRUCacher(max=5)
        await cacher.start()

        for i in range(10):
            await cacher.set(f"k{i}", i)

        present = 0
        for i in range(10):
            if await cacher.get(f"k{i}") is not None:
                present += 1

        assert present <= 5, f"LRU exceeded cap: {present} entries"
        r.details.append(f"kept {present}/5 entries")

        # Oldest (k0..k4) should be evicted, newest (k5..k9) kept
        assert await cacher.get("k0") is None, "k0 should be evicted"
        assert await cacher.get("k9") == 9, "k9 should be kept"
        r.details.append("oldest evicted, newest kept")

        await cacher.stop()
        r.status = "PASS"
    except Exception as e:
        r.status = "FAIL"
        r.error = f"{type(e).__name__}: {e}"
    r.duration = time.perf_counter() - t0
    return r


# ─── Test 3: Redis real get/set/TTL ─────────────────────────────────────────


async def test_redis_real() -> TestResult:
    r = TestResult(name="redis_real")
    t0 = time.perf_counter()
    try:
        await _flush_db()
        cacher = RedisCacher(REDIS_URL)
        cacher.connected = False
        await cacher.connect()

        await cacher.set("hello", {"msg": "world", "n": 42})
        val = await cacher.get("hello")
        assert val == {"msg": "world", "n": 42}, f"got {val}"
        r.details.append("set/get roundtrip ok")

        await cacher.set("short", "bye", ttl=1)
        assert await cacher.get("short") == "bye"
        await asyncio.sleep(1.5)
        assert await cacher.get("short") is None, "TTL not expired"
        r.details.append("ttl expire ok")

        await cacher.disconnect()
        await _flush_db()
        r.status = "PASS"
    except Exception as e:
        r.status = "FAIL"
        r.error = f"{type(e).__name__}: {e}"
    r.duration = time.perf_counter() - t0
    return r


# ─── Test 4: @cache decorator via middleware ────────────────────────────────


class CountService(Service):
    name = "counter"

    def __init__(self) -> None:
        super().__init__(self.name)
        self.calls = 0

    @action(cache=True)
    async def compute(self, ctx: Any) -> int:
        self.calls += 1
        return int(ctx.params["x"]) * 2


async def test_caching_middleware() -> TestResult:
    r = TestResult(name="caching_middleware")
    t0 = time.perf_counter()
    broker: ServiceBroker | None = None
    try:
        await _flush_db()
        cacher = RedisCacher(REDIS_URL)
        broker = ServiceBroker(id="demo-cacher-mw", cacher=cacher)
        svc = CountService()
        await broker.register(svc)
        await broker.start()

        r1 = await broker.call("counter.compute", {"x": 7})
        r2 = await broker.call("counter.compute", {"x": 7})
        r3 = await broker.call("counter.compute", {"x": 7})

        assert r1 == r2 == r3 == 14, f"results {r1},{r2},{r3}"
        assert svc.calls == 1, f"handler invoked {svc.calls}x, expected 1 (cache miss only)"
        r.details.append(f"3 calls, handler run {svc.calls}x")

        r4 = await broker.call("counter.compute", {"x": 9})
        assert r4 == 18
        assert svc.calls == 2, f"new params should miss cache; calls={svc.calls}"
        r.details.append("new params → miss ok")

        r.status = "PASS"
    except Exception as e:
        r.status = "FAIL"
        r.error = f"{type(e).__name__}: {e}"
    finally:
        if broker:
            try:
                await broker.stop()
            except Exception:
                pass
        try:
            await _flush_db()
        except Exception:
            pass
    r.duration = time.perf_counter() - t0
    return r


# ─── Test 5: concurrent access ──────────────────────────────────────────────


async def test_concurrent() -> TestResult:
    r = TestResult(name="concurrent")
    t0 = time.perf_counter()
    try:
        await _flush_db()
        cacher = RedisCacher(REDIS_URL)
        await cacher.connect()

        async def setter(i: int) -> None:
            await cacher.set(f"c{i}", {"i": i})

        await asyncio.gather(*(setter(i) for i in range(100)))

        async def getter(i: int) -> Any:
            return await cacher.get(f"c{i}")

        results = await asyncio.gather(*(getter(i) for i in range(100)))
        missing = [i for i, v in enumerate(results) if v != {"i": i}]
        assert not missing, f"missing/wrong: {missing[:5]}"
        r.details.append("100 parallel set+get ok")

        await cacher.disconnect()
        await _flush_db()
        r.status = "PASS"
    except Exception as e:
        r.status = "FAIL"
        r.error = f"{type(e).__name__}: {e}"
    r.duration = time.perf_counter() - t0
    return r


# ─── Test 6: pattern clean ──────────────────────────────────────────────────


async def test_pattern_clean() -> TestResult:
    r = TestResult(name="pattern_clean")
    t0 = time.perf_counter()
    try:
        await _flush_db()
        cacher = RedisCacher(REDIS_URL)
        await cacher.connect()

        for i in range(10):
            await cacher.set(f"users.get:{i}", {"id": i})
        await cacher.set("posts.get:1", {"id": 1})

        await cacher.clean("users.*")

        for i in range(10):
            assert await cacher.get(f"users.get:{i}") is None, f"users.get:{i} not cleaned"
        assert await cacher.get("posts.get:1") == {"id": 1}, "posts should survive"
        r.details.append("10 users cleaned, posts kept")

        await cacher.disconnect()
        await _flush_db()
        r.status = "PASS"
    except Exception as e:
        r.status = "FAIL"
        r.error = f"{type(e).__name__}: {e}"
    r.duration = time.perf_counter() - t0
    return r


# ─── Test 7: get_with_ttl ───────────────────────────────────────────────────


async def test_get_with_ttl() -> TestResult:
    r = TestResult(name="get_with_ttl")
    t0 = time.perf_counter()
    try:
        await _flush_db()
        cacher = RedisCacher(REDIS_URL)
        await cacher.connect()

        await cacher.set("ttl_real", {"x": 1}, ttl=30)
        data, ttl = await cacher.get_with_ttl("ttl_real")
        assert data == {"x": 1}, f"data got {data}"
        assert ttl is not None and 0 < ttl <= 30, f"ttl got {ttl}"
        r.details.append(f"remaining ttl={ttl}s")

        # Missing key
        data2, _ttl2 = await cacher.get_with_ttl("nonexistent")
        assert data2 is None
        r.details.append("missing key ok")

        await cacher.disconnect()
        await _flush_db()
        r.status = "PASS"
    except Exception as e:
        r.status = "FAIL"
        r.error = f"{type(e).__name__}: {e}"
    r.duration = time.perf_counter() - t0
    return r


# ─── Runner + output ────────────────────────────────────────────────────────


def _print_table(results: list[TestResult]) -> None:
    print()
    print(f"{BOLD}{CYAN}═══ Cacher Demo Stand Results ═══{RESET}")
    print()
    print(f"  {BOLD}{'#':<3}{'Test':<25}{'Status':<10}{'Time':<10}Details{RESET}")
    print(f"  {'-' * 78}")
    for i, r in enumerate(results, 1):
        color = GREEN if r.status == "PASS" else RED
        status = f"{color}{r.status}{RESET}"
        details = ", ".join(r.details) if r.status == "PASS" else (YELLOW + r.error + RESET)
        dur = f"{r.duration * 1000:.0f}ms"
        # Pad colored status manually
        print(f"  {i:<3}{r.name:<25}{status:<19}{dur:<10}{details}")
    print()
    passed = sum(1 for r in results if r.status == "PASS")
    total = len(results)
    summary_color = GREEN if passed == total else RED
    print(f"  {summary_color}{BOLD}{passed}/{total} tests passed{RESET}")
    print()


async def main() -> int:
    print(f"{BOLD}{CYAN}MoleculerPy Cacher Demo Stand{RESET}")
    print(f"  Redis target: {REDIS_URL}")

    if not _port_open(REDIS_HOST, REDIS_PORT):
        print(f"  {RED}{BOLD}ERROR:{RESET} Redis not reachable on {REDIS_HOST}:{REDIS_PORT}")
        print(f"  Start: docker run -d -p {REDIS_PORT}:6379 valkey/valkey:7-alpine")
        return 1
    print(f"  {GREEN}Redis reachable.{RESET}")

    tests = [
        test_memory_get_set,
        test_lru_eviction,
        test_redis_real,
        test_caching_middleware,
        test_concurrent,
        test_pattern_clean,
        test_get_with_ttl,
    ]

    results: list[TestResult] = []
    for t in tests:
        print(f"  running {t.__name__}...")
        results.append(await t())

    _print_table(results)
    return 0 if all(r.status == "PASS" for r in results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
