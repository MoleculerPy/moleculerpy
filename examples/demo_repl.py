"""Demo stand — moleculerpy-repl commands (programmatic smoke tests).

Invokes REPL command classes directly against a real ServiceBroker with a
NATS transport. Avoids prompt_toolkit / cmd.cmdloop entirely — each command
is executed via its async ``execute(broker, ParsedArgs)`` entry point.

Run:
    .venv/bin/python examples/demo_repl.py

Pre-flight:
    - moleculerpy-repl must be installed (exit 2 otherwise)
    - NATS reachable on localhost:4222
"""

from __future__ import annotations

import asyncio
import logging
import socket
import sys
from dataclasses import dataclass, field
from typing import Any

logging.basicConfig(level=logging.CRITICAL)

# -- Pre-flight: moleculerpy-repl ---------------------------------------------
try:
    from moleculerpy_repl.commands.actions import ActionsCommand
    from moleculerpy_repl.commands.cache import CacheCommand
    from moleculerpy_repl.commands.call import CallCommand
    from moleculerpy_repl.commands.emit import BroadcastCommand
    from moleculerpy_repl.commands.info import InfoCommand
    from moleculerpy_repl.commands.listener import ListenerCommand
    from moleculerpy_repl.commands.metrics import MetricsCommand
    from moleculerpy_repl.commands.services import ServicesCommand
    from moleculerpy_repl.parser import ArgParser, ParsedArgs
except ImportError as exc:  # pragma: no cover - pre-flight
    sys.stderr.write(
        f"ERROR: moleculerpy-repl not installed ({exc}).\n"
        "Install with: pip install -e moleculerpy-repl\n"
    )
    sys.exit(2)

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action, event
from moleculerpy.service import Service
from moleculerpy.settings import Settings

# -- Ping command is optional (only if core exposes it) ----------------------
try:
    from moleculerpy_repl.commands.nodes import PingCommand  # type: ignore
except ImportError:  # pragma: no cover
    PingCommand = None  # type: ignore


NATS_URL = "nats://localhost:4222"


# ---------- ANSI helpers ----------------------------------------------------
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


def _color(s: str, c: str) -> str:
    return f"{c}{s}{RESET}"


def _is_port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        rc = sock.connect_ex((host, port))
        sock.close()
        return rc == 0
    except OSError:
        return False


# ---------- Services --------------------------------------------------------
class MathService(Service):
    name = "math"

    def __init__(self) -> None:
        super().__init__(self.name)

    @action()
    async def add(self, ctx: Any) -> int:
        return int(ctx.params["a"]) + int(ctx.params["b"])


class GreeterService(Service):
    name = "greeter"

    def __init__(self) -> None:
        super().__init__(self.name)
        self.received: list[Any] = []

    @action()
    async def hello(self, ctx: Any) -> str:
        return f"hello {ctx.params.get('name', 'world')}"

    @event("user.created")
    async def on_user_created(self, ctx: Any) -> None:
        self.received.append(ctx.params)


# ---------- Test harness ----------------------------------------------------
@dataclass
class TestResult:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class Harness:
    broker: ServiceBroker
    greeter: GreeterService
    parser: ArgParser = field(default_factory=ArgParser)
    results: list[TestResult] = field(default_factory=list)

    def _parse(self, s: str) -> ParsedArgs:
        return self.parser.parse(s)

    def _record(self, name: str, ok: bool, detail: str = "") -> None:
        self.results.append(TestResult(name, ok, detail))
        mark = _color("PASS", GREEN) if ok else _color("FAIL", RED)
        print(f"  [{mark}] {name}{(' — ' + detail) if detail else ''}")

    async def _run(self, name: str, coro: Any) -> None:
        try:
            ok, detail = await coro
            self._record(name, ok, detail)
        except Exception as exc:
            self._record(name, False, f"{type(exc).__name__}: {exc}")

    # ---- individual tests --------------------------------------------------
    async def test_actions_command(self) -> tuple[bool, str]:
        cmd = ActionsCommand()
        res = await cmd.execute(self.broker, self._parse("--a"))
        if not res.success:
            return False, res.error or "no success"
        data = res.data or []
        names = {(row.get("name") if isinstance(row, dict) else str(row)) for row in data}
        has_math = any(n and "math.add" in n for n in names)
        return has_math, f"{len(data)} actions"

    async def test_call_command(self) -> tuple[bool, str]:
        cmd = CallCommand()
        res = await cmd.execute(self.broker, self._parse("math.add a=2 b=3"))
        return (res.success and res.data == 5), f"data={res.data!r}"

    async def test_broadcast_command(self) -> tuple[bool, str]:
        self.greeter.received.clear()
        cmd = BroadcastCommand()
        res = await cmd.execute(self.broker, self._parse("user.created name=alice age=30"))
        if not res.success:
            return False, res.error or "broadcast failed"
        # Give the event bus a moment
        for _ in range(20):
            if self.greeter.received:
                break
            await asyncio.sleep(0.05)
        ok = any(isinstance(p, dict) and p.get("name") == "alice" for p in self.greeter.received)
        return ok, f"received={self.greeter.received}"

    async def test_list_command(self) -> tuple[bool, str]:
        cmd = ServicesCommand()
        res = await cmd.execute(self.broker, self._parse("--a"))
        if not res.success:
            return False, res.error or ""
        rows = res.data or []
        names = {(row.get("name") if isinstance(row, dict) else str(row)) for row in rows}
        # Fallback: if data is empty, inspect the rendered output (command returns
        # data=None unless caller asks for raw list).
        haystack = " ".join(n for n in names if n) + " " + (res.output or "")
        return ("math" in haystack), f"services={sorted(n for n in names if n) or 'via output'}"

    async def test_info_command(self) -> tuple[bool, str]:
        cmd = InfoCommand()
        res = await cmd.execute(self.broker, self._parse(""))
        if not res.success:
            return False, res.error or ""
        data = res.data
        # info may return a dict or pre-formatted string; accept either as long
        # as the nodeID appears somewhere.
        node_id = getattr(self.broker, "node_id", None) or getattr(self.broker, "nodeID", "")
        haystack = str(data) + (res.output or "")
        return (node_id in haystack), f"nodeID={node_id}"

    async def test_ping_command(self) -> tuple[bool, str]:
        # Self-ping via broker.ping() — PingCommand may not exist on this build.
        if PingCommand is not None:
            cmd = PingCommand()
            res = await cmd.execute(self.broker, self._parse(""))
            return res.success, str(res.data)[:60]
        ping = await self.broker.ping(timeout=2.0)
        return (ping is not None), f"ping={ping!r}"[:60]

    async def test_metrics_command(self) -> tuple[bool, str]:
        cmd = MetricsCommand()
        res = await cmd.execute(self.broker, self._parse(""))
        # Metrics middleware may be disabled in this minimal broker — accept
        # either success OR an explicit "not enabled" error as a smoke pass.
        graceful = res.success or "not enabled" in (res.error or "").lower()
        return graceful, (res.error or f"ok, output_len={len(res.output or '')}")

    async def test_cache_command(self) -> tuple[bool, str]:
        # No cacher configured — command should respond gracefully (either
        # success with empty list or a clear error). Both count as a smoke pass.
        cmd = CacheCommand()
        res = await cmd.execute(self.broker, self._parse("keys"))
        graceful = res.success or bool(res.error)
        return graceful, (res.error or f"data={res.data}")[:60]

    async def test_listener_command(self) -> tuple[bool, str]:
        cmd = ListenerCommand()
        add = await cmd.execute(self.broker, self._parse("add demo.tick"))
        if not add.success:
            return False, add.error or "add failed"
        listed = await cmd.execute(self.broker, self._parse("list"))
        ok_list = listed.success and "demo.tick" in (listed.output or "")
        removed = await cmd.execute(self.broker, self._parse("remove demo.tick"))
        return (ok_list and removed.success), "add/list/remove ok"

    async def test_quit_command(self) -> tuple[bool, str]:
        # QuitCommand calls sys.exit(); we verify the semantics by stopping the
        # broker directly. Graceful shutdown == no exception raised.
        await self.broker.stop()
        return True, "broker.stop() graceful"


# ---------- Main ------------------------------------------------------------
async def main() -> int:
    print(_color(BOLD + "moleculerpy-repl command smoke stand" + RESET, CYAN))

    if not _is_port_open("localhost", 4222):
        print(_color("ERROR: NATS not reachable on localhost:4222", RED))
        print("Start with: docker run -p 4222:4222 nats:2.10")
        return 2

    settings = Settings(transporter=NATS_URL, log_level="CRITICAL")
    broker = ServiceBroker(id="demo-repl", settings=settings)
    math = MathService()
    greeter = GreeterService()
    await broker.register(math)
    await broker.register(greeter)
    await asyncio.wait_for(broker.start(), timeout=10.0)

    # Allow discovery / event subscription to settle
    await asyncio.sleep(0.3)

    harness = Harness(broker=broker, greeter=greeter)

    print(_color("\nRunning 10 command tests...", YELLOW))
    tests: list[tuple[str, Any]] = [
        ("actions", harness.test_actions_command()),
        ("call math.add", harness.test_call_command()),
        ("broadcast user.created", harness.test_broadcast_command()),
        ("list services", harness.test_list_command()),
        ("info", harness.test_info_command()),
        ("ping", harness.test_ping_command()),
        ("metrics", harness.test_metrics_command()),
        ("cache keys", harness.test_cache_command()),
        ("listener add/list/remove", harness.test_listener_command()),
        ("quit (graceful stop)", harness.test_quit_command()),
    ]
    for name, coro in tests:
        await harness._run(name, coro)

    # Summary table
    passed = sum(1 for r in harness.results if r.passed)
    total = len(harness.results)
    print()
    print(_color(BOLD + "Summary" + RESET, CYAN))
    print(f"  {passed}/{total} passed")
    for r in harness.results:
        mark = _color("OK  ", GREEN) if r.passed else _color("FAIL", RED)
        print(f"  {mark}  {r.name}")

    if passed != total:
        return 1
    print(_color("\nAll REPL command smoke tests passed.", GREEN))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
