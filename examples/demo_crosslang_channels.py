"""Bidirectional Channels Interop Demo — MoleculerPy ↔ direct nats.js.

Companion to ``demo_crosslang.py``. ``demo_crosslang`` verifies the core
Moleculer v4 protocol (actions, events, discovery, lifecycle) cross-
language. This file verifies that the *JetStream wire* used by
``moleculerpy-channels`` is binary-compatible with a native Node.js
consumer/publisher on the same JetStream streams.

Why "direct nats.js", not @moleculer/channels
---------------------------------------------
``@moleculer/channels`` 0.2.0 has a regression with current ``nats`` (the
JavaScript client) 2.29.x: its ``manager.streams.add()`` call reports
``did_create: true`` in debug logs but the streams never actually land on
the NATS server, and the follow-up subscribe silently registers zero
consumers. ``moleculerpy-channels`` does not have that bug (see
``demo_channels`` which passes 7/7 against the same NATS broker), so
the issue is specifically on the Node.js side.

To answer the "can my Python + Node apps talk through channels?"
question meaningfully, this demo proves the *wire contract*:

    * Both sides use JetStream streams named ``payments_completed`` /
      ``orders_created`` (channel-name with dots → underscores).
    * Both sides publish/consume on the original subject names
      (``payments.completed`` / ``orders.created``).
    * Envelope is a plain JSON-encoded object.

If a Python service built on ``moleculerpy-channels`` and a Node.js
script using raw ``nats`` agree on those three things, any other Node.js
library that also agrees (including a future fixed ``@moleculer/channels``)
will interoperate the same way. This is the correct level to validate
wire compatibility.

Test matrix
-----------
    T1. Python publishes payments.completed → Node consumer picks it up
        and writes the payload to a file marker we inspect.

    T2. Node publishes orders.created (via a core NATS request from
        Python) → Python's moleculerpy-channels consumer hands it to
        its async handler.

Both tests use a per-run marker string that MUST round-trip intact
through the JetStream wire — a partial delivery (e.g. envelope drops)
is detected by the demo and fails loudly.

Run
---
    (cd moleculerpy && docker compose up -d nats)
    .venv/bin/python examples/demo_crosslang_channels.py

Exit codes
----------
    0 — both T1 and T2 passed
    1 — one or more tests failed
    2 — missing dependency (moleculerpy_channels, NATS, Node toolchain)
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# ANSI colors
# ---------------------------------------------------------------------------
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[36m"
DIM = "\033[2m"
BOLD = "\033[1m"
RST = "\033[0m"


# ---------------------------------------------------------------------------
# Pre-flight dependency check
# ---------------------------------------------------------------------------
try:
    from moleculerpy_channels import ChannelsMiddleware
    from moleculerpy_channels.adapters import NatsAdapter
except ImportError:
    print(f"{RED}ERROR: moleculerpy_channels not installed.{RST}", file=sys.stderr)
    print(f"{DIM}Install: pip install -e moleculerpy-channels[all]{RST}", file=sys.stderr)
    sys.exit(2)

try:
    import nats as nats_client  # noqa: F401 — used only in Python-side probes
except ImportError:
    print(f"{RED}ERROR: 'nats' package missing (pip install nats-py){RST}", file=sys.stderr)
    sys.exit(2)

from moleculerpy import Service, ServiceBroker
from moleculerpy.settings import Settings

NATS_HOST = "localhost"
NATS_PORT = 4223  # matches top-level docker-compose.yml
NATS_URL = f"nats://{NATS_HOST}:{NATS_PORT}"

REPO_ROOT = Path(__file__).resolve().parent.parent
NODE_SERVICES_DIR = REPO_ROOT / "tests" / "integration" / "node_services"
NODE_DIRECT_JS = NODE_SERVICES_DIR / "channels_interop_direct.js"


def _check_port(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _check_node_setup() -> tuple[bool, str]:
    if not NODE_DIRECT_JS.exists():
        return False, f"missing {NODE_DIRECT_JS}"
    if not (NODE_SERVICES_DIR / "node_modules" / "nats").exists():
        return False, f"'nats' package not installed in {NODE_SERVICES_DIR}"
    try:
        result = subprocess.run(
            ["node", "--version"], capture_output=True, text=True, timeout=3, check=False
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False, "node binary not found in PATH"
    if result.returncode != 0:
        return False, f"node --version returned {result.returncode}"
    return True, result.stdout.strip()


# ---------------------------------------------------------------------------
# Node.js subprocess wrapper
# ---------------------------------------------------------------------------
class NodeDirectBroker:
    """Spawns ``channels_interop_direct.js`` and waits for the ready marker."""

    def __init__(self, tag: str) -> None:
        self.proc: subprocess.Popen[bytes] | None = None
        self.log_path = Path(f"/tmp/crosslang_channels_node_{tag}.log")
        self.ready_path = Path(f"/tmp/crosslang_channels_ready_{tag}.marker")
        self._log_fh: Any = None

    async def start(self, settle: float = 8.0) -> None:
        # Ensure no stale marker from a previous run fools the wait loop.
        try:
            self.ready_path.unlink()
        except FileNotFoundError:
            pass

        env = os.environ.copy()
        env["NATS_URL"] = NATS_URL
        env["CROSSLANG_CH_LOG"] = str(self.log_path)
        env["CROSSLANG_CH_READY"] = str(self.ready_path)

        # Pipe Node stdout/stderr through a file for postmortem — tee'ing
        # through a pipe reader would work too but is more fragile.
        self._log_fh = open(str(self.log_path) + ".stdout", "wb")
        self.proc = subprocess.Popen(
            ["node", str(NODE_DIRECT_JS)],
            cwd=str(NODE_SERVICES_DIR),
            env=env,
            stdout=self._log_fh,
            stderr=subprocess.STDOUT,
        )

        deadline = time.monotonic() + settle
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(
                    f"Node.js direct harness exited early (log at {self.log_path}.stdout):\n"
                    f"{self._dump_stdout()}"
                )
            if self.ready_path.exists():
                # Ready marker written AFTER the consumer handle is
                # acquired, so publishes after this point are guaranteed
                # to be routed to the consumer.
                return
            await asyncio.sleep(0.1)
        raise TimeoutError(
            f"Node.js direct harness did not become ready in {settle}s "
            f"(log at {self.log_path}.stdout)"
        )

    async def request_publish_order(
        self, marker: str, *, product: str = "widget", quantity: int = 1
    ) -> None:
        """Ask Node to publish an orders.created message.

        Uses a fire-and-forget core NATS request subject the harness
        listens on. Kept on a separate short-lived NATS connection (not
        the demo broker's transit) so a bug in the Python broker's NATS
        transport can't mask a wire issue we're here to test.
        """
        import nats as _nats

        nc = await _nats.connect(NATS_URL)
        try:
            payload = json.dumps(
                {"marker": marker, "product": product, "quantity": quantity}
            ).encode()
            await nc.request("crosslang.directnode.publishOrder", payload, timeout=5.0)
        finally:
            await nc.close()

    def _dump_stdout(self) -> str:
        try:
            return (self.log_path.with_suffix(".log.stdout")).read_text(errors="replace")
        except OSError:
            try:
                return (Path(str(self.log_path) + ".stdout")).read_text(errors="replace")
            except OSError:
                return "(log unreadable)"

    def received_payments(self) -> list[dict[str, Any]]:
        """Parse the JSONL log the Node harness writes."""
        if not self.log_path.exists():
            return []
        out: list[dict[str, Any]] = []
        for raw_line in self.log_path.read_text(errors="replace").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return out

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=2)
        self.proc = None
        if self._log_fh is not None:
            try:
                self._log_fh.close()
            except Exception:
                pass
            self._log_fh = None


# ---------------------------------------------------------------------------
# Python service
# ---------------------------------------------------------------------------
_captured_orders: list[dict[str, Any]] = []


class PyInteropService(Service):
    """Python counterpart: consumes orders.created.

    Uses group ``py-analytics`` — distinct from whatever durable name the
    Node harness registers (``node_payments_consumer``) — so neither side
    accidentally load-balances the other out of delivery.
    """

    name = "py_interop"

    @property
    def schema(self) -> dict[str, Any]:
        return {
            "channels": {
                "orders.created": {
                    "group": "py-analytics",
                    "handler": self._handle_order,
                }
            }
        }

    async def _handle_order(self, payload: Any, raw: Any) -> None:
        _captured_orders.append(payload if isinstance(payload, dict) else {"raw": payload})


# ---------------------------------------------------------------------------
# Main test runner
# ---------------------------------------------------------------------------
async def _wipe_streams() -> None:
    """Delete any lingering NATS streams so each run starts from a known state."""
    import nats as _nats

    nc = await _nats.connect(NATS_URL)
    try:
        js = nc.jetstream()
        for s in await js.streams_info():
            await js.delete_stream(s.config.name)
    finally:
        await nc.close()


async def run() -> int:
    print(f"{BOLD}{CYAN}moleculerpy-channels ↔ direct nats.js channels interop{RST}\n")

    # ---- Pre-flight ------------------------------------------------------
    if not _check_port(NATS_HOST, NATS_PORT):
        print(f"{RED}NATS not reachable at {NATS_URL}{RST}")
        print(f"{DIM}Start with: (cd moleculerpy && docker compose up -d nats){RST}")
        return 2

    node_ok, node_msg = _check_node_setup()
    if not node_ok:
        print(f"{RED}Node.js setup not ready: {node_msg}{RST}")
        print(f"{DIM}(cd {NODE_SERVICES_DIR} && npm install){RST}")
        return 2
    print(f"{GREEN}✓{RST} NATS at {NATS_URL}")
    print(f"{GREEN}✓{RST} Node.js: {node_msg}")
    print()

    # Fresh NATS state so a previous demo run's streams don't mask the
    # real stream-creation path under test.
    await _wipe_streams()

    results: list[tuple[str, bool, str]] = []

    def record(name: str, ok: bool, detail: str = "") -> None:
        results.append((name, ok, detail))
        tag = f"{GREEN}PASS{RST}" if ok else f"{RED}FAIL{RST}"
        extra = f" {DIM}{detail}{RST}" if detail else ""
        print(f"  {tag}  {name}{extra}")

    run_tag = uuid.uuid4().hex[:8]
    node = NodeDirectBroker(tag=run_tag)
    py_broker: ServiceBroker | None = None

    try:
        print(f"{CYAN}Starting Node.js direct harness...{RST}")
        await node.start(settle=8.0)
        print(f"{GREEN}✓{RST} Node ready")

        print(f"{CYAN}Starting Python channels broker...{RST}")
        adapter = NatsAdapter(url=NATS_URL)
        py_broker = ServiceBroker(
            id="demo-crosslang-channels-py",
            settings=Settings(transporter=NATS_URL, log_level="CRITICAL"),
            middlewares=[ChannelsMiddleware(adapter=adapter)],
        )
        await py_broker.register(PyInteropService())
        await py_broker.start()

        # Give JetStream time to finish registering Python's consumer on
        # orders.created before anyone publishes there.
        await asyncio.sleep(1.5)
        print(f"{GREEN}✓{RST} Python broker connected\n")

        # ============================================================
        # T1: Python → Node channel delivery
        #     Python publishes payments.completed, Node's direct consumer
        #     writes to the JSONL log, we assert the marker appears.
        # ============================================================
        print(f"{BOLD}T1. Python publishes payments.completed → Node consumes{RST}")
        marker_t1 = f"py-run-{run_tag}"
        payment_payload = {
            "payment_id": marker_t1,
            "amount": 100.0,
            "currency": "EUR",
            "source": "python",
        }
        await py_broker.send_to_channel("payments.completed", payment_payload)

        delivered_to_node = False
        got: list[dict[str, Any]] = []
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            got = node.received_payments()
            if any(
                isinstance(entry, dict)
                and isinstance(entry.get("payload"), dict)
                and entry["payload"].get("payment_id") == marker_t1
                for entry in got
            ):
                delivered_to_node = True
                break
            await asyncio.sleep(0.3)

        record(
            "t1_python_publish_to_node",
            delivered_to_node,
            (
                f"marker={marker_t1} echoed in Node JSONL log"
                if delivered_to_node
                else f"marker not observed; node log entries={len(got)}"
            ),
        )

        # ============================================================
        # T2: Node → Python channel delivery
        #     Trigger Node's direct harness to publish on orders.created
        #     via a core NATS request subject. Python's
        #     moleculerpy-channels consumer picks it up and records.
        # ============================================================
        print(f"\n{BOLD}T2. Node publishes orders.created → Python consumes{RST}")
        _captured_orders.clear()
        marker_t2 = f"node-run-{run_tag}"

        await node.request_publish_order(marker_t2, product="crosslang-widget", quantity=7)

        delivered_to_python = False
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if any(
                isinstance(order, dict) and order.get("marker") == marker_t2
                for order in _captured_orders
            ):
                delivered_to_python = True
                break
            await asyncio.sleep(0.3)

        record(
            "t2_node_publish_to_python",
            delivered_to_python,
            (
                f"marker={marker_t2} captured by PyInteropService"
                if delivered_to_python
                else f"marker not captured; captured={_captured_orders!r}"
            ),
        )

    except Exception as e:
        print(f"{RED}fatal: {type(e).__name__}: {e}{RST}", file=sys.stderr)
        print(f"{DIM}--- Node stdout ---{RST}", file=sys.stderr)
        print(node._dump_stdout()[-4000:], file=sys.stderr)
        print(f"{DIM}--- end Node stdout ---{RST}", file=sys.stderr)
        results.append(("setup", False, f"{type(e).__name__}: {e}"))
    finally:
        if py_broker is not None:
            try:
                await py_broker.stop()
            except Exception:
                pass
        node.stop()

    # ---- Summary ----------------------------------------------------------
    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)
    failed = total - passed
    print()
    print(f"{CYAN}{'=' * 72}{RST}")
    print(f"  {BOLD}Results{RST}: {passed}/{total} passed, {failed} failed")
    print(f"{CYAN}{'=' * 72}{RST}")
    for name, ok, detail in results:
        mark = f"{GREEN}[OK]  {RST}" if ok else f"{RED}[FAIL]{RST}"
        line = f"  {mark} {name}"
        if detail and not ok:
            line += f" — {detail}"
        print(line)
    print()

    return 0 if failed == 0 and passed > 0 else 1


def main() -> int:
    try:
        return asyncio.run(run())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
