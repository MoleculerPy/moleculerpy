#!/usr/bin/env python3
"""Unified orchestrator for MoleculerPy component demo stands.

Runs all demo stands sequentially (or a filtered subset), captures their
output, parses the PASS/FAIL summary, and prints a unified sweep table.

Usage:
    python examples/run_all_demos.py                 # run everything
    python examples/run_all_demos.py --quick         # skip comprehensive
    python examples/run_all_demos.py --only demo_cacher
    python examples/run_all_demos.py --skip demo_web
    python examples/run_all_demos.py --list          # just print the demo list

Exit code: 0 if all selected demos pass, 1 otherwise.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

EXAMPLES_DIR = Path(__file__).resolve().parent
PY = sys.executable

_IS_TTY = sys.stdout.isatty()


def _c(text: str, code: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _IS_TTY else text


def _green(s: str) -> str:
    return _c(s, "32")


def _red(s: str) -> str:
    return _c(s, "31")


def _yellow(s: str) -> str:
    return _c(s, "33")


def _cyan(s: str) -> str:
    return _c(s, "36")


def _bold(s: str) -> str:
    return _c(s, "1")


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


@dataclass
class Demo:
    name: str
    path: Path
    expected: str  # e.g. "7/7"
    timeout: int  # seconds
    needs: list[str] = field(default_factory=list)  # pre-flight labels, advisory


@dataclass
class Result:
    demo: Demo
    exit_code: int
    duration: float
    summary: str  # e.g. "7/7" or "-"
    ok: bool
    tail: list[str]


# ----- Demo registry ---------------------------------------------------------

DEMOS: list[Demo] = [
    Demo(
        name="demo_matrix",
        path=EXAMPLES_DIR / "demo_matrix.py",
        expected="28/28",
        timeout=180,
        needs=["nats", "redis", "mqtt", "rabbitmq", "kafka"],
    ),
    Demo(
        name="demo_comprehensive",
        path=EXAMPLES_DIR / "demo_comprehensive.py",
        expected="103/103",
        timeout=300,
        needs=["nats", "redis"],
    ),
    Demo(
        name="demo_crosslang",
        path=EXAMPLES_DIR / "demo_crosslang.py",
        expected="5/5",
        timeout=120,
        needs=["nats"],
    ),
    Demo(
        name="demo_crosslang_channels",
        path=EXAMPLES_DIR / "demo_crosslang_channels.py",
        expected="2/2",
        timeout=60,
        needs=["nats"],
    ),
    Demo(
        name="demo_cacher",
        path=EXAMPLES_DIR / "demo_cacher.py",
        expected="7/7",
        timeout=60,
        needs=["redis"],
    ),
    Demo(
        name="demo_channels",
        path=EXAMPLES_DIR / "demo_channels.py",
        expected="7/7",
        timeout=90,
        needs=["redis", "nats"],
    ),
    Demo(
        name="demo_repl",
        path=EXAMPLES_DIR / "demo_repl.py",
        expected="10/10",
        timeout=60,
        needs=["nats"],
    ),
    Demo(
        name="demo_web",
        path=EXAMPLES_DIR / "demo_web.py",
        expected="13/13",
        timeout=60,
        needs=[],
    ),
    Demo(
        name="demo_observability",
        path=EXAMPLES_DIR / "demo_observability.py",
        expected="9/9",
        timeout=60,
        needs=[],
    ),
]


# ----- Pre-flight ------------------------------------------------------------

# Map of logical label → (image/name substring to look for in `docker ps`)
BROKER_PATTERNS = {
    "nats": re.compile(r"nats", re.I),
    "redis": re.compile(r"redis|valkey", re.I),
    "mqtt": re.compile(r"mosquitto|mqtt|emqx", re.I),
    "rabbitmq": re.compile(r"rabbit", re.I),
    "kafka": re.compile(r"kafka|redpanda", re.I),
}


def preflight() -> dict[str, bool]:
    """Return a dict label → running? based on `docker ps`."""
    status = {k: False for k in BROKER_PATTERNS}
    if shutil.which("docker") is None:
        return status
    try:
        out = subprocess.run(
            ["docker", "ps", "--format", "{{.Image}} {{.Names}}"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (subprocess.TimeoutExpired, OSError):
        return status
    if out.returncode != 0:
        return status
    for line in out.stdout.splitlines():
        for label, pattern in BROKER_PATTERNS.items():
            if pattern.search(line):
                status[label] = True
    return status


def print_preflight(status: dict[str, bool]) -> None:
    print(_bold(_cyan("Pre-flight: Docker brokers")))
    for label, running in status.items():
        tag = _green("running") if running else _yellow("not detected")
        print(f"  {label:<10} {tag}")
    print()


# ----- Summary parsing -------------------------------------------------------

SUMMARY_PATTERNS = [
    # "7/7 tests passed", "5/5 passed", "28/28"
    re.compile(r"(\d+)\s*/\s*(\d+)\s*(?:tests?\s*)?passed", re.I),
    # "Passed: 90  | Failed: 0" — paired with Total
    re.compile(r"Passed:\s*(\d+).*?(?:Failed|Total):\s*(\d+)", re.I),
    # "Total: 28  |  OK: 28" → treat OK as pass count of Total
    re.compile(r"Total:\s*(\d+).*?OK:\s*(\d+)", re.I),
    # "Total: 9/9 passed"
    re.compile(r"Total:\s*(\d+)\s*/\s*(\d+)\s*passed", re.I),
]


def parse_summary(tail_lines: list[str]) -> str | None:
    """Return 'N/M' string or None if not found.

    Scans from the bottom up so the final summary wins.
    """
    joined = "\n".join(_strip_ansi(line) for line in tail_lines)
    # Pattern 1: N/M passed
    matches = list(SUMMARY_PATTERNS[0].finditer(joined))
    if matches:
        m = matches[-1]
        return f"{m.group(1)}/{m.group(2)}"
    # Pattern 2: Passed: X ... Total: Y
    m2 = SUMMARY_PATTERNS[1].search(joined)
    if m2:
        passed, total = m2.group(1), m2.group(2)
        # Second capture may be "Failed" count; detect.
        if "total" in m2.group(0).lower():
            return f"{passed}/{total}"
        failed = int(total)
        return f"{passed}/{int(passed) + failed}"
    # Pattern 3: Total: N ... OK: M
    m3 = SUMMARY_PATTERNS[2].search(joined)
    if m3:
        total, ok = m3.group(1), m3.group(2)
        return f"{ok}/{total}"
    # Pattern 4
    m4 = SUMMARY_PATTERNS[3].search(joined)
    if m4:
        return f"{m4.group(1)}/{m4.group(2)}"
    return None


# ----- Runner ----------------------------------------------------------------


def run_demo(demo: Demo) -> Result:
    print(f"  {_cyan('▶')} running {_bold(demo.name)} (expected {demo.expected})...")
    start = time.monotonic()
    try:
        proc = subprocess.run(
            [PY, str(demo.path)],
            cwd=str(EXAMPLES_DIR.parent),
            capture_output=True,
            text=True,
            timeout=demo.timeout,
            check=False,
        )
        duration = time.monotonic() - start
        tail = (proc.stdout or "").splitlines()[-40:]
        summary = parse_summary(tail) or "-"
        ok = proc.returncode == 0
        icon = _green("OK") if ok else _red("FAIL")
        print(f"    {icon}  exit={proc.returncode}  summary={summary}  ({duration:.1f}s)")
        return Result(demo, proc.returncode, duration, summary, ok, tail)
    except subprocess.TimeoutExpired:
        duration = time.monotonic() - start
        print(f"    {_red('TIMEOUT')} after {duration:.1f}s")
        return Result(demo, 124, duration, "-", False, ["TIMEOUT"])
    except FileNotFoundError:
        duration = time.monotonic() - start
        print(f"    {_red('MISSING')} file not found: {demo.path}")
        return Result(demo, 127, duration, "-", False, ["FILE NOT FOUND"])


# ----- Table -----------------------------------------------------------------


def print_table(results: list[Result]) -> None:
    name_w = 28
    print()
    top = "╔" + "═" * (name_w + 2) + "╦════════╦════════╦══════════╗"
    sep = "╠" + "═" * (name_w + 2) + "╬════════╬════════╬══════════╣"
    bot = "╚" + "═" * (name_w + 2) + "╩════════╩════════╩══════════╝"
    print(top)
    print(f"║ {_bold('Demo'):<{name_w + 9}} ║ Status ║ Result ║ Duration ║")
    print(sep)
    for r in results:
        status_raw = "OK" if r.ok else "FAIL"
        status = _green("OK    ") if r.ok else _red("FAIL  ")
        # status cell width accounting for ANSI escapes
        pad_status = status + " " * (6 - len(status_raw))
        dur = f"{r.duration:.1f}s"
        print(f"║ {r.demo.name:<{name_w}} ║ {pad_status} ║ {r.summary:<6} ║ {dur:<8} ║")
    print(bot)

    total_demos = len(results)
    passed_demos = sum(1 for r in results if r.ok)
    total_tests = 0
    passed_tests = 0
    for r in results:
        if "/" in r.summary:
            try:
                p, t = r.summary.split("/")
                passed_tests += int(p)
                total_tests += int(t)
            except ValueError:
                pass
    total_dur = sum(r.duration for r in results)
    line = (
        f"Total: {passed_demos}/{total_demos} demos"
        f" | {passed_tests}/{total_tests} tests"
        f" | {total_dur:.0f}s"
    )
    print(_bold(_green(line) if passed_demos == total_demos else _red(line)))
    print()


# ----- CLI -------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description="MoleculerPy demo orchestrator")
    parser.add_argument(
        "--quick",
        action="store_true",
        help="skip long-running demos (demo_comprehensive)",
    )
    parser.add_argument("--only", metavar="NAME", help="run only this demo")
    parser.add_argument(
        "--skip",
        metavar="NAME",
        action="append",
        default=[],
        help="skip a demo (repeatable)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="list demos and exit",
    )
    args = parser.parse_args()

    if args.list:
        for d in DEMOS:
            print(f"  {d.name:<22} {d.expected:<8} ~{d.timeout}s  {d.path}")
        return 0

    selected: list[Demo] = []
    for d in DEMOS:
        if args.only and d.name != args.only:
            continue
        if d.name in args.skip:
            continue
        if args.quick and d.name == "demo_comprehensive":
            continue
        selected.append(d)

    if not selected:
        print(_red("No demos selected."))
        return 1

    print(_bold(_cyan("═══ MoleculerPy Demo Orchestrator ═══")))
    print(f"  repo: {EXAMPLES_DIR.parent}")
    print(f"  python: {PY}")
    print(f"  demos: {len(selected)}")
    print()

    print_preflight(preflight())

    results: list[Result] = []
    for demo in selected:
        results.append(run_demo(demo))

    print_table(results)
    return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
