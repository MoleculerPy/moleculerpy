"""System health status for the MoleculerPy framework.

Port of Moleculer.js health.js (~109 LOC).
Provides point-in-time system health information using stdlib + psutil.

Moleculer.js compatible keys: cpu, mem, os, process, client, net, time
"""

from __future__ import annotations

import os
import platform
import socket
import sys
import time
from typing import Any

import psutil

from .metrics import get_ip_list, get_static_metrics


def get_health_status() -> dict[str, Any]:
    """Get system health information.

    Node.js equivalent: health.js getHealthStatus()

    Returns:
        Dict with keys: cpu, mem, os, process, client, net, time
    """
    static = get_static_metrics()

    # CPU
    cpu_percent = psutil.cpu_percent(interval=None)
    try:
        load_avg = os.getloadavg()
    except (AttributeError, OSError):
        load_avg = (0.0, 0.0, 0.0)

    cpu: dict[str, Any] = {
        "load1": round(load_avg[0], 2),
        "load5": round(load_avg[1], 2),
        "load15": round(load_avg[2], 2),
        "cores": static["cpu_cores_logical"],
        "utilization": round(cpu_percent, 1),
    }

    # Memory
    mem_info = psutil.virtual_memory()
    mem: dict[str, Any] = {
        "free": mem_info.available,
        "total": mem_info.total,
        # Node.js: percent = free * 100 / total (percent FREE, not used)
        "percent": round((mem_info.available * 100) / mem_info.total, 1)
        if mem_info.total > 0
        else 0,
    }

    # OS (Node.js: uptime, type, release, hostname, arch, platform, user)
    try:
        os_uptime = time.time() - psutil.boot_time()
    except Exception:
        os_uptime = 0.0

    os_info: dict[str, Any] = {
        "uptime": round(os_uptime, 3),
        "type": platform.system(),
        "release": platform.release(),
        "hostname": static["hostname"],
        "arch": platform.machine(),
        "platform": static["platform"],
        "user": _get_current_user(),
    }

    # Process
    proc_mem: Any = None
    proc_cpu: float = 0.0
    proc_create_time: float = time.time()
    try:
        proc = psutil.Process(os.getpid())
        proc_mem = proc.memory_info()
        with proc.oneshot():
            proc_cpu = proc.cpu_percent(interval=None)
            proc_create_time = proc.create_time()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass

    process: dict[str, Any] = {
        "pid": os.getpid(),
        "memory": {
            "rss": proc_mem.rss if proc_mem else 0,
            "vms": proc_mem.vms if proc_mem else 0,
        },
        "uptime": round(time.time() - proc_create_time, 3),
        "cpuUsage": round(proc_cpu, 1),
        "argv": sys.argv,
        "pythonVersion": static["python_version"],
    }

    # Client (framework info)
    from . import __version__  # noqa: PLC0415

    client: dict[str, Any] = {
        "type": "python",
        "version": __version__,
        "langVersion": sys.version,
    }

    # Network interfaces
    try:
        net_ifaces: dict[str, Any] = {}
        for iface_name, addrs in psutil.net_if_addrs().items():
            ipv4 = [a.address for a in addrs if a.family == socket.AF_INET]
            ipv6 = [a.address for a in addrs if a.family == socket.AF_INET6]
            if ipv4 or ipv6:
                net_ifaces[iface_name] = {"ip4": ipv4, "ip6": ipv6}
    except Exception:
        net_ifaces = {}

    net: dict[str, Any] = {
        "ip": get_ip_list(),
        "ifaces": net_ifaces,
    }

    # Time
    now = time.time()
    time_info: dict[str, Any] = {
        "now": round(now * 1000),  # milliseconds (Moleculer.js compatible)
        "iso": _format_iso(now),
        "utc": _format_utc(now),
    }

    return {
        "cpu": cpu,
        "mem": mem,
        "os": os_info,
        "process": process,
        "client": client,
        "net": net,
        "time": time_info,
    }


def _get_current_user() -> str:
    """Get current OS user name.

    Returns:
        Username string or empty string on failure.
    """
    try:
        import getpass  # noqa: PLC0415

        return getpass.getuser()
    except Exception:
        return ""


def _format_iso(ts: float) -> str:
    """Format Unix timestamp as ISO 8601 string (UTC).

    Args:
        ts: Unix timestamp in seconds

    Returns:
        ISO 8601 formatted string
    """
    import datetime  # noqa: PLC0415

    return datetime.datetime.fromtimestamp(ts, tz=datetime.UTC).isoformat()


def _format_utc(ts: float) -> str:
    """Format Unix timestamp as RFC 2822 UTC string.

    Args:
        ts: Unix timestamp in seconds

    Returns:
        UTC formatted string (e.g. "Thu, 01 Jan 2026 00:00:00 GMT")
    """
    import email.utils  # noqa: PLC0415

    return email.utils.formatdate(ts, usegmt=True)
