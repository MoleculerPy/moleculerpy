"""System metrics collection for the MoleculerPy framework.

This module provides async-safe system metrics collection using psutil.
Designed to be compatible with Moleculer.js heartbeat protocol while
extending with additional Python-specific metrics.

Moleculer.js compatibility:
- cpu: CPU usage percentage (0-100, rounded to int)
- cpuSeq: Increment counter when CPU changes

Python extensions:
- memory: Memory usage percentage
- cpuCores: Number of CPU cores (logical)
- cpuPhysical: Number of physical CPU cores
"""

from __future__ import annotations

import asyncio
import os
import platform
import socket
from dataclasses import dataclass, field
from functools import lru_cache
from typing import TypedDict

import psutil


def _safe_cpu_count(*, logical: bool) -> int:
    """Return CPU count with fallback for restricted/sandboxed environments."""
    try:
        count = psutil.cpu_count(logical=logical)
        return count if isinstance(count, int) and count > 0 else 1
    except Exception:
        return 1


class StaticMetrics(TypedDict):
    """Cached static system info shape."""

    hostname: str
    platform: str
    python_version: str
    cpu_cores_logical: int
    cpu_cores_physical: int
    ip_list: list[str]


@dataclass(slots=True)
class CpuMetrics:
    """CPU metrics snapshot.

    Attributes:
        usage: CPU usage percentage (0-100, rounded to int like Moleculer.js)
        cores_logical: Number of logical CPU cores
        cores_physical: Number of physical CPU cores
        load_avg: System load average (1, 5, 15 minutes) - Unix only
    """

    usage: int
    cores_logical: int
    cores_physical: int
    load_avg: tuple[float, float, float] | None = None


@dataclass(slots=True)
class MemoryMetrics:
    """Memory metrics snapshot.

    Attributes:
        usage_percent: Memory usage percentage (0-100)
        total_bytes: Total physical memory in bytes
        available_bytes: Available memory in bytes
        used_bytes: Used memory in bytes
    """

    usage_percent: float
    total_bytes: int
    available_bytes: int
    used_bytes: int


@dataclass(slots=True)
class SystemMetrics:
    """Complete system metrics snapshot.

    Used for heartbeat and node info reporting.

    Attributes:
        cpu: CPU metrics
        memory: Memory metrics
        hostname: System hostname
        platform: Platform name (linux, darwin, win32)
        python_version: Python interpreter version
        ip_list: List of IP addresses
    """

    cpu: CpuMetrics
    memory: MemoryMetrics
    hostname: str
    platform: str
    python_version: str
    ip_list: list[str] = field(default_factory=list)


@lru_cache(maxsize=1)
def get_static_metrics() -> StaticMetrics:
    """Get static system metrics (cached, called once).

    Returns:
        Dictionary with static system info:
        - hostname: System hostname
        - platform: Platform name
        - python_version: Python version
        - cpu_cores_logical: Logical CPU count
        - cpu_cores_physical: Physical CPU count
        - ip_list: List of IP addresses
    """
    return {
        "hostname": socket.gethostname(),
        "platform": platform.system().lower(),
        "python_version": platform.python_version(),
        "cpu_cores_logical": _safe_cpu_count(logical=True),
        "cpu_cores_physical": _safe_cpu_count(logical=False),
        "ip_list": get_ip_list(),
    }


def get_ip_list() -> list[str]:
    """Get list of IP addresses for this machine.

    Moleculer.js compatible: prefers external IPs over internal.

    Returns:
        List of IPv4 addresses (external first, then internal)
    """
    external_ips: list[str] = []
    internal_ips: list[str] = []

    try:
        interfaces = psutil.net_if_addrs()
        for _iface_name, addrs in interfaces.items():
            for addr in addrs:
                # Only IPv4 addresses
                if addr.family == socket.AF_INET:
                    ip = addr.address
                    # Skip loopback
                    if ip.startswith("127."):
                        continue
                    # Classify as external or internal
                    if ip.startswith(("10.", "172.16.", "192.168.")):
                        internal_ips.append(ip)
                    else:
                        external_ips.append(ip)
    except Exception:
        # Silent fail for IP collection - fallback to localhost below
        pass

    # Prefer external IPs (like Moleculer.js)
    result = external_ips + internal_ips

    # Fallback to localhost if no addresses found
    if not result:
        result = ["127.0.0.1"]

    return result


def get_cpu_usage_sync(interval: float = 0.1) -> int:
    """Get CPU usage percentage (blocking).

    Moleculer.js compatible: returns rounded integer (0-100).

    Args:
        interval: Measurement interval in seconds (default 0.1)

    Returns:
        CPU usage percentage as integer (0-100)
    """
    # psutil.cpu_percent with interval > 0 blocks for that duration
    # and returns accurate measurement
    usage = psutil.cpu_percent(interval=interval)
    return round(float(usage))


async def get_cpu_usage(interval: float = 0.1) -> int:
    """Get CPU usage percentage (async-safe).

    Runs blocking psutil call in executor to avoid blocking event loop.
    Moleculer.js compatible: returns rounded integer.

    Args:
        interval: Measurement interval in seconds (default 0.1)

    Returns:
        CPU usage percentage as integer (0-100)
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, get_cpu_usage_sync, interval)


def get_memory_usage_sync() -> MemoryMetrics:
    """Get memory usage (blocking).

    Returns:
        MemoryMetrics with usage details
    """
    mem = psutil.virtual_memory()
    return MemoryMetrics(
        usage_percent=round(mem.percent, 1),
        total_bytes=mem.total,
        available_bytes=mem.available,
        used_bytes=mem.used,
    )


async def get_memory_usage() -> MemoryMetrics:
    """Get memory usage (async-safe).

    Returns:
        MemoryMetrics with usage details
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, get_memory_usage_sync)


def get_load_avg() -> tuple[float, float, float] | None:
    """Get system load average (Unix only).

    Returns:
        Tuple of (1min, 5min, 15min) load averages, or None on Windows
    """
    try:
        load = os.getloadavg()
        return (round(load[0], 2), round(load[1], 2), round(load[2], 2))
    except (AttributeError, OSError):
        # Windows doesn't support getloadavg
        return None


async def get_system_metrics(cpu_interval: float = 0.1) -> SystemMetrics:
    """Get complete system metrics snapshot (async-safe).

    This is the main function to call for heartbeat metrics collection.
    Runs blocking calls in executor to avoid blocking event loop.

    Args:
        cpu_interval: CPU measurement interval in seconds

    Returns:
        SystemMetrics with all metrics
    """
    loop = asyncio.get_running_loop()

    # Run CPU and memory collection in parallel
    cpu_task = loop.run_in_executor(None, get_cpu_usage_sync, cpu_interval)
    mem_task = loop.run_in_executor(None, get_memory_usage_sync)

    cpu_usage, memory = await asyncio.gather(cpu_task, mem_task)

    # Get cached static metrics
    static = get_static_metrics()

    return SystemMetrics(
        cpu=CpuMetrics(
            usage=cpu_usage,
            cores_logical=static["cpu_cores_logical"],
            cores_physical=static["cpu_cores_physical"],
            load_avg=get_load_avg(),
        ),
        memory=memory,
        hostname=static["hostname"],
        platform=static["platform"],
        python_version=static["python_version"],
        ip_list=static["ip_list"],
    )


class MetricsCollector:
    """Stateful metrics collector with cpuSeq tracking.

    Moleculer.js compatible: tracks cpuSeq that increments only when
    CPU value actually changes.

    Usage:
        collector = MetricsCollector()
        metrics = await collector.collect()
        # metrics.cpu_seq increments only if cpu changed
    """

    __slots__ = ("_cpu_seq", "_last_cpu")

    def __init__(self) -> None:
        """Initialize the metrics collector."""
        self._last_cpu: int | None = None
        self._cpu_seq: int = 0

    @property
    def cpu_seq(self) -> int:
        """Get current cpuSeq value."""
        return self._cpu_seq

    async def collect(self, cpu_interval: float = 0.1) -> dict[str, int | float]:
        """Collect metrics for heartbeat.

        Returns dict compatible with Moleculer.js HEARTBEAT packet:
        - cpu: CPU usage percentage (int 0-100)
        - cpuSeq: Sequence number (increments on change)
        - memory: Memory usage percentage (extension)

        Args:
            cpu_interval: CPU measurement interval

        Returns:
            Dictionary with metrics for heartbeat
        """
        loop = asyncio.get_running_loop()

        # Collect CPU and memory in parallel (perf: ~2x faster than sequential)
        cpu_task = loop.run_in_executor(None, get_cpu_usage_sync, cpu_interval)
        mem_task = loop.run_in_executor(None, get_memory_usage_sync)
        cpu_usage, memory = await asyncio.gather(cpu_task, mem_task)

        # Increment cpuSeq only if CPU changed (Moleculer.js behavior)
        if self._last_cpu is None or cpu_usage != self._last_cpu:
            self._cpu_seq += 1
            self._last_cpu = cpu_usage

        return {
            "cpu": cpu_usage,
            "cpuSeq": self._cpu_seq,
            "memory": memory.usage_percent,
        }

    def reset(self) -> None:
        """Reset the collector state."""
        self._last_cpu = None
        self._cpu_seq = 0
