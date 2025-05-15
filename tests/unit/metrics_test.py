"""Unit tests for metrics collection module (Phase 5.1).

Tests cover:
- CpuMetrics, MemoryMetrics, SystemMetrics dataclasses
- get_cpu_usage(), get_memory_usage() async functions
- get_static_metrics() caching behavior
- MetricsCollector cpuSeq tracking
- Moleculer.js compatibility (rounded int CPU, cpuSeq increment on change)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from moleculerpy.metrics import (
    CpuMetrics,
    MemoryMetrics,
    MetricsCollector,
    SystemMetrics,
    get_cpu_usage,
    get_cpu_usage_sync,
    get_ip_list,
    get_load_avg,
    get_memory_usage,
    get_memory_usage_sync,
    get_static_metrics,
    get_system_metrics,
)


class TestCpuMetrics:
    """Tests for CpuMetrics dataclass."""

    def test_cpu_metrics_creation(self) -> None:
        """CpuMetrics stores CPU information correctly."""
        metrics = CpuMetrics(
            usage=45,
            cores_logical=8,
            cores_physical=4,
            load_avg=(1.5, 1.2, 0.9),
        )

        assert metrics.usage == 45
        assert metrics.cores_logical == 8
        assert metrics.cores_physical == 4
        assert metrics.load_avg == (1.5, 1.2, 0.9)

    def test_cpu_metrics_default_load_avg(self) -> None:
        """CpuMetrics defaults load_avg to None (Windows compat)."""
        metrics = CpuMetrics(usage=50, cores_logical=4, cores_physical=2)

        assert metrics.load_avg is None


class TestMemoryMetrics:
    """Tests for MemoryMetrics dataclass."""

    def test_memory_metrics_creation(self) -> None:
        """MemoryMetrics stores memory information correctly."""
        metrics = MemoryMetrics(
            usage_percent=65.5,
            total_bytes=16 * 1024**3,
            available_bytes=5 * 1024**3,
            used_bytes=11 * 1024**3,
        )

        assert metrics.usage_percent == 65.5
        assert metrics.total_bytes == 16 * 1024**3
        assert metrics.available_bytes == 5 * 1024**3
        assert metrics.used_bytes == 11 * 1024**3


class TestSystemMetrics:
    """Tests for SystemMetrics dataclass."""

    def test_system_metrics_creation(self) -> None:
        """SystemMetrics combines CPU, memory, and system info."""
        cpu = CpuMetrics(usage=30, cores_logical=8, cores_physical=4)
        memory = MemoryMetrics(
            usage_percent=50.0,
            total_bytes=8 * 1024**3,
            available_bytes=4 * 1024**3,
            used_bytes=4 * 1024**3,
        )

        metrics = SystemMetrics(
            cpu=cpu,
            memory=memory,
            hostname="test-host",
            platform="darwin",
            python_version="3.12.0",
            ip_list=["192.168.1.100"],
        )

        assert metrics.cpu.usage == 30
        assert metrics.memory.usage_percent == 50.0
        assert metrics.hostname == "test-host"
        assert metrics.platform == "darwin"
        assert "192.168.1.100" in metrics.ip_list


class TestCpuUsage:
    """Tests for CPU usage collection functions."""

    @pytest.mark.parametrize(
        "cpu_float,expected_int",
        [
            (45.4, 45),  # Round down
            (45.5, 46),  # Round half up
            (45.6, 46),  # Round up
            (0.0, 0),  # Zero
            (100.0, 100),  # Max
            (99.9, 100),  # Near max
            (0.4, 0),  # Near zero
        ],
    )
    def test_get_cpu_usage_sync_rounding(self, cpu_float: float, expected_int: int) -> None:
        """get_cpu_usage_sync rounds correctly (Moleculer.js compat)."""
        with patch("psutil.cpu_percent", return_value=cpu_float):
            result = get_cpu_usage_sync(interval=0.01)

        assert result == expected_int
        assert isinstance(result, int)

    @pytest.mark.asyncio
    async def test_get_cpu_usage_async(self) -> None:
        """get_cpu_usage is async and returns int."""
        with patch("psutil.cpu_percent", return_value=33.3):
            result = await get_cpu_usage(interval=0.01)

        assert result == 33
        assert isinstance(result, int)


class TestMemoryUsage:
    """Tests for memory usage collection functions."""

    def test_get_memory_usage_sync(self) -> None:
        """get_memory_usage_sync returns MemoryMetrics with all fields."""
        mock_mem = MagicMock(
            percent=75.5,
            total=32 * 1024**3,
            available=8 * 1024**3,
            used=24 * 1024**3,
        )

        with patch("psutil.virtual_memory", return_value=mock_mem):
            result = get_memory_usage_sync()

        assert isinstance(result, MemoryMetrics)
        assert result.usage_percent == 75.5
        assert result.total_bytes == 32 * 1024**3
        assert result.available_bytes == 8 * 1024**3
        assert result.used_bytes == 24 * 1024**3

    @pytest.mark.asyncio
    async def test_get_memory_usage_async(self) -> None:
        """get_memory_usage is async and returns MemoryMetrics."""
        mock_mem = MagicMock(
            percent=60.0,
            total=16 * 1024**3,
            available=6 * 1024**3,
            used=10 * 1024**3,
        )

        with patch("psutil.virtual_memory", return_value=mock_mem):
            result = await get_memory_usage()

        assert isinstance(result, MemoryMetrics)
        assert result.usage_percent == 60.0


class TestStaticMetrics:
    """Tests for static system metrics collection."""

    def test_get_static_metrics_returns_dict(self) -> None:
        """get_static_metrics returns dict with system info."""
        # Cache is auto-cleared by conftest.py fixture
        result = get_static_metrics()

        assert "hostname" in result
        assert "platform" in result
        assert "python_version" in result
        assert "cpu_cores_logical" in result
        assert "cpu_cores_physical" in result
        assert "ip_list" in result

    def test_get_static_metrics_is_cached(self) -> None:
        """get_static_metrics is cached (called once)."""
        # Cache is auto-cleared by conftest.py fixture
        result1 = get_static_metrics()
        result2 = get_static_metrics()

        # Same object due to caching
        assert result1 is result2


class TestIpList:
    """Tests for IP address collection."""

    def test_get_ip_list_returns_list(self) -> None:
        """get_ip_list returns list of IP addresses."""
        result = get_ip_list()

        assert isinstance(result, list)
        assert len(result) > 0

    def test_get_ip_list_fallback_to_localhost(self) -> None:
        """get_ip_list returns localhost if no interfaces found."""
        with patch("psutil.net_if_addrs", return_value={}):
            result = get_ip_list()

        assert result == ["127.0.0.1"]

    def test_get_ip_list_handles_error(self) -> None:
        """get_ip_list handles psutil errors gracefully."""
        with patch("psutil.net_if_addrs", side_effect=Exception("mock error")):
            result = get_ip_list()

        assert result == ["127.0.0.1"]


class TestLoadAverage:
    """Tests for load average collection."""

    def test_get_load_avg_returns_tuple(self) -> None:
        """get_load_avg returns tuple on Unix systems."""
        with patch("os.getloadavg", return_value=(1.5, 1.2, 0.9)):
            result = get_load_avg()

        assert result == (1.5, 1.2, 0.9)

    def test_get_load_avg_returns_none_on_windows(self) -> None:
        """get_load_avg returns None on Windows (no getloadavg)."""
        with patch("os.getloadavg", side_effect=AttributeError):
            result = get_load_avg()

        assert result is None


class TestMetricsCollector:
    """Tests for MetricsCollector class (stateful cpuSeq tracking)."""

    def test_collector_initial_state(self) -> None:
        """MetricsCollector starts with cpu_seq=0."""
        collector = MetricsCollector()

        assert collector.cpu_seq == 0

    @pytest.mark.asyncio
    async def test_collector_increments_cpu_seq_on_first_call(self) -> None:
        """MetricsCollector increments cpuSeq on first collect()."""
        with patch("psutil.cpu_percent", return_value=50.0):
            with patch("psutil.virtual_memory") as mock_mem:
                mock_mem.return_value = MagicMock(
                    percent=60.0, total=16e9, available=6e9, used=10e9
                )
                collector = MetricsCollector()

                result = await collector.collect(cpu_interval=0.01)

        assert result["cpuSeq"] == 1
        assert collector.cpu_seq == 1

    @pytest.mark.asyncio
    async def test_collector_increments_cpu_seq_on_change(self) -> None:
        """MetricsCollector increments cpuSeq only when CPU changes."""
        cpu_values = [50.0, 50.0, 60.0, 60.0, 70.0]
        cpu_iter = iter(cpu_values)

        with patch("psutil.cpu_percent", side_effect=lambda interval: next(cpu_iter)):
            with patch("psutil.virtual_memory") as mock_mem:
                mock_mem.return_value = MagicMock(percent=50.0, total=16e9, available=8e9, used=8e9)
                collector = MetricsCollector()

                results = []
                for _ in range(5):
                    result = await collector.collect(cpu_interval=0.01)
                    results.append(result)

        # cpuSeq increments: 1 (50), 1 (50 same), 2 (60), 2 (60 same), 3 (70)
        assert results[0]["cpuSeq"] == 1  # First call (50)
        assert results[1]["cpuSeq"] == 1  # Same CPU (50)
        assert results[2]["cpuSeq"] == 2  # Changed (60)
        assert results[3]["cpuSeq"] == 2  # Same CPU (60)
        assert results[4]["cpuSeq"] == 3  # Changed (70)

    @pytest.mark.asyncio
    async def test_collector_returns_moleculer_compatible_format(self) -> None:
        """MetricsCollector returns Moleculer.js compatible dict."""
        with patch("psutil.cpu_percent", return_value=45.0):
            with patch("psutil.virtual_memory") as mock_mem:
                mock_mem.return_value = MagicMock(percent=55.5, total=16e9, available=7e9, used=9e9)
                collector = MetricsCollector()

                result = await collector.collect()

        assert "cpu" in result
        assert "cpuSeq" in result
        assert "memory" in result

        assert isinstance(result["cpu"], int)
        assert isinstance(result["cpuSeq"], int)
        assert isinstance(result["memory"], float)

    def test_collector_reset(self) -> None:
        """MetricsCollector.reset() clears state."""
        collector = MetricsCollector()
        collector._cpu_seq = 10
        collector._last_cpu = 50

        collector.reset()

        assert collector.cpu_seq == 0
        assert collector._last_cpu is None


class TestSystemMetricsCollection:
    """Tests for complete system metrics collection."""

    @pytest.mark.asyncio
    async def test_get_system_metrics(self) -> None:
        """get_system_metrics returns complete SystemMetrics."""
        with patch("psutil.cpu_percent", return_value=40.0):
            with patch("psutil.virtual_memory") as mock_mem:
                mock_mem.return_value = MagicMock(
                    percent=65.0, total=32e9, available=11e9, used=21e9
                )
                # Cache is auto-cleared by conftest.py fixture
                result = await get_system_metrics(cpu_interval=0.01)

        assert isinstance(result, SystemMetrics)
        assert result.cpu.usage == 40
        assert result.memory.usage_percent == 65.0
        assert result.hostname is not None
        assert result.platform is not None


class TestMoleculerJsCompatibility:
    """Tests for Moleculer.js protocol compatibility."""

    @pytest.mark.asyncio
    async def test_cpu_is_rounded_integer(self) -> None:
        """CPU value is rounded to int like Moleculer.js."""
        with patch("psutil.cpu_percent", return_value=45.6):
            with patch("psutil.virtual_memory") as mock_mem:
                mock_mem.return_value = MagicMock(percent=50.0, total=16e9, available=8e9, used=8e9)
                collector = MetricsCollector()
                result = await collector.collect()

        # Moleculer.js rounds: Math.round(45.6) = 46
        assert result["cpu"] == 46
        assert isinstance(result["cpu"], int)

    @pytest.mark.asyncio
    async def test_cpu_seq_starts_at_one(self) -> None:
        """cpuSeq starts at 1 on first heartbeat (not 0)."""
        with patch("psutil.cpu_percent", return_value=50.0):
            with patch("psutil.virtual_memory") as mock_mem:
                mock_mem.return_value = MagicMock(percent=50.0, total=16e9, available=8e9, used=8e9)
                collector = MetricsCollector()
                result = await collector.collect()

        assert result["cpuSeq"] == 1

    @pytest.mark.asyncio
    async def test_cpu_seq_only_increments_on_change(self) -> None:
        """cpuSeq ONLY increments when CPU actually changes."""
        with patch("psutil.cpu_percent", return_value=50.0):
            with patch("psutil.virtual_memory") as mock_mem:
                mock_mem.return_value = MagicMock(percent=50.0, total=16e9, available=8e9, used=8e9)
                collector = MetricsCollector()

                # Call 3 times with same CPU
                r1 = await collector.collect()
                r2 = await collector.collect()
                r3 = await collector.collect()

        # cpuSeq should stay at 1 (only incremented once on first call)
        assert r1["cpuSeq"] == 1
        assert r2["cpuSeq"] == 1
        assert r3["cpuSeq"] == 1
