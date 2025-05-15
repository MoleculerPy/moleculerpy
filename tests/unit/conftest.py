"""Shared fixtures for unit tests.

Provides common mocks and setup for Phase 5.1 (metrics) and Phase 5.2 (latency) tests.
"""

from __future__ import annotations

from collections.abc import Generator
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

if TYPE_CHECKING:
    pass


# =============================================================================
# Broker Fixtures
# =============================================================================


@pytest.fixture
def mock_broker() -> MagicMock:
    """Shared mock broker for latency and metrics tests.

    Provides a fully configured mock broker with:
    - nodeID, logger, node_catalog, transit
    - AsyncMock for transit.publish
    """
    broker = MagicMock()
    broker.nodeID = "test-node"
    broker.logger = MagicMock()
    broker.node_catalog = MagicMock()
    broker.node_catalog.nodes = {}
    broker.node_catalog.get_node = MagicMock(return_value=None)
    broker.transit = MagicMock()
    broker.transit.publish = AsyncMock()
    return broker


@pytest.fixture
def mock_broker_with_node(mock_broker: MagicMock) -> MagicMock:
    """Mock broker with a configured remote node."""
    mock_node = MagicMock()
    mock_node.hostname = "remote-host"
    mock_node.available = True
    mock_broker.node_catalog.get_node.return_value = mock_node
    mock_broker.node_catalog.nodes = {"remote-node": mock_node}
    return mock_broker


# =============================================================================
# psutil Fixtures
# =============================================================================


@pytest.fixture
def mock_psutil_cpu() -> Generator[MagicMock, None, None]:
    """Mock psutil.cpu_percent with default 50%."""
    with patch("psutil.cpu_percent", return_value=50.0) as mock:
        yield mock


@pytest.fixture
def mock_psutil_memory() -> Generator[MagicMock, None, None]:
    """Mock psutil.virtual_memory with default values."""
    mock_mem = MagicMock(
        percent=50.0,
        total=16 * 1024**3,  # 16 GB
        available=8 * 1024**3,  # 8 GB
        used=8 * 1024**3,  # 8 GB
    )
    with patch("psutil.virtual_memory", return_value=mock_mem) as mock:
        yield mock


@pytest.fixture
def mock_psutil_both(
    mock_psutil_cpu: MagicMock,
    mock_psutil_memory: MagicMock,
) -> tuple[MagicMock, MagicMock]:
    """Mock both CPU and memory psutil calls."""
    return mock_psutil_cpu, mock_psutil_memory


# =============================================================================
# Cache Management
# =============================================================================


@pytest.fixture(autouse=True)
def clear_static_metrics_cache() -> Generator[None, None, None]:
    """Auto-clear static metrics cache before and after each test.

    This ensures test isolation for get_static_metrics() which uses @lru_cache.
    """
    from moleculerpy.metrics import get_static_metrics

    get_static_metrics.cache_clear()
    yield
    get_static_metrics.cache_clear()
