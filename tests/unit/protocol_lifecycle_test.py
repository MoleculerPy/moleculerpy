"""System-level tests for the Protocol Correctness & Graceful Lifecycle sprint.

These tests exercise the COMBINED behaviour of Tasks #1-#5:

- Task #1: short broker hook aliases (starting/started/stopping)
- Task #2: PacketHeartbeat proto schema extended with seq/instanceID/memory/cpuSeq
- Task #3: Settings.tracking TrackingConfig
- Task #4: transit.send_disconnect_info + broker.stop() drain ordering
- Task #5: ContextTrackerMiddleware auto-registration via tracking.enabled

The intent is to document the protocol contract end-to-end. Per-task unit
tests live next to their owning agent's changes; this file consolidates the
integration story.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

from moleculerpy.broker import ServiceBroker
from moleculerpy.middleware.base import Middleware
from moleculerpy.middleware.context_tracker import ContextTrackerMiddleware
from moleculerpy.settings import Settings, TrackingConfig
from moleculerpy.transit import Transit

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_transit() -> AsyncMock:
    """Mock Transit so brokers don't talk to real transports."""
    transit = AsyncMock(spec=Transit)
    transit.connect = AsyncMock()
    transit.disconnect = AsyncMock()
    transit.send_disconnect_info = AsyncMock()
    transit.ready = AsyncMock()
    transit.send_node_info = AsyncMock()
    transit.transporter = Mock(name="mock_transport")
    return transit


def _make_broker(
    mock_transit: AsyncMock,
    *,
    middlewares: list[Middleware] | None = None,
    tracking: TrackingConfig | None = None,
) -> ServiceBroker:
    settings = Settings(transporter="mock://localhost", tracking=tracking)
    return ServiceBroker(
        id="test-node",
        settings=settings,
        transit=mock_transit,
        middlewares=middlewares or [],
    )


# ---------------------------------------------------------------------------
# Hook aliases (Task #1)
# ---------------------------------------------------------------------------


class ShortAliasMiddleware(Middleware):
    """Node.js-style middleware using short alias names only."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def starting(self, broker):  # type: ignore[override]
        self.calls.append("starting")

    async def started(self, broker):  # type: ignore[override]
        self.calls.append("started")

    async def stopping(self, broker):  # type: ignore[override]
        self.calls.append("stopping")


class LongNameMiddleware(Middleware):
    """Legacy MoleculerPy middleware using broker_* hook names."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def broker_starting(self, broker):  # type: ignore[override]
        self.calls.append("broker_starting")

    async def broker_started(self, broker):  # type: ignore[override]
        self.calls.append("broker_started")

    async def broker_stopping(self, broker):  # type: ignore[override]
        self.calls.append("broker_stopping")


class BothNamesMiddleware(Middleware):
    """Middleware that defines BOTH broker_* and short alias variants."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def broker_started(self, broker):  # type: ignore[override]
        self.calls.append("broker_started")

    async def started(self, broker):  # type: ignore[override]
        self.calls.append("started")


@pytest.mark.asyncio
async def test_middleware_with_short_alias_invoked(mock_transit: AsyncMock) -> None:
    mw = ShortAliasMiddleware()
    broker = _make_broker(mock_transit, middlewares=[mw])
    await broker.start()
    await broker.stop()
    assert "starting" in mw.calls
    assert "started" in mw.calls
    assert "stopping" in mw.calls


@pytest.mark.asyncio
async def test_middleware_with_long_name_still_works(mock_transit: AsyncMock) -> None:
    mw = LongNameMiddleware()
    broker = _make_broker(mock_transit, middlewares=[mw])
    await broker.start()
    await broker.stop()
    assert mw.calls == [
        "broker_starting",
        "broker_started",
        "broker_stopping",
    ]


@pytest.mark.asyncio
async def test_both_names_independent(mock_transit: AsyncMock) -> None:
    mw = BothNamesMiddleware()
    broker = _make_broker(mock_transit, middlewares=[mw])
    await broker.start()
    await broker.stop()
    # Both names get called — neither swallows the other
    assert mw.calls.count("broker_started") == 1
    assert mw.calls.count("started") == 1


# ---------------------------------------------------------------------------
# Heartbeat ProtoBuf roundtrip (Task #2)
# ---------------------------------------------------------------------------


def _protobuf_serializer():
    pytest.importorskip("google.protobuf")
    from moleculerpy.serializers.protobuf import ProtoBufSerializer

    return ProtoBufSerializer()


def test_heartbeat_protobuf_roundtrip_nodejs_parity() -> None:
    # PacketHeartbeat schema matches Node.js exactly: only ver/sender/cpu.
    # See ADR-heartbeat-schema.md "Revert decision".
    serializer = _protobuf_serializer()
    payload = {"ver": "4", "sender": "node-A", "cpu": 12.5}
    raw = serializer.serialize(payload, "HEARTBEAT")
    decoded = serializer.deserialize(raw, "HEARTBEAT")
    assert decoded.get("sender") == "node-A"
    assert decoded.get("cpu") == 12.5
    assert decoded.get("ver") == "4"


def test_heartbeat_protobuf_drops_extra_fields() -> None:
    # Extra payload keys (legacy seq/instanceID/memory/cpuSeq) must be silently
    # dropped — field numbers 4-7 are reserved in packets.proto.
    serializer = _protobuf_serializer()
    payload = {
        "ver": "4",
        "sender": "node-A",
        "cpu": 7.0,
        "seq": 42,
        "instanceID": "abc-123-instance",
        "memory": 33.3,
        "cpuSeq": 9,
    }
    raw = serializer.serialize(payload, "HEARTBEAT")
    decoded = serializer.deserialize(raw, "HEARTBEAT")
    assert decoded.get("cpu") == 7.0
    for dropped in ("seq", "instanceID", "memory", "cpuSeq"):
        assert dropped not in decoded


def test_heartbeat_json_still_works() -> None:
    from moleculerpy.serializers.json import JsonSerializer

    serializer = JsonSerializer()
    payload = {"ver": "4", "sender": "n1", "cpu": 1.0, "seq": 7, "instanceID": "x"}
    raw = serializer.serialize(payload, "HEARTBEAT")
    decoded = serializer.deserialize(raw, "HEARTBEAT")
    assert decoded == payload


# ---------------------------------------------------------------------------
# TrackingConfig (Task #3)
# ---------------------------------------------------------------------------


def test_tracking_config_import() -> None:
    # Public re-export check — must be importable from moleculerpy.settings
    import moleculerpy.settings as _settings

    cfg = _settings.TrackingConfig()
    assert cfg.enabled is False
    assert cfg.shutdown_timeout == 5.0


def test_settings_default_tracking_disabled() -> None:
    settings = Settings()
    assert isinstance(settings.tracking, TrackingConfig)
    assert settings.tracking.enabled is False
    assert settings.tracking.shutdown_timeout == 5.0


# ---------------------------------------------------------------------------
# Connection drain on stop (Task #4)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_broker_stop_calls_send_disconnect_info(mock_transit: AsyncMock) -> None:
    """send_disconnect_info must be called BEFORE transit.disconnect."""
    call_order: list[str] = []

    async def _record_drain() -> None:
        call_order.append("drain")

    async def _record_disconnect() -> None:
        call_order.append("disconnect")

    mock_transit.send_disconnect_info.side_effect = _record_drain
    mock_transit.disconnect.side_effect = _record_disconnect

    broker = _make_broker(mock_transit)
    await broker.start()
    await broker.stop()

    assert "drain" in call_order
    assert "disconnect" in call_order
    assert call_order.index("drain") < call_order.index("disconnect")


@pytest.mark.asyncio
async def test_send_disconnect_info_empty_services() -> None:
    """Verify the drain INFO actually broadcasts services=[]."""
    from moleculerpy.packet import Packet, Topic

    transit = Transit.__new__(Transit)  # bypass __init__
    transit._was_connected = True  # type: ignore[attr-defined]
    transit.logger = Mock()
    transit.publish = AsyncMock()  # type: ignore[method-assign]

    fake_local_node = Mock()
    fake_local_node.get_info.return_value = {
        "sender": "node-A",
        "services": [{"name": "math"}, {"name": "users"}],
        "ver": "4",
    }
    transit.node_catalog = Mock()
    transit.node_catalog.local_node = fake_local_node

    await Transit.send_disconnect_info(transit)

    transit.publish.assert_awaited_once()
    pkt = transit.publish.await_args.args[0]
    assert isinstance(pkt, Packet)
    assert pkt.type == Topic.INFO
    assert pkt.target is None
    assert pkt.payload["services"] == []
    # Other fields preserved
    assert pkt.payload["sender"] == "node-A"


# ---------------------------------------------------------------------------
# ContextTracker integration (Task #5)
# ---------------------------------------------------------------------------


def test_tracking_disabled_no_middleware(mock_transit: AsyncMock) -> None:
    broker = _make_broker(mock_transit)  # default tracking → disabled
    assert not any(isinstance(mw, ContextTrackerMiddleware) for mw in broker.middlewares)


def test_tracking_enabled_adds_middleware(mock_transit: AsyncMock) -> None:
    broker = _make_broker(
        mock_transit,
        tracking=TrackingConfig(enabled=True, shutdown_timeout=2.5),
    )
    trackers = [mw for mw in broker.middlewares if isinstance(mw, ContextTrackerMiddleware)]
    assert len(trackers) == 1
