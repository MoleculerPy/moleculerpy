"""Consolidated regression tests for audit fixes from recent sprints.

Each test here guards against a specific audit finding that was fixed
without an accompanying regression test. Grouped in one file so the
historical list of "things not to regress" stays discoverable.

Findings covered:
    1-2. ContextTracker honors both camelCase (`$shutdownTimeout`) and
         snake_case (`$shutdown_timeout`) service settings for Node.js
         parity.
    3.   Broker guards against double-registration of ContextTracker
         middleware when user pre-adds it AND tracking is enabled.
    4.   ProtoBuf serializer silently drops fields not in the schema
         (proto3 unknown-field semantics; matches Node.js parity).
    5.   ContextTracker wait loop uses monotonic wall-clock deadline
         so a slow event loop still triggers timeout in bounded time.
    6.   TrackingConfig.__post_init__ rejects zero and negative
         shutdown_timeout values.
    7.   Broker's broker_stopped → stopped alias dispatch invokes the
         hook exactly once when both names are defined (signature
         introspection + same-method detection).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from moleculerpy.broker import Broker
from moleculerpy.middleware.base import Middleware
from moleculerpy.middleware.context_tracker import ContextTrackerMiddleware
from moleculerpy.settings import Settings, TrackingConfig

try:
    import google.protobuf

    from moleculerpy.serializers.protobuf import ProtoBufSerializer

    PROTOBUF_AVAILABLE = True
except ImportError:
    PROTOBUF_AVAILABLE = False
    ProtoBufSerializer = None  # type: ignore[assignment,misc]

# ---------------------------------------------------------------------------
# 1-2. ContextTracker: camelCase / snake_case $shutdownTimeout parity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_context_tracker_camelcase_shutdown_timeout():
    """Node.js `$shutdownTimeout` (camelCase) on a service is honored."""
    mw = ContextTrackerMiddleware(poll_interval=0.01, shutdown_timeout=10.0)
    service = MagicMock()
    service.name = "users"
    service._tracked_contexts = [MagicMock()]
    # ONLY camelCase — verifies it wins over the (missing) default.
    service.settings = {"$shutdownTimeout": 0.05}

    await mw.service_stopping(service)

    # List cleared by timeout means the 0.05s (NOT 10.0s default) was used.
    assert len(service._tracked_contexts) == 0


@pytest.mark.asyncio
async def test_context_tracker_snakecase_shutdown_timeout():
    """Python `$shutdown_timeout` (snake_case) on a service is honored."""
    mw = ContextTrackerMiddleware(poll_interval=0.01, shutdown_timeout=10.0)
    service = MagicMock()
    service.name = "users"
    service._tracked_contexts = [MagicMock()]
    service.settings = {"$shutdown_timeout": 0.05}

    await mw.service_stopping(service)

    assert len(service._tracked_contexts) == 0


# ---------------------------------------------------------------------------
# 3. Broker: ContextTracker double-registration guard
# ---------------------------------------------------------------------------


def test_context_tracker_double_registration_guard():
    """User-supplied ContextTrackerMiddleware + tracking=enabled → one instance."""
    pre_added = ContextTrackerMiddleware(shutdown_timeout=7.0)
    settings = Settings(tracking=TrackingConfig(enabled=True, shutdown_timeout=2.5))

    broker = Broker(
        id="test-double-reg-guard",
        settings=settings,
        middlewares=[pre_added],
    )

    trackers = [mw for mw in broker.middlewares if isinstance(mw, ContextTrackerMiddleware)]
    assert len(trackers) == 1, f"Expected exactly 1 ContextTrackerMiddleware, got {len(trackers)}"
    # The pre-added instance must be preserved (not replaced by auto-reg).
    assert trackers[0] is pre_added
    assert trackers[0]._default_timeout == 7.0


# ---------------------------------------------------------------------------
# 4. ProtoBuf: extra fields silently dropped (proto3 unknown-field parity)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not PROTOBUF_AVAILABLE, reason="protobuf not installed")
def test_heartbeat_proto_no_extra_fields():
    """Extra fields in heartbeat payload are silently dropped on serialize."""
    serializer = ProtoBufSerializer()

    payload = {
        "ver": "4",
        "sender": "node-1",
        "cpu": 0.42,
        # Extras that do NOT exist in PacketHeartbeat proto schema:
        "seq": 99,
        "instanceID": "abc-123",
        "memory": 1024,
    }

    data = serializer.serialize(payload, packet_type="HEARTBEAT")
    assert isinstance(data, bytes)
    assert len(data) > 0

    roundtrip = serializer.deserialize(data, packet_type="HEARTBEAT")

    # Valid fields survive.
    assert roundtrip.get("sender") == "node-1"
    assert roundtrip.get("cpu") == pytest.approx(0.42)
    # Extra fields MUST be silently absent (not raised, not preserved).
    assert "seq" not in roundtrip
    assert "instanceID" not in roundtrip
    assert "memory" not in roundtrip


# ---------------------------------------------------------------------------
# 5. ContextTracker: wall-clock deadline, not cumulative asyncio.sleep
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_walltime_deadline_respects_slow_loop():
    """Timeout triggers in bounded real time even with many short polls.

    Uses a short real timeout (0.1s) and a never-clearing tracked list.
    Regression: previously the loop summed `poll_interval` values, which
    undercounted real elapsed time on a slow event loop and could hang
    well past the configured deadline.
    """
    import time

    mw = ContextTrackerMiddleware(poll_interval=0.01)
    tracked = [MagicMock()]  # never emptied — must time out

    timeout_sec = 0.1
    start = time.monotonic()
    with pytest.raises(Exception):  # GracefulStopTimeoutError
        await mw._wait_for_contexts(tracked, timeout_sec, "svc")
    elapsed = time.monotonic() - start

    # Must fire within 2x the configured deadline.
    assert elapsed < timeout_sec * 2, f"Timeout took {elapsed:.3f}s, expected < {timeout_sec * 2}s"
    # And must not fire early.
    assert elapsed >= timeout_sec * 0.5


# ---------------------------------------------------------------------------
# 6. TrackingConfig: zero / negative shutdown_timeout rejected
# ---------------------------------------------------------------------------


def test_trackingconfig_zero_timeout_rejected():
    """TrackingConfig(shutdown_timeout <= 0) fails in __post_init__."""
    with pytest.raises(ValueError, match="shutdown_timeout"):
        TrackingConfig(shutdown_timeout=0)

    with pytest.raises(ValueError, match="shutdown_timeout"):
        TrackingConfig(shutdown_timeout=-1.0)

    # Sanity check: positive values still accepted.
    cfg = TrackingConfig(shutdown_timeout=0.5)
    assert cfg.shutdown_timeout == 0.5


# ---------------------------------------------------------------------------
# 7. Broker: broker_stopped → stopped alias no double-invoke
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_broker_stop_hook_alias_no_double_invoke():
    """Middleware defining both broker_stopped and stopped → each called once.

    The dispatcher in _call_middleware_hooks detects when two differently
    named hooks resolve to the same bound method and skips the alias; when
    they are distinct overrides, it invokes both — but each exactly once.
    """
    call_log: list[str] = []

    class DualHookMW(Middleware):
        async def broker_stopped(self, broker: Any) -> None:  # primary
            call_log.append("broker_stopped")

        async def stopped(self, broker: Any) -> None:  # type: ignore[override]
            call_log.append("stopped")

    broker = Broker(id="t-dual-stop")
    broker.middlewares.append(DualHookMW())

    await broker._execute_middleware_hooks("broker_stopped", broker, reverse=True)

    # Both defined → each runs exactly once (no double-invoke of either).
    assert call_log.count("broker_stopped") == 1
    assert call_log.count("stopped") == 1
    assert len(call_log) == 2


# ---------------------------------------------------------------------------
# 8. KNOWN-ISSUES #17: service.settings with callables must not hang
# the INFO serializer. _serializable_settings strips non-JSON entries.
# ---------------------------------------------------------------------------


def test_bug17_service_settings_with_callables_are_stripped() -> None:
    """Regression for KNOWN-ISSUES #17.

    ``ApiGatewayService.settings`` carries route hooks (``onBeforeCall``,
    ``authorization``, …) that are callable. Previously they flowed verbatim
    into the INFO packet and crashed/hung JSON/msgpack/cbor serializers. The
    node layer now routes settings through ``_serializable_settings`` which
    keeps only JSON-encodable top-level values and logs a warning for the
    dropped ones.
    """
    import json

    from moleculerpy.node import _serializable_settings

    def _hook() -> None:  # pragma: no cover — probe only
        pass

    raw = {
        "port": 3000,
        "host": "0.0.0.0",
        "routes": [{"path": "/api"}],
        "onBeforeCall": _hook,  # callable — must be dropped
        "authorization": lambda req: None,  # callable — must be dropped
        "path_obj": object(),  # non-encodable — must be dropped
    }
    cleaned = _serializable_settings(raw, service_name="test-gateway")

    # JSON-safe values survive.
    assert cleaned["port"] == 3000
    assert cleaned["host"] == "0.0.0.0"
    assert cleaned["routes"] == [{"path": "/api"}]
    # Non-serializable values removed.
    assert "onBeforeCall" not in cleaned
    assert "authorization" not in cleaned
    assert "path_obj" not in cleaned
    # Exact key set — guards against a regression that would over-zealously
    # drop safe sibling keys while stripping the callables. Without this,
    # a bug that dropped EVERY dict key would still pass the weaker
    # "unsafe keys gone" assertions above.
    assert set(cleaned.keys()) == {"port", "host", "routes"}
    # Result round-trips through json without raising — this is the exact
    # contract the transit/transporter serializers rely on.
    json.dumps(cleaned)


def test_bug17_non_dict_settings_return_empty_dict() -> None:
    """_serializable_settings accepts any shape; non-dict input yields {}."""
    from moleculerpy.node import _serializable_settings

    assert _serializable_settings(None) == {}
    assert _serializable_settings("string") == {}
    assert _serializable_settings(123) == {}


def test_bug17_rejects_nan_and_inf_for_binary_serializer_safety() -> None:
    """Regression for a wire-audit HIGH finding.

    ``json.dumps`` defaults to ``allow_nan=True`` and happily encodes
    ``float('nan')`` as the literal string ``'NaN'`` — which is NOT valid
    JSON per RFC 8259 §6 and which ``msgpack.packb`` rejects outright. A
    naive ``json.dumps`` probe that permits NaN/inf would pass those
    values through and crash the INFO packet mid-handshake on NATS with
    MsgPack. ``_serializable_settings`` explicitly uses ``allow_nan=False``
    so the probe rejects these IEEE 754 edge cases and they are stripped.
    """
    import json

    from moleculerpy.node import _serializable_settings

    raw = {
        "clean_int": 1,
        "nan_value": float("nan"),
        "pos_inf": float("inf"),
        "neg_inf": float("-inf"),
    }
    cleaned = _serializable_settings(raw, service_name="nan-probe")

    # Clean scalar survives.
    assert cleaned["clean_int"] == 1
    # NaN/inf are stripped (would otherwise poison the wire).
    assert "nan_value" not in cleaned
    assert "pos_inf" not in cleaned
    assert "neg_inf" not in cleaned
    # Strict-JSON round-trip: this is the contract downstream consumers
    # (msgpack / cbor / strict JSON parsers) rely on.
    json.dumps(cleaned, allow_nan=False)


def test_bug17_recursive_sanitisation_preserves_siblings() -> None:
    """Regression for a wire-audit HIGH finding.

    Real-world ``ApiGatewayService.settings`` looks like::

        {
            "routes": [
                {"path": "/api", "aliases": {...}, "onBeforeCall": callable},
            ],
        }

    A naive top-level-only filter would probe the whole ``routes`` value,
    find the callable nested inside, and drop the entire ``routes`` list —
    losing the valid ``path`` / ``aliases`` structure too. That defeats
    the whole point of shipping settings over the wire (remote nodes want
    to SEE the route structure even if they cannot execute the hooks).

    The recursive sanitiser keeps non-serialisable leaves out but preserves
    their siblings all the way down.
    """
    from moleculerpy.node import _serializable_settings

    raw = {
        "port": 3000,
        "routes": [
            {
                "path": "/api",
                "method": "GET",
                "onBeforeCall": lambda req: None,  # dropped
                "aliases": {
                    "GET /users": "users.list",
                    "auth": lambda tok: None,  # dropped
                },
            },
            {
                "path": "/health",
                # No callables at all — this entry should survive intact.
            },
        ],
    }
    cleaned = _serializable_settings(raw, service_name="gateway-probe")

    assert cleaned["port"] == 3000
    assert "routes" in cleaned
    assert len(cleaned["routes"]) == 2
    first = cleaned["routes"][0]
    assert first["path"] == "/api"
    assert first["method"] == "GET"
    assert "onBeforeCall" not in first
    assert first["aliases"]["GET /users"] == "users.list"
    assert "auth" not in first["aliases"]
    assert cleaned["routes"][1] == {"path": "/health"}


def test_bug17_send_event_with_ack_uses_data_field() -> None:
    """Regression for wire-audit CRITICAL finding.

    ``Transit.send_event_with_ack`` used to call
    ``self.publish(Packet(Topic.EVENT, ..., context.marshall()))`` directly,
    bypassing the EVENT wire schema fix entirely — the payload landed on
    the wire under the legacy ``params`` key instead of ``data``. The
    reliable-event (needAck) path was silently broken for cross-language
    consumers even after KNOWN-ISSUES #18 was closed on the normal
    ``send_event`` path. The fix delegates to ``send_event`` so all code
    paths share the same wire construction.

    This is a signature probe rather than a full wire test: it asserts
    the method body delegates through ``send_event``, which
    ``test_bug18_send_event_builds_node_js_wire_schema`` already proves
    produces the correct wire shape.
    """
    import asyncio
    import inspect
    from unittest.mock import AsyncMock, MagicMock, patch

    from moleculerpy.transit import Transit

    # Static probe: the method MUST delegate to self.send_event(). A
    # regression would pull the publish back into this method body and
    # break the wire shape for the ACK path. (We intentionally don't
    # assert that "context.marshall()" is absent from the source because
    # the docstring mentions it as historical context.)
    source = inspect.getsource(Transit.send_event_with_ack)
    assert "self.send_event(" in source, (
        "send_event_with_ack does not delegate to send_event — wire schema fix likely regressed"
    )

    # Functional probe: construct a Transit with the event_ack_test
    # fixture pattern and verify send_event_with_ack routes through
    # send_event without crashing on the missing broker setup.
    async def run() -> None:
        mock_transporter = MagicMock()
        mock_transporter.connect = AsyncMock()
        mock_transporter.publish = AsyncMock()
        mock_transporter.has_built_in_balancer = False

        settings = MagicMock()
        settings.transporter = "memory"
        settings.serializer = "JSON"
        settings.disable_balancer = False
        settings.ack_timeout = 0.1

        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(
                node_id="t-ack",
                registry=MagicMock(),
                node_catalog=MagicMock(),
                settings=settings,
                logger=MagicMock(),
                lifecycle=MagicMock(),
            )

            endpoint = MagicMock()
            endpoint.node_id = "peer"

            ctx = MagicMock()
            ctx.id = "evt-1"
            ctx.event = "user.created"
            ctx.params = {"id": 1}
            ctx.meta = {}
            ctx.level = 1
            ctx.tracing = None
            ctx.parent_id = None
            ctx.request_id = "req-1"
            ctx.caller = None
            ctx.need_ack = None
            ctx.ack_id = None

            # We expect a timeout waiting for ACK (no receiver) — swallow
            # it; the publish call is the thing we care about.
            try:
                await transit.send_event_with_ack(endpoint, ctx, timeout=0.1)
            except TimeoutError:
                pass

        # The prepublish path ends at transporter.publish; pull the packet
        # and assert the payload is built with the EVENT wire schema, not
        # context.marshall()'s "params" schema.
        mock_transporter.publish.assert_called_once()
        packet = mock_transporter.publish.call_args[0][0]
        assert packet.payload["data"] == {"id": 1}
        assert "params" not in packet.payload
        assert packet.payload["needAck"] is True

    asyncio.run(run())


# ---------------------------------------------------------------------------
# 9. KNOWN-ISSUES #18: EVENT packets use Node.js-compatible "data" field
# and carry broadcast/groups/caller/needAck; receive-side accepts both the
# new "data" wire schema and the legacy "params" one for rolling upgrades.
# ---------------------------------------------------------------------------


def test_bug18_send_event_builds_node_js_wire_schema() -> None:
    """Regression for KNOWN-ISSUES #18.

    Previously ``transit.send_event`` forwarded ``context.marshall()`` which
    placed the event payload under ``params`` — breaking every Node.js
    consumer that reads ``ctx.data``. The fix builds an EVENT-specific wire
    payload matching ``moleculer/src/transit.js#sendEvent`` exactly.
    """
    import asyncio
    from unittest.mock import AsyncMock, MagicMock, patch

    from moleculerpy.packet import Packet, Topic
    from moleculerpy.transit import Transit

    async def run() -> Packet:
        mock_transporter = MagicMock()
        mock_transporter.connect = AsyncMock()
        mock_transporter.publish = AsyncMock()
        mock_transporter.has_built_in_balancer = False

        # Minimal concrete settings — Transit resolves a real serializer from
        # the string, so MagicMock attributes would fail the registry lookup.
        settings = MagicMock()
        settings.transporter = "memory"
        settings.serializer = "JSON"
        settings.disable_balancer = False

        with patch("moleculerpy.transit.Transporter.get_by_name", return_value=mock_transporter):
            transit = Transit(
                node_id="t-node",
                registry=MagicMock(),
                node_catalog=MagicMock(),
                settings=settings,
                logger=MagicMock(),
                lifecycle=MagicMock(),
            )

            endpoint = MagicMock()
            endpoint.node_id = "peer"

            ctx = MagicMock()
            ctx.id = "ctx-1"
            ctx.event = "user.created"
            ctx.params = {"id": 42}
            ctx.meta = {"correlationId": "abc"}
            ctx.level = 1
            ctx.tracing = None
            ctx.parent_id = None
            ctx.request_id = "req-1"
            ctx.caller = "v1.auth"
            ctx.need_ack = False

            await transit.send_event(endpoint, ctx, groups=["reporting"], broadcast=True)
            return mock_transporter.publish.call_args[0][0]

    packet = asyncio.run(run())

    assert packet.type == Topic.EVENT
    # Node.js parity: the field is "data", not "params".
    assert packet.payload["data"] == {"id": 42}
    assert "params" not in packet.payload
    # Broadcast flag is propagated to the wire so remote receivers can
    # distinguish emit vs broadcast dispatch.
    assert packet.payload["broadcast"] is True
    # Groups and cross-call metadata are present.
    assert packet.payload["groups"] == ["reporting"]
    assert packet.payload["caller"] == "v1.auth"
    assert packet.payload["needAck"] is False
    assert packet.payload["requestID"] == "req-1"
    assert packet.payload["meta"] == {"correlationId": "abc"}


def test_bug18_rebuild_event_context_accepts_data_and_params() -> None:
    """Regression for KNOWN-ISSUES #18 — receive side.

    A freshly upgraded Python peer must accept BOTH the new Node.js-parity
    wire schema (``data``) and the legacy Python schema (``params``) so
    rolling-upgrade clusters continue to deliver events during a deploy.
    ``rebuild_event_context`` is the sole entry point that owns that aliasing.
    """
    from moleculerpy.lifecycle import Lifecycle

    # Context.__init__ reads broker.nodeID when no explicit node_id is passed,
    # so provide that one attribute on a stub broker. No other broker APIs are
    # touched by the rebuild path.
    stub_broker = MagicMock()
    stub_broker.nodeID = "test-node"
    lifecycle = Lifecycle(stub_broker)

    # New wire schema: data carries the payload.
    ctx_new = lifecycle.rebuild_event_context(
        {"id": "e1", "event": "user.created", "data": {"id": 42}}
    )
    assert ctx_new.params == {"id": 42}
    assert ctx_new.event == "user.created"

    # Legacy wire schema from pre-0.14.22 Python peers: params carries it.
    ctx_legacy = lifecycle.rebuild_event_context(
        {"id": "e2", "event": "user.updated", "params": {"id": 7}}
    )
    assert ctx_legacy.params == {"id": 7}

    # Both set (shouldn't happen, but defensive): "data" wins because the
    # Node.js-parity field is the authoritative source going forward.
    ctx_both = lifecycle.rebuild_event_context(
        {"id": "e3", "event": "user.removed", "data": "fresh", "params": "stale"}
    )
    assert ctx_both.params == "fresh"
