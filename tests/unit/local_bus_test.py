"""Unit tests for LocalBus internal event system.

Tests cover:
- Basic event subscription and emission
- Wildcard pattern matching
- Async handler support
- Once handlers (auto-remove)
- Multiple handlers per event
- Event unsubscription
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from moleculerpy.local_bus import (
    EventRegistration,
    LocalBus,
    LocalBusOptions,
    get_local_bus,
    reset_local_bus,
)


class TestLocalBusBasic:
    """Basic LocalBus functionality tests."""

    def test_init_default_options(self) -> None:
        """LocalBus initializes with default options."""
        bus = LocalBus()
        assert bus._options.wildcard is True
        assert bus._options.max_listeners == 100

    def test_init_custom_options(self) -> None:
        """LocalBus accepts custom options."""
        options = LocalBusOptions(wildcard=False, max_listeners=50)
        bus = LocalBus(options=options)
        assert bus._options.wildcard is False
        assert bus._options.max_listeners == 50

    def test_on_returns_self(self) -> None:
        """on() returns self for method chaining."""
        bus = LocalBus()
        result = bus.on("test", lambda x: None)
        assert result is bus

    def test_off_returns_self(self) -> None:
        """off() returns self for method chaining."""
        bus = LocalBus()
        result = bus.off("test")
        assert result is bus

    def test_off_all_returns_self(self) -> None:
        """off_all() returns self for method chaining."""
        bus = LocalBus()
        result = bus.off_all()
        assert result is bus


class TestLocalBusEmit:
    """Tests for LocalBus.emit()."""

    @pytest.mark.asyncio
    async def test_emit_sync_handler(self) -> None:
        """Emit calls synchronous handlers."""
        bus = LocalBus()
        received: list[Any] = []

        def handler(payload: Any) -> None:
            received.append(payload)

        bus.on("test.event", handler)
        result = await bus.emit("test.event", {"data": "value"})

        assert result is True
        assert len(received) == 1
        assert received[0] == {"data": "value"}

    @pytest.mark.asyncio
    async def test_emit_async_handler(self) -> None:
        """Emit awaits asynchronous handlers."""
        bus = LocalBus()
        received: list[Any] = []

        async def handler(payload: Any) -> None:
            await asyncio.sleep(0.01)
            received.append(payload)

        bus.on("test.event", handler)
        await bus.emit("test.event", {"async": True})

        assert len(received) == 1
        assert received[0] == {"async": True}

    @pytest.mark.asyncio
    async def test_emit_multiple_handlers(self) -> None:
        """Emit calls all handlers for an event."""
        bus = LocalBus()
        received: list[str] = []

        bus.on("multi", lambda _: received.append("first"))
        bus.on("multi", lambda _: received.append("second"))
        bus.on("multi", lambda _: received.append("third"))

        await bus.emit("multi", None)

        assert received == ["first", "second", "third"]

    @pytest.mark.asyncio
    async def test_emit_no_handlers(self) -> None:
        """Emit returns False if no handlers found."""
        bus = LocalBus()
        result = await bus.emit("unknown.event", None)
        assert result is False

    @pytest.mark.asyncio
    async def test_emit_handler_error_ignored(self) -> None:
        """Emit silently ignores handler errors."""
        bus = LocalBus()
        received: list[str] = []

        def error_handler(payload: Any) -> None:
            raise ValueError("Handler error")

        bus.on("error.event", error_handler)
        bus.on("error.event", lambda _: received.append("after"))

        # Should not raise, should continue to next handler
        result = await bus.emit("error.event", None)

        assert result is True
        assert received == ["after"]


class TestLocalBusWildcard:
    """Tests for wildcard pattern matching."""

    @pytest.mark.asyncio
    async def test_wildcard_single_star(self) -> None:
        """Single * matches single segment."""
        bus = LocalBus()
        received: list[str] = []

        bus.on("$node.*", lambda p: received.append(p["event"]))

        await bus.emit("$node.connected", {"event": "$node.connected"})
        await bus.emit("$node.disconnected", {"event": "$node.disconnected"})
        await bus.emit("$node.updated", {"event": "$node.updated"})

        assert "$node.connected" in received
        assert "$node.disconnected" in received
        assert "$node.updated" in received

    @pytest.mark.asyncio
    async def test_wildcard_double_star(self) -> None:
        """Double ** matches any segments."""
        bus = LocalBus()
        received: list[str] = []

        bus.on("$**", lambda p: received.append(p.get("name", "unknown")))

        await bus.emit("$broker.started", {"name": "started"})
        await bus.emit("$node.connected", {"name": "connected"})
        await bus.emit("$transporter.error", {"name": "error"})

        assert "started" in received
        assert "connected" in received
        assert "error" in received

    @pytest.mark.asyncio
    async def test_wildcard_disabled(self) -> None:
        """Wildcard matching can be disabled."""
        options = LocalBusOptions(wildcard=False)
        bus = LocalBus(options=options)
        received: list[str] = []

        bus.on("$node.*", lambda _: received.append("pattern"))
        bus.on("$node.connected", lambda _: received.append("exact"))

        await bus.emit("$node.connected", None)

        # Only exact match should fire, not pattern
        assert received == ["exact"]


class TestLocalBusOnce:
    """Tests for once() handlers."""

    @pytest.mark.asyncio
    async def test_once_fires_once(self) -> None:
        """Once handlers fire only once."""
        bus = LocalBus()
        count = [0]

        bus.once("single", lambda _: count.__setitem__(0, count[0] + 1))

        await bus.emit("single", None)
        await bus.emit("single", None)
        await bus.emit("single", None)

        assert count[0] == 1

    @pytest.mark.asyncio
    async def test_once_removed_after_call(self) -> None:
        """Once handler is removed from handlers list after call."""
        bus = LocalBus()

        bus.once("remove.me", lambda _: None)
        assert bus.listener_count("remove.me") == 1

        await bus.emit("remove.me", None)
        assert bus.listener_count("remove.me") == 0


class TestLocalBusOff:
    """Tests for event unsubscription."""

    @pytest.mark.asyncio
    async def test_off_specific_handler(self) -> None:
        """off() removes specific handler."""
        bus = LocalBus()
        received: list[str] = []

        handler1 = lambda _: received.append("first")  # noqa: E731
        handler2 = lambda _: received.append("second")  # noqa: E731

        bus.on("event", handler1)
        bus.on("event", handler2)
        bus.off("event", handler1)

        await bus.emit("event", None)

        assert received == ["second"]

    @pytest.mark.asyncio
    async def test_off_all_handlers_for_event(self) -> None:
        """off() with no handler removes all handlers for event."""
        bus = LocalBus()

        bus.on("event", lambda _: None)
        bus.on("event", lambda _: None)
        assert bus.listener_count("event") == 2

        bus.off("event")
        assert bus.listener_count("event") == 0

    def test_off_all_removes_everything(self) -> None:
        """off_all() removes all handlers."""
        bus = LocalBus()

        bus.on("event1", lambda _: None)
        bus.on("event2", lambda _: None)
        bus.on_any(lambda _: None)

        assert bus.listener_count() > 0

        bus.off_all()

        assert bus.listener_count() == 0
        assert len(bus._all_handlers) == 0


class TestLocalBusOnAny:
    """Tests for on_any() catch-all handlers."""

    @pytest.mark.asyncio
    async def test_on_any_receives_all_events(self) -> None:
        """on_any() handler receives all events."""
        bus = LocalBus()
        received: list[dict] = []

        bus.on_any(received.append)

        await bus.emit("event1", {"val": 1})
        await bus.emit("event2", {"val": 2})

        assert len(received) == 2
        assert received[0] == {"event": "event1", "payload": {"val": 1}}
        assert received[1] == {"event": "event2", "payload": {"val": 2}}


class TestLocalBusListeners:
    """Tests for listener inspection methods."""

    def test_listeners_returns_handlers(self) -> None:
        """listeners() returns list of handlers for event."""
        bus = LocalBus()

        handler1 = lambda _: None  # noqa: E731
        handler2 = lambda _: None  # noqa: E731

        bus.on("event", handler1)
        bus.on("event", handler2)

        handlers = bus.listeners("event")
        assert len(handlers) == 2
        assert handler1 in handlers
        assert handler2 in handlers

    def test_listener_count_for_event(self) -> None:
        """listener_count() returns count for specific event."""
        bus = LocalBus()

        bus.on("event", lambda _: None)
        bus.on("event", lambda _: None)
        bus.on("other", lambda _: None)

        assert bus.listener_count("event") == 2
        assert bus.listener_count("other") == 1
        assert bus.listener_count("nonexistent") == 0

    def test_listener_count_total(self) -> None:
        """listener_count() with no arg returns total count."""
        bus = LocalBus()

        bus.on("event1", lambda _: None)
        bus.on("event1", lambda _: None)
        bus.on("event2", lambda _: None)

        assert bus.listener_count() == 3


class TestLocalBusMaxListeners:
    """Tests for max listeners limit."""

    def test_max_listeners_raises(self) -> None:
        """Exceeding max listeners raises RuntimeError."""
        options = LocalBusOptions(max_listeners=2)
        bus = LocalBus(options=options)

        bus.on("event", lambda _: None)
        bus.on("event", lambda _: None)

        with pytest.raises(RuntimeError, match="Max listeners"):
            bus.on("event", lambda _: None)

    def test_max_listeners_zero_unlimited(self) -> None:
        """max_listeners=0 means unlimited."""
        options = LocalBusOptions(max_listeners=0)
        bus = LocalBus(options=options)

        # Should not raise for many handlers
        for _i in range(200):
            bus.on("event", lambda _: None)

        assert bus.listener_count("event") == 200


class TestLocalBusSingleton:
    """Tests for singleton factory functions."""

    def test_get_local_bus_returns_same_instance(self) -> None:
        """get_local_bus() returns singleton."""
        reset_local_bus()

        bus1 = get_local_bus()
        bus2 = get_local_bus()

        assert bus1 is bus2

    def test_reset_local_bus_creates_new(self) -> None:
        """reset_local_bus() allows new instance."""
        bus1 = get_local_bus()
        reset_local_bus()
        bus2 = get_local_bus()

        assert bus1 is not bus2


class TestEventRegistration:
    """Tests for EventRegistration dataclass."""

    def test_registration_default_once(self) -> None:
        """EventRegistration defaults once to False."""
        reg = EventRegistration(handler=lambda _: None)
        assert reg.once is False

    def test_registration_once_true(self) -> None:
        """EventRegistration accepts once=True."""
        reg = EventRegistration(handler=lambda _: None, once=True)
        assert reg.once is True
