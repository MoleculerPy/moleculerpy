"""E2E tests for event broadcasting and handling.

Tests verify that events are correctly broadcast to all nodes
and that event groups work as expected.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from moleculerpy.broker import ServiceBroker
from moleculerpy.decorators import action, event
from moleculerpy.service import Service
from moleculerpy.settings import Settings

if TYPE_CHECKING:
    pass


# Event tracking storage
_event_log: list[dict] = []


def reset_event_log() -> None:
    """Reset event log for test isolation."""
    global _event_log
    _event_log = []


def get_event_log() -> list[dict]:
    """Get current event log."""
    return _event_log.copy()


class EventListenerService(Service):
    """Service that listens to events and logs them."""

    name = "event-listener"

    def __init__(self, node_id: str):
        super().__init__(self.name)
        self._node_id = node_id

    @event("user.created")
    async def on_user_created(self, ctx) -> None:
        """Handle user.created event."""
        _event_log.append(
            {
                "event": "user.created",
                "node_id": self._node_id,
                "params": ctx.params,
            }
        )

    @event("order.*")
    async def on_order_events(self, ctx) -> None:
        """Handle all order.* events (wildcard)."""
        _event_log.append(
            {
                "event": ctx.event,
                "node_id": self._node_id,
                "params": ctx.params,
            }
        )

    @action()
    async def get_events(self, ctx) -> list[dict]:
        """Get events received by this node."""
        return [e for e in _event_log if e["node_id"] == self._node_id]


class TaskEventService(Service):
    """Service for task event testing."""

    name = "task-event"

    def __init__(self, node_id: str):
        super().__init__(self.name)
        self._node_id = node_id

    @event("task.assigned")
    async def on_task_assigned(self, ctx) -> None:
        """Handle task.assigned event."""
        _event_log.append(
            {
                "event": "task.assigned",
                "node_id": self._node_id,
                "params": ctx.params,
            }
        )


@pytest.fixture(autouse=True)
def clean_event_log():
    """Reset event log before each test."""
    reset_event_log()
    yield
    reset_event_log()


@pytest.mark.asyncio
@pytest.mark.e2e
class TestEventBroadcast:
    """Tests for event broadcasting."""

    async def test_event_reaches_all_nodes(self) -> None:
        """Broadcast event reaches all nodes in cluster."""
        brokers: list[ServiceBroker] = []

        try:
            # Create 3 brokers with event listener
            for i in range(3):
                node_id = f"event-node-{i}"
                settings = Settings(transporter="memory://", prefer_local=False)
                broker = ServiceBroker(id=node_id, settings=settings)
                await broker.register(EventListenerService(node_id))
                brokers.append(broker)

            # Start all brokers
            for broker in brokers:
                await broker.start()

            # Wait for discovery
            await asyncio.sleep(0.3)

            # Broadcast event from first broker
            await brokers[0].broadcast("user.created", {"userId": "123", "name": "Test"})

            # Wait for event propagation
            await asyncio.sleep(0.2)

            # Check that all nodes received the event
            events = get_event_log()
            user_created_events = [e for e in events if e["event"] == "user.created"]

            assert len(user_created_events) == 3, (
                f"Expected 3 events, got {len(user_created_events)}"
            )

            # Verify all nodes received it
            nodes_received = {e["node_id"] for e in user_created_events}
            expected_nodes = {f"event-node-{i}" for i in range(3)}
            assert nodes_received == expected_nodes

        finally:
            for broker in brokers:
                await broker.stop()

    async def test_event_with_params(self) -> None:
        """Event params are passed correctly."""
        settings = Settings(transporter="memory://", prefer_local=False)
        broker = ServiceBroker(id="params-test-node", settings=settings)
        await broker.register(EventListenerService("params-test-node"))
        await broker.start()

        try:
            await asyncio.sleep(0.1)

            # Emit event with params
            await broker.emit(
                "user.created",
                {
                    "userId": "456",
                    "name": "John Doe",
                    "email": "john@example.com",
                },
            )

            await asyncio.sleep(0.1)

            events = get_event_log()
            assert len(events) == 1
            assert events[0]["params"]["userId"] == "456"
            assert events[0]["params"]["name"] == "John Doe"

        finally:
            await broker.stop()


@pytest.mark.asyncio
@pytest.mark.e2e
class TestEventWildcards:
    """Tests for wildcard event subscriptions."""

    async def test_wildcard_event_matching(self) -> None:
        """Wildcard subscriptions match multiple events."""
        settings = Settings(transporter="memory://", prefer_local=False)
        broker = ServiceBroker(id="wildcard-node", settings=settings)
        await broker.register(EventListenerService("wildcard-node"))
        await broker.start()

        try:
            await asyncio.sleep(0.1)

            # Emit different order events
            await broker.emit("order.created", {"orderId": "1"})
            await broker.emit("order.shipped", {"orderId": "1"})
            await broker.emit("order.delivered", {"orderId": "1"})

            await asyncio.sleep(0.2)

            events = get_event_log()
            order_events = [e for e in events if e["event"].startswith("order.")]

            assert len(order_events) == 3
            event_names = {e["event"] for e in order_events}
            assert event_names == {"order.created", "order.shipped", "order.delivered"}

        finally:
            await broker.stop()


@pytest.mark.asyncio
@pytest.mark.e2e
class TestEventBroadcastToAll:
    """Tests for broadcast event reaching all nodes."""

    async def test_event_reaches_all_listeners(self) -> None:
        """Event reaches all nodes with listener."""
        brokers: list[ServiceBroker] = []

        try:
            # Create 3 brokers with task event service
            for i in range(3):
                node_id = f"task-node-{i}"
                settings = Settings(transporter="memory://", prefer_local=False)
                broker = ServiceBroker(id=node_id, settings=settings)
                await broker.register(TaskEventService(node_id))
                brokers.append(broker)

            # Start all brokers
            for broker in brokers:
                await broker.start()

            # Wait for discovery
            await asyncio.sleep(0.3)

            # Broadcast event
            await brokers[0].broadcast("task.assigned", {"taskId": "task-1"})

            await asyncio.sleep(0.2)

            # Check events
            events = get_event_log()
            task_events = [e for e in events if e["event"] == "task.assigned"]

            # All 3 nodes should receive the broadcast event
            assert len(task_events) == 3, (
                f"Expected 3 events (one per node), got {len(task_events)}"
            )

            # Verify all nodes received it
            nodes_received = {e["node_id"] for e in task_events}
            expected_nodes = {f"task-node-{i}" for i in range(3)}
            assert nodes_received == expected_nodes

        finally:
            for broker in brokers:
                await broker.stop()


@pytest.mark.asyncio
@pytest.mark.e2e
class TestEventFromAnyBroker:
    """Tests for emitting events from different brokers."""

    async def test_any_broker_can_emit(self) -> None:
        """Any broker in cluster can emit events."""
        brokers: list[ServiceBroker] = []

        try:
            for i in range(2):
                node_id = f"emitter-node-{i}"
                settings = Settings(transporter="memory://", prefer_local=False)
                broker = ServiceBroker(id=node_id, settings=settings)
                await broker.register(EventListenerService(node_id))
                brokers.append(broker)

            for broker in brokers:
                await broker.start()

            await asyncio.sleep(0.3)

            # Broadcast from second broker
            await brokers[1].broadcast("user.created", {"from": "broker-1"})

            await asyncio.sleep(0.1)

            events = get_event_log()
            # Both nodes should receive the event
            assert len(events) == 2

        finally:
            for broker in brokers:
                await broker.stop()
