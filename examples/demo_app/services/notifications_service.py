"""Notification service that reacts to channel events."""

from __future__ import annotations

from typing import Any

from moleculerpy import Context, Service, action


class NotificationsService(Service):
    """Generate user-facing notifications from asynchronous channel events."""

    name = "notifications"

    def __init__(self) -> None:
        super().__init__(self.name)
        self._messages: list[dict[str, Any]] = []

    @property
    def schema(self) -> dict[str, Any]:
        """Expose notification channel handlers."""
        return {
            "channels": {
                "orders.created": {
                    "group": "demo-notifications",
                    "context": True,
                    "handler": self._on_order_created,
                },
                "payments.completed": {
                    "group": "demo-notifications",
                    "context": True,
                    "handler": self._on_payment_completed,
                },
            }
        }

    async def _on_order_created(self, ctx: Context, raw: Any) -> None:
        """Queue an order confirmation message."""
        self._messages.append(
            {
                "kind": "order_created",
                "request_id": ctx.request_id,
                "message": (
                    f"Queued confirmation for {ctx.params.get('customer_name')} "
                    f"on order {ctx.params.get('order_id')}"
                ),
            }
        )

    async def _on_payment_completed(self, ctx: Context, raw: Any) -> None:
        """Queue a payment receipt message."""
        self._messages.append(
            {
                "kind": "payment_completed",
                "request_id": ctx.request_id,
                "message": (
                    f"Sent receipt {ctx.params.get('receipt_id')} "
                    f"for order {ctx.params.get('order_id')}"
                ),
            }
        )

    @action()
    async def recent(self, ctx: Context) -> list[dict[str, Any]]:
        """Return the last five notification messages."""
        return self._messages[-5:]
