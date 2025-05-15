"""Analytics service that consumes channel events."""

from __future__ import annotations

from typing import Any

from moleculerpy import Context, Service, action


class AnalyticsService(Service):
    """Track checkout metrics based on channel-driven events."""

    name = "analytics"

    def __init__(self) -> None:
        super().__init__(self.name)
        self._stats: dict[str, Any] = {
            "orders_created": 0,
            "payments_completed": 0,
            "gross_revenue": 0.0,
            "last_customer": None,
            "recent_requests": [],
        }

    @property
    def schema(self) -> dict[str, Any]:
        """Expose channel handlers via MoleculerPy Channels schema."""
        return {
            "channels": {
                "orders.created": {
                    "group": "demo-analytics",
                    "context": True,
                    "handler": self._on_order_created,
                },
                "payments.completed": {
                    "group": "demo-analytics",
                    "context": True,
                    "handler": self._on_payment_completed,
                },
            }
        }

    async def _on_order_created(self, ctx: Context, raw: Any) -> None:
        """Track newly created orders."""
        self._stats["orders_created"] += 1
        self._stats["last_customer"] = ctx.params.get("customer_name")
        self._remember_request(ctx)

    async def _on_payment_completed(self, ctx: Context, raw: Any) -> None:
        """Track successful payments and aggregate revenue."""
        self._stats["payments_completed"] += 1
        self._stats["gross_revenue"] = round(
            float(self._stats["gross_revenue"]) + float(ctx.params.get("total", 0.0)),
            2,
        )
        self._stats["last_customer"] = ctx.params.get("customer_name")
        self._remember_request(ctx)

    def _remember_request(self, ctx: Context) -> None:
        """Keep a compact history of propagated request identifiers."""
        recent_requests = list(self._stats["recent_requests"])
        recent_requests.append(
            {
                "request_id": ctx.request_id,
                "channel_name": getattr(ctx, "channel_name", None),
                "parent_channel_name": getattr(ctx, "parent_channel_name", None),
            }
        )
        self._stats["recent_requests"] = recent_requests[-5:]

    @action()
    async def stats(self, ctx: Context) -> dict[str, Any]:
        """Return current analytics counters."""
        return dict(self._stats)
