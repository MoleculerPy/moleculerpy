"""Payment service for the demo application."""

from __future__ import annotations

from typing import Any

from moleculerpy import Context, Service, action


class PaymentsService(Service):
    """Process demo payments and coordinate order state transitions."""

    name = "payments"

    def __init__(self) -> None:
        super().__init__(self.name)
        self._sequence = 0
        self._receipts: dict[str, dict[str, Any]] = {}

    @action(params=["order_id", "total", "customer_name", "payment_method"])
    async def charge(self, ctx: Context) -> dict[str, Any]:
        """Create a receipt and mark the related order as paid."""
        self._sequence += 1
        receipt_id = f"rcpt-{self._sequence:04d}"
        receipt = {
            "receipt_id": receipt_id,
            "order_id": str(ctx.params.get("order_id", "")),
            "customer_name": str(ctx.params.get("customer_name", "Guest")),
            "payment_method": str(ctx.params.get("payment_method", "card")),
            "amount": round(float(ctx.params.get("total", 0.0)), 2),
            "status": "captured",
        }
        self._receipts[receipt_id] = receipt

        await ctx.call(
            "orders.mark_paid",
            {
                "order_id": receipt["order_id"],
                "receipt_id": receipt_id,
            },
        )
        return receipt

    @action()
    async def list(self, ctx: Context) -> list[dict[str, Any]]:
        """Return all captured receipts."""
        return list(self._receipts.values())
