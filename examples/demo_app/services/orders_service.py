"""Order storage and order event publishing for the demo application."""

from __future__ import annotations

from typing import Any

from moleculerpy import Context, Service, action


class OrdersService(Service):
    """Store demo orders and publish channel events on state changes."""

    name = "orders"

    def __init__(self) -> None:
        super().__init__(self.name)
        self._sequence = 0
        self._orders: dict[str, dict[str, Any]] = {}

    @action(params=["product_id", "product_name", "quantity", "unit_price", "customer_name"])
    async def place(self, ctx: Context) -> dict[str, Any]:
        """Create an order and emit an ``orders.created`` channel message."""
        self._sequence += 1
        quantity = int(ctx.params.get("quantity", 1))
        unit_price = float(ctx.params.get("unit_price", 0.0))
        total = round(quantity * unit_price, 2)
        order_id = f"ord-{self._sequence:04d}"

        order = {
            "order_id": order_id,
            "product_id": str(ctx.params.get("product_id", "")),
            "product_name": str(ctx.params.get("product_name", "")),
            "customer_name": str(ctx.params.get("customer_name", "Guest")),
            "quantity": quantity,
            "unit_price": unit_price,
            "total": total,
            "status": "created",
        }
        self._orders[order_id] = order

        await self.broker.send_to_channel("orders.created", order, {"ctx": ctx})
        return order

    @action(params=["order_id", "receipt_id"])
    async def mark_paid(self, ctx: Context) -> dict[str, Any]:
        """Mark an order as paid and emit a ``payments.completed`` channel message."""
        order_id = str(ctx.params.get("order_id", ""))
        receipt_id = str(ctx.params.get("receipt_id", ""))
        order = self._orders.get(order_id)
        if order is None:
            raise ValueError(f"Unknown order_id: {order_id}")

        order["status"] = "paid"
        order["receipt_id"] = receipt_id

        await self.broker.send_to_channel("payments.completed", dict(order), {"ctx": ctx})
        return order

    @action()
    async def list(self, ctx: Context) -> list[dict[str, Any]]:
        """Return all orders sorted by creation order."""
        return list(self._orders.values())

    @action(params=["order_id"])
    async def get(self, ctx: Context) -> dict[str, Any]:
        """Return a single order by identifier."""
        order_id = str(ctx.params.get("order_id", ""))
        order = self._orders.get(order_id)
        if order is None:
            raise ValueError(f"Unknown order_id: {order_id}")
        return order
