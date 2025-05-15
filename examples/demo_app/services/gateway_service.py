"""Gateway service that orchestrates the demo checkout flow."""

from __future__ import annotations

from typing import Any

from moleculerpy import Context, Service, action


class GatewayService(Service):
    """Expose higher-level workflow actions for the demo application."""

    name = "gateway"

    def __init__(self) -> None:
        super().__init__(self.name)

    @action(params=["product_id", "quantity", "customer_name"])
    async def checkout(self, ctx: Context) -> dict[str, Any]:
        """Run catalog lookup, order placement, and payment capture."""
        product_id = str(ctx.params.get("product_id", "starter-kit"))
        quantity = int(ctx.params.get("quantity", 1))
        customer_name = str(ctx.params.get("customer_name", "Guest"))
        payment_method = str(ctx.params.get("payment_method", "card"))

        product = await ctx.call("catalog.get", {"product_id": product_id})
        order = await ctx.call(
            "orders.place",
            {
                "product_id": product["id"],
                "product_name": product["name"],
                "quantity": quantity,
                "unit_price": product["price"],
                "customer_name": customer_name,
            },
        )
        payment = await ctx.call(
            "payments.charge",
            {
                "order_id": order["order_id"],
                "total": order["total"],
                "customer_name": customer_name,
                "payment_method": payment_method,
            },
        )

        return {
            "order": order,
            "payment": payment,
            "message": f"Checkout completed for {customer_name}",
        }

    @action()
    async def dashboard(self, ctx: Context) -> dict[str, Any]:
        """Return a consolidated snapshot for the demo."""
        orders = await ctx.call("orders.list")
        receipts = await ctx.call("payments.list")
        analytics = await ctx.call("analytics.stats")
        notifications = await ctx.call("notifications.recent")

        return {
            "orders": orders,
            "receipts": receipts,
            "analytics": analytics,
            "notifications": notifications,
        }
