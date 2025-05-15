"""Product catalog service for the demo application."""

from __future__ import annotations

from typing import Any, Final

from moleculerpy import Context, Service, action


PRODUCTS: Final[dict[str, dict[str, Any]]] = {
    "starter-kit": {
        "id": "starter-kit",
        "name": "Starter Kit",
        "price": 19.99,
        "currency": "USD",
    },
    "pro-kit": {
        "id": "pro-kit",
        "name": "Pro Kit",
        "price": 49.50,
        "currency": "USD",
    },
    "team-bundle": {
        "id": "team-bundle",
        "name": "Team Bundle",
        "price": 129.00,
        "currency": "USD",
    },
}


class CatalogService(Service):
    """Read-only product catalog used by the checkout flow."""

    name = "catalog"

    def __init__(self) -> None:
        super().__init__(self.name)

    @action()
    async def list(self, ctx: Context) -> list[dict[str, Any]]:
        """Return all demo products."""
        return list(PRODUCTS.values())

    @action(params=["product_id"])
    async def get(self, ctx: Context) -> dict[str, Any]:
        """Return a single product by identifier."""
        product_id = str(ctx.params.get("product_id", ""))
        product = PRODUCTS.get(product_id)
        if product is None:
            raise ValueError(f"Unknown product_id: {product_id}")
        return product
