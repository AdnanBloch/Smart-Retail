"""Live price and availability at read time (D9, AST-2, AST-D11, AST-Q5).

One catalog read per product (anonymous: published only) and inventory reads
for its active variants, with bounded concurrency and a per-call timeout.

* catalog 404 → the product is no longer visible: the result is dropped and
  the caller removes it from the index (fail closed).
* catalog or inventory down → ``hydrated: false`` with null price/stock;
  a stale price is never shown as current.
"""

import asyncio
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from app.core.clients.catalog import CatalogClient
from app.core.clients.inventory import InventoryClient
from smartretail_common.errors import DependencyUnavailable

MAX_VARIANTS_CHECKED = 5


@dataclass(frozen=True)
class Live:
    price: Decimal | None = None
    currency: str | None = None
    in_stock: bool | None = None
    hydrated: bool = False
    removed: bool = False
    name: str | None = None


class Hydrator:
    def __init__(
        self,
        catalog: CatalogClient,
        inventory: InventoryClient,
        *,
        concurrency: int = 10,
        timeout: float = 2.0,
    ) -> None:
        self.catalog = catalog
        self.inventory = inventory
        self.semaphore = asyncio.Semaphore(concurrency)
        self.timeout = timeout

    async def hydrate(self, product_ids: list[uuid.UUID]) -> dict[uuid.UUID, Live]:
        unique = list(dict.fromkeys(product_ids))
        results = await asyncio.gather(*(self._one(p) for p in unique))
        return dict(zip(unique, results, strict=True))

    async def _call(self, coro: Any) -> Any:
        async with self.semaphore:
            return await asyncio.wait_for(coro, self.timeout)

    async def _one(self, product_id: uuid.UUID) -> Live:
        try:
            product = await self._call(self.catalog.get_product(product_id))
        except (DependencyUnavailable, TimeoutError):
            return Live()
        if product is None or product.get("status") not in (None, "published"):
            return Live(removed=True)
        try:
            price: Decimal | None = Decimal(str(product.get("base_price")))
        except (InvalidOperation, TypeError):
            price = None
        currency = product.get("currency")
        variants = [v for v in product.get("variants") or [] if v.get("is_active", True)]
        variant_ids = [v.get("id") or v.get("variant_id") for v in variants]
        variant_ids = [v for v in variant_ids if v][:MAX_VARIANTS_CHECKED]
        if not variant_ids:
            return Live(price, currency, False, True, name=product.get("name"))
        try:
            stocks = await asyncio.gather(
                *(self._call(self.inventory.get_availability(v)) for v in variant_ids)
            )
        except (DependencyUnavailable, TimeoutError):
            return Live(price, currency, None, False, name=product.get("name"))
        in_stock = any(s is not None and int(s.get("available", 0)) > 0 for s in stocks)
        return Live(price, currency, in_stock, True, name=product.get("name"))
