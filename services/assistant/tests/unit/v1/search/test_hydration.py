"""``Hydrator`` with fake catalog/inventory clients: live facts, fail closed on a
404, ``hydrated: false`` on an outage or timeout, and never a stale price
(D9, AST-D11)."""

import asyncio
import uuid
from decimal import Decimal
from typing import Any

from app.core.clients.inventory import InventoryUnavailable
from app.core.errors import CatalogUnavailable
from app.v1.search.hydration import MAX_VARIANTS_CHECKED, Hydrator, Live


class FakeCatalog:
    def __init__(self, products: dict[uuid.UUID, Any], *, delay: float = 0.0) -> None:
        self.products = products
        self.delay = delay
        self.calls: list[uuid.UUID] = []

    async def get_product(self, product_id: uuid.UUID, *, token: str | None = None):
        self.calls.append(product_id)
        if self.delay:
            await asyncio.sleep(self.delay)
        value = self.products.get(product_id)
        if isinstance(value, Exception):
            raise value
        return value


class FakeInventory:
    def __init__(self, stock: dict[str, Any]) -> None:
        self.stock = stock
        self.calls: list[str] = []
        self.in_flight = 0
        self.peak = 0

    async def get_availability(self, variant_id):
        self.calls.append(str(variant_id))
        self.in_flight += 1
        self.peak = max(self.peak, self.in_flight)
        try:
            await asyncio.sleep(0.01)
            value = self.stock.get(str(variant_id))
            if isinstance(value, Exception):
                raise value
            return value
        finally:
            self.in_flight -= 1


def product(*variants: dict[str, Any], price: Any = "12.50", status: str = "published"):
    return {
        "name": "Shoe",
        "base_price": price,
        "currency": "USD",
        "status": status,
        "variants": list(variants),
    }


def variant(vid: str, *, active: bool = True, key: str = "id") -> dict[str, Any]:
    return {key: vid, "is_active": active}


async def test_live_price_and_stock_from_any_in_stock_variant():
    pid = uuid.uuid4()
    catalog = FakeCatalog({pid: product(variant("v1"), variant("v2", key="variant_id"))})
    inventory = FakeInventory({"v1": {"available": 0}, "v2": {"available": 4}})
    live = (await Hydrator(catalog, inventory).hydrate([pid, pid]))[pid]
    assert live == Live(Decimal("12.50"), "USD", True, True, name="Shoe")
    assert catalog.calls == [pid]  # duplicate ids are hydrated once
    assert sorted(inventory.calls) == ["v1", "v2"]


async def test_a_variant_without_a_stock_row_is_not_in_stock():
    pid = uuid.uuid4()
    catalog = FakeCatalog({pid: product(variant("v1"))})
    live = (await Hydrator(catalog, FakeInventory({})).hydrate([pid]))[pid]
    assert live.hydrated and live.in_stock is False


async def test_an_unparseable_price_is_null_not_stale():
    pid, other = uuid.uuid4(), uuid.uuid4()
    catalog = FakeCatalog(
        {pid: product(variant("v1"), price="n/a"), other: product(variant("v1"), price=None)}
    )
    inventory = FakeInventory({"v1": {"available": 1}})
    out = await Hydrator(catalog, inventory).hydrate([pid, other])
    for live in out.values():
        assert live.price is None and live.hydrated and live.in_stock is True


async def test_no_active_variant_is_out_of_stock_without_asking_inventory():
    pid, bare = uuid.uuid4(), uuid.uuid4()
    catalog = FakeCatalog(
        {
            pid: product(variant("v1", active=False), {"is_active": True}),  # no id at all
            bare: {**product(), "variants": None},
        }
    )
    inventory = FakeInventory({"v1": {"available": 9}})
    out = await Hydrator(catalog, inventory).hydrate([pid, bare])
    for live in out.values():
        assert live == Live(Decimal("12.50"), "USD", False, True, name="Shoe")
    assert inventory.calls == []


async def test_only_the_first_variants_are_checked():
    pid = uuid.uuid4()
    variants = [variant(f"v{i}") for i in range(MAX_VARIANTS_CHECKED + 3)]
    inventory = FakeInventory({f"v{MAX_VARIANTS_CHECKED + 1}": {"available": 5}})
    live = (await Hydrator(FakeCatalog({pid: product(*variants)}), inventory).hydrate([pid]))[pid]
    assert len(inventory.calls) == MAX_VARIANTS_CHECKED
    assert live.in_stock is False  # the in-stock variant lies beyond the bound


async def test_not_visible_or_not_published_is_removed():
    gone, draft = uuid.uuid4(), uuid.uuid4()
    catalog = FakeCatalog({gone: None, draft: product(variant("v1"), status="draft")})
    out = await Hydrator(catalog, FakeInventory({})).hydrate([gone, draft])
    assert out[gone] == Live(removed=True) and out[draft] == Live(removed=True)


async def test_catalog_outage_or_timeout_is_unhydrated():
    down, slow = uuid.uuid4(), uuid.uuid4()
    catalog = FakeCatalog({down: CatalogUnavailable("down")})
    assert (await Hydrator(catalog, FakeInventory({})).hydrate([down]))[down] == Live()

    sleepy = FakeCatalog({slow: product(variant("v1"))}, delay=1.0)
    live = (await Hydrator(sleepy, FakeInventory({}), timeout=0.05).hydrate([slow]))[slow]
    assert live == Live() and live.price is None and not live.removed


async def test_inventory_outage_keeps_the_price_but_not_the_stock():
    pid = uuid.uuid4()
    catalog = FakeCatalog({pid: product(variant("v1"), variant("v2"))})
    inventory = FakeInventory({"v1": {"available": 3}, "v2": InventoryUnavailable("down")})
    live = (await Hydrator(catalog, inventory).hydrate([pid]))[pid]
    assert live == Live(Decimal("12.50"), "USD", None, False, name="Shoe")


async def test_concurrency_is_bounded():
    ids = [uuid.uuid4() for _ in range(6)]
    catalog = FakeCatalog({p: product(variant(f"v-{p}")) for p in ids})
    inventory = FakeInventory({f"v-{p}": {"available": 1} for p in ids})
    out = await Hydrator(catalog, inventory, concurrency=2).hydrate(ids)
    assert all(live.in_stock for live in out.values())
    assert inventory.peak <= 2
