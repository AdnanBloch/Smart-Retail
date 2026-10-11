"""The catalog snapshot at checkout (RFC 0005 §3.1.1.5, B8, A14, I9).

Live lookup, then copy: name, SKU, options and price are frozen into the
order lines, so a later catalog change never reaches a historical order.
Amounts come from the snapshot, never from the client. Stock is **not**
checked here — the reservation is the check, and checking twice would race.
"""

import asyncio
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from app.clients.catalog import CatalogClient
from smartretail_common.errors import Unprocessable

PUBLISHED = "published"
CONCURRENCY = 8


@dataclass(frozen=True)
class LineRequest:
    product_id: uuid.UUID
    variant_id: uuid.UUID
    quantity: int


@dataclass(frozen=True)
class LineSnapshot:
    product_id: uuid.UUID
    variant_id: uuid.UUID
    merchant_id: uuid.UUID
    product_name: str
    sku: str
    variant_options: dict[str, Any]
    unit_price: Decimal
    quantity: int
    currency: str


def _violation(index: int, field: str, message: str, code: str) -> dict[str, str]:
    return {"field": f"items[{index}].{field}", "message": message, "code": code}


def check_shape(lines: list[LineRequest], *, max_lines: int, max_quantity: int) -> None:
    """Rules that need no catalog: line count, quantity bounds, I10 (one line per variant)."""
    violations: list[dict[str, str]] = []
    if len(lines) > max_lines:
        violations.append(
            {"field": "items", "message": f"at most {max_lines} lines", "code": "too_many"}
        )
    seen: set[uuid.UUID] = set()
    for i, line in enumerate(lines):
        if line.variant_id in seen:
            # Required: inventory reservations are unique per (order, variant) (ORD-6).
            violations.append(_violation(i, "variant_id", "duplicate variant", "duplicate"))
        seen.add(line.variant_id)
        if line.quantity > max_quantity:
            violations.append(_violation(i, "quantity", f"at most {max_quantity}", "too_large"))
    if violations:
        raise Unprocessable("The order lines are invalid", violations=violations)


async def take(catalog: CatalogClient, token: str, lines: list[LineRequest]) -> list[LineSnapshot]:
    """One lookup per distinct product, at most eight at a time, with the user's token."""
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def fetch(product_id: uuid.UUID) -> tuple[uuid.UUID, dict[str, Any] | None]:
        async with semaphore:
            return product_id, await catalog.get_product(product_id, token)

    product_ids = list(dict.fromkeys(line.product_id for line in lines))
    products = dict(await asyncio.gather(*(fetch(p) for p in product_ids)))

    violations: list[dict[str, str]] = []
    snapshots: list[LineSnapshot] = []
    for i, line in enumerate(lines):
        product = products.get(line.product_id)
        if product is None or product.get("status") != PUBLISHED:
            violations.append(
                _violation(i, "product_id", "unknown or unpublished product", "unavailable")
            )
            continue
        variant = next(
            (v for v in product.get("variants", []) if v["id"] == str(line.variant_id)), None
        )
        if variant is None:
            violations.append(
                _violation(i, "variant_id", "not a variant of this product", "unknown")
            )
            continue
        if not variant.get("is_active", False):
            violations.append(_violation(i, "variant_id", "variant is not for sale", "inactive"))
            continue
        price = variant.get("price_override")
        snapshots.append(
            LineSnapshot(
                product_id=line.product_id,
                variant_id=line.variant_id,
                merchant_id=uuid.UUID(product["merchant_id"]),
                product_name=product["name"],
                sku=variant["sku"],
                variant_options=variant.get("options") or {},
                # ORD-4: the variant's override if set, else the product's base price.
                unit_price=Decimal(str(price if price is not None else product["base_price"])),
                quantity=line.quantity,
                currency=product["currency"],
            )
        )
    if not violations and len({s.currency for s in snapshots}) > 1:
        # I5: mixed currencies are rejected, never converted.
        violations.append(
            {"field": "items", "message": "all lines must share one currency", "code": "currency"}
        )
    if violations:
        raise Unprocessable("Some order lines cannot be bought", violations=violations)
    return snapshots
