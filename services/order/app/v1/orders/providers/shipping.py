"""``ShippingProvider`` and its deterministic fake (A9, B10)."""

import asyncio
import hashlib
from dataclasses import dataclass, field
from typing import Any, Protocol

from app.core.config import Settings
from app.v1.orders.providers.errors import AddressRejected, ProviderTransientError


@dataclass(frozen=True)
class ShipLine:
    sku: str
    quantity: int


@dataclass(frozen=True)
class ShipmentResult:
    reference: str
    tracking_number: str


class ShippingProvider(Protocol):
    async def create_shipment(
        self,
        *,
        idempotency_key: str,
        order_number: str,
        address: dict[str, Any],
        lines: list[ShipLine],
    ) -> ShipmentResult: ...


@dataclass
class FakeShippingProvider:
    settings: Settings
    shipments: dict[str, ShipmentResult] = field(default_factory=dict)

    async def create_shipment(
        self,
        *,
        idempotency_key: str,
        order_number: str,
        address: dict[str, Any],
        lines: list[ShipLine],
    ) -> ShipmentResult:
        if idempotency_key in self.shipments:
            return self.shipments[idempotency_key]
        postcode = str(address.get("postal_code", ""))
        if postcode == self.settings.fake_shipping_reject_postcode:
            raise AddressRejected(f"carrier cannot deliver to {postcode}")
        if postcode == self.settings.fake_shipping_timeout_postcode:
            await asyncio.sleep(self.settings.fake_provider_timeout_seconds)
            raise ProviderTransientError("carrier timed out")
        digest = hashlib.sha256(idempotency_key.encode()).hexdigest()
        result = ShipmentResult(f"shp_{digest[:16]}", f"TRK{int(digest[:12], 16) % 10**10:010d}")
        self.shipments[idempotency_key] = result
        return result
