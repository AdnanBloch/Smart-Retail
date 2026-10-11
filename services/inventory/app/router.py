"""Every URL the service serves, in one place.

This file only includes routers: it owns the version prefixes and each
entity's prefix, so the full path of any endpoint can be read here. The
internal surface lives under ``/internal/v1`` so that the gateway's prefix
routing can never expose it by accident (it also blocks ``/internal/*``).
"""

from fastapi import APIRouter

from app.reservations.v1 import routes as reservations_v1
from app.stock.v1 import routes as stock_v1

router = APIRouter()

# Route: /api/v1/
v1 = APIRouter(prefix="/api/v1")
v1.include_router(stock_v1.router, prefix="/inventory", tags=["inventory"])

# Route: /internal/v1/inventory/ — service tokens with scope inventory:internal
internal_v1 = APIRouter(prefix="/internal/v1/inventory")
internal_v1.include_router(
    reservations_v1.router, prefix="/reservations", tags=["internal: reservations"]
)
internal_v1.include_router(stock_v1.internal_router, tags=["internal: stock"])

router.include_router(v1)
router.include_router(internal_v1)
