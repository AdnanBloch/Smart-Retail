"""Every URL the service serves, in one place.

This file only includes routers: it owns the version prefix and each entity's
prefix, so the full path of any endpoint can be read here.
"""

from fastapi import APIRouter

from app.orders.v1 import routes as orders_v1

router = APIRouter()

# Route: /api/v1/
v1 = APIRouter(prefix="/api/v1")
v1.include_router(orders_v1.router, prefix="/orders", tags=["orders"])

router.include_router(v1)
