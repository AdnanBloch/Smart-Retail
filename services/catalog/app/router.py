"""Every URL the service serves, in one place.

This file only includes routers: it owns the version prefix and each entity's
prefix, so the full path of any endpoint can be read here. Paths below an
entity's prefix live in that entity's ``v<N>/routes.py``. Adding a version
means adding ``<entity>/v2/`` and one ``include_router`` line below.
"""

from fastapi import APIRouter

from app.v1.categories import routes as categories_v1
from app.v1.media import routes as media_v1
from app.v1.products import routes as products_v1
from app.v1.variants import routes as variants_v1

router = APIRouter()

# Route: /api/v1/
v1 = APIRouter(prefix="/api/v1")
v1.include_router(categories_v1.router, prefix="/categories", tags=["categories"])
v1.include_router(products_v1.router, prefix="/products", tags=["products"])
v1.include_router(variants_v1.router, prefix="/variants", tags=["variants"])
v1.include_router(media_v1.router, prefix="/media", tags=["media"])

router.include_router(v1)
