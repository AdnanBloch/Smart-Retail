"""Every URL the service serves, in one place.

This file only includes routers: it owns the version prefix and each entity's
prefix, so the full path of any endpoint can be read here. Paths below an
entity's prefix live in that entity's ``v<N>/routes.py``. Adding a version
means adding ``<entity>/v2/`` and one ``include_router`` line below.
"""

from fastapi import APIRouter

from app.auth.v1 import routes as auth_v1
from app.merchants.v1 import routes as merchants_v1
from app.users.v1 import routes as users_v1

router = APIRouter()

# Route: /api/v1/
v1 = APIRouter(prefix="/api/v1")
v1.include_router(auth_v1.router, prefix="/auth", tags=["auth"])
v1.include_router(users_v1.router, prefix="/users", tags=["users"])
v1.include_router(merchants_v1.router, prefix="/merchants", tags=["merchants"])

router.include_router(v1)
