"""Every URL the service serves, in one place.

This file only includes routers: it owns the version prefix and the
``/notifications`` prefix for both entity routers. ``stream/v1`` is included
before ``notifications/v1`` so its literal segments are never captured by
``/{id}`` (RFC 0007 §3.1.1.1).
"""

from fastapi import APIRouter

from app.v1.notifications import routes as notifications_v1
from app.v1.stream import routes as stream_v1

router = APIRouter()

# Route: /api/v1/notifications/
v1 = APIRouter(prefix="/api/v1")
v1.include_router(stream_v1.router, prefix="/notifications", tags=["notifications: stream"])
v1.include_router(notifications_v1.router, prefix="/notifications", tags=["notifications"])

router.include_router(v1)
