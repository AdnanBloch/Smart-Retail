"""Every URL the service serves, in one place.

Only includes routers: owns ``/api/v1`` and the ``/analytics`` prefix, so the
full path of any endpoint can be read here. The gateway routes these paths
to ``analytics:8006`` (RFC 0002, ANL-D22).
"""

from fastapi import APIRouter

from app.v1.metrics import routes as metrics_v1

router = APIRouter()

# Route: /api/v1/analytics/
v1 = APIRouter(prefix="/api/v1")
v1.include_router(metrics_v1.router, prefix="/analytics", tags=["analytics"])

router.include_router(v1)
