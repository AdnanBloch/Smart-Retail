"""Every URL the gateway serves, in one place.

The gateway owns no API contract, so it has no versioned folders: the
``/api/v<N>/...`` paths it accepts are rows in ``proxy/routing.py`` and are
forwarded unchanged. This file only includes routers and owns their prefixes.
"""

from fastapi import APIRouter

from app.docs import routes as docs
from app.health import routes as health
from app.proxy import routes as proxy

router = APIRouter()

# Route: /health/
router.include_router(health.router, prefix="/health")

# Route: /openapi.json, /docs
router.include_router(docs.router)

# Route: /{path} — included last so the operational routes above win.
router.include_router(proxy.router)
