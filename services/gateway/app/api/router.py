from fastapi import APIRouter

from app.api.handlers import docs, health, proxy

PROXY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"]

# Route: /
router = APIRouter()

# Route: /health/
health.router.get("")(health.get_health)
health.router.get("/ready")(health.get_ready)

router.include_router(health.router, prefix="/health")

# Route: /openapi.json, /docs
docs.router.get("/openapi.json")(docs.get_openapi)
docs.router.get("/docs")(docs.get_docs)

router.include_router(docs.router)

# Route: /{path} — registered last so the operational routes above win.
proxy.router.api_route("/{path:path}", methods=PROXY_METHODS)(proxy.proxy)

router.include_router(proxy.router)
