"""ASGI entry point: ``uvicorn --factory main:create_application`` (RFC 0002).

Pipeline per request: correlation → CORS → route match → size limit →
auth policy → forward → relay. Operational routes are served locally.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from app.api.router import router
from app.clients.upstream.forward import make_client
from app.clients.upstream.openapi import OpenApiAggregator
from app.configs.env import GatewaySettings, get_settings
from app.infrastructure.logger import init_logger
from app.middleware.cors import EdgeCORSMiddleware
from app.utils.routing import ROUTES, RouteTable
from smartretail_common.app import install_platform
from smartretail_common.auth import JwtVerifier


def create_application(
    settings: GatewaySettings | None = None,
    *,
    verifier: JwtVerifier | None = None,
    transports: dict[str, httpx.AsyncBaseTransport] | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    init_logger(settings)
    routes = RouteTable(ROUTES, settings.upstreams)
    verifier = verifier or JwtVerifier.from_settings(settings)

    clients: dict[str, httpx.AsyncClient] = {}
    for name, base_url in settings.upstreams.items():
        if not base_url:
            continue  # an unconfigured upstream answers 503
        client = make_client(
            base_url,
            connect=settings.gateway_upstream_connect_timeout_s,
            read=settings.gateway_upstream_read_timeout_s,
            max_connections=settings.gateway_upstream_max_connections,
            keepalive=settings.gateway_upstream_max_keepalive,
            transport=(transports or {}).get(name),
        )
        clients[name] = client

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await verifier.refresh_jwks(force=True)
        if not verifier.has_keys:
            raise RuntimeError("no JWT verification key available")
        yield
        for client in clients.values():
            await client.aclose()

    application = FastAPI(
        title="SmartRetail gateway",
        version="1.0.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    application.state.settings = settings
    application.state.routes = routes
    application.state.verifier = verifier
    application.state.clients = clients
    application.state.upstream_names = tuple(settings.upstreams)
    application.state.openapi = OpenApiAggregator(
        clients,
        public_url=settings.gateway_public_url,
        ttl_seconds=settings.gateway_openapi_cache_ttl_s,
    )

    # Added first so the correlation middleware (added by install_platform)
    # wraps it: preflight responses carry X-Request-ID too.
    application.add_middleware(
        EdgeCORSMiddleware, origins=settings.cors_origins, max_age=settings.gateway_cors_max_age_s
    )
    install_platform(application, adopt_inbound_request_id=False)
    application.include_router(router)
    return application
