"""ASGI entry point: ``uvicorn --factory main:create_application``."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from app.core.config import Settings, get_settings
from app.core.db import create_database
from app.core.logger import init_logger
from app.health import routes as health
from app.router import router
from app.v1.orders.clients.catalog import CatalogClient
from app.v1.orders.workflows.client import TemporalOrderWorkflows
from smartretail_common.app import install_platform
from smartretail_common.auth import JwtVerifier
from smartretail_common.temporal import TemporalClientFactory


def create_application(
    settings: Settings | None = None,
    verifier: JwtVerifier | None = None,
    *,
    workflows: TemporalOrderWorkflows | None = None,
    catalog_http: httpx.AsyncClient | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    init_logger(settings)
    db = create_database(settings)
    # Tokens are verified locally (A5): no call to identity per request.
    verifier = verifier or JwtVerifier.from_settings(settings)
    http = catalog_http or httpx.AsyncClient()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await verifier.refresh_jwks(force=True)
        if not verifier.has_keys:
            raise RuntimeError("no JWT verification key available")
        yield
        await http.aclose()
        await db.dispose()

    application = FastAPI(
        title="SmartRetail order",
        version="1.0.0",
        description="Orders, idempotent checkout and the order saga (RFC 0005).",
        lifespan=lifespan,
    )
    application.state.settings = settings
    application.state.db = db
    application.state.verifier = verifier
    application.state.catalog = CatalogClient(
        settings.catalog_base_url, http, timeout=settings.catalog_timeout_seconds
    )
    # Temporal is connected on first use; checkout accepts orders without it (ORD-9).
    application.state.workflows = workflows or TemporalOrderWorkflows(
        TemporalClientFactory(settings.temporal_address, settings.temporal_namespace), settings
    )
    install_platform(application)
    application.include_router(health.build_router(db))
    application.include_router(router)
    return application
