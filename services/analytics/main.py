"""ASGI entry point: ``uvicorn --factory main:create_application``."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.core.config import Settings, get_settings
from app.core.db import Mongo, create_mongo
from app.core.logger import init_logger
from app.health import routes as health
from app.router import router
from app.v1.metrics.models import prepare_store
from smartretail_common.app import install_platform
from smartretail_common.auth import JwtVerifier


def create_application(
    settings: Settings | None = None,
    verifier: JwtVerifier | None = None,
    mongo: Mongo | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    init_logger(settings)
    mongo = mongo or create_mongo(settings)
    # Tokens are verified locally (A5): no call to identity per request.
    verifier = verifier or JwtVerifier.from_settings(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await verifier.refresh_jwks(force=True)
        if not verifier.has_keys:
            raise RuntimeError("no JWT verification key available")
        # Refuses a stand-alone mongod (ANL-1) and creates indexes before traffic.
        await prepare_store(app.state.mongo, settings)
        yield
        await app.state.mongo.close()

    application = FastAPI(
        title="SmartRetail analytics",
        version="1.0.0",
        description="Event-fed metric rollups and their read API (RFC 0008).",
        lifespan=lifespan,
    )
    application.state.settings = settings
    application.state.mongo = mongo
    application.state.verifier = verifier
    install_platform(application)
    application.include_router(health.build_router(mongo))
    application.include_router(router)
    return application
