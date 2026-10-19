"""ASGI entry point: ``uvicorn --factory main:create_application``."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.core.config import Settings, get_settings
from app.core.db import create_database
from app.core.logger import init_logger
from app.health import routes as health
from app.router import router
from app.v1.stream.fanout import FanoutHub
from smartretail_common.app import install_platform
from smartretail_common.auth import JwtVerifier
from smartretail_common.redis import create_redis


def create_application(
    settings: Settings | None = None, verifier: JwtVerifier | None = None
) -> FastAPI:
    settings = settings or get_settings()
    init_logger(settings)
    db = create_database(settings)
    redis = create_redis(settings.redis_url)
    hub = FanoutHub(redis, subscribe_timeout=settings.stream_subscribe_timeout_seconds)
    # Tokens are verified locally (A5): no call to identity per request.
    verifier = verifier or JwtVerifier.from_settings(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await verifier.refresh_jwks(force=True)
        if not verifier.has_keys:
            raise RuntimeError("no JWT verification key available")
        yield
        # Drain: end every open stream with a retry hint before the pod exits.
        await hub.close()
        await redis.aclose()
        await db.dispose()

    application = FastAPI(
        title="SmartRetail notification",
        version="1.0.0",
        description="Notifications, dispatch and the SSE push stream (RFC 0007).",
        lifespan=lifespan,
    )
    application.state.settings = settings
    application.state.db = db
    application.state.redis = redis
    application.state.hub = hub
    application.state.verifier = verifier
    install_platform(application)
    application.include_router(health.build_router(db, redis))
    application.include_router(router)
    return application
