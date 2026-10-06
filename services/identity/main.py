"""ASGI entry point: ``uvicorn --factory main:create_application``."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.core.config import Settings, get_settings
from app.core.db import create_database
from app.core.logger import init_logger
from app.core.security import KeyRing, PasswordHasher
from app.health import routes as health
from app.router import router
from smartretail_common.app import install_platform
from smartretail_common.auth import JwtVerifier


def create_application(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    init_logger(settings)

    # Fails start-up if the active signing key is missing (IDN-5).
    keyring = KeyRing.load(settings.jwt_keys_dir, settings.jwt_active_kid)
    db = create_database(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        await db.dispose()

    application = FastAPI(
        title="SmartRetail identity",
        version="1.0.0",
        description="Users, credentials, roles, merchant profiles, tokens and JWKS (RFC 0003).",
        lifespan=lifespan,
    )
    application.state.settings = settings
    application.state.db = db
    application.state.keyring = keyring
    application.state.hasher = PasswordHasher(settings)
    # identity verifies its own tokens against its key ring, no JWKS round trip.
    application.state.verifier = JwtVerifier(
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        leeway_seconds=settings.jwt_leeway_seconds,
        static_keys=keyring.public_keys(),
    )

    install_platform(application)
    application.include_router(health.build_router(db, keyring))
    application.include_router(router)
    return application
