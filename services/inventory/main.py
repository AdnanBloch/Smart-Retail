"""ASGI entry point: ``uvicorn --factory main:create_application``."""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.core.config import Settings, get_settings
from app.core.db import create_database
from app.core.logger import init_logger
from app.health import routes as health
from app.reservations import sweeper
from app.router import router
from smartretail_common.app import install_platform
from smartretail_common.auth import JwtVerifier


def create_application(
    settings: Settings | None = None, verifier: JwtVerifier | None = None
) -> FastAPI:
    settings = settings or get_settings()
    init_logger(settings)
    db = create_database(settings)
    # Tokens are verified locally (A5): no call to identity per request.
    verifier = verifier or JwtVerifier.from_settings(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await verifier.refresh_jwks(force=True)
        if not verifier.has_keys:
            raise RuntimeError("no JWT verification key available")
        task = (
            asyncio.create_task(sweeper.run_forever(db, settings))
            if settings.inventory_sweeper_inprocess
            else None
        )
        yield
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await db.dispose()

    application = FastAPI(
        title="SmartRetail inventory",
        version="1.0.0",
        description="Stock, reservations and the stock ledger (RFC 0006).",
        lifespan=lifespan,
    )
    application.state.settings = settings
    application.state.db = db
    application.state.verifier = verifier
    install_platform(application)
    application.include_router(health.build_router(db))
    application.include_router(router)
    return application
