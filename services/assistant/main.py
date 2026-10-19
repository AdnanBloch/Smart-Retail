"""ASGI entry point: ``uvicorn --factory main:create_application``.

Providers and clients are built here from settings and can be injected for
tests: the embedding provider, the LLM provider, the shared HTTP client (so
catalog and inventory can be faked with ``httpx.MockTransport``) and the
task enqueuer.
"""

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from app.core.celery_app import enqueue as celery_enqueue
from app.core.clients.catalog import CatalogClient
from app.core.clients.inventory import InventoryClient
from app.core.clients.llm import LLMProvider, build_llm
from app.core.config import Settings, get_settings
from app.core.db import create_database
from app.core.logger import init_logger
from app.health import routes as health
from app.router import router
from app.v1.chat.agent.graph import build_graph
from app.v1.chat.agent.turn import ChatTurn
from app.v1.generations.service import Generator
from app.v1.search.clients.embeddings import EmbeddingProvider, build_embeddings
from app.v1.search.hydration import Hydrator
from smartretail_common.app import install_platform
from smartretail_common.auth import JwtVerifier


def create_application(
    settings: Settings | None = None,
    verifier: JwtVerifier | None = None,
    *,
    http: httpx.AsyncClient | None = None,
    embeddings: EmbeddingProvider | None = None,
    llm: LLMProvider | None = None,
    enqueue: Callable[..., None] | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    init_logger(settings)
    db = create_database(settings)
    # Tokens are verified locally (A5): no call to identity per request.
    verifier = verifier or JwtVerifier.from_settings(settings)
    http = http or httpx.AsyncClient(
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
    )
    embeddings = embeddings or build_embeddings(settings, http)
    llm = llm or build_llm(settings)
    catalog = CatalogClient(
        settings.catalog_base_url, http, timeout=settings.hydration_timeout_seconds
    )
    inventory = InventoryClient(
        settings.inventory_base_url, http, timeout=settings.hydration_timeout_seconds
    )
    hydrator = Hydrator(
        catalog,
        inventory,
        concurrency=settings.hydration_concurrency,
        timeout=settings.hydration_timeout_seconds,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await verifier.refresh_jwks(force=True)
        if not verifier.has_keys:
            raise RuntimeError("no JWT verification key available")
        yield
        await http.aclose()
        await db.dispose()

    application = FastAPI(
        title="SmartRetail assistant",
        version="1.0.0",
        description="Semantic search, the shopping assistant and merchant generation (RFC 0009).",
        lifespan=lifespan,
    )
    state = application.state
    state.settings = settings
    state.db = db
    state.verifier = verifier
    state.http = http
    state.embeddings = embeddings
    state.llm = llm
    state.catalog = catalog
    state.inventory = inventory
    state.hydrator = hydrator
    state.enqueue = enqueue or celery_enqueue
    state.active_streams = 0
    state.chat_turn = ChatTurn(
        db,
        graph=build_graph(),
        provider=llm,
        embeddings=embeddings,
        hydrator=hydrator,
        settings=settings,
    )
    state.generator = Generator(db, llm, settings)
    install_platform(application)
    application.include_router(health.build_router(db))
    application.include_router(router)
    return application
