"""Builds the search service for a request."""

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.db import get_db
from app.core.dependencies import get_settings
from app.v1.search.clients.embeddings import EmbeddingProvider
from app.v1.search.hydration import Hydrator
from app.v1.search.service import SearchService


def get_embeddings(request: Request) -> EmbeddingProvider:
    return request.app.state.embeddings


def get_hydrator(request: Request) -> Hydrator:
    return request.app.state.hydrator


def search_service(
    session: AsyncSession = Depends(get_db),
    embeddings: EmbeddingProvider = Depends(get_embeddings),
    hydrator: Hydrator = Depends(get_hydrator),
    settings: Settings = Depends(get_settings),
) -> SearchService:
    return SearchService(session, embeddings, hydrator, settings)
