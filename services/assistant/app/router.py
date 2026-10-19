"""Every URL the service serves, in one place (RFC 0009 §3.1.1.1).

This file only includes routers: it owns ``/api/v1`` and each entity's
prefix. ``search/v1`` is mounted at ``/search``; ``chat/v1`` and
``generations/v1`` share ``/assistant`` because the public URLs do.
``prompts/`` has no HTTP surface.
"""

from fastapi import APIRouter

from app.v1.chat import routes as chat_v1
from app.v1.generations import routes as generations_v1
from app.v1.search import routes as search_v1

router = APIRouter()

# Route: /api/v1/
v1 = APIRouter(prefix="/api/v1")
v1.include_router(search_v1.router, prefix="/search", tags=["search"])
v1.include_router(chat_v1.router, prefix="/assistant", tags=["assistant: chat"])
v1.include_router(generations_v1.router, prefix="/assistant", tags=["assistant: generation"])

router.include_router(v1)
