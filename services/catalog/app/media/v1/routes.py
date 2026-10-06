"""``/api/v1/media``: remove one media item (RFC 0004 §3.1.2).

Media is added under its product: ``POST /api/v1/products/{id}/media``.
"""

import uuid

from fastapi import APIRouter, Depends, Query, Response, status

from app.core.dependencies import require_catalog_writer
from app.media.dependencies import media_repo
from app.media.repo import MediaRepo
from smartretail_common.auth import Principal

router = APIRouter()


@router.delete("/{media_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_media(
    media_id: uuid.UUID,
    version: int = Query(..., ge=1),
    principal: Principal = Depends(require_catalog_writer),
    repo: MediaRepo = Depends(media_repo),
) -> Response:
    await repo.remove(principal, media_id, version)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
