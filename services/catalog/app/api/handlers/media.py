import uuid

from fastapi import APIRouter, Depends, Query, Response, status

from app.api.handlers.utils import require_catalog_writer, variant_repo
from app.services.variant_repo import VariantRepo
from smartretail_common.auth import Principal

router = APIRouter()


async def remove_media(
    media_id: uuid.UUID,
    version: int = Query(..., ge=1),
    principal: Principal = Depends(require_catalog_writer),
    repo: VariantRepo = Depends(variant_repo),
) -> Response:
    await repo.remove_media(principal, media_id, version)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
