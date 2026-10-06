import uuid

from fastapi import APIRouter, Depends, Query

from app.api.handlers.utils import require_catalog_writer, variant_repo
from app.models.variant import VariantDeactivated, VariantPatch, VariantWritten
from app.services.variant_repo import VariantRepo
from smartretail_common.auth import Principal

router = APIRouter()


async def update_variant(
    variant_id: uuid.UUID,
    body: VariantPatch,
    principal: Principal = Depends(require_catalog_writer),
    repo: VariantRepo = Depends(variant_repo),
) -> VariantWritten:
    return await repo.update(principal, variant_id, body)


async def deactivate_variant(
    variant_id: uuid.UUID,
    version: int = Query(..., ge=1),
    principal: Principal = Depends(require_catalog_writer),
    repo: VariantRepo = Depends(variant_repo),
) -> VariantDeactivated:
    return await repo.deactivate(principal, variant_id, version)
