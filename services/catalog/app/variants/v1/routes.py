"""``/api/v1/variants``: change or deactivate one variant (RFC 0004 §3.1.2).

Variants are created under their product: ``POST /api/v1/products/{id}/variants``.
"""

import uuid

from fastapi import APIRouter, Depends, Query

from app.core.dependencies import require_catalog_writer
from app.variants.dependencies import variant_repo
from app.variants.repo import VariantRepo
from app.variants.v1.schemas import VariantDeactivated, VariantPatch, VariantWritten
from smartretail_common.auth import Principal

router = APIRouter()


@router.patch("/{variant_id}", response_model=VariantWritten)
async def update_variant(
    variant_id: uuid.UUID,
    body: VariantPatch,
    principal: Principal = Depends(require_catalog_writer),
    repo: VariantRepo = Depends(variant_repo),
) -> VariantWritten:
    changes = body.model_dump(exclude_unset=True, exclude={"version"})
    write = await repo.update(principal, variant_id, changes, body.version)
    return VariantWritten.from_write(write)


@router.delete("/{variant_id}", response_model=VariantDeactivated)
async def deactivate_variant(
    variant_id: uuid.UUID,
    version: int = Query(..., ge=1),
    principal: Principal = Depends(require_catalog_writer),
    repo: VariantRepo = Depends(variant_repo),
) -> VariantDeactivated:
    return VariantDeactivated.from_write(await repo.deactivate(principal, variant_id, version))
