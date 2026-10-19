"""``/api/v1/merchants``: merchant profiles (RFC 0003 §3.1.2, IDN-14)."""

import uuid

from fastapi import APIRouter, Depends, Query

from app.core.dependencies import optional_principal, require_admin, require_merchant
from app.core.enums import MerchantStatus
from app.v1.merchants.dependencies import merchant_repo
from app.v1.merchants.repo import MerchantRepo
from app.v1.merchants.schemas import MerchantMePatch, MerchantOut
from smartretail_common.auth import Principal
from smartretail_common.pagination import Page, PageParams, page_params

router = APIRouter()


@router.get("", response_model=Page[MerchantOut], dependencies=[Depends(require_admin)])
async def list_merchants(
    status: MerchantStatus | None = None,
    q: str | None = Query(None, max_length=200),
    page: PageParams = Depends(page_params),
    repo: MerchantRepo = Depends(merchant_repo),
) -> Page[MerchantOut]:
    profiles = await repo.list(page, status=status, q=q)
    return profiles.to_page(MerchantOut.from_profile)


# Registered before /{merchant_id} so "me" is not parsed as an id.
@router.get("/me", response_model=MerchantOut)
async def get_my_profile(
    principal: Principal = Depends(require_merchant),
    repo: MerchantRepo = Depends(merchant_repo),
) -> MerchantOut:
    return MerchantOut.from_profile(await repo.get_me(principal))


@router.patch("/me", response_model=MerchantOut)
async def update_my_profile(
    body: MerchantMePatch,
    principal: Principal = Depends(require_merchant),
    repo: MerchantRepo = Depends(merchant_repo),
) -> MerchantOut:
    profile = await repo.update_me(principal, body.model_dump(exclude_unset=True))
    return MerchantOut.from_profile(profile)


@router.get("/{merchant_id}", response_model=MerchantOut)
async def get_merchant(
    merchant_id: uuid.UUID,
    viewer: Principal | None = Depends(optional_principal),
    repo: MerchantRepo = Depends(merchant_repo),
) -> MerchantOut:
    return MerchantOut.from_profile(await repo.get_public(merchant_id, viewer))
