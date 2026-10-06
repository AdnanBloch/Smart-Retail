import uuid

from fastapi import APIRouter, Depends, Query

from app.api.handlers.utils import merchant_repo, optional_principal, require_merchant
from app.models.enums import MerchantStatus
from app.models.merchant_profile import MerchantMePatch, MerchantOut
from app.services.merchant_repo import MerchantRepo
from smartretail_common.auth import Principal
from smartretail_common.pagination import Page, PageParams, page_params

router = APIRouter()


async def list_merchants(
    status: MerchantStatus | None = None,
    q: str | None = Query(None, max_length=200),
    page: PageParams = Depends(page_params),
    repo: MerchantRepo = Depends(merchant_repo),
) -> Page[MerchantOut]:
    return await repo.list(page, status=status, q=q)


async def get_my_profile(
    principal: Principal = Depends(require_merchant),
    repo: MerchantRepo = Depends(merchant_repo),
) -> MerchantOut:
    return await repo.get_me(principal)


async def update_my_profile(
    body: MerchantMePatch,
    principal: Principal = Depends(require_merchant),
    repo: MerchantRepo = Depends(merchant_repo),
) -> MerchantOut:
    return await repo.update_me(principal, body)


async def get_merchant(
    merchant_id: uuid.UUID,
    viewer: Principal | None = Depends(optional_principal),
    repo: MerchantRepo = Depends(merchant_repo),
) -> MerchantOut:
    return await repo.get_public(merchant_id, viewer)
