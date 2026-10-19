"""``/api/v1/merchants`` request and response models."""

import uuid
from typing import Annotated

from pydantic import Field

from app.core.enums import MerchantStatus
from app.core.schemas import OutModel, StrictModel
from app.v1.merchants.models import MerchantProfile
from smartretail_common.enums import widen_for_response


class MerchantOut(OutModel):
    merchant_id: uuid.UUID
    business_name: str
    slug: str
    description: str | None
    status: Annotated[MerchantStatus | str, widen_for_response(MerchantStatus)]

    @classmethod
    def from_profile(cls, profile: MerchantProfile) -> "MerchantOut":
        # The public id of a merchant is its user id (RFC 0000 I4).
        return cls(
            merchant_id=profile.user_id,
            business_name=profile.business_name,
            slug=profile.slug,
            description=profile.description,
            status=profile.status,
        )


class MerchantMePatch(StrictModel):
    business_name: Annotated[str, Field(min_length=1, max_length=200)] | None = None
    description: Annotated[str, Field(max_length=2000)] | None = None
