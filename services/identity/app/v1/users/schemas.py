"""``/api/v1/users`` request and response models."""

import uuid
from datetime import datetime
from typing import Annotated

from app.core.enums import MerchantStatus, UserRole
from app.core.schemas import FullName, OutModel, Password, Phone, StrictModel
from smartretail_common.enums import widen_for_response


class UserOut(OutModel):
    id: uuid.UUID
    email: str
    full_name: str
    phone: str | None
    role: Annotated[UserRole | str, widen_for_response(UserRole)]
    is_active: bool
    email_verified: bool
    created_at: datetime
    updated_at: datetime


class MerchantProfileOut(OutModel):
    business_name: str
    slug: str
    description: str | None
    status: Annotated[MerchantStatus | str, widen_for_response(MerchantStatus)]


class UserDetailOut(UserOut):
    merchant_profile: MerchantProfileOut | None = None


class UserMePatch(StrictModel):
    full_name: FullName | None = None
    phone: Phone | None = None


class PasswordChangeIn(StrictModel):
    current_password: Password
    new_password: Password


class AdminUserPatch(StrictModel):
    role: UserRole | None = None
    is_active: bool | None = None
