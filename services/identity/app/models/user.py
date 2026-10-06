import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Annotated

from pydantic import EmailStr, Field, field_validator
from sqlalchemy import Boolean, Index, Text, text
from sqlalchemy.dialects.postgresql import CITEXT
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.enums import SELF_REGISTERABLE_ROLES, MerchantStatus, UserRole
from app.models.response import OutModel, StrictModel
from smartretail_common.db import Base, Timestamps, UUIDPrimaryKey
from smartretail_common.db_types import DomainEnumText
from smartretail_common.enums import widen_for_response

if TYPE_CHECKING:
    from app.models.merchant_profile import MerchantProfile


class User(UUIDPrimaryKey, Timestamps, Base):
    __tablename__ = "users"
    __table_args__ = (
        Index("ix_users_role", "role"),
        Index("ix_users_inactive", "is_active", postgresql_where=text("is_active = false")),
    )

    email: Mapped[str] = mapped_column(CITEXT, unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    full_name: Mapped[str] = mapped_column(Text, nullable=False)
    phone: Mapped[str | None] = mapped_column(Text, nullable=True)
    role: Mapped[UserRole | str] = mapped_column(
        DomainEnumText(UserRole),
        nullable=False,
        comment="DomainEnum UserRole — see services/identity/app/models/enums.py",
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    email_verified: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )

    merchant_profile: Mapped["MerchantProfile | None"] = relationship(
        back_populates="user", uselist=False, lazy="selectin"
    )


# --- request / response models ------------------------------------------

FullName = Annotated[str, Field(min_length=1, max_length=200)]
Phone = Annotated[str, Field(min_length=1, max_length=32)]
Password = Annotated[str, Field(min_length=1, max_length=1024)]


class RegisterIn(StrictModel):
    email: EmailStr
    password: Password
    full_name: FullName
    role: UserRole = UserRole.CUSTOMER
    business_name: Annotated[str, Field(min_length=1, max_length=200)] | None = None

    @field_validator("role")
    @classmethod
    def _not_admin(cls, role: UserRole) -> UserRole:
        if role not in SELF_REGISTERABLE_ROLES:
            raise ValueError("role must be customer or merchant")
        return role


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
