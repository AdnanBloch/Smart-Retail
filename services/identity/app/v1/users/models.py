"""``users`` table (RFC 0003 §3.1.3)."""

from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Index, Text, text
from sqlalchemy.dialects.postgresql import CITEXT
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.enums import UserRole
from smartretail_common.db import Base, Timestamps, UUIDPrimaryKey
from smartretail_common.db_types import DomainEnumText

if TYPE_CHECKING:
    from app.v1.merchants.models import MerchantProfile


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
        comment="DomainEnum UserRole — see services/identity/app/core/enums.py",
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    email_verified: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )

    merchant_profile: Mapped["MerchantProfile | None"] = relationship(
        back_populates="user", uselist=False, lazy="selectin"
    )
