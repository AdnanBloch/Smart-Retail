"""``merchant_profiles`` table (RFC 0003 §3.1.3)."""

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, Text, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.enums import MerchantStatus
from smartretail_common.db import Base, Timestamps
from smartretail_common.db_types import DomainEnumText

if TYPE_CHECKING:
    from app.v1.users.models import User


class MerchantProfile(Timestamps, Base):
    __tablename__ = "merchant_profiles"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    business_name: Mapped[str] = mapped_column(Text, nullable=False)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[MerchantStatus | str] = mapped_column(
        DomainEnumText(MerchantStatus),
        nullable=False,
        server_default=text("'pending'"),
        default=MerchantStatus.PENDING,
        comment="DomainEnum MerchantStatus — see services/identity/app/core/enums.py",
    )

    user: Mapped["User"] = relationship(back_populates="merchant_profile")
