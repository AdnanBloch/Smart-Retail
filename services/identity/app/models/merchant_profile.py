import uuid
from typing import TYPE_CHECKING, Annotated

from pydantic import Field
from sqlalchemy import ForeignKey, Text, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.enums import MerchantStatus
from app.models.response import OutModel, StrictModel
from smartretail_common.db import Base, Timestamps
from smartretail_common.db_types import DomainEnumText
from smartretail_common.enums import widen_for_response

if TYPE_CHECKING:
    from app.models.user import User


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
        comment="DomainEnum MerchantStatus — see services/identity/app/models/enums.py",
    )

    user: Mapped["User"] = relationship(back_populates="merchant_profile")


# --- request / response models ------------------------------------------


class MerchantOut(OutModel):
    merchant_id: uuid.UUID
    business_name: str
    slug: str
    description: str | None
    status: Annotated[MerchantStatus | str, widen_for_response(MerchantStatus)]


class MerchantMePatch(StrictModel):
    business_name: Annotated[str, Field(min_length=1, max_length=200)] | None = None
    description: Annotated[str, Field(max_length=2000)] | None = None
