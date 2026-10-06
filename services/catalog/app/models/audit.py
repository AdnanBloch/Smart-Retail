import uuid
from typing import Any

from sqlalchemy import BigInteger, Index
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.enums import ProductAuditAction
from smartretail_common.db import Base, CreatedAt
from smartretail_common.db_types import DomainEnumText


class ProductAuditLog(CreatedAt, Base):
    """Append-only; written by the service layer in the change's transaction (S7)."""

    __tablename__ = "product_audit_log"
    __table_args__ = (
        Index("ix_product_audit_log_product_id", "product_id"),
        Index("ix_product_audit_log_created_at", "created_at"),
    )

    # bigserial: the S1 exception for high-volume append-only logs.
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    product_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    actor_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    action: Mapped[ProductAuditAction | str] = mapped_column(
        DomainEnumText(ProductAuditAction),
        nullable=False,
        comment="DomainEnum ProductAuditAction — see services/catalog/app/models/enums.py",
    )
    changes: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
