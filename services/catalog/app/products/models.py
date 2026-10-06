"""``products`` and ``product_audit_log`` tables (RFC 0004 §3.1.3)."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    CHAR,
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.products.enums import ProductAuditAction, ProductStatus
from smartretail_common.db import Base, CreatedAt, Timestamps, UUIDPrimaryKey
from smartretail_common.db_types import DomainEnumText

if TYPE_CHECKING:
    from app.categories.models import Category
    from app.media.models import ProductMedia
    from app.variants.models import ProductVariant


class Product(UUIDPrimaryKey, Timestamps, Base):
    __tablename__ = "products"
    __table_args__ = (
        UniqueConstraint("merchant_id", "sku", name="uq_products_merchant_id_sku"),
        CheckConstraint("base_price >= 0", name="base_price_non_negative"),
        Index("ix_products_merchant_id", "merchant_id"),
        Index("ix_products_category_id", "category_id"),
        Index("ix_products_status", "status"),
        Index("ix_products_status_created_at", "status", text("created_at DESC")),
        Index("ix_products_attributes", "attributes", postgresql_using="gin"),
    )

    # A claim from identity, not a foreign key (RFC 0000 S15).
    merchant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, comment="identity user id; a claim, not a FK"
    )
    sku: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    category_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("categories.id", ondelete="RESTRICT"), nullable=True
    )
    base_price: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[str] = mapped_column(CHAR(3), nullable=False, server_default=text("'USD'"))
    status: Mapped[ProductStatus | str] = mapped_column(
        DomainEnumText(ProductStatus),
        nullable=False,
        server_default=text("'draft'"),
        default=ProductStatus.DRAFT,
        comment=(
            "DomainEnum ProductStatus — see services/catalog/app/products/enums.py; "
            "transitions in app/products/state_machine.py"
        ),
    )
    attributes: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb"), default=dict
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    version: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("1"), default=1
    )

    category: Mapped["Category | None"] = relationship(lazy="raise")
    variants: Mapped[list["ProductVariant"]] = relationship(
        lazy="raise", order_by="ProductVariant.created_at", passive_deletes=True
    )
    media: Mapped[list["ProductMedia"]] = relationship(
        lazy="raise",
        order_by="(ProductMedia.position, ProductMedia.created_at)",
        passive_deletes=True,
    )


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
        comment="DomainEnum ProductAuditAction — see services/catalog/app/products/enums.py",
    )
    changes: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
