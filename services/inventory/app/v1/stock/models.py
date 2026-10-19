"""``stock_items`` and ``stock_movements`` (RFC 0006 §3.1.3)."""

import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Computed,
    DateTime,
    Index,
    Integer,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.v1.stock.enums import StockMovementReason
from smartretail_common.db import Base, Timestamps, UUIDPrimaryKey
from smartretail_common.db_types import DomainEnumText


class StockItem(UUIDPrimaryKey, Timestamps, Base):
    """One row per variant: the lock target. Quantities change only under a row lock."""

    __tablename__ = "stock_items"
    __table_args__ = (
        # I1: the last line of defence against oversell, even for an unlocked writer.
        CheckConstraint("on_hand >= reserved AND reserved >= 0", name="quantities"),
        Index("ix_stock_items_product_id", "product_id"),
        Index("ix_stock_items_merchant_id", "merchant_id"),
    )

    variant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, unique=True)
    # Snapshots from catalog, not foreign keys (R2, S15).
    product_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    merchant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    sku: Mapped[str] = mapped_column(Text, nullable=False)
    on_hand: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    reserved: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    # I2: generated, so no caller can define it differently.
    available: Mapped[int] = mapped_column(
        Integer, Computed("on_hand - reserved", persisted=True), nullable=False
    )
    low_stock_threshold: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("5")
    )
    version: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("1"), default=1
    )


class StockMovement(Base):
    """Append-only signed ledger (B6, I6, I9). Written only by ``ledger_repo``."""

    __tablename__ = "stock_movements"
    __table_args__ = (
        Index("ix_stock_movements_variant_id", "variant_id"),
        Index("ix_stock_movements_created_at", "created_at"),
        Index("ix_stock_movements_reference", "reference_type", "reference_id"),
    )
    __mapper_args__ = {"eager_defaults": True}

    # bigserial: the S1 exception for append-only logs.
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    variant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    delta_on_hand: Mapped[int] = mapped_column(Integer, nullable=False)
    delta_reserved: Mapped[int] = mapped_column(Integer, nullable=False)
    reason: Mapped[StockMovementReason | str] = mapped_column(
        DomainEnumText(StockMovementReason),
        nullable=False,
        comment="DomainEnum StockMovementReason — see services/inventory/app/v1/stock/enums.py",
    )
    reference_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    reference_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    actor: Mapped[str] = mapped_column(Text, nullable=False)
    correlation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Append-only: created_at and no updated_at (the S3 exception).
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
