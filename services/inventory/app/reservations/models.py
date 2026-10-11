"""``reservations`` (RFC 0006 §3.1.3)."""

import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.reservations.enums import ReservationStatus
from smartretail_common.db import Base, Timestamps, UUIDPrimaryKey
from smartretail_common.db_types import DomainEnumText


class Reservation(UUIDPrimaryKey, Timestamps, Base):
    __tablename__ = "reservations"
    __table_args__ = (
        # I3: one reservation per (order, variant) — the idempotency of reserve (B3).
        UniqueConstraint("order_id", "variant_id", name="uq_reservations_order_id_variant_id"),
        CheckConstraint("quantity > 0", name="quantity_positive"),
        Index("ix_reservations_variant_id", "variant_id"),
        # Exactly the sweeper's predicate (INV-13).
        Index(
            "ix_reservations_expires_at_held",
            "expires_at",
            postgresql_where=text("status = 'held'"),
        ),
    )

    # A cross-boundary id: no FK (R2).
    order_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    # A real FK inside inventory_db (INV-13).
    variant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("stock_items.variant_id"), nullable=False
    )
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[ReservationStatus | str] = mapped_column(
        # A state machine never branches on a bare string: unknown reads as held.
        DomainEnumText(ReservationStatus, on_unknown="fallback", fallback=ReservationStatus.HELD),
        nullable=False,
        comment=(
            "DomainEnum ReservationStatus — see services/inventory/app/reservations/enums.py; "
            "transitions in app/reservations/state_machine.py"
        ),
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
