"""``orders``, ``order_items``, ``order_status_history`` and ``idempotency_keys``
(RFC 0005 §3.1.3). Persistence only."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    CHAR,
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    Sequence,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.v1.orders.enums import IdempotencyState, OrderStatus, PaymentStatus, ShipmentStatus
from smartretail_common.db import Base, CreatedAt, Timestamps, UUIDPrimaryKey
from smartretail_common.db_types import DomainEnumText

# ORD-3: the numeric part of `order_number`, never a key across a boundary.
ORDER_NUMBER_SEQ = Sequence("order_number_seq", metadata=Base.metadata)

_MACHINE = "transitions in app/v1/orders/state_machine.py"


def _comment(enum: str) -> str:
    return f"DomainEnum {enum} — see services/order/app/v1/orders/enums.py; {_MACHINE}"


class Order(UUIDPrimaryKey, Timestamps, Base):
    """The aggregate root, with payment and shipment as sub-state (RFC 0005 §3.1.1.1)."""

    __tablename__ = "orders"
    __table_args__ = (
        CheckConstraint("subtotal >= 0", name="subtotal_non_negative"),
        CheckConstraint("shipping_fee >= 0", name="shipping_fee_non_negative"),
        CheckConstraint("total >= 0", name="total_non_negative"),
        Index("ix_orders_customer_id_created_at", "customer_id", text("created_at DESC")),
        Index("ix_orders_status", "status"),
        Index("ix_orders_status_created_at", "status", "created_at"),
        Index("ix_orders_workflow_id", "workflow_id"),
        Index("ix_orders_workflow_run_id", "workflow_run_id"),
    )

    order_number: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    # A claim from the verified token, not a FK (S15).
    customer_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    # State machines never branch on a bare string: unknown values read as the
    # initial state and every edge out of it is refused (freeze loudly).
    status: Mapped[OrderStatus | str] = mapped_column(
        DomainEnumText(OrderStatus, on_unknown="fallback", fallback=OrderStatus.PENDING),
        nullable=False,
        comment=_comment("OrderStatus"),
    )
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    subtotal: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    shipping_fee: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    total: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[str] = mapped_column(CHAR(3), nullable=False)
    shipping_address: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    payment_status: Mapped[PaymentStatus | str] = mapped_column(
        DomainEnumText(PaymentStatus, on_unknown="fallback", fallback=PaymentStatus.NONE),
        nullable=False,
        comment=_comment("PaymentStatus"),
    )
    payment_reference: Mapped[str | None] = mapped_column(Text, nullable=True)
    shipment_status: Mapped[ShipmentStatus | str] = mapped_column(
        DomainEnumText(ShipmentStatus, on_unknown="fallback", fallback=ShipmentStatus.NONE),
        nullable=False,
        comment=_comment("ShipmentStatus"),
    )
    shipment_reference: Mapped[str | None] = mapped_column(Text, nullable=True)
    tracking_number: Mapped[str | None] = mapped_column(Text, nullable=True)
    workflow_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    workflow_run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))
    placed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # A confirmed or failed order with notified_at null never ran its notify step.
    notified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # The checkout request's X-Client-Id, for workflow-written events (C21).
    origin_client_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    # The opaque provider token from checkout (never card data), so an order
    # whose workflow did not start can be started later (ORD-9, ORD-Q5).
    payment_method: Mapped[str | None] = mapped_column(Text, nullable=True)

    items: Mapped[list["OrderItem"]] = relationship(
        lazy="raise", order_by="OrderItem.variant_id", passive_deletes=True
    )


class OrderItem(UUIDPrimaryKey, Base):
    """A snapshot at purchase (B8, I7): written once with its order, never updated."""

    __tablename__ = "order_items"
    __table_args__ = (
        CheckConstraint("quantity > 0", name="quantity_positive"),
        CheckConstraint("unit_price >= 0", name="unit_price_non_negative"),
        Index("ix_order_items_order_id", "order_id"),
        Index("ix_order_items_merchant_id", "merchant_id"),
    )

    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("orders.id", ondelete="CASCADE"), nullable=False
    )
    # Snapshots from catalog; no FK crosses a boundary (R2).
    product_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    variant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    merchant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    product_name: Mapped[str] = mapped_column(Text, nullable=False)
    sku: Mapped[str] = mapped_column(Text, nullable=False)
    variant_options: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    unit_price: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    line_total: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)


class OrderStatusHistory(Base):
    """Append-only timeline (S7). Plain ``text`` statuses: history must stay readable
    after a value is retired from the enum."""

    __tablename__ = "order_status_history"
    __table_args__ = (Index("ix_order_status_history_order_id", "order_id"),)
    __mapper_args__ = {"eager_defaults": True}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    order_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    from_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    to_status: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    # customer:<id> | workflow | admin:<id> | system — a composite, not a closed set.
    actor: Mapped[str] = mapped_column(Text, nullable=False)
    correlation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class IdempotencyKey(CreatedAt, Base):
    """An ``Idempotency-Key`` claim on ``POST /orders`` (ORD-7, ORD-8).

    The claim, replay and release rules are in ``idempotency_repo.py``.
    """

    __tablename__ = "idempotency_keys"
    __table_args__ = (Index("ix_idempotency_keys_expires_at", "expires_at"),)

    # PK (user_id, key): a key is scoped to its caller (S11, I1).
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    key: Mapped[str] = mapped_column(Text, primary_key=True)
    request_hash: Mapped[str] = mapped_column(Text, nullable=False)
    order_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    response_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    response_body: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    state: Mapped[IdempotencyState | str] = mapped_column(
        DomainEnumText(IdempotencyState),
        nullable=False,
        comment="DomainEnum IdempotencyState — see services/order/app/v1/orders/enums.py",
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
