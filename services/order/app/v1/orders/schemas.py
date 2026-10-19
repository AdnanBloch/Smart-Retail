"""``/api/v1/orders`` request and response models (RFC 0005 §3.1.2.1)."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import Field

from app.core.schemas import OutModel, StrictModel
from app.v1.orders.enums import OrderStatus, PaymentStatus, ShipmentStatus
from app.v1.orders.models import Order, OrderItem, OrderStatusHistory
from app.v1.orders.repo import OrderDetail
from app.v1.orders.snapshot import LineRequest

Text100 = Annotated[str, Field(min_length=1, max_length=100)]
Text200 = Annotated[str, Field(min_length=1, max_length=200)]


class LineIn(StrictModel):
    product_id: uuid.UUID
    variant_id: uuid.UUID
    quantity: Annotated[int, Field(gt=0)]

    def to_request(self) -> LineRequest:
        return LineRequest(self.product_id, self.variant_id, self.quantity)


class ShippingAddress(StrictModel):
    name: Text200
    line1: Text200
    line2: Annotated[str, Field(max_length=200)] | None = None
    city: Text100
    region: Annotated[str, Field(max_length=100)] | None = None
    postal_code: Annotated[str, Field(min_length=1, max_length=20)]
    country: Annotated[str, Field(pattern=r"^[A-Z]{2}$")]


class CheckoutIn(StrictModel):
    """No price field exists: amounts come from the catalog snapshot (I9)."""

    items: Annotated[list[LineIn], Field(min_length=1)]
    shipping_address: ShippingAddress
    # An opaque provider token, never card data (ORD-Q5).
    payment_method: Annotated[str, Field(min_length=1, max_length=64)]


class CancelIn(StrictModel):
    reason: Annotated[str, Field(max_length=500)] | None = None


class ShipmentStatusIn(StrictModel):
    status: ShipmentStatus = Field(description="shipped | in_transit | delivered")
    tracking_number: Annotated[str, Field(max_length=64)] | None = None


class OrderSummary(OutModel):
    order_id: uuid.UUID = Field(validation_alias="id")
    order_number: str
    status: OrderStatus
    total: Decimal
    currency: str
    placed_at: datetime
    payment_status: PaymentStatus
    shipment_status: ShipmentStatus


class ItemOut(OutModel):
    product_id: uuid.UUID
    variant_id: uuid.UUID
    merchant_id: uuid.UUID
    product_name: str
    sku: str
    variant_options: dict[str, Any]
    unit_price: Decimal
    quantity: int
    line_total: Decimal


class PaymentOut(OutModel):
    status: PaymentStatus
    reference: str | None


class ShipmentOut(OutModel):
    status: ShipmentStatus
    reference: str | None
    tracking_number: str | None


class TimelineEntry(OutModel):
    from_: str | None = Field(serialization_alias="from")
    to: str
    reason: str | None
    actor: str
    at: datetime

    @classmethod
    def from_row(cls, row: OrderStatusHistory) -> "TimelineEntry":
        return cls(
            from_=row.from_status,
            to=row.to_status,
            reason=row.reason,
            actor=row.actor,
            at=row.created_at,
        )


class OrderOut(OutModel):
    order_id: uuid.UUID
    order_number: str
    customer_id: uuid.UUID
    status: OrderStatus
    failure_reason: str | None
    currency: str
    subtotal: Decimal
    shipping_fee: Decimal
    total: Decimal
    shipping_address: dict[str, Any]
    items: list[ItemOut]
    payment: PaymentOut
    shipment: ShipmentOut
    placed_at: datetime
    confirmed_at: datetime | None
    notified_at: datetime | None
    version: int
    timeline: list[TimelineEntry]

    @classmethod
    def from_detail(cls, detail: OrderDetail) -> "OrderOut":
        order = detail.order
        return cls(
            order_id=order.id,
            order_number=order.order_number,
            customer_id=order.customer_id,
            status=OrderStatus(order.status),
            failure_reason=order.failure_reason,
            currency=order.currency,
            subtotal=order.subtotal,
            shipping_fee=order.shipping_fee,
            total=order.total,
            shipping_address=order.shipping_address,
            items=[ItemOut.model_validate(i) for i in order.items],
            payment=PaymentOut(
                status=PaymentStatus(order.payment_status), reference=order.payment_reference
            ),
            shipment=ShipmentOut(
                status=ShipmentStatus(order.shipment_status),
                reference=order.shipment_reference,
                tracking_number=order.tracking_number,
            ),
            placed_at=order.placed_at,
            confirmed_at=order.confirmed_at,
            notified_at=order.notified_at,
            version=order.version,
            timeline=[TimelineEntry.from_row(r) for r in detail.timeline],
        )


class MerchantOrderOut(OutModel):
    """Only this merchant's lines: never another's, the order total, the payment
    reference or the shipping address (ORD-16)."""

    order_id: uuid.UUID
    order_number: str
    status: OrderStatus
    placed_at: datetime
    shipment_status: ShipmentStatus
    lines: list[ItemOut]
    merchant_subtotal: Decimal
    currency: str

    @classmethod
    def from_row(cls, row: tuple[Order, list[OrderItem]]) -> "MerchantOrderOut":
        order, lines = row
        return cls(
            order_id=order.id,
            order_number=order.order_number,
            status=OrderStatus(order.status),
            placed_at=order.placed_at,
            shipment_status=ShipmentStatus(order.shipment_status),
            lines=[ItemOut.model_validate(i) for i in lines],
            merchant_subtotal=sum((i.line_total for i in lines), Decimal("0.00")),
            currency=order.currency,
        )


class CancellationOut(OutModel):
    order_id: uuid.UUID
    status: OrderStatus
    cancellation: str = "requested"


class ShipmentUpdateOut(OutModel):
    order_id: uuid.UUID
    status: OrderStatus
    applied: bool
    shipment: ShipmentOut


class WorkflowLinks(OutModel):
    temporal_ui: str


class WorkflowOut(OutModel):
    workflow_id: str
    run_id: str | None
    execution_status: str | None
    stage: str | None
    failure_reason: str | None
    cancel_requested: bool | None
    started_at: datetime | None
    closed_at: datetime | None
    links: WorkflowLinks = Field(serialization_alias="_links")
