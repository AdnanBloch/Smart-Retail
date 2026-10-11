"""Order vocabularies (RFC 0005 §3.1.1.2). The only place these values live (R8)."""

from enum import StrEnum

from smartretail_common.enums import DomainEnum


class OrderStatus(DomainEnum):
    PENDING = "pending"
    AWAITING_PAYMENT = "awaiting_payment"
    CONFIRMED = "confirmed"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"
    FAILED = "failed"
    REFUNDING = "refunding"
    REFUNDED = "refunded"


class PaymentStatus(DomainEnum):
    NONE = "none"
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    REFUNDED = "refunded"


class ShipmentStatus(DomainEnum):
    NONE = "none"
    CREATED = "created"
    SHIPPED = "shipped"
    IN_TRANSIT = "in_transit"
    DELIVERED = "delivered"


class OrderFailureReason(StrEnum):
    """ORD-5. Plain text in the database; a closed set in code so metric labels stay bounded."""

    INSUFFICIENT_STOCK = "INSUFFICIENT_STOCK"
    INVENTORY_UNAVAILABLE = "INVENTORY_UNAVAILABLE"
    PAYMENT_DECLINED = "PAYMENT_DECLINED"
    PAYMENT_TIMEOUT = "PAYMENT_TIMEOUT"
    RESERVATION_EXPIRED = "RESERVATION_EXPIRED"
    COMMIT_FAILED = "COMMIT_FAILED"
    FULFILMENT_FAILED = "FULFILMENT_FAILED"
    REFUND_FAILED = "REFUND_FAILED"
    OPERATOR_TERMINATED = "OPERATOR_TERMINATED"


class OrderEvent(StrEnum):
    """Event types written to the outbox (RFC 0005 §3.1.2.3)."""

    CREATED = "order.created"
    CONFIRMED = "order.confirmed"
    FAILED = "order.failed"
    CANCELLED = "order.cancelled"
    PAYMENT_SUCCEEDED = "payment.succeeded"
    PAYMENT_FAILED = "payment.failed"
    SHIPMENT_STATUS_CHANGED = "shipment.status_changed"


class RefundOutcome(StrEnum):
    REFUNDED = "refunded"
    NOTHING_CAPTURED = "nothing_captured"


class WorkflowStage(StrEnum):
    """``OrderWorkflow`` stages, as ``get_state`` reports them."""

    STARTING = "starting"
    RESERVING = "reserving"
    CHARGING = "charging"
    COMMITTING = "committing"
    SHIPPING = "shipping"
    CONFIRMING = "confirming"
    NOTIFYING = "notifying"
    DONE = "done"
    COMPENSATING = "compensating"
    FAILED = "failed"
    CANCELLED = "cancelled"


# Cancellation is honoured only before the cancellable point: the commit.
CANCELLABLE_STAGES = frozenset(
    {WorkflowStage.STARTING, WorkflowStage.RESERVING, WorkflowStage.CHARGING}
)
