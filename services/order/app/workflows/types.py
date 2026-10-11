"""What crosses the Temporal boundary for ``OrderWorkflow``.

Identifiers and step results only (B2): the workflow carries ``order_id``,
never the order. ``payment_method`` is an opaque provider token — never card
data — and travels only here because no column persists it (ORD-Q5).
"""

from dataclasses import dataclass, field

WORKFLOW_NAME = "OrderWorkflow"
GET_STATE_QUERY = "get_state"
CANCEL_SIGNAL = "request_cancellation"


@dataclass(frozen=True)
class OrderInput:
    order_id: str
    correlation_id: str | None
    payment_method: str


@dataclass(frozen=True)
class StatusInput:
    order_id: str
    status: str
    correlation_id: str | None = None
    failure_reason: str | None = None
    payment_status: str | None = None
    stage: str | None = None
    actor: str = "workflow"
    reason: str | None = None


@dataclass(frozen=True)
class ReservationsInput:
    order_id: str
    reservation_ids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class CancelRequest:
    actor: str
    reason: str | None = None


@dataclass(frozen=True)
class WorkflowState:
    stage: str
    failure_reason: str | None
    cancel_requested: bool


# Activity names (RFC 0005 §3.1.1.4).
RESERVE_INVENTORY = "reserve_inventory"
SET_ORDER_STATUS = "set_order_status"
CHARGE_PAYMENT = "charge_payment"
COMMIT_INVENTORY = "commit_inventory"
CREATE_SHIPMENT = "create_shipment"
NOTIFY = "notify"
RELEASE_INVENTORY = "release_inventory"
REFUND_PAYMENT = "refund_payment"
REVERSE_COMMIT_INVENTORY = "reverse_commit_inventory"
