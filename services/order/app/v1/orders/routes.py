"""``/api/v1/orders`` (RFC 0005 §3.1.2.1).

``POST /orders`` returns ``202``: the order exists and is addressable, its
lifecycle is asynchronous (B9). ``/merchant`` is declared before ``/{id}``,
and ``{id}`` is a UUID, so ``merchant`` is never parsed as an id.
"""

import uuid
from datetime import datetime

from fastapi import APIRouter, Body, Depends, Header, Query, Request, status
from fastapi.responses import JSONResponse

from app.core.dependencies import (
    require_admin,
    require_customer,
    require_customer_or_admin,
    require_merchant,
    user_token,
)
from app.core.errors import OrderNotFound, TemporalUnavailable
from app.v1.orders import access
from app.v1.orders.clients.catalog import CatalogClient
from app.v1.orders.dependencies import (
    catalog_client,
    idempotency_repo,
    order_repo,
    order_workflows,
    shipment_repo,
)
from app.v1.orders.enums import OrderStatus
from app.v1.orders.idempotency_repo import IdempotencyRepo
from app.v1.orders.repo import Checkout, OrderFilters, OrderRepo
from app.v1.orders.schemas import (
    CancelIn,
    CancellationOut,
    CheckoutIn,
    MerchantOrderOut,
    OrderOut,
    OrderSummary,
    ShipmentOut,
    ShipmentStatusIn,
    ShipmentUpdateOut,
    WorkflowLinks,
    WorkflowOut,
)
from app.v1.orders.shipment_repo import ShipmentRepo
from app.v1.orders.workflows.client import TemporalOrderWorkflows
from smartretail_common.auth import Principal
from smartretail_common.idempotency import HEADER, REPLAYED_HEADER, canonical_key, request_hash
from smartretail_common.pagination import Page, PageParams, page_params

router = APIRouter()

TEMPORAL_UI = "http://localhost:8080/namespaces/smartretail/workflows/{workflow_id}"


def order_filters(
    status_: OrderStatus | None = Query(None, alias="status"),
    since: datetime | None = Query(None, alias="from"),
    until: datetime | None = Query(None, alias="to"),
    customer_id: uuid.UUID | None = Query(None, description="Admins only"),
) -> OrderFilters:
    return OrderFilters(status_, since, until, customer_id)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        202: {"description": "Accepted; a replay adds `Idempotency-Replayed: true`"},
        409: {"description": "Same key in flight (`Retry-After: 1`)"},
        422: {"description": "Validation, unpublished product, mixed currency, key reused"},
        503: {"description": "Catalog unavailable; the claim is released"},
    },
)
async def place_order(
    request: Request,
    body: CheckoutIn = Body(...),
    idempotency_key: str | None = Header(None, alias=HEADER),
    client_id: str | None = Header(None, alias="X-Client-Id", max_length=128),
    principal: Principal = Depends(require_customer),
    token: str = Depends(user_token),
    repo: OrderRepo = Depends(order_repo),
    idempotency: IdempotencyRepo = Depends(idempotency_repo),
    catalog: CatalogClient = Depends(catalog_client),
    workflows: TemporalOrderWorkflows = Depends(order_workflows),
) -> JSONResponse:
    key = canonical_key(idempotency_key)
    placement = await repo.place(
        principal,
        Checkout(
            lines=[line.to_request() for line in body.items],
            shipping_address=body.shipping_address.model_dump(),
            payment_method=body.payment_method,
            origin_client_id=client_id,
        ),
        key=key,
        request_hash=request_hash(body.model_dump(mode="json")),
        token=token,
        idempotency=idempotency,
        catalog=catalog,
        workflows=workflows,
    )
    reply = placement.reply
    headers: dict[str, str] = {}
    if reply.status == status.HTTP_202_ACCEPTED:
        headers["Location"] = reply.body["_links"]["self"]
    if placement.replayed:
        headers[REPLAYED_HEADER] = "true"
    return JSONResponse(
        reply.body,
        status_code=reply.status,
        headers=headers,
        media_type="application/json" if reply.status < 400 else "application/problem+json",
    )


@router.get("", response_model=Page[OrderSummary])
async def list_orders(
    filters: OrderFilters = Depends(order_filters),
    page: PageParams = Depends(page_params),
    principal: Principal = Depends(require_customer_or_admin),
    repo: OrderRepo = Depends(order_repo),
) -> Page[OrderSummary]:
    """The caller's own orders; an admin sees all and may filter by ``customer_id``."""
    orders = await repo.list_orders(principal, filters, page)
    return orders.to_page(OrderSummary.model_validate)


@router.get("/merchant", response_model=Page[MerchantOrderOut])
async def merchant_orders(
    filters: OrderFilters = Depends(order_filters),
    page: PageParams = Depends(page_params),
    principal: Principal = Depends(require_merchant),
    repo: OrderRepo = Depends(order_repo),
) -> Page[MerchantOrderOut]:
    rows = await repo.list_for_merchant(principal, filters, page)
    return rows.to_page(MerchantOrderOut.from_row)


@router.get("/{order_id}", response_model=OrderOut)
async def get_order(
    order_id: uuid.UUID,
    principal: Principal = Depends(require_customer_or_admin),
    repo: OrderRepo = Depends(order_repo),
) -> OrderOut:
    return OrderOut.from_detail(await repo.detail(principal, order_id))


@router.post(
    "/{order_id}/cancel", status_code=status.HTTP_202_ACCEPTED, response_model=CancellationOut
)
async def cancel_order(
    order_id: uuid.UUID,
    body: CancelIn | None = None,
    principal: Principal = Depends(require_customer_or_admin),
    repo: OrderRepo = Depends(order_repo),
    workflows: TemporalOrderWorkflows = Depends(order_workflows),
) -> CancellationOut:
    """Signals the workflow; the cancellation completes asynchronously."""
    order = await repo.request_cancellation(
        principal, order_id, body.reason if body else None, workflows
    )
    return CancellationOut(order_id=order.id, status=OrderStatus(order.status))


@router.post("/{order_id}/shipment/status", response_model=ShipmentUpdateOut)
async def simulate_carrier(
    order_id: uuid.UUID,
    body: ShipmentStatusIn,
    principal: Principal = Depends(require_admin),
    repo: ShipmentRepo = Depends(shipment_repo),
) -> ShipmentUpdateOut:
    """Test-only carrier simulation; the signed webhook replaces it in Milestone 3."""
    update = await repo.apply_carrier_update(
        order_id,
        body.status,
        actor=access.actor(principal),
        tracking_number=body.tracking_number,
    )
    order = update.order
    return ShipmentUpdateOut(
        order_id=order.id,
        status=OrderStatus(order.status),
        applied=update.applied,
        shipment=ShipmentOut(
            status=order.shipment_status,
            reference=order.shipment_reference,
            tracking_number=order.tracking_number,
        ),
    )


@router.get("/{order_id}/workflow", response_model=WorkflowOut)
async def order_workflow(
    order_id: uuid.UUID,
    principal: Principal = Depends(require_admin),
    repo: OrderRepo = Depends(order_repo),
    workflows: TemporalOrderWorkflows = Depends(order_workflows),
) -> WorkflowOut:
    """Diagnosis without opening the Temporal UI."""
    order = await repo.get_any(order_id)
    if order.workflow_id is None:  # pragma: no cover - every order is given one
        raise OrderNotFound()
    try:
        described = await workflows.describe(order.workflow_id)
    except Exception as exc:
        raise TemporalUnavailable("Workflow state is unavailable; retry shortly") from exc
    return WorkflowOut(
        workflow_id=order.workflow_id,
        links=WorkflowLinks(temporal_ui=TEMPORAL_UI.format(workflow_id=order.workflow_id)),
        **described,
    )
