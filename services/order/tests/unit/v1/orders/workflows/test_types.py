"""What crosses the Temporal boundary (B2): identifiers and step results only,
round-tripped through the default data converter exactly as history stores them."""

import dataclasses

import pytest
from temporalio.converter import DataConverter

from app.v1.orders.workflows import types as t
from app.v1.orders.workflows.activities import OrderActivities
from app.v1.orders.workflows.order_workflow import OrderWorkflow


@pytest.mark.parametrize(
    "value",
    [
        t.OrderInput("o-1", "corr-1", "tok_ok"),
        t.OrderInput("o-1", None, "tok_ok"),
        t.StatusInput("o-1", "failed", failure_reason="PAYMENT_DECLINED", stage="charging"),
        t.ReservationsInput("o-1", ["r-1", "r-2"]),
        t.CancelRequest("customer:c1", "changed my mind"),
        t.WorkflowState("charging", None, True),
    ],
)
async def test_round_trips_through_the_data_converter(value) -> None:
    converter = DataConverter.default
    [payload] = await converter.encode([value])
    [decoded] = await converter.decode([payload], [type(value)])
    assert decoded == value


def test_defaults() -> None:
    status = t.StatusInput("o-1", "confirmed")
    assert (status.actor, status.failure_reason, status.stage, status.reason) == (
        "workflow",
        None,
        None,
        None,
    )
    assert t.ReservationsInput("o-1").reservation_ids == []
    assert t.CancelRequest("admin:a").reason is None


def test_no_order_state_travels() -> None:
    """B2: the workflow carries ``order_id``, never the order or its amounts."""
    fields = {f.name for f in dataclasses.fields(t.OrderInput)}
    assert fields == {"order_id", "correlation_id", "payment_method"}


def test_every_activity_name_is_registered_once() -> None:
    names = {
        getattr(t, n)
        for n in dir(t)
        if n.isupper() and n not in {"WORKFLOW_NAME", "GET_STATE_QUERY", "CANCEL_SIGNAL"}
    }
    registered = [
        fn.__temporal_activity_definition.name  # type: ignore[attr-defined]
        for fn in OrderActivities.all(OrderActivities.__new__(OrderActivities))
    ]
    assert sorted(registered) == sorted(names) and len(registered) == len(set(registered))
    assert OrderWorkflow.__temporal_workflow_definition.name == t.WORKFLOW_NAME  # type: ignore[attr-defined]
