"""The API side of Temporal for order: start, query, signal, describe.

Implements ``OrderRepo``'s ``OrderWorkflows``. Every call is bounded, so an
endpoint answers 503 promptly when Temporal is down.
"""

import asyncio
import uuid
from datetime import timedelta
from typing import Any

from temporalio.client import WorkflowHandle
from temporalio.common import WorkflowIDReusePolicy

from app.core.config import Settings
from app.workflows import types as t
from smartretail_common.temporal import TemporalClientFactory

CALL_TIMEOUT_SECONDS = 5


class TemporalOrderWorkflows:
    def __init__(self, factory: TemporalClientFactory, settings: Settings) -> None:
        self.factory = factory
        self.settings = settings

    async def _handle(self, workflow_id: str) -> WorkflowHandle[Any, Any]:
        client = await asyncio.wait_for(self.factory.get(), CALL_TIMEOUT_SECONDS)
        return client.get_workflow_handle(workflow_id)

    async def start(
        self,
        *,
        order_id: uuid.UUID,
        workflow_id: str,
        correlation_id: str | None,
        payment_method: str,
    ) -> str:
        handle = await self.start_handle(
            order_id=order_id,
            workflow_id=workflow_id,
            correlation_id=correlation_id,
            payment_method=payment_method,
        )
        return handle.result_run_id or ""

    async def start_handle(
        self,
        *,
        order_id: uuid.UUID,
        workflow_id: str,
        correlation_id: str | None,
        payment_method: str,
    ) -> WorkflowHandle[Any, Any]:
        client = await asyncio.wait_for(self.factory.get(), CALL_TIMEOUT_SECONDS)
        return await asyncio.wait_for(
            client.start_workflow(
                t.WORKFLOW_NAME,
                t.OrderInput(str(order_id), correlation_id, payment_method),
                id=workflow_id,
                task_queue=self.settings.order_task_queue,
                execution_timeout=timedelta(
                    seconds=self.settings.order_workflow_execution_timeout_seconds
                ),
                # Second line of defence behind the idempotency key (Q3).
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            ),
            CALL_TIMEOUT_SECONDS,
        )

    async def state(self, workflow_id: str) -> dict[str, Any]:
        handle = await self._handle(workflow_id)
        result: dict[str, Any] = await asyncio.wait_for(
            handle.query(t.GET_STATE_QUERY), CALL_TIMEOUT_SECONDS
        )
        return result

    async def request_cancellation(
        self, workflow_id: str, *, actor: str, reason: str | None
    ) -> None:
        handle = await self._handle(workflow_id)
        await asyncio.wait_for(
            handle.signal(t.CANCEL_SIGNAL, t.CancelRequest(actor, reason)), CALL_TIMEOUT_SECONDS
        )

    async def describe(self, workflow_id: str) -> dict[str, Any]:
        """``GET /orders/{id}/workflow``: ``describe`` combined with ``get_state``."""
        handle = await self._handle(workflow_id)
        description = await asyncio.wait_for(handle.describe(), CALL_TIMEOUT_SECONDS)
        try:
            state = await self.state(workflow_id)
        except Exception:  # a closed workflow with no worker cannot answer a query
            state = {}
        return {
            "run_id": description.run_id,
            "execution_status": description.status.name if description.status else None,
            "started_at": description.start_time,
            "closed_at": description.close_time,
            "stage": state.get("stage"),
            "failure_reason": state.get("failure_reason"),
            "cancel_requested": state.get("cancel_requested"),
        }
