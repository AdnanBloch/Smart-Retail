"""The API side of Temporal for catalog: start a publish, ask whether one runs,
read its current step. Implements ``PublishingRepo``'s ``PublishStarter``."""

import asyncio
from datetime import timedelta
from typing import Any

from temporalio.client import WorkflowExecutionStatus, WorkflowHandle
from temporalio.common import WorkflowIDReusePolicy
from temporalio.service import RPCError, RPCStatusCode

from app.core.config import Settings
from app.workflows import types as t
from smartretail_common.temporal import TemporalClientFactory

# The endpoint must answer promptly when Temporal is down (CAT-9 returns 503).
CALL_TIMEOUT_SECONDS = 5


class TemporalPublishStarter:
    def __init__(self, factory: TemporalClientFactory, settings: Settings) -> None:
        self.factory = factory
        self.settings = settings

    async def start(
        self, *, product_id: Any, workflow_id: str, actor_id: str, correlation_id: str | None
    ) -> None:
        await self.start_handle(
            product_id=product_id,
            workflow_id=workflow_id,
            actor_id=actor_id,
            correlation_id=correlation_id,
        )

    async def start_handle(
        self, *, product_id: Any, workflow_id: str, actor_id: str, correlation_id: str | None
    ) -> WorkflowHandle[Any, Any]:
        client = await asyncio.wait_for(self.factory.get(), CALL_TIMEOUT_SECONDS)
        return await asyncio.wait_for(
            client.start_workflow(
                t.WORKFLOW_NAME,
                t.PublishInput(str(product_id), workflow_id, actor_id, correlation_id),
                id=workflow_id,
                task_queue=self.settings.publish_task_queue,
                execution_timeout=timedelta(
                    seconds=self.settings.publish_workflow_execution_timeout_seconds
                ),
                # A second line of defence behind the status guard (CAT-7).
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            ),
            CALL_TIMEOUT_SECONDS,
        )

    async def is_running(self, workflow_id: str) -> bool:
        client = await asyncio.wait_for(self.factory.get(), CALL_TIMEOUT_SECONDS)
        try:
            description = await asyncio.wait_for(
                client.get_workflow_handle(workflow_id).describe(), CALL_TIMEOUT_SECONDS
            )
        except RPCError as exc:
            if exc.status == RPCStatusCode.NOT_FOUND:
                return False
            raise
        return description.status == WorkflowExecutionStatus.RUNNING

    async def current_step(self, workflow_id: str) -> dict[str, Any] | None:
        client = await asyncio.wait_for(self.factory.get(), CALL_TIMEOUT_SECONDS)
        result: dict[str, Any] = await asyncio.wait_for(
            client.get_workflow_handle(workflow_id).query(t.CURRENT_STEP_QUERY),
            CALL_TIMEOUT_SECONDS,
        )
        return result
