"""``ProductPublishingWorkflow`` — Part A §2 as a durable workflow (RFC 0004 §3.1.1.4).

Sequence and compensation only (B1): every rule is in ``PublishingRepo``,
reached through the activities. The steps are ordered so the only act that
makes the product visible — ``mark_published`` — is last and atomic; any
failure before it runs ``record_publish_failure`` and the product returns
to ``draft``. Nothing before step 5 needs undoing because nothing before it
is visible.

Determinism (RFC 0001 PLT-15): no I/O here, time from ``workflow.now()``,
the sandbox stays on. Retry policies and timeouts are CAT-8.
"""

import asyncio
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError

with workflow.unsafe.imports_passed_through():
    from app.v1.products.workflows import types as t


def _retry(initial: float, maximum: float, attempts: int, *non_retryable: str) -> RetryPolicy:
    return RetryPolicy(
        initial_interval=timedelta(seconds=initial),
        backoff_coefficient=2.0,
        maximum_interval=timedelta(seconds=maximum),
        maximum_attempts=attempts,
        non_retryable_error_types=list(non_retryable),
    )


# CAT-8: (start_to_close, schedule_to_close, heartbeat, retry)
STEP_OPTIONS: dict[str, dict[str, Any]] = {
    t.VALIDATE_PRODUCT: {
        "start_to_close_timeout": timedelta(seconds=10),
        "schedule_to_close_timeout": timedelta(minutes=1),
        "retry_policy": _retry(1, 10, 5, "ProductIncomplete", "PublishSuperseded"),
    },
    t.PROCESS_MEDIA: {
        "start_to_close_timeout": timedelta(minutes=5),
        "schedule_to_close_timeout": timedelta(minutes=20),
        "heartbeat_timeout": timedelta(seconds=30),
        "retry_policy": _retry(2, 30, 3, "MediaUndecodable", "PublishSuperseded"),
    },
    t.BUILD_SEARCH_DOCUMENT: {
        "start_to_close_timeout": timedelta(seconds=30),
        "schedule_to_close_timeout": timedelta(minutes=2),
        "retry_policy": _retry(1, 10, 5, "PublishSuperseded"),
    },
    t.SYNC_DOWNSTREAM: {
        "start_to_close_timeout": timedelta(seconds=30),
        "schedule_to_close_timeout": timedelta(minutes=5),
        "retry_policy": _retry(1, 30, 8),
    },
    t.MARK_PUBLISHED: {
        "start_to_close_timeout": timedelta(seconds=10),
        "schedule_to_close_timeout": timedelta(minutes=2),
        "retry_policy": _retry(1, 10, 10, "ProductIncomplete", "PublishSuperseded"),
    },
    # A compensation must eventually happen: unlimited attempts within 24 h.
    t.RECORD_PUBLISH_FAILURE: {
        "start_to_close_timeout": timedelta(seconds=10),
        "schedule_to_close_timeout": timedelta(hours=24),
        "retry_policy": _retry(1, 300, 0),
    },
}


def failure_reason(err: BaseException) -> str:
    """The innermost message: what the failing activity said, not Temporal's wrapper."""
    cause: BaseException | None = err
    while isinstance(cause, ActivityError) and cause.cause is not None:
        cause = cause.cause
    if isinstance(cause, ApplicationError):
        return f"{cause.type or 'Error'}: {cause.message}"
    return f"{type(cause).__name__}: {cause}"


@workflow.defn(name=t.WORKFLOW_NAME)
class ProductPublishingWorkflow:
    def __init__(self) -> None:
        self._step = "starting"
        self._started_at: str | None = None

    @workflow.query(name=t.CURRENT_STEP_QUERY)
    def current_step(self) -> dict[str, str | None]:
        return {"step": self._step, "started_at": self._started_at}

    async def _run_step(self, name: str, arg: object) -> Any:
        self._step = name
        self._started_at = workflow.now().isoformat()
        return await workflow.execute_activity(name, arg, **STEP_OPTIONS[name])

    @workflow.run
    async def run(self, inp: t.PublishInput) -> t.PublishOutcome:
        try:
            media_ids: list[str] = await self._run_step(t.VALIDATE_PRODUCT, inp)
            for media_id in media_ids:
                await self._run_step(t.PROCESS_MEDIA, t.MediaInput(inp, media_id))
            await self._run_step(t.BUILD_SEARCH_DOCUMENT, inp)
            await self._run_step(t.SYNC_DOWNSTREAM, inp)
            await self._run_step(t.MARK_PUBLISHED, inp)
        except ActivityError as err:
            if not isinstance(err.cause, CancelledError):
                return await self._compensate(inp, failure_reason(err))
            # The workflow was cancelled (e.g. the merchant was suspended) while an
            # activity ran: compensate, then end as cancelled.
            await self._compensate(inp, "cancelled")
            raise asyncio.CancelledError() from err
        except asyncio.CancelledError:
            # Cancelled between activities.
            await self._compensate(inp, "cancelled")
            raise
        self._step = "done"
        return t.PublishOutcome("published")

    async def _compensate(self, inp: t.PublishInput, reason: str) -> t.PublishOutcome:
        failed_step = self._step
        await workflow.execute_activity(
            t.RECORD_PUBLISH_FAILURE,
            t.FailureInput(inp, failed_step, reason),
            **STEP_OPTIONS[t.RECORD_PUBLISH_FAILURE],
        )
        self._step = "failed"
        return t.PublishOutcome("failed", failed_step=failed_step, reason=reason)
