"""``TemporalPublishStarter.is_running``: only NOT_FOUND means "not running"."""

import pytest
from temporalio.client import WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

from app.core.config import Settings
from app.v1.products.workflows.client import TemporalPublishStarter


class FakeHandle:
    def __init__(self, outcome) -> None:
        self.outcome = outcome

    async def describe(self):
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return type("Description", (), {"status": self.outcome})()


class FakeClient:
    def __init__(self, outcome) -> None:
        self.outcome = outcome

    def get_workflow_handle(self, workflow_id):
        return FakeHandle(self.outcome)


class FakeFactory:
    def __init__(self, outcome) -> None:
        self.client = FakeClient(outcome)

    async def get(self):
        return self.client


def starter(outcome) -> TemporalPublishStarter:
    return TemporalPublishStarter(FakeFactory(outcome), Settings(database_url="x"))


@pytest.mark.parametrize(
    ("outcome", "running"),
    [
        (WorkflowExecutionStatus.RUNNING, True),
        (WorkflowExecutionStatus.COMPLETED, False),
        (WorkflowExecutionStatus.TERMINATED, False),
        (RPCError("gone", RPCStatusCode.NOT_FOUND, b""), False),
    ],
)
async def test_is_running(outcome, running) -> None:
    assert await starter(outcome).is_running("wf") is running


async def test_other_rpc_errors_propagate() -> None:
    """Temporal unavailable must not read as "not running", or the accept guard
    would recover a live publish as a stuck row and start a second workflow."""
    with pytest.raises(RPCError):
        await starter(RPCError("down", RPCStatusCode.UNAVAILABLE, b"")).is_running("wf")
