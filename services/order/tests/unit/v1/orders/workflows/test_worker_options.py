"""ORD-D37: the worker's pull limits come from settings, never SDK defaults."""

from app.core.config import Settings
from app.v1.orders.workflows.worker import worker_options


def test_defaults_are_explicit() -> None:
    options = worker_options(Settings(database_url="x"))
    assert options == {"max_concurrent_activities": 50, "max_concurrent_workflow_tasks": 50}


def test_overrides_and_the_optional_rate_cap() -> None:
    settings = Settings(
        database_url="x",
        order_worker_max_concurrent_activities=8,
        order_worker_max_concurrent_workflow_tasks=4,
        order_worker_max_activities_per_second=25.0,
    )
    assert worker_options(settings) == {
        "max_concurrent_activities": 8,
        "max_concurrent_workflow_tasks": 4,
        "max_activities_per_second": 25.0,
    }
