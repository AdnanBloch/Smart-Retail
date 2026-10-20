"""The Celery app factory and task defaults (RFC 0001 §3.1.1.4, C7, A12).

No broker is needed: configuration is read off ``app.conf``, routing off the
app's router, and the retry contract is exercised with ``Task.apply`` (eager
execution runs the autoretry wrapper and its retries in-process)."""

import asyncio

import pytest
from celery.app import autoretry
from sqlalchemy.exc import OperationalError

from smartretail_common.celery_app import (
    EXCHANGE,
    TASK_DEFAULTS,
    make_celery,
    queue_name,
    run_async,
)
from smartretail_common.errors import DependencyUnavailable


def _app(**kwargs):
    return make_celery("inventory", broker_url="memory://", include=["app.tasks"], **kwargs)


# --- configuration -----------------------------------------------------------------


def test_delivery_is_at_least_once_one_task_at_a_time() -> None:
    conf = _app().conf
    # Acknowledged after the task finishes; a killed worker's task is redelivered.
    assert conf.task_acks_late is True
    assert conf.task_reject_on_worker_lost is True
    # A worker never hoards more than the task it is running.
    assert conf.worker_prefetch_multiplier == 1
    assert conf.broker_connection_retry_on_startup is True
    assert conf.timezone == "UTC"


def test_json_only_and_no_results_by_default() -> None:
    conf = _app().conf
    assert conf.task_serializer == "json"
    assert conf.accept_content == ["json"]
    assert conf.task_ignore_result is True
    assert conf.result_backend is None


def test_one_durable_queue_per_service_workload() -> None:
    app = _app(queue="sweeps")
    conf = app.conf
    assert queue_name("inventory", "sweeps") == "inventory.sweeps"
    assert conf.task_default_queue == "inventory.sweeps"
    assert conf.task_default_routing_key == "inventory.sweeps"
    assert conf.task_default_exchange == EXCHANGE.name == "celery"
    [queue] = conf.task_queues
    assert queue.name == "inventory.sweeps" and queue.routing_key == "inventory.sweeps"
    assert queue.durable is True
    assert queue.exchange.name == "celery" and queue.exchange.type == "direct"
    assert queue.exchange.durable is True
    assert app.main == "smartretail.inventory"
    assert "app.tasks" in app.conf.include


def test_the_default_queue_is_maintenance() -> None:
    assert _app().conf.task_default_queue == "inventory.maintenance"


def test_the_services_tasks_are_routed_to_its_queue_by_name() -> None:
    route = _app().amqp.router.route({}, "smartretail.inventory.release_expired")
    assert route["queue"].name == "inventory.maintenance"
    assert route["routing_key"] == "inventory.maintenance"


@pytest.mark.parametrize(("limit", "soft"), [(60, 50), (300, 290), (5, 1), (1, 1)])
def test_soft_time_limit_leaves_ten_seconds_but_never_below_one(limit: int, soft: int) -> None:
    conf = _app(time_limit=limit).conf
    assert conf.task_time_limit == limit
    assert conf.task_soft_time_limit == soft


def test_beat_schedule_is_passed_through_and_defaults_to_empty() -> None:
    assert _app().conf.beat_schedule == {}
    schedule = {"sweep": {"task": "smartretail.inventory.sweep", "schedule": 30.0}}
    assert _app(beat_schedule=schedule).conf.beat_schedule == schedule


def test_a_result_backend_turns_results_on_with_an_expiry() -> None:
    conf = _app(result_backend="redis://localhost/1").conf
    assert conf.result_backend == "redis://localhost/1"
    assert conf.task_ignore_result is False
    assert conf.result_serializer == "json"
    assert conf.result_expires == 3600


# --- the retry contract ------------------------------------------------------------


def test_task_defaults_retry_transient_failures_three_times_with_jittered_backoff() -> None:
    assert TASK_DEFAULTS["autoretry_for"] == (OSError, OperationalError, DependencyUnavailable)
    assert TASK_DEFAULTS["max_retries"] == 3
    assert TASK_DEFAULTS["retry_backoff"] is True
    assert TASK_DEFAULTS["retry_backoff_max"] == 60
    assert TASK_DEFAULTS["retry_jitter"] is True


def _task(app, body):
    return app.task(name="smartretail.inventory.flaky", **TASK_DEFAULTS)(body)


@pytest.mark.parametrize(
    "transient",
    [
        OSError("connection reset"),
        OperationalError("SELECT 1", {}, Exception("server closed the connection")),
        DependencyUnavailable("payments down"),
    ],
    ids=["os-error", "db-operational", "dependency-unavailable"],
)
def test_a_transient_failure_is_retried_until_it_succeeds(transient: Exception) -> None:
    calls: list[int] = []

    def body() -> str:
        calls.append(1)
        if len(calls) < 3:
            raise transient
        return "done"

    result = _task(_app(), body).apply()
    assert result.successful() and result.result == "done"
    assert len(calls) == 3


def test_a_persistent_failure_gives_up_after_three_retries() -> None:
    calls: list[int] = []

    def body() -> None:
        calls.append(1)
        raise OSError("still down")

    result = _task(_app(), body).apply()
    assert result.failed() and isinstance(result.result, OSError)
    assert len(calls) == 4  # the first run plus max_retries


def test_a_bug_is_not_retried() -> None:
    calls: list[int] = []

    def body() -> None:
        calls.append(1)
        raise ValueError("a bug, not an outage")

    result = _task(_app(), body).apply()
    assert result.failed() and isinstance(result.result, ValueError)
    assert len(calls) == 1


def test_retries_back_off_exponentially_capped_with_full_jitter(monkeypatch) -> None:
    seen: list[dict[str, object]] = []
    real = autoretry.get_exponential_backoff_interval

    def recording(**kwargs: object) -> int:
        seen.append(kwargs)
        return real(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(autoretry, "get_exponential_backoff_interval", recording)

    def body() -> None:
        raise OSError("down")

    _task(_app(), body).apply()
    assert [s["retries"] for s in seen] == [0, 1, 2, 3]
    assert all(s["factor"] == 1 and s["maximum"] == 60 for s in seen)
    assert all(s["full_jitter"] is True for s in seen)


# --- run_async ---------------------------------------------------------------------


def test_run_async_returns_the_coroutines_result() -> None:
    async def work() -> int:
        await asyncio.sleep(0)
        return 42

    assert run_async(work) == 42


def test_run_async_uses_a_fresh_loop_each_time_and_closes_it() -> None:
    loops: list[asyncio.AbstractEventLoop] = []

    async def work() -> None:
        loops.append(asyncio.get_running_loop())

    run_async(work)
    run_async(work)
    assert loops[0] is not loops[1]
    assert loops[0].is_closed() and loops[1].is_closed()


def test_run_async_propagates_the_failure_for_autoretry() -> None:
    async def work() -> None:
        raise DependencyUnavailable("redis down")

    with pytest.raises(DependencyUnavailable):
        run_async(work)


def test_an_async_task_body_is_retried_like_a_sync_one() -> None:
    calls: list[int] = []

    async def work() -> str:
        calls.append(1)
        if len(calls) == 1:
            raise OSError("first attempt fails")
        return "ok"

    def body() -> str:
        return run_async(work)

    result = _task(_app(), body).apply()
    assert result.result == "ok" and len(calls) == 2
