"""The Celery app factory (RFC 0001 §3.1.1.4, C7, A12).

Celery runs one-shot jobs off the request path — sweeps, recoveries, media —
over RabbitMQ. It is point-to-point: each task goes to exactly one worker and
is deleted once acknowledged. Facts go through Kafka; multi-step processes
through Temporal.

The execution defaults are the platform's, not each service's:

* ``task_acks_late`` + ``task_reject_on_worker_lost``: a task is acknowledged
  after it finishes, so a killed worker's task is redelivered to another —
  which is why every task must be idempotent.
* ``worker_prefetch_multiplier=1``: a worker holds one task at a time, so a
  long task cannot hoard messages other workers could take.
* JSON, identifiers only as arguments (B2); results are not stored.
* One durable queue per service workload, ``<service>.<queue>`` — every
  service's package is named ``app``, so one worker cannot import two
  services' tasks, and each service runs its own worker (PLT-D36).
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from celery import Celery
from kombu import Exchange, Queue
from sqlalchemy.exc import OperationalError

from smartretail_common.errors import DependencyUnavailable

T = TypeVar("T")

EXCHANGE = Exchange("celery", type="direct", durable=True)

# Applied to every task with ``@app.task(**TASK_DEFAULTS)``: transient failures
# retry with jittered exponential backoff, at most three times (RFC 0001 §3.1.1.4).
TASK_DEFAULTS: dict[str, Any] = {
    "autoretry_for": (OSError, OperationalError, DependencyUnavailable),
    "retry_backoff": True,
    "retry_backoff_max": 60,
    "retry_jitter": True,
    "max_retries": 3,
}


def queue_name(service: str, queue: str) -> str:
    return f"{service}.{queue}"


def make_celery(
    service: str,
    *,
    broker_url: str,
    include: list[str],
    queue: str = "maintenance",
    beat_schedule: dict[str, dict[str, Any]] | None = None,
    time_limit: int = 60,
    result_backend: str | None = None,
) -> Celery:
    name = queue_name(service, queue)
    app = Celery(f"smartretail.{service}", broker=broker_url, include=include)
    app.conf.update(
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        worker_prefetch_multiplier=1,
        task_serializer="json",
        accept_content=["json"],
        task_ignore_result=True,
        task_default_queue=name,
        task_default_exchange=EXCHANGE.name,
        task_default_routing_key=name,
        task_queues=[Queue(name, EXCHANGE, routing_key=name, durable=True)],
        # Routed by name, so a task cannot land on the wrong queue by a caller's mistake.
        task_routes={f"smartretail.{service}.*": {"queue": name, "routing_key": name}},
        task_time_limit=time_limit,
        task_soft_time_limit=max(1, time_limit - 10),
        beat_schedule=beat_schedule or {},
        timezone="UTC",
        broker_connection_retry_on_startup=True,
    )
    if result_backend is not None:
        # Only for a task whose caller waits on it (process_media, PLT-D43).
        app.conf.update(
            result_backend=result_backend,
            task_ignore_result=False,
            result_serializer="json",
            result_expires=3600,
        )
    return app


def run_async(work: Callable[[], Awaitable[T]]) -> T:
    """Runs an async task body on a fresh event loop. Each task opens and
    disposes its own database engine inside ``work``, so no connection outlives
    the loop it was made on."""
    return asyncio.run(work())  # type: ignore[arg-type]
