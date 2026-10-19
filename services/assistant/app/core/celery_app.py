"""assistant's Celery app: ``celery -A app.core.celery_app worker -B -Q assistant.bulk``.

One worker for assistant (``assistant-celery``, RFC 0001 PLT-12: the only
Celery worker holding the provider keys), Beat embedded, on the durable queue
``assistant.bulk`` (AST-D37): bulk generation and reindex are slow, so they
never share a queue with another service's work. Every task is safe to run twice.
"""

import os

from celery.schedules import crontab

# Register every ORM model: a worker reaches only some through its own imports.
from app import models as _models  # noqa: F401
from smartretail_common.celery_app import make_celery

# Read directly, not through Settings: importing the app (Beat, a test) must not
# need the database settings that only a running task uses.
BROKER_URL = os.environ.get("CELERY_BROKER_URL", "amqp://guest:guest@rabbitmq:5672/smartretail")

TASK_PREFIX = "smartretail.assistant."
RUN_REINDEX = TASK_PREFIX + "run_reindex"
GENERATE_BULK = TASK_PREFIX + "generate_bulk_content"

app = make_celery(
    "assistant",
    broker_url=BROKER_URL,
    queue="bulk",
    include=[
        "app.core.tasks.maintenance",
        "app.v1.search.tasks.maintenance",
        "app.v1.chat.tasks.maintenance",
        "app.v1.generations.tasks.bulk",
    ],
    # A 200-product bulk job runs well past the 60 s platform default.
    time_limit=3600,
    beat_schedule={
        # PLT-17: 30-day retention of the indexer's dedupe rows.
        "sweep-processed-events": {
            "task": TASK_PREFIX + "sweep_processed_events",
            "schedule": crontab(hour=3, minute=10),
        },
        # Q5 (0005): anonymous conversations live 24 hours.
        "sweep-anonymous-conversations": {
            "task": TASK_PREFIX + "sweep_anonymous_conversations",
            "schedule": crontab(minute=20),
        },
        # Q10 (0004): query_log.user_id kept 90 days.
        "anonymise-query-log": {
            "task": TASK_PREFIX + "anonymise_query_log",
            "schedule": crontab(hour=3, minute=30),
        },
    },
)


def enqueue(task_name: str, *args: object) -> None:
    """Sends a task by name (the API process never imports task bodies)."""
    app.send_task(task_name, args=list(args))
