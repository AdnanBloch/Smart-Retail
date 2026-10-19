"""analytics' Celery app: ``celery -A app.core.celery_app worker -B -Q analytics.maintenance``.

One worker with Beat embedded (PLT-13) on the durable queue
``analytics.maintenance`` (ANL-D35: per-service queue, as every service runs
its own worker because every service's package is named ``app``). The one
scheduled task, ``rebuild_daily_rollups``, replaces documents rather than
incrementing them, so it is safe to run twice or on two workers.
"""

import os

from celery.schedules import crontab

from smartretail_common.celery_app import make_celery

# Read directly, not through Settings: importing the app (Beat, a test) must not
# need the Mongo settings that only a running task uses.
BROKER_URL = os.environ.get("CELERY_BROKER_URL", "amqp://guest:guest@rabbitmq:5672/smartretail")
RECONCILE_MINUTE = os.environ.get("RECONCILE_MINUTE", "7")

app = make_celery(
    "analytics",
    broker_url=BROKER_URL,
    include=["app.v1.metrics.tasks.maintenance"],
    # Seven days of envelopes re-folded per run; generous for local volume.
    time_limit=600,
    beat_schedule={
        # RFC 0008 §3.1.1.4: hourly reconciliation of the lookback window.
        "rebuild-daily-rollups": {
            "task": "smartretail.analytics.rebuild_daily_rollups",
            "schedule": crontab(minute=RECONCILE_MINUTE),
        },
    },
)
