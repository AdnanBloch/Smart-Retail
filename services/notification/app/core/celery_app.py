"""notification's Celery app: ``celery -A app.core.celery_app worker -B`` (RFC 0001 §3.1.1.4).

One worker with Beat embedded (PLT-13), on one durable queue,
``notification.notifications`` (NTF-D32): dispatch and the sweeps share it.
The schedule lives here so it is reviewable in one diff. Every task is safe
to run twice.
"""

import os

from celery.schedules import crontab

from smartretail_common.celery_app import make_celery

# Read directly, not through Settings: importing the app (Beat, a test, the
# consumer's enqueue) must not need settings only a running task uses.
BROKER_URL = os.environ.get("CELERY_BROKER_URL", "amqp://guest:guest@rabbitmq:5672/smartretail")
REDISPATCH_EVERY_SECONDS = float(os.environ.get("REDISPATCH_INTERVAL_SECONDS", "300"))

QUEUE = "notifications"
DISPATCH_TASK = "smartretail.notification.dispatch_notification"
REDISPATCH_TASK = "smartretail.notification.redispatch_undispatched_notifications"
SWEEP_DEDUPE_TASK = "smartretail.notification.sweep_processed_events"
SWEEP_CURSORS_TASK = "smartretail.notification.sweep_stale_stream_cursors"

app = make_celery(
    "notification",
    broker_url=BROKER_URL,
    queue=QUEUE,
    include=[
        "app.v1.notifications.tasks.dispatch",
        "app.v1.stream.tasks.maintenance",
        "app.core.tasks",
    ],
    beat_schedule={
        # NTF-6: re-enqueue dispatched-channel rows the consumer failed to enqueue.
        "redispatch-undispatched-notifications": {
            "task": REDISPATCH_TASK,
            "schedule": REDISPATCH_EVERY_SECONDS,
        },
        # PLT-17: 30-day retention of the consumer's dedupe rows.
        "sweep-processed-events": {
            "task": SWEEP_DEDUPE_TASK,
            "schedule": crontab(hour=3, minute=0),
        },
        # C22: idle cursors (90 days) and the 20-per-user cap.
        "sweep-stale-stream-cursors": {
            "task": SWEEP_CURSORS_TASK,
            "schedule": crontab(hour=3, minute=30),
        },
    },
)
