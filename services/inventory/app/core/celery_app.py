"""inventory's Celery app: ``celery -A app.core.celery_app worker -B`` (RFC 0001 §3.1.1.4).

One worker for inventory, with Beat embedded (PLT-13), on the durable queue
``inventory.maintenance``. The schedule lives here so it is reviewable in one
diff. Every scheduled task is safe to run twice.
"""

import os

from celery.schedules import crontab

from smartretail_common.celery_app import make_celery

# Read directly, not through Settings: importing the app (Beat, a test) must not
# need the database settings that only a running task uses.
BROKER_URL = os.environ.get("CELERY_BROKER_URL", "amqp://guest:guest@rabbitmq:5672/smartretail")
SWEEP_EVERY_SECONDS = float(os.environ.get("INVENTORY_SWEEPER_INTERVAL_SECONDS", "60"))

app = make_celery(
    "inventory",
    broker_url=BROKER_URL,
    include=["app.v1.reservations.tasks.maintenance", "app.core.tasks"],
    beat_schedule={
        # B5: replaces the in-process sweeper loop (INV-10).
        "sweep-expired-reservations": {
            "task": "smartretail.inventory.sweep_expired_reservations",
            "schedule": SWEEP_EVERY_SECONDS,
        },
        # PLT-17: 30-day retention of the consumer's dedupe rows.
        "sweep-processed-events": {
            "task": "smartretail.inventory.sweep_processed_events",
            "schedule": crontab(hour=3, minute=0),
        },
    },
)
