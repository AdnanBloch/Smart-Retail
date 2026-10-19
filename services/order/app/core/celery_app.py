"""order's Celery app: ``celery -A app.core.celery_app worker -B`` (RFC 0001 §3.1.1.4).

One worker for order, with Beat embedded (PLT-13), on the durable queue
``order.maintenance``. Every scheduled task is safe to run twice.
"""

import os

from smartretail_common.celery_app import make_celery

# Read directly, not through Settings: importing the app must not need the
# database settings that only a running task uses.
BROKER_URL = os.environ.get("CELERY_BROKER_URL", "amqp://guest:guest@rabbitmq:5672/smartretail")

app = make_celery(
    "order",
    broker_url=BROKER_URL,
    include=["app.v1.orders.tasks.maintenance"],
    beat_schedule={
        # ORD-9: start the workflow of an order accepted while Temporal was down.
        "resume-unstarted-orders": {
            "task": "smartretail.order.resume_unstarted_orders",
            "schedule": 60.0,
        },
    },
)
