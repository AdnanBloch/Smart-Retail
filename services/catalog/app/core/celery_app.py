"""catalog's Celery app: ``celery -A app.core.celery_app worker -Q catalog.media``.

RFC 0001 §3.1.1.4. One queue, ``catalog.media``, for image processing. Results return to the
waiting publishing activity over RabbitMQ (``rpc://``): only the sender reads
a result, which is exactly the rpc backend's model, so no result store is
needed (PLT-D43). No Beat: catalog has no scheduled job yet.
"""

import os

from smartretail_common.celery_app import make_celery

# Read directly, not through Settings: the activity imports this app to send tasks,
# and Beat/tests must not need the database settings.
BROKER_URL = os.environ.get("CELERY_BROKER_URL", "amqp://guest:guest@rabbitmq:5672/smartretail")

app = make_celery(
    "catalog",
    broker_url=BROKER_URL,
    queue="media",
    include=["app.v1.media.tasks.jobs"],
    time_limit=300,
    result_backend="rpc://",
)
