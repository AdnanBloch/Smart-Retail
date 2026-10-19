"""``payment.succeeded`` / ``payment.failed``: consumed, nothing recorded (NTF-7).

The customer-visible outcome is ``order.confirmed`` / ``order.failed``;
notifying both would be the duplicate this service exists to prevent. A
handler (rather than none) makes the base record the event in
``processed_events``, so replay is uniform across every subscribed type
(NTF-D36).
"""

from typing import Any

from smartretail_common.events.consumer import Handler
from smartretail_common.events.envelope import Envelope

EVENT_TYPES = ("payment.succeeded", "payment.failed")


def handler() -> Handler:
    async def handle(event: Envelope, session: Any) -> None:
        return None

    return handle
