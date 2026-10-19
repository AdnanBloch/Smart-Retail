"""`make dlq-replay TOPIC=<topic> [EVENT_ID=<id>]`: re-produce dead-lettered
messages to their source topic (RFC 0001 §3.1.1.6, 0003-Q8).

A deliberate action, never automatic: replaying a poison message in a loop
would never end. Reads ``<topic>.dlq`` as consumer group ``dlq-replay``, whose
committed offset is also what "DLQ depth" is measured against. Replaying an
event that did eventually succeed is harmless: the source consumer dedupes on
``event_id``. The ``dlq.*`` headers are dropped; the original key, value and
headers are kept.

Runs in the outbox-relay image.
"""

import argparse
import os
import sys

from confluent_kafka import Consumer, Producer

from smartretail_common.events.producer import PRODUCER_CONFIG
from smartretail_common.events.registry import DLQ_SUFFIX


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--topic", required=True, help="the source topic, e.g. smartretail.order.cancelled"
    )
    parser.add_argument("--event-id", help="replay only this event")
    parser.add_argument("--idle-seconds", type=float, default=5.0)
    args = parser.parse_args()
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "dlq-replay",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({**PRODUCER_CONFIG, "bootstrap.servers": bootstrap})
    consumer.subscribe([args.topic + DLQ_SUFFIX])
    replayed = skipped = 0
    try:
        while True:
            message = consumer.poll(args.idle_seconds)
            if message is None:
                break  # caught up
            if message.error():
                print(f"error: {message.error()}", file=sys.stderr)
                continue
            headers = [(k, v) for k, v in (message.headers() or []) if not k.startswith("dlq.")]
            event_id = dict(headers).get("event_id", b"").decode()
            if args.event_id and event_id != args.event_id:
                skipped += 1
            else:
                producer.produce(
                    args.topic, value=message.value(), key=message.key(), headers=headers
                )
                producer.flush(10)
                replayed += 1
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
    print(f"replayed {replayed}, skipped {skipped}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
