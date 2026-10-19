"""`make topics`: create every topic and DLQ, set BACKWARD, register the schemas
(RFC 0001 §3.1.1.6). Idempotent: an existing topic or schema version is left as is.

    python scripts/topics.py                 create topics, register schemas
    python scripts/topics.py --schemas-only  register schemas only (no Kafka admin calls)
    python scripts/topics.py --check         only check every schema is BACKWARD-compatible

CI registers ``main``'s schemas with ``--schemas-only`` from a worktree of
``main``, then runs ``--check`` with the branch's: an incompatible change fails
the build instead of a consumer at runtime (C5).

Runs in the outbox-relay image (it has ``smartretail-common[events]``).
"""

import argparse
import asyncio
import os
import sys

from confluent_kafka.admin import AdminClient, NewTopic

from smartretail_common.events import registry
from smartretail_common.events.schema_registry import HttpSchemaRegistry


def create_topics(bootstrap: str) -> None:
    cat = registry.catalogue()
    rf = int(os.environ.get("KAFKA_REPLICATION_FACTOR", cat.replication_factor))
    wanted: list[NewTopic] = []
    for spec in cat.events:
        config = {"cleanup.policy": spec.cleanup}
        if spec.cleanup == "compact":
            config |= {"min.cleanable.dirty.ratio": "0.5", "segment.ms": str(86_400_000)}
        else:
            config["retention.ms"] = str(cat.retention_ms)
        wanted.append(NewTopic(spec.topic, cat.partitions, rf, config=config))
        wanted.append(
            NewTopic(
                spec.dlq_topic,
                cat.partitions,
                rf,
                config={"cleanup.policy": "delete", "retention.ms": str(cat.dlq_retention_ms)},
            )
        )
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    missing = [t for t in wanted if t.topic not in existing]
    # librdkafka rejects an empty list, so a re-run with every topic present skips the call.
    for topic, future in (admin.create_topics(missing) if missing else {}).items():
        future.result()
        print(f"created {topic}")
    print(f"{len(wanted) - len(missing)} topics already present, {len(missing)} created")


async def schemas(url: str, *, check_only: bool) -> bool:
    client = HttpSchemaRegistry(url)
    ok = True
    try:
        if not check_only:
            await client.set_compatibility("BACKWARD")
        for spec in registry.catalogue().events:
            if spec.schema_file is None:
                continue
            schema = registry.message_schema_str(spec.event_type)
            if not await client.is_compatible(spec.subject, schema):
                print(f"INCOMPATIBLE {spec.subject}", file=sys.stderr)
                ok = False
                continue
            if not check_only:
                schema_id = await client.register(spec.subject, schema)
                print(f"registered {spec.subject} id={schema_id}")
    finally:
        await client.aclose()
    return ok


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="compatibility check only")
    mode.add_argument("--schemas-only", action="store_true", help="register, no topics")
    args = parser.parse_args()
    if not (args.check or args.schemas_only):
        create_topics(os.environ["KAFKA_BOOTSTRAP_SERVERS"])
    ok = asyncio.run(schemas(os.environ["SCHEMA_REGISTRY_URL"], check_only=args.check))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
