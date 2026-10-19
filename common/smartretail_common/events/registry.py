"""The event catalogue (RFC 0000 §3.1.2.3, PLT-26), loaded from ``catalogue.json``.

Each event type maps to one topic, one key and one Avro schema. The registered
schema of a topic is the envelope record with that topic's payload record as
its ``payload`` field, so the envelope is written once (``envelope.avsc``) and
every topic stays in step with it.
"""

import copy
import json
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

import fastavro

HERE = Path(__file__).resolve().parent
SCHEMAS = HERE / "schemas"
DLQ_SUFFIX = ".dlq"


class UnknownEvent(LookupError):
    """An event type or topic that is not in the catalogue, or has no schema yet."""


@dataclass(frozen=True)
class EventSpec:
    event_type: str
    topic: str
    key: str
    cleanup: str
    schema_file: str | None
    version: int = 1

    @property
    def dlq_topic(self) -> str:
        return self.topic + DLQ_SUFFIX

    @property
    def subject(self) -> str:
        # Confluent's TopicNameStrategy: one value schema per topic.
        return f"{self.topic}-value"


@dataclass(frozen=True)
class Catalogue:
    prefix: str
    partitions: int
    replication_factor: int
    retention_ms: int
    dlq_retention_ms: int
    events: tuple[EventSpec, ...]

    def spec(self, event_type: str) -> EventSpec:
        for spec in self.events:
            if spec.event_type == event_type:
                return spec
        raise UnknownEvent(event_type)

    def by_topic(self, topic: str) -> EventSpec:
        for spec in self.events:
            if spec.topic == topic:
                return spec
        raise UnknownEvent(topic)

    def topic(self, event_type: str) -> str:
        return self.spec(event_type).topic


@cache
def catalogue() -> Catalogue:
    raw = json.loads((HERE / "catalogue.json").read_text())
    prefix = raw["prefix"]
    return Catalogue(
        prefix=prefix,
        partitions=raw["partitions"],
        replication_factor=raw["replication_factor"],
        retention_ms=raw["retention_ms"],
        dlq_retention_ms=raw["dlq_retention_ms"],
        events=tuple(
            EventSpec(
                event_type=e["type"],
                topic=f"{prefix}.{e['type']}",
                key=e["key"],
                cleanup=e["cleanup"],
                schema_file=e["schema"],
            )
            for e in raw["events"]
        ),
    )


def _load(name: str) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads((SCHEMAS / name).read_text())
    return loaded


@cache
def _message_schema(event_type: str) -> str:
    spec = catalogue().spec(event_type)
    if spec.schema_file is None:
        raise UnknownEvent(f"{event_type} has no registered schema yet")
    message = _load("envelope.avsc")
    # One record name per topic, so the registry can tell the topics' schemas apart.
    message["name"] = "".join(part.capitalize() for part in event_type.replace("_", ".").split("."))
    message["fields"] = [*message["fields"], {"name": "payload", "type": _load(spec.schema_file)}]
    return json.dumps(message, sort_keys=True)


def message_schema(event_type: str) -> dict[str, Any]:
    """The full message schema of ``event_type``, as plain JSON (a fresh copy)."""
    schema: dict[str, Any] = json.loads(_message_schema(event_type))
    return schema


def message_schema_str(event_type: str) -> str:
    """The canonical text registered under the topic's subject."""
    return _message_schema(event_type)


@cache
def parsed_schema(event_type: str) -> Any:
    return fastavro.parse_schema(copy.deepcopy(message_schema(event_type)))


def payload_schema(event_type: str) -> dict[str, Any]:
    fields = message_schema(event_type)["fields"]
    payload: dict[str, Any] = next(f["type"] for f in fields if f["name"] == "payload")
    return payload


def has_schema(event_type: str) -> bool:
    try:
        return catalogue().spec(event_type).schema_file is not None
    except UnknownEvent:
        return False
