"""In-memory stand-ins for the Kafka client, shaped like ``confluent_kafka``'s."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FakeMessage:
    _topic: str
    _value: bytes | None
    _key: bytes | None = None
    _headers: list[tuple[str, bytes]] | None = None
    _partition: int = 0
    _offset: int = 0
    _error: Any = None

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes | None:
        return self._value

    def headers(self) -> list[tuple[str, bytes]] | None:
        return self._headers

    def error(self) -> Any:
        return self._error


@dataclass
class FakeSource:
    messages: list[FakeMessage] = field(default_factory=list)
    committed: list[FakeMessage] = field(default_factory=list)
    closed: bool = False

    def poll(self, timeout: float) -> FakeMessage | None:
        return self.messages.pop(0) if self.messages else None

    def commit(self, message: FakeMessage) -> None:
        self.committed.append(message)

    def close(self) -> None:
        self.closed = True


@dataclass
class FakePublisher:
    """Records produced messages; ``flush`` delivers them, failing those whose
    key is in ``fail_keys`` (or every one when ``fail_all``)."""

    produced: list[dict[str, Any]] = field(default_factory=list)
    fail_keys: set[bytes] = field(default_factory=set)
    fail_all: bool = False
    raise_on_flush: Exception | None = None
    _pending: list[tuple[dict[str, Any], Any]] = field(default_factory=list)

    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
        on_delivery: Any = None,
    ) -> None:
        record = {"topic": topic, "value": value, "key": key, "headers": headers or []}
        self._pending.append((record, on_delivery))

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        if self.raise_on_flush is not None:
            raise self.raise_on_flush
        pending, self._pending = self._pending, []
        for record, callback in pending:
            failed = self.fail_all or record["key"] in self.fail_keys
            if not failed:
                self.produced.append(record)
            if callback is not None:
                callback("broker said no" if failed else None, record)
        return 0
