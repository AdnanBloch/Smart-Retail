"""Schema Registry access (RFC 0001 §3.1.1.6).

Producers and consumers only *look up* schemas: registration is the job of
``make topics`` (``scripts/topics.py``), so a producer can never register a
schema the compatibility check has not seen (``auto.register.schemas=false``).
"""

from typing import Protocol

import httpx

CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


class SchemaNotRegistered(LookupError):
    """The subject has no version equal to the schema being looked up."""


class SchemaRegistry(Protocol):
    async def lookup(self, subject: str, schema: str) -> int: ...

    async def schema_by_id(self, schema_id: int) -> str: ...


class HttpSchemaRegistry:
    """The Confluent REST API, with ids and schemas cached for the process lifetime
    (schema ids are immutable, so the cache never goes stale)."""

    def __init__(self, url: str, *, http: httpx.AsyncClient | None = None) -> None:
        self.url = url.rstrip("/")
        self.http = http or httpx.AsyncClient(timeout=5.0, headers={"Accept": CONTENT_TYPE})
        self._ids: dict[tuple[str, str], int] = {}
        self._schemas: dict[int, str] = {}

    async def lookup(self, subject: str, schema: str) -> int:
        cached = self._ids.get((subject, schema))
        if cached is not None:
            return cached
        response = await self.http.post(
            f"{self.url}/subjects/{subject}",
            json={"schema": schema},
            headers={"Content-Type": CONTENT_TYPE},
        )
        if response.status_code == 404:
            raise SchemaNotRegistered(subject)
        response.raise_for_status()
        schema_id = int(response.json()["id"])
        self._ids[(subject, schema)] = schema_id
        self._schemas[schema_id] = schema
        return schema_id

    async def schema_by_id(self, schema_id: int) -> str:
        cached = self._schemas.get(schema_id)
        if cached is not None:
            return cached
        response = await self.http.get(f"{self.url}/schemas/ids/{schema_id}")
        response.raise_for_status()
        schema = str(response.json()["schema"])
        self._schemas[schema_id] = schema
        return schema

    async def register(self, subject: str, schema: str) -> int:
        response = await self.http.post(
            f"{self.url}/subjects/{subject}/versions",
            json={"schema": schema},
            headers={"Content-Type": CONTENT_TYPE},
        )
        response.raise_for_status()
        return int(response.json()["id"])

    async def is_compatible(self, subject: str, schema: str) -> bool:
        response = await self.http.post(
            f"{self.url}/compatibility/subjects/{subject}/versions/latest",
            json={"schema": schema},
            headers={"Content-Type": CONTENT_TYPE},
        )
        if response.status_code == 404:  # a new subject is compatible by definition
            return True
        response.raise_for_status()
        return bool(response.json()["is_compatible"])

    async def set_compatibility(self, level: str) -> None:
        response = await self.http.put(
            f"{self.url}/config",
            json={"compatibility": level},
            headers={"Content-Type": CONTENT_TYPE},
        )
        response.raise_for_status()

    async def aclose(self) -> None:
        await self.http.aclose()


class InMemorySchemaRegistry:
    """A registry in a dict, for tests and for producing without a registry."""

    def __init__(self) -> None:
        self._by_subject: dict[tuple[str, str], int] = {}
        self._schemas: dict[int, str] = {}

    def register_now(self, subject: str, schema: str) -> int:
        key = (subject, schema)
        if key not in self._by_subject:
            schema_id = len(self._schemas) + 1
            self._by_subject[key] = schema_id
            self._schemas[schema_id] = schema
        return self._by_subject[key]

    async def register(self, subject: str, schema: str) -> int:
        return self.register_now(subject, schema)

    async def lookup(self, subject: str, schema: str) -> int:
        try:
            return self._by_subject[(subject, schema)]
        except KeyError:
            raise SchemaNotRegistered(subject) from None

    async def schema_by_id(self, schema_id: int) -> str:
        return self._schemas[schema_id]
