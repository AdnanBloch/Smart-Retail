"""The Schema Registry REST client, against a mocked registry."""

import json

import httpx
import pytest

from smartretail_common.events.schema_registry import HttpSchemaRegistry, SchemaNotRegistered


class Registry:
    """A minimal Confluent Schema Registry: subjects → versions, ids → schemas."""

    def __init__(self) -> None:
        self.subjects: dict[str, list[str]] = {}
        self.ids: dict[str, int] = {}
        self.calls: list[str] = []
        self.compatibility: str | None = None

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(f"{request.method} {request.url.path}")
        path = request.url.path.split("/")[1:]
        body = json.loads(request.content) if request.content else {}
        if path[0] == "config":
            self.compatibility = body["compatibility"]
            return httpx.Response(200, json=body)
        if path[0] == "schemas":
            schema_id = int(path[2])
            schema = next(s for s, i in self.ids.items() if i == schema_id)
            return httpx.Response(200, json={"schema": schema})
        if path[0] == "compatibility":
            if path[2] not in self.subjects:
                return httpx.Response(404, json={"error_code": 40401})
            return httpx.Response(200, json={"is_compatible": "bad" not in body["schema"]})
        subject = path[1]
        if len(path) == 3:  # register
            schema_id = self.ids.setdefault(body["schema"], len(self.ids) + 1)
            self.subjects.setdefault(subject, []).append(body["schema"])
            return httpx.Response(200, json={"id": schema_id})
        if body["schema"] in self.subjects.get(subject, []):  # lookup
            return httpx.Response(200, json={"id": self.ids[body["schema"]]})
        return httpx.Response(404, json={"error_code": 40403})


@pytest.fixture
def registry() -> Registry:
    return Registry()


@pytest.fixture
def client(registry: Registry) -> HttpSchemaRegistry:
    http = httpx.AsyncClient(transport=httpx.MockTransport(registry.handler))
    return HttpSchemaRegistry("http://registry/", http=http)


async def test_register_then_lookup_and_fetch_are_cached(
    client: HttpSchemaRegistry, registry: Registry
) -> None:
    await client.set_compatibility("BACKWARD")
    assert registry.compatibility == "BACKWARD"
    schema_id = await client.register("t-value", '{"type":"string"}')
    assert await client.lookup("t-value", '{"type":"string"}') == schema_id
    assert await client.lookup("t-value", '{"type":"string"}') == schema_id
    assert await client.schema_by_id(schema_id) == '{"type":"string"}'
    lookups = [c for c in registry.calls if c == "POST /subjects/t-value"]
    assert len(lookups) == 1  # ids are immutable, so cached for the process
    assert not any(c.startswith("GET") for c in registry.calls)  # learned from the lookup
    await client.aclose()


async def test_schema_by_id_fetches_unknown_ids(
    client: HttpSchemaRegistry, registry: Registry
) -> None:
    schema_id = await client.register("t-value", '{"type":"long"}')
    assert await client.schema_by_id(schema_id) == '{"type":"long"}'
    assert f"GET /schemas/ids/{schema_id}" in registry.calls


async def test_lookup_of_an_unregistered_schema_raises(client: HttpSchemaRegistry) -> None:
    with pytest.raises(SchemaNotRegistered):
        await client.lookup("t-value", '{"type":"int"}')


async def test_compatibility_check(client: HttpSchemaRegistry) -> None:
    assert await client.is_compatible("new-value", "anything")  # new subject
    await client.register("t-value", '{"type":"string"}')
    assert await client.is_compatible("t-value", '{"type":"string","doc":"ok"}')
    assert not await client.is_compatible("t-value", '{"type":"bad"}')
