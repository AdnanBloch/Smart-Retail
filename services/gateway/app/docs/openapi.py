"""Aggregated OpenAPI across upstreams (RFC 0002 GW-15).

Fetched concurrently, cached briefly; component schemas are namespaced per
service and ``$ref``s rewritten. A failing upstream is listed in
``x-upstreams-unavailable`` and its last good copy reused. Always 200.
"""

import asyncio
import copy
import logging
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

REF_PREFIX = "#/components/schemas/"


def _rewrite_refs(node: Any, service: str) -> Any:
    if isinstance(node, dict):
        out = {}
        for key, value in node.items():
            if key == "$ref" and isinstance(value, str) and value.startswith(REF_PREFIX):
                out[key] = f"{REF_PREFIX}{service}.{value[len(REF_PREFIX):]}"
            else:
                out[key] = _rewrite_refs(value, service)
        return out
    if isinstance(node, list):
        return [_rewrite_refs(v, service) for v in node]
    return node


def merge(
    documents: dict[str, dict[str, Any]], *, public_url: str, unavailable: list[str]
) -> dict[str, Any]:
    merged: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {
            "title": "SmartRetail API",
            "version": "1.0.0",
            "description": "All public routes, served through the gateway.",
        },
        "servers": [{"url": public_url}],
        "paths": {},
        "components": {"schemas": {}},
        "tags": [],
    }
    for service, document in sorted(documents.items()):
        document = _rewrite_refs(copy.deepcopy(document), service)
        merged["tags"].append({"name": service})
        for path, item in document.get("paths", {}).items():
            # Only the public API is routed; upstream operational routes are not.
            if not path.startswith("/api/v1/"):
                continue
            if path in merged["paths"]:
                logger.error("openapi path collision", extra={"path": path, "service": service})
                continue
            for operation in item.values():
                if isinstance(operation, dict) and "responses" in operation:
                    operation["tags"] = [service, *operation.get("tags", [])]
            merged["paths"][path] = item
        for name, schema in document.get("components", {}).get("schemas", {}).items():
            merged["components"]["schemas"][f"{service}.{name}"] = schema
    merged["tags"].append({"name": "gateway"})
    merged["paths"]["/health"] = {
        "get": {
            "tags": ["gateway"],
            "summary": "Liveness",
            "responses": {"200": {"description": "OK"}},
        }
    }
    merged["paths"]["/health/ready"] = {
        "get": {
            "tags": ["gateway"],
            "summary": "Readiness",
            "responses": {"200": {"description": "Ready"}, "503": {"description": "Not ready"}},
        }
    }
    if unavailable:
        merged["x-upstreams-unavailable"] = sorted(unavailable)
    return merged


class OpenApiAggregator:
    def __init__(
        self,
        upstreams: dict[str, httpx.AsyncClient],
        *,
        public_url: str,
        ttl_seconds: int,
        timeout: float = 2.0,
    ) -> None:
        self.upstreams = upstreams
        self.public_url = public_url
        self.ttl = ttl_seconds
        self.timeout = timeout
        self._last_good: dict[str, dict[str, Any]] = {}
        self._cached: dict[str, Any] | None = None
        self._cached_at = 0.0
        self._lock = asyncio.Lock()

    async def _fetch(self, name: str, client: httpx.AsyncClient) -> dict[str, Any] | None:
        try:
            response = await client.get("/openapi.json", timeout=self.timeout)
            response.raise_for_status()
            document: dict[str, Any] = response.json()
            return document
        except (httpx.HTTPError, ValueError):
            logger.warning("openapi fetch failed", extra={"upstream": name})
            return None

    async def document(self) -> dict[str, Any]:
        async with self._lock:
            if self._cached is not None and time.monotonic() - self._cached_at < self.ttl:
                return self._cached
            names = list(self.upstreams)
            results = await asyncio.gather(*(self._fetch(n, self.upstreams[n]) for n in names))
            unavailable = []
            for name, document in zip(names, results, strict=True):
                if document is None:
                    unavailable.append(name)
                else:
                    self._last_good[name] = document
            self._cached = merge(
                self._last_good, public_url=self.public_url, unavailable=unavailable
            )
            # Don't cache a degraded view for long: retry on the next request.
            self._cached_at = time.monotonic() if not unavailable else 0.0
            return self._cached
