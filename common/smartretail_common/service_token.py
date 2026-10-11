"""Client-credentials service tokens for internal calls (RFC 0003 IDN-T3, ORD-11).

A workflow outlives the request that started it and the user token with it,
so activities call ``/internal/*`` routes with a short-lived ``typ=service``
token that ``identity`` mints from a client-credentials grant. The token is
cached and refreshed at 80 % of its lifetime; a 401 from a callee forces a
refresh on the next call.
"""

import asyncio
import time
from dataclasses import dataclass

import httpx

from smartretail_common.errors import DependencyUnavailable

REFRESH_AT = 0.8


@dataclass(frozen=True)
class ServiceCredentials:
    token_url: str
    client_id: str
    client_secret: str
    scope: str = ""


class ServiceTokenError(DependencyUnavailable):
    type_slug = "service-token-unavailable"
    title = "Could not obtain a service token"


class ServiceTokenClient:
    def __init__(
        self, credentials: ServiceCredentials, *, http_client: httpx.AsyncClient | None = None
    ) -> None:
        self.credentials = credentials
        self._http = http_client
        self._token: str | None = None
        self._refresh_after = 0.0
        self._lock = asyncio.Lock()

    async def token(self) -> str:
        if self._token is not None and time.monotonic() < self._refresh_after:
            return self._token
        async with self._lock:
            if self._token is None or time.monotonic() >= self._refresh_after:
                await self._fetch()
            assert self._token is not None
            return self._token

    def invalidate(self) -> None:
        self._token = None

    async def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {await self.token()}"}

    async def _fetch(self) -> None:
        form = {
            "grant_type": "client_credentials",
            "client_id": self.credentials.client_id,
            "client_secret": self.credentials.client_secret,
        }
        if self.credentials.scope:
            form["scope"] = self.credentials.scope
        client = self._http or httpx.AsyncClient(timeout=5.0)
        try:
            response = await client.post(self.credentials.token_url, data=form)
        except httpx.HTTPError as exc:
            raise ServiceTokenError(f"token endpoint unreachable: {exc}") from exc
        finally:
            if self._http is None:
                await client.aclose()
        if response.status_code != 200:
            raise ServiceTokenError(f"token endpoint answered {response.status_code}")
        body = response.json()
        self._token = body["access_token"]
        self._refresh_after = time.monotonic() + REFRESH_AT * int(body["expires_in"])
