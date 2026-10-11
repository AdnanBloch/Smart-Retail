"""Temporal plumbing shared by every service that owns a workflow (RFC 0001 §3.1.1.3).

Mechanism only: a lazily connected client, the platform's default activity
policy, the deterministic idempotency token (B3), and the error contract for
activities that call another service over HTTP (RFC 0006 INV-5). Workflow
steps, timeouts and compensations belong to the owning service.

Requires the ``temporalio`` package (``smartretail-common[temporal]``).
"""

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from datetime import timedelta
from typing import Any, TypeVar

import httpx
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from smartretail_common.errors import DomainError
from smartretail_common.service_token import ServiceTokenClient

DEFAULT_NAMESPACE = "smartretail"

T = TypeVar("T")


class TemporalClientFactory:
    """One client per process, connected on first use and reconnected after a failure.

    Endpoints that need Temporal (start, signal, query) fail with 503 when it
    is down; nothing else in the service depends on it.
    """

    def __init__(self, address: str, namespace: str = DEFAULT_NAMESPACE) -> None:
        self.address = address
        self.namespace = namespace
        self._client: Client | None = None
        self._lock = asyncio.Lock()

    async def get(self) -> Client:
        if self._client is not None:
            return self._client
        async with self._lock:
            if self._client is None:
                self._client = await Client.connect(self.address, namespace=self.namespace)
            return self._client

    def set(self, client: Client) -> None:
        """Injects a client (tests use a local or time-skipping environment)."""
        self._client = client


def default_retry(
    *,
    initial_seconds: float = 1,
    maximum_seconds: float = 30,
    attempts: int = 3,
    non_retryable: tuple[str, ...] = (),
) -> RetryPolicy:
    """The platform default (RFC 0001): 1 s, doubling, capped. ``attempts=0`` is unlimited."""
    return RetryPolicy(
        initial_interval=timedelta(seconds=initial_seconds),
        backoff_coefficient=2.0,
        maximum_interval=timedelta(seconds=maximum_seconds),
        maximum_attempts=attempts,
        non_retryable_error_types=list(non_retryable),
    )


def activity_idempotency_token(workflow_id: str, activity_name: str, *parts: object) -> str:
    """B3: ``{workflow_id}:{activity}:{attempt-invariant input}``, stable across retries."""
    return ":".join([workflow_id, activity_name, *(str(p) for p in parts)])


def business_failure(error_type: str, message: str, **details: Any) -> ApplicationError:
    """A non-retryable failure: the workflow compensates instead of retrying."""
    extra = [details] if details else []
    return ApplicationError(message, *extra, type=error_type, non_retryable=True)


async def as_activity_errors(call: Callable[[], Awaitable[T]]) -> T:
    """Runs a service-layer call inside an activity under the same contract:
    a 4xx ``DomainError`` is a business outcome (non-retryable), a 5xx one is
    retried, and anything else propagates for the retry policy to judge."""
    try:
        return await call()
    except DomainError as exc:
        message = exc.detail or exc.title
        if exc.status < 500:
            raise business_failure(type(exc).__name__, message) from exc
        raise ApplicationError(message, type=type(exc).__name__) from exc


def _problem_slug(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return f"http-{response.status_code}"
    raw = body.get("type", "") if isinstance(body, Mapping) else ""
    return raw.rsplit("/", 1)[-1] or f"http-{response.status_code}"


def raise_for_activity(
    response: httpx.Response, *, tokens: ServiceTokenClient | None = None
) -> None:
    """INV-5: a 4xx is a business outcome (non-retryable); anything else is retried.

    401 is the exception among 4xx: an expired service token is transient, so
    the cached token is dropped and the activity retried with a fresh one.
    """
    status = response.status_code
    if status < 400:
        return
    slug = _problem_slug(response)
    if status == 401:
        if tokens is not None:
            tokens.invalidate()
        raise ApplicationError(f"unauthorised ({slug})", type="Unauthorised")
    if 400 <= status < 500 and status not in (408, 429):
        request = response.request
        raise business_failure(slug, f"{request.method} {request.url.path} → {status} {slug}")
    raise ApplicationError(f"{status} {slug}", type="DependencyUnavailable")
