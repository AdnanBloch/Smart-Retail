"""The activity error contract (RFC 0006 INV-5): which failures a workflow
retries and which it compensates for, for in-process service calls
(``as_activity_errors``) and for HTTP calls (``raise_for_activity``)."""

import httpx
import pytest
from temporalio.exceptions import ApplicationError

from smartretail_common.errors import (
    BadRequest,
    Conflict,
    DependencyUnavailable,
    DomainError,
    Forbidden,
    NotFound,
    Unprocessable,
)
from smartretail_common.service_token import ServiceCredentials, ServiceTokenClient
from smartretail_common.temporal import as_activity_errors, raise_for_activity


class InsufficientStock(Conflict):
    type_slug = "insufficient-stock"
    title = "Insufficient stock"


# --- as_activity_errors ------------------------------------------------------------


async def test_a_successful_call_returns_its_value() -> None:
    async def call() -> int:
        return 7

    assert await as_activity_errors(call) == 7


@pytest.mark.parametrize(
    "error",
    [BadRequest("bad"), Forbidden("no"), NotFound("gone"), Unprocessable("invalid")],
    ids=["400", "403", "404", "422"],
)
async def test_a_4xx_domain_error_is_a_non_retryable_business_failure(error: DomainError) -> None:
    async def call() -> None:
        raise error

    with pytest.raises(ApplicationError) as info:
        await as_activity_errors(call)
    assert info.value.non_retryable is True
    assert info.value.type == type(error).__name__
    assert info.value.message == error.detail
    assert info.value.__cause__ is error


async def test_the_title_stands_in_for_a_missing_detail() -> None:
    async def call() -> None:
        raise InsufficientStock()

    with pytest.raises(ApplicationError) as info:
        await as_activity_errors(call)
    assert info.value.type == "InsufficientStock"
    assert info.value.message == "Insufficient stock"


@pytest.mark.parametrize(
    "error", [DependencyUnavailable("db down"), DomainError("boom")], ids=["503", "500"]
)
async def test_a_5xx_domain_error_is_retried(error: DomainError) -> None:
    async def call() -> None:
        raise error

    with pytest.raises(ApplicationError) as info:
        await as_activity_errors(call)
    assert info.value.non_retryable is False
    assert info.value.type == type(error).__name__


async def test_anything_else_propagates_unchanged_for_the_retry_policy() -> None:
    async def call() -> None:
        raise ConnectionResetError("peer reset")

    with pytest.raises(ConnectionResetError):
        await as_activity_errors(call)


# --- raise_for_activity ------------------------------------------------------------


def _response(status: int, **kwargs: object) -> httpx.Response:
    request = httpx.Request("POST", "http://inventory/internal/v1/reservations?x=1")
    return httpx.Response(status, request=request, **kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize("status", [200, 201, 204, 302])
def test_a_non_error_status_passes(status: int) -> None:
    raise_for_activity(_response(status))


@pytest.mark.parametrize("status", [400, 403, 404, 409, 410, 422])
def test_a_4xx_is_a_business_failure_naming_the_call(status: int) -> None:
    body = {"type": "https://smartretail.local/errors/insufficient-stock"}
    with pytest.raises(ApplicationError) as info:
        raise_for_activity(_response(status, json=body))
    assert info.value.non_retryable is True
    assert info.value.type == "insufficient-stock"
    # Path only: the query string is not part of the message.
    assert info.value.message == (f"POST /internal/v1/reservations → {status} insufficient-stock")


@pytest.mark.parametrize("status", [408, 429])
def test_timeout_and_rate_limit_are_retried_despite_being_4xx(status: int) -> None:
    with pytest.raises(ApplicationError) as info:
        raise_for_activity(_response(status, json={"type": "x/slow-down"}))
    assert info.value.non_retryable is False
    assert info.value.type == "DependencyUnavailable"
    assert info.value.message == f"{status} slow-down"


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_a_5xx_is_retried(status: int) -> None:
    with pytest.raises(ApplicationError) as info:
        raise_for_activity(_response(status, text="<html>bad gateway</html>"))
    assert info.value.non_retryable is False
    assert info.value.message == f"{status} http-{status}"


def test_401_without_a_token_client_is_still_retried() -> None:
    with pytest.raises(ApplicationError) as info:
        raise_for_activity(_response(401, json={"type": "x/invalid-token"}))
    assert info.value.non_retryable is False
    assert info.value.type == "Unauthorised"
    assert info.value.message == "unauthorised (invalid-token)"


def test_401_drops_only_the_cached_token() -> None:
    tokens = ServiceTokenClient(ServiceCredentials("http://identity", "order", "secret"))
    tokens._token = "stale"
    with pytest.raises(ApplicationError):
        raise_for_activity(_response(401), tokens=tokens)
    assert tokens._token is None
    # A business failure leaves a good token alone.
    tokens._token = "fresh"
    with pytest.raises(ApplicationError):
        raise_for_activity(_response(409, json={"type": "x/conflict"}), tokens=tokens)
    assert tokens._token == "fresh"


@pytest.mark.parametrize(
    ("kwargs", "slug"),
    [
        ({"json": {"type": ""}}, "http-404"),  # empty type
        ({"json": {"title": "no type"}}, "http-404"),  # no type at all
        ({"json": {"type": "https://x/errors/"}}, "http-404"),  # trailing slash
        ({"json": {"type": "plain-slug"}}, "plain-slug"),  # not a URL
        ({"json": "just a string"}, "http-404"),  # not an object
        ({"content": b""}, "http-404"),  # no body
    ],
)
def test_the_problem_slug_falls_back_to_the_status(kwargs: dict, slug: str) -> None:
    with pytest.raises(ApplicationError) as info:
        raise_for_activity(_response(404, **kwargs))
    assert info.value.type == slug
