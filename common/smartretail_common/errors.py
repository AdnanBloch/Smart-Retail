"""RFC 9457 problem details for every error (RFC 0000 decision A7).

Services subclass these errors and own their ``type`` slugs; this module only
knows how to serialise them.
"""

import logging
from collections.abc import Mapping, Sequence
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError
from starlette.exceptions import HTTPException as StarletteHTTPException

from smartretail_common.context import get_request_id

PROBLEM_CONTENT_TYPE = "application/problem+json"
PROBLEM_TYPE_BASE = "https://smartretail.local/errors/"

logger = logging.getLogger(__name__)


class DomainError(Exception):
    status: int = 500
    type_slug: str = "internal-error"
    title: str = "Internal server error"

    def __init__(
        self,
        detail: str | None = None,
        *,
        type_slug: str | None = None,
        title: str | None = None,
        extras: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(detail or title or self.title)
        self.detail = detail
        if type_slug is not None:
            self.type_slug = type_slug
        if title is not None:
            self.title = title
        self.extras = dict(extras or {})
        self.headers = dict(headers or {})


class BadRequest(DomainError):
    status = 400
    type_slug = "bad-request"
    title = "Bad request"


class Unauthorized(DomainError):
    status = 401
    type_slug = "unauthorized"
    title = "Authentication required"


class Forbidden(DomainError):
    status = 403
    type_slug = "forbidden"
    title = "Forbidden"


class NotFound(DomainError):
    status = 404
    type_slug = "not-found"
    title = "Not found"


class Conflict(DomainError):
    status = 409
    type_slug = "conflict"
    title = "Conflict"


class Unprocessable(DomainError):
    status = 422
    type_slug = "validation"
    title = "Validation failed"

    def __init__(
        self,
        detail: str | None = None,
        *,
        violations: Sequence[Mapping[str, Any]] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(detail, **kwargs)
        if violations:
            self.extras["violations"] = [dict(v) for v in violations]


class NotImplementedYet(DomainError):
    status = 501
    type_slug = "not-implemented"
    title = "Not implemented"


class DependencyUnavailable(DomainError):
    status = 503
    type_slug = "dependency-unavailable"
    title = "A dependency is unavailable"

    def __init__(self, detail: str | None = None, *, retry_after: int = 5, **kwargs: Any) -> None:
        super().__init__(detail, **kwargs)
        self.headers.setdefault("Retry-After", str(retry_after))


def problem_response(
    *,
    status: int,
    type_slug: str,
    title: str,
    detail: str | None = None,
    instance: str | None = None,
    extras: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    body: dict[str, Any] = {
        "type": PROBLEM_TYPE_BASE + type_slug,
        "title": title,
        "status": status,
    }
    if detail:
        body["detail"] = detail
    if instance:
        body["instance"] = instance
    request_id = get_request_id()
    if request_id:
        body["request_id"] = request_id
    if extras:
        body.update(extras)
    response_headers = dict(headers or {})
    if request_id:
        response_headers["X-Request-ID"] = request_id
    return JSONResponse(
        body, status_code=status, headers=response_headers, media_type=PROBLEM_CONTENT_TYPE
    )


def _violations(exc: RequestValidationError) -> list[dict[str, Any]]:
    out = []
    for err in exc.errors():
        loc = [str(p) for p in err.get("loc", ()) if p not in ("body", "query", "path", "header")]
        out.append({"field": ".".join(loc), "message": err.get("msg", ""), "code": err.get("type")})
    return out


_HTTP_SLUGS = {
    404: ("not-found", "Not found"),
    405: ("method-not-allowed", "Method not allowed"),
}


def is_connection_error(exc: BaseException) -> bool:
    """True for a store that is unreachable, not a request that is wrong: the
    API maps it to 503 and the consumer base waits instead of dead-lettering (NTF-14)."""
    # asyncpg surfaces refused connections and DNS failures as OSError.
    if isinstance(exc, OperationalError | InterfaceError | OSError | TimeoutError):
        return True
    return isinstance(exc, DBAPIError) and exc.connection_invalidated


def install_error_handlers(app: FastAPI) -> None:
    async def on_domain_error(request: Request, exc: Exception) -> JSONResponse:
        assert isinstance(exc, DomainError)
        return problem_response(
            status=exc.status,
            type_slug=exc.type_slug,
            title=exc.title,
            detail=exc.detail,
            instance=request.url.path,
            extras=exc.extras,
            headers=exc.headers,
        )

    async def on_validation_error(request: Request, exc: Exception) -> JSONResponse:
        assert isinstance(exc, RequestValidationError)
        return problem_response(
            status=422,
            type_slug="validation",
            title="Validation failed",
            instance=request.url.path,
            extras={"violations": _violations(exc)},
        )

    async def on_http_exception(request: Request, exc: Exception) -> JSONResponse:
        assert isinstance(exc, StarletteHTTPException)
        slug, title = _HTTP_SLUGS.get(exc.status_code, ("http-error", "HTTP error"))
        detail = exc.detail if isinstance(exc.detail, str) and exc.detail != title else None
        return problem_response(
            status=exc.status_code,
            type_slug=slug,
            title=title,
            detail=detail,
            instance=request.url.path,
            headers=exc.headers,
        )

    async def on_unhandled(request: Request, exc: Exception) -> JSONResponse:
        if is_connection_error(exc):
            logger.warning("dependency unavailable", exc_info=exc)
            return problem_response(
                status=503,
                type_slug="dependency-unavailable",
                title="A dependency is unavailable",
                instance=request.url.path,
                headers={"Retry-After": "5"},
            )
        # Never leak exception text, SQL or stack traces to the caller.
        logger.error("unhandled exception", exc_info=exc)
        return problem_response(
            status=500,
            type_slug="internal-error",
            title="Internal server error",
            instance=request.url.path,
        )

    app.add_exception_handler(DomainError, on_domain_error)
    app.add_exception_handler(RequestValidationError, on_validation_error)
    app.add_exception_handler(StarletteHTTPException, on_http_exception)
    app.add_exception_handler(Exception, on_unhandled)
