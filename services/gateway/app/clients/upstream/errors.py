"""Gateway-originated problem types (RFC 0002 GW-11)."""

from smartretail_common.errors import DomainError


class RouteNotFound(DomainError):
    status = 404
    type_slug = "route-not-found"
    title = "No route matches this request"


class MethodNotAllowed(DomainError):
    status = 405
    type_slug = "method-not-allowed"
    title = "Method not allowed"

    def __init__(self, allowed: frozenset[str]) -> None:
        super().__init__(headers={"Allow": ", ".join(sorted(allowed))})


class PayloadTooLarge(DomainError):
    status = 413
    type_slug = "payload-too-large"
    title = "Request body too large"

    def __init__(self, limit: int) -> None:
        super().__init__(extras={"limit_bytes": limit})


class UpstreamUnavailable(DomainError):
    status = 503
    type_slug = "upstream-unavailable"
    title = "Upstream service unavailable"

    def __init__(self, upstream: str) -> None:
        super().__init__(extras={"upstream": upstream}, headers={"Retry-After": "5"})


class UpstreamTimeout(DomainError):
    status = 504
    type_slug = "upstream-timeout"
    title = "Upstream service timed out"

    def __init__(self, upstream: str, timeout: float) -> None:
        super().__init__(extras={"upstream": upstream, "timeout_seconds": timeout})


class UpstreamBadResponse(DomainError):
    status = 502
    type_slug = "upstream-bad-response"
    title = "Upstream returned an invalid response"

    def __init__(self, upstream: str) -> None:
        super().__init__(extras={"upstream": upstream})


class UpstreamError(DomainError):
    """A non-problem 5xx from upstream: status kept, body discarded."""

    type_slug = "upstream-error"
    title = "Upstream service error"

    def __init__(self, upstream: str, status: int) -> None:
        super().__init__(extras={"upstream": upstream})
        self.status = status
