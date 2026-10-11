"""order problem types (RFC 0005 §3.1.2)."""

from typing import Any

from smartretail_common.errors import (
    PROBLEM_TYPE_BASE,
    Conflict,
    DependencyUnavailable,
    DomainError,
    NotFound,
)


class OrderNotFound(NotFound):
    type_slug = "order-not-found"
    title = "Order not found"


class VersionConflict(Conflict):
    type_slug = "version-conflict"
    title = "The order was modified by someone else"

    def __init__(self, current_version: int) -> None:
        super().__init__(
            f"Current version is {current_version}; reload and retry",
            extras={"current_version": current_version},
        )


class OrderTerminal(Conflict):
    type_slug = "order-terminal"
    title = "The order is already final"


class PastCancellablePoint(Conflict):
    type_slug = "past-cancellable-point"
    title = "The order can no longer be cancelled"

    def __init__(self, stage: str) -> None:
        super().__init__(
            f"The order is {stage}: payment is settled and stock committed. "
            "After this point a cancellation would be a refund request.",
            extras={"stage": stage},
        )


class TemporalUnavailable(DependencyUnavailable):
    type_slug = "workflow-unavailable"
    title = "The order workflow service is unavailable"


def problem_body(exc: DomainError, instance: str) -> dict[str, Any]:
    """The RFC 9457 body of ``exc``, for a reply stored and replayed verbatim.

    ``request_id`` is left out: each replay is a new request with its own id,
    which the ``X-Request-ID`` header carries.
    """
    body: dict[str, Any] = {
        "type": PROBLEM_TYPE_BASE + exc.type_slug,
        "title": exc.title,
        "status": exc.status,
        "instance": instance,
    }
    if exc.detail:
        body["detail"] = exc.detail
    body.update(exc.extras)
    return body
