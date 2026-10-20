"""Catch-all: route match → per-IP limits → auth policy → per-subject limits →
forward → relay (RFC 0002 request pipeline)."""

from fastapi import APIRouter, Request
from starlette.responses import Response

from app.core.ratelimit import tightest
from app.proxy.auth import enforce_policy
from app.proxy.errors import MethodNotAllowed, RouteNotFound
from app.proxy.forward import forward
from app.proxy.routing import Policy
from smartretail_common.auth import bearer_token
from smartretail_common.client_id import HEADER as CLIENT_ID_HEADER
from smartretail_common.client_id import valid_client_id

PROXY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"]

router = APIRouter(include_in_schema=False)


@router.api_route("/{path:path}", methods=PROXY_METHODS)
async def proxy(request: Request, path: str) -> Response:
    state = request.app.state
    match = state.routes.match(request.method, request.url.path)
    if match.rule is None:
        if match.allowed_methods:
            raise MethodNotAllowed(match.allowed_methods)
        raise RouteNotFound()
    rule = match.rule
    if rule.policy == Policy.BLOCKED:
        raise RouteNotFound()
    limiter = state.limiter
    # Stage 5 (before auth): per-IP. The notification stream is exempt (GW-7).
    decisions = await limiter.before_auth(
        request.client.host if request.client else "",
        rule.rate,
        has_token=bearer_token(request) is not None,
        exempt=rule.stream and rule.policy == Policy.PUBLIC,
    )
    principal = await enforce_policy(request, rule, state.verifier)
    # Stage 8 (after auth): per-subject.
    decisions += await limiter.after_auth(
        principal.subject if principal is not None else None,
        rule.rate,
        client_id=valid_client_id(request.headers.get(CLIENT_ID_HEADER, "")),
    )
    settings = state.settings
    response = await forward(
        request,
        upstream=rule.upstream,
        client=state.clients.get(rule.upstream),
        body_limit=(
            settings.gateway_max_upload_bytes if rule.upload else settings.gateway_max_body_bytes
        ),
        read_timeout=(
            settings.gateway_stream_read_timeout_s
            if rule.stream
            else settings.gateway_upload_read_timeout_s
            if rule.upload
            else settings.gateway_upstream_read_timeout_s
        ),
    )
    if (advertised := tightest(decisions)) is not None:  # GW-21
        response.headers.update(advertised.headers())
    return response
