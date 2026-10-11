"""Catch-all: route match → auth policy → forward → relay (RFC 0002)."""

from fastapi import APIRouter, Request
from starlette.responses import Response

from app.proxy.auth import enforce_policy
from app.proxy.errors import MethodNotAllowed, RouteNotFound
from app.proxy.forward import forward
from app.proxy.routing import Policy

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
    await enforce_policy(request, rule, state.verifier)
    settings = state.settings
    return await forward(
        request,
        upstream=rule.upstream,
        client=state.clients.get(rule.upstream),
        body_limit=(
            settings.gateway_max_upload_bytes if rule.upload else settings.gateway_max_body_bytes
        ),
        read_timeout=(
            settings.gateway_upload_read_timeout_s
            if rule.upload
            else settings.gateway_upstream_read_timeout_s
        ),
    )
