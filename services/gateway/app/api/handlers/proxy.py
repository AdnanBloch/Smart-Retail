"""Catch-all: route match → auth policy → forward → relay (RFC 0002)."""

from fastapi import APIRouter, Request
from starlette.responses import Response

from app.clients.upstream.errors import MethodNotAllowed, RouteNotFound
from app.clients.upstream.forward import forward
from app.infrastructure.security import enforce_policy
from app.utils.routing import Policy

router = APIRouter(include_in_schema=False)


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
    return await forward(
        request,
        upstream=rule.upstream,
        client=state.clients.get(rule.upstream),
        body_limit=state.settings.gateway_max_body_bytes,
        read_timeout=state.settings.gateway_upstream_read_timeout_s,
    )
