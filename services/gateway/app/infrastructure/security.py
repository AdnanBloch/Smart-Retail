"""Edge authentication (RFC 0002 GW-1, GW-2). Early rejection only."""

from fastapi import Request

from app.utils.routing import Policy, RouteRule
from smartretail_common.auth import InvalidToken, JwtVerifier, Principal, bearer_token
from smartretail_common.errors import Forbidden


async def enforce_policy(
    request: Request, rule: RouteRule, verifier: JwtVerifier
) -> Principal | None:
    if rule.policy == Policy.PUBLIC:
        return None
    token = bearer_token(request)
    if token is None:
        if rule.policy == Policy.OPTIONAL:
            return None
        raise InvalidToken("missing")
    principal = await verifier.verify(token)
    if (
        rule.policy == Policy.BEARER
        and rule.roles
        and (principal.is_service or principal.role not in rule.roles)
    ):
        raise Forbidden("This action is not permitted for your role")
    return principal
