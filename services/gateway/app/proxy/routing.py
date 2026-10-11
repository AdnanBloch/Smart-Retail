"""The route table (RFC 0002 §3.1.2.1), Milestone 1 rows: identity and catalog.

Patterns are matched segment by segment: a literal, ``{id}`` for any single
segment, or a trailing ``**`` for the rest of the path. The most specific
matching pattern wins (most literal segments, then longest); then method.
Paths are forwarded unchanged. Anything unmatched is ``404 route-not-found``.

Role sets here are coarse early rejection and must be looser than or equal
to the service's own checks (GW-1); a gateway allow is never authorisation.
"""

from dataclasses import dataclass, field
from enum import StrEnum

ADMIN = "admin"
MERCHANT = "merchant"
CATALOG_WRITERS = frozenset({MERCHANT, ADMIN})


class Policy(StrEnum):
    PUBLIC = "public"  # Authorization ignored here and forwarded unchanged
    OPTIONAL = "optional"  # a token, if present, must verify
    BEARER = "bearer"  # a valid user token is required; roles if non-empty
    BLOCKED = "blocked"  # 404, as if the route did not exist


@dataclass(frozen=True)
class RouteRule:
    pattern: str
    methods: frozenset[str]
    upstream: str
    policy: Policy
    roles: frozenset[str] = field(default_factory=frozenset)

    @property
    def segments(self) -> tuple[str, ...]:
        return tuple(s for s in self.pattern.split("/") if s)

    @property
    def specificity(self) -> tuple[int, int]:
        segs = self.segments
        literals = sum(1 for s in segs if s not in ("{id}", "**"))
        return literals, len(segs)

    def matches(self, path_segments: tuple[str, ...]) -> bool:
        segs = self.segments
        if segs and segs[-1] == "**":
            head = segs[:-1]
            if len(path_segments) < len(head):
                return False
            segs, path_segments = head, path_segments[: len(head)]
        if len(segs) != len(path_segments):
            return False
        return all(s == "{id}" or s == p for s, p in zip(segs, path_segments, strict=True))


def _r(pattern: str, methods: str, upstream: str, policy: Policy, roles=frozenset()) -> RouteRule:
    return RouteRule(pattern, frozenset(methods.split(",")), upstream, policy, frozenset(roles))


P = Policy
ROUTES: tuple[RouteRule, ...] = (
    # identity
    _r("/api/v1/auth/register", "POST", "identity", P.PUBLIC),
    _r("/api/v1/auth/login", "POST", "identity", P.PUBLIC),
    _r("/api/v1/auth/refresh", "POST", "identity", P.PUBLIC),
    _r("/api/v1/auth/jwks", "GET", "identity", P.PUBLIC),
    _r("/api/v1/auth/logout", "POST", "identity", P.BEARER),
    _r("/api/v1/auth/token", "POST", "identity", P.BLOCKED),  # service tokens: never routed
    _r("/api/v1/users/me", "GET,PATCH", "identity", P.BEARER),
    _r("/api/v1/users/me/password", "POST", "identity", P.BEARER),
    _r("/api/v1/users", "GET", "identity", P.BEARER, {ADMIN}),
    _r("/api/v1/users/{id}", "GET,PATCH", "identity", P.BEARER, {ADMIN}),
    _r("/api/v1/merchants", "GET", "identity", P.OPTIONAL),
    _r("/api/v1/merchants/{id}", "GET", "identity", P.OPTIONAL),
    # Not in the RFC 0002 table, which routes /merchants/* as GET only; added
    # so the M1 identity endpoint PATCH /merchants/me is reachable.
    _r("/api/v1/merchants/me", "GET", "identity", P.OPTIONAL),
    _r("/api/v1/merchants/me", "PATCH", "identity", P.BEARER, {MERCHANT}),
    # catalog
    _r("/api/v1/categories", "GET", "catalog", P.OPTIONAL),
    _r("/api/v1/categories/{id}", "GET", "catalog", P.OPTIONAL),
    _r("/api/v1/categories", "POST", "catalog", P.BEARER, {ADMIN}),
    _r("/api/v1/categories/{id}", "PATCH,DELETE", "catalog", P.BEARER, {ADMIN}),
    _r("/api/v1/products", "GET", "catalog", P.OPTIONAL),
    _r("/api/v1/products", "POST", "catalog", P.BEARER, CATALOG_WRITERS),
    _r("/api/v1/products/me", "GET", "catalog", P.BEARER, {MERCHANT}),
    _r("/api/v1/products/{id}", "GET", "catalog", P.OPTIONAL),
    _r("/api/v1/products/{id}", "PATCH,DELETE", "catalog", P.BEARER, CATALOG_WRITERS),
    _r("/api/v1/products/{id}/history", "GET", "catalog", P.BEARER, CATALOG_WRITERS),
    _r("/api/v1/products/{id}/status", "POST", "catalog", P.BEARER, CATALOG_WRITERS),
    _r("/api/v1/products/{id}/variants", "POST", "catalog", P.BEARER, CATALOG_WRITERS),
    _r("/api/v1/products/{id}/media", "POST", "catalog", P.BEARER, CATALOG_WRITERS),
    _r("/api/v1/variants/{id}", "PATCH,DELETE", "catalog", P.BEARER, CATALOG_WRITERS),
    _r("/api/v1/media/{id}", "DELETE", "catalog", P.BEARER, CATALOG_WRITERS),
    # Internal routes are service-to-service only and never routed (RFC 0000).
    _r("/internal/**", "GET,POST,PUT,PATCH,DELETE", "", P.BLOCKED),
)


@dataclass(frozen=True)
class Match:
    rule: RouteRule | None
    allowed_methods: frozenset[str]


class RouteTable:
    def __init__(self, rules: tuple[RouteRule, ...], upstreams: dict[str, str]) -> None:
        self.rules = rules
        self._validate(upstreams)

    def _validate(self, upstreams: dict[str, str]) -> None:
        """Fail start-up on ambiguous rules or unknown upstreams."""
        seen: dict[tuple[str, str], RouteRule] = {}
        for rule in self.rules:
            if rule.policy != Policy.BLOCKED and rule.upstream not in upstreams:
                raise ValueError(f"{rule.pattern}: unknown upstream {rule.upstream!r}")
            for method in rule.methods:
                key = (rule.pattern, method)
                if key in seen:
                    raise ValueError(f"duplicate route {method} {rule.pattern}")
                seen[key] = rule

    def match(self, method: str, path: str) -> Match:
        segments = tuple(s for s in path.split("/") if s)
        candidates = [r for r in self.rules if r.matches(segments)]
        if not candidates:
            return Match(None, frozenset())
        best = max(r.specificity for r in candidates)
        top = [r for r in candidates if r.specificity == best]
        allowed = frozenset().union(*(r.methods for r in top))
        for rule in top:
            if method in rule.methods:
                return Match(rule, allowed)
        return Match(None, allowed)
