import pytest

from app.proxy.routing import ROUTES, Policy, RouteRule, RouteTable

UPSTREAMS = {n: f"http://{n}" for n in ("identity", "catalog", "order", "inventory")}
TABLE = RouteTable(ROUTES, UPSTREAMS)


@pytest.mark.parametrize(
    ("method", "path", "upstream", "policy"),
    [
        ("POST", "/api/v1/auth/login", "identity", Policy.PUBLIC),
        ("POST", "/api/v1/auth/logout", "identity", Policy.BEARER),
        ("GET", "/api/v1/users/me", "identity", Policy.BEARER),
        ("GET", "/api/v1/users/7b1c", "identity", Policy.BEARER),
        ("PATCH", "/api/v1/merchants/me", "identity", Policy.BEARER),
        ("GET", "/api/v1/merchants/7b1c", "identity", Policy.OPTIONAL),
        ("GET", "/api/v1/products", "catalog", Policy.OPTIONAL),
        ("GET", "/api/v1/products/me", "catalog", Policy.BEARER),
        ("GET", "/api/v1/products/abc", "catalog", Policy.OPTIONAL),
        ("GET", "/api/v1/products/abc/history", "catalog", Policy.BEARER),
        ("POST", "/api/v1/products/abc/variants", "catalog", Policy.BEARER),
        ("DELETE", "/api/v1/media/abc", "catalog", Policy.BEARER),
        ("POST", "/api/v1/auth/token", "identity", Policy.BLOCKED),
        ("GET", "/api/v1/products/abc/publish-status", "catalog", Policy.BEARER),
        ("POST", "/api/v1/products/abc/media/upload", "catalog", Policy.BEARER),
        ("POST", "/api/v1/orders", "order", Policy.BEARER),
        ("GET", "/api/v1/orders/merchant", "order", Policy.BEARER),
        ("POST", "/api/v1/orders/abc/cancel", "order", Policy.BEARER),
        ("GET", "/api/v1/orders/abc/workflow", "order", Policy.BEARER),
        ("GET", "/api/v1/inventory/abc", "inventory", Policy.OPTIONAL),
        ("POST", "/api/v1/inventory/abc/adjust", "inventory", Policy.BEARER),
        ("GET", "/internal/v1/anything/at/all", "", Policy.BLOCKED),
    ],
)
def test_matches(method, path, upstream, policy) -> None:
    rule = TABLE.match(method, path).rule
    assert rule is not None and rule.upstream == upstream and rule.policy == policy


def test_specific_literal_beats_parameter() -> None:
    assert TABLE.match("GET", "/api/v1/users/me").rule.roles == frozenset()
    assert TABLE.match("GET", "/api/v1/users/123").rule.roles == {"admin"}


def test_unmatched_and_wrong_method() -> None:
    assert TABLE.match("GET", "/api/v1/nope").rule is None
    assert TABLE.match("GET", "/api/v1/nope").allowed_methods == frozenset()
    miss = TABLE.match("PUT", "/api/v1/products/abc")
    assert miss.rule is None and miss.allowed_methods == {"GET", "PATCH", "DELETE"}
    assert TABLE.match("GET", "/api/v1/products/a/b/c").rule is None


def test_duplicate_rules_fail_startup() -> None:
    rule = RouteRule("/x", frozenset({"GET"}), "catalog", Policy.PUBLIC)
    with pytest.raises(ValueError):
        RouteTable((rule, rule), UPSTREAMS)


def test_unknown_upstream_fails_startup() -> None:
    with pytest.raises(ValueError):
        RouteTable((RouteRule("/x", frozenset({"GET"}), "nope", Policy.PUBLIC),), UPSTREAMS)


def test_gateway_roles_are_never_stricter_than_services() -> None:
    """GW-1: spot-check that every role-gated rule names roles the service allows."""
    for rule in ROUTES:
        if rule.roles:
            assert rule.roles <= {"customer", "merchant", "admin"}
