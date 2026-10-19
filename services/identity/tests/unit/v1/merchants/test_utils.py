"""Merchant slugs and status transitions (RFC 0003 §3.1.1.3)."""

from app.v1.merchants.utils import base_slug, next_free_slug


def test_slugs() -> None:
    assert base_slug("Café Déjà Vu!") == "cafe-deja-vu"
    assert base_slug("!!!") == "merchant"
    assert next_free_slug("shop", set()) == "shop"
    assert next_free_slug("shop", {"shop", "shop-2"}) == "shop-3"
