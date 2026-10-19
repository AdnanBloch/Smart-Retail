"""Structured product record → natural-language prose (D5, AST-D6).

The input is catalog's ``search_document`` (RFC 0004 §3.1.2.3): name, sku,
description, category {id, name, path}, attributes, variants [{options, price,
is_active}], base_price, currency, media. Embedding models are trained on
prose, so ``{"material": "mesh"}`` becomes "Material: mesh." Price and stock
are never rendered (D9): they are hydrated live. Missing fields produce no
text — never "None".
"""

from dataclasses import dataclass
from typing import Any

from app.core.safety import sanitize_text


@dataclass(frozen=True)
class RenderedProduct:
    name: str
    identity_prefix: str
    identity: str
    description: str
    attributes: str
    variants: str
    category: str


def _label(key: str) -> str:
    words = str(key).replace("_", " ").replace("-", " ").strip()
    return words[:1].upper() + words[1:] if words else ""


def _value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, list | tuple):
        return ", ".join(v for v in (_value(x) for x in value) if v)
    if isinstance(value, dict):
        return "; ".join(
            f"{_label(k).lower()} {_value(v)}".strip() for k, v in value.items() if _value(v)
        )
    return sanitize_text(str(value), max_chars=500)


def category_path(document: dict[str, Any]) -> list[str]:
    category = document.get("category") or {}
    path = [sanitize_text(str(p), max_chars=100) for p in (category.get("path") or []) if p]
    if not path and category.get("name"):
        path = [sanitize_text(str(category["name"]), max_chars=100)]
    return path


def render_attributes(attributes: dict[str, Any] | None) -> str:
    sentences = []
    for key, value in (attributes or {}).items():
        rendered = _value(value)
        if rendered and _label(key):
            sentences.append(f"{_label(key)}: {rendered}.")
    return " ".join(sentences)


def render_options(options: dict[str, Any] | None) -> str:
    return ", ".join(
        f"{_label(k).lower()} {_value(v)}" for k, v in (options or {}).items() if _value(v)
    )


def render_variants(variants: list[dict[str, Any]] | None) -> str:
    active = [v for v in (variants or []) if v.get("is_active", True)]
    described = [r for r in (render_options(v.get("options")) for v in active) if r]
    if not described:
        return ""
    noun = "variant" if len(described) == 1 else "variants"
    return f"Available in {len(described)} {noun}: " + "; ".join(described) + "."


def render(document: dict[str, Any]) -> RenderedProduct:
    name = sanitize_text(document.get("name"), max_chars=300) or "Unnamed product"
    sku = sanitize_text(document.get("sku"), max_chars=100)
    attributes = document.get("attributes") or {}
    brand = _value(attributes.get("brand")) if isinstance(attributes, dict) else ""
    path = category_path(document)

    prefix = [f"PRODUCT: {name}"]
    if brand:
        prefix.append(f"BRAND: {brand}")
    if sku:
        prefix.append(f"SKU: {sku}")
    if path:
        prefix.append(f"CATEGORY: {' > '.join(path)}")

    identity = f"{name} is a product"
    if brand:
        identity += f" by {brand}"
    if path:
        identity += f" in the {path[-1]} category"
    identity += "."
    return RenderedProduct(
        name=name,
        identity_prefix=" | ".join(prefix),
        identity=identity,
        description=sanitize_text(document.get("description")),
        attributes=render_attributes(attributes if isinstance(attributes, dict) else {}),
        variants=render_variants(document.get("variants")),
        category=f"Category: {' > '.join(path)}." if path else "",
    )


def document_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """The search document of a ``catalog.product.*`` payload; a minimal one
    from the payload's own fields when ``search_document`` is null."""
    document = payload.get("search_document")
    if isinstance(document, dict):
        return document
    return {
        "product_id": payload.get("product_id"),
        "merchant_id": payload.get("merchant_id"),
        "name": payload.get("name"),
        "sku": payload.get("sku"),
        "description": None,
        "category": (
            {"id": payload["category_id"], "name": None, "path": []}
            if payload.get("category_id")
            else None
        ),
        "attributes": {},
        "variants": [
            {"options": v.get("options") or {}, "is_active": v.get("is_active", True)}
            for v in payload.get("variants") or []
        ],
        "base_price": payload.get("base_price"),
        "currency": payload.get("currency"),
        "media": [],
    }


def document_from_catalog_product(product: dict[str, Any]) -> dict[str, Any]:
    """A search document from catalog's ``GET /products/{id}`` body (merchant
    generation works on drafts, which have no search document yet)."""
    category = product.get("category") or None
    return {
        "product_id": product.get("id"),
        "merchant_id": product.get("merchant_id"),
        "name": product.get("name"),
        "sku": product.get("sku"),
        "description": product.get("description"),
        "category": (
            {
                "id": category.get("id"),
                "name": category.get("name"),
                "path": category.get("path") or [category.get("name")],
            }
            if isinstance(category, dict)
            else None
        ),
        "attributes": product.get("attributes") or {},
        "variants": [
            {"options": v.get("options") or {}, "is_active": v.get("is_active", True)}
            for v in product.get("variants") or []
        ],
        "base_price": product.get("base_price"),
        "currency": product.get("currency"),
    }


def summary(document: dict[str, Any], *, max_chars: int = 1500) -> str:
    """One paragraph of product facts, for generation prompts."""
    rendered = render(document)
    parts = [rendered.identity, rendered.description, rendered.attributes, rendered.variants]
    return " ".join(p for p in parts if p)[:max_chars]
