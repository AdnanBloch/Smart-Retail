from app.core.tokens import count_tokens
from app.v1.search.chunker import chunk_product, content_hash, split_description
from app.v1.search.enums import ChunkType
from app.v1.search.renderer import (
    document_from_catalog_product,
    document_from_payload,
    render,
    render_attributes,
    render_variants,
    summary,
)

DOC = {
    "name": "Acme Runner X1",
    "sku": "AR-X1",
    "description": "Lightweight shoes. Breathable mesh upper.",
    "category": {"id": "c1", "name": "Running", "path": ["Footwear", "Running"]},
    "attributes": {
        "brand": "Acme",
        "weight_g": 280,
        "waterproof": False,
        "colors": ["red", "blue"],
    },
    "variants": [
        {"options": {"size": "M", "color": "red"}, "is_active": True},
        {"options": {"size": "L"}, "is_active": False},
    ],
}


def test_render_produces_prose_and_identity_prefix():
    r = render(DOC)
    assert r.identity_prefix == (
        "PRODUCT: Acme Runner X1 | BRAND: Acme | SKU: AR-X1 | CATEGORY: Footwear > Running"
    )
    assert "Weight g: 280." in r.attributes
    assert "Waterproof: no." in r.attributes
    assert "Colors: red, blue." in r.attributes
    assert r.variants == "Available in 1 variant: size M, color red."
    assert r.category == "Category: Footwear > Running."
    assert "in the Running category" in r.identity


def test_render_handles_missing_fields_without_none():
    r = render({"name": None, "attributes": {"x": None, "nested": {"a": 1}}, "variants": None})
    for text in (r.identity_prefix, r.identity, r.description, r.attributes, r.variants):
        assert "None" not in text
    assert r.name == "Unnamed product"
    assert r.attributes == "Nested: a 1."
    assert render_variants([]) == ""
    assert render_attributes(None) == ""


def test_document_fallbacks():
    payload = {
        "product_id": "p",
        "merchant_id": "m",
        "name": "N",
        "sku": "S",
        "category_id": "c",
        "variants": [{"options": {"size": "S"}, "is_active": True}],
        "search_document": None,
    }
    doc = document_from_payload(payload)
    assert doc["name"] == "N" and doc["category"]["id"] == "c"
    assert document_from_payload({"search_document": DOC}) is DOC
    catalog = document_from_catalog_product(
        {"id": "p", "merchant_id": "m", "name": "N", "category": {"id": "c", "name": "Shoes"}}
    )
    assert catalog["category"]["path"] == ["Shoes"]
    assert "N is a product" in summary(catalog)


def test_every_chunk_carries_the_prefix_and_types():
    plan = chunk_product(render(DOC))
    types = [c.chunk_type for c in plan.chunks]
    assert types == [
        ChunkType.IDENTITY,
        ChunkType.DESCRIPTION,
        ChunkType.ATTRIBUTES,
        ChunkType.VARIANTS,
        ChunkType.CATEGORY,
    ]
    assert all(c.content.startswith("PRODUCT: Acme Runner X1") for c in plan.chunks)
    assert [c.chunk_index for c in plan.chunks] == list(range(5))
    assert not plan.truncated


def test_name_only_product_produces_one_chunk():
    plan = chunk_product(render({"name": "Just a name"}))
    assert len(plan.chunks) == 1
    assert plan.chunks[0].chunk_type == ChunkType.IDENTITY


def test_long_description_respects_bounds_and_overlap():
    sentence = "This sentence describes one more useful feature of the product in detail. "
    doc = {**DOC, "description": sentence * 120}
    plan = chunk_product(render(doc), target=256, max_tokens=512, overlap=80, max_chunks=40)
    descriptions = [c for c in plan.chunks if c.chunk_type == ChunkType.DESCRIPTION]
    assert len(descriptions) > 3
    assert all(c.token_count <= 512 for c in plan.chunks)
    assert all(count_tokens(c.content) == c.token_count for c in plan.chunks)
    # Overlap: the second window opens with the tail of the first.
    first = descriptions[0].content.split("\n", 1)[1].split()
    second = descriptions[1].content.split("\n", 1)[1].split()
    carried = 0
    while carried < len(second) and first[len(first) - carried - 1 :] != second[: carried + 1]:
        carried += 1
    assert 0 < count_tokens(" ".join(second[: carried + 1])) <= 80


def test_chunk_cap_truncates_descriptions():
    doc = {**DOC, "description": "Word " * 20000}
    plan = chunk_product(render(doc), max_chunks=12)
    assert len(plan.chunks) == 12
    assert plan.truncated
    assert plan.chunks[-1].chunk_type == ChunkType.CATEGORY  # short fields kept


def test_single_huge_sentence_is_split_by_words():
    windows = split_description("x" * 5 + " word" * 2000, target=100, hard_max=200, overlap=20)
    assert len(windows) > 5
    assert all(count_tokens(w) <= 200 for w in windows)
    assert split_description("   ", target=10, hard_max=20, overlap=2) == []


def test_content_hash_normalises_whitespace_and_case():
    assert content_hash("m", "Hello   World") == content_hash("m", "hello world")
    assert content_hash("m", "hello world") != content_hash("m", "hello there")
    assert content_hash("m1", "x") != content_hash("m2", "x")
