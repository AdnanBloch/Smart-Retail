import uuid

import httpx
import numpy as np
import pytest

from app.core.errors import EmbeddingUnavailable
from app.v1.search.clients.embeddings import EmbeddingError, HashingEmbeddings, VoyageEmbeddings
from app.v1.search.enums import IndexStatus
from app.v1.search.repo import ChunkRow, SearchFilters, group_by_product, keyword_query
from app.v1.search.utils import (
    TRANSITIONS,
    IllegalTransition,
    check_transition,
    path_to_indexed,
    path_to_removed,
    rrf,
)


def cos(a, b):
    return float(np.dot(a, b))


async def test_hashing_is_deterministic_normalised_and_similarity_aware():
    e = HashingEmbeddings()
    v1, v2 = await e.embed(["red running shoes", "red running shoes"], input_type="document")
    assert v1 == v2
    assert len(v1) == 1024
    assert abs(np.linalg.norm(v1) - 1.0) < 1e-9
    near = e.vector("lightweight shoes for running")
    far = e.vector("stainless steel kitchen blender")
    assert cos(v1, near) > cos(v1, far)
    assert abs(np.linalg.norm(e.vector("")) - 1.0) < 1e-9  # no zero vectors
    assert abs(np.linalg.norm(e.vector("the and of")) - 1.0) < 1e-9


def voyage(handler) -> VoyageEmbeddings:
    return VoyageEmbeddings("key", httpx.AsyncClient(transport=httpx.MockTransport(handler)))


async def test_voyage_success_orders_by_index_and_sends_input_type():
    seen = {}

    def handler(request):
        seen.update(__import__("json").loads(request.content))
        assert request.headers["authorization"] == "Bearer key"
        return httpx.Response(
            200,
            json={
                "data": [
                    {"index": 1, "embedding": [0.0] * 1024},
                    {"index": 0, "embedding": [1.0] * 1024},
                ],
                "usage": {"total_tokens": 7},
            },
        )

    client = voyage(handler)
    out = await client.embed(["a", "b"], input_type="query")
    assert out[0][0] == 1.0 and out[1][0] == 0.0
    assert seen["input_type"] == "query" and seen["model"] == "voyage-3.5"
    assert seen["output_dimension"] == 1024
    assert client.tokens_used == 7
    assert await client.embed([], input_type="document") == []


@pytest.mark.parametrize("status", [429, 500, 503])
async def test_voyage_outage_is_dependency_unavailable(status):
    with pytest.raises(EmbeddingUnavailable):
        await voyage(lambda r: httpx.Response(status)).embed(["a"], input_type="document")


async def test_voyage_timeout_and_rejections():
    def boom(request):
        raise httpx.ReadTimeout("slow", request=request)

    with pytest.raises(EmbeddingUnavailable):
        await voyage(boom).embed(["a"], input_type="document")
    with pytest.raises(EmbeddingError):
        await voyage(lambda r: httpx.Response(400)).embed(["a"], input_type="document")
    bad_shape = {"data": [{"index": 0, "embedding": [1.0]}]}
    with pytest.raises(EmbeddingError):
        await voyage(lambda r: httpx.Response(200, json=bad_shape)).embed(
            ["a"], input_type="document"
        )


def test_rrf_known_order_weights_and_empty_leg():
    scores = rrf([(["a", "b", "c"], 1.0), (["c", "a"], 1.0)], k=60)
    order = sorted(scores, key=lambda k: -scores[k])
    assert order == ["a", "c", "b"]
    assert scores["a"] == pytest.approx(1 / 61 + 1 / 62)
    heavy = rrf([(["a", "b"], 1.0), (["b"], 5.0)], k=60)
    assert heavy["b"] > heavy["a"]
    assert rrf([([], 1.0), (["x"], 1.0)]) == {"x": pytest.approx(1 / 61)}


def test_index_state_transition_table():
    assert set(TRANSITIONS) == {None, *IndexStatus}
    assert path_to_indexed(None) == [IndexStatus.PENDING, IndexStatus.INDEXED]
    assert path_to_indexed(IndexStatus.PENDING) == [IndexStatus.INDEXED]
    assert path_to_indexed(IndexStatus.REMOVED) == [IndexStatus.PENDING, IndexStatus.INDEXED]
    assert path_to_removed(IndexStatus.REMOVED) == []
    assert path_to_removed(IndexStatus.INDEXED) == [IndexStatus.REMOVED]
    with pytest.raises(IllegalTransition):
        check_transition(IndexStatus.INDEXED, IndexStatus.FAILED)
    with pytest.raises(IllegalTransition):
        check_transition(IndexStatus.REMOVED, IndexStatus.INDEXED)


def test_keyword_query_is_injection_free():
    assert keyword_query("Red shoes'; DROP TABLE x;--") == "red | shoes | drop | table | x"
    assert keyword_query("!!!") is None


def test_group_by_product_keeps_best_chunk():
    p1, p2 = uuid.uuid4(), uuid.uuid4()
    c = [uuid.uuid4() for _ in range(3)]
    rows = [
        ChunkRow(c[0], p1, p1, None, "identity", "a", {"name": "One"}, 0.1),
        ChunkRow(c[1], p1, p1, None, "description", "b", {"name": "One"}, 0.2),
        ChunkRow(c[2], p2, p2, None, "identity", "c", {"name": "Two"}, 0.3),
    ]
    hits = group_by_product(
        rows, {c[0]: 0.5, c[1]: 0.9, c[2]: 0.7}, {p1: {"semantic", "keyword"}, p2: {"keyword"}}
    )
    assert [h.product_id for h in hits] == [p1, p2]
    assert hits[0].chunk_id == c[1] and hits[0].match_reason == "semantic+keyword"
    assert hits[1].match_reason == "keyword"
    assert SearchFilters(min_price=None).as_dict() == {}
