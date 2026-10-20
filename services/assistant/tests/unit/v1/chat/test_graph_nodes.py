"""Nodes in isolation with a constructed state (E2)."""

import uuid
from types import SimpleNamespace

from app.core.clients.llm import ScriptedProvider
from app.core.config import Settings
from app.v1.chat.agent.graph import RETRIEVE_LIMIT, classify, compare, load_memory, refuse, route
from app.v1.chat.agent.state import AgentDeps, Usage
from app.v1.prompts.repo import Template
from app.v1.search.repo import Hit, RawSearch, SearchFilters


def deps(provider) -> AgentDeps:
    settings = Settings(database_url="postgresql+asyncpg://x/y", memory_window_turns=1)
    return AgentDeps(
        search=SimpleNamespace(),  # type: ignore[arg-type]
        provider=provider,
        settings=settings,
        chat_system=Template(None, "chat", 1, "SYSTEM"),
        classifier=Template(None, "classifier", 1, "prior: $has_prior"),
    )


def config(provider):
    return {"configurable": {"deps": deps(provider)}}


async def test_classify_uses_the_provider_and_falls_back_on_failure():
    provider = ScriptedProvider()
    out = await classify({"message": "compare a vs b", "usage": Usage()}, config(provider))
    assert out["intent"] == "compare"
    assert provider.requests[0].system == "prior: no"
    assert out["usage"].prompt_tokens > 0
    down = ScriptedProvider(fail_before=frozenset({"classify"}))
    out = await classify({"message": "hello", "usage": Usage()}, config(down))
    assert out["intent"] == "smalltalk"


async def test_memory_window_and_routing():
    history = [{"role": "user", "content": str(i)} for i in range(6)]
    out = await load_memory({"history": history}, config(ScriptedProvider()))
    assert [t["content"] for t in out["memory"]] == ["4", "5"]
    assert route({"intent": "too_vague"}) == "clarify"
    assert route({"intent": "unknown-thing"}) == "retrieve"
    refused = await refuse({}, config(ScriptedProvider()))
    assert refused["request"] is None and "shopping" in refused["canned"]


class FakeSearchRepo:
    """``SearchRepo.search`` scripted per query; records every call."""

    def __init__(self, results: dict[str, tuple[list[Hit], str | None]]) -> None:
        self.results = results
        self.queries: list[tuple[str, int]] = []

    async def search(self, query, filters, mode, limit):
        self.queries.append((query, limit))
        hits, degraded = self.results.get(query, ([], None))
        return RawSearch(hits[:limit], mode, degraded, False)


def hit(product_id: uuid.UUID, name: str) -> Hit:
    return Hit(
        product_id=product_id,
        merchant_id=uuid.uuid4(),
        category_id=None,
        name=name,
        score=1.0,
        chunk_id=uuid.uuid4(),
        chunk_type="identity",
        content=name,
        match_reason="keyword",
    )


def compare_config(repo: FakeSearchRepo):
    d = deps(ScriptedProvider())
    d.search = SimpleNamespace(repo=repo)  # type: ignore[assignment]
    return {"configurable": {"deps": d}}


def compare_state(message: str, terms: list[str]) -> dict:
    return {
        "message": message,
        "filters": SearchFilters(),
        "classification": {"compare_terms": terms},
    }


async def test_compare_takes_the_best_distinct_product_per_term():
    a, b, c = (uuid.uuid4() for _ in range(3))
    repo = FakeSearchRepo(
        {
            "shoes": ([hit(a, "Shoe"), hit(b, "Boot")], None),
            # The best match for "boots" was already picked for "shoes": take the next.
            "boots": ([hit(a, "Shoe"), hit(b, "Boot")], "keyword_only"),
            "blender": ([hit(c, "Blender")], None),
            "kettle": ([hit(uuid.uuid4(), "Kettle")], None),
        }
    )
    out = await compare(
        compare_state("x", ["shoes", "boots", "blender", "kettle"]), compare_config(repo)
    )
    assert [h.product_id for h in out["candidates"]] == [a, b, c]
    assert out["degraded"] == "keyword_only" and out["retrieved"] is True
    assert repo.queries == [("shoes", 3), ("boots", 3), ("blender", 3)]  # at most three terms


async def test_compare_falls_back_to_the_whole_message_when_terms_find_too_little():
    a, b, c, d = (uuid.uuid4() for _ in range(4))
    repo = FakeSearchRepo(
        {
            "shoes": ([hit(a, "Shoe")], None),
            "unicorns": ([], None),
            "shoes vs unicorns": (
                [hit(a, "Shoe"), hit(b, "Boot"), hit(c, "Sandal"), hit(d, "Clog")],
                "keyword_only",
            ),
        }
    )
    out = await compare(
        compare_state("shoes vs unicorns", ["shoes", "unicorns"]), compare_config(repo)
    )
    # The term's pick keeps its place; the message fills up to three products.
    assert [h.product_id for h in out["candidates"]] == [a, b, c]
    assert out["degraded"] == "keyword_only"
    assert repo.queries[-1] == ("shoes vs unicorns", RETRIEVE_LIMIT)


async def test_compare_without_terms_or_results_is_empty_not_an_error():
    repo = FakeSearchRepo({})
    out = await compare(compare_state("compare these", []), compare_config(repo))
    assert out["candidates"] == [] and out["degraded"] is None
    assert repo.queries == [("compare these", RETRIEVE_LIMIT)]
