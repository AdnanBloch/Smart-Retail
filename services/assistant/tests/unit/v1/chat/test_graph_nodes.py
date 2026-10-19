"""Nodes in isolation with a constructed state (E2)."""

from types import SimpleNamespace

from app.core.clients.llm import ScriptedProvider
from app.core.config import Settings
from app.v1.chat.agent.graph import classify, load_memory, refuse, route
from app.v1.chat.agent.state import AgentDeps, Usage
from app.v1.prompts.repo import Template


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
