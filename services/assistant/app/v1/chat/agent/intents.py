"""Deterministic intent rules, classifier-output parsing and follow-up resolution.

The rules are the scripted provider's classifier (passed as a hint, so both
providers go through the same parse) and the fallback whenever a real model's
answer cannot be parsed or the provider is down — so routing never fails.
Pure functions: no I/O.
"""

import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any, TypedDict

from app.v1.chat.enums import Intent


class Classification(TypedDict):
    intent: str
    is_specific_enough: bool
    refers_to_prior: bool
    compare_terms: list[str]


_GREETING = re.compile(
    r"^\s*(hi|hello|hey|hiya|thanks|thank you|thx|good (morning|afternoon|evening)|bye)\b"
    r"[\s!.,?]*(there|so much|a lot)?[\s!.,?]*$",
    re.IGNORECASE,
)
_COMPARE = re.compile(r"\bcompare\b|\bvs\.?\b|\bversus\b|\bdifference between\b", re.IGNORECASE)
_OUT_OF_SCOPE = re.compile(
    r"\b(weather|politic\w*|election|president|stock market|bitcoin|crypto\w*|recipe|"
    r"homework|poem|joke|write (me )?(a |some )?code|python|javascript|medical advice|"
    r"diagnos\w*|lawyer|legal advice|capital of|translate)\b",
    re.IGNORECASE,
)
_FOLLOW_UP = re.compile(
    r"\b(which (one|is|of)|cheapest|most expensive|priciest|that one|this one|the first|"
    r"the second|the third|first one|second one|third one|last one|it|them|those|these|"
    r"in stock|available|more about)\b",
    re.IGNORECASE,
)
_WORD = re.compile(r"[a-z0-9]+")
_STOP = frozenset(
    "a an the some something anything for my me i to of and or with in on at is are it "
    "want need looking look find get buy show please good nice any thing stuff give".split()
)
_ORDINALS = {"first": 0, "second": 1, "third": 2}


def content_words(message: str) -> list[str]:
    return [w for w in _WORD.findall(message.lower()) if w not in _STOP]


def compare_terms(message: str) -> list[str]:
    body = re.sub(r"^.*?\bcompare\b", "", message, flags=re.IGNORECASE).strip(" ?.!:")
    body = re.sub(r"\bdifference between\b", "", body, flags=re.IGNORECASE)
    parts = re.split(r"\bvs\.?\b|\bversus\b|\band\b|,|\bwith\b", body, flags=re.IGNORECASE)
    return [p.strip(" ?.!:") for p in parts if p.strip(" ?.!:")][:3]


def rule_classify(message: str, *, has_prior: bool) -> Classification:
    words = content_words(message)
    if _GREETING.match(message):
        return Classification(
            intent=Intent.SMALLTALK,
            is_specific_enough=True,
            refers_to_prior=False,
            compare_terms=[],
        )
    if _OUT_OF_SCOPE.search(message):
        return Classification(
            intent=Intent.OUT_OF_SCOPE,
            is_specific_enough=True,
            refers_to_prior=False,
            compare_terms=[],
        )
    if _COMPARE.search(message):
        return Classification(
            intent=Intent.COMPARE,
            is_specific_enough=True,
            refers_to_prior=False,
            compare_terms=compare_terms(message),
        )
    if has_prior and _FOLLOW_UP.search(message) and len(words) <= 6:
        return Classification(
            intent=Intent.FOLLOW_UP,
            is_specific_enough=True,
            refers_to_prior=True,
            compare_terms=[],
        )
    if len(words) <= 1:
        return Classification(
            intent=Intent.TOO_VAGUE,
            is_specific_enough=False,
            refers_to_prior=False,
            compare_terms=[],
        )
    return Classification(
        intent=Intent.PRODUCT_SEARCH,
        is_specific_enough=len(words) >= 2,
        refers_to_prior=False,
        compare_terms=[],
    )


def parse_classification(text: str, fallback: Classification, *, has_prior: bool) -> Classification:
    """The model's JSON, validated; ``fallback`` for anything unusable."""
    match = re.search(r"\{.*\}", text or "", re.DOTALL)
    if not match:
        return fallback
    try:
        data: dict[str, Any] = json.loads(match.group(0))
    except json.JSONDecodeError:
        return fallback
    intent = data.get("intent")
    if not Intent.has(intent):
        return fallback
    if intent == Intent.FOLLOW_UP and not has_prior:
        intent = Intent.PRODUCT_SEARCH
    terms = data.get("compare_terms") or []
    return Classification(
        intent=str(intent),
        is_specific_enough=bool(data.get("is_specific_enough", True)),
        refers_to_prior=bool(data.get("refers_to_prior", False)),
        compare_terms=[str(t)[:100] for t in terms if isinstance(t, str)][:3],
    )


def _price(source: dict[str, Any]) -> Decimal | None:
    try:
        return Decimal(str(source["price"])) if source.get("price") is not None else None
    except (InvalidOperation, KeyError):
        return None


def resolve_follow_up(message: str, sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The subset and order of the prior results a follow-up refers to (G3 (0005))."""
    text = message.lower()
    for word, index in _ORDINALS.items():
        if re.search(rf"\b{word}\b", text):
            return sources[index : index + 1] if index < len(sources) else sources
    if "last" in text.split() and sources:
        return sources[-1:]
    priced = [s for s in sources if _price(s) is not None]
    if "cheapest" in text or "least expensive" in text or "lowest price" in text:
        return sorted(priced, key=lambda s: _price(s) or Decimal(0))[:1] or sources
    if "most expensive" in text or "priciest" in text:
        return sorted(priced, key=lambda s: _price(s) or Decimal(0), reverse=True)[:1] or sources
    if "in stock" in text or "available" in text:
        return [s for s in sources if s.get("in_stock")] or sources
    return sources
