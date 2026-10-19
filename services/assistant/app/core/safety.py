"""Sanitisation, injection scan and output filter (E8, AST-D17, RFC 0009 §6.2).

Used by search/ (index time), chat/ (guard and assembly) and generations/
(output). Best-effort by design: a layer among several, never the only control.
"""

import re
import unicodedata

MAX_FIELD_CHARS = 20_000

_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_WS = re.compile(r"\s+")

# Instruction-like patterns aimed at a model. Matched case-insensitively.
_INJECTION = [
    r"ignore (all |any )?(the )?(previous|prior|above|earlier) (instructions|prompts?|rules)",
    r"disregard (all |any )?(the )?(previous|prior|above|system) (instructions|prompts?)",
    r"forget (all |your )?(previous |prior )?instructions",
    r"you are now (a|an|the) ",
    r"(reveal|print|show|repeat) (me )?(your|the) (system )?prompt",
    r"system prompt",
    r"</?(system|assistant|instructions?)>",
    r"\bact as (a|an) (?!gift)",
    r"new instructions?:",
]
_INJECTION_RE = re.compile("|".join(f"(?:{p})" for p in _INJECTION), re.IGNORECASE)

_URL = re.compile(r"\b(?:https?://|www\.)\S+", re.IGNORECASE)
_EMAIL = re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b")
_PHONE = re.compile(r"(?<!\w)\+?\d[\d\s().-]{8,}\d(?!\w)")


def sanitize_text(text: str | None, *, max_chars: int = MAX_FIELD_CHARS) -> str:
    """Strips control characters, normalises Unicode and whitespace, bounds length."""
    if not text:
        return ""
    cleaned = unicodedata.normalize("NFKC", str(text))
    cleaned = _CONTROL.sub(" ", cleaned)
    cleaned = _WS.sub(" ", cleaned).strip()
    return cleaned[:max_chars]


def looks_like_injection(text: str) -> bool:
    return bool(_INJECTION_RE.search(text or ""))


def fence(text: str) -> str:
    """Neutralises the delimiter tags the context assembly uses, so product
    text can never close its own ``<product>`` block."""
    return re.sub(r"</?\s*(product|products|context)\b[^>]*>", "", text, flags=re.IGNORECASE)


def filter_output(text: str) -> tuple[str, bool]:
    """Removes URLs, e-mail addresses and phone numbers from generated copy.

    Returns the filtered text and whether anything was removed.
    """
    filtered = _URL.sub("[link removed]", text)
    filtered = _EMAIL.sub("[contact removed]", filtered)
    filtered = _PHONE.sub("[contact removed]", filtered)
    return filtered, filtered != text
