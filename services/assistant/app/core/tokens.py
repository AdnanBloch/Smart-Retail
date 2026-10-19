"""Approximate token counting (AST-D33: tiktoken deferred).

One token per word or punctuation mark, plus one per four characters beyond
eight in long words. Within ~20% of BPE tokenisers on English product text,
which is enough for chunk sizing, context bounds and the scripted provider's
usage figures. The real provider reports exact usage, and that is what is billed.
"""

import re

_TOKEN = re.compile(r"\w+|[^\w\s]", re.UNICODE)


def count_tokens(text: str) -> int:
    total = 0
    for piece in _TOKEN.findall(text):
        total += 1 + max(0, len(piece) - 8) // 4
    return total


def split_words(text: str) -> list[str]:
    """Whitespace-separated words, the unit chunk windows are built from."""
    return text.split()
