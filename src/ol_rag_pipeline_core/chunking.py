from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TextChunk:
    text: str
    token_count: int
    section_path: str | None = None
    page_start: int | None = None
    page_end: int | None = None
    locator: str | None = None


def _tokens(text: str) -> list[str]:
    return [t for t in (text or "").split() if t]


def chunk_text(
    *,
    text: str,
    max_tokens: int = 500,
    overlap_tokens: int = 50,
) -> list[TextChunk]:
    """
    Deterministic, lightweight chunking.

    Phase 5 does not yet require "true" semantic chunking; this produces stable windowed
    chunks with overlap so reruns overwrite rather than grow.
    """
    if max_tokens <= 0:
        raise ValueError("max_tokens must be > 0")
    if overlap_tokens < 0:
        raise ValueError("overlap_tokens must be >= 0")
    if overlap_tokens >= max_tokens:
        raise ValueError("overlap_tokens must be < max_tokens")

    words = _tokens(text.strip())
    if not words:
        return []

    chunks: list[TextChunk] = []
    start = 0
    while start < len(words):
        end = min(start + max_tokens, len(words))
        window = words[start:end]
        chunk_text_value = " ".join(window).strip()
        if chunk_text_value:
            chunks.append(TextChunk(text=chunk_text_value, token_count=len(window)))
        if end >= len(words):
            break
        start = end - overlap_tokens

    return chunks

