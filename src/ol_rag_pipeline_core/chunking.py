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


def chunk_pages(
    *,
    pages: list[tuple[int, str]],
    max_tokens: int = 500,
    overlap_tokens: int = 50,
) -> list[TextChunk]:
    """
    Page-aware chunking.

    Each page is chunked independently and chunk metadata is tagged with:
    - page_start=page_end=<page_number>
    - locator="p.<page_number>"
    """
    chunks: list[TextChunk] = []
    for page_number, text in pages:
        page_chunks = chunk_text(text=text, max_tokens=max_tokens, overlap_tokens=overlap_tokens)
        for c in page_chunks:
            chunks.append(
                TextChunk(
                    text=c.text,
                    token_count=c.token_count,
                    section_path=c.section_path,
                    page_start=page_number,
                    page_end=page_number,
                    locator=f"p.{page_number}",
                )
            )
    return chunks
