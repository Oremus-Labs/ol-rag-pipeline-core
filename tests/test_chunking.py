from ol_rag_pipeline_core.chunking import chunk_text


def test_chunk_text_empty() -> None:
    assert chunk_text(text="") == []


def test_chunk_text_respects_max_tokens_and_overlap() -> None:
    text = " ".join(str(i) for i in range(1000))
    chunks = chunk_text(text=text, max_tokens=100, overlap_tokens=10)

    assert len(chunks) > 1
    assert all(c.token_count <= 100 for c in chunks)

    # Deterministic: same input -> same output sizes
    chunks2 = chunk_text(text=text, max_tokens=100, overlap_tokens=10)
    assert [c.token_count for c in chunks] == [c.token_count for c in chunks2]

