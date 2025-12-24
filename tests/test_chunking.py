from ol_rag_pipeline_core.chunking import chunk_text
from ol_rag_pipeline_core.embedding import EmbeddingClient


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


def test_embedding_client_batching() -> None:
    client = EmbeddingClient(base_url="http://example.invalid", max_batch_texts=3, max_batch_chars=10)
    batches = client._batch(["aaaa", "bbbb", "cccc", "dddd"])

    # With max_batch_chars=10, first two fit (8 chars), adding third would exceed (12).
    assert batches == [["aaaa", "bbbb"], ["cccc", "dddd"]]
