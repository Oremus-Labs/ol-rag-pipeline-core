from ol_rag_pipeline_core.chunking import chunk_pages


def test_chunk_pages_assigns_page_metadata() -> None:
    pages = [(1, "a " * 120), (2, "b " * 50)]
    chunks = chunk_pages(pages=pages, max_tokens=100, overlap_tokens=10)
    assert chunks
    assert all(c.page_start == c.page_end for c in chunks)
    assert all(c.locator and c.locator.startswith("p.") for c in chunks)
    assert all(c.token_count <= 100 for c in chunks)


def test_chunk_pages_deterministic() -> None:
    pages = [(1, "hello " * 120), (2, "world " * 80)]
    c1 = chunk_pages(pages=pages, max_tokens=50, overlap_tokens=5)
    c2 = chunk_pages(pages=pages, max_tokens=50, overlap_tokens=5)
    assert [c.text for c in c1] == [c.text for c in c2]

