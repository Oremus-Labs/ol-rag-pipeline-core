from __future__ import annotations

from ol_rag_pipeline_core.sources.archive_org import is_archive_details_url, resolve_details_html_to_pdf_url


def test_is_archive_details_url() -> None:
    assert is_archive_details_url("https://archive.org/details/some-item")
    assert not is_archive_details_url("https://archive.org/download/some-item/file.pdf")
    assert not is_archive_details_url("https://www.archive.org/details/some-item")
    assert not is_archive_details_url("https://example.com/details/some-item")


def test_resolve_details_html_to_pdf_url_finds_first_pdf() -> None:
    html = '<a href="/download/item/Item.pdf">pdf</a><a href="/download/item/Other.pdf">pdf2</a>'
    url = resolve_details_html_to_pdf_url(details_url="https://archive.org/details/item", html_text=html)
    assert url == "https://archive.org/download/item/Item.pdf"

