from __future__ import annotations

import zipfile
from io import BytesIO

from ol_rag_pipeline_core.calibre.export import (
    build_calibre_opf,
    build_epub_bytes,
    build_markdown,
)


def test_build_markdown_includes_front_matter_and_title() -> None:
    md = build_markdown(
        meta={"title": "Hello", "author": "A", "document_id": "x", "published_year": 1972},
        body_text="Line 1\n\nLine 2\n",
    )
    assert md.startswith("---\n")
    assert "\n# Hello\n" in md
    assert "Line 1" in md


def test_build_calibre_opf_contains_core_fields() -> None:
    opf = build_calibre_opf(
        title="T",
        authors=["A1", "A2"],
        language="en",
        published_year=1972,
        identifiers={"uuid": "u", "ol_document_id": "doc"},
        tags=["Tag1", "Tag2"],
        comments="c",
    ).decode("utf-8")
    assert "<dc:title>T</dc:title>" in opf
    assert "dc:creator" in opf
    assert "<dc:language>en</dc:language>" in opf
    assert "<dc:date>1972-01-01</dc:date>" in opf
    assert "ol_document_id" in opf


def test_build_epub_is_valid_zip_with_mimetype_first() -> None:
    epub = build_epub_bytes(title="T", authors=["A"], language="en", body_text="Hello\n\nWorld")
    z = zipfile.ZipFile(BytesIO(epub))
    names = z.namelist()
    assert names[0] == "mimetype"
    assert z.read("mimetype") == b"application/epub+zip"
    assert b"<html" in z.read("OEBPS/content.xhtml")
