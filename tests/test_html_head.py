from __future__ import annotations

from ol_rag_pipeline_core.html_head import extract_html_head_metadata


def test_extract_html_head_metadata_basic() -> None:
    html = b"""
    <html><head>
      <title>  Hello   World </title>
      <link rel="canonical" href="https://example.com/canonical" />
      <meta name="description" content="desc here" />
      <meta property="og:image" content="https://example.com/img.png" />
    </head><body>ok</body></html>
    """
    meta = extract_html_head_metadata(html)
    assert meta.title == "Hello World"
    assert meta.canonical_url == "https://example.com/canonical"
    assert meta.description == "desc here"
    assert meta.open_graph["og:image"] == "https://example.com/img.png"

