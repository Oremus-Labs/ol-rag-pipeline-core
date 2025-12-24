from ol_rag_pipeline_core.extractors.basic import extract_text


def test_extract_text_from_html() -> None:
    html = b"<html><head><title>x</title></head><body><h1>Hello</h1><p>World</p></body></html>"
    res = extract_text(data=html, content_type="text/html", filename="x.html")
    assert res.is_scanned is False
    assert "Hello" in res.text
    assert "World" in res.text


def test_extract_text_skips_nav_and_header_blocks() -> None:
    html = b"""
    <html>
      <head><title>x</title></head>
      <body>
        <header>THE NAV SHOULD NOT APPEAR</header>
        <nav><ul><li>Home</li><li>Search</li></ul></nav>
        <main>
          <h1>Title</h1>
          <p>Body paragraph.</p>
        </main>
        <footer>FOOTER SHOULD NOT APPEAR</footer>
      </body>
    </html>
    """
    res = extract_text(data=html, content_type="text/html", filename="x.html")
    assert "Title" in res.text
    assert "Body paragraph" in res.text
    assert "NAV SHOULD NOT APPEAR" not in res.text
    assert "FOOTER SHOULD NOT APPEAR" not in res.text


def test_extract_text_html_meta_refresh_is_recorded() -> None:
    html = (
        b"<!DOCTYPE html><html><head>"
        b'<meta http-equiv="refresh" content="0; url=./1jo001.htm">'
        b"</head><body></body></html>"
    )
    res = extract_text(data=html, content_type="text/html", filename="1jo000.htm")
    assert res.metrics.get("meta_refresh_url") == "./1jo001.htm"


def test_unknown_binary_routes_to_scanned() -> None:
    res = extract_text(
        data=b"\x00\x01\x02",
        content_type="application/octet-stream",
        filename="x.bin",
    )
    assert res.is_scanned is True
