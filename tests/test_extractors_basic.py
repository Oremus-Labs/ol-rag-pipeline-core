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


def test_extract_text_strips_vatican_language_nav_block() -> None:
    html = b"""
    <html>
      <body>
        <div>La Santa Sede</div>
        <div>italiano</div>
        <div>Fran\xc3\xa7ais</div>
        <div>English</div>
        <div>Deutsch</div>
        <div>Magisterium</div>
        <div>Calendario</div>
        <div>Vatican News - Radio Vaticana</div>
        <h1>GIOVANNI PAOLO II</h1>
        <h2>UDIENZA GENERALE</h2>
        <p>1. Quest'oggi desidero dedicare la consueta catechesi.</p>
      </body>
    </html>
    """
    res = extract_text(data=html, content_type="text/html", filename="x.html")
    assert "Quest'oggi desidero" in res.text
    assert "La Santa Sede" not in res.text
    assert "Vatican News" not in res.text


def test_extract_text_strips_pdf_generation_boilerplate_without_nav_mode() -> None:
    html = b"""
    <html>
      <body>
        <div>24 ottobre 1990</div>
        <div>Generazione pdf in corso.....</div>
        <h1>GIOVANNI PAOLO II</h1>
        <p>1. Nel suo intervento nella sinagoga di Nazaret.</p>
      </body>
    </html>
    """
    res = extract_text(data=html, content_type="text/html", filename="x.html")
    assert "Generazione pdf in corso" not in res.text
    assert "GIOVANNI PAOLO II" in res.text
    assert "Nel suo intervento" in res.text


def test_extract_text_strips_pdf_generation_and_lang_bullets() -> None:
    html = b"""
    <html>
      <body>
        <h1>POPE FRANCIS</h1>
        <div>PDF generation in progress.....</div>
        <div>&nbsp;-&nbsp; HR</div>
        <div>&nbsp;-&nbsp; PL</div>
        <p>Dear brothers and sisters, good morning!</p>
      </body>
    </html>
    """
    res = extract_text(data=html, content_type="text/html", filename="x.html")
    assert "PDF generation in progress" not in res.text
    assert "- HR" not in res.text
    assert "- PL" not in res.text
    assert "Dear brothers and sisters" in res.text


def test_unknown_binary_routes_to_scanned() -> None:
    res = extract_text(
        data=b"\x00\x01\x02",
        content_type="application/octet-stream",
        filename="x.bin",
    )
    assert res.is_scanned is True
