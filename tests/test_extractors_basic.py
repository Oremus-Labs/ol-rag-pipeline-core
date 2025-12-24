from ol_rag_pipeline_core.extractors.basic import extract_text


def test_extract_text_from_html() -> None:
    html = b"<html><head><title>x</title></head><body><h1>Hello</h1><p>World</p></body></html>"
    res = extract_text(data=html, content_type="text/html", filename="x.html")
    assert res.is_scanned is False
    assert "Hello" in res.text
    assert "World" in res.text


def test_unknown_binary_routes_to_scanned() -> None:
    res = extract_text(
        data=b"\x00\x01\x02",
        content_type="application/octet-stream",
        filename="x.bin",
    )
    assert res.is_scanned is True
