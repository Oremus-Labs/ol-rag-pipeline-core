from ol_rag_pipeline_core.validation import validate_extracted_text


def test_validate_extracted_text_empty() -> None:
    issues = validate_extracted_text(text="", content_type="text/plain")
    assert [i.code for i in issues] == ["extraction_empty"]


def test_validate_extracted_text_too_short() -> None:
    issues = validate_extracted_text(text="hello", content_type="text/plain", min_chars=10)
    assert any(i.code == "extraction_too_short" for i in issues)

