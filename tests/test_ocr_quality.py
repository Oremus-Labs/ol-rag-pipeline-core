from ol_rag_pipeline_core.ocr.quality import OcrQualityGate, assess_ocr_text_quality, passes_quality_gate


def test_assess_ocr_text_quality_empty() -> None:
    r = assess_ocr_text_quality("")
    assert r.looks_empty is True
    assert r.chars == 0


def test_passes_quality_gate_happy_path() -> None:
    gate = OcrQualityGate(min_chars_per_page=5, min_alpha_ratio=0.2, min_printable_ratio=0.9)
    r = assess_ocr_text_quality("Hello world")
    assert passes_quality_gate(r, gate) is True


def test_passes_quality_gate_low_alpha_ratio() -> None:
    gate = OcrQualityGate(min_chars_per_page=5, min_alpha_ratio=0.6, min_printable_ratio=0.9)
    r = assess_ocr_text_quality("1234567890")
    assert passes_quality_gate(r, gate) is False

