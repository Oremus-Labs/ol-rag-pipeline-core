from __future__ import annotations

import re
from dataclasses import dataclass


_ALPHA_RE = re.compile(r"[A-Za-z]")
_PRINTABLE_RE = re.compile(r"[\x20-\x7E\n\t]")


@dataclass(frozen=True)
class OcrQualityReport:
    chars: int
    alpha_chars: int
    alpha_ratio: float
    printable_ratio: float
    looks_empty: bool


@dataclass(frozen=True)
class OcrQualityGate:
    min_chars_per_page: int = 40
    min_alpha_ratio: float = 0.10
    min_printable_ratio: float = 0.85


def assess_ocr_text_quality(text: str) -> OcrQualityReport:
    normalized = (text or "").strip()
    if not normalized:
        return OcrQualityReport(
            chars=0,
            alpha_chars=0,
            alpha_ratio=0.0,
            printable_ratio=0.0,
            looks_empty=True,
        )

    chars = len(normalized)
    alpha = len(_ALPHA_RE.findall(normalized))
    printable = len(_PRINTABLE_RE.findall(normalized))
    return OcrQualityReport(
        chars=chars,
        alpha_chars=alpha,
        alpha_ratio=alpha / max(chars, 1),
        printable_ratio=printable / max(chars, 1),
        looks_empty=False,
    )


def passes_quality_gate(report: OcrQualityReport, gate: OcrQualityGate) -> bool:
    if report.looks_empty:
        return False
    if report.chars < gate.min_chars_per_page:
        return False
    if report.alpha_ratio < gate.min_alpha_ratio:
        return False
    if report.printable_ratio < gate.min_printable_ratio:
        return False
    return True

