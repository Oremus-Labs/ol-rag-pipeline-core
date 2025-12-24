from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    message: str
    details: dict[str, object] | None = None


_ALPHA_RE = re.compile(r"[A-Za-z]")


def validate_extracted_text(
    *,
    text: str,
    content_type: str | None,
    min_chars: int = 200,
    min_alpha_ratio: float = 0.15,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []

    normalized = (text or "").strip()
    if not normalized:
        issues.append(ValidationIssue(code="extraction_empty", message="Extracted text is empty."))
        return issues

    if len(normalized) < min_chars:
        issues.append(
            ValidationIssue(
                code="extraction_too_short",
                message="Extracted text is too short.",
                details={"chars": len(normalized), "min_chars": min_chars},
            )
        )

    alpha = len(_ALPHA_RE.findall(normalized))
    alpha_ratio = alpha / max(len(normalized), 1)
    if alpha_ratio < min_alpha_ratio:
        issues.append(
            ValidationIssue(
                code="extraction_low_alpha_ratio",
                message="Extracted text looks low-quality (low alphabetic ratio).",
                details={
                    "alpha_ratio": round(alpha_ratio, 4),
                    "min_alpha_ratio": min_alpha_ratio,
                    "content_type": content_type or None,
                },
            )
        )

    return issues

