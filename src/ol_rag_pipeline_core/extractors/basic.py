from __future__ import annotations

import io
import re
from dataclasses import dataclass
from html.parser import HTMLParser


@dataclass(frozen=True)
class ExtractResult:
    extractor: str
    is_scanned: bool
    text: str
    metrics: dict[str, object]


class _HTMLToText(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._chunks: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs) -> None:  # noqa: ANN001
        if tag in {"script", "style"}:
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style"} and self._skip_depth > 0:
            self._skip_depth -= 1
        if tag in {"p", "br", "div", "li", "tr", "h1", "h2", "h3", "h4"}:
            self._chunks.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        if data.strip():
            self._chunks.append(data)
            self._chunks.append(" ")

    def text(self) -> str:
        return "".join(self._chunks)


_WS_RE = re.compile(r"[ \t\r\f\v]+")
_NL_RE = re.compile(r"\n{3,}")


def _normalize_text(text: str, *, max_chars: int = 2_000_000) -> str:
    text = text.replace("\x00", "")
    text = text.replace("\u200b", "")
    text = _WS_RE.sub(" ", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _NL_RE.sub("\n\n", text)
    text = text.strip()
    if len(text) > max_chars:
        text = text[:max_chars]
    return text


def _decode_text(data: bytes) -> str:
    return data.decode("utf-8", errors="replace")


def _extract_html(data: bytes) -> str:
    parser = _HTMLToText()
    parser.feed(_decode_text(data))
    return parser.text()


def _extract_pdf_text(data: bytes) -> str:
    try:
        from pypdf import PdfReader  # type: ignore[import-not-found]
    except Exception:  # noqa: BLE001
        return ""
    try:
        reader = PdfReader(io.BytesIO(data))
        chunks: list[str] = []
        for page in reader.pages:
            try:
                chunks.append(page.extract_text() or "")
            except Exception:  # noqa: BLE001
                continue
        return "\n".join(chunks)
    except Exception:  # noqa: BLE001
        return ""


def extract_text(
    *,
    data: bytes,
    content_type: str | None,
    filename: str | None = None,
    pdf_text_min_chars: int = 200,
) -> ExtractResult:
    ct = (content_type or "").lower()
    name = (filename or "").lower()

    if ct.startswith("image/") or name.endswith((".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp")):
        return ExtractResult(
            extractor="noop_scanned",
            is_scanned=True,
            text="",
            metrics={"reason": "image_content_type"},
        )

    if "pdf" in ct or name.endswith(".pdf"):
        raw = _extract_pdf_text(data)
        text = _normalize_text(raw)
        if len(text) < pdf_text_min_chars:
            return ExtractResult(
                extractor="pypdf",
                is_scanned=True,
                text="",
                metrics={"pdf_text_chars": len(text), "reason": "low_pdf_text"},
            )
        return ExtractResult(
            extractor="pypdf",
            is_scanned=False,
            text=text,
            metrics={"pdf_text_chars": len(text)},
        )

    if "html" in ct or name.endswith((".html", ".htm")):
        text = _normalize_text(_extract_html(data))
        return ExtractResult(
            extractor="html_parser",
            is_scanned=False,
            text=text,
            metrics={"chars": len(text)},
        )

    if ct.startswith("text/") or name.endswith((".txt", ".md")):
        text = _normalize_text(_decode_text(data))
        return ExtractResult(
            extractor="text_utf8",
            is_scanned=False,
            text=text,
            metrics={"chars": len(text)},
        )

    # Unknown binary: treat as scanned for now (OCR or manual review).
    return ExtractResult(
        extractor="unknown_binary",
        is_scanned=True,
        text="",
        metrics={"content_type": content_type or None, "filename": filename or None},
    )

