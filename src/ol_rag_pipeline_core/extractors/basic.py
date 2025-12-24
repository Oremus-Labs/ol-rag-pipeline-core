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
        self._skip_tag_stack: list[str] = []
        self.meta_refresh_url: str | None = None

    @staticmethod
    def _attrs_dict(attrs) -> dict[str, str]:  # noqa: ANN001
        out: dict[str, str] = {}
        for k, v in attrs or []:
            if isinstance(k, str) and isinstance(v, str):
                out[k.lower()] = v
        return out

    @staticmethod
    def _should_skip(tag: str, attrs: dict[str, str]) -> bool:
        skip_tags = {
            "script",
            "style",
            "nav",
            "header",
            "footer",
            "aside",
            "noscript",
            "form",
        }
        if tag in skip_tags:
            return True

        role = (attrs.get("role") or "").lower()
        if role in {"navigation", "banner", "contentinfo"}:
            return True
        return False

    @staticmethod
    def _is_meta_refresh(attrs: dict[str, str]) -> bool:
        equiv = (attrs.get("http-equiv") or "").lower()
        return equiv == "refresh"

    @staticmethod
    def _parse_meta_refresh_url(content: str) -> str | None:
        # content is commonly: "0; url=./page.htm"
        low = content.lower()
        if "url=" not in low:
            return None
        _, rest = low.split("url=", 1)
        url = rest.strip().strip("'\"")
        return url or None

    def handle_starttag(self, tag: str, attrs) -> None:  # noqa: ANN001
        tag = tag.lower()
        attrs_d = self._attrs_dict(attrs)

        void_tags = {
            "area",
            "base",
            "br",
            "col",
            "embed",
            "hr",
            "img",
            "input",
            "link",
            "meta",
            "param",
            "source",
            "track",
            "wbr",
        }
        if tag in void_tags:
            if self._skip_depth == 0 and tag == "br":
                self._chunks.append("\n")
            if self._skip_depth == 0 and tag == "meta" and self._is_meta_refresh(attrs_d):
                url = self._parse_meta_refresh_url(attrs_d.get("content", ""))
                if url and self.meta_refresh_url is None:
                    self.meta_refresh_url = url
            return

        should_skip = self._skip_depth > 0 or self._should_skip(tag, attrs_d)
        if should_skip:
            self._skip_depth += 1
            self._skip_tag_stack.append(tag)
            return

    def handle_startendtag(self, tag: str, attrs) -> None:  # noqa: ANN001
        # Normalize void tags which may be emitted as <tag/> by some sources.
        self.handle_starttag(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        if self._skip_depth > 0 and self._skip_tag_stack:
            # Pop until we close the most recent skipped element.
            tag = tag.lower()
            while self._skip_tag_stack:
                popped = self._skip_tag_stack.pop()
                self._skip_depth -= 1
                if popped == tag:
                    break
            return
        if tag.lower() in {"p", "br", "div", "li", "tr", "h1", "h2", "h3", "h4"}:
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
_LEADING_A_RE = re.compile(r"^(?:A\\s+){1,}A$|^A$")
_LANG_LINE_RE = re.compile(r"^(?:[A-Z]{2}\\s*(?:-|–|—)\\s*){1,10}[A-Z]{2}$")
_LANG_CODE_RE = re.compile(r"^[A-Z]{2}$")
_COMMON_LANG_CODES = {"AR", "DE", "EN", "ES", "FR", "IT", "LA", "PT", "ZH"}
_PUNCT_ONLY_RE = re.compile(r"[^A-Za-z0-9]+")
_VATICAN_LANG_NAMES = {
    "italiano",
    "français",
    "english",
    "português",
    "español",
    "deutsch",
    "latine",
    "العربيّة",
    "中文",
}
_VATICAN_NAV_PHRASES = {
    "la santa sede",
    "the holy see",
    "magisterium",
    "calendario",
    "celebrazioni liturgiche",
    "biglietti udienze e celebrazioni pontificie",
    "sommi pontefici",
    "collegio cardinalizio",
    "curia romana e altre organizzazioni",
    "sinodo",
    "sala stampa",
    "vatican news - radio vaticana",
    "l'osservatore romano",
    "generazione pdf in corso.....",
}


def _looks_like_vatican_nav(head_lines: list[str]) -> bool:
    """
    Heuristic: Vatican pages often include a large language/menu block before the actual content.
    We only enable the more aggressive stripping when this pattern is present.
    """
    hits = 0
    for line in head_lines[:50]:
        stripped = line.strip()
        if not stripped:
            continue
        low = stripped.lower()
        if "la santa sede" in low:
            hits += 3
        if low in _VATICAN_LANG_NAMES:
            hits += 1
        if low in _VATICAN_NAV_PHRASES:
            hits += 1
        if "vatican" in low and "news" in low:
            hits += 1
    return hits >= 5


def _strip_leading_boilerplate_lines(text: str, *, max_scan_lines: int = 120) -> str:
    """
    Removes common nav/toolbar artifacts that sometimes survive HTML-to-text conversion.

    This is intentionally conservative and only considers the first N lines.
    """
    lines = text.splitlines()
    vatican_mode = _looks_like_vatican_nav(lines)
    kept: list[str] = []
    scanned = 0
    for line in lines:
        scanned += 1
        stripped = line.strip()
        if scanned <= max_scan_lines:
            if not stripped:
                continue
            if stripped in {"×"}:
                continue
            if _LEADING_A_RE.match(stripped):
                continue
            if _LANG_LINE_RE.match(stripped):
                continue
            if _LANG_CODE_RE.match(stripped) and stripped in _COMMON_LANG_CODES:
                continue
            if vatican_mode:
                low = stripped.lower().strip()
                if low.startswith("× "):
                    low = low.removeprefix("× ").strip()
                if low in _VATICAN_LANG_NAMES:
                    continue
                if low in _VATICAN_NAV_PHRASES:
                    continue
            codes = re.findall(r"[A-Z]{2}", stripped)
            if len(codes) == 1 and codes[0] in _COMMON_LANG_CODES:
                remainder = stripped.replace(codes[0], "")
                remainder = _PUNCT_ONLY_RE.sub("", remainder)
                if not remainder and len(stripped) <= 12:
                    continue
            if len(codes) >= 2 and all(c in _COMMON_LANG_CODES for c in codes):
                remainder = stripped
                for c in codes:
                    remainder = remainder.replace(c, "")
                remainder = _PUNCT_ONLY_RE.sub("", remainder)
                if not remainder:
                    continue
        kept.append(line)
    return "\n".join(kept)


def _normalize_text(text: str, *, max_chars: int = 2_000_000) -> str:
    text = text.replace("\x00", "")
    text = text.replace("\u200b", "")
    text = _WS_RE.sub(" ", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _NL_RE.sub("\n\n", text)
    text = _strip_leading_boilerplate_lines(text)
    text = text.strip()
    if len(text) > max_chars:
        text = text[:max_chars]
    return text


def _decode_text(data: bytes) -> str:
    return data.decode("utf-8", errors="replace")


def _extract_html(data: bytes) -> tuple[str, str | None]:
    parser = _HTMLToText()
    parser.feed(_decode_text(data))
    return parser.text(), parser.meta_refresh_url


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
        raw_text, meta_refresh_url = _extract_html(data)
        text = _normalize_text(raw_text)
        return ExtractResult(
            extractor="html_parser",
            is_scanned=False,
            text=text,
            metrics={"chars": len(text), "meta_refresh_url": meta_refresh_url},
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
