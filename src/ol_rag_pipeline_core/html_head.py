from __future__ import annotations

import re
from dataclasses import dataclass
from html.parser import HTMLParser


@dataclass(frozen=True)
class HtmlHeadMetadata:
    title: str | None
    canonical_url: str | None
    description: str | None
    open_graph: dict[str, str]
    meta_by_name: dict[str, str]


def _norm(s: str | None) -> str:
    return (s or "").strip()


class _HeadParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.in_head = False
        self.in_title = False
        self._title_parts: list[str] = []
        self.canonical_url: str | None = None
        self.description: str | None = None
        self.open_graph: dict[str, str] = {}
        self.meta_by_name: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attrs_dict = {k.lower(): v for k, v in attrs}

        if tag == "head":
            self.in_head = True
            return
        if tag == "title" and self.in_head:
            self.in_title = True
            return

        if not self.in_head:
            return

        if tag == "link":
            rel = _norm(attrs_dict.get("rel")).lower()
            href = _norm(attrs_dict.get("href"))
            if rel == "canonical" and href and not self.canonical_url:
                self.canonical_url = href
            return

        if tag == "meta":
            content = _norm(attrs_dict.get("content"))
            if not content:
                return
            name = _norm(attrs_dict.get("name")).lower()
            prop = _norm(attrs_dict.get("property")).lower()
            if name:
                if name == "description" and not self.description:
                    self.description = content
                if name not in self.meta_by_name:
                    self.meta_by_name[name] = content
            if prop.startswith("og:") and prop not in self.open_graph:
                self.open_graph[prop] = content

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag == "title":
            self.in_title = False
        if tag == "head":
            self.in_head = False

    def handle_data(self, data: str) -> None:
        if self.in_head and self.in_title:
            self._title_parts.append(data)

    def title(self) -> str | None:
        text = re.sub(r"\s+", " ", "".join(self._title_parts)).strip()
        return text or None


def extract_html_head_metadata(body: bytes, *, max_bytes: int = 262144) -> HtmlHeadMetadata:
    """
    Lightweight HTML <head> metadata extraction using stdlib only.

    Safe for dirty HTML: we decode with replacement and parse just the first `max_bytes`.
    """
    try:
        text = body[:max_bytes].decode("utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        text = ""

    parser = _HeadParser()
    try:
        parser.feed(text)
    except Exception:  # noqa: BLE001
        pass

    return HtmlHeadMetadata(
        title=parser.title(),
        canonical_url=parser.canonical_url,
        description=parser.description,
        open_graph=parser.open_graph,
        meta_by_name=parser.meta_by_name,
    )

