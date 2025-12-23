from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import httpx


@dataclass(frozen=True)
class WebPage:
    url: str
    content_type: str | None
    body: bytes


def fetch_pages(urls: Iterable[str], *, timeout_s: float = 20.0) -> list[WebPage]:
    pages: list[WebPage] = []
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        for url in urls:
            r = client.get(url)
            r.raise_for_status()
            ct = r.headers.get("content-type")
            pages.append(WebPage(url=url, content_type=ct, body=r.content))
    return pages

