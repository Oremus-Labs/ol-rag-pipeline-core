from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import httpx

from ol_rag_pipeline_core.vpn import VpnRotationGuard


@dataclass(frozen=True)
class WebPage:
    url: str
    content_type: str | None
    body: bytes


def fetch_pages(
    urls: Iterable[str],
    *,
    timeout_s: float = 20.0,
    vpn_guard: VpnRotationGuard | None = None,
) -> list[WebPage]:
    pages: list[WebPage] = []
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        for url in urls:
            if vpn_guard:
                vpn_guard.before_request(url)
            r = client.get(url)
            r.raise_for_status()
            ct = r.headers.get("content-type")
            pages.append(WebPage(url=url, content_type=ct, body=r.content))
    return pages
