from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

import httpx


@dataclass(frozen=True)
class ArchiveOrgResolvedDownload:
    details_url: str
    download_url: str
    content_type: str | None
    body: bytes


_DOWNLOAD_PDF_RE = re.compile(r'href="(?P<href>/download/[^"]+?\.pdf)"', re.IGNORECASE)


def is_archive_details_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:  # noqa: BLE001
        return False
    host = (parsed.hostname or "").lower()
    if host != "archive.org":
        return False
    return parsed.path.startswith("/details/")


def resolve_details_html_to_pdf_url(*, details_url: str, html_text: str) -> str | None:
    """
    Best-effort resolver for archive.org /details/<id> landing pages.
    """
    m = _DOWNLOAD_PDF_RE.search(html_text)
    if not m:
        return None
    href = m.group("href")
    if not href:
        return None
    return urljoin(details_url, href)


def resolve_and_download_pdf(
    *,
    client: httpx.Client,
    details_url: str,
) -> ArchiveOrgResolvedDownload | None:
    """
    Fetch /details/<id> HTML and download the first linked PDF under /download/.

    Returns None if no PDF can be found.
    """
    r = client.get(details_url)
    r.raise_for_status()
    if "text/html" not in (r.headers.get("content-type") or "").lower():
        return None
    html_text = r.text
    pdf_url = resolve_details_html_to_pdf_url(details_url=details_url, html_text=html_text)
    if not pdf_url:
        return None

    pdf = client.get(pdf_url)
    pdf.raise_for_status()
    ct = pdf.headers.get("content-type")
    return ArchiveOrgResolvedDownload(
        details_url=details_url,
        download_url=str(pdf.url),
        content_type=ct,
        body=pdf.content,
    )
