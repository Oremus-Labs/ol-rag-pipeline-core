from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from urllib.parse import urljoin

import httpx


@dataclass(frozen=True)
class WebDavFile:
    href: str
    etag: str | None
    size: int | None
    content_type: str | None

    @property
    def name(self) -> str:
        return self.href.rstrip("/").split("/")[-1]


def _propfind_body() -> bytes:
    return b"""<?xml version="1.0" encoding="utf-8" ?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:getetag/>
    <d:getcontentlength/>
    <d:getcontenttype/>
    <d:resourcetype/>
  </d:prop>
</d:propfind>
"""


def list_webdav_files(
    *,
    webdav_base_url: str,
    folder_path: str,
    username: str,
    app_password: str,
    timeout_s: float = 20.0,
) -> list[WebDavFile]:
    base = webdav_base_url if webdav_base_url.endswith("/") else webdav_base_url + "/"
    folder = folder_path.strip("/") + "/"
    url = urljoin(base, folder)

    headers = {"Depth": "1"}
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        r = client.request(
            "PROPFIND",
            url,
            headers=headers,
            content=_propfind_body(),
            auth=(username, app_password),
        )
        r.raise_for_status()

    root = ET.fromstring(r.text)
    ns = {"d": "DAV:"}
    files: list[WebDavFile] = []
    for resp in root.findall("d:response", ns):
        href = resp.findtext("d:href", default="", namespaces=ns)
        if not href or href.endswith("/"):
            continue
        prop = resp.find("d:propstat/d:prop", ns)
        if prop is None:
            continue
        res_type = prop.find("d:resourcetype", ns)
        if res_type is not None and list(res_type):
            continue  # directory
        etag = prop.findtext("d:getetag", default=None, namespaces=ns)
        size_text = prop.findtext("d:getcontentlength", default=None, namespaces=ns)
        content_type = prop.findtext("d:getcontenttype", default=None, namespaces=ns)
        size = int(size_text) if size_text and size_text.isdigit() else None
        files.append(WebDavFile(href=href, etag=etag, size=size, content_type=content_type))
    return files


def download_webdav_file(
    *,
    base_url: str,
    href: str,
    username: str,
    app_password: str,
    timeout_s: float = 60.0,
) -> bytes:
    url = urljoin(base_url, href)
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        r = client.get(url, auth=(username, app_password))
        r.raise_for_status()
        return r.content

