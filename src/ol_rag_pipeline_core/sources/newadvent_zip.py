from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass


@dataclass(frozen=True)
class ZipEntry:
    path: str
    content_type: str | None
    body: bytes


_META_REFRESH_RE = re.compile(r"http-equiv\s*=\s*([\"'])refresh\1", re.IGNORECASE)


def _is_ignored_path(path: str) -> bool:
    norm = path.replace("\\\\", "/")
    parts = [p for p in norm.split("/") if p]
    if not parts:
        return True
    if parts[0].lower() == "__macosx":
        return True
    name = parts[-1]
    lower = name.lower()
    if lower in {".ds_store", "thumbs.db"}:
        return True
    if name.startswith("."):
        return True
    return False


def _is_html_meta_refresh_stub(body: bytes) -> bool:
    if len(body) > 2000:
        return False
    try:
        text = body.decode("utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        return False
    return bool(_META_REFRESH_RE.search(text))


def iter_zip_entries(zip_bytes: bytes, *, limit: int = 200) -> list[ZipEntry]:
    entries: list[ZipEntry] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for i, info in enumerate(zf.infolist()):
            if i >= limit:
                break
            if info.is_dir():
                continue
            if _is_ignored_path(info.filename):
                continue
            body = zf.read(info.filename)
            ct = "text/html" if info.filename.lower().endswith((".html", ".htm")) else None
            if ct and _is_html_meta_refresh_stub(body):
                continue
            entries.append(ZipEntry(path=info.filename, content_type=ct, body=body))
    return entries
