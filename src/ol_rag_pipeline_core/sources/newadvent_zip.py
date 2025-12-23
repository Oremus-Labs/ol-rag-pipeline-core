from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass


@dataclass(frozen=True)
class ZipEntry:
    path: str
    content_type: str | None
    body: bytes


def iter_zip_entries(zip_bytes: bytes, *, limit: int = 200) -> list[ZipEntry]:
    entries: list[ZipEntry] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for i, info in enumerate(zf.infolist()):
            if i >= limit:
                break
            if info.is_dir():
                continue
            body = zf.read(info.filename)
            ct = "text/html" if info.filename.lower().endswith((".html", ".htm")) else None
            entries.append(ZipEntry(path=info.filename, content_type=ct, body=body))
    return entries

