from __future__ import annotations

import io
import zipfile

from ol_rag_pipeline_core.sources.newadvent_zip import iter_zip_entries


def test_iter_zip_entries_skips_dotfiles_and_meta_refresh_stubs() -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("bible/.DS_Store", b"nope")
        zf.writestr(
            "bible/1jo000.htm",
            (
                b'<!DOCTYPE html><html><head><meta http-equiv="refresh" '
                b'content="0; url=./1jo001.htm"></head><body></body></html>'
            ),
        )
        zf.writestr("bible/1jo001.htm", b"<html><body><h1>OK</h1><p>Hello</p></body></html>")

    entries = iter_zip_entries(buf.getvalue(), limit=50)
    assert [e.path for e in entries] == ["bible/1jo001.htm"]
    assert entries[0].content_type == "text/html"
