from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from html import escape as html_escape
from io import BytesIO
from typing import Iterable
from uuid import NAMESPACE_URL, uuid4, uuid5

from ol_rag_pipeline_core.models import Document
from ol_rag_pipeline_core.storage.s3 import S3Client


_WHITESPACE_RE = re.compile(r"[ \t]+")


@dataclass(frozen=True)
class CalibreExportInput:
    document: Document
    pipeline_version: str
    source_uri: str | None
    extracted_text: str
    raw_bytes: bytes
    raw_content_type: str | None
    raw_filename: str | None
    categories: list[str]


@dataclass(frozen=True)
class CalibreExportResult:
    base_prefix: str
    opf_uri: str
    markdown_uri: str
    epub_uri: str
    original_uri: str


def _safe_filename(name: str) -> str:
    name = (name or "").strip()
    if not name:
        return "document"
    # Keep it filesystem-friendly (Calibre imports are happier with simple names).
    name = re.sub(r"[^\w\s.-]+", "", name, flags=re.UNICODE)
    name = _WHITESPACE_RE.sub(" ", name).strip()
    return name[:160] or "document"


def _authors_list(author: str | None) -> list[str]:
    if not author:
        return ["Unknown"]
    # Calibre typically stores authors as "Last, First" but accepts plain strings.
    parts = [a.strip() for a in re.split(r"[;|]", author) if a.strip()]
    return parts or [author.strip()]


def _guess_original_ext(content_type: str | None, filename: str | None) -> str:
    low = (content_type or "").lower()
    if "pdf" in low:
        return ".pdf"
    if "html" in low:
        return ".html"
    if filename:
        if "." in filename:
            ext = "." + filename.rsplit(".", 1)[-1].lower()
            if ext in {".pdf", ".html", ".htm"}:
                return ext
    return ".bin"


def _normalize_text(text: str) -> str:
    text = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    return text.strip() + "\n"


def _iter_paragraphs(text: str) -> Iterable[str]:
    lines = _normalize_text(text).splitlines()
    buf: list[str] = []
    for line in lines:
        if not line.strip():
            if buf:
                yield " ".join([l.strip() for l in buf if l.strip()])
                buf = []
            continue
        buf.append(line)
    if buf:
        yield " ".join([l.strip() for l in buf if l.strip()])


def build_markdown(*, meta: dict[str, str | int | None], body_text: str) -> str:
    body_text = _normalize_text(body_text)
    front = ["---"]
    for k, v in meta.items():
        if v is None or v == "":
            continue
        front.append(f"{k}: {v}")
    front.append("---\n")
    title = str(meta.get("title") or "").strip()
    md = "\n".join(front)
    if title:
        md += f"# {title}\n\n"
    md += body_text
    return md


def build_calibre_opf(
    *,
    title: str,
    authors: list[str],
    language: str | None,
    published_year: int | None,
    identifiers: dict[str, str],
    tags: list[str],
    comments: str,
) -> bytes:
    # Minimal OPF that Calibre (and Calibre-Web import) can ingest. This is separate from the EPUB.
    # Calibre tolerates OPF 2.0 fairly well for imports.
    dc = "http://purl.org/dc/elements/1.1/"
    opf = "http://www.idpf.org/2007/opf"

    def x(s: str) -> str:
        return html_escape(s, quote=True)

    now = datetime.now(UTC).isoformat()
    uid = identifiers.get("uuid") or str(uuid4())

    date_tag = ""
    if published_year:
        date_tag = f"<dc:date>{published_year:04d}-01-01</dc:date>"

    creators = "\n".join(
        [f'<dc:creator opf:role="aut">{x(a)}</dc:creator>' for a in (authors or ["Unknown"])]
    )
    subjects = "\n".join([f"<dc:subject>{x(t)}</dc:subject>" for t in tags])
    ids = "\n".join([f'<dc:identifier opf:scheme="{x(k)}">{x(v)}</dc:identifier>' for k, v in identifiers.items()])

    xml = f"""<?xml version='1.0' encoding='utf-8'?>
<package xmlns="{opf}" unique-identifier="uuid_id" version="2.0">
  <metadata xmlns:dc="{dc}" xmlns:opf="{opf}">
    <dc:title>{x(title or 'Untitled')}</dc:title>
{creators}
    <dc:language>{x(language or 'und')}</dc:language>
{date_tag}
{ids}
{subjects}
    <dc:description>{x(comments)}</dc:description>
    <meta name="calibre:timestamp" content="{x(now)}"/>
    <meta name="calibre:uuid" content="{x(uid)}"/>
  </metadata>
</package>
"""
    return xml.encode("utf-8")


def build_epub_bytes(*, title: str, authors: list[str], language: str | None, body_markdown: str) -> bytes:
    # Very small EPUB3 generator (no external deps). Calibre-Web accepts this and renders it.
    # We store the markdown-derived text as simple paragraphs to keep it readable.
    text_paragraphs = "\n".join([f"<p>{html_escape(p)}</p>" for p in _iter_paragraphs(body_markdown)])

    content_xhtml = f"""<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="{html_escape(language or 'und')}">
  <head>
    <meta charset="utf-8" />
    <title>{html_escape(title or 'Untitled')}</title>
  </head>
  <body>
    <h1>{html_escape(title or 'Untitled')}</h1>
    <p><em>{html_escape(", ".join(authors or ['Unknown']))}</em></p>
    {text_paragraphs}
  </body>
</html>
"""

    book_uuid = str(uuid4())
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    package_opf = f"""<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" unique-identifier="bookid" version="3.0">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:identifier id="bookid">urn:uuid:{book_uuid}</dc:identifier>
    <dc:title>{html_escape(title or 'Untitled')}</dc:title>
    <dc:language>{html_escape(language or 'und')}</dc:language>
    <meta property="dcterms:modified">{now}</meta>
  </metadata>
  <manifest>
    <item id="content" href="content.xhtml" media-type="application/xhtml+xml"/>
    <item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>
  </manifest>
  <spine>
    <itemref idref="nav"/>
    <itemref idref="content"/>
  </spine>
</package>
"""

    nav_xhtml = """<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Navigation</title></head>
  <body>
    <nav epub:type="toc" xmlns:epub="http://www.idpf.org/2007/ops">
      <ol>
        <li><a href="content.xhtml">Content</a></li>
      </ol>
    </nav>
  </body>
</html>
"""

    container_xml = """<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="OEBPS/package.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
"""

    out = BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        # Per spec: mimetype must be stored uncompressed and first.
        zf.writestr("mimetype", "application/epub+zip", compress_type=zipfile.ZIP_STORED)
        zf.writestr("META-INF/container.xml", container_xml, compress_type=zipfile.ZIP_DEFLATED)
        zf.writestr("OEBPS/package.opf", package_opf, compress_type=zipfile.ZIP_DEFLATED)
        zf.writestr("OEBPS/nav.xhtml", nav_xhtml, compress_type=zipfile.ZIP_DEFLATED)
        zf.writestr("OEBPS/content.xhtml", content_xhtml, compress_type=zipfile.ZIP_DEFLATED)
    return out.getvalue()


class CalibreExporter:
    def __init__(self, *, calibre_s3: S3Client, calibre_prefix: str):
        self._s3 = calibre_s3
        self._prefix = (calibre_prefix or "").strip().strip("/")

    def export(self, inp: CalibreExportInput) -> CalibreExportResult:
        doc = inp.document
        title = (doc.title or "").strip() or "Untitled"
        authors = _authors_list(doc.author)
        tags = sorted({t for t in (inp.categories or []) if t and not t.startswith("_")})[:50]

        stable_uuid = str(uuid5(NAMESPACE_URL, f"ol-etl:{inp.pipeline_version}:{doc.document_id}"))
        identifiers: dict[str, str] = {
            "uuid": stable_uuid,
            "ol_document_id": doc.document_id,
        }
        if inp.source_uri:
            identifiers["source_uri"] = inp.source_uri
        if doc.canonical_url:
            identifiers["canonical_url"] = doc.canonical_url

        comments = "\n".join(
            [
                f"document_id: {doc.document_id}",
                f"pipeline_version: {inp.pipeline_version}",
                f"source: {doc.source}",
                f"source_uri: {inp.source_uri or ''}".strip(),
                f"canonical_url: {doc.canonical_url or ''}".strip(),
            ]
        ).strip()

        meta = {
            "title": title,
            "author": ", ".join(authors),
            "language": doc.language or "",
            "published_year": doc.published_year,
            "source": doc.source,
            "source_uri": inp.source_uri,
            "canonical_url": doc.canonical_url,
            "document_id": doc.document_id,
            "pipeline_version": inp.pipeline_version,
        }
        md = build_markdown(meta=meta, body_text=inp.extracted_text)
        epub = build_epub_bytes(title=title, authors=authors, language=doc.language, body_markdown=md)
        opf = build_calibre_opf(
            title=title,
            authors=authors,
            language=doc.language,
            published_year=doc.published_year,
            identifiers=identifiers,
            tags=tags,
            comments=comments,
        )

        safe = _safe_filename(title)
        ext = _guess_original_ext(inp.raw_content_type, inp.raw_filename)
        base_prefix = "/".join(
            p
            for p in [
                self._prefix,
                inp.pipeline_version,
                doc.source,
                doc.document_id,
            ]
            if p
        )
        # Files are intentionally stable/deterministic so reruns overwrite instead of duplicating.
        opf_uri = self._s3.put_bytes(f"{base_prefix}/metadata.opf", opf, content_type="application/oebps-package+xml")
        md_uri = self._s3.put_bytes(
            f"{base_prefix}/{safe}.md",
            md.encode("utf-8"),
            content_type="text/markdown; charset=utf-8",
        )
        epub_uri = self._s3.put_bytes(
            f"{base_prefix}/{safe}.epub",
            epub,
            content_type="application/epub+zip",
        )
        original_uri = self._s3.put_bytes(
            f"{base_prefix}/original{ext}",
            inp.raw_bytes,
            content_type=inp.raw_content_type or "application/octet-stream",
        )

        return CalibreExportResult(
            base_prefix=base_prefix,
            opf_uri=opf_uri,
            markdown_uri=md_uri,
            epub_uri=epub_uri,
            original_uri=original_uri,
        )
