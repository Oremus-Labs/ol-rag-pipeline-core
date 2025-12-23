from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Document:
    document_id: str
    source: str
    source_uri: str
    pipeline_version: str | None = None
    content_fingerprint: str | None = None

    canonical_url: str | None = None
    title: str | None = None
    author: str | None = None
    published_year: int | None = None
    language: str | None = None
    content_type: str | None = None
    is_scanned: bool | None = None
    status: str = "discovered"
    canonical_sha256: str | None = None
    canonical_etag: str | None = None
    categories_json: dict | None = None
    source_dataset: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class Chunk:
    document_id: str
    pipeline_version: str
    chunk_id: str
    chunk_index: int
    text_uri: str | None = None
    sha256: str | None = None
    token_count: int | None = None
    section_path: str | None = None
    page_start: int | None = None
    page_end: int | None = None
    locator: str | None = None


@dataclass(frozen=True)
class DocumentLink:
    document_id: str
    link_type: str
    url: str
    label: str | None = None

