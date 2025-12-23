from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class Document:
    document_id: str
    source: str
    source_uri: str
    pipeline_version: Optional[str] = None
    content_fingerprint: Optional[str] = None

    canonical_url: Optional[str] = None
    title: Optional[str] = None
    author: Optional[str] = None
    published_year: Optional[int] = None
    language: Optional[str] = None
    content_type: Optional[str] = None
    is_scanned: Optional[bool] = None
    status: str = "discovered"
    canonical_sha256: Optional[str] = None
    canonical_etag: Optional[str] = None
    categories_json: Optional[dict] = None
    source_dataset: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass(frozen=True)
class Chunk:
    document_id: str
    pipeline_version: str
    chunk_id: str
    chunk_index: int
    text_uri: Optional[str] = None
    sha256: Optional[str] = None
    token_count: Optional[int] = None
    section_path: Optional[str] = None
    page_start: Optional[int] = None
    page_end: Optional[int] = None
    locator: Optional[str] = None


@dataclass(frozen=True)
class DocumentLink:
    document_id: str
    link_type: str
    url: str
    label: Optional[str] = None

