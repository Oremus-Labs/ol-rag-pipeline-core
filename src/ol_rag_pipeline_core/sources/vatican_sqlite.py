from __future__ import annotations

import json
import sqlite3
import zlib
from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass(frozen=True)
class VaticanSqliteDocumentRow:
    """
    A single Vatican source record discovered from `vatican.db`.

    Note: `row_id` is the Vatican sqlite primary key. We do NOT use it as the
    pipeline `document_id` (we keep `document_id` stable from `source_uri`), but
    it is useful for provenance and metadata joins later.
    """

    row_id: str
    url: str
    title: str | None = None
    short_title: str | None = None
    year: int | None = None
    display_year: str | None = None
    author: str | None = None
    publisher: str | None = None
    bibliography: str | None = None
    language: str | None = None
    categories: list[str] | None = None
    raw_json: dict | None = None


def _list_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "select name from sqlite_master where type='table' order by name"
    ).fetchall()
    return [r[0] for r in rows]


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"pragma table_info({table})").fetchall()
    return [r[1] for r in rows]


def _safe_json_loads(value: str | None) -> object | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _normalize_hosts(hosts: list[str] | None) -> list[str] | None:
    if not hosts:
        return None
    out: list[str] = []
    for h in hosts:
        h = (h or "").strip().lower()
        if not h:
            continue
        out.append(h)
    return out or None


def _url_host(url: str) -> str | None:
    url = (url or "").strip()
    if not url:
        return None
    parsed = urlparse(url if "://" in url else f"https://{url.lstrip('/')}")
    host = (parsed.netloc or "").strip().lower()
    return host or None


def discover_document_rows(
    sqlite_path: str,
    *,
    limit: int | None = 100,
    hosts: list[str] | None = None,
    sample_per_host: int | None = None,
    partition_index: int | None = None,
    num_partitions: int | None = None,
) -> list[VaticanSqliteDocumentRow]:
    """
    Vatican sqlite adapter.

    Expected schema (from the generator) is a `documents` table with rich fields:
    id, title, year, author, link, language, categories_json, raw_json, etc.

    If the schema differs, fall back to a best-effort scan for a URL-like column.
    """
    effective_limit = None if limit is None or int(limit) <= 0 else int(limit)
    normalized_hosts = _normalize_hosts(hosts)
    effective_sample_per_host = (
        int(sample_per_host) if sample_per_host is not None and int(sample_per_host) > 0 else None
    )
    if (partition_index is None) != (num_partitions is None):
        raise ValueError("partition_index and num_partitions must be set together (or both unset)")
    if num_partitions is not None:
        num_partitions = int(num_partitions)
        partition_index = int(partition_index or 0)
        if num_partitions <= 0:
            raise ValueError("num_partitions must be > 0")
        if partition_index < 0 or partition_index >= num_partitions:
            raise ValueError("partition_index must be within [0, num_partitions)")

    def _partition_for_row(row: VaticanSqliteDocumentRow) -> int:
        if num_partitions is None:
            return 0
        try:
            rid = int(row.row_id)
            if rid >= 0:
                return rid % num_partitions
        except (TypeError, ValueError):
            pass
        return (zlib.crc32(row.url.encode("utf-8")) & 0xFFFFFFFF) % num_partitions

    with sqlite3.connect(sqlite_path) as conn:
        tables = _list_tables(conn)

        if "documents" in tables:
            cols = _table_columns(conn, "documents")
            required = {"id", "link"}
            if required.issubset(set(cols)):
                select_cols = [
                    "id",
                    "link",
                    "title",
                    "short_title",
                    "year",
                    "display_year",
                    "author",
                    "publisher",
                    "bibliography",
                    "language",
                    "categories_json",
                    "raw_json",
                ]
                # Keep only columns that exist to avoid breaking if generator changes.
                select_cols = [c for c in select_cols if c in cols]
                sql = f"select {', '.join(select_cols)} from documents where link is not null"
                rows = conn.execute(sql).fetchall()
                out: list[VaticanSqliteDocumentRow] = []
                for r in rows:
                    data = dict(zip(select_cols, r, strict=True))
                    url = data.get("link")
                    if not url:
                        continue
                    if normalized_hosts:
                        host = _url_host(str(url))
                        if not host or host not in normalized_hosts:
                            continue
                    categories = _safe_json_loads(data.get("categories_json"))
                    raw = _safe_json_loads(data.get("raw_json"))
                    if isinstance(categories, list):
                        categories = [str(c).strip() for c in categories if c is not None and str(c).strip()]
                    out.append(
                        VaticanSqliteDocumentRow(
                            row_id=str(data.get("id")),
                            url=str(url),
                            title=(str(data["title"]) if data.get("title") else None),
                            short_title=(str(data["short_title"]) if data.get("short_title") else None),
                            year=(int(data["year"]) if data.get("year") is not None else None),
                            display_year=(str(data["display_year"]) if data.get("display_year") else None),
                            author=(str(data["author"]) if data.get("author") else None),
                            publisher=(str(data["publisher"]) if data.get("publisher") else None),
                            bibliography=(str(data["bibliography"]) if data.get("bibliography") else None),
                            language=(str(data["language"]) if data.get("language") else None),
                            categories=categories if isinstance(categories, list) else None,
                            raw_json=raw if isinstance(raw, dict) else None,
                        )
                    )
                if num_partitions is not None:
                    out = [row for row in out if _partition_for_row(row) == partition_index]
                if not normalized_hosts:
                    return out[:effective_limit] if effective_limit is not None else out
                if not effective_sample_per_host:
                    return out[:effective_limit] if effective_limit is not None else out
                by_host: dict[str, list[VaticanSqliteDocumentRow]] = {h: [] for h in normalized_hosts}
                for row in out:
                    host = _url_host(row.url)
                    if not host:
                        continue
                    if host in by_host:
                        by_host[host].append(row)
                sampled: list[VaticanSqliteDocumentRow] = []
                for h in normalized_hosts:
                    sampled.extend(by_host.get(h, [])[:effective_sample_per_host])
                if num_partitions is not None:
                    sampled = [row for row in sampled if _partition_for_row(row) == partition_index]
                return sampled[:effective_limit] if effective_limit is not None else sampled

        # Fallback: find the first table containing a URL-like column and return up to `limit` rows.
        for table in tables:
            cols = _table_columns(conn, table)
            url_cols = [c for c in cols if "url" in c.lower() or "link" in c.lower()]
            if not url_cols:
                continue
            url_col = url_cols[0]
            id_col = cols[0]
            sql = f"select {id_col}, {url_col} from {table} where {url_col} is not null"
            rows = conn.execute(sql).fetchall()
            out: list[VaticanSqliteDocumentRow] = []
            for rid, url in rows:
                if not url:
                    continue
                if normalized_hosts:
                    host = _url_host(str(url))
                    if not host or host not in normalized_hosts:
                        continue
                out.append(VaticanSqliteDocumentRow(row_id=str(rid), url=str(url)))
            if out:
                if num_partitions is not None:
                    out = [row for row in out if _partition_for_row(row) == partition_index]
                return out[:effective_limit] if effective_limit is not None else out

    return []


def discover_url_rows(sqlite_path: str, *, limit: int = 100) -> list[VaticanSqliteDocumentRow]:
    """
    Backwards-compatible helper: returns Vatican rows that always have at least (row_id, url).
    Prefer `discover_document_rows` for richer metadata.
    """
    return discover_document_rows(sqlite_path, limit=limit)
