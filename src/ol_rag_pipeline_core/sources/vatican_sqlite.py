from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class SqliteUrlRow:
    row_id: str
    url: str


def _list_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "select name from sqlite_master where type='table' order by name"
    ).fetchall()
    return [r[0] for r in rows]


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"pragma table_info({table})").fetchall()
    return [r[1] for r in rows]


def discover_url_rows(sqlite_path: str, *, limit: int = 100) -> list[SqliteUrlRow]:
    """
    Best-effort: find the first table containing a URL-like column and return up to `limit` rows.
    """
    with sqlite3.connect(sqlite_path) as conn:
        tables = _list_tables(conn)
        for table in tables:
            cols = _table_columns(conn, table)
            url_cols = [c for c in cols if "url" in c.lower() or "link" in c.lower()]
            if not url_cols:
                continue
            url_col = url_cols[0]
            id_col = cols[0]
            sql = f"select {id_col}, {url_col} from {table} where {url_col} is not null limit ?"
            rows = conn.execute(sql, (limit,)).fetchall()
            out: list[SqliteUrlRow] = []
            for rid, url in rows:
                if not url:
                    continue
                out.append(SqliteUrlRow(row_id=str(rid), url=str(url)))
            if out:
                return out
    return []
