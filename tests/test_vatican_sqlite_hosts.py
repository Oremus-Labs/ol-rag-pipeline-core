from __future__ import annotations

import sqlite3

from ol_rag_pipeline_core.sources.vatican_sqlite import discover_document_rows


def test_discover_document_rows_filters_by_hosts(tmp_path) -> None:
    db_path = tmp_path / "vatican.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            create table documents (
              id text primary key,
              link text not null,
              title text
            )
            """
        )
        conn.execute(
            "insert into documents(id, link, title) values (?, ?, ?)",
            ("1", "https://archive.org/details/example", "Example Archive"),
        )
        conn.execute(
            "insert into documents(id, link, title) values (?, ?, ?)",
            ("2", "https://www.vatican.va/content/test", "Example Vatican"),
        )
        conn.commit()

    rows = discover_document_rows(
        str(db_path),
        limit=10,
        hosts=["archive.org"],
    )
    assert len(rows) == 1
    assert rows[0].url == "https://archive.org/details/example"


def test_discover_document_rows_samples_per_host(tmp_path) -> None:
    db_path = tmp_path / "vatican.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            create table documents (
              id text primary key,
              link text not null
            )
            """
        )
        conn.executemany(
            "insert into documents(id, link) values (?, ?)",
            [
                ("1", "https://archive.org/details/a"),
                ("2", "https://archive.org/details/b"),
                ("3", "https://www.vatican.va/content/a"),
                ("4", "https://www.vatican.va/content/b"),
            ],
        )
        conn.commit()

    rows = discover_document_rows(
        str(db_path),
        limit=10,
        hosts=["archive.org", "www.vatican.va"],
        sample_per_host=1,
    )
    assert len(rows) == 2
    assert {r.url for r in rows} == {
        "https://archive.org/details/a",
        "https://www.vatican.va/content/a",
    }

