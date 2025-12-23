from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import psycopg


@dataclass(frozen=True)
class Migration:
    version: str
    path: Path


def _migrations_dir() -> Path:
    return Path(__file__).resolve().parent / "sql"


def discover_migrations() -> list[Migration]:
    migrations: list[Migration] = []
    for path in sorted(_migrations_dir().glob("*.sql")):
        migrations.append(Migration(version=path.stem, path=path))
    return migrations


def _ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    conn.execute(f'create schema if not exists "{schema}"')
    conn.execute(f'set search_path to "{schema}"')


def _ensure_migrations_table(conn: psycopg.Connection) -> None:
    conn.execute(
        """
        create table if not exists schema_migrations (
          version text primary key,
          applied_at timestamptz not null default now()
        )
        """
    )


def _applied_versions(conn: psycopg.Connection) -> set[str]:
    rows = conn.execute("select version from schema_migrations").fetchall()
    return {r[0] for r in rows}


def apply_migrations(
    dsn: str,
    *,
    schema: str = "public",
    migrations: Iterable[Migration] | None = None,
) -> list[str]:
    """
    Applies migrations into the given schema. Idempotent:
    - already-recorded versions are skipped
    - migrations should use CREATE ... IF NOT EXISTS where appropriate
    """
    if migrations is None:
        migrations = discover_migrations()

    applied: list[str] = []
    with psycopg.connect(dsn) as conn:
        conn.execute("set timezone to 'UTC'")
        _ensure_schema(conn, schema)
        _ensure_migrations_table(conn)
        done = _applied_versions(conn)

        for mig in migrations:
            if mig.version in done:
                continue
            sql = mig.path.read_text(encoding="utf-8")
            conn.execute(sql)
            conn.execute(
                "insert into schema_migrations(version) values (%s)",
                (mig.version,),
            )
            conn.commit()
            applied.append(mig.version)

    return applied

