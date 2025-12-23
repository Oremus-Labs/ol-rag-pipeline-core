from __future__ import annotations

import os
import uuid
from collections.abc import Generator

import psycopg
import pytest

from ol_rag_pipeline_core.db import connect
from ol_rag_pipeline_core.migrations.runner import apply_migrations


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    dsn = os.environ.get("PG_DSN")
    if not dsn:
        pytest.skip("PG_DSN not set; skipping DB integration tests")
    return dsn


@pytest.fixture(scope="session")
def pg_schema(pg_dsn: str) -> str:
    schema = f"test_{uuid.uuid4().hex[:10]}"
    apply_migrations(pg_dsn, schema=schema)
    return schema


@pytest.fixture(scope="session", autouse=True)
def _drop_schema_at_end(pg_dsn: str, pg_schema: str) -> Generator[None, None, None]:
    yield
    with psycopg.connect(pg_dsn) as conn:
        conn.execute(f'drop schema if exists "{pg_schema}" cascade')
        conn.commit()


@pytest.fixture()
def conn(pg_dsn: str, pg_schema: str) -> Generator[psycopg.Connection, None, None]:
    with connect(pg_dsn, schema=pg_schema) as c:
        yield c

