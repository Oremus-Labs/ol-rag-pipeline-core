from ol_rag_pipeline_core.migrations.runner import apply_migrations


def test_migrations_are_idempotent(pg_dsn: str, pg_schema: str) -> None:
    applied = apply_migrations(pg_dsn, schema=pg_schema)
    assert applied == []

