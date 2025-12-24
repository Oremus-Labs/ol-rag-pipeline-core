from pydantic import SecretStr

from ol_rag_pipeline_core.db import PostgresConfig


def test_build_dsn_accepts_plain_password_string() -> None:
    dsn = PostgresConfig(
        host="localhost",
        db="research",
        user="user",
        password="pass",
    ).build_dsn()
    assert dsn == "postgresql://user:pass@localhost:5432/research"


def test_build_dsn_accepts_secretstr_password() -> None:
    dsn = PostgresConfig(
        host="localhost",
        db="research",
        user="user",
        password=SecretStr("pass"),
    ).build_dsn()
    assert dsn == "postgresql://user:pass@localhost:5432/research"

