from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, Optional

import psycopg
from pydantic import SecretStr


@dataclass(frozen=True)
class PostgresConfig:
    dsn: Optional[str] = None
    host: Optional[str] = None
    port: int = 5432
    db: Optional[str] = None
    user: Optional[str] = None
    password: Optional[SecretStr] = None

    def build_dsn(self) -> str:
        if self.dsn:
            return self.dsn
        missing = []
        if not self.host:
            missing.append("POSTGRES_HOST")
        if not self.db:
            missing.append("POSTGRES_DB")
        if not self.user:
            missing.append("POSTGRES_USER")
        if not self.password:
            missing.append("POSTGRES_PASSWORD")
        if missing:
            raise ValueError(f"Missing Postgres config: {', '.join(missing)} (or set PG_DSN)")
        return (
            f"postgresql://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.db}"
        )


@contextmanager
def connect(dsn: str, *, schema: str = "public") -> Iterator[psycopg.Connection]:
    options = f"-c search_path={schema} -c timezone=UTC"
    with psycopg.connect(dsn, options=options) as conn:
        yield conn

