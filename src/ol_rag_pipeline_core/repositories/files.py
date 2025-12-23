from __future__ import annotations

import psycopg


class DocumentFileRepository:
    def __init__(self, conn: psycopg.Connection):
        self._conn = conn

    def upsert_file(
        self,
        *,
        document_id: str,
        variant: str,
        storage_uri: str,
        sha256: str | None = None,
        bytes_size: int | None = None,
        mime_type: str | None = None,
    ) -> None:
        self._conn.execute(
            """
            insert into document_files(document_id, variant, storage_uri, sha256, bytes, mime_type)
            values (%s, %s, %s, %s, %s, %s)
            on conflict (document_id, variant) do update set
              storage_uri = excluded.storage_uri,
              sha256 = excluded.sha256,
              bytes = excluded.bytes,
              mime_type = excluded.mime_type
            """,
            (document_id, variant, storage_uri, sha256, bytes_size, mime_type),
        )
        self._conn.commit()

