from __future__ import annotations

from dataclasses import dataclass

import psycopg


@dataclass(frozen=True)
class DocumentFile:
    document_id: str
    variant: str
    storage_uri: str
    sha256: str | None
    bytes_size: int | None
    mime_type: str | None


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

    def get_file(self, *, document_id: str, variant: str) -> DocumentFile | None:
        row = self._conn.execute(
            """
            select document_id, variant, storage_uri, sha256, bytes, mime_type
            from document_files
            where document_id=%s and variant=%s
            """,
            (document_id, variant),
        ).fetchone()
        if not row:
            return None
        return DocumentFile(
            document_id=row[0],
            variant=row[1],
            storage_uri=row[2],
            sha256=row[3],
            bytes_size=row[4],
            mime_type=row[5],
        )
