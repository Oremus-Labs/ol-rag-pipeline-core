from __future__ import annotations

from typing import Iterable

import psycopg

from ol_rag_pipeline_core.models import Chunk


class ChunkRepository:
    def __init__(self, conn: psycopg.Connection):
        self._conn = conn

    def replace_chunks(self, *, document_id: str, pipeline_version: str, chunks: Iterable[Chunk]) -> None:
        """
        Replace-all semantics for a document+pipeline_version chunk set.
        """
        with self._conn.transaction():
            self._conn.execute(
                "delete from chunks where document_id=%s and pipeline_version=%s",
                (document_id, pipeline_version),
            )
            for c in chunks:
                self._conn.execute(
                    """
                    insert into chunks (
                      document_id, pipeline_version, chunk_id, chunk_index,
                      section_path, token_count, sha256, text_uri,
                      page_start, page_end, locator,
                      updated_at
                    ) values (
                      %s, %s, %s, %s,
                      %s, %s, %s, %s,
                      %s, %s, %s,
                      now()
                    )
                    """,
                    (
                        c.document_id,
                        c.pipeline_version,
                        c.chunk_id,
                        c.chunk_index,
                        c.section_path,
                        c.token_count,
                        c.sha256,
                        c.text_uri,
                        c.page_start,
                        c.page_end,
                        c.locator,
                    ),
                )
        self._conn.commit()

