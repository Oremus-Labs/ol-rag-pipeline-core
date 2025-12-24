from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import psycopg


@dataclass(frozen=True)
class Extraction:
    document_id: str
    pipeline_version: str
    extractor: str
    extracted_uri: str | None
    metrics_json: dict[str, Any] | None


class ExtractionRepository:
    def __init__(self, conn: psycopg.Connection):
        self._conn = conn

    def get_extraction(
        self,
        *,
        document_id: str,
        pipeline_version: str,
        extractor: str,
    ) -> Extraction | None:
        row = self._conn.execute(
            """
            select document_id, pipeline_version, extractor, extracted_uri, metrics_json
            from extractions
            where document_id=%s and pipeline_version=%s and extractor=%s
            """,
            (document_id, pipeline_version, extractor),
        ).fetchone()
        if not row:
            return None
        return Extraction(
            document_id=row[0],
            pipeline_version=row[1],
            extractor=row[2],
            extracted_uri=row[3],
            metrics_json=row[4],
        )

    def upsert_extraction(self, ext: Extraction) -> None:
        self._conn.execute(
            """
            insert into extractions(
              document_id, pipeline_version, extractor, extracted_uri, metrics_json
            )
            values (%s, %s, %s, %s, %s::jsonb)
            on conflict (document_id, pipeline_version, extractor) do update set
              extracted_uri = excluded.extracted_uri,
              metrics_json = excluded.metrics_json
            """,
            (
                ext.document_id,
                ext.pipeline_version,
                ext.extractor,
                ext.extracted_uri,
                json.dumps(ext.metrics_json) if ext.metrics_json is not None else None,
            ),
        )
        self._conn.commit()
