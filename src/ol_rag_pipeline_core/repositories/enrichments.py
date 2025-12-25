from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg


@dataclass(frozen=True)
class ChunkEnrichmentRow:
    chunk_id: str
    enrichment_version: str
    model: str
    chunk_sha256: str
    input_sha256: str
    confidence: float | None
    accepted: bool
    output_json: dict[str, Any] | None
    error: str | None
    applied_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ChunkEnrichmentCandidate:
    document_id: str
    pipeline_version: str
    chunk_id: str
    chunk_index: int
    chunk_sha256: str | None
    text_uri: str | None
    existing_confidence: float | None
    existing_accepted: bool | None
    existing_applied_at: datetime | None
    existing_output_json: dict[str, Any] | None
    existing_error: str | None


class ChunkEnrichmentRepository:
    def __init__(self, conn: psycopg.Connection):
        self._conn = conn

    def upsert(
        self,
        *,
        chunk_id: str,
        enrichment_version: str,
        model: str,
        chunk_sha256: str,
        input_sha256: str,
        confidence: float | None,
        accepted: bool,
        output_json: dict[str, Any] | None,
        error: str | None = None,
        applied_at: datetime | None = None,
    ) -> None:
        payload_json = (
            json.dumps(output_json, ensure_ascii=False) if output_json is not None else None
        )
        self._conn.execute(
            """
            insert into chunk_enrichments (
              chunk_id, enrichment_version, model,
              chunk_sha256, input_sha256,
              confidence, accepted, output_json, error,
              applied_at, updated_at
            ) values (
              %s, %s, %s,
              %s, %s,
              %s, %s, %s::jsonb, %s,
              %s, now()
            )
            on conflict (chunk_id, enrichment_version) do update set
              model = excluded.model,
              chunk_sha256 = excluded.chunk_sha256,
              input_sha256 = excluded.input_sha256,
              confidence = excluded.confidence,
              accepted = excluded.accepted,
              output_json = excluded.output_json,
              error = excluded.error,
              applied_at = excluded.applied_at,
              updated_at = now()
            """,
            (
                chunk_id,
                enrichment_version,
                model,
                chunk_sha256,
                input_sha256,
                confidence,
                accepted,
                payload_json,
                error,
                applied_at,
            ),
        )
        self._conn.commit()

    def get(
        self,
        *,
        chunk_id: str,
        enrichment_version: str,
    ) -> ChunkEnrichmentRow | None:
        row = self._conn.execute(
            """
            select
              chunk_id, enrichment_version, model,
              chunk_sha256, input_sha256,
              confidence, accepted,
              output_json, error, applied_at,
              created_at, updated_at
            from chunk_enrichments
            where chunk_id=%s and enrichment_version=%s
            """,
            (chunk_id, enrichment_version),
        ).fetchone()
        if not row:
            return None
        return ChunkEnrichmentRow(
            chunk_id=row[0],
            enrichment_version=row[1],
            model=row[2],
            chunk_sha256=row[3],
            input_sha256=row[4],
            confidence=row[5],
            accepted=row[6],
            output_json=row[7],
            error=row[8],
            applied_at=row[9],
            created_at=row[10],
            updated_at=row[11],
        )

    def list_candidates(
        self,
        *,
        pipeline_version: str,
        enrichment_version: str,
        source: str | None = None,
        limit: int = 500,
        include_rejected: bool = False,
    ) -> list[ChunkEnrichmentCandidate]:
        """
        Returns chunks which are missing enrichment for the given version, or whose chunk_sha256
        changed.

        If include_rejected is false, chunks with a non-null enrichment row where accepted=false and
        chunk_sha256 matches are skipped (they were processed but not accepted).
        """
        sql = """
        select
          c.document_id,
          c.pipeline_version,
          c.chunk_id,
          c.chunk_index,
          c.sha256,
          c.text_uri,
          e.accepted,
          e.applied_at,
          e.output_json,
          e.confidence,
          e.error
        from chunks c
        join documents d on d.document_id = c.document_id
        left join chunk_enrichments e
          on e.chunk_id = c.chunk_id and e.enrichment_version = %s
        where
          c.pipeline_version = %s
          and d.status = 'indexed_ok'
          and (%s::text is null or d.source = %s::text)
          and (
            e.chunk_id is null
            or e.chunk_sha256 is distinct from c.sha256
            or (e.accepted is true and e.applied_at is null)
            or (e.accepted is false and %s is true)
          )
        order by c.document_id, c.chunk_index
        limit %s
        """
        rows = self._conn.execute(
            sql,
            (
                enrichment_version,
                pipeline_version,
                source,
                source,
                include_rejected,
                limit,
            ),
        ).fetchall()
        return [
            ChunkEnrichmentCandidate(
                document_id=r[0],
                pipeline_version=r[1],
                chunk_id=r[2],
                chunk_index=r[3],
                chunk_sha256=r[4],
                text_uri=r[5],
                existing_accepted=r[6],
                existing_applied_at=r[7],
                existing_output_json=r[8],
                existing_confidence=r[9],
                existing_error=r[10],
            )
            for r in rows
        ]
