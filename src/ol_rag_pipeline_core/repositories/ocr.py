from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import psycopg


@dataclass(frozen=True)
class OcrRun:
    ocr_run_id: UUID
    document_id: str
    pipeline_version: str
    engine: str
    status: str
    metrics_json: dict[str, Any] | None = None


@dataclass(frozen=True)
class OcrPage:
    ocr_run_id: UUID
    page_number: int
    consensus_uri: str | None = None
    quality_json: dict[str, Any] | None = None


class OcrRepository:
    def __init__(self, conn: psycopg.Connection):
        self._conn = conn

    def upsert_ocr_run(self, run: OcrRun) -> None:
        self._conn.execute(
            """
            insert into ocr_runs(
              ocr_run_id, document_id, pipeline_version, engine, status, metrics_json
            )
            values (%s::uuid, %s, %s, %s, %s, %s::jsonb)
            on conflict (ocr_run_id) do update set
              engine = excluded.engine,
              status = excluded.status,
              metrics_json = excluded.metrics_json
            """,
            (
                str(run.ocr_run_id),
                run.document_id,
                run.pipeline_version,
                run.engine,
                run.status,
                json.dumps(run.metrics_json) if run.metrics_json is not None else None,
            ),
        )
        self._conn.commit()

    def upsert_ocr_page(self, page: OcrPage) -> None:
        self._conn.execute(
            """
            insert into ocr_pages(
              ocr_run_id, page_number, consensus_uri, quality_json
            )
            values (%s::uuid, %s, %s, %s::jsonb)
            on conflict (ocr_run_id, page_number) do update set
              consensus_uri = excluded.consensus_uri,
              quality_json = excluded.quality_json
            """,
            (
                str(page.ocr_run_id),
                page.page_number,
                page.consensus_uri,
                json.dumps(page.quality_json) if page.quality_json is not None else None,
            ),
        )
        self._conn.commit()

    def get_ocr_run(self, ocr_run_id: UUID) -> OcrRun | None:
        row = self._conn.execute(
            """
            select ocr_run_id, document_id, pipeline_version, engine, status, metrics_json
            from ocr_runs
            where ocr_run_id=%s::uuid
            """,
            (str(ocr_run_id),),
        ).fetchone()
        if not row:
            return None
        return OcrRun(
            ocr_run_id=row[0],
            document_id=row[1],
            pipeline_version=row[2],
            engine=row[3],
            status=row[4],
            metrics_json=row[5],
        )

    def get_latest_run_for_document(
        self,
        *,
        document_id: str,
        pipeline_version: str,
        status: str | None = None,
    ) -> OcrRun | None:
        sql = """
        select ocr_run_id, document_id, pipeline_version, engine, status, metrics_json
        from ocr_runs
        where document_id=%s and pipeline_version=%s
        """
        params: list[object] = [document_id, pipeline_version]
        if status is not None:
            sql += " and status=%s"
            params.append(status)
        sql += " order by created_at desc limit 1"

        row = self._conn.execute(sql, params).fetchone()
        if not row:
            return None
        return OcrRun(
            ocr_run_id=row[0],
            document_id=row[1],
            pipeline_version=row[2],
            engine=row[3],
            status=row[4],
            metrics_json=row[5],
        )

    def list_pages(self, *, ocr_run_id: UUID) -> list[OcrPage]:
        rows = self._conn.execute(
            """
            select ocr_run_id, page_number, consensus_uri, quality_json
            from ocr_pages
            where ocr_run_id=%s::uuid
            order by page_number
            """,
            (str(ocr_run_id),),
        ).fetchall()
        return [
            OcrPage(
                ocr_run_id=r[0],
                page_number=r[1],
                consensus_uri=r[2],
                quality_json=r[3],
            )
            for r in rows
        ]

    def set_run_status(self, *, ocr_run_id: UUID, status: str, metrics_json: dict[str, Any] | None) -> None:
        self._conn.execute(
            """
            update ocr_runs
            set
              status=%s,
              metrics_json = coalesce(ocr_runs.metrics_json, '{}'::jsonb) || coalesce(%s::jsonb, '{}'::jsonb)
            where ocr_run_id=%s::uuid
            """,
            (status, json.dumps(metrics_json) if metrics_json is not None else None, str(ocr_run_id)),
        )
        self._conn.commit()
