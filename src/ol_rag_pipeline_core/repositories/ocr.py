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
