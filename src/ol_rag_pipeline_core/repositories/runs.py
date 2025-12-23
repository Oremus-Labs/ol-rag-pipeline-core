from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional
from uuid import UUID

import psycopg


@dataclass(frozen=True)
class ProcessingRun:
    run_id: UUID
    correlation_id: UUID
    pipeline_version: str
    document_id: Optional[str]
    status: str
    idempotency_key: Optional[str] = None
    metrics_json: Optional[dict[str, Any]] = None


@dataclass(frozen=True)
class ProcessingError:
    error_id: UUID
    run_id: UUID
    correlation_id: UUID
    pipeline_version: str
    document_id: Optional[str]
    step: str
    message: str
    error_code: Optional[str] = None
    details_json: Optional[dict[str, Any]] = None


class RunRepository:
    def __init__(self, conn: psycopg.Connection):
        self._conn = conn

    def insert_run(self, run: ProcessingRun) -> None:
        self._conn.execute(
            """
            insert into processing_runs(
              run_id, correlation_id, pipeline_version, document_id,
              idempotency_key, status, started_at, metrics_json
            ) values (
              %s::uuid, %s::uuid, %s, %s,
              %s, %s, now(), %s::jsonb
            )
            """,
            (
                str(run.run_id),
                str(run.correlation_id),
                run.pipeline_version,
                run.document_id,
                run.idempotency_key,
                run.status,
                json.dumps(run.metrics_json) if run.metrics_json is not None else None,
            ),
        )
        self._conn.commit()

    def insert_error(self, err: ProcessingError) -> None:
        self._conn.execute(
            """
            insert into processing_errors(
              error_id, run_id, correlation_id, pipeline_version, document_id,
              step, error_code, message, details_json
            ) values (
              %s::uuid, %s::uuid, %s::uuid, %s, %s,
              %s, %s, %s, %s::jsonb
            )
            """,
            (
                str(err.error_id),
                str(err.run_id),
                str(err.correlation_id),
                err.pipeline_version,
                err.document_id,
                err.step,
                err.error_code,
                err.message,
                json.dumps(err.details_json) if err.details_json is not None else None,
            ),
        )
        self._conn.commit()

    def get_runs_for_document(self, document_id: str, pipeline_version: str) -> list[tuple[str, str]]:
        rows = self._conn.execute(
            """
            select run_id::text, status
            from processing_runs
            where document_id=%s and pipeline_version=%s
            order by started_at desc nulls last
            """,
            (document_id, pipeline_version),
        ).fetchall()
        return [(r[0], r[1]) for r in rows]

    def get_errors_for_run(self, run_id: str) -> list[str]:
        rows = self._conn.execute(
            """
            select message
            from processing_errors
            where run_id=%s::uuid
            order by created_at asc
            """,
            (run_id,),
        ).fetchall()
        return [r[0] for r in rows]

