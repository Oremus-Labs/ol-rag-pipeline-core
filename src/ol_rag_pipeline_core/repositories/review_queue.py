from __future__ import annotations

from dataclasses import dataclass
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import psycopg


@dataclass(frozen=True)
class ReviewItem:
    review_id: UUID
    document_id: str
    pipeline_version: str
    reason: str
    status: str


def deterministic_review_id(*, pipeline_version: str, document_id: str, reason: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"{pipeline_version}:{document_id}:review:{reason}")


class ReviewQueueRepository:
    def __init__(self, conn: psycopg.Connection):
        self._conn = conn

    def get_open_item(
        self,
        *,
        document_id: str,
        pipeline_version: str,
        reason: str,
    ) -> ReviewItem | None:
        row = self._conn.execute(
            """
            select review_id, document_id, pipeline_version, reason, status
            from review_queue
            where document_id=%s and pipeline_version=%s and reason=%s and status='open'
            order by created_at desc
            limit 1
            """,
            (document_id, pipeline_version, reason),
        ).fetchone()
        if not row:
            return None
        return ReviewItem(
            review_id=UUID(row[0]),
            document_id=row[1],
            pipeline_version=row[2],
            reason=row[3],
            status=row[4],
        )

    def ensure_open_item(
        self,
        *,
        document_id: str,
        pipeline_version: str,
        reason: str,
        deterministic: bool = True,
    ) -> UUID:
        existing = self.get_open_item(
            document_id=document_id,
            pipeline_version=pipeline_version,
            reason=reason,
        )
        if existing:
            return existing.review_id

        review_id = (
            deterministic_review_id(
                pipeline_version=pipeline_version,
                document_id=document_id,
                reason=reason,
            )
            if deterministic
            else uuid4()
        )
        self._conn.execute(
            """
            insert into review_queue(review_id, document_id, pipeline_version, reason, status)
            values (%s::uuid, %s, %s, %s, 'open')
            """,
            (str(review_id), document_id, pipeline_version, reason),
        )
        self._conn.commit()
        return review_id
