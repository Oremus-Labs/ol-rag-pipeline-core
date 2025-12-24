from __future__ import annotations

import json
from dataclasses import asdict

import psycopg

from ol_rag_pipeline_core.models import Document, DocumentLink


class DocumentRepository:
    def __init__(self, conn: psycopg.Connection):
        self._conn = conn

    def upsert_document(self, doc: Document) -> None:
        sql = """
        insert into documents (
          document_id, source, source_uri, canonical_url,
          title, author, published_year, language,
          content_type, is_scanned, status,
          content_fingerprint, canonical_sha256, canonical_etag,
          categories_json, source_dataset,
          updated_at
        ) values (
          %(document_id)s, %(source)s, %(source_uri)s, %(canonical_url)s,
          %(title)s, %(author)s, %(published_year)s, %(language)s,
          %(content_type)s, %(is_scanned)s, %(status)s,
          %(content_fingerprint)s, %(canonical_sha256)s, %(canonical_etag)s,
          %(categories_json)s::jsonb, %(source_dataset)s,
          now()
        )
        on conflict (document_id) do update set
          source = excluded.source,
          source_uri = excluded.source_uri,
          canonical_url = excluded.canonical_url,
          title = excluded.title,
          author = excluded.author,
          published_year = excluded.published_year,
          language = excluded.language,
          content_type = excluded.content_type,
          is_scanned = excluded.is_scanned,
          status = excluded.status,
          content_fingerprint = excluded.content_fingerprint,
          canonical_sha256 = excluded.canonical_sha256,
          canonical_etag = excluded.canonical_etag,
          categories_json = excluded.categories_json,
          source_dataset = excluded.source_dataset,
          updated_at = now()
        """
        payload = asdict(doc)
        categories_json = payload.get("categories_json")
        payload["categories_json"] = (
            json.dumps(categories_json) if categories_json is not None else None
        )
        self._conn.execute(sql, payload)
        self._conn.commit()

    def get_document(self, document_id: str) -> Document | None:
        row = self._conn.execute(
            """
            select
              document_id, source, source_uri, canonical_url,
              title, author, published_year, language,
              content_type, is_scanned, status,
              content_fingerprint, canonical_sha256, canonical_etag,
              categories_json, source_dataset,
              created_at, updated_at
            from documents
            where document_id=%s
            """,
            (document_id,),
        ).fetchone()
        if not row:
            return None
        return Document(
            document_id=row[0],
            source=row[1],
            source_uri=row[2],
            canonical_url=row[3],
            title=row[4],
            author=row[5],
            published_year=row[6],
            language=row[7],
            content_type=row[8],
            is_scanned=row[9],
            status=row[10],
            content_fingerprint=row[11],
            canonical_sha256=row[12],
            canonical_etag=row[13],
            categories_json=row[14],
            source_dataset=row[15],
            created_at=row[16],
            updated_at=row[17],
        )

    def add_category(self, document_id: str, category: str) -> None:
        self._conn.execute(
            """
            insert into document_categories(document_id, category)
            values (%s, %s)
            on conflict do nothing
            """,
            (document_id, category),
        )
        self._conn.commit()

    def list_categories(self, document_id: str) -> list[str]:
        rows = self._conn.execute(
            "select category from document_categories where document_id=%s order by category",
            (document_id,),
        ).fetchall()
        return [r[0] for r in rows]

    def upsert_search_preview(self, document_id: str, preview_text: str) -> None:
        self._conn.execute(
            """
            insert into document_search(document_id, preview_text, search_tsv, updated_at)
            select
              d.document_id,
              %s as preview_text,
              to_tsvector(
                'english',
                coalesce(d.title,'') || ' ' || coalesce(d.author,'') || ' ' || %s
              ) as search_tsv,
              now() as updated_at
            from documents d
            where d.document_id=%s
            on conflict (document_id) do update set
              preview_text = excluded.preview_text,
              search_tsv = excluded.search_tsv,
              updated_at = now()
            """,
            (preview_text, preview_text, document_id),
        )
        self._conn.commit()

    def search_documents(self, query: str, *, limit: int = 10) -> list[str]:
        rows = self._conn.execute(
            """
            select document_id
            from document_search
            where search_tsv @@ plainto_tsquery('english', %s)
            order by updated_at desc
            limit %s
            """,
            (query, limit),
        ).fetchall()
        return [r[0] for r in rows]

    def add_link(self, link: DocumentLink) -> None:
        self._conn.execute(
            """
            insert into document_links(document_id, link_type, url, label)
            values (%s, %s, %s, %s)
            on conflict (document_id, link_type, url) do update set
              label = excluded.label
            """,
            (link.document_id, link.link_type, link.url, link.label),
        )
        self._conn.commit()

    def list_links(self, document_id: str) -> list[DocumentLink]:
        rows = self._conn.execute(
            """
            select document_id, link_type, url, label
            from document_links
            where document_id=%s
            order by link_type, url
            """,
            (document_id,),
        ).fetchall()
        return [DocumentLink(document_id=r[0], link_type=r[1], url=r[2], label=r[3]) for r in rows]

    def set_processing_state(
        self,
        *,
        document_id: str,
        status: str,
        is_scanned: bool | None,
    ) -> None:
        self._conn.execute(
            """
            update documents
            set status=%s, is_scanned=%s, updated_at=now()
            where document_id=%s
            """,
            (status, is_scanned, document_id),
        )
        self._conn.commit()
