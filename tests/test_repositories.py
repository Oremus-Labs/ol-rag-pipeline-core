from __future__ import annotations

from uuid import uuid4

from ol_rag_pipeline_core.models import Chunk, Document, DocumentLink
from ol_rag_pipeline_core.repositories.chunks import ChunkRepository
from ol_rag_pipeline_core.repositories.documents import DocumentRepository
from ol_rag_pipeline_core.repositories.enrichments import ChunkEnrichmentRepository
from ol_rag_pipeline_core.repositories.runs import ProcessingError, ProcessingRun, RunRepository


def test_documents_crud_categories_links_and_search(conn) -> None:  # noqa: ANN001
    docs = DocumentRepository(conn)

    doc = Document(
        document_id="doc-1",
        source="nextcloud",
        source_uri="nextcloud://ETL/Ingest/doc-1.pdf",
        title="Saint Augustine Confessions",
        author="Augustine",
        language="en",
        published_year=397,
        content_fingerprint="fp1",
        source_dataset="nextcloud:live",
    )
    docs.upsert_document(doc)

    loaded = docs.get_document("doc-1")
    assert loaded is not None
    assert loaded.document_id == "doc-1"
    assert loaded.title == "Saint Augustine Confessions"

    docs.add_category("doc-1", "church_fathers")
    docs.add_category("doc-1", "church_fathers")  # idempotent insert
    assert docs.list_categories("doc-1") == ["church_fathers"]

    docs.add_link(DocumentLink(document_id="doc-1", link_type="source_uri", url=doc.source_uri))
    docs.add_link(DocumentLink(document_id="doc-1", link_type="canonical_url", url="https://example/doc-1"))
    assert [link.link_type for link in docs.list_links("doc-1")] == ["canonical_url", "source_uri"]

    docs.upsert_search_preview("doc-1", "Augustine wrote about grace and conversion.")
    hits = docs.search_documents("conversion")
    assert "doc-1" in hits


def test_chunks_persist_page_fields(conn) -> None:  # noqa: ANN001
    docs = DocumentRepository(conn)
    chunks = ChunkRepository(conn)

    docs.upsert_document(
        Document(
            document_id="doc-2",
            source="nextcloud",
            source_uri="nextcloud://ETL/Ingest/doc-2.pdf",
            title="Test Doc",
            author="Tester",
        )
    )

    chunk_set = [
        Chunk(
            document_id="doc-2",
            pipeline_version="v1",
            chunk_id="doc-2:v1:0",
            chunk_index=0,
            page_start=1,
            page_end=2,
            locator="p. 1–2",
        )
    ]
    chunks.replace_chunks(document_id="doc-2", pipeline_version="v1", chunks=chunk_set)

    row = conn.execute(
        "select page_start, page_end, locator from chunks where chunk_id=%s",
        ("doc-2:v1:0",),
    ).fetchone()
    assert row == (1, 2, "p. 1–2")


def test_processing_runs_and_errors(conn) -> None:  # noqa: ANN001
    docs = DocumentRepository(conn)
    runs = RunRepository(conn)

    docs.upsert_document(
        Document(
            document_id="doc-3",
            source="nextcloud",
            source_uri="nextcloud://ETL/Ingest/doc-3.pdf",
        )
    )

    corr = uuid4()
    run_id = uuid4()
    runs.insert_run(
        ProcessingRun(
            run_id=run_id,
            correlation_id=corr,
            pipeline_version="v1",
            document_id="doc-3",
            status="running",
            idempotency_key="v1:doc-3:fp1",
            metrics_json={"step": "discover"},
        )
    )
    runs.insert_error(
        ProcessingError(
            error_id=uuid4(),
            run_id=run_id,
            correlation_id=corr,
            pipeline_version="v1",
            document_id="doc-3",
            step="extract",
            message="boom",
            details_json={"hint": "unit test"},
        )
    )

    got = runs.get_runs_for_document("doc-3", "v1")
    assert got[0][0] == str(run_id)
    assert runs.get_errors_for_run(str(run_id)) == ["boom"]


def test_chunk_enrichments_upsert_and_candidates(conn) -> None:  # noqa: ANN001
    docs = DocumentRepository(conn)
    chunks = ChunkRepository(conn)
    enrich = ChunkEnrichmentRepository(conn)

    docs.upsert_document(
        Document(
            document_id="doc-4",
            source="nextcloud",
            source_uri="nextcloud://ETL/Ingest/doc-4.pdf",
            status="indexed_ok",
        )
    )
    chunks.replace_chunks(
        document_id="doc-4",
        pipeline_version="v1",
        chunks=[
            Chunk(
                document_id="doc-4",
                pipeline_version="v1",
                chunk_id="doc-4:v1:0",
                chunk_index=0,
                sha256="sha-a",
                text_uri="s3://bucket/library/chunks/v1/nextcloud/doc-4.jsonl",
            )
        ],
    )

    candidates = enrich.list_candidates(pipeline_version="v1", enrichment_version="gpt20b_v1", limit=10)
    assert [c.chunk_id for c in candidates] == ["doc-4:v1:0"]

    enrich.upsert(
        chunk_id="doc-4:v1:0",
        enrichment_version="gpt20b_v1",
        model="gpt-20b",
        chunk_sha256="sha-a",
        input_sha256="input-a",
        confidence=0.5,
        accepted=False,
        output_json={"confidence": 0.5, "tags": ["x"]},
    )
    candidates = enrich.list_candidates(pipeline_version="v1", enrichment_version="gpt20b_v1", limit=10)
    assert candidates == []

    candidates = enrich.list_candidates(
        pipeline_version="v1",
        enrichment_version="gpt20b_v1",
        limit=10,
        include_rejected=True,
    )
    assert [c.chunk_id for c in candidates] == ["doc-4:v1:0"]

    # Accepted-but-not-applied rows are also candidates (common when a dry-run stored output_json but
    # did not apply Qdrant payload updates).
    enrich.upsert(
        chunk_id="doc-4:v1:0",
        enrichment_version="gpt20b_v1",
        model="gpt-20b",
        chunk_sha256="sha-a",
        input_sha256="input-a2",
        confidence=0.99,
        accepted=True,
        output_json={"confidence": 0.99, "summary": "ok", "keywords": [], "topics": [], "entities": {}},
        applied_at=None,
    )
    candidates = enrich.list_candidates(pipeline_version="v1", enrichment_version="gpt20b_v1", limit=10)
    assert [c.chunk_id for c in candidates] == ["doc-4:v1:0"]

    # If the chunk changes, it should be selected even without include_rejected.
    chunks.replace_chunks(
        document_id="doc-4",
        pipeline_version="v1",
        chunks=[
            Chunk(
                document_id="doc-4",
                pipeline_version="v1",
                chunk_id="doc-4:v1:0",
                chunk_index=0,
                sha256="sha-b",
                text_uri="s3://bucket/library/chunks/v1/nextcloud/doc-4.jsonl",
            )
        ],
    )
    candidates = enrich.list_candidates(pipeline_version="v1", enrichment_version="gpt20b_v1", limit=10)
    assert [c.chunk_id for c in candidates] == ["doc-4:v1:0"]
