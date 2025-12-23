from ol_rag_pipeline_core.util import sha256_bytes, stable_document_id


def test_sha256_bytes() -> None:
    assert (
        sha256_bytes(b"abc")
        == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )


def test_stable_document_id_is_deterministic() -> None:
    a = stable_document_id("nextcloud", "nextcloud://ETL/Ingest/a.pdf")
    b = stable_document_id("nextcloud", "nextcloud://ETL/Ingest/a.pdf")
    assert a == b
