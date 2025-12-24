from __future__ import annotations

from uuid import NAMESPACE_URL, UUID, uuid5


def deterministic_ocr_run_id(*, pipeline_version: str, document_id: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"{pipeline_version}:{document_id}:ocr")

