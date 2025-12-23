from __future__ import annotations

import hashlib


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def stable_document_id(source: str, source_uri: str) -> str:
    """
    Deterministic document_id derived from the source URI.
    """
    digest = hashlib.sha1(source_uri.encode("utf-8")).hexdigest()  # noqa: S324
    return f"{source}:{digest}"

