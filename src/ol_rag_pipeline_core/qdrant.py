from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import httpx


def deterministic_point_id(*, chunk_id: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"qdrant:{chunk_id}")


@dataclass(frozen=True)
class QdrantClient:
    base_url: str
    api_key: str | None = None

    def _headers(self) -> dict[str, str]:
        if not self.api_key:
            return {}
        # Qdrant supports either `api-key` or bearer auth; use api-key.
        return {"api-key": self.api_key}

    def ensure_collection(
        self,
        *,
        name: str,
        vector_size: int,
        distance: str = "Cosine",
    ) -> None:
        base = self.base_url.rstrip("/")
        headers = self._headers()
        with httpx.Client(timeout=30) as client:
            get_resp = client.get(f"{base}/collections/{name}", headers=headers)
            if get_resp.status_code == 200:
                return
            if get_resp.status_code not in (404,):
                get_resp.raise_for_status()

            create = client.put(
                f"{base}/collections/{name}",
                headers=headers,
                json={
                    "vectors": {"size": vector_size, "distance": distance},
                },
            )
            create.raise_for_status()

    def delete_points_for_document(
        self,
        *,
        collection: str,
        document_id: str,
        pipeline_version: str,
    ) -> None:
        base = self.base_url.rstrip("/")
        headers = self._headers()
        payload = {
            "filter": {
                "must": [
                    {"key": "document_id", "match": {"value": document_id}},
                    {"key": "pipeline_version", "match": {"value": pipeline_version}},
                ]
            }
        }
        with httpx.Client(timeout=60) as client:
            resp = client.post(
                f"{base}/collections/{collection}/points/delete",
                params={"wait": "true"},
                headers=headers,
                json=payload,
            )
            resp.raise_for_status()

    def upsert_points(
        self,
        *,
        collection: str,
        points: list[dict[str, Any]],
    ) -> None:
        if not points:
            return
        base = self.base_url.rstrip("/")
        headers = self._headers()
        with httpx.Client(timeout=120) as client:
            resp = client.put(
                f"{base}/collections/{collection}/points",
                params={"wait": "true"},
                headers=headers,
                json={"points": points},
            )
            resp.raise_for_status()

    def search(
        self,
        *,
        collection: str,
        vector: list[float],
        limit: int = 10,
        query_filter: dict[str, Any] | None = None,
        with_payload: bool = True,
    ) -> list[dict[str, Any]]:
        base = self.base_url.rstrip("/")
        headers = self._headers()
        body: dict[str, Any] = {
            "vector": vector,
            "limit": limit,
            "with_payload": with_payload,
        }
        if query_filter is not None:
            body["filter"] = query_filter
        with httpx.Client(timeout=60) as client:
            resp = client.post(
                f"{base}/collections/{collection}/points/search",
                headers=headers,
                json=body,
            )
            resp.raise_for_status()
            data = resp.json()
            result = data.get("result")
            if not isinstance(result, list):
                raise RuntimeError("Unexpected Qdrant search response shape")
            return result

    def count(
        self,
        *,
        collection: str,
        query_filter: dict[str, Any] | None = None,
    ) -> int:
        base = self.base_url.rstrip("/")
        headers = self._headers()
        body: dict[str, Any] = {"exact": True}
        if query_filter is not None:
            body["filter"] = query_filter
        with httpx.Client(timeout=30) as client:
            resp = client.post(
                f"{base}/collections/{collection}/points/count",
                headers=headers,
                json=body,
            )
            resp.raise_for_status()
            data = resp.json()
            result = data.get("result") or {}
            cnt = result.get("count")
            if not isinstance(cnt, int):
                raise RuntimeError("Unexpected Qdrant count response shape")
            return cnt
