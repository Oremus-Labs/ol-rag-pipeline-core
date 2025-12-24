from __future__ import annotations

from dataclasses import dataclass

import httpx


@dataclass(frozen=True)
class EmbeddingClient:
    base_url: str
    api_key: str | None = None
    model: str = "default"

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        headers: dict[str, str] = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        url = self.base_url.rstrip("/") + "/v1/embeddings"
        with httpx.Client(timeout=30) as client:
            resp = client.post(
                url,
                headers=headers,
                json={"model": self.model, "input": texts},
            )
            resp.raise_for_status()
            payload = resp.json()

        data = payload.get("data") or []
        embeddings: list[list[float]] = []
        for item in data:
            embeddings.append(item["embedding"])
        if len(embeddings) != len(texts):
            raise RuntimeError(f"Embedding count mismatch: {len(embeddings)} != {len(texts)}")
        return embeddings

