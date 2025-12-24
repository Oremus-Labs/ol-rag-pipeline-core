from __future__ import annotations

from dataclasses import dataclass

import httpx


@dataclass(frozen=True)
class EmbeddingClient:
    base_url: str
    api_key: str | None = None
    model: str = "default"
    max_batch_texts: int = 16
    max_batch_chars: int = 12_000

    def _batch(self, texts: list[str]) -> list[list[str]]:
        if self.max_batch_texts <= 0:
            raise ValueError("max_batch_texts must be > 0")
        if self.max_batch_chars <= 0:
            raise ValueError("max_batch_chars must be > 0")

        batches: list[list[str]] = []
        cur: list[str] = []
        cur_chars = 0

        for t in texts:
            t = t or ""
            t_chars = len(t)
            would_exceed = cur and (
                (len(cur) + 1 > self.max_batch_texts) or (cur_chars + t_chars > self.max_batch_chars)
            )
            if would_exceed:
                batches.append(cur)
                cur = []
                cur_chars = 0
            cur.append(t)
            cur_chars += t_chars

        if cur:
            batches.append(cur)

        return batches

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        headers: dict[str, str] = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        url = self.base_url.rstrip("/") + "/v1/embeddings"
        embeddings: list[list[float]] = []
        with httpx.Client(timeout=30) as client:
            for batch in self._batch(texts):
                resp = client.post(
                    url,
                    headers=headers,
                    json={"model": self.model, "input": batch},
                )
                resp.raise_for_status()
                payload = resp.json()
                data = payload.get("data") or []
                for item in data:
                    embeddings.append(item["embedding"])

        if len(embeddings) != len(texts):
            raise RuntimeError(f"Embedding count mismatch: {len(embeddings)} != {len(texts)}")
        return embeddings
