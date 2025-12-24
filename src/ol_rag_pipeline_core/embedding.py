from __future__ import annotations

from dataclasses import dataclass
from time import sleep

import httpx


@dataclass(frozen=True)
class EmbeddingClient:
    base_url: str
    api_key: str | None = None
    model: str = "default"
    max_batch_texts: int = 16
    max_batch_chars: int = 12_000
    timeout_s: float = 120.0
    max_retries: int = 2
    retry_backoff_s: float = 1.0

    def _batch(self, texts: list[str]) -> list[list[str]]:
        if self.max_batch_texts <= 0:
            raise ValueError("max_batch_texts must be > 0")
        if self.max_batch_chars <= 0:
            raise ValueError("max_batch_chars must be > 0")
        if self.timeout_s <= 0:
            raise ValueError("timeout_s must be > 0")
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.retry_backoff_s < 0:
            raise ValueError("retry_backoff_s must be >= 0")

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

        def _embed_batch(client: httpx.Client, batch: list[str]) -> list[list[float]]:
            attempt = 0
            while True:
                try:
                    resp = client.post(
                        url,
                        headers=headers,
                        json={"model": self.model, "input": batch},
                    )
                    resp.raise_for_status()
                    payload = resp.json()
                    data = payload.get("data") or []
                    return [item["embedding"] for item in data]
                except httpx.HTTPStatusError as e:
                    if e.response is not None and e.response.status_code == 413:
                        if len(batch) == 1:
                            raise RuntimeError(
                                "Embedding request too large (413) for a single chunk. "
                                "Reduce CHUNK_MAX_TOKENS or decrease per-chunk size."
                            ) from e
                        mid = len(batch) // 2
                        return _embed_batch(client, batch[:mid]) + _embed_batch(client, batch[mid:])
                    raise
                except (httpx.TimeoutException, httpx.TransportError):
                    if attempt >= self.max_retries:
                        raise
                    sleep(self.retry_backoff_s * (2**attempt))
                    attempt += 1

        embeddings: list[list[float]] = []
        with httpx.Client(timeout=self.timeout_s) as client:
            for batch in self._batch(texts):
                embeddings.extend(_embed_batch(client, batch))

        if len(embeddings) != len(texts):
            raise RuntimeError(f"Embedding count mismatch: {len(embeddings)} != {len(texts)}")
        return embeddings
