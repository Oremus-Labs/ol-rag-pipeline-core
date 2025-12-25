from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(frozen=True)
class OcrEngineSpec:
    """
    Engine name as configured in ol-llm-service.

    The OpenAI-compatible model id is `ocr/<engine>`.
    """

    engine: str

    @property
    def openai_model(self) -> str:
        return f"ocr/{self.engine}"


@dataclass(frozen=True)
class OcrPageInput:
    page_number: int  # 1-based
    png_bytes: bytes


def _image_data_url(png_bytes: bytes) -> str:
    b64 = base64.b64encode(png_bytes).decode("ascii")
    return f"data:image/png;base64,{b64}"


def _extract_message_content(payload: dict[str, Any]) -> str:
    choices = payload.get("choices") or []
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("OpenAI response missing choices")
    first = choices[0] if isinstance(choices[0], dict) else {}
    msg = first.get("message") if isinstance(first.get("message"), dict) else {}
    content = msg.get("content")
    if isinstance(content, str) and content.strip():
        return content.strip()
    if isinstance(content, str):
        # Allow empty content (some OCR engines occasionally return empty text for blank pages).
        # The caller should treat this as low-quality output and route to review/quality gates.
        return ""
    # Some backends can return `text` at the top-level choice; accept as fallback.
    text = first.get("text")
    if isinstance(text, str) and text.strip():
        return text.strip()
    return ""


@dataclass(frozen=True)
class LlmServiceClient:
    base_url: str
    api_key: str | None = None
    timeout_s: float = 900.0

    def _headers(self) -> dict[str, str]:
        if not self.api_key:
            return {}
        return {"Authorization": f"Bearer {self.api_key}"}

    def ocr_page(
        self,
        *,
        engine: OcrEngineSpec,
        page: OcrPageInput,
        prompt: str = "Read all text in the image. Output plain text only.",
        max_tokens: int = 512,
    ) -> str:
        """
        Calls `ol-llm-service` OpenAI-compatible `/v1/chat/completions`.
        """

        return self.chat_completion(
            model=engine.openai_model,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": _image_data_url(page.png_bytes)},
                        },
                        {"type": "text", "text": prompt},
                    ],
                }
            ],
            max_tokens=max_tokens,
        )

    def chat_completion(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
        max_tokens: int = 512,
        temperature: float = 0.0,
    ) -> str:
        """
        Calls `ol-llm-service` OpenAI-compatible `/v1/chat/completions` and returns the first
        message content.
        """
        body = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        url = self.base_url.rstrip("/") + "/v1/chat/completions"
        with httpx.Client(timeout=self.timeout_s) as client:
            r = client.post(url, headers=self._headers(), json=body)
            r.raise_for_status()
            return _extract_message_content(r.json())
