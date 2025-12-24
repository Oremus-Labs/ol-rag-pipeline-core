from __future__ import annotations

import difflib
from dataclasses import dataclass
from typing import Any

from ol_rag_pipeline_core.ocr.client import LlmServiceClient, OcrEngineSpec, OcrPageInput
from ol_rag_pipeline_core.ocr.quality import OcrQualityGate, assess_ocr_text_quality, passes_quality_gate


def _similarity(a: str, b: str) -> float:
    return difflib.SequenceMatcher(a=a, b=b).ratio()


def _choose_consensus(candidates: dict[str, str]) -> tuple[str, dict[str, Any]]:
    """
    Select the "best" OCR text among engine outputs.

    We prefer the output that is most similar to the other outputs (medoid).
    Ties break by longer text length (more content).
    """
    engines = list(candidates.keys())
    if not engines:
        raise ValueError("No OCR candidates provided")
    if len(engines) == 1:
        e = engines[0]
        return candidates[e], {"winner": e, "pairwise_similarity": {}}

    pairwise: dict[str, dict[str, float]] = {e: {} for e in engines}
    for i, a in enumerate(engines):
        for b in engines[i + 1 :]:
            s = _similarity(candidates[a], candidates[b])
            pairwise[a][b] = s
            pairwise[b][a] = s

    scored: list[tuple[float, int, str]] = []
    for e in engines:
        sims = list(pairwise[e].values())
        avg = sum(sims) / max(len(sims), 1)
        scored.append((avg, len(candidates[e]), e))

    scored.sort(reverse=True)
    best = scored[0][2]
    return candidates[best], {
        "winner": best,
        "avg_similarity": scored[0][0],
        "pairwise_similarity": pairwise,
    }


@dataclass(frozen=True)
class OcrPageResult:
    page_number: int
    consensus_text: str
    engine_texts: dict[str, str]
    quality_by_engine: dict[str, dict[str, float]]
    consensus_meta: dict[str, Any]
    passed_gate: bool


@dataclass(frozen=True)
class OcrEnsembleResult:
    pages: list[OcrPageResult]
    merged_text: str
    overall_passed: bool


@dataclass(frozen=True)
class OcrEnsembleConfig:
    engines: list[OcrEngineSpec]
    quality_gate: OcrQualityGate = OcrQualityGate()
    prompt: str = "Read all text in the image. Output plain text only."
    max_tokens: int = 512


def run_ocr_ensemble(
    *,
    client: LlmServiceClient,
    pages: list[OcrPageInput],
    cfg: OcrEnsembleConfig,
) -> OcrEnsembleResult:
    if not cfg.engines:
        raise ValueError("cfg.engines must not be empty")

    results: list[OcrPageResult] = []
    overall_ok = True

    for page in pages:
        engine_texts: dict[str, str] = {}
        quality_by_engine: dict[str, dict[str, float]] = {}

        for engine in cfg.engines:
            text = client.ocr_page(
                engine=engine,
                page=page,
                prompt=cfg.prompt,
                max_tokens=cfg.max_tokens,
            )
            engine_texts[engine.engine] = text
            q = assess_ocr_text_quality(text)
            quality_by_engine[engine.engine] = {
                "chars": q.chars,
                "alpha_ratio": round(q.alpha_ratio, 4),
                "printable_ratio": round(q.printable_ratio, 4),
            }

        consensus_text, consensus_meta = _choose_consensus(engine_texts)
        q_consensus = assess_ocr_text_quality(consensus_text)
        passed = passes_quality_gate(q_consensus, cfg.quality_gate)
        if not passed:
            overall_ok = False

        results.append(
            OcrPageResult(
                page_number=page.page_number,
                consensus_text=consensus_text,
                engine_texts=engine_texts,
                quality_by_engine=quality_by_engine,
                consensus_meta=consensus_meta,
                passed_gate=passed,
            )
        )

    merged_lines: list[str] = []
    for r in results:
        merged_lines.append(f"\n\n--- PAGE {r.page_number} ---\n\n")
        merged_lines.append(r.consensus_text.strip())
    merged = "".join(merged_lines).strip() + "\n"

    return OcrEnsembleResult(pages=results, merged_text=merged, overall_passed=overall_ok)

