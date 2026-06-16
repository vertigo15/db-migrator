import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

from llm_client import LLMClient
from prompt_builder import (
    build_fallback,
    build_system_message,
    build_template_only,
    build_user_message,
)
from schemas import (
    BatchMergeRequest,
    BatchMergeResponse,
    LLMOverride,
    MergeRequest,
    MergeResult,
)

env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("prompt-merger")

app = FastAPI(title="Prompt Merger", version="1.0.0")
llm = LLMClient()


def _batch_worker_count(total: int) -> int:
    try:
        configured = int(os.getenv("PROMPT_MERGER_MAX_WORKERS", "4"))
    except ValueError:
        configured = 4
    return max(1, min(configured, total))


def _has_any_content(req: MergeRequest) -> bool:
    return any(
        v and v.strip()
        for v in (req.tone, req.guardrail, req.response)
    )


def _resolve_llm(override: Optional[LLMOverride]) -> LLMClient:
    """Return an LLMClient built from overrides, or fall back to the default."""
    if override and any([override.base_url, override.model, override.api_key]):
        return LLMClient(
            base_url=override.base_url,
            model=override.model,
            api_key=override.api_key,
        )
    return llm


def _merge_single(req: MergeRequest, company_name: str, template: Optional[str],
                   client: Optional[LLMClient] = None) -> MergeResult:
    active_llm = client or llm
    if not _has_any_content(req):
        return MergeResult(
            bot_id=req.bot_id,
            merged_instruction=build_template_only(company_name, template),
            status="template_only",
        )

    system = build_system_message(company_name, template)
    user = build_user_message(req.tone, req.guardrail, req.response)

    try:
        merged = active_llm.chat(system, user)
        return MergeResult(
            bot_id=req.bot_id,
            merged_instruction=merged,
            status="ok",
        )
    except Exception as exc:
        logger.exception("LLM call failed for bot_id=%s", req.bot_id)
        return MergeResult(
            bot_id=req.bot_id,
            merged_instruction=build_fallback(req.tone, req.guardrail, req.response),
            status="fallback",
            error_message=str(exc),
        )


@app.post("/merge-prompts", response_model=MergeResult)
def merge_single(req: MergeRequest, company_name: str = "Company"):
    return _merge_single(req, company_name, template=None)


@app.post("/merge-prompts/batch", response_model=BatchMergeResponse)
def merge_batch(req: BatchMergeRequest):
    if not req.agents:
        return BatchMergeResponse(results=[], total=0, succeeded=0, failed=0)

    active_client = _resolve_llm(req.llm_override)
    results: List[Optional[MergeResult]] = [None] * len(req.agents)
    max_workers = _batch_worker_count(len(req.agents))
    logger.info("Processing %d prompt merge request(s) with %d worker(s)", len(req.agents), max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_merge_single, agent, req.company_name, req.template, active_client): (idx, agent)
            for idx, agent in enumerate(req.agents)
        }
        for processed, future in enumerate(as_completed(future_map), start=1):
            idx, agent = future_map[future]
            result = future.result()
            results[idx] = result
            logger.info(
                "Processed %d/%d  bot_id=%s  status=%s",
                processed, len(req.agents), agent.bot_id, result.status,
            )

    final_results = [result for result in results if result is not None]

    succeeded = sum(1 for r in final_results if r.status in ("ok", "template_only"))
    failed = sum(1 for r in final_results if r.status == "fallback")

    return BatchMergeResponse(
        results=final_results,
        total=len(final_results),
        succeeded=succeeded,
        failed=failed,
    )


@app.get("/health")
def health():
    return {
        "status": "ok",
        "provider": llm.provider,
        "model": llm.model,
        "base_url": llm.effective_url,
    }


@app.get("/ready")
def ready():
    try:
        readiness = llm.readiness()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"LLM readiness check failed: {exc}") from exc

    if not readiness.get("chat_ok"):
        raise HTTPException(
            status_code=503,
            detail=f"Configured model `{llm.model}` is not reachable via chat/completions",
        )

    return {
        "status": "ready",
        **readiness,
    }
