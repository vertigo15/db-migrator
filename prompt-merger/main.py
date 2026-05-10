import logging
import os
from typing import List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI

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
    MergeRequest,
    MergeResult,
)

env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("prompt-merger")

app = FastAPI(title="Prompt Merger", version="1.0.0")
llm = LLMClient()


def _has_any_content(req: MergeRequest) -> bool:
    return any(
        v and v.strip()
        for v in (req.tone, req.guardrail, req.response)
    )


def _merge_single(req: MergeRequest, company_name: str, template: Optional[str]) -> MergeResult:
    if not _has_any_content(req):
        return MergeResult(
            bot_id=req.bot_id,
            merged_instruction=build_template_only(company_name, template),
            status="template_only",
        )

    system = build_system_message(company_name, template)
    user = build_user_message(req.tone, req.guardrail, req.response)

    try:
        merged = llm.chat(system, user)
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
    results: List[MergeResult] = []
    for agent in req.agents:
        result = _merge_single(agent, req.company_name, req.template)
        results.append(result)
        logger.info(
            "Processed %d/%d  bot_id=%s  status=%s",
            len(results), len(req.agents), agent.bot_id, result.status,
        )

    succeeded = sum(1 for r in results if r.status in ("ok", "template_only"))
    failed = sum(1 for r in results if r.status == "fallback")

    return BatchMergeResponse(
        results=results,
        total=len(results),
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
