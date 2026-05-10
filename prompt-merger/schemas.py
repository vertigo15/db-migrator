from typing import List, Optional
from pydantic import BaseModel


class MergeRequest(BaseModel):
    bot_id: str
    tone: Optional[str] = None
    guardrail: Optional[str] = None
    response: Optional[str] = None


class BatchMergeRequest(BaseModel):
    agents: List[MergeRequest]
    company_name: str
    template: Optional[str] = None


class MergeResult(BaseModel):
    bot_id: str
    merged_instruction: str
    status: str  # "ok", "fallback", "template_only"
    error_message: Optional[str] = None


class BatchMergeResponse(BaseModel):
    results: List[MergeResult]
    total: int
    succeeded: int
    failed: int
