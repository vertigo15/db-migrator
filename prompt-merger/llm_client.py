import os
import logging
from dataclasses import dataclass
from typing import Optional

from openai import OpenAI

logger = logging.getLogger(__name__)

_READINESS_SYSTEM = "You are a connectivity smoke test. Reply with exactly: OK"
_READINESS_USER = "Return OK"


def validate_base_url(base_url: str) -> str:
    """
    Validate and normalize an OpenAI-compatible API base URL.

    Raises:
        ValueError: If the URL is empty or includes a resource path such as
            /chat/completions instead of the API root (/v1).
    """
    url = (base_url or "").strip().rstrip("/")
    if not url:
        raise ValueError("Base URL is required")

    lower = url.lower()
    if lower.endswith("/chat/completions"):
        suggested = url[: -len("/chat/completions")].rstrip("/") or "https://host/v1"
        raise ValueError(
            "Base URL should end with /v1, not /chat/completions. "
            f"Use the API root instead, e.g. `{suggested}`"
        )

    return url


@dataclass(frozen=True)
class LLMConfig:
    """Environment-driven configuration for an OpenAI-compatible LLM endpoint."""

    provider_name: str
    base_url: str
    api_key: str
    model: str
    temperature: float
    max_tokens: int

    @classmethod
    def from_env(
        cls,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> "LLMConfig":
        resolved_base_url = base_url or os.getenv("LLM_BASE_URL")
        resolved_model = model or os.getenv("LLM_MODEL")

        if not resolved_base_url:
            raise ValueError("LLM_BASE_URL is required")
        if not resolved_model:
            raise ValueError("LLM_MODEL is required")

        return cls(
            provider_name=os.getenv("LLM_PROVIDER_NAME", "openai_compatible"),
            base_url=validate_base_url(resolved_base_url),
            api_key=api_key or os.getenv("LLM_API_KEY", "not-needed"),
            model=resolved_model,
            temperature=temperature if temperature is not None else float(os.getenv("LLM_TEMPERATURE", "0.3")),
            max_tokens=max_tokens if max_tokens is not None else int(os.getenv("LLM_MAX_TOKENS", "4096")),
        )


class LLMClient:
    """OpenAI-compatible LLM wrapper for prompt merger."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ):
        self.config = LLMConfig.from_env(
            base_url=base_url,
            api_key=api_key,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        self.provider = self.config.provider_name
        self.base_url = self.config.base_url
        self.api_key = self.config.api_key
        self.model = self.config.model
        self.temperature = self.config.temperature
        self.max_tokens = self.config.max_tokens
        self.client = OpenAI(base_url=self.base_url, api_key=self.api_key)

    @property
    def effective_url(self) -> str:
        return self.base_url

    def readiness(self) -> dict:
        """
        Verify the endpoint via POST /chat/completions.

        On-prem gateways often do not implement GET /models; a minimal chat
        request matches the path used by prompt merging in production.
        """
        logger.info(
            "LLM readiness check: provider=%s model=%s base_url=%s",
            self.provider,
            self.model,
            self.base_url,
        )
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": _READINESS_SYSTEM},
                {"role": "user", "content": _READINESS_USER},
            ],
            temperature=0,
            max_tokens=16,
        )
        content = response.choices[0].message.content or ""
        logger.info("LLM readiness OK: provider=%s chars=%d", self.provider, len(content))
        return {
            "provider": self.provider,
            "base_url": self.base_url,
            "model": self.model,
            "chat_ok": True,
            "response_preview": content[:200],
            # Kept for callers that still check model_available.
            "model_available": True,
        }

    def chat(self, system: str, user: str) -> str:
        """Send a chat request and return the assistant's text."""
        logger.info("LLM request: provider=%s model=%s base_url=%s", self.provider, self.model, self.base_url)
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=self.temperature,
            max_tokens=self.max_tokens,
        )
        content = response.choices[0].message.content or ""
        logger.info("LLM response: provider=%s chars=%d", self.provider, len(content))
        return content
