import os
import logging
from typing import Optional

import requests
from openai import OpenAI

logger = logging.getLogger(__name__)


class LLMClient:
    """Generic LLM wrapper for prompt merger.

    Supported providers:
    - openai_compatible: any endpoint implementing OpenAI chat completions
    - bedrock_converse: Bedrock Converse-style HTTP endpoint with bearer token
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ):
        self.provider = os.getenv("LLM_PROVIDER", "openai_compatible").strip().lower()
        self.base_url = base_url or os.getenv("LLM_BASE_URL", "http://localhost:8000/v1")
        self.api_key = api_key or os.getenv("LLM_API_KEY", "not-needed")
        self.bedrock_converse_url = os.getenv("BEDROCK_CONVERSE_URL", "")
        self.bedrock_auth_token = os.getenv("BEDROCK_AUTH_TOKEN", "")
        self.model = model or os.getenv("LLM_MODEL", "gemma-3-12b-it")
        self.temperature = temperature if temperature is not None else float(os.getenv("LLM_TEMPERATURE", "0.3"))
        self.max_tokens = max_tokens if max_tokens is not None else int(os.getenv("LLM_MAX_TOKENS", "4096"))

        self.client = None
        if self.provider == "openai_compatible":
            self.client = OpenAI(base_url=self.base_url, api_key=self.api_key)

        if self.provider == "bedrock_converse" and not self.bedrock_converse_url:
            raise ValueError("BEDROCK_CONVERSE_URL is required when LLM_PROVIDER=bedrock_converse")
        if self.provider == "bedrock_converse" and not self.bedrock_auth_token:
            raise ValueError("BEDROCK_AUTH_TOKEN is required when LLM_PROVIDER=bedrock_converse")

    @property
    def effective_url(self) -> str:
        if self.provider == "bedrock_converse":
            return self.bedrock_converse_url
        return self.base_url

    def chat(self, system: str, user: str) -> str:
        """Send a chat request and return the assistant's text."""
        if self.provider == "bedrock_converse":
            return self._chat_bedrock_converse(system, user)
        return self._chat_openai_compatible(system, user)

    def _chat_openai_compatible(self, system: str, user: str) -> str:
        logger.info("OpenAI-compatible LLM request: model=%s base_url=%s", self.model, self.base_url)
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
        logger.info("OpenAI-compatible LLM response: %d chars", len(content))
        return content

    def _chat_bedrock_converse(self, system: str, user: str) -> str:
        logger.info("Bedrock Converse LLM request: model=%s url=%s", self.model, self.bedrock_converse_url)
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": [{"text": user or "Processing..."}],
                }
            ],
            "inferenceConfig": {
                "temperature": self.temperature,
                "maxTokens": self.max_tokens,
            },
            "system": [{"text": system}] if system else [],
        }
        response = requests.post(
            self.bedrock_converse_url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.bedrock_auth_token}",
            },
            json=payload,
            timeout=300,
        )
        response.raise_for_status()
        data = response.json()
        contents = data.get("output", {}).get("message", {}).get("content", [])
        text_parts = [item["text"] for item in contents if isinstance(item, dict) and item.get("text")]
        content = "".join(text_parts).strip()
        logger.info("Bedrock Converse LLM response: %d chars", len(content))
        if not content:
            raise ValueError("Bedrock Converse response did not contain text output")
        return content
