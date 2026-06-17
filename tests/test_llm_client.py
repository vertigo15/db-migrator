"""Tests for prompt-merger LLM client URL validation and readiness check."""
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

PROMPT_MERGER_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompt-merger")
if PROMPT_MERGER_DIR not in sys.path:
    sys.path.insert(0, PROMPT_MERGER_DIR)

from llm_client import LLMClient, validate_base_url  # noqa: E402


def test_validate_base_url_accepts_v1_root():
    assert validate_base_url("http://itcids-gateway/v1") == "http://itcids-gateway/v1"
    assert validate_base_url("http://host/v1/") == "http://host/v1"


def test_validate_base_url_rejects_chat_completions_path():
    with pytest.raises(ValueError, match="should end with /v1, not /chat/completions"):
        validate_base_url("http://itcids-gateway/v1/chat/completions")


def test_validate_base_url_requires_value():
    with pytest.raises(ValueError, match="Base URL is required"):
        validate_base_url("   ")


@patch("llm_client.OpenAI")
def test_readiness_uses_chat_completions_not_models_list(mock_openai_cls):
    mock_client = MagicMock()
    mock_openai_cls.return_value = mock_client
    mock_response = MagicMock()
    mock_response.choices = [MagicMock(message=MagicMock(content="OK"))]
    mock_client.chat.completions.create.return_value = mock_response

    client = LLMClient(
        base_url="http://itcids-gateway/v1",
        model="itc-g5",
        api_key="test-key",
    )
    info = client.readiness()

    mock_client.models.list.assert_not_called()
    mock_client.chat.completions.create.assert_called_once()
    assert info["chat_ok"] is True
    assert info["response_preview"] == "OK"
    assert info["model_available"] is True
