import importlib.util
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))


def _load_select_data_page():
    spec = importlib.util.spec_from_file_location(
        "select_data_page",
        BASE_DIR / "pages" / "2_select_data.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


select_data = _load_select_data_page()


def test_no_doc_default_tone_uses_v5_default_instructions():
    v4_default, v5_default = select_data._load_no_doc_prompt_defaults()

    merged, status = select_data._resolve_no_doc_instruction(
        v4_default,
        v4_default,
        v5_default,
    )

    assert status == "v5_default_response_instructions"
    assert merged == v5_default.strip()


def test_no_doc_default_comparison_ignores_outer_whitespace_and_line_endings():
    v4_default, v5_default = select_data._load_no_doc_prompt_defaults()
    stored_tone = "\r\n" + v4_default.replace("\n", "\r\n") + "  "

    merged, status = select_data._resolve_no_doc_instruction(
        stored_tone,
        v4_default,
        v5_default,
    )

    assert status == "v5_default_response_instructions"
    assert merged == v5_default.strip()


def test_no_doc_custom_tone_uses_user_prompt_directly():
    v4_default, v5_default = select_data._load_no_doc_prompt_defaults()
    custom_tone = "You are a finance specialist. Answer in Hebrew."

    merged, status = select_data._resolve_no_doc_instruction(
        custom_tone,
        v4_default,
        v5_default,
    )

    assert status == "v4_user_prompt"
    assert merged == custom_tone


def test_has_agent_knowledge_detects_docs_or_folders():
    assert select_data._has_agent_knowledge(None, None) is False
    assert select_data._has_agent_knowledge([], "{}") is False
    assert select_data._has_agent_knowledge(["doc-1"], None) is True
    assert select_data._has_agent_knowledge(None, "{123}") is True


def test_build_prompt_merge_routes_sends_knowledge_agents_to_llm_only():
    v4_default, v5_default = select_data._load_no_doc_prompt_defaults()
    prompt_parts = {
        "knowledge_bot": {
            "tone": "tone",
            "guardrail": "guardrail",
            "response": "response",
            "has_knowledge": True,
        },
        "no_doc_bot": {
            "tone": "Customized prompt",
            "guardrail": "ignored",
            "response": "ignored",
            "has_knowledge": False,
        },
    }

    merge_requests, local_results = select_data._build_prompt_merge_routes(
        ["knowledge_bot", "no_doc_bot"],
        prompt_parts,
        v4_default,
        v5_default,
    )

    assert merge_requests == [{
        "bot_id": "knowledge_bot",
        "tone": "tone",
        "guardrail": "guardrail",
        "response": "response",
    }]
    assert local_results == [{
        "bot_id": "no_doc_bot",
        "merged_instruction": "Customized prompt",
        "status": "v4_user_prompt",
        "error_message": None,
        "sent_to_llm": False,
    }]
