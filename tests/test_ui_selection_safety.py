import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]


def _identity_decorator(func=None, **_kwargs):
    if func is not None:
        return func
    return lambda decorated: decorated


def _load_page(module_name, relative_path):
    fake_streamlit = MagicMock()
    fake_streamlit.session_state = {}
    fake_streamlit.cache_data = _identity_decorator
    fake_streamlit.fragment = _identity_decorator
    original_streamlit = sys.modules.get("streamlit")
    sys.modules["streamlit"] = fake_streamlit
    try:
        spec = importlib.util.spec_from_file_location(
            module_name,
            BASE_DIR / relative_path,
        )
        module = importlib.util.module_from_spec(spec)
        assert spec.loader
        spec.loader.exec_module(module)
        return module
    finally:
        if original_streamlit is None:
            sys.modules.pop("streamlit", None)
        else:
            sys.modules["streamlit"] = original_streamlit


select_page = _load_page("select_data_page", "pages/2_select_data.py")
connect_page = _load_page("connect_page", "pages/1_connect.py")


def test_ui_search_treats_regex_characters_as_literal_text():
    values = pd.Series(["yaela+1@jeen.ai", "yaelaa1@jeen.ai", None])

    matches = select_page._contains_literal(values, "yaela+1")

    assert matches.tolist() == [True, False, False]


def test_bulk_selection_invalidates_stale_editor_state():
    select_page.st.session_state.clear()
    select_page.st.session_state["_p2_users_editor_revision"] = 4

    select_page._queue_bulk_user_selection(
        ["first@example.com", "first@example.com", "second@example.com"]
    )

    assert select_page.st.session_state["_p2_batch_saved_emails"] == [
        "first@example.com",
        "second@example.com",
    ]
    assert select_page.st.session_state["_p2_users_editor_revision"] == 5


def test_pasted_emails_match_fixed_width_source_values():
    matched, unmatched = select_page._match_user_emails(
        [
            "Mark@jeen.ai       ",
            "ido@jeen.ai       ",
        ],
        [
            " mark@JEEN.AI ",
            "missing@jeen.ai",
        ],
    )

    assert matched == ["Mark@jeen.ai"]
    assert unmatched == ["missing@jeen.ai"]


def test_live_selection_merge_preserves_hidden_rows_and_applies_visible_edits():
    selected = select_page._merge_visible_selection(
        previous=["hidden@example.com", "visible-old@example.com"],
        visible_ids=["visible-old@example.com", "visible-new@example.com"],
        selected_visible_ids=["visible-new@example.com"],
        valid_ids=[
            "hidden@example.com",
            "visible-old@example.com",
            "visible-new@example.com",
        ],
    )

    assert selected == ["hidden@example.com", "visible-new@example.com"]


def test_live_selection_merge_drops_ids_missing_from_current_dataset():
    selected = select_page._merge_visible_selection(
        previous=["deleted-user", "still-present"],
        visible_ids=[],
        selected_visible_ids=[],
        valid_ids=["still-present"],
    )

    assert selected == ["still-present"]


def test_selected_users_move_first_without_changing_active_sort_order():
    frame = pd.DataFrame(
        {
            "email": [
                "highest-docs@example.com",
                "selected-first@example.com",
                "selected-second@example.com",
                "lowest-docs@example.com",
            ],
            "doc_count": [100, 80, 60, 40],
            "selected": [False, True, True, False],
        }
    )

    prioritized = select_page._prioritize_selected_rows(frame)

    assert prioritized["email"].tolist() == [
        "selected-first@example.com",
        "selected-second@example.com",
        "highest-docs@example.com",
        "lowest-docs@example.com",
    ]


def test_audit_summary_ignores_cached_verification_counts(monkeypatch):
    table_status = {
        "users": {
            "exists": True,
            "actual_name": "jeen_dev_users",
            "row_count": 10,
        },
        "agents": {
            "exists": True,
            "actual_name": "playground_bot_generator_config",
            "row_count": 20,
        },
    }
    fresh_counts = {
        "jeen_dev_users": 11,
        "playground_bot_generator_config": 22,
    }
    monkeypatch.setattr(
        connect_page,
        "get_table_row_count",
        lambda _config, table: fresh_counts[table],
    )
    monkeypatch.setattr(
        connect_page,
        "_get_agent_knowledge_counts",
        lambda _config, _prefix: {
            "total": 22,
            "with_knowledge": 8,
            "without_knowledge": 14,
        },
    )

    summary = connect_page._build_fresh_audit_summary(
        object(), "jeen_dev", table_status
    )

    assert [item["count"] for item in summary] == [11, 22]
    assert table_status["users"]["row_count"] == 11
    assert table_status["agents"]["row_count"] == 22


def test_workload_guardrail_blocks_unsafe_embedding_volume(monkeypatch):
    monkeypatch.setenv("MIGRATION_MAX_SAFE_EMBEDDING_BYTES", "1000")
    monkeypatch.setenv("MIGRATION_HEAVY_USER_AVG_EMBEDDING_BYTES", "10000")

    blocked, messages = select_page._evaluate_workload_guardrails(
        10,
        1001,
        override=False,
    )

    assert blocked is True
    assert any(level == "error" for level, _ in messages)


def test_workload_guardrail_requires_explicit_override_for_mixed_heavy_users(
    monkeypatch,
):
    monkeypatch.setenv("MIGRATION_MAX_SAFE_EMBEDDING_BYTES", "1000000")
    monkeypatch.setenv("MIGRATION_HEAVY_USER_AVG_EMBEDDING_BYTES", "100")

    blocked, _ = select_page._evaluate_workload_guardrails(
        2,
        1000,
        override=False,
    )
    overridden, messages = select_page._evaluate_workload_guardrails(
        2,
        1000,
        override=True,
    )

    assert blocked is True
    assert overridden is False
    assert any("override accepted" in message.lower() for _, message in messages)


def test_workload_guardrail_recommends_smaller_batches_without_hard_block(
    monkeypatch,
):
    monkeypatch.setenv("MIGRATION_MAX_SAFE_EMBEDDING_BYTES", "1000000")
    monkeypatch.setenv("MIGRATION_HEAVY_USER_AVG_EMBEDDING_BYTES", "1000000")

    blocked, messages = select_page._evaluate_workload_guardrails(
        75,
        1000,
        override=False,
    )

    assert blocked is False
    assert any("25–50" in message for _, message in messages)
