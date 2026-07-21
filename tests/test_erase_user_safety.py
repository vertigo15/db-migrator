import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock


BASE_DIR = Path(__file__).resolve().parents[1]


def _load_erase_page():
    fake_streamlit = MagicMock()
    fake_streamlit.session_state = {}
    fake_streamlit.form_submit_button.return_value = False
    fake_streamlit.columns.side_effect = (
        lambda count: tuple(MagicMock() for _ in range(count))
    )
    original_streamlit = sys.modules.get("streamlit")
    sys.modules["streamlit"] = fake_streamlit
    try:
        spec = importlib.util.spec_from_file_location(
            "erase_user_page",
            BASE_DIR / "pages" / "5_erase_user_data_v5.py",
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


erase_page = _load_erase_page()


def test_migration_relations_are_optional_and_batch_logs_are_preserved():
    assert (
        "completion_db",
        "migration.id_mappings",
    ) in erase_page.OPTIONAL_RELATIONS
    assert (
        "completion_db",
        "public.legacy_bot_to_agent_mapping",
    ) in erase_page.OPTIONAL_RELATIONS

    all_labels = [step[1] for step in erase_page.DELETION_STEPS]
    assert "migration.batch_log" not in all_labels
    assert "migration.migration_runs" not in all_labels
    assert "migration.migration_user_results" in all_labels
    assert "conversions" in all_labels
    assert "migration.id_mappings (conversions)" in all_labels


def test_missing_optional_tracking_relation_is_skipped(monkeypatch):
    optional_key = ("completion_db", "migration.id_mappings")
    availability = {
        (db, erase_page._relation_name(table)): True
        for db, table, _description, _query in erase_page.COUNT_QUERIES
    }
    availability[optional_key] = False

    monkeypatch.setattr(
        erase_page,
        "_scan_relations",
        lambda _config: (availability, []),
    )
    monkeypatch.setattr(
        erase_page,
        "_run_count",
        lambda *_args, **_kwargs: (0, None),
    )

    plan = erase_page._build_deletion_plan(object(), ["user-id"])

    assert plan["blockers"] == []
    assert (
        "completion_db",
        "migration.id_mappings (conversations)",
    ) in plan["skipped_steps"]
    matching = [
        row for row in plan["rows"]
        if row["Table"] == "migration.id_mappings (conversations)"
    ][0]
    assert matching["Rows to Affect"] == "NOT PRESENT"
    assert matching["Status"] == "Optional — skipped"


def test_missing_required_relation_blocks_erasure(monkeypatch):
    required_key = ("user_db", "public.users")
    availability = {
        (db, erase_page._relation_name(table)): True
        for db, table, _description, _query in erase_page.COUNT_QUERIES
    }
    availability[required_key] = False

    monkeypatch.setattr(
        erase_page,
        "_scan_relations",
        lambda _config: (
            availability,
            ["Required relation user_db.public.users does not exist"],
        ),
    )
    monkeypatch.setattr(
        erase_page,
        "_run_count",
        lambda *_args, **_kwargs: (0, None),
    )

    plan = erase_page._build_deletion_plan(object(), ["user-id"])

    assert plan["blockers"]
    matching = [
        row for row in plan["rows"] if row["Table"] == "users"
    ][0]
    assert matching["Rows to Affect"] == "BLOCKED"
    assert matching["Status"] == "Required — missing"


def test_optional_tracking_query_incompatibility_does_not_block(monkeypatch):
    availability = {
        (db, erase_page._relation_name(table)): True
        for db, table, _description, _query in erase_page.COUNT_QUERIES
    }

    monkeypatch.setattr(
        erase_page,
        "_scan_relations",
        lambda _config: (availability, []),
    )

    def fake_count(_config, db_name, query, _user_ids):
        if db_name == "completion_db" and "migration.id_mappings" in query:
            return None, "legacy tracking schema mismatch"
        return 0, None

    monkeypatch.setattr(erase_page, "_run_count", fake_count)
    plan = erase_page._build_deletion_plan(object(), ["user-id"])

    assert plan["blockers"] == []
    assert (
        "completion_db",
        "migration.id_mappings (agents)",
    ) in plan["skipped_steps"]
