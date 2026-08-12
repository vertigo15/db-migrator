import pandas as pd

from utils.agent_preflight import (
    build_agent_manifest,
    inspect_agent_conflicts,
)
from utils.db import ConnectionConfig


TARGET = ConnectionConfig("target", 5432, "user_db", "user", "password")
OWNER_ID = "22222222-2222-4222-8222-222222222222"


def test_agent_preflight_reports_exact_adoption_and_all_mismatches(
    monkeypatch,
):
    agents = pd.DataFrame([
        {
            "bot_id": "exact-bot",
            "user_id": "legacy-user",
            "docs_chosen": [],
            "chosen_docs_folders": [],
        },
        {
            "bot_id": "mismatch-bot",
            "user_id": "legacy-user",
            "docs_chosen": [],
            "chosen_docs_folders": [],
        },
    ])
    manifest = build_agent_manifest(
        agents, {"legacy-user": OWNER_ID}
    )
    by_bot = {row["bot_id"]: row for row in manifest}

    def fake_query(_config, query, _params=None):
        if "FROM migration.id_mappings" in query:
            return pd.DataFrame(columns=[
                "old_id", "new_id", "mapping_owner_run"
            ])
        if "FROM agents WHERE" in query:
            return pd.DataFrame([
                {
                    "id": by_bot["exact-bot"]["agent_id"],
                    "user_id": OWNER_ID,
                },
                {
                    "id": by_bot["mismatch-bot"]["agent_id"],
                    "user_id": OWNER_ID,
                },
            ])
        if "FROM agent_settings" in query:
            return pd.DataFrame([
                {
                    "id": by_bot["exact-bot"]["settings_id"],
                    "agent_id": by_bot["exact-bot"]["agent_id"],
                },
                {
                    "id": "44444444-4444-4444-8444-444444444444",
                    "agent_id": by_bot["mismatch-bot"]["agent_id"],
                },
            ])
        if "FROM knowledge_bases" in query:
            return pd.DataFrame(columns=["id"])
        if "FROM knowledge_base_assignments" in query:
            return pd.DataFrame(columns=[
                "id", "knowledge_base_id", "assigned_to_id"
            ])
        if "FROM knowledge_base_items" in query:
            return pd.DataFrame(columns=["id", "knowledge_base_id"])
        raise AssertionError(query)

    monkeypatch.setattr("utils.agent_preflight.execute_query", fake_query)

    result = inspect_agent_conflicts(TARGET, manifest)

    assert [row["bot_id"] for row in result["will_adopt"]] == [
        "exact-bot"
    ]
    assert [row["bot_id"] for row in result["conflicts"]] == [
        "mismatch-bot"
    ]


def test_agent_preflight_rejects_canonical_wrong_owner(monkeypatch):
    agents = pd.DataFrame([{
        "bot_id": "mapped-bot",
        "user_id": "legacy-user",
        "docs_chosen": [],
        "chosen_docs_folders": [],
    }])
    manifest = build_agent_manifest(
        agents, {"legacy-user": OWNER_ID}
    )
    planned = manifest[0]

    def fake_query(_config, query, _params=None):
        if "FROM migration.id_mappings" in query:
            return pd.DataFrame([{
                "old_id": planned["bot_id"],
                "new_id": planned["agent_id"],
                "mapping_owner_run": None,
            }])
        if "FROM agents WHERE" in query:
            return pd.DataFrame([{
                "id": planned["agent_id"],
                "user_id": "33333333-3333-4333-8333-333333333333",
            }])
        if "FROM agent_settings" in query:
            return pd.DataFrame(columns=["id", "agent_id"])
        if "FROM knowledge_bases" in query:
            return pd.DataFrame(columns=["id"])
        if "FROM knowledge_base_assignments" in query:
            return pd.DataFrame(columns=[
                "id", "knowledge_base_id", "assigned_to_id"
            ])
        return pd.DataFrame(columns=["id", "knowledge_base_id"])

    monkeypatch.setattr("utils.agent_preflight.execute_query", fake_query)

    result = inspect_agent_conflicts(TARGET, manifest)

    assert result["will_adopt"] == []
    assert result["conflicts"][0]["reason"] == "canonical_owner_mismatch"
