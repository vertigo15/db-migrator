import pandas as pd

from utils.db import ConnectionConfig
from utils.ownership_preflight import (
    find_canonical_ownership_conflicts,
    ownership_conflict_message,
)


TARGET = ConnectionConfig("target", 5432, "user_db", "user", "password")


def test_preflight_reports_live_mapping_owned_by_another_user(monkeypatch):
    calls = []

    def fake_query(config, query, params=None):
        calls.append((config.database, query, params))
        if params[2] == "folders":
            return pd.DataFrame([{
                "old_id": "202",
                "mapped_entity_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "mapping_owner_run": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                "record_action": "created",
                "actual_owner_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
                "expected_owner_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            }])
        return pd.DataFrame()

    monkeypatch.setattr(
        "utils.ownership_preflight.execute_query",
        fake_query,
    )

    conflicts = find_canonical_ownership_conflicts(
        TARGET,
        {
            "folders": [{"old_id": "202", "owner_id": "legacy-owner"}],
            "documents": [],
        },
        {
            "legacy-owner": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        },
    )

    assert calls[0][0] == "document_db"
    assert conflicts == [{
        "entity_type": "folders",
        "old_id": "202",
        "mapped_entity_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "mapping_owner_run": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "record_action": "created",
        "actual_owner_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        "expected_owner_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    }]
    message = ownership_conflict_message(conflicts)
    assert "stopped before any migration shards ran" in message
    assert "folders:202" in message


def test_preflight_accepts_empty_owned_scope(monkeypatch):
    monkeypatch.setattr(
        "utils.ownership_preflight.execute_query",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("empty scope must not query")
        ),
    )

    assert find_canonical_ownership_conflicts(
        TARGET,
        {"folders": [], "documents": []},
    ) == []
