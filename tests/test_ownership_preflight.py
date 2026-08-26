import pandas as pd

from utils.db import ConnectionConfig
from utils.ownership_preflight import (
    active_owner_folder_conflicts,
    find_canonical_ownership_conflicts,
    ownership_conflict_message,
    repair_orphaned_document_owners,
    repair_orphaned_folder_owners,
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


def test_orphaned_document_owner_is_repaired_only_to_existing_user(
    monkeypatch,
):
    expected_owner = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    actual_owner = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    conflict = {
        "entity_type": "documents",
        "old_id": "legacy-doc",
        "mapped_entity_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "actual_owner_id": actual_owner,
        "expected_owner_id": expected_owner,
    }
    monkeypatch.setattr(
        "utils.ownership_preflight.execute_query",
        lambda *_args, **_kwargs: pd.DataFrame([{"id": expected_owner}]),
    )

    class FakeCursor:
        rowcount = 0

        def __init__(self):
            self.params = None

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, _query, params):
            self.params = params
            self.rowcount = 1

    class FakeConnection:
        def __init__(self):
            self.cursor_instance = FakeCursor()
            self.committed = False
            self.closed = False

        def cursor(self):
            return self.cursor_instance

        def commit(self):
            self.committed = True

        def rollback(self):
            raise AssertionError("repair should not roll back")

        def close(self):
            self.closed = True

    connection = FakeConnection()
    monkeypatch.setattr(
        "utils.ownership_preflight.get_connection",
        lambda _config: connection,
    )

    repaired = repair_orphaned_document_owners(TARGET, [conflict])

    assert repaired == [conflict]
    assert connection.cursor_instance.params == (
        expected_owner,
        conflict["mapped_entity_id"],
        actual_owner,
        "documents",
        conflict["old_id"],
    )
    assert connection.committed is True
    assert connection.closed is True


def test_active_folder_owner_is_excluded_from_automatic_repair(monkeypatch):
    active_owner = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    conflict = {
        "entity_type": "folders",
        "old_id": "565",
        "mapped_entity_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "actual_owner_id": active_owner,
        "expected_owner_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    }
    monkeypatch.setattr(
        "utils.ownership_preflight.execute_query",
        lambda *_args, **_kwargs: pd.DataFrame([{"id": active_owner}]),
    )
    monkeypatch.setattr(
        "utils.ownership_preflight.get_connection",
        lambda _config: (_ for _ in ()).throw(
            AssertionError("active owner must never be reassigned")
        ),
    )

    assert repair_orphaned_folder_owners(TARGET, [conflict]) == []
    assert active_owner_folder_conflicts(TARGET, [conflict]) == [conflict]
