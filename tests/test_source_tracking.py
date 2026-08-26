from unittest.mock import MagicMock, call

from utils.db import ConnectionConfig
from utils.migration_tracking import (
    SOURCE_TRACKING_DDL,
    create_source_run,
    update_source_run,
)


SOURCE = ConnectionConfig("src-host", 5432, "postgres", "user", "pass")


def test_source_tracking_ddl_is_audit_only():
    lowered = SOURCE_TRACKING_DDL.lower()
    assert "create schema if not exists migration" in lowered
    assert "migration.migration_runs" in lowered
    assert "migration.migration_user_results" in lowered
    assert "migration.migration_steps" in lowered
    # Never touch V4 business tables.
    assert "jeen_" not in lowered
    assert "playground_bot" not in lowered
    assert "delete from public" not in lowered


def test_create_source_run_writes_run_users_and_steps(monkeypatch):
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    monkeypatch.setattr(
        "utils.migration_tracking.get_connection",
        lambda _config: conn,
    )
    monkeypatch.setattr(
        "utils.migration_tracking.ensure_source_tracking_schema",
        lambda _config: None,
    )

    create_source_run(
        SOURCE,
        "11111111-1111-4111-8111-111111111111",
        [{
            "email": "a@example.com",
            "legacy_user_id": "legacy-a",
            "v5_user_id": "22222222-2222-4222-8222-222222222222",
            "action": "created",
        }],
        {"host": "src-host", "database": "postgres", "prefix": "jeen_dev"},
        {"host": "tgt-host", "port": 5434, "databases": ["user_db"]},
    )

    sql_blobs = "\n".join(str(args[0]) for args, _kwargs in cursor.execute.call_args_list)
    assert "INSERT INTO migration.migration_runs" in sql_blobs
    assert "INSERT INTO migration.migration_steps" in sql_blobs
    assert "INSERT INTO migration.migration_user_results" in sql_blobs
    assert cursor.execute.call_count >= 3


def test_update_source_run_sets_status(monkeypatch):
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    monkeypatch.setattr(
        "utils.migration_tracking.get_connection",
        lambda _config: conn,
    )
    monkeypatch.setattr(
        "utils.migration_tracking.ensure_source_tracking_schema",
        lambda _config: None,
    )

    update_source_run(
        SOURCE,
        "11111111-1111-4111-8111-111111111111",
        "completed",
    )

    args, _kwargs = cursor.execute.call_args
    assert "UPDATE migration.migration_runs" in args[0]
    assert args[1][0] == "completed"
