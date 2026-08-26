import pandas as pd

from utils.db import ConnectionConfig
from utils.extraction import get_selection_summary
from utils.file_preview import (
    migration_file_metadata,
    read_inline_download,
    read_text_preview,
)
from utils.rollback import load_batch_step_statuses
from utils import storage
from utils.ui_performance import resolve_lazy_value


def test_sql_preview_is_bounded_and_large_download_is_not_loaded(tmp_path):
    path = tmp_path / "04_chunks_embeddings.sql"
    path.write_text("x" * 100_000, encoding="utf-8")

    preview, truncated = read_text_preview(str(path), max_bytes=50_000)

    assert len(preview.encode("utf-8")) == 50_000
    assert truncated is True
    assert read_inline_download(str(path), max_bytes=10_000) is None


def test_migration_files_are_scoped_to_explicit_active_paths(tmp_path):
    active = tmp_path / "01_users_active.sql"
    stale = tmp_path / "02_folders_stale.sql"
    active.write_text("SELECT 1;", encoding="utf-8")
    stale.write_text("SELECT 2;", encoding="utf-8")

    files = migration_file_metadata(
        [str(active)],
        {"01_users_": "user_db", "02_folders_": "document_db"},
    )

    assert [item["filename"] for item in files] == [active.name]
    assert files[0]["target_db"] == "user_db"


def test_lazy_value_does_not_build_until_requested():
    cache = {}
    calls = []

    def builder():
        calls.append(True)
        return {"rows": 42}

    assert resolve_lazy_value(
        cache,
        value_key="plan",
        fingerprint_key="plan_key",
        fingerprint=("user-1",),
        requested=False,
        builder=builder,
    ) is None
    assert calls == []

    value = resolve_lazy_value(
        cache,
        value_key="plan",
        fingerprint_key="plan_key",
        fingerprint=("user-1",),
        requested=True,
        builder=builder,
    )
    assert value == {"rows": 42}
    assert len(calls) == 1

    cached = resolve_lazy_value(
        cache,
        value_key="plan",
        fingerprint_key="plan_key",
        fingerprint=("user-1",),
        requested=False,
        builder=builder,
    )
    assert cached == value
    assert len(calls) == 1


def test_unchanged_storage_value_skips_browser_write(monkeypatch):
    writes = []
    monkeypatch.setattr(storage.st, "session_state", {})
    monkeypatch.setattr(
        storage,
        "_write_to_localstorage",
        lambda key, value: writes.append((key, value)),
    )

    assert storage.save_selected_users(["user@example.com"]) is True
    assert storage.save_selected_users(["user@example.com"]) is True

    assert len(writes) == 1


def test_selection_summary_uses_one_query(monkeypatch):
    calls = []

    def fake_execute(config, query, params):
        calls.append((query, params))
        return pd.DataFrame(
            [{
                "documents": 3,
                "folders": 2,
                "embeddings": 12,
                "embedding_bytes": 1024,
                "agents": 4,
                "logs": 5,
            }]
        )

    monkeypatch.setattr("utils.extraction.execute_query", fake_execute)
    result = get_selection_summary(
        ConnectionConfig("host", 5432, "source", "user", "pass"),
        "jeen_dev",
        ["user-1"],
        filters={},
    )

    assert len(calls) == 1
    assert result["documents"] == 3
    assert result["embedding_bytes"] == 1024


def test_step_status_reads_are_grouped_per_database(monkeypatch):
    calls = []

    def fake_execute(config, query, params):
        calls.append(config.database)
        return pd.DataFrame(
            {
                "step_key": params[1],
                "status": ["executed"] * len(params[1]),
            }
        )

    monkeypatch.setattr("utils.rollback.execute_query", fake_execute)
    statuses = load_batch_step_statuses(
        ConnectionConfig("host", 5432, "user_db", "user", "pass"),
        "00000000-0000-0000-0000-000000000001",
    )

    assert calls == ["user_db", "document_db", "completion_db"]
    assert len(statuses) == 7
    assert set(statuses.values()) == {"executed"}
