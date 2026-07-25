"""Unit tests for the pure progress-aggregation helpers behind the async UI,
plus an integration test proving enqueue_run reads real manifests correctly."""
import json
import uuid

import psycopg2
import pytest

from utils import shard_queue
from utils.db import ConnectionConfig
from utils.migration_tracking import config_for_database, ensure_tracking_schema
from utils.queue_ui import (
    enqueue_step,
    enqueue_run,
    failed_shard_details,
    has_actionable_failures,
    has_in_flight_shards,
    is_run_fully_enqueued_and_done,
    overall_counts,
    resume_run,
    run_progress_by_step,
)
from utils.shard_queue import claim_shard, complete_shard, fail_shard


def test_is_run_fully_enqueued_and_done_requires_all_completed():
    assert is_run_fully_enqueued_and_done({"01_users": {"completed": 3}}) is True
    assert is_run_fully_enqueued_and_done({"01_users": {"completed": 2, "queued": 1}}) is False
    assert is_run_fully_enqueued_and_done({"01_users": {}}) is False
    assert is_run_fully_enqueued_and_done({}) is False


def test_has_actionable_failures_and_in_flight():
    progress = {"01_users": {"completed": 1}, "02_folders": {"failed": 1}}
    assert has_actionable_failures(progress) is True
    assert has_in_flight_shards(progress) is False

    progress2 = {"01_users": {"running": 1}}
    assert has_actionable_failures(progress2) is False
    assert has_in_flight_shards(progress2) is True


def test_overall_counts_sums_across_steps():
    progress = {
        "01_users": {"completed": 2, "failed": 1},
        "02_folders": {"completed": 3},
    }
    totals = overall_counts(progress)
    assert totals["completed"] == 5
    assert totals["failed"] == 1
    assert totals["queued"] == 0


def _connect(base: ConnectionConfig, database: str):
    return psycopg2.connect(host=base.host, port=base.port, dbname=database, user=base.username)


@pytest.fixture(autouse=True)
def _clean_shards(postgres_cluster):
    for database in ("user_db", "document_db", "completion_db"):
        ensure_tracking_schema(
            config_for_database(postgres_cluster, database),
            coordinator=database == "user_db",
        )
        conn = _connect(postgres_cluster, database)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("SELECT to_regclass('migration.migration_shards')")
                if cursor.fetchone()[0] is not None:
                    cursor.execute("TRUNCATE TABLE migration.migration_shards")
        finally:
            conn.close()
    yield


def _seed_step(base, run_id, step_key, database, status="running"):
    config = config_for_database(base, database)
    ensure_tracking_schema(config, coordinator=database == "user_db")
    conn = _connect(base, database)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO migration.migration_runs (id, status, total_users) "
                "VALUES (%s::uuid, 'running', 1) ON CONFLICT (id) DO NOTHING",
                (run_id,),
            )
            cursor.execute(
                """
                INSERT INTO migration.migration_steps
                    (migration_run_id, step_key, target_database, status)
                VALUES (%s::uuid, %s, %s, %s)
                ON CONFLICT (migration_run_id, step_key) DO UPDATE SET status = EXCLUDED.status
                """,
                (run_id, step_key, database, status),
            )
    finally:
        conn.close()


def _write_manifest(tmp_path, primary_file, shard_count):
    shards = [
        {
            "shard_index": index,
            "file_path": str(primary_file) if index == 1 else str(tmp_path / f"shard{index}.sql"),
            "expected_rows": 5,
            "byte_size": 100,
            "checksum": f"chk-{index}",
        }
        for index in range(1, shard_count + 1)
    ]
    manifest = {"shards": shards, "total_rows": 5 * shard_count}
    manifest_path = str(primary_file) + ".manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh)
    return manifest_path


def test_enqueue_run_reads_manifests_and_populates_queue(postgres_cluster, tmp_path):
    run_id = str(uuid.uuid4())
    _seed_step(postgres_cluster, run_id, "01_users", "user_db")
    _seed_step(postgres_cluster, run_id, "02_folders", "document_db")

    users_file = tmp_path / "01_users_20260101.sql"
    users_file.write_text("-- primary shard\n", encoding="utf-8")
    _write_manifest(tmp_path, users_file, shard_count=2)

    folders_file = tmp_path / "02_folders_20260101.sql"
    folders_file.write_text("-- primary shard\n", encoding="utf-8")
    _write_manifest(tmp_path, folders_file, shard_count=1)

    # 03_documents has no generated file (zero-row step) and must be skipped.
    enqueued = enqueue_run(
        postgres_cluster,
        run_id,
        {"01_users_": str(users_file), "02_folders_": str(folders_file)},
        owner_emails=["a@x.com"],
    )
    assert enqueued == {"01_users": 2, "02_folders": 1}

    progress = run_progress_by_step(postgres_cluster, run_id)
    assert progress["01_users"] == {"queued": 2}
    assert progress["02_folders"] == {"queued": 1}
    assert progress["03_documents"] == {}
    assert is_run_fully_enqueued_and_done(progress) is False


def test_enqueue_step_targets_only_requested_manifest(postgres_cluster, tmp_path):
    run_id = str(uuid.uuid4())
    _seed_step(postgres_cluster, run_id, "01_users", "user_db")
    users_file = tmp_path / "01_users_targeted.sql"
    users_file.write_text("-- primary shard\n", encoding="utf-8")
    _write_manifest(tmp_path, users_file, shard_count=2)

    assert enqueue_step(
        postgres_cluster,
        run_id,
        "01_users",
        str(users_file),
        owner_emails=["targeted@example.com"],
    ) == 2
    progress = run_progress_by_step(postgres_cluster, run_id)
    assert progress["01_users"] == {"queued": 2}
    assert progress["02_folders"] == {}


def test_resume_run_requeues_failed_shards_for_workers(
    postgres_cluster,
    tmp_path,
    monkeypatch,
):
    run_id = str(uuid.uuid4())
    _seed_step(postgres_cluster, run_id, "01_users", "user_db")
    users_file = tmp_path / "01_users_20260101.sql"
    users_file.write_text("-- primary shard\n", encoding="utf-8")
    _write_manifest(tmp_path, users_file, shard_count=1)

    enqueue_run(postgres_cluster, run_id, {"01_users_": str(users_file)})
    status = "retrying"
    while status == "retrying":
        claimed = claim_shard(postgres_cluster, "user_db", worker_id="w1")
        assert claimed is not None
        status = fail_shard(postgres_cluster, "user_db", claimed.id, "w1", "boom")
    assert status == "failed"

    monkeypatch.setattr(
        shard_queue,
        "ensure_tracking_schema",
        lambda *_args, **_kwargs: pytest.fail(
            "status reads and resume must not execute tracking DDL"
        ),
    )
    progress = run_progress_by_step(postgres_cluster, run_id)
    assert has_actionable_failures(progress) is True
    details = failed_shard_details(postgres_cluster, run_id)
    assert len(details) == 1
    assert details[0]["error_message"] == "boom"
    assert details[0]["step_key"] == "01_users"

    resumed = resume_run(postgres_cluster, run_id)
    assert resumed == 1
    progress_after = run_progress_by_step(postgres_cluster, run_id)
    assert has_actionable_failures(progress_after) is False
    assert progress_after["01_users"] == {"queued": 1}
