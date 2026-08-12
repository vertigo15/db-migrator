"""End-to-end proof for the shard worker: claim, execute, commit, and only
finalize (verify + close out) a step once every one of its shards lands.

Uses hand-written shard SQL against ``migration.id_mappings`` (rather than
the full production `users`/`documents` schema) so this stays a fast,
schema-light exercise of the worker's execution and finalization mechanics;
the SQL *generators* themselves are covered by ``test_migration_safety.py``
and ``test_dollar_quoting.py``.
"""
import hashlib
import os
import uuid

import psycopg2
import pytest

from utils.db import ConnectionConfig
from utils.migration_tracking import (
    config_for_database,
    create_distributed_run,
    ensure_tracking_schema,
)
from utils.shard_queue import (
    claim_shard,
    enqueue_shards,
    recover_stale_leases,
    resume_run_shards,
    step_shard_summary,
)
from utils.sql_generator import generate_migration_schema_setup
from utils.step_verification import verify_step
from utils.worker_runtime import (
    _prepare_shard_sql_for_worker,
    _read_verified_shard,
    ensure_worker_runtime_schema,
    execute_claimed_shard,
    reconcile_terminal_failures,
)


def _connect(base: ConnectionConfig, database: str):
    return psycopg2.connect(
        host=base.host, port=base.port, dbname=database, user=base.username
    )


@pytest.fixture(autouse=True)
def _clean_tables(postgres_cluster):
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
                cursor.execute("SELECT to_regclass('migration.id_mappings')")
                if cursor.fetchone()[0] is not None:
                    cursor.execute("TRUNCATE TABLE migration.id_mappings")
                cursor.execute("SELECT to_regclass('migration.migration_steps')")
                if cursor.fetchone()[0] is not None:
                    cursor.execute("TRUNCATE TABLE migration.migration_steps CASCADE")
                cursor.execute("SELECT to_regclass('migration.migration_runs')")
                if cursor.fetchone()[0] is not None:
                    cursor.execute("TRUNCATE TABLE migration.migration_runs CASCADE")
        finally:
            conn.close()
    yield


def _seed_run_and_step(base, run_id, step_key="01_users", expected_count=3, database="user_db"):
    config = config_for_database(base, database)
    ensure_tracking_schema(config, coordinator=database == "user_db")
    conn = _connect(base, database)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
            cursor.execute(generate_migration_schema_setup())
            cursor.execute(
                "INSERT INTO migration.migration_runs (id, status, total_users) "
                "VALUES (%s::uuid, 'running', 1)",
                (run_id,),
            )
            cursor.execute(
                """
                INSERT INTO migration.migration_steps
                    (migration_run_id, step_key, target_database, status, expected_count)
                VALUES (%s::uuid, %s, %s, 'running', %s)
                """,
                (run_id, step_key, database, expected_count),
            )
    finally:
        conn.close()


def _write_shard(tmp_path, shard_index, run_id, row_old_ids, step_key="01_users"):
    preamble = generate_migration_schema_setup() + f"\n-- shard for run {run_id}\n"
    body_lines = []
    for old_id in row_old_ids:
        body_lines.append(
            f"""INSERT INTO migration.id_mappings
                (table_name, old_id, new_id, migration_batch, migration_run_id, record_action)
            VALUES ('users', {_sql_str(old_id)}, gen_random_uuid(), {_sql_str(run_id)},
                    '{run_id}'::uuid, 'created')
            ON CONFLICT (table_name, old_id) DO NOTHING;"""
        )
    content = preamble + "\n".join(body_lines) + "\n"
    path = tmp_path / f"{step_key}_shard{shard_index}.sql"
    path.write_text(content, encoding="utf-8")
    return str(path), hashlib.sha256(content.encode("utf-8")).hexdigest(), len(content.encode("utf-8"))


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def test_worker_strips_bootstrap_ddl_after_checksum_verification():
    content = (
        'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";\n'
        + generate_migration_schema_setup()
        + "\nINSERT INTO public.example VALUES (1);\n"
    )

    prepared = _prepare_shard_sql_for_worker(content)

    assert "CREATE EXTENSION" not in prepared
    assert "CREATE TABLE IF NOT EXISTS migration.id_mappings" not in prepared
    assert "ALTER TABLE migration.migration_steps" not in prepared
    assert "INSERT INTO public.example VALUES (1);" in prepared


def test_worker_strips_legacy_mapping_setup_by_stable_markers():
    legacy_setup = """-- ============================================================
-- MIGRATION MAPPING TABLE SETUP (idempotent)
-- ============================================================
CREATE TABLE migration.legacy_bootstrap (id integer);
-- ============================================================
-- MIGRATION MAPPING TABLE SETUP COMPLETE
-- ============================================================
"""
    prepared = _prepare_shard_sql_for_worker(
        legacy_setup + "SELECT 1;\n"
    )

    assert "legacy_bootstrap" not in prepared
    assert "SELECT 1;" in prepared


def test_worker_waits_for_bind_mount_to_reach_expected_checksum(
    monkeypatch,
    tmp_path,
):
    path = tmp_path / "synchronizing_shard.sql"
    path.write_text("stale contents\n", encoding="utf-8")
    expected_content = "SELECT 1;\n"
    expected_checksum = hashlib.sha256(
        expected_content.encode("utf-8")
    ).hexdigest()
    sleep_calls = []

    def synchronize_file(delay):
        sleep_calls.append(delay)
        path.write_text(expected_content, encoding="utf-8")

    monkeypatch.setattr(
        "utils.worker_runtime.time.sleep",
        synchronize_file,
    )

    content, error = _read_verified_shard(
        str(path),
        expected_checksum,
        attempts=3,
        retry_delay=0.01,
    )

    assert error is None
    assert content == expected_content
    assert sleep_calls == [0.01]


def test_worker_checksum_preserves_crlf_payload_bytes(tmp_path):
    path = tmp_path / "crlf_shard.sql"
    expected_bytes = b"SELECT 'first line\\r\\nsecond line';\n"
    path.write_bytes(expected_bytes)

    content, error = _read_verified_shard(
        str(path),
        hashlib.sha256(expected_bytes).hexdigest(),
        attempts=1,
    )

    assert error is None
    assert content is not None
    assert content.encode("utf-8") == expected_bytes


def test_worker_runtime_schema_bootstrap_is_idempotent(postgres_cluster):
    config = config_for_database(postgres_cluster, "document_db")

    ensure_worker_runtime_schema(config)
    ensure_worker_runtime_schema(config)

    conn = _connect(postgres_cluster, "document_db")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM migration.runtime_schema_versions
                WHERE version = 'worker-runtime-v3'
                """
            )
            assert cursor.fetchone()[0] == 1
            cursor.execute(
                "SELECT to_regprocedure("
                "'migration.deterministic_uuid_v4(uuid,text)')"
            )
            assert cursor.fetchone()[0] is not None
    finally:
        conn.close()


def test_conversation_verification_accepts_canonical_reused_mapping(
    postgres_cluster,
):
    run_id = str(uuid.uuid4())
    old_run_id = str(uuid.uuid4())
    conversation_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())
    _seed_run_and_step(
        postgres_cluster,
        run_id,
        step_key="05_conversations",
        expected_count=1,
        database="completion_db",
    )

    conn = _connect(postgres_cluster, "completion_db")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS public.conversations (
                    id uuid PRIMARY KEY,
                    user_id uuid NOT NULL,
                    payload jsonb NOT NULL
                )
                """
            )
            cursor.execute(
                """
                INSERT INTO migration.migration_runs (id, status, total_users)
                VALUES (%s::uuid, 'completed', 1)
                """,
                (old_run_id,),
            )
            cursor.execute(
                """
                INSERT INTO public.conversations (id, user_id, payload)
                VALUES (%s::uuid, %s::uuid, '{}'::jsonb)
                """,
                (conversation_id, user_id),
            )
            cursor.execute(
                """
                INSERT INTO migration.id_mappings
                    (table_name, old_id, new_id, migration_run_id, record_action)
                VALUES (
                    'conversations', %s, %s::uuid, %s::uuid, 'created'
                )
                """,
                (conversation_id, conversation_id, old_run_id),
            )
            cursor.execute(
                """
                INSERT INTO migration.migration_step_entities
                    (migration_run_id, step_key, table_name, old_id, new_id,
                     record_action)
                VALUES (
                    %s::uuid, '05_conversations', 'conversations',
                    %s, %s::uuid, 'reused'
                )
                """,
                (run_id, conversation_id, conversation_id),
            )

        with conn.cursor() as cursor:
            affected_count, details = verify_step(
                cursor, "05_conversations", run_id
            )

        assert affected_count == 1
        assert details["created_entities"] == 0
        assert details["reused_entities"] == 1
        assert details["invalid_entities"] == 0
    finally:
        # Keep the session-scoped integration database isolated. This also
        # removes public.conversations when this test had to create it.
        conn.rollback()
        conn.close()


def _manifest_from_shards(shard_specs):
    return {
        "shards": [
            {
                "shard_index": index + 1,
                "file_path": file_path,
                "expected_rows": len(ids),
                "byte_size": byte_size,
                "checksum": checksum,
            }
            for index, (file_path, checksum, byte_size, ids) in enumerate(shard_specs)
        ]
    }


def _mapping_count(base, run_id, database="user_db"):
    conn = _connect(base, database)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM migration.id_mappings WHERE migration_run_id = %s::uuid",
                (run_id,),
            )
            return cursor.fetchone()[0]
    finally:
        conn.close()


def _step_status(base, run_id, step_key, database="user_db"):
    conn = _connect(base, database)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT status, affected_count FROM migration.migration_steps "
                "WHERE migration_run_id = %s::uuid AND step_key = %s",
                (run_id, step_key),
            )
            return cursor.fetchone()
    finally:
        conn.close()


def test_worker_completes_and_verifies_multi_shard_step(postgres_cluster, tmp_path):
    run_id = str(uuid.uuid4())
    _seed_run_and_step(postgres_cluster, run_id, expected_count=3)

    shard_specs = [
        (*_write_shard(tmp_path, 1, run_id, ["u1"]), ["u1"]),
        (*_write_shard(tmp_path, 2, run_id, ["u2"]), ["u2"]),
        (*_write_shard(tmp_path, 3, run_id, ["u3"]), ["u3"]),
    ]
    enqueue_shards(postgres_cluster, run_id, "01_users", _manifest_from_shards(shard_specs))

    outcomes = []
    finalized_flags = []
    for _ in range(3):
        claimed = claim_shard(postgres_cluster, "user_db", worker_id="worker-a")
        assert claimed is not None
        outcome = execute_claimed_shard(postgres_cluster, claimed, "worker-a")
        assert outcome.success, outcome.error
        outcomes.append(outcome)
        finalized_flags.append(outcome.step_finalized)

    # Only the last shard (once its siblings are all done) triggers finalization.
    assert finalized_flags == [None, None, True]
    assert all(outcome.driver_rowcount == 1 for outcome in outcomes)
    assert _mapping_count(postgres_cluster, run_id) == 3
    status, affected = _step_status(postgres_cluster, run_id, "01_users")
    assert status == "completed"
    assert affected == 3

    conn = _connect(postgres_cluster, "user_db")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT affected_count, driver_rowcount
                FROM migration.migration_shards
                WHERE migration_run_id = %s::uuid
                ORDER BY shard_index
                """,
                (run_id,),
            )
            assert cursor.fetchall() == [(None, 1), (None, 1), (None, 1)]
    finally:
        conn.close()

    assert claim_shard(postgres_cluster, "user_db", worker_id="worker-b") is None


def test_worker_marks_step_failed_when_verification_count_mismatches(postgres_cluster, tmp_path):
    run_id = str(uuid.uuid4())
    # expected_count deliberately wrong (extraction claimed 5, only 2 will be inserted).
    _seed_run_and_step(postgres_cluster, run_id, expected_count=5)

    shard_specs = [
        (*_write_shard(tmp_path, 1, run_id, ["u1"]), ["u1"]),
        (*_write_shard(tmp_path, 2, run_id, ["u2"]), ["u2"]),
    ]
    enqueue_shards(postgres_cluster, run_id, "01_users", _manifest_from_shards(shard_specs))

    outcomes = []
    for _ in range(2):
        claimed = claim_shard(postgres_cluster, "user_db", worker_id="worker-a")
        outcomes.append(execute_claimed_shard(postgres_cluster, claimed, "worker-a"))

    assert all(o.success for o in outcomes)
    assert outcomes[-1].step_finalized is False
    status, _ = _step_status(postgres_cluster, run_id, "01_users")
    assert status == "failed"


def test_worker_rejects_shard_with_bad_checksum(
    monkeypatch,
    postgres_cluster,
    tmp_path,
):
    monkeypatch.setattr("utils.worker_runtime.time.sleep", lambda _delay: None)
    run_id = str(uuid.uuid4())
    _seed_run_and_step(postgres_cluster, run_id, expected_count=1)
    file_path, _real_checksum, byte_size = _write_shard(tmp_path, 1, run_id, ["u1"])
    manifest = {
        "shards": [
            {
                "shard_index": 1,
                "file_path": file_path,
                "expected_rows": 1,
                "byte_size": byte_size,
                "checksum": "0" * 64,
            }
        ]
    }
    enqueue_shards(postgres_cluster, run_id, "01_users", manifest)

    claimed = claim_shard(postgres_cluster, "user_db", worker_id="worker-a")
    outcome = execute_claimed_shard(postgres_cluster, claimed, "worker-a")
    assert outcome.success is False
    assert "checksum" in outcome.error.lower()
    assert _mapping_count(postgres_cluster, run_id) == 0

    # A persistent checksum mismatch cannot heal by rerunning the same shard.
    reclaimed = claim_shard(postgres_cluster, "user_db", worker_id="worker-b")
    assert reclaimed is None
    summary = step_shard_summary(postgres_cluster, run_id, "01_users")
    assert summary["failed"] == 1


def test_enqueue_rejects_manifest_belonging_to_a_different_run(
    postgres_cluster,
    tmp_path,
):
    run_id = str(uuid.uuid4())
    other_run_id = str(uuid.uuid4())
    _seed_run_and_step(postgres_cluster, run_id, expected_count=1)
    file_path, checksum, byte_size = _write_shard(tmp_path, 1, other_run_id, ["u1"])
    manifest = {
        "migration_run_id": other_run_id,
        "step_key": "01_users",
        "target_database": "user_db",
        "shards": [
            {"shard_index": 1, "file_path": file_path, "expected_rows": 1,
             "byte_size": byte_size, "checksum": checksum}
        ]
    }

    with pytest.raises(ValueError, match="belongs to migration run"):
        enqueue_shards(postgres_cluster, run_id, "01_users", manifest)


def test_worker_accepts_valid_checksummed_shard_without_run_id_in_sql(
    postgres_cluster,
    tmp_path,
):
    run_id = str(uuid.uuid4())
    _seed_run_and_step(postgres_cluster, run_id, expected_count=0)
    content = "SELECT 1;\n"
    path = tmp_path / "run_agnostic_shard.sql"
    path.write_text(content, encoding="utf-8")
    sibling_path = tmp_path / "run_agnostic_shard_2.sql"
    sibling_path.write_text(content, encoding="utf-8")
    checksum = hashlib.sha256(content.encode("utf-8")).hexdigest()
    manifest = {
        "migration_run_id": run_id,
        "step_key": "01_users",
        "target_database": "user_db",
        "shards": [
            {
                "shard_index": 1,
                "file_path": str(path),
                "expected_rows": 0,
                "byte_size": len(content.encode("utf-8")),
                "checksum": checksum,
            },
            {
                "shard_index": 2,
                "file_path": str(sibling_path),
                "expected_rows": 0,
                "byte_size": len(content.encode("utf-8")),
                "checksum": checksum,
            },
        ],
    }
    enqueue_shards(postgres_cluster, run_id, "01_users", manifest)

    claimed = claim_shard(postgres_cluster, "user_db", worker_id="worker-a")
    outcome = execute_claimed_shard(postgres_cluster, claimed, "worker-a")

    assert outcome.success is True
    assert outcome.error is None


def test_worker_fails_schema_error_without_repeated_attempts(
    postgres_cluster, tmp_path
):
    run_id = str(uuid.uuid4())
    _seed_run_and_step(postgres_cluster, run_id, expected_count=1)
    preamble = generate_migration_schema_setup()
    # Deliberately broken SQL (references a nonexistent table).
    content = preamble + f"\n-- {run_id}\nINSERT INTO migration.no_such_table VALUES (1);\n"
    path = tmp_path / "broken_shard.sql"
    path.write_text(content, encoding="utf-8")
    checksum = hashlib.sha256(content.encode("utf-8")).hexdigest()
    manifest = {
        "shards": [
            {"shard_index": 1, "file_path": str(path), "expected_rows": 1,
             "byte_size": len(content.encode("utf-8")), "checksum": checksum}
        ]
    }
    enqueue_shards(postgres_cluster, run_id, "01_users", manifest)

    claimed = claim_shard(postgres_cluster, "user_db", worker_id="worker-a")
    outcome = execute_claimed_shard(postgres_cluster, claimed, "worker-a")
    assert outcome.success is False
    assert outcome.error

    summary = step_shard_summary(postgres_cluster, run_id, "01_users")
    assert summary["total"] == 1
    assert summary["completed"] == 0
    assert summary["failed"] == 1

    reclaimed = claim_shard(postgres_cluster, "user_db", worker_id="worker-b")
    assert reclaimed is None


def test_terminal_failure_updates_step_and_only_affected_user(
    postgres_cluster, tmp_path
):
    run_id = str(uuid.uuid4())
    _seed_run_and_step(postgres_cluster, run_id, expected_count=1)
    conn = _connect(postgres_cluster, "user_db")
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO migration.migration_batches
                    (id, status, total_users)
                VALUES (%s::uuid, 'running', 2)
                """,
                (run_id,),
            )
            for index, email in enumerate(
                ("affected@example.com", "unaffected@example.com"), 1
            ):
                cursor.execute(
                    """
                    INSERT INTO migration.migration_user_results
                        (batch_id, email, legacy_user_id, v5_user_id, result)
                    VALUES (%s::uuid, %s, %s, %s::uuid, 'pending')
                    """,
                    (
                        run_id,
                        email,
                        f"legacy-{index}",
                        f"10000000-0000-4000-8000-00000000000{index}",
                    ),
                )
    finally:
        conn.close()

    content = (
        generate_migration_schema_setup()
        + f"\n-- {run_id}\nINSERT INTO migration.no_such_table VALUES (1);\n"
    )
    path = tmp_path / "terminal_failure.sql"
    path.write_text(content, encoding="utf-8")
    enqueue_shards(
        postgres_cluster,
        run_id,
        "01_users",
        {
            "shards": [{
                "shard_index": 1,
                "file_path": str(path),
                "expected_rows": 1,
                "byte_size": len(content.encode("utf-8")),
                "checksum": hashlib.sha256(content.encode("utf-8")).hexdigest(),
            }]
        },
        owner_emails=["affected@example.com"],
    )
    conn = _connect(postgres_cluster, "user_db")
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE migration.migration_shards
                SET max_attempts = 1
                WHERE migration_run_id = %s::uuid
                """,
                (run_id,),
            )
    finally:
        conn.close()

    claimed = claim_shard(postgres_cluster, "user_db", "terminal-worker")
    outcome = execute_claimed_shard(
        postgres_cluster, claimed, "terminal-worker"
    )
    assert outcome.success is False
    assert _step_status(postgres_cluster, run_id, "01_users")[0] == "failed"

    conn = _connect(postgres_cluster, "user_db")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT email, result, failed_step
                FROM migration.migration_user_results
                WHERE batch_id = %s::uuid
                ORDER BY email
                """,
                (run_id,),
            )
            assert cursor.fetchall() == [
                ("affected@example.com", "failed", "01_users"),
                ("unaffected@example.com", "pending", None),
            ]
            cursor.execute(
                "SELECT status FROM migration.migration_batches WHERE id = %s::uuid",
                (run_id,),
            )
            assert cursor.fetchone()[0] == "partial"
    finally:
        conn.close()

    assert resume_run_shards(postgres_cluster, run_id, "01_users") == 1
    assert _step_status(postgres_cluster, run_id, "01_users")[0] == "running"
    assert claim_shard(postgres_cluster, "user_db", "retry-worker") is not None


def test_last_completed_step_automatically_finalizes_batch(
    postgres_cluster, tmp_path
):
    run_id = str(uuid.uuid4())
    create_distributed_run(
        postgres_cluster,
        run_id,
        [{
            "email": "auto-finalize@example.com",
            "legacy_user_id": "legacy-auto",
            "v5_user_id": "20000000-0000-4000-8000-000000000001",
            "action": "created",
        }],
        {"database": "test"},
    )
    for database in ("user_db", "document_db", "completion_db"):
        conn = _connect(postgres_cluster, database)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
                cursor.execute(generate_migration_schema_setup())
                cursor.execute(
                    """
                    UPDATE migration.migration_steps
                    SET status = CASE
                            WHEN step_key = '01_users' THEN 'running'
                            ELSE 'skipped'
                        END,
                        expected_count = CASE
                            WHEN step_key = '01_users' THEN 1
                            ELSE 0
                        END
                    WHERE migration_run_id = %s::uuid
                    """,
                    (run_id,),
                )
        finally:
            conn.close()

    shard = (*_write_shard(tmp_path, 1, run_id, ["legacy-auto"]), ["legacy-auto"])
    enqueue_shards(
        postgres_cluster,
        run_id,
        "01_users",
        _manifest_from_shards([shard]),
        owner_emails=["auto-finalize@example.com"],
    )
    claimed = claim_shard(postgres_cluster, "user_db", "finalize-worker")
    outcome = execute_claimed_shard(
        postgres_cluster, claimed, "finalize-worker"
    )
    assert outcome.success is True
    assert outcome.step_finalized is True

    conn = _connect(postgres_cluster, "user_db")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT status FROM migration.migration_batches WHERE id = %s::uuid",
                (run_id,),
            )
            assert cursor.fetchone()[0] == "completed"
            cursor.execute(
                """
                SELECT result
                FROM migration.migration_user_results
                WHERE batch_id = %s::uuid
                """,
                (run_id,),
            )
            assert cursor.fetchone()[0] == "success"
    finally:
        conn.close()


def test_terminal_stale_lease_is_reconciled_into_step_failure(
    postgres_cluster, tmp_path
):
    run_id = str(uuid.uuid4())
    _seed_run_and_step(postgres_cluster, run_id, expected_count=1)
    shard = (*_write_shard(tmp_path, 1, run_id, ["stale"]), ["stale"])
    enqueue_shards(
        postgres_cluster,
        run_id,
        "01_users",
        _manifest_from_shards([shard]),
    )
    conn = _connect(postgres_cluster, "user_db")
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE migration.migration_shards
                SET max_attempts = 1
                WHERE migration_run_id = %s::uuid
                """,
                (run_id,),
            )
    finally:
        conn.close()

    claimed = claim_shard(
        postgres_cluster, "user_db", "dead-worker", lease_seconds=0
    )
    assert claimed is not None
    assert recover_stale_leases(postgres_cluster, "user_db") == 1
    assert reconcile_terminal_failures(postgres_cluster, "user_db") == 1
    assert _step_status(postgres_cluster, run_id, "01_users")[0] == "failed"
