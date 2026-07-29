"""Proof that the durable shard queue claims atomically, respects step and
shard-order dependencies, and recovers from stale/failed workers."""
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import pytest

from utils import shard_queue
from utils.db import ConnectionConfig
from utils.migration_diagnostic_store import list_diagnostic_events
from utils.migration_tracking import config_for_database, ensure_tracking_schema
from utils.shard_queue import (
    cancel_run_shards,
    claim_shard,
    complete_shard,
    enqueue_shards,
    fail_shard,
    get_shard_progress,
    heartbeat_shard,
    recover_stale_leases,
    resume_run_shards,
    step_shard_summary,
)


def _connect(base: ConnectionConfig, database: str):
    return psycopg2.connect(
        host=base.host, port=base.port, dbname=database, user=base.username
    )


@pytest.fixture(autouse=True)
def _isolated_shard_tables(postgres_cluster):
    """Each test uses a fresh run_id, but claim_shard scans an entire target
    database's queue by design (a worker doesn't know run ids in advance).
    Truncate between tests so one test's leftover queued shards can never be
    claimed by another test running against the shared session cluster."""
    for database in ("user_db", "document_db", "completion_db"):
        ensure_tracking_schema(
            config_for_database(postgres_cluster, database),
            coordinator=database == "user_db",
        )
        conn = _connect(postgres_cluster, database)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT to_regclass('migration.migration_shards')"
                )
                if cursor.fetchone()[0] is not None:
                    cursor.execute("TRUNCATE TABLE migration.migration_shards")
        finally:
            conn.close()
    yield


def _manifest(count: int, prefix: str = "shard"):
    return {
        "shards": [
            {
                "shard_index": index,
                "file_path": f"/tmp/{prefix}-{index}.sql",
                "expected_rows": 10,
                "byte_size": 1000,
                "checksum": f"checksum-{index}",
            }
            for index in range(1, count + 1)
        ]
    }


def _seed_step_row(base, database, run_id, step_key, status="pending"):
    config = config_for_database(base, database)
    ensure_tracking_schema(config, coordinator=database == "user_db")
    conn = _connect(base, database)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO migration.migration_runs (id, status, total_users)
                VALUES (%s::uuid, 'running', 1)
                ON CONFLICT (id) DO NOTHING
                """,
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


def _set_step_status(base, database, run_id, step_key, status):
    conn = _connect(base, database)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE migration.migration_steps SET status = %s
                WHERE migration_run_id = %s::uuid AND step_key = %s
                """,
                (status, run_id, step_key),
            )
    finally:
        conn.close()


def test_two_workers_never_claim_the_same_shard(postgres_cluster):
    run_id = str(uuid.uuid4())
    _seed_step_row(postgres_cluster, "user_db", run_id, "01_users", status="running")
    enqueue_shards(postgres_cluster, run_id, "01_users", _manifest(5))

    claimed_ids = set()
    for _ in range(5):
        claimed = claim_shard(postgres_cluster, "user_db", worker_id="worker-a")
        assert claimed is not None
        assert claimed.id not in claimed_ids
        claimed_ids.add(claimed.id)
        # The last shard's epilogue requires siblings completed first, so
        # complete each shard immediately to unblock the next claim.
        complete_shard(
            postgres_cluster,
            "user_db",
            claimed.id,
            "worker-a",
            driver_rowcount=10,
        )

    assert claim_shard(postgres_cluster, "user_db", worker_id="worker-b") is None


def test_true_concurrent_workers_never_double_claim_a_shard(postgres_cluster):
    """Race many real threads (each with its own DB connection) against the
    same queue simultaneously, proving ``FOR UPDATE SKIP LOCKED`` -- not just
    sequential test ordering -- is what prevents duplicate claims."""
    run_id = str(uuid.uuid4())
    shard_count = 16
    _seed_step_row(postgres_cluster, "user_db", run_id, "01_users", status="running")
    enqueue_shards(postgres_cluster, run_id, "01_users", _manifest(shard_count))

    barrier = threading.Barrier(shard_count * 2)
    results = []
    results_lock = threading.Lock()

    def worker(worker_index):
        barrier.wait()  # maximize actual overlap between threads
        # Real workers poll-loop rather than giving up after one attempt; a
        # single ``SKIP LOCKED`` pass can transiently see zero candidates
        # under heavy contention even though shards remain available, so
        # retry briefly like ``worker.py`` does.
        claimed = None
        for _ in range(25):
            claimed = claim_shard(postgres_cluster, "user_db", worker_id=f"race-worker-{worker_index}")
            if claimed is not None:
                break
        with results_lock:
            results.append(claimed)

    with ThreadPoolExecutor(max_workers=shard_count * 2) as pool:
        list(pool.map(worker, range(shard_count * 2)))

    successful = [c for c in results if c is not None]
    claimed_ids = [c.id for c in successful]
    assert len(claimed_ids) == len(set(claimed_ids)), "a shard was claimed by more than one worker"
    # Non-sequential step: only the highest-indexed (epilogue) shard is
    # gated behind its siblings completing, so exactly shard_count - 1 of
    # the shards are claimable up front.
    assert len(successful) == shard_count - 1


def test_dependency_checks_do_not_hold_candidate_row_locks(
    postgres_cluster,
    monkeypatch,
):
    run_id = str(uuid.uuid4())
    _seed_step_row(
        postgres_cluster,
        "user_db",
        run_id,
        "01_users",
        status="running",
    )
    enqueue_shards(postgres_cluster, run_id, "01_users", _manifest(3))

    dependency_check_started = threading.Event()
    release_dependency_check = threading.Event()
    result = {}

    def blocking_dependency_check(*_args, **_kwargs):
        dependency_check_started.set()
        assert release_dependency_check.wait(timeout=5)
        return True

    monkeypatch.setattr(
        shard_queue,
        "_prior_step_satisfied",
        blocking_dependency_check,
    )

    def claim_in_background():
        try:
            result["claimed"] = claim_shard(
                postgres_cluster,
                "user_db",
                worker_id="waiting-worker",
            )
        except Exception as exc:  # pragma: no cover - asserted below
            result["error"] = exc

    thread = threading.Thread(target=claim_in_background)
    thread.start()
    assert dependency_check_started.wait(timeout=5)

    # A dependency check may wait on another database, but its candidate scan
    # must not lock every queued shard while it does so.
    conn = _connect(postgres_cluster, "user_db")
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("SET lock_timeout = '500ms'")
            cursor.execute(
                """
                UPDATE migration.migration_shards
                SET error_message = 'lock probe'
                WHERE id = (
                    SELECT id
                    FROM migration.migration_shards
                    WHERE migration_run_id = %s::uuid
                    ORDER BY shard_index
                    LIMIT 1
                )
                """,
                (run_id,),
            )
            assert cursor.rowcount == 1
    finally:
        conn.close()
        release_dependency_check.set()
        thread.join(timeout=5)

    assert "error" not in result
    assert result.get("claimed") is not None


def test_dependent_step_blocked_until_prior_step_completes(postgres_cluster):
    run_id = str(uuid.uuid4())
    _seed_step_row(postgres_cluster, "user_db", run_id, "01_users", status="running")
    _seed_step_row(postgres_cluster, "document_db", run_id, "02_folders", status="running")
    enqueue_shards(postgres_cluster, run_id, "02_folders", _manifest(2))

    assert claim_shard(postgres_cluster, "document_db", worker_id="worker-a") is None

    _set_step_status(postgres_cluster, "user_db", run_id, "01_users", "completed")

    claimed = claim_shard(postgres_cluster, "document_db", worker_id="worker-a")
    assert claimed is not None
    assert claimed.step_key == "02_folders"
    assert claimed.shard_index == 1


def test_sequential_step_enforces_shard_index_order(postgres_cluster):
    run_id = str(uuid.uuid4())
    _seed_step_row(postgres_cluster, "user_db", run_id, "01_users", status="completed")
    _seed_step_row(postgres_cluster, "document_db", run_id, "02_folders", status="running")
    enqueue_shards(postgres_cluster, run_id, "02_folders", _manifest(3))

    first = claim_shard(postgres_cluster, "document_db", worker_id="worker-a")
    assert first.shard_index == 1

    # Shard 2 must not be claimable while shard 1 is still running.
    assert claim_shard(postgres_cluster, "document_db", worker_id="worker-b") is None

    assert complete_shard(
        postgres_cluster,
        "document_db",
        first.id,
        "worker-a",
        driver_rowcount=10,
    )

    second = claim_shard(postgres_cluster, "document_db", worker_id="worker-b")
    assert second.shard_index == 2


def test_final_shard_epilogue_waits_for_sibling_shards_even_when_unordered(postgres_cluster):
    """The last shard of a non-sequential step carries the one-time epilogue
    (validation/bookkeeping SQL) which assumes every other shard already
    committed, so it must not be claimable until its siblings are done even
    though shards of this step may otherwise run in any order."""
    run_id = str(uuid.uuid4())
    _seed_step_row(postgres_cluster, "user_db", run_id, "01_users", status="running")
    enqueue_shards(postgres_cluster, run_id, "01_users", _manifest(3))

    first = claim_shard(postgres_cluster, "user_db", worker_id="worker-a")
    second = claim_shard(postgres_cluster, "user_db", worker_id="worker-b")
    assert {first.shard_index, second.shard_index} == {1, 2}

    # Shard 3 (the last / epilogue-bearing shard) must not be claimable yet.
    assert claim_shard(postgres_cluster, "user_db", worker_id="worker-c") is None

    complete_shard(
        postgres_cluster,
        "user_db",
        first.id,
        "worker-a",
        driver_rowcount=10,
    )
    assert claim_shard(postgres_cluster, "user_db", worker_id="worker-c") is None

    complete_shard(
        postgres_cluster,
        "user_db",
        second.id,
        "worker-b",
        driver_rowcount=10,
    )
    third = claim_shard(postgres_cluster, "user_db", worker_id="worker-c")
    assert third is not None
    assert third.shard_index == 3


def test_independent_step_shards_run_concurrently(postgres_cluster):
    run_id = str(uuid.uuid4())
    _seed_step_row(postgres_cluster, "user_db", run_id, "01_users", status="running")
    enqueue_shards(postgres_cluster, run_id, "01_users", _manifest(3))

    first = claim_shard(postgres_cluster, "user_db", worker_id="worker-a")
    second = claim_shard(postgres_cluster, "user_db", worker_id="worker-b")
    assert first is not None and second is not None
    assert first.id != second.id


def test_fail_shard_retries_then_terminally_fails(postgres_cluster):
    run_id = str(uuid.uuid4())
    _seed_step_row(postgres_cluster, "user_db", run_id, "01_users", status="running")
    enqueue_shards(postgres_cluster, run_id, "01_users", _manifest(1))

    claimed = claim_shard(postgres_cluster, "user_db", worker_id="worker-a")
    assert claimed.attempts == 1
    status = fail_shard(postgres_cluster, "user_db", claimed.id, "worker-a", "boom")
    assert status == "retrying"

    # Retry: claimable again, attempts increments toward max_attempts (3).
    claimed2 = claim_shard(postgres_cluster, "user_db", worker_id="worker-b")
    assert claimed2.id == claimed.id
    assert claimed2.attempts == 2
    status2 = fail_shard(postgres_cluster, "user_db", claimed2.id, "worker-b", "boom again")
    assert status2 == "retrying"

    claimed3 = claim_shard(postgres_cluster, "user_db", worker_id="worker-c")
    assert claimed3.attempts == 3
    status3 = fail_shard(postgres_cluster, "user_db", claimed3.id, "worker-c", "final boom")
    assert status3 == "failed"

    # A terminally-failed shard must never be claimable again.
    assert claim_shard(postgres_cluster, "user_db", worker_id="worker-d") is None
    summary = step_shard_summary(postgres_cluster, run_id, "01_users")
    assert summary["any_failed"] is True
    assert summary["all_completed"] is False


def test_stale_lease_is_recovered_for_retry(postgres_cluster):
    run_id = str(uuid.uuid4())
    _seed_step_row(postgres_cluster, "user_db", run_id, "01_users", status="running")
    enqueue_shards(postgres_cluster, run_id, "01_users", _manifest(1))

    claimed = claim_shard(postgres_cluster, "user_db", worker_id="dead-worker", lease_seconds=0)
    assert claimed is not None

    # Lease already expired (0-second lease); a live worker should heartbeat successfully
    # only while it still owns the shard, and recovery should reclaim it once expired.
    recovered = recover_stale_leases(postgres_cluster, "user_db")
    assert recovered == 1
    events = list_diagnostic_events(
        config_for_database(postgres_cluster, "user_db"),
        run_id,
    )
    assert len(events) == 1
    assert events[0]["code"] == "SHARD_STALE_LEASE"
    assert events[0]["severity"] == "warning"

    claimed_again = claim_shard(postgres_cluster, "user_db", worker_id="worker-b")
    assert claimed_again is not None
    assert claimed_again.id == claimed.id
    assert claimed_again.attempts == 2

    # The dead worker's heartbeat/complete calls must no longer succeed.
    assert heartbeat_shard(postgres_cluster, "user_db", claimed.id, "dead-worker") is False
    assert complete_shard(postgres_cluster, "user_db", claimed.id, "dead-worker", 5) is False


def test_cancel_and_resume_run_shards(postgres_cluster):
    run_id = str(uuid.uuid4())
    _seed_step_row(postgres_cluster, "user_db", run_id, "01_users", status="running")
    enqueue_shards(postgres_cluster, run_id, "01_users", _manifest(2))

    cancelled = cancel_run_shards(postgres_cluster, run_id)
    assert cancelled == 2
    assert claim_shard(postgres_cluster, "user_db", worker_id="worker-a") is None

    resumed = resume_run_shards(postgres_cluster, run_id)
    assert resumed == 2
    assert claim_shard(postgres_cluster, "user_db", worker_id="worker-a") is not None


def test_owner_emails_are_persisted_for_attribution(postgres_cluster):
    run_id = str(uuid.uuid4())
    _seed_step_row(postgres_cluster, "user_db", run_id, "01_users", status="running")
    enqueue_shards(
        postgres_cluster, run_id, "01_users", _manifest(1), owner_emails=["a@x.com", "b@x.com"]
    )
    claimed = claim_shard(postgres_cluster, "user_db", worker_id="worker-a")
    assert sorted(claimed.owner_emails) == ["a@x.com", "b@x.com"]


def test_manifest_legacy_owners_resolve_to_per_shard_emails(postgres_cluster):
    run_id = str(uuid.uuid4())
    _seed_step_row(postgres_cluster, "user_db", run_id, "01_users", status="running")
    conn = _connect(postgres_cluster, "user_db")
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO migration.migration_batches (id, status, total_users)
                VALUES (%s::uuid, 'running', 2)
                """,
                (run_id,),
            )
            for index, (legacy_id, email) in enumerate(
                (("legacy-a", "a@x.com"), ("legacy-b", "b@x.com")), 1
            ):
                cursor.execute(
                    """
                    INSERT INTO migration.migration_user_results
                        (batch_id, email, legacy_user_id, v5_user_id)
                    VALUES (%s::uuid, %s, %s, %s::uuid)
                    """,
                    (
                        run_id,
                        email,
                        legacy_id,
                        f"30000000-0000-4000-8000-00000000000{index}",
                    ),
                )
    finally:
        conn.close()

    manifest = _manifest(2)
    manifest["shards"][0]["owner_legacy_ids"] = ["legacy-a"]
    manifest["shards"][1]["owner_legacy_ids"] = ["legacy-b"]
    manifest["shards"][0]["owner_scope_complete"] = True
    manifest["shards"][1]["owner_scope_complete"] = True
    enqueue_shards(postgres_cluster, run_id, "01_users", manifest)

    conn = _connect(postgres_cluster, "user_db")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT shard_index, owner_emails
                FROM migration.migration_shards
                WHERE migration_run_id = %s::uuid
                ORDER BY shard_index
                """,
                (run_id,),
            )
            assert cursor.fetchall() == [
                (1, ["a@x.com"]),
                (2, ["b@x.com"]),
            ]
    finally:
        conn.close()


def test_get_shard_progress_reports_counts(postgres_cluster):
    run_id = str(uuid.uuid4())
    _seed_step_row(postgres_cluster, "user_db", run_id, "01_users", status="running")
    enqueue_shards(postgres_cluster, run_id, "01_users", _manifest(2))
    claimed = claim_shard(postgres_cluster, "user_db", worker_id="worker-a")
    complete_shard(
        postgres_cluster,
        "user_db",
        claimed.id,
        "worker-a",
        driver_rowcount=10,
    )

    progress = get_shard_progress(postgres_cluster, run_id)
    by_status = {row["status"]: row["shard_count"] for row in progress if row["step_key"] == "01_users"}
    assert by_status.get("completed") == 1
    assert by_status.get("queued") == 1
    completed = next(
        row
        for row in progress
        if row["step_key"] == "01_users" and row["status"] == "completed"
    )
    assert completed["driver_rowcount_total"] == 10
    assert "affected_total" not in completed

    conn = _connect(postgres_cluster, "user_db")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT affected_count, driver_rowcount
                FROM migration.migration_shards
                WHERE id = %s::uuid
                """,
                (claimed.id,),
            )
            assert cursor.fetchone() == (None, 10)
    finally:
        conn.close()


def test_enqueue_is_idempotent(postgres_cluster):
    run_id = str(uuid.uuid4())
    _seed_step_row(postgres_cluster, "user_db", run_id, "01_users", status="running")
    enqueue_shards(postgres_cluster, run_id, "01_users", _manifest(2))
    enqueue_shards(postgres_cluster, run_id, "01_users", _manifest(2))

    progress = get_shard_progress(postgres_cluster, run_id)
    total = sum(row["shard_count"] for row in progress if row["step_key"] == "01_users")
    assert total == 2
