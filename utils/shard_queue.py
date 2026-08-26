"""Durable PostgreSQL-backed shard queue.

Shards for a step live in that step's target database (mirroring
``migration.migration_steps`` placement), so cross-database dependencies
(e.g. ``02_folders`` in ``document_db`` depending on ``01_users`` in
``user_db``) are resolved by checking the prior step's summary row in its
own database rather than by a local join.

Concurrency model:
- Workers claim one shard at a time with ``FOR UPDATE SKIP LOCKED`` so two
  workers can never claim the same shard.
- A claimed shard gets a time-boxed lease; ``recover_stale_leases`` resets
  shards whose worker died mid-flight back to ``queued`` so another worker
  can retry them.
- ``02_folders`` (and any future step listed in ``SEQUENTIAL_SHARD_STEPS``)
  must execute its shards in ``shard_index`` order because later shards can
  depend on rows an earlier shard inserted; every other step's shards are
  independent and may run concurrently in any order.
- A step is only marked ``completed`` once every one of its shards commits;
  only then does the worker run the shared ``utils.step_verification``
  check and flip ``migration.migration_steps.status``.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import List, Mapping, Optional, Sequence

from utils.db import ConnectionConfig, get_connection
from utils.migration_diagnostic_store import insert_diagnostic_event
from utils.migration_diagnostics import classify_error, is_non_retryable_failure
from utils.migration_steps import (
    STEP_INDEX,
    STEP_TARGET_DB,
    SEQUENTIAL_SHARD_STEPS,
    normalize_step_key,
    prior_step,
)
from utils.migration_tracking import config_for_database, ensure_tracking_schema

DEFAULT_LEASE_SECONDS = 300
# Bound how many candidate rows a single claim attempt locks and inspects so
# one worker can never starve others by scanning the whole queue.
CLAIM_SCAN_LIMIT = 200


@dataclass
class ClaimedShard:
    id: str
    migration_run_id: str
    step_key: str
    target_database: str
    shard_index: int
    total_shards: int
    file_path: str
    expected_rows: int
    checksum: str
    attempts: int
    max_attempts: int
    owner_emails: List[str]


def enqueue_shards(
    base_config: ConnectionConfig,
    run_id: str,
    step_key: str,
    manifest: Mapping[str, object],
    owner_emails: Optional[Sequence[str]] = None,
) -> int:
    """Insert one queue row per shard in ``manifest``. Idempotent per run+step+index."""
    step_key = normalize_step_key(step_key)
    target_database = STEP_TARGET_DB[step_key]
    step_order = STEP_INDEX[step_key]

    manifest_run_id = manifest.get("migration_run_id")
    if manifest_run_id and str(manifest_run_id) != str(run_id):
        raise ValueError(
            f"Shard manifest belongs to migration run {manifest_run_id}, "
            f"not {run_id}"
        )
    manifest_step_key = manifest.get("step_key")
    if manifest_step_key and normalize_step_key(str(manifest_step_key)) != step_key:
        raise ValueError(
            f"Shard manifest belongs to step {manifest_step_key}, not {step_key}"
        )
    manifest_database = manifest.get("target_database")
    if manifest_database and str(manifest_database) != target_database:
        raise ValueError(
            f"Shard manifest targets {manifest_database}, not {target_database}"
        )

    config = config_for_database(base_config, target_database)
    ensure_tracking_schema(config, coordinator=target_database == "user_db")

    shards = manifest.get("shards", [])
    fallback_owner_emails = list(owner_emails or [])
    owner_ids = sorted(
        {
            str(owner_id)
            for shard in shards
            for owner_id in shard.get("owner_legacy_ids", [])
            if owner_id is not None and str(owner_id)
        }
    )
    emails_by_legacy_id = {}
    if owner_ids:
        coordinator = config_for_database(base_config, "user_db")
        ensure_tracking_schema(coordinator, coordinator=True)
        owner_conn = get_connection(coordinator)
        try:
            with owner_conn.cursor() as owner_cursor:
                owner_cursor.execute(
                    """
                    SELECT legacy_user_id, email
                    FROM migration.migration_user_results
                    WHERE batch_id = %s::uuid
                      AND legacy_user_id = ANY(%s::text[])
                    """,
                    (run_id, owner_ids),
                )
                emails_by_legacy_id = {
                    str(legacy_user_id): email
                    for legacy_user_id, email in owner_cursor.fetchall()
                    if legacy_user_id and email
                }
        finally:
            owner_conn.close()

    conn = get_connection(config)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            for shard in shards:
                shard_owner_ids = [
                    str(value)
                    for value in shard.get("owner_legacy_ids", [])
                    if value is not None and str(value)
                ]
                shard_owner_emails = sorted(
                    {
                        emails_by_legacy_id[owner_id]
                        for owner_id in shard_owner_ids
                        if owner_id in emails_by_legacy_id
                    }
                )
                if (
                    not shard.get("owner_scope_complete", False)
                    or not shard_owner_ids
                ):
                    # Backward compatibility for manifests generated before
                    # per-shard ownership metadata existed, and conservative
                    # attribution if even one unit lacked an owner.
                    shard_owner_emails = fallback_owner_emails
                cursor.execute(
                    """
                    INSERT INTO migration.migration_shards
                        (migration_run_id, step_key, step_order, target_database,
                         shard_index, total_shards, file_path, expected_rows,
                         byte_size, checksum, status, owner_emails)
                    VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            'queued', %s::jsonb)
                    ON CONFLICT (migration_run_id, step_key, shard_index) DO NOTHING
                    """,
                    (
                        run_id,
                        step_key,
                        step_order,
                        target_database,
                        shard["shard_index"],
                        len(shards),
                        shard["file_path"],
                        shard.get("expected_rows", 0),
                        shard.get("byte_size", 0),
                        shard.get("checksum", ""),
                        json.dumps(shard_owner_emails),
                    ),
                )
    finally:
        conn.close()
    return len(shards)


def _prior_step_satisfied(base_config: ConnectionConfig, run_id: str, step_key: str) -> bool:
    prior = prior_step(step_key)
    if prior is None:
        return True
    prior_database = STEP_TARGET_DB[prior]
    config = config_for_database(base_config, prior_database)
    conn = get_connection(config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT status FROM migration.migration_steps
                WHERE migration_run_id = %s::uuid AND step_key = %s
                """,
                (run_id, prior),
            )
            row = cursor.fetchone()
            return bool(row) and row[0] in ("completed", "skipped")
    finally:
        conn.close()


def claim_shard(
    base_config: ConnectionConfig,
    target_database: str,
    worker_id: str,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
) -> Optional[ClaimedShard]:
    """Atomically claim the next runnable shard in ``target_database``, or None."""
    config = config_for_database(base_config, target_database)
    conn = get_connection(config)
    # Every statement commits independently. In particular, the candidate
    # scan must not retain row/relation locks while dependency checks open
    # connections to other databases. Holding a broad FOR UPDATE scan here
    # used to create an application-level lock cycle with shard bootstrap DDL.
    conn.autocommit = True
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, migration_run_id, step_key, shard_index
                FROM migration.migration_shards
                WHERE target_database = %s AND status IN ('queued', 'retrying')
                  AND EXISTS (
                      SELECT 1
                      FROM migration.migration_steps ms
                      WHERE ms.migration_run_id =
                                migration.migration_shards.migration_run_id
                        AND ms.step_key = migration.migration_shards.step_key
                        AND ms.status IN ('pending', 'running')
                  )
                ORDER BY step_order, created_at
                LIMIT %s
                """,
                (target_database, CLAIM_SCAN_LIMIT),
            )
            candidates = cursor.fetchall()

            # Cache prior-step and same-step-ordering lookups within this
            # claim attempt; several candidates usually share a run/step.
            prior_ok_cache = {}
            earliest_incomplete_cache = {}
            sibling_status_cache = {}

            for shard_id, run_id, step_key, shard_index in candidates:
                prior_key = (run_id, step_key)
                if prior_key not in prior_ok_cache:
                    prior_ok_cache[prior_key] = _prior_step_satisfied(
                        base_config, run_id, step_key
                    )
                if not prior_ok_cache[prior_key]:
                    continue

                if step_key in SEQUENTIAL_SHARD_STEPS:
                    if prior_key not in earliest_incomplete_cache:
                        cursor.execute(
                            """
                            SELECT COALESCE(MIN(shard_index), 2147483647)
                            FROM migration.migration_shards
                            WHERE migration_run_id = %s::uuid AND step_key = %s
                              AND status <> 'completed'
                            """,
                            (run_id, step_key),
                        )
                        earliest_incomplete_cache[prior_key] = cursor.fetchone()[0]
                    if shard_index != earliest_incomplete_cache[prior_key]:
                        continue
                else:
                    # The highest-indexed shard of every step carries the
                    # step's one-time epilogue (validation blocks, batch
                    # bookkeeping) merged in by ShardWriter. That SQL assumes
                    # every other shard's rows already exist, so it must not
                    # run until its siblings are all committed, even though
                    # non-sequential steps otherwise allow any shard order.
                    if prior_key not in sibling_status_cache:
                        cursor.execute(
                            """
                            SELECT total_shards,
                                   COUNT(*) FILTER (WHERE status = 'completed'
                                                     AND shard_index < total_shards)
                            FROM migration.migration_shards
                            WHERE migration_run_id = %s::uuid AND step_key = %s
                            GROUP BY total_shards
                            """,
                            (run_id, step_key),
                        )
                        row = cursor.fetchone()
                        sibling_status_cache[prior_key] = row if row else (1, 0)
                    total_shards, completed_siblings = sibling_status_cache[prior_key]
                    if shard_index == total_shards and total_shards > 1:
                        if completed_siblings != total_shards - 1:
                            continue

                cursor.execute(
                    """
                    WITH claimable AS (
                        SELECT id
                        FROM migration.migration_shards
                        WHERE id = %s
                          AND status IN ('queued', 'retrying')
                          AND EXISTS (
                              SELECT 1
                              FROM migration.migration_steps ms
                              WHERE ms.migration_run_id =
                                    migration.migration_shards.migration_run_id
                                AND ms.step_key =
                                    migration.migration_shards.step_key
                                AND ms.status IN ('pending', 'running')
                          )
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE migration.migration_shards AS shard
                    SET status = 'running',
                        owner = %s,
                        lease_expires_at = now() + make_interval(secs => %s),
                        started_at = COALESCE(started_at, now()),
                        attempts = attempts + 1
                    FROM claimable
                    WHERE shard.id = claimable.id
                    RETURNING shard.id, shard.migration_run_id, shard.step_key,
                              shard.target_database, shard.shard_index,
                              shard.total_shards, shard.file_path,
                              shard.expected_rows, shard.checksum, shard.attempts,
                              shard.max_attempts, shard.owner_emails
                    """,
                    (shard_id, worker_id, lease_seconds),
                )
                row = cursor.fetchone()
                if row is None:
                    # Another worker won this candidate after our lock-free
                    # scan. Continue looking rather than waiting on its lock.
                    continue
                return ClaimedShard(
                    id=str(row[0]),
                    migration_run_id=str(row[1]),
                    step_key=row[2],
                    target_database=row[3],
                    shard_index=row[4],
                    total_shards=row[5],
                    file_path=row[6],
                    expected_rows=row[7],
                    checksum=row[8],
                    attempts=row[9],
                    max_attempts=row[10],
                    owner_emails=list(row[11] or []),
                )

            return None
    finally:
        conn.close()


def heartbeat_shard(
    base_config: ConnectionConfig,
    target_database: str,
    shard_id: str,
    worker_id: str,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
) -> bool:
    """Extend a shard's lease. Returns False if this worker no longer owns it."""
    config = config_for_database(base_config, target_database)
    conn = get_connection(config)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE migration.migration_shards
                SET lease_expires_at = now() + make_interval(secs => %s)
                WHERE id = %s AND owner = %s AND status = 'running'
                """,
                (lease_seconds, shard_id, worker_id),
            )
            return cursor.rowcount == 1
    finally:
        conn.close()


def complete_shard(
    base_config: ConnectionConfig,
    target_database: str,
    shard_id: str,
    worker_id: str,
    driver_rowcount: int,
) -> bool:
    """Mark a shard completed and retain only the diagnostic DB-API rowcount."""
    config = config_for_database(base_config, target_database)
    conn = get_connection(config)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE migration.migration_shards
                SET status = 'completed',
                    affected_count = NULL,
                    driver_rowcount = %s,
                    completed_at = now(), error_message = NULL
                WHERE id = %s AND owner = %s AND status = 'running'
                """,
                (driver_rowcount, shard_id, worker_id),
            )
            return cursor.rowcount == 1
    finally:
        conn.close()


def fail_shard(
    base_config: ConnectionConfig,
    target_database: str,
    shard_id: str,
    worker_id: str,
    error_message: str,
    diagnostic_context: Optional[Mapping[str, object]] = None,
    force_terminal: bool = False,
) -> str:
    """Record a shard failure. Returns 'retrying' or 'failed' (terminal)."""
    force_terminal = force_terminal or is_non_retryable_failure(
        error_message,
        diagnostic_context,
    )
    config = config_for_database(base_config, target_database)
    conn = get_connection(config)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE migration.migration_shards
                SET status = CASE
                        WHEN %s OR attempts >= max_attempts THEN 'failed'
                        ELSE 'retrying'
                    END,
                    error_message = %s,
                    lease_expires_at = NULL,
                    owner = NULL
                WHERE id = %s AND owner = %s AND status = 'running'
                RETURNING status, migration_run_id, step_key, attempts,
                          max_attempts, file_path, owner_emails
                """,
                (
                    force_terminal,
                    error_message[:2000] if error_message else None,
                    shard_id,
                    worker_id,
                ),
            )
            row = cursor.fetchone()
            if row and error_message:
                classification = classify_error(
                    error_message,
                    phase="shard",
                    step_key=row[2],
                )
                insert_diagnostic_event(
                    cursor,
                    str(row[1]),
                    phase="shard_execute",
                    code=classification["code"],
                    message=error_message,
                    step_key=row[2],
                    shard_id=shard_id,
                    context={
                        "target_database": target_database,
                        "status": row[0],
                        "attempt": row[3],
                        "max_attempts": row[4],
                        "file_path": row[5],
                        "owner_emails": list(row[6] or []),
                        "worker_id": worker_id,
                        "non_retryable": force_terminal,
                        **classification.get("facts", {}),
                        **dict(diagnostic_context or {}),
                    },
                )
            return row[0] if row else "unknown"
    finally:
        conn.close()


def recover_stale_leases(base_config: ConnectionConfig, target_database: str) -> int:
    """Reset shards whose worker died mid-lease back to 'queued' for retry."""
    config = config_for_database(base_config, target_database)
    conn = get_connection(config)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE migration.migration_shards
                SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'retrying' END,
                    error_message = COALESCE(error_message, 'Recovered from stale worker lease'),
                    owner = NULL,
                    lease_expires_at = NULL
                WHERE target_database = %s
                  AND status = 'running'
                  AND lease_expires_at < now()
                RETURNING migration_run_id, step_key, id, status, attempts,
                          max_attempts, file_path, owner_emails
                """,
                (target_database,),
            )
            recovered = cursor.fetchall()
            for row in recovered:
                insert_diagnostic_event(
                    cursor,
                    str(row[0]),
                    phase="worker_lease",
                    code="SHARD_STALE_LEASE",
                    message="Worker lease expired before the shard completed.",
                    step_key=row[1],
                    shard_id=str(row[2]),
                    severity="warning" if row[3] == "retrying" else "error",
                    context={
                        "target_database": target_database,
                        "status": row[3],
                        "attempt": row[4],
                        "max_attempts": row[5],
                        "file_path": row[6],
                        "owner_emails": list(row[7] or []),
                    },
                )
            return len(recovered)
    finally:
        conn.close()


def get_unreported_terminal_failures(
    base_config: ConnectionConfig,
    target_database: str,
) -> List[dict]:
    """Return terminal shard failures not yet reflected in step tracking."""
    config = config_for_database(base_config, target_database)
    conn = get_connection(config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT s.migration_run_id, s.step_key, s.error_message,
                       s.owner_emails
                FROM migration.migration_shards s
                JOIN migration.migration_steps ms
                  ON ms.migration_run_id = s.migration_run_id
                 AND ms.step_key = s.step_key
                WHERE s.target_database = %s
                  AND s.status = 'failed'
                  AND ms.status <> 'failed'
                ORDER BY s.created_at
                """,
                (target_database,),
            )
            grouped = {}
            for run_id, step_key, error_message, owner_emails in cursor.fetchall():
                key = (str(run_id), step_key)
                item = grouped.setdefault(
                    key,
                    {
                        "migration_run_id": str(run_id),
                        "step_key": step_key,
                        "error_messages": [],
                        "owner_emails": set(),
                    },
                )
                if error_message:
                    item["error_messages"].append(error_message)
                item["owner_emails"].update(owner_emails or [])
            return [
                {
                    **item,
                    "owner_emails": sorted(item["owner_emails"]),
                }
                for item in grouped.values()
            ]
    finally:
        conn.close()


def cancel_run_shards(
    base_config: ConnectionConfig,
    run_id: str,
    step_key: Optional[str] = None,
) -> int:
    """Cancel queued work without lying about shards already executing.

    Running shards keep their status and lease until they commit or fail, so
    rollback guards can reliably see that database mutation is still active.
    """
    cancelled = 0
    for target_database in ("user_db", "document_db", "completion_db"):
        config = config_for_database(base_config, target_database)
        conn = get_connection(config)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                if step_key:
                    cursor.execute(
                        """
                        UPDATE migration.migration_shards
                        SET status = 'cancelled'
                        WHERE migration_run_id = %s::uuid AND step_key = %s
                          AND status IN ('queued', 'retrying')
                        """,
                        (run_id, normalize_step_key(step_key)),
                    )
                else:
                    cursor.execute(
                        """
                        UPDATE migration.migration_shards
                        SET status = 'cancelled'
                        WHERE migration_run_id = %s::uuid
                          AND status IN ('queued', 'retrying')
                        """,
                        (run_id,),
                    )
                cancelled += cursor.rowcount
        finally:
            conn.close()
    return cancelled


def resume_run_shards(
    base_config: ConnectionConfig,
    run_id: str,
    step_key: Optional[str] = None,
) -> int:
    """Re-queue cancelled/failed shards for a run so workers pick them up again."""
    resumed = 0
    for target_database in ("user_db", "document_db", "completion_db"):
        config = config_for_database(base_config, target_database)
        conn = get_connection(config)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                if step_key:
                    cursor.execute(
                        """
                        SELECT DISTINCT step_key
                        FROM migration.migration_shards
                        WHERE migration_run_id = %s::uuid AND step_key = %s
                          AND status IN ('cancelled', 'failed')
                        """,
                        (run_id, normalize_step_key(step_key)),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT DISTINCT step_key
                        FROM migration.migration_shards
                        WHERE migration_run_id = %s::uuid
                          AND status IN ('cancelled', 'failed')
                        """,
                        (run_id,),
                    )
                resumed_step_keys = [row[0] for row in cursor.fetchall()]
                if step_key:
                    cursor.execute(
                        """
                        UPDATE migration.migration_shards
                        SET status = 'queued', attempts = 0, error_message = NULL,
                            owner = NULL, lease_expires_at = NULL
                        WHERE migration_run_id = %s::uuid AND step_key = %s
                          AND status IN ('cancelled', 'failed')
                        """,
                        (run_id, normalize_step_key(step_key)),
                    )
                else:
                    cursor.execute(
                        """
                        UPDATE migration.migration_shards
                        SET status = 'queued', attempts = 0, error_message = NULL,
                            owner = NULL, lease_expires_at = NULL
                        WHERE migration_run_id = %s::uuid
                          AND status IN ('cancelled', 'failed')
                        """,
                        (run_id,),
                    )
                resumed += cursor.rowcount
                if resumed_step_keys:
                    cursor.execute(
                        """
                        UPDATE migration.migration_steps
                        SET status = 'running', error_message = NULL,
                            completed_at = NULL
                        WHERE migration_run_id = %s::uuid
                          AND step_key = ANY(%s::text[])
                        """,
                        (run_id, resumed_step_keys),
                    )
        finally:
            conn.close()
    return resumed


def get_shard_progress(base_config: ConnectionConfig, run_id: str) -> List[dict]:
    """Return shard status counts without performing schema-changing DDL."""
    progress = []
    for target_database in ("user_db", "document_db", "completion_db"):
        config = config_for_database(base_config, target_database)
        conn = get_connection(config)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT step_key,
                           status,
                           COUNT(*) AS shard_count,
                           COALESCE(SUM(driver_rowcount), 0) AS driver_rowcount_total,
                           COALESCE(SUM(expected_rows), 0) AS expected_total
                    FROM migration.migration_shards
                    WHERE migration_run_id = %s::uuid
                    GROUP BY step_key, status
                    ORDER BY step_key, status
                    """,
                    (run_id,),
                )
                for (
                    step_key,
                    status,
                    shard_count,
                    driver_rowcount_total,
                    expected_total,
                ) in cursor.fetchall():
                    progress.append(
                        {
                            "target_database": target_database,
                            "step_key": step_key,
                            "status": status,
                            "shard_count": shard_count,
                            "driver_rowcount_total": driver_rowcount_total,
                            "expected_total": expected_total,
                        }
                    )
        finally:
            conn.close()
    return progress


def get_failed_shard_details(
    base_config: ConnectionConfig,
    run_id: str,
) -> List[dict]:
    """Return actionable shard errors for operator-facing diagnostics."""
    details = []
    for target_database in ("user_db", "document_db", "completion_db"):
        config = config_for_database(base_config, target_database)
        conn = get_connection(config)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, step_key, shard_index, total_shards, file_path,
                           status, attempts, max_attempts, error_message,
                           owner_emails, expected_rows, driver_rowcount,
                           started_at, completed_at
                    FROM migration.migration_shards
                    WHERE migration_run_id = %s::uuid
                      AND status IN ('failed', 'retrying')
                      AND error_message IS NOT NULL
                    ORDER BY step_order, shard_index
                    """,
                    (run_id,),
                )
                for row in cursor.fetchall():
                    details.append(
                        {
                            "id": str(row[0]),
                            "target_database": target_database,
                            "step_key": row[1],
                            "shard_index": row[2],
                            "total_shards": row[3],
                            "file_path": row[4],
                            "status": row[5],
                            "attempts": row[6],
                            "max_attempts": row[7],
                            "error_message": row[8],
                            "owner_emails": list(row[9] or []),
                            "expected_rows": row[10],
                            "driver_rowcount": row[11],
                            "started_at": row[12],
                            "completed_at": row[13],
                        }
                    )
        finally:
            conn.close()
    return details


def step_shard_summary(base_config: ConnectionConfig, run_id: str, step_key: str) -> dict:
    """Return counts of shards by status for one step, used to decide finalization."""
    step_key = normalize_step_key(step_key)
    target_database = STEP_TARGET_DB[step_key]
    config = config_for_database(base_config, target_database)
    conn = get_connection(config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status = 'completed') AS completed,
                    COUNT(*) FILTER (WHERE status = 'failed') AS failed,
                    COALESCE(
                        jsonb_agg(owner_emails) FILTER (WHERE status = 'failed'),
                        '[]'::jsonb
                    ) AS failed_owner_groups
                FROM migration.migration_shards
                WHERE migration_run_id = %s::uuid AND step_key = %s
                """,
                (run_id, step_key),
            )
            total, completed, failed, failed_owner_groups = cursor.fetchone()
            return {
                "total": total,
                "completed": completed,
                "failed": failed,
                "failed_owner_emails": sorted(
                    {
                        email
                        for group in (failed_owner_groups or [])
                        for email in (group or [])
                    }
                ),
                "all_completed": total > 0 and completed == total,
                "any_failed": failed > 0,
            }
    finally:
        conn.close()
