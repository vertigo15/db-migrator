"""Execution logic for a single migration shard worker process.

This module contains the pure, testable pieces used by ``worker.py``:
executing one claimed shard file, and finalizing a step (running the shared
verification and flipping ``migration.migration_steps``) once every shard
for that step has committed. Kept separate from ``worker.py`` so both can be
exercised directly in tests without spawning a subprocess.
"""
from __future__ import annotations

import hashlib
import logging
import re
import time
import threading
from dataclasses import dataclass
from typing import Optional

from utils.db import ConnectionConfig, get_connection
from utils.migration_diagnostics import exception_context
from utils.migration_steps import STEP_TARGET_DB, normalize_step_key
from utils.migration_tracking import (
    config_for_database,
    ensure_tracking_schema,
    finalize_distributed_run,
    is_distributed_run_ready,
    record_step_result,
)
from utils.sql_generator import generate_migration_schema_setup
from utils.shard_queue import (
    DEFAULT_LEASE_SECONDS,
    ClaimedShard,
    cancel_run_shards,
    complete_shard,
    fail_shard,
    get_unreported_terminal_failures,
    heartbeat_shard,
    step_shard_summary,
)
from utils.step_verification import verify_step

logger = logging.getLogger("migration_worker")

_RUNTIME_SCHEMA_LOCK_KEY = 727837466
_RUNTIME_SCHEMA_VERSION = "worker-runtime-v3"
_SHARD_FILE_VERIFY_ATTEMPTS = 10
_SHARD_FILE_VERIFY_RETRY_SECONDS = 1.0
_UUID_EXTENSION_DDL = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'
_MAPPING_SETUP_PATTERN = re.compile(
    r"-- =+\r?\n"
    r"-- MIGRATION MAPPING TABLE SETUP \(idempotent\)\r?\n"
    r".*?"
    r"-- MIGRATION MAPPING TABLE SETUP COMPLETE\r?\n"
    r"-- =+\r?\n",
    re.DOTALL,
)
_LEGACY_EMBEDDING_DIMENSION_DDL = """DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'embeddings' AND column_name = 'dimension'
    ) THEN
        RAISE NOTICE 'Adding missing dimension column to embeddings table';
        ALTER TABLE public.embeddings ADD COLUMN dimension smallint;
    END IF;
END $$;"""
_EMBEDDING_DIMENSION_DDL = """DO $$
BEGIN
    IF to_regclass('public.embeddings') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema = 'public'
             AND table_name = 'embeddings'
             AND column_name = 'dimension'
       )
    THEN
        ALTER TABLE public.embeddings ADD COLUMN dimension smallint;
    END IF;
END $$;"""


@dataclass
class ShardOutcome:
    shard_id: str
    step_key: str
    success: bool
    driver_rowcount: int
    error: Optional[str]
    step_finalized: Optional[bool] = None


def _checksum(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _read_verified_shard(
    file_path: str,
    expected_checksum: Optional[str],
    *,
    attempts: int = _SHARD_FILE_VERIFY_ATTEMPTS,
    retry_delay: float = _SHARD_FILE_VERIFY_RETRY_SECONDS,
) -> tuple[Optional[str], Optional[str]]:
    """Read a shard after its bind-mounted file has reached the expected state.

    ``newline=""`` is essential: SQL payloads can contain CRLF text, and the
    default universal-newline reader would silently convert those bytes before
    hashing. Retrying the local read also avoids consuming all queue attempts
    for a transient bind-mount visibility delay. A persistently different file
    still fails closed and is never executed.
    """
    attempts = max(1, attempts)
    last_error = ""
    for attempt in range(1, attempts + 1):
        try:
            with open(file_path, "r", encoding="utf-8", newline="") as fh:
                content = fh.read()
        except (OSError, UnicodeError) as exc:
            last_error = f"Could not read shard file {file_path}: {exc}"
        else:
            actual_checksum = _checksum(content)
            if not expected_checksum or actual_checksum == expected_checksum:
                if attempt > 1:
                    logger.info(
                        "Shard file %s synchronized after %s verification reads",
                        file_path,
                        attempt,
                    )
                return content, None
            last_error = (
                f"Checksum mismatch for shard {file_path}; expected "
                f"{expected_checksum}, got {actual_checksum}; refusing to "
                "execute a possibly corrupted or replaced file"
            )

        if attempt < attempts:
            logger.warning(
                "Shard file verification failed (%s/%s); retrying in %.1fs: %s",
                attempt,
                attempts,
                retry_delay,
                file_path,
            )
            time.sleep(max(0.0, retry_delay))

    return None, last_error


def ensure_worker_runtime_schema(
    config: ConnectionConfig,
    *,
    coordinator: bool = False,
) -> None:
    """Install SQL-shard support objects once, before any queue claims.

    Generated files remain self-contained for manual ``psql`` use, but workers
    remove this bootstrap block after checksum verification. A database marker
    ensures only the first concurrently starting worker runs the DDL; later
    workers merely observe the installed version and begin polling.
    """
    ensure_tracking_schema(config, coordinator=coordinator)
    conn = get_connection(config)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_lock(%s)",
                (_RUNTIME_SCHEMA_LOCK_KEY,),
            )
            try:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS migration.runtime_schema_versions (
                        version VARCHAR(100) PRIMARY KEY,
                        installed_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cursor.execute(
                    """
                    SELECT 1
                    FROM migration.runtime_schema_versions
                    WHERE version = %s
                    """,
                    (_RUNTIME_SCHEMA_VERSION,),
                )
                if cursor.fetchone() is None:
                    cursor.execute(_UUID_EXTENSION_DDL)
                    cursor.execute(generate_migration_schema_setup())
                    if config.database == "document_db":
                        cursor.execute(_EMBEDDING_DIMENSION_DDL)
                    cursor.execute(
                        """
                        INSERT INTO migration.runtime_schema_versions (version)
                        VALUES (%s)
                        ON CONFLICT (version) DO NOTHING
                        """,
                        (_RUNTIME_SCHEMA_VERSION,),
                    )
            finally:
                cursor.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    (_RUNTIME_SCHEMA_LOCK_KEY,),
                )
    finally:
        conn.close()


def _prepare_shard_sql_for_worker(sql_content: str) -> str:
    """Remove bootstrap DDL that workers install once during startup.

    The checksum is always verified against the original on-disk content
    before this transformation, preserving tamper detection and compatibility
    with already-generated manifests.
    """
    # Match the stable setup markers rather than only today's generated DDL.
    # This also strips bootstrap blocks from shards generated by older releases.
    prepared = _MAPPING_SETUP_PATTERN.sub("", sql_content)
    prepared = prepared.replace(
        _UUID_EXTENSION_DDL,
        "-- uuid-ossp is initialized by the migration worker",
    )
    prepared = prepared.replace(
        _LEGACY_EMBEDDING_DIMENSION_DDL,
        "-- embedding dimension schema is initialized by the migration worker",
    )
    return prepared


def _record_terminal_failure(
    base_config: ConnectionConfig,
    claimed: ClaimedShard,
    error: str,
    source_config: Optional[ConnectionConfig],
) -> None:
    """Fail the step promptly and stop all not-yet-started work for the run."""
    cancel_run_shards(base_config, claimed.migration_run_id)
    record_step_result(
        base_config,
        claimed.migration_run_id,
        claimed.step_key,
        claimed.target_database,
        success=False,
        affected_count=None,
        error_message=error,
        source_config=source_config,
        owner_emails=claimed.owner_emails or None,
    )


def _fail_claimed_shard(
    base_config: ConnectionConfig,
    claimed: ClaimedShard,
    worker_id: str,
    error: str,
    source_config: Optional[ConnectionConfig],
    diagnostic_context: Optional[dict] = None,
) -> None:
    status = fail_shard(
        base_config,
        claimed.target_database,
        claimed.id,
        worker_id,
        error,
        diagnostic_context=diagnostic_context,
    )
    if status == "failed":
        _record_terminal_failure(base_config, claimed, error, source_config)


def reconcile_terminal_failures(
    base_config: ConnectionConfig,
    target_database: str,
    source_config: Optional[ConnectionConfig] = None,
) -> int:
    """Propagate terminal stale-lease failures into step/run/user tracking."""
    failures = get_unreported_terminal_failures(base_config, target_database)
    for failure in failures:
        cancel_run_shards(base_config, failure["migration_run_id"])
        messages = failure["error_messages"]
        error = messages[-1] if messages else "Migration shard exhausted its retry limit"
        record_step_result(
            base_config,
            failure["migration_run_id"],
            failure["step_key"],
            target_database,
            success=False,
            affected_count=None,
            error_message=error,
            source_config=source_config,
            owner_emails=failure["owner_emails"] or None,
        )
    return len(failures)


class _LeaseHeartbeat:
    """Renews a claimed shard's lease from a background thread while its SQL
    runs, so a shard whose execution time approaches the lease window is
    never mistaken for a dead worker and reclaimed mid-flight."""

    def __init__(self, base_config, claimed: ClaimedShard, worker_id: str, lease_seconds: int):
        self._base_config = base_config
        self._claimed = claimed
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        interval = max(1.0, self._lease_seconds / 3)
        while not self._stop.wait(interval):
            try:
                heartbeat_shard(
                    self._base_config,
                    self._claimed.target_database,
                    self._claimed.id,
                    self._worker_id,
                    lease_seconds=self._lease_seconds,
                )
            except Exception:  # noqa: BLE001 - best-effort; execution continues
                logger.warning("Heartbeat failed for shard %s", self._claimed.id, exc_info=True)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc_info):
        self._stop.set()
        self._thread.join(timeout=5)


def execute_claimed_shard(
    base_config: ConnectionConfig,
    claimed: ClaimedShard,
    worker_id: str,
    source_config: Optional[ConnectionConfig] = None,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
) -> ShardOutcome:
    """Execute one shard file end to end: read, verify checksum, run, record.

    On success, also checks whether this was the step's last outstanding
    shard and if so runs step finalization (verification + status flip).
    """
    sql_content, error = _read_verified_shard(
        claimed.file_path,
        claimed.checksum,
    )
    if error:
        logger.error(error)
        _fail_claimed_shard(
            base_config, claimed, worker_id, error, source_config
        )
        return ShardOutcome(claimed.id, claimed.step_key, False, 0, error)
    assert sql_content is not None

    sql_content = _prepare_shard_sql_for_worker(sql_content)

    config = config_for_database(base_config, claimed.target_database)
    conn = get_connection(config)
    conn.autocommit = False
    driver_rowcount = 0
    try:
        with _LeaseHeartbeat(base_config, claimed, worker_id, lease_seconds):
            with conn.cursor() as cursor:
                cursor.execute(sql_content)
                driver_rowcount = max(cursor.rowcount, 0)
            conn.commit()
    except Exception as exc:  # noqa: BLE001 - surfaced via fail_shard/error_message
        conn.rollback()
        error = str(exc)
        logger.error("Shard %s failed: %s", claimed.file_path, error)
        _fail_claimed_shard(
            base_config,
            claimed,
            worker_id,
            error,
            source_config,
            diagnostic_context=exception_context(exc),
        )
        return ShardOutcome(claimed.id, claimed.step_key, False, 0, error)
    finally:
        conn.close()

    if not complete_shard(
        base_config,
        claimed.target_database,
        claimed.id,
        worker_id,
        driver_rowcount,
    ):
        # Lease expired mid-execution and another worker already reclaimed
        # this shard; the SQL we just ran is idempotent (ON CONFLICT/NOT
        # EXISTS guards), so this is safe to surface as a benign race rather
        # than a failure that needs retry accounting.
        logger.warning(
            "Shard %s committed but lease was lost before completion could be recorded",
            claimed.file_path,
        )
        return ShardOutcome(
            claimed.id,
            claimed.step_key,
            True,
            driver_rowcount,
            None,
        )

    finalized = finalize_step_if_complete(
        base_config, claimed.migration_run_id, claimed.step_key, source_config
    )
    if finalized is True and is_distributed_run_ready(
        base_config, claimed.migration_run_id
    ):
        finalize_distributed_run(
            base_config,
            claimed.migration_run_id,
            source_config=source_config,
        )
    return ShardOutcome(
        claimed.id,
        claimed.step_key,
        True,
        driver_rowcount,
        None,
        step_finalized=finalized,
    )


def finalize_step_if_complete(
    base_config: ConnectionConfig,
    run_id: str,
    step_key: str,
    source_config: Optional[ConnectionConfig] = None,
) -> Optional[bool]:
    """If every shard for this step has resolved, verify and close it out.

    Returns True if the step was just finalized as completed, False if it
    was just finalized as failed, or None if the step still has shards in
    flight (nothing to do yet).
    """
    summary = step_shard_summary(base_config, run_id, step_key)
    if summary["total"] == 0:
        return None

    unresolved = summary["total"] - summary["completed"] - summary["failed"]
    if unresolved > 0:
        return None

    target_database = STEP_TARGET_DB[normalize_step_key(step_key)]

    if summary["any_failed"]:
        error_message = (
            f"{summary['failed']}/{summary['total']} shard(s) failed for step {step_key}"
        )
        logger.error("Step %s failed for run %s: %s", step_key, run_id, error_message)
        record_step_result(
            base_config,
            run_id,
            step_key,
            target_database,
            success=False,
            affected_count=None,
            error_message=error_message,
            source_config=source_config,
            owner_emails=summary["failed_owner_emails"] or None,
        )
        cancel_run_shards(base_config, run_id)
        return False

    config = config_for_database(base_config, target_database)
    conn = get_connection(config)
    conn.autocommit = False
    try:
        with conn.cursor() as cursor:
            affected_count, verification_details = verify_step(cursor, step_key, run_id)
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        error_message = f"Step verification failed for {step_key}: {exc}"
        logger.error(error_message)
        record_step_result(
            base_config,
            run_id,
            step_key,
            target_database,
            success=False,
            affected_count=None,
            error_message=error_message,
            source_config=source_config,
        )
        cancel_run_shards(base_config, run_id)
        return False
    finally:
        conn.close()

    logger.info(
        "Step %s completed for run %s (%s affected rows across %s shards)",
        step_key,
        run_id,
        affected_count,
        summary["total"],
    )
    record_step_result(
        base_config,
        run_id,
        step_key,
        target_database,
        success=True,
        affected_count=affected_count,
        verification_details=verification_details,
        source_config=source_config,
    )
    return True


def reverify_completed_step(
    base_config: ConnectionConfig,
    run_id: str,
    step_key: str,
    source_config: Optional[ConnectionConfig] = None,
) -> bool:
    """Re-run verification only after every shard committed successfully."""
    summary = step_shard_summary(base_config, run_id, step_key)
    if summary["total"] == 0 or summary["completed"] != summary["total"]:
        raise RuntimeError(
            f"Cannot re-verify {step_key}: not every shard is completed"
        )
    result = finalize_step_if_complete(
        base_config,
        run_id,
        step_key,
        source_config=source_config,
    )
    if result is True and is_distributed_run_ready(base_config, run_id):
        finalize_distributed_run(
            base_config,
            run_id,
            source_config=source_config,
        )
    return result is True
