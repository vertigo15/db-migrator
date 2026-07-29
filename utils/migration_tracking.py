"""Distributed migration-run tracking shared by the Streamlit pages."""
from __future__ import annotations

import json
import threading
from typing import Iterable, Mapping, Optional

from utils.db import ConnectionConfig, get_connection
from utils.migration_diagnostic_store import insert_diagnostic_event
from utils.migration_diagnostics import classify_error
from utils.migration_steps import STEP_TARGET_DB as STEP_TARGETS


TARGET_DATABASES = ("user_db", "document_db", "completion_db")

LOCAL_TRACKING_DDL = """
CREATE SCHEMA IF NOT EXISTS migration;

CREATE TABLE IF NOT EXISTS migration.migration_runs (
    id UUID PRIMARY KEY,
    status VARCHAR(30) NOT NULL DEFAULT 'running',
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    total_users INTEGER NOT NULL DEFAULT 0,
    source_info JSONB,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS migration.migration_steps (
    migration_run_id UUID NOT NULL
        REFERENCES migration.migration_runs(id) ON DELETE CASCADE,
    step_key VARCHAR(50) NOT NULL,
    target_database VARCHAR(100) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'pending',
    expected_count INTEGER,
    affected_count INTEGER,
    verification_details JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (migration_run_id, step_key)
);

ALTER TABLE migration.migration_steps
    ADD COLUMN IF NOT EXISTS verification_details JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS migration.migration_step_entities (
    migration_run_id UUID NOT NULL
        REFERENCES migration.migration_runs(id) ON DELETE CASCADE,
    step_key VARCHAR(50) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    old_id VARCHAR(255) NOT NULL,
    new_id UUID NOT NULL,
    record_action VARCHAR(20) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (migration_run_id, step_key, table_name, old_id)
);

CREATE INDEX IF NOT EXISTS idx_migration_step_entities_new_id
    ON migration.migration_step_entities(table_name, new_id);

CREATE TABLE IF NOT EXISTS migration.migration_diagnostic_events (
    id BIGSERIAL PRIMARY KEY,
    migration_run_id UUID NOT NULL,
    step_key VARCHAR(50),
    shard_id UUID,
    phase VARCHAR(40) NOT NULL,
    severity VARCHAR(20) NOT NULL DEFAULT 'error',
    code VARCHAR(80) NOT NULL,
    message TEXT NOT NULL,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_migration_diagnostics_run
    ON migration.migration_diagnostic_events(migration_run_id, created_at);
CREATE INDEX IF NOT EXISTS ix_migration_diagnostics_step
    ON migration.migration_diagnostic_events(migration_run_id, step_key);
"""

# Durable job/shard queue. Lives in whichever database a step's shards
# execute against (mirrors migration_steps placement). Workers claim shards
# atomically with FOR UPDATE SKIP LOCKED and track leases for stale recovery.
SHARD_TRACKING_DDL = """
CREATE SCHEMA IF NOT EXISTS migration;

CREATE TABLE IF NOT EXISTS migration.migration_shards (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    migration_run_id UUID NOT NULL,
    step_key VARCHAR(50) NOT NULL,
    step_order INTEGER NOT NULL,
    target_database VARCHAR(100) NOT NULL,
    shard_index INTEGER NOT NULL,
    total_shards INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    expected_rows INTEGER NOT NULL DEFAULT 0,
    byte_size BIGINT NOT NULL DEFAULT 0,
    checksum VARCHAR(64) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'queued',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    owner VARCHAR(200),
    lease_expires_at TIMESTAMPTZ,
    affected_count INTEGER,
    driver_rowcount INTEGER,
    error_message TEXT,
    owner_emails JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    CONSTRAINT uq_migration_shard UNIQUE (migration_run_id, step_key, shard_index)
);

ALTER TABLE migration.migration_shards
    ADD COLUMN IF NOT EXISTS owner_emails JSONB NOT NULL DEFAULT '[]'::jsonb;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'migration'
          AND table_name = 'migration_shards'
          AND column_name = 'driver_rowcount'
    ) THEN
        ALTER TABLE migration.migration_shards
            ADD COLUMN driver_rowcount INTEGER;
        UPDATE migration.migration_shards
        SET driver_rowcount = affected_count,
            affected_count = NULL
        WHERE affected_count IS NOT NULL;
    END IF;
END $$;

COMMENT ON COLUMN migration.migration_shards.affected_count IS
    'Reserved for an exact shard-level entity count; NULL when not explicitly verified.';
COMMENT ON COLUMN migration.migration_shards.driver_rowcount IS
    'Raw DB-API rowcount for the final statement in the shard; diagnostic only.';

CREATE INDEX IF NOT EXISTS ix_migration_shards_claimable
    ON migration.migration_shards (target_database, status, created_at);
CREATE INDEX IF NOT EXISTS ix_migration_shards_run_step
    ON migration.migration_shards (migration_run_id, step_key);
"""

COORDINATOR_TRACKING_DDL = """
CREATE TABLE IF NOT EXISTS migration.migration_batches (
    id UUID PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    status VARCHAR(30) NOT NULL DEFAULT 'running',
    total_users INTEGER NOT NULL DEFAULT 0,
    source_info JSONB,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS migration.migration_user_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id UUID NOT NULL
        REFERENCES migration.migration_batches(id) ON DELETE CASCADE,
    email VARCHAR(255) NOT NULL,
    legacy_user_id VARCHAR(255),
    v5_user_id UUID,
    user_action VARCHAR(20) NOT NULL DEFAULT 'created',
    result VARCHAR(50) NOT NULL DEFAULT 'pending',
    failed_step VARCHAR(100),
    error_message TEXT,
    steps_completed JSONB DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT uq_migration_user_result UNIQUE (batch_id, email)
);

ALTER TABLE migration.migration_user_results
    ADD COLUMN IF NOT EXISTS user_action VARCHAR(20) NOT NULL DEFAULT 'created';
ALTER TABLE migration.migration_user_results
    ADD COLUMN IF NOT EXISTS steps_completed JSONB DEFAULT '{}'::jsonb;
CREATE UNIQUE INDEX IF NOT EXISTS uq_migration_user_result_batch_email
    ON migration.migration_user_results(batch_id, email);
"""

# V4 source audit only. Never touch prefixed business tables.
SOURCE_TRACKING_DDL = """
CREATE SCHEMA IF NOT EXISTS migration;

CREATE TABLE IF NOT EXISTS migration.migration_runs (
    id UUID PRIMARY KEY,
    status VARCHAR(30) NOT NULL DEFAULT 'running',
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    total_users INTEGER NOT NULL DEFAULT 0,
    source_info JSONB,
    target_info JSONB,
    error_message TEXT
);

ALTER TABLE migration.migration_runs
    ADD COLUMN IF NOT EXISTS target_info JSONB;

CREATE TABLE IF NOT EXISTS migration.migration_steps (
    migration_run_id UUID NOT NULL
        REFERENCES migration.migration_runs(id) ON DELETE CASCADE,
    step_key VARCHAR(50) NOT NULL,
    target_database VARCHAR(100) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'pending',
    expected_count INTEGER,
    affected_count INTEGER,
    verification_details JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (migration_run_id, step_key)
);

ALTER TABLE migration.migration_steps
    ADD COLUMN IF NOT EXISTS verification_details JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS migration.migration_user_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id UUID NOT NULL
        REFERENCES migration.migration_runs(id) ON DELETE CASCADE,
    email VARCHAR(255) NOT NULL,
    legacy_user_id VARCHAR(255),
    v5_user_id UUID,
    user_action VARCHAR(20) NOT NULL DEFAULT 'created',
    result VARCHAR(50) NOT NULL DEFAULT 'pending',
    failed_step VARCHAR(100),
    error_message TEXT,
    steps_completed JSONB DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT uq_source_migration_user_result UNIQUE (batch_id, email)
);

ALTER TABLE migration.migration_user_results
    ADD COLUMN IF NOT EXISTS user_action VARCHAR(20) NOT NULL DEFAULT 'created';
ALTER TABLE migration.migration_user_results
    ADD COLUMN IF NOT EXISTS steps_completed JSONB DEFAULT '{}'::jsonb;
CREATE UNIQUE INDEX IF NOT EXISTS uq_source_migration_user_result_batch_email
    ON migration.migration_user_results(batch_id, email);
"""


def config_for_database(base: ConnectionConfig, database: str) -> ConnectionConfig:
    return ConnectionConfig(
        host=base.host,
        port=base.port,
        database=database,
        username=base.username,
        password=base.password,
    )


# Arbitrary fixed key identifying the "create migration tracking DDL"
# advisory lock domain. Plain `CREATE TABLE IF NOT EXISTS` is *not* safe
# against concurrent first-time callers (multiple worker processes can start
# at once against a brand-new database and race on the same catalog row,
# raising a spurious `duplicate key value violates unique constraint
# "pg_type_typname_nsp_index"`), so every DDL-running function below
# serializes on this session-scoped lock instead.
_SCHEMA_DDL_LOCK_KEY = 727837465
_SCHEMA_READY = set()
_SCHEMA_READY_LOCK = threading.Lock()


def _schema_cache_key(
    config: ConnectionConfig,
    schema_kind: str,
    coordinator: bool = False,
) -> tuple:
    return (
        schema_kind,
        config.host,
        int(config.port),
        config.database,
        config.username,
        coordinator,
    )


def ensure_tracking_schema(config: ConnectionConfig, coordinator: bool = False) -> None:
    """Install tracking DDL once per process and target database.

    Queue/status reads must never call this function. The cache prevents hot
    mutation paths from repeatedly requesting PostgreSQL table locks after
    startup or run creation has already installed the schema.
    """
    cache_key = _schema_cache_key(config, "target", coordinator)
    with _SCHEMA_READY_LOCK:
        if cache_key in _SCHEMA_READY:
            return
        conn = get_connection(config)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_lock(%s)", (_SCHEMA_DDL_LOCK_KEY,))
                try:
                    cursor.execute(LOCAL_TRACKING_DDL)
                    cursor.execute(SHARD_TRACKING_DDL)
                    if coordinator:
                        cursor.execute(COORDINATOR_TRACKING_DDL)
                finally:
                    cursor.execute(
                        "SELECT pg_advisory_unlock(%s)",
                        (_SCHEMA_DDL_LOCK_KEY,),
                    )
            _SCHEMA_READY.add(cache_key)
        finally:
            conn.close()


def ensure_source_tracking_schema(source_config: ConnectionConfig) -> None:
    """Create V4 audit tables only; never mutate V4 business schemas."""
    cache_key = _schema_cache_key(source_config, "source")
    with _SCHEMA_READY_LOCK:
        if cache_key in _SCHEMA_READY:
            return
        conn = get_connection(source_config)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_lock(%s)", (_SCHEMA_DDL_LOCK_KEY,))
                try:
                    cursor.execute(SOURCE_TRACKING_DDL)
                finally:
                    cursor.execute(
                        "SELECT pg_advisory_unlock(%s)",
                        (_SCHEMA_DDL_LOCK_KEY,),
                    )
            _SCHEMA_READY.add(cache_key)
        finally:
            conn.close()


def _target_info_from_base(base_config: ConnectionConfig) -> dict:
    return {
        "host": base_config.host,
        "port": base_config.port,
        "databases": list(TARGET_DATABASES),
    }


def update_source_run(
    source_config: ConnectionConfig,
    run_id: str,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    ensure_source_tracking_schema(source_config)
    conn = get_connection(source_config)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE migration.migration_runs
                SET status = %s,
                    error_message = %s,
                    completed_at = CASE
                        WHEN %s IN (
                            'completed', 'failed', 'rolled_back', 'partial'
                        )
                        THEN COALESCE(completed_at, now())
                        ELSE completed_at
                    END
                WHERE id = %s::uuid
                """,
                (status, error_message, status, run_id),
            )
    finally:
        conn.close()


def create_source_run(
    source_config: ConnectionConfig,
    run_id: str,
    users: Iterable[Mapping[str, object]],
    source_info: Mapping[str, object],
    target_info: Mapping[str, object],
) -> None:
    """Mirror a migration run into the V4 source as audit-only metadata."""
    user_rows = list(users)
    ensure_source_tracking_schema(source_config)
    conn = get_connection(source_config)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO migration.migration_runs
                    (id, status, total_users, source_info, target_info)
                VALUES (%s::uuid, 'running', %s, %s::jsonb, %s::jsonb)
                ON CONFLICT (id) DO UPDATE SET
                    status = EXCLUDED.status,
                    total_users = EXCLUDED.total_users,
                    source_info = EXCLUDED.source_info,
                    target_info = EXCLUDED.target_info,
                    error_message = NULL,
                    completed_at = NULL
                """,
                (
                    run_id,
                    len(user_rows),
                    json.dumps(dict(source_info)),
                    json.dumps(dict(target_info)),
                ),
            )
            for step_key, target_database in STEP_TARGETS.items():
                cursor.execute(
                    """
                    INSERT INTO migration.migration_steps
                        (migration_run_id, step_key, target_database, status)
                    VALUES (%s::uuid, %s, %s, 'pending')
                    ON CONFLICT (migration_run_id, step_key) DO NOTHING
                    """,
                    (run_id, step_key, target_database),
                )
            for user in user_rows:
                cursor.execute(
                    """
                    INSERT INTO migration.migration_user_results
                        (batch_id, email, legacy_user_id, v5_user_id,
                         user_action, result)
                    VALUES (%s::uuid, %s, %s, %s::uuid, %s, 'pending')
                    ON CONFLICT (batch_id, email) DO UPDATE SET
                        legacy_user_id = EXCLUDED.legacy_user_id,
                        v5_user_id = EXCLUDED.v5_user_id,
                        user_action = EXCLUDED.user_action
                    """,
                    (
                        run_id,
                        user["email"],
                        user["legacy_user_id"],
                        user["v5_user_id"],
                        user["action"],
                    ),
                )
    finally:
        conn.close()


def create_distributed_run(
    base_config: ConnectionConfig,
    run_id: str,
    users: Iterable[Mapping[str, object]],
    source_info: Mapping[str, object],
    source_config: Optional[ConnectionConfig] = None,
) -> None:
    """Create the same run in all target DBs and optionally mirror it to V4."""
    user_rows = list(users)
    source_json = json.dumps(dict(source_info))

    created_databases = []
    try:
        for database in TARGET_DATABASES:
            config = config_for_database(base_config, database)
            ensure_tracking_schema(config, coordinator=database == "user_db")
            conn = get_connection(config)
            try:
                conn.autocommit = True
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO migration.migration_runs
                            (id, status, total_users, source_info)
                        VALUES (%s::uuid, 'running', %s, %s::jsonb)
                        ON CONFLICT (id) DO NOTHING
                        """,
                        (run_id, len(user_rows), source_json),
                    )
                    for step_key, target_database in STEP_TARGETS.items():
                        if target_database != database:
                            continue
                        cursor.execute(
                            """
                            INSERT INTO migration.migration_steps
                                (migration_run_id, step_key, target_database, status)
                            VALUES (%s::uuid, %s, %s, 'pending')
                            ON CONFLICT (migration_run_id, step_key) DO NOTHING
                            """,
                            (run_id, step_key, database),
                        )
                    if database == "user_db":
                        cursor.execute(
                            """
                            INSERT INTO migration.migration_batches
                                (id, status, total_users, source_info)
                            VALUES (%s::uuid, 'running', %s, %s::jsonb)
                            ON CONFLICT (id) DO NOTHING
                            """,
                            (run_id, len(user_rows), source_json),
                        )
                        for user in user_rows:
                            cursor.execute(
                                """
                                INSERT INTO migration.migration_user_results
                                    (batch_id, email, legacy_user_id, v5_user_id,
                                     user_action, result)
                                VALUES (%s::uuid, %s, %s, %s::uuid, %s, 'pending')
                                ON CONFLICT (batch_id, email) DO UPDATE SET
                                    legacy_user_id = EXCLUDED.legacy_user_id,
                                    v5_user_id = EXCLUDED.v5_user_id,
                                    user_action = EXCLUDED.user_action
                                """,
                                (
                                    run_id,
                                    user["email"],
                                    user["legacy_user_id"],
                                    user["v5_user_id"],
                                    user["action"],
                                ),
                            )
            finally:
                conn.close()
            created_databases.append(database)

        if source_config is not None:
            create_source_run(
                source_config,
                run_id,
                user_rows,
                source_info,
                _target_info_from_base(base_config),
            )
    except Exception:
        # A partial run is intentionally retained as evidence for reconciliation.
        for database in created_databases:
            try:
                update_local_run(
                    base_config,
                    database,
                    run_id,
                    "partial",
                    error_message="Run initialization failed in another target database",
                )
            except Exception:
                pass
        if source_config is not None:
            try:
                update_source_run(
                    source_config,
                    run_id,
                    "partial",
                    error_message="Run initialization failed while creating distributed tracking",
                )
            except Exception:
                pass
        raise


def update_local_run(
    base_config: ConnectionConfig,
    database: str,
    run_id: str,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    config = config_for_database(base_config, database)
    ensure_tracking_schema(config, coordinator=database == "user_db")
    conn = get_connection(config)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE migration.migration_runs
                SET status = %s,
                    error_message = %s,
                    completed_at = CASE
                        WHEN %s IN ('completed', 'failed', 'rolled_back')
                        THEN now() ELSE completed_at END
                WHERE id = %s::uuid
                """,
                (status, error_message, status, run_id),
            )
            if database == "user_db":
                cursor.execute(
                    """
                    UPDATE migration.migration_batches
                    SET status = %s,
                        completed_at = CASE
                            WHEN %s IN ('completed', 'failed', 'rolled_back')
                            THEN now() ELSE completed_at END
                    WHERE id = %s::uuid
                    """,
                    (status, status, run_id),
                )
                if error_message and status in {"failed", "partial"}:
                    classification = classify_error(
                        error_message,
                        phase="extraction",
                    )
                    insert_diagnostic_event(
                        cursor,
                        run_id,
                        phase="extraction",
                        code=classification["code"],
                        message=error_message,
                        context={"run_status": status},
                    )
    finally:
        conn.close()


def mark_unproduced_steps_skipped(
    base_config: ConnectionConfig,
    run_id: str,
    generated_step_keys: Iterable[str],
    source_config: Optional[ConnectionConfig] = None,
) -> None:
    generated = set(generated_step_keys)
    for database in TARGET_DATABASES:
        config = config_for_database(base_config, database)
        conn = get_connection(config)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE migration.migration_steps
                    SET status = 'skipped', completed_at = now()
                    WHERE migration_run_id = %s::uuid
                      AND NOT (step_key = ANY(%s))
                    """,
                    (run_id, list(generated)),
                )
        finally:
            conn.close()

    if source_config is not None:
        ensure_source_tracking_schema(source_config)
        conn = get_connection(source_config)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE migration.migration_steps
                    SET status = 'skipped', completed_at = now()
                    WHERE migration_run_id = %s::uuid
                      AND NOT (step_key = ANY(%s))
                    """,
                    (run_id, list(generated)),
                )
        finally:
            conn.close()


def record_step_expectations(
    base_config: ConnectionConfig,
    run_id: str,
    expectations: Mapping[str, Mapping[str, object]],
    source_config: Optional[ConnectionConfig] = None,
) -> None:
    """Persist final post-top-up expectations in target and source tracking."""
    for step_key, expectation in expectations.items():
        target_database = STEP_TARGETS[step_key]
        config = config_for_database(base_config, target_database)
        ensure_tracking_schema(config, coordinator=target_database == "user_db")
        conn = get_connection(config)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE migration.migration_steps
                    SET expected_count = %s,
                        verification_details =
                            COALESCE(verification_details, '{}'::jsonb)
                            || %s::jsonb
                    WHERE migration_run_id = %s::uuid AND step_key = %s
                    """,
                    (
                        int(expectation.get("expected_count", 0)),
                        json.dumps(dict(expectation.get("details", {}))),
                        run_id,
                        step_key,
                    ),
                )
        finally:
            conn.close()

    if source_config is None:
        return
    ensure_source_tracking_schema(source_config)
    conn = get_connection(source_config)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            for step_key, expectation in expectations.items():
                cursor.execute(
                    """
                    UPDATE migration.migration_steps
                    SET expected_count = %s,
                        verification_details =
                            COALESCE(verification_details, '{}'::jsonb)
                            || %s::jsonb
                    WHERE migration_run_id = %s::uuid AND step_key = %s
                    """,
                    (
                        int(expectation.get("expected_count", 0)),
                        json.dumps(dict(expectation.get("details", {}))),
                        run_id,
                        step_key,
                    ),
                )
    finally:
        conn.close()


def _mirror_source_step_and_users(
    source_config: ConnectionConfig,
    run_id: str,
    step_key: str,
    target_database: str,
    success: bool,
    affected_count: Optional[int],
    error_message: Optional[str],
    owner_emails: Optional[Iterable[str]] = None,
) -> None:
    ensure_source_tracking_schema(source_config)
    conn = get_connection(source_config)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO migration.migration_steps
                    (migration_run_id, step_key, target_database, status,
                     affected_count, error_message, started_at, completed_at)
                VALUES (%s::uuid, %s, %s, %s, %s, %s, now(), now())
                ON CONFLICT (migration_run_id, step_key) DO UPDATE SET
                    status = EXCLUDED.status,
                    affected_count = EXCLUDED.affected_count,
                    error_message = EXCLUDED.error_message,
                    completed_at = now()
                """,
                (
                    run_id,
                    step_key,
                    target_database,
                    "completed" if success else "failed",
                    affected_count,
                    error_message,
                ),
            )
            cursor.execute(
                """
                UPDATE migration.migration_runs
                SET status = %s, error_message = %s
                WHERE id = %s::uuid
                """,
                ("running" if success else "partial", error_message, run_id),
            )
            step_value = "success" if success else "failed"
            scoped_emails = list(owner_emails) if owner_emails else None
            cursor.execute(
                """
                UPDATE migration.migration_user_results
                SET steps_completed =
                        COALESCE(steps_completed, '{}'::jsonb) || %s::jsonb,
                    result = CASE
                        WHEN %s AND result = 'failed' AND failed_step = %s
                            THEN 'pending'
                        WHEN %s THEN result
                        ELSE 'failed'
                    END,
                    failed_step = CASE
                        WHEN %s AND failed_step = %s THEN NULL
                        WHEN %s THEN failed_step
                        ELSE %s
                    END,
                    error_message = CASE
                        WHEN %s AND failed_step = %s THEN NULL
                        WHEN %s THEN error_message
                        ELSE %s
                    END,
                    completed_at = CASE
                        WHEN %s AND failed_step = %s THEN NULL
                        WHEN %s THEN completed_at
                        ELSE now()
                    END
                WHERE batch_id = %s::uuid
                  AND result IN ('pending', 'failed')
                  AND (%s::text[] IS NULL OR email = ANY(%s::text[]))
                """,
                (
                    json.dumps({step_key: step_value}),
                    success,
                    step_key,
                    success,
                    success,
                    step_key,
                    success,
                    step_key,
                    success,
                    step_key,
                    success,
                    error_message,
                    success,
                    step_key,
                    success,
                    run_id,
                    scoped_emails,
                    scoped_emails,
                ),
            )
    finally:
        conn.close()


def record_step_result(
    base_config: ConnectionConfig,
    run_id: str,
    step_key: str,
    target_database: str,
    success: bool,
    affected_count: Optional[int] = None,
    error_message: Optional[str] = None,
    verification_details: Optional[Mapping[str, object]] = None,
    source_config: Optional[ConnectionConfig] = None,
    owner_emails: Optional[Iterable[str]] = None,
) -> None:
    """Store the local fact and update the canonical per-user summary."""
    scoped_owner_emails = list(owner_emails) if owner_emails else None
    target = config_for_database(base_config, target_database)
    ensure_tracking_schema(target)
    conn = get_connection(target)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO migration.migration_steps
                    (migration_run_id, step_key, target_database, status,
                     affected_count, verification_details, error_message,
                     started_at, completed_at)
                VALUES (%s::uuid, %s, %s, %s, %s, %s::jsonb, %s, now(), now())
                ON CONFLICT (migration_run_id, step_key) DO UPDATE SET
                    status = EXCLUDED.status,
                    affected_count = EXCLUDED.affected_count,
                    verification_details =
                        migration.migration_steps.verification_details
                        || EXCLUDED.verification_details,
                    error_message = EXCLUDED.error_message,
                    completed_at = now()
                """,
                (
                    run_id,
                    step_key,
                    target_database,
                    "completed" if success else "failed",
                    affected_count,
                    json.dumps(dict(verification_details or {})),
                    error_message,
                ),
            )
            cursor.execute(
                """
                UPDATE migration.migration_runs
                SET status = %s, error_message = %s
                WHERE id = %s::uuid
                """,
                ("running" if success else "partial", error_message, run_id),
            )
            if not success and error_message:
                classification = classify_error(
                    error_message,
                    phase="step",
                    step_key=step_key,
                )
                insert_diagnostic_event(
                    cursor,
                    run_id,
                    phase="step_verify",
                    code=classification["code"],
                    message=error_message,
                    step_key=step_key,
                    context={
                        "target_database": target_database,
                        "affected_count": affected_count,
                        "owner_emails": scoped_owner_emails or [],
                        "verification_details": dict(verification_details or {}),
                        **classification.get("facts", {}),
                    },
                )
    finally:
        conn.close()

    coordinator = config_for_database(base_config, "user_db")
    ensure_tracking_schema(coordinator, coordinator=True)
    conn = get_connection(coordinator)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            step_value = "success" if success else "failed"
            scoped_emails = scoped_owner_emails
            cursor.execute(
                """
                UPDATE migration.migration_user_results
                SET steps_completed =
                        COALESCE(steps_completed, '{}'::jsonb) || %s::jsonb,
                    result = CASE
                        WHEN %s AND result = 'failed' AND failed_step = %s
                            THEN 'pending'
                        WHEN %s THEN result
                        ELSE 'failed'
                    END,
                    failed_step = CASE
                        WHEN %s AND failed_step = %s THEN NULL
                        WHEN %s THEN failed_step
                        ELSE %s
                    END,
                    error_message = CASE
                        WHEN %s AND failed_step = %s THEN NULL
                        WHEN %s THEN error_message
                        ELSE %s
                    END,
                    completed_at = CASE
                        WHEN %s AND failed_step = %s THEN NULL
                        WHEN %s THEN completed_at
                        ELSE now()
                    END
                WHERE batch_id = %s::uuid
                  AND result IN ('pending', 'failed')
                  AND (%s::text[] IS NULL OR email = ANY(%s::text[]))
                """,
                (
                    json.dumps({step_key: step_value}),
                    success,
                    step_key,
                    success,
                    success,
                    step_key,
                    success,
                    step_key,
                    success,
                    step_key,
                    success,
                    error_message,
                    success,
                    step_key,
                    success,
                    run_id,
                    scoped_emails,
                    scoped_emails,
                ),
            )
            if not success:
                cursor.execute(
                    """
                    UPDATE migration.migration_runs
                    SET status = 'partial', error_message = %s
                    WHERE id = %s::uuid
                    """,
                    (error_message, run_id),
                )
                cursor.execute(
                    """
                    UPDATE migration.migration_batches
                    SET status = 'partial'
                    WHERE id = %s::uuid
                    """,
                    (run_id,),
                )
    finally:
        conn.close()

    if source_config is not None:
        _mirror_source_step_and_users(
            source_config,
            run_id,
            step_key,
            target_database,
            success,
            affected_count,
            error_message,
            scoped_owner_emails,
        )


def finalize_distributed_run(
    base_config: ConnectionConfig,
    run_id: str,
    source_config: Optional[ConnectionConfig] = None,
) -> None:
    """Complete only when every local recorded step succeeded."""
    incomplete = []
    for database in TARGET_DATABASES:
        config = config_for_database(base_config, database)
        conn = get_connection(config)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT step_key, status
                    FROM migration.migration_steps
                    WHERE migration_run_id = %s::uuid
                      AND status NOT IN ('completed', 'skipped')
                    """,
                    (run_id,),
                )
                incomplete.extend(
                    f"{database}.{step_key}={status}"
                    for step_key, status in cursor.fetchall()
                )
        finally:
            conn.close()
    if incomplete:
        raise RuntimeError(
            "Cannot complete migration run with unfinished local steps: "
            + ", ".join(incomplete)
        )

    for database in TARGET_DATABASES:
        update_local_run(base_config, database, run_id, "completed")

    coordinator = config_for_database(base_config, "user_db")
    conn = get_connection(coordinator)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE migration.migration_user_results
                SET result = CASE
                        WHEN user_action = 'reused'
                        THEN 'reused_existing_user'
                        ELSE 'success'
                    END,
                    completed_at = now()
                WHERE batch_id = %s::uuid AND result = 'pending'
                """,
                (run_id,),
            )
    finally:
        conn.close()

    if source_config is not None:
        ensure_source_tracking_schema(source_config)
        conn = get_connection(source_config)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE migration.migration_runs
                    SET status = 'completed',
                        error_message = NULL,
                        completed_at = now()
                    WHERE id = %s::uuid
                    """,
                    (run_id,),
                )
                cursor.execute(
                    """
                    UPDATE migration.migration_user_results
                    SET result = CASE
                            WHEN user_action = 'reused'
                            THEN 'reused_existing_user'
                            ELSE 'success'
                        END,
                        completed_at = now()
                    WHERE batch_id = %s::uuid AND result = 'pending'
                    """,
                    (run_id,),
                )
        finally:
            conn.close()


def is_distributed_run_ready(
    base_config: ConnectionConfig,
    run_id: str,
) -> bool:
    """True only when every canonical step exists and is completed/skipped."""
    for step_key, database in STEP_TARGETS.items():
        config = config_for_database(base_config, database)
        conn = get_connection(config)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT status
                    FROM migration.migration_steps
                    WHERE migration_run_id = %s::uuid AND step_key = %s
                    """,
                    (run_id, step_key),
                )
                row = cursor.fetchone()
                if row is None or row[0] not in ("completed", "skipped"):
                    return False
        finally:
            conn.close()
    return True


def reconcile_rollback_status(
    base_config: ConnectionConfig,
    run_id: str,
    source_config: Optional[ConnectionConfig] = None,
) -> str:
    """Reconcile local rollback facts into the canonical user_db status."""
    statuses = []
    step_facts = []
    for database in TARGET_DATABASES:
        config = config_for_database(base_config, database)
        ensure_tracking_schema(config, coordinator=database == "user_db")
        conn = get_connection(config)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT step_key, status
                    FROM migration.migration_steps
                    WHERE migration_run_id = %s::uuid
                    """,
                    (run_id,),
                )
                for step_key, status in cursor.fetchall():
                    statuses.append(status)
                    step_facts.append((step_key, status))
        finally:
            conn.close()

    overall = (
        "rolled_back"
        if statuses and all(
            status in ("rolled_back", "skipped") for status in statuses
        )
        else "rollback_pending"
    )
    for database in TARGET_DATABASES:
        update_local_run(base_config, database, run_id, overall)

    coordinator = config_for_database(base_config, "user_db")
    conn = get_connection(coordinator)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE migration.migration_user_results
                SET result = %s,
                    completed_at = CASE
                        WHEN %s = 'rolled_back' THEN now() ELSE completed_at END
                WHERE batch_id = %s::uuid
                  AND (%s = 'rolled_back' OR result <> 'failed')
                  AND (%s = 'rolled_back' OR result <> 'rolled_back')
                """,
                (overall, overall, run_id, overall, overall),
            )
    finally:
        conn.close()

    if source_config is not None:
        ensure_source_tracking_schema(source_config)
        conn = get_connection(source_config)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                for step_key, status in step_facts:
                    cursor.execute(
                        """
                        UPDATE migration.migration_steps
                        SET status = %s,
                            completed_at = CASE
                                WHEN %s IN ('rolled_back', 'skipped')
                                THEN COALESCE(completed_at, now())
                                ELSE completed_at
                            END
                        WHERE migration_run_id = %s::uuid AND step_key = %s
                        """,
                        (status, status, run_id, step_key),
                    )
                cursor.execute(
                    """
                    UPDATE migration.migration_runs
                    SET status = %s,
                        completed_at = CASE
                            WHEN %s = 'rolled_back' THEN now() ELSE completed_at END
                    WHERE id = %s::uuid
                    """,
                    (overall, overall, run_id),
                )
                cursor.execute(
                    """
                    UPDATE migration.migration_user_results
                    SET result = %s,
                        completed_at = CASE
                            WHEN %s = 'rolled_back' THEN now() ELSE completed_at END
                    WHERE batch_id = %s::uuid
                      AND (%s = 'rolled_back' OR result <> 'failed')
                      AND (%s = 'rolled_back' OR result <> 'rolled_back')
                    """,
                    (overall, overall, run_id, overall, overall),
                )
        finally:
            conn.close()
    return overall


def record_user_rollback_result(
    base_config: ConnectionConfig,
    run_id: str,
    email: str,
    source_config: Optional[ConnectionConfig] = None,
) -> int:
    """Mark one user's rollback everywhere and return users not yet rolled back."""
    coordinator = config_for_database(base_config, "user_db")
    ensure_tracking_schema(coordinator, coordinator=True)
    conn = get_connection(coordinator)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE migration.migration_user_results
                SET result = 'rolled_back',
                    failed_step = NULL,
                    error_message = NULL,
                    completed_at = now()
                WHERE batch_id = %s::uuid AND email = %s
                """,
                (run_id, email),
            )
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM migration.migration_user_results
                WHERE batch_id = %s::uuid
                  AND result <> 'rolled_back'
                """,
                (run_id,),
            )
            remaining = int(cursor.fetchone()[0])
    finally:
        conn.close()

    if source_config is not None:
        ensure_source_tracking_schema(source_config)
        conn = get_connection(source_config)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE migration.migration_user_results
                    SET result = 'rolled_back',
                        failed_step = NULL,
                        error_message = NULL,
                        completed_at = now()
                    WHERE batch_id = %s::uuid AND email = %s
                    """,
                    (run_id, email),
                )
        finally:
            conn.close()

    if remaining:
        for database in TARGET_DATABASES:
            update_local_run(base_config, database, run_id, "partial")
        if source_config is not None:
            update_source_run(source_config, run_id, "partial")

    return remaining
