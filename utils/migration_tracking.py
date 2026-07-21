"""Distributed migration-run tracking shared by the Streamlit pages."""
from __future__ import annotations

import json
from typing import Iterable, Mapping, Optional

from utils.db import ConnectionConfig, get_connection


TARGET_DATABASES = ("user_db", "document_db", "completion_db")
STEP_TARGETS = {
    "01_users": "user_db",
    "02_folders": "document_db",
    "03_documents": "document_db",
    "04_chunks_embeddings": "document_db",
    "05_conversations": "completion_db",
    "06_agents": "completion_db",
    "07_conversions": "completion_db",
}

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


def ensure_tracking_schema(config: ConnectionConfig, coordinator: bool = False) -> None:
    conn = get_connection(config)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(LOCAL_TRACKING_DDL)
            if coordinator:
                cursor.execute(COORDINATOR_TRACKING_DDL)
    finally:
        conn.close()


def ensure_source_tracking_schema(source_config: ConnectionConfig) -> None:
    """Create V4 audit tables only; never mutate V4 business schemas."""
    conn = get_connection(source_config)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(SOURCE_TRACKING_DDL)
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
            cursor.execute(
                """
                UPDATE migration.migration_user_results
                SET steps_completed =
                        COALESCE(steps_completed, '{}'::jsonb) || %s::jsonb,
                    result = CASE WHEN %s THEN result ELSE 'failed' END,
                    failed_step = CASE WHEN %s THEN failed_step ELSE %s END,
                    error_message = CASE WHEN %s THEN error_message ELSE %s END,
                    completed_at = CASE WHEN %s THEN completed_at ELSE now() END
                WHERE batch_id = %s::uuid
                  AND result IN ('pending', 'failed')
                """,
                (
                    json.dumps({step_key: step_value}),
                    success,
                    success,
                    step_key,
                    success,
                    error_message,
                    success,
                    run_id,
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
) -> None:
    """Store the local fact and update the canonical per-user summary."""
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
    finally:
        conn.close()

    coordinator = config_for_database(base_config, "user_db")
    ensure_tracking_schema(coordinator, coordinator=True)
    conn = get_connection(coordinator)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            step_value = "success" if success else "failed"
            cursor.execute(
                """
                UPDATE migration.migration_user_results
                SET steps_completed =
                        COALESCE(steps_completed, '{}'::jsonb) || %s::jsonb,
                    result = CASE WHEN %s THEN result ELSE 'failed' END,
                    failed_step = CASE WHEN %s THEN failed_step ELSE %s END,
                    error_message = CASE WHEN %s THEN error_message ELSE %s END,
                    completed_at = CASE WHEN %s THEN completed_at ELSE now() END
                WHERE batch_id = %s::uuid
                  AND result IN ('pending', 'failed')
                """,
                (
                    json.dumps({step_key: step_value}),
                    success,
                    success,
                    step_key,
                    success,
                    error_message,
                    success,
                    run_id,
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
                  AND result <> 'failed'
                """,
                (overall, overall, run_id),
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
                      AND result <> 'failed'
                    """,
                    (overall, overall, run_id),
                )
        finally:
            conn.close()
    return overall
