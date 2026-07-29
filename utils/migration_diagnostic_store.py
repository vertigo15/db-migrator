"""Persistence helpers for append-only migration diagnostic events."""
from __future__ import annotations

import json
from typing import Mapping, Optional

from utils.db import ConnectionConfig, get_connection


def insert_diagnostic_event(
    cursor,
    migration_run_id: str,
    *,
    phase: str,
    code: str,
    message: str,
    step_key: Optional[str] = None,
    shard_id: Optional[str] = None,
    severity: str = "error",
    context: Optional[Mapping[str, object]] = None,
) -> None:
    """Insert an event using an existing transaction or autocommit cursor."""
    cursor.execute(
        """
        INSERT INTO migration.migration_diagnostic_events (
            migration_run_id, step_key, shard_id, phase, severity,
            code, message, context
        )
        VALUES (
            %s::uuid, %s, %s::uuid, %s, %s, %s, %s, %s::jsonb
        )
        """,
        (
            migration_run_id,
            step_key,
            shard_id,
            phase,
            severity,
            code,
            message,
            json.dumps(dict(context or {}), default=str),
        ),
    )


def record_diagnostic_event(
    config: ConnectionConfig,
    migration_run_id: str,
    *,
    phase: str,
    code: str,
    message: str,
    step_key: Optional[str] = None,
    shard_id: Optional[str] = None,
    severity: str = "error",
    context: Optional[Mapping[str, object]] = None,
) -> None:
    """Persist a diagnostic event in the database where it occurred."""
    conn = get_connection(config)
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            insert_diagnostic_event(
                cursor,
                migration_run_id,
                phase=phase,
                code=code,
                message=message,
                step_key=step_key,
                shard_id=shard_id,
                severity=severity,
                context=context,
            )
    finally:
        conn.close()


def list_diagnostic_events(
    config: ConnectionConfig,
    migration_run_id: str,
) -> list[dict]:
    """Load diagnostic events from one migration database."""
    conn = get_connection(config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, migration_run_id, step_key, shard_id, phase,
                       severity, code, message, context, created_at
                FROM migration.migration_diagnostic_events
                WHERE migration_run_id = %s::uuid
                ORDER BY created_at, id
                """,
                (migration_run_id,),
            )
            events = []
            for row in cursor.fetchall():
                context = dict(row[8] or {})
                events.append({
                    "id": row[0],
                    "migration_run_id": str(row[1]),
                    "step_key": row[2],
                    "shard_id": str(row[3]) if row[3] else None,
                    "phase": row[4],
                    "severity": row[5],
                    "code": row[6],
                    "message": row[7],
                    "context": context,
                    "owner_emails": list(context.get("owner_emails") or []),
                    "created_at": row[9],
                })
            return events
    finally:
        conn.close()
