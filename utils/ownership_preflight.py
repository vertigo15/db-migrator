"""Preflight checks for canonical V5 entity ownership."""
from __future__ import annotations

from typing import Iterable, Mapping, Optional

from utils.db import ConnectionConfig, execute_query, get_connection
from utils.migration_tracking import config_for_database
from utils.sql_generator import USER_NAMESPACE_UUID, deterministic_uuid_v4_py


_ENTITY_TABLES = {
    "folders": "folders",
    "documents": "documents",
}
_PREFLIGHT_BATCH_SIZE = 5000


def _chunks(rows: list[dict], size: int) -> Iterable[list[dict]]:
    for index in range(0, len(rows), size):
        yield rows[index:index + size]


def _expected_user_id(
    owner_id: str,
    user_id_overrides: Mapping[str, str],
) -> str:
    override = user_id_overrides.get(str(owner_id))
    if override:
        return str(override)
    return str(deterministic_uuid_v4_py(USER_NAMESPACE_UUID, str(owner_id)))


def find_canonical_ownership_conflicts(
    base_target_config: ConnectionConfig,
    ownership_manifest: Mapping[str, Iterable[Mapping[str, object]]],
    user_id_overrides: Optional[Mapping[str, str]] = None,
) -> list[dict]:
    """Find live canonical mappings owned by a different V5 user.

    This is read-only and runs after source extraction but before shards are
    queued, turning a deterministic worker failure into an extraction failure.
    """
    overrides = {
        str(old_id): str(new_id)
        for old_id, new_id in (user_id_overrides or {}).items()
    }
    document_config = config_for_database(base_target_config, "document_db")
    conflicts = []
    for mapping_table, target_table in _ENTITY_TABLES.items():
        rows = [
            {
                "old_id": str(row.get("old_id") or "").strip(),
                "owner_id": str(row.get("owner_id") or "").strip(),
            }
            for row in ownership_manifest.get(mapping_table, [])
            if row.get("old_id") and row.get("owner_id")
        ]
        for batch in _chunks(rows, _PREFLIGHT_BATCH_SIZE):
            old_ids = [row["old_id"] for row in batch]
            expected_owner_ids = [
                _expected_user_id(row["owner_id"], overrides)
                for row in batch
            ]
            frame = execute_query(
                document_config,
                f"""
                WITH requested AS (
                    SELECT *
                    FROM unnest(%s::text[], %s::uuid[])
                        AS requested(old_id, expected_owner_id)
                )
                SELECT requested.old_id,
                       mappings.new_id::text AS mapped_entity_id,
                       mappings.migration_run_id::text AS mapping_owner_run,
                       mappings.record_action,
                       target.user_id::text AS actual_owner_id,
                       requested.expected_owner_id::text AS expected_owner_id
                FROM requested
                JOIN migration.id_mappings mappings
                  ON mappings.table_name = %s
                 AND mappings.old_id = requested.old_id
                JOIN public.{target_table} target
                  ON target.id = mappings.new_id
                WHERE target.user_id IS DISTINCT FROM requested.expected_owner_id
                ORDER BY requested.old_id
                """,
                (old_ids, expected_owner_ids, mapping_table),
            )
            for row in frame.to_dict("records"):
                conflicts.append(
                    {
                        "entity_type": mapping_table,
                        "old_id": str(row["old_id"]),
                        "mapped_entity_id": str(row["mapped_entity_id"]),
                        "mapping_owner_run": (
                            str(row["mapping_owner_run"])
                            if row.get("mapping_owner_run")
                            else None
                        ),
                        "record_action": row.get("record_action"),
                        "actual_owner_id": str(row["actual_owner_id"]),
                        "expected_owner_id": str(row["expected_owner_id"]),
                    }
                )
    return conflicts


def ownership_conflict_message(conflicts: Iterable[Mapping[str, object]]) -> str:
    rows = list(conflicts)
    sample = ", ".join(
        f"{row['entity_type']}:{row['old_id']} "
        f"(older run {row.get('mapping_owner_run') or 'unknown'})"
        for row in rows[:10]
    )
    suffix = f"; first conflicts: {sample}" if sample else ""
    return (
        f"Owned-data preflight found {len(rows)} existing V5 mapping(s) whose "
        "target entity belongs to a different user. These are usually shared "
        "copies created by an older reassign migration. The batch was stopped "
        f"before any migration shards ran{suffix}."
    )


def repair_orphaned_document_owners(
    base_target_config: ConnectionConfig,
    conflicts: Iterable[Mapping[str, object]],
    migration_run_id: Optional[str] = None,
) -> list[dict]:
    """Repair only documents whose current owner no longer exists in user_db.

    The expected owner must exist, and the update is guarded by both the mapped
    document UUID and the observed orphan owner UUID.
    """
    return _repair_orphaned_entity_owners(
        base_target_config,
        conflicts,
        entity_type="documents",
        target_table="documents",
        migration_run_id=migration_run_id,
    )


def repair_orphaned_folder_owners(
    base_target_config: ConnectionConfig,
    conflicts: Iterable[Mapping[str, object]],
    migration_run_id: Optional[str] = None,
) -> list[dict]:
    """Repair canonical folders only when their current V5 owner is orphaned."""
    return _repair_orphaned_entity_owners(
        base_target_config,
        conflicts,
        entity_type="folders",
        target_table="folders",
        migration_run_id=migration_run_id,
    )


def active_owner_folder_conflicts(
    base_target_config: ConnectionConfig,
    conflicts: Iterable[Mapping[str, object]],
) -> list[dict]:
    """Return folder conflicts whose current owner is a live V5 user."""
    folder_conflicts = [
        dict(row)
        for row in conflicts
        if row.get("entity_type") == "folders"
    ]
    if not folder_conflicts:
        return []
    actual_owner_ids = sorted({
        str(row["actual_owner_id"])
        for row in folder_conflicts
        if row.get("actual_owner_id")
    })
    user_config = config_for_database(base_target_config, "user_db")
    existing_frame = execute_query(
        user_config,
        """
        SELECT id::text
        FROM public.users
        WHERE id = ANY(%s::uuid[])
        """,
        (actual_owner_ids,),
    )
    existing_user_ids = {
        str(value) for value in existing_frame.get("id", [])
    }
    return [
        row
        for row in folder_conflicts
        if str(row["actual_owner_id"]) in existing_user_ids
    ]


def _repair_orphaned_entity_owners(
    base_target_config: ConnectionConfig,
    conflicts: Iterable[Mapping[str, object]],
    *,
    entity_type: str,
    target_table: str,
    migration_run_id: Optional[str],
) -> list[dict]:
    entity_conflicts = [
        dict(row)
        for row in conflicts
        if row.get("entity_type") == entity_type
    ]
    if not entity_conflicts:
        return []

    relevant_user_ids = sorted({
        str(row[key])
        for row in entity_conflicts
        for key in ("actual_owner_id", "expected_owner_id")
        if row.get(key)
    })
    user_config = config_for_database(base_target_config, "user_db")
    existing_frame = execute_query(
        user_config,
        """
        SELECT id::text
        FROM public.users
        WHERE id = ANY(%s::uuid[])
        """,
        (relevant_user_ids,),
    )
    existing_user_ids = {
        str(value) for value in existing_frame.get("id", [])
    }
    proven_expected_ids = set(existing_user_ids)
    if migration_run_id:
        planned_frame = execute_query(
            user_config,
            """
            SELECT v5_user_id::text AS id
            FROM migration.migration_run_users
            WHERE migration_run_id = %s::uuid
              AND v5_user_id = ANY(%s::uuid[])
            """,
            (
                migration_run_id,
                [
                    str(row["expected_owner_id"])
                    for row in entity_conflicts
                ],
            ),
        )
        proven_expected_ids.update(
            str(value) for value in planned_frame.get("id", [])
        )
    eligible = [
        row
        for row in entity_conflicts
        if str(row["actual_owner_id"]) not in existing_user_ids
        and str(row["expected_owner_id"]) in proven_expected_ids
    ]
    if not eligible:
        return []

    document_config = config_for_database(
        base_target_config, "document_db"
    )
    conn = get_connection(document_config)
    repaired = []
    dependency_guard_sql = ""
    if entity_type == "folders":
        dependency_guard_sql = f"""
                      AND NOT EXISTS (
                          SELECT 1 FROM public.documents dependency
                          WHERE dependency.folder_id = public.{target_table}.id
                            AND dependency.user_id IS DISTINCT FROM %s::uuid
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM public.folders dependency
                          WHERE dependency.parent_id = public.{target_table}.id
                            AND dependency.user_id IS DISTINCT FROM %s::uuid
                      )
"""
    try:
        conn.autocommit = False
        with conn.cursor() as cursor:
            for row in eligible:
                cursor.execute(
                    f"""
                    UPDATE public.{target_table}
                    SET user_id = %s::uuid
                    WHERE id = %s::uuid
                      AND user_id = %s::uuid
                      AND EXISTS (
                          SELECT 1
                          FROM migration.id_mappings mapping
                          WHERE mapping.table_name = %s
                            AND mapping.old_id = %s
                            AND mapping.new_id = public.{target_table}.id
                      )
{dependency_guard_sql}
                    """,
                    tuple([
                        row["expected_owner_id"],
                        row["mapped_entity_id"],
                        row["actual_owner_id"],
                        entity_type,
                        row["old_id"],
                        *(
                            [
                                row["expected_owner_id"],
                                row["expected_owner_id"],
                            ]
                            if entity_type == "folders"
                            else []
                        ),
                    ]),
                )
                if cursor.rowcount == 1:
                    repaired.append(row)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return repaired
