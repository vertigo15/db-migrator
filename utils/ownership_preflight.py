"""Preflight checks for canonical V5 entity ownership."""
from __future__ import annotations

from typing import Iterable, Mapping, Optional

from utils.db import ConnectionConfig, execute_query
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
