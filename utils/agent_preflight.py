"""Batch-wide preflight for deterministic V5 agent collisions."""
from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Mapping, Optional

import pandas as pd

from utils.db import ConnectionConfig, execute_query
from utils.migration_tracking import config_for_database
from utils.sql_generator import (
    NAMESPACE_UUID,
    USER_NAMESPACE_UUID,
    deterministic_uuid_v4_py,
)


def build_agent_manifest(
    agents_df: pd.DataFrame,
    user_id_overrides: Optional[Mapping[str, str]] = None,
) -> list[dict]:
    """Describe the deterministic primary/helper IDs planned for each bot."""
    overrides = {
        str(old_id): str(new_id)
        for old_id, new_id in (user_id_overrides or {}).items()
    }
    manifest = []
    for _, row in agents_df.iterrows():
        bot_id = str(row.get("bot_id") or "").strip()
        legacy_owner_id = str(row.get("user_id") or "").strip()
        if not bot_id or not legacy_owner_id:
            continue
        expected_owner_id = overrides.get(legacy_owner_id) or str(
            deterministic_uuid_v4_py(USER_NAMESPACE_UUID, legacy_owner_id)
        )
        documents = _parse_values(row.get("docs_chosen"))
        folders = [
            folder_id
            for folder_id in (
                _normalize_folder_id(value)
                for value in _parse_values(row.get("chosen_docs_folders"))
            )
            if folder_id
        ]
        has_knowledge_base = bool(documents or folders)
        manifest.append({
            "bot_id": bot_id,
            "legacy_owner_id": legacy_owner_id,
            "expected_owner_id": expected_owner_id,
            "agent_id": _deterministic(f"{bot_id}-agent"),
            "settings_id": _deterministic(f"{bot_id}-settings"),
            "has_knowledge_base": has_knowledge_base,
            "knowledge_base_id": _deterministic(f"{bot_id}-kb"),
            "assignment_id": _deterministic(f"{bot_id}-kb-assignment"),
            "knowledge_base_item_ids": sorted([
                *[
                    _deterministic(f"{bot_id}-kb-item-{document_id}")
                    for document_id in documents
                ],
                *[
                    _deterministic(
                        f"{bot_id}-kb-item-folder-{folder_id}"
                    )
                    for folder_id in folders
                ],
            ]),
        })
    return manifest


def inspect_agent_conflicts(
    base_target_config: ConnectionConfig,
    manifest: Iterable[Mapping[str, object]],
    collision_policy: str = "adopt_exact",
) -> dict:
    """Classify all planned agents before shards are allowed to queue."""
    planned = [dict(row) for row in manifest]
    result = {"will_adopt": [], "conflicts": []}
    if not planned:
        return result

    config = config_for_database(base_target_config, "completion_db")
    bot_ids = [str(row["bot_id"]) for row in planned]
    agent_ids = [str(row["agent_id"]) for row in planned]
    settings_ids = [str(row["settings_id"]) for row in planned]
    kb_ids = [str(row["knowledge_base_id"]) for row in planned]
    assignment_ids = [str(row["assignment_id"]) for row in planned]

    mappings = execute_query(
        config,
        """
        SELECT old_id, new_id::text, migration_run_id::text AS mapping_owner_run
        FROM migration.id_mappings
        WHERE table_name = 'agents'
          AND (old_id = ANY(%s::text[]) OR new_id = ANY(%s::uuid[]))
        """,
        (bot_ids, agent_ids),
    )
    agents = execute_query(
        config,
        "SELECT id::text, user_id::text FROM agents WHERE id = ANY(%s::uuid[])",
        (agent_ids,),
    )
    settings = execute_query(
        config,
        """
        SELECT id::text, agent_id::text
        FROM agent_settings
        WHERE id = ANY(%s::uuid[]) OR agent_id = ANY(%s::uuid[])
        """,
        (settings_ids, agent_ids),
    )
    knowledge_bases = execute_query(
        config,
        "SELECT id::text FROM knowledge_bases WHERE id = ANY(%s::uuid[])",
        (kb_ids,),
    )
    assignments = execute_query(
        config,
        """
        SELECT id::text, knowledge_base_id::text, assigned_to_id::text
        FROM knowledge_base_assignments
        WHERE id = ANY(%s::uuid[]) OR assigned_to_id = ANY(%s::uuid[])
        """,
        (assignment_ids, agent_ids),
    )
    items = execute_query(
        config,
        """
        SELECT id::text, knowledge_base_id::text
        FROM knowledge_base_items
        WHERE knowledge_base_id = ANY(%s::uuid[])
        """,
        (kb_ids,),
    )

    mapping_by_old = {
        str(row["old_id"]): row for row in mappings.to_dict("records")
    }
    mapping_by_new = {
        str(row["new_id"]): row for row in mappings.to_dict("records")
    }
    agents_by_id = {
        str(row["id"]): row for row in agents.to_dict("records")
    }
    settings_by_agent = _ids_by_parent(settings, "agent_id")
    settings_by_id = {
        str(row["id"]): row for row in settings.to_dict("records")
    }
    kb_ids_present = {
        str(value) for value in knowledge_bases.get("id", [])
    }
    assignments_by_agent = _ids_by_parent(
        assignments, "assigned_to_id"
    )
    assignments_by_id = {
        str(row["id"]): row for row in assignments.to_dict("records")
    }
    items_by_kb = _ids_by_parent(items, "knowledge_base_id")

    for row in planned:
        bot_id = str(row["bot_id"])
        agent_id = str(row["agent_id"])
        existing = agents_by_id.get(agent_id)
        mapping = mapping_by_old.get(bot_id) or mapping_by_new.get(agent_id)
        if mapping:
            if (
                str(mapping["old_id"]) != bot_id
                or str(mapping["new_id"]) != agent_id
            ):
                result["conflicts"].append(_detail(
                    row, "canonical_mapping_mismatch", existing, mapping
                ))
            elif (
                not existing
                or str(existing.get("user_id"))
                != str(row["expected_owner_id"])
            ):
                result["conflicts"].append(_detail(
                    row, "canonical_owner_mismatch", existing, mapping
                ))
            continue

        expected_settings = str(row["settings_id"])
        expected_kb = str(row["knowledge_base_id"])
        expected_assignment = str(row["assignment_id"])
        expected_items = {
            str(value)
            for value in row.get("knowledge_base_item_ids", [])
        }
        settings_exact = (
            set(settings_by_agent[agent_id]) == {expected_settings}
            and str(settings_by_id.get(expected_settings, {}).get("agent_id"))
            == agent_id
        )
        if row.get("has_knowledge_base"):
            assignment = assignments_by_id.get(expected_assignment, {})
            helpers_exact = (
                expected_kb in kb_ids_present
                and set(assignments_by_agent[agent_id])
                == {expected_assignment}
                and str(assignment.get("knowledge_base_id")) == expected_kb
                and str(assignment.get("assigned_to_id")) == agent_id
                and set(items_by_kb[expected_kb]) == expected_items
            )
        else:
            helpers_exact = not assignments_by_agent[agent_id]

        exact = (
            existing is not None
            and str(existing.get("user_id"))
            == str(row["expected_owner_id"])
            and settings_exact
            and helpers_exact
        )
        helper_collision = (
            expected_settings in settings_by_id
            or bool(settings_by_agent[agent_id])
            or bool(assignments_by_agent[agent_id])
            or (
                row.get("has_knowledge_base")
                and (
                    expected_kb in kb_ids_present
                    or expected_assignment in assignments_by_id
                    or bool(items_by_kb[expected_kb])
                )
            )
        )
        if existing is None and not helper_collision:
            continue
        detail = _detail(
            row,
            "unmapped_exact_copy" if exact else "unmapped_content_mismatch",
            existing,
            None,
        )
        if exact and collision_policy == "adopt_exact":
            result["will_adopt"].append(detail)
        else:
            result["conflicts"].append(detail)
    return result


def agent_conflict_message(conflicts: Iterable[Mapping[str, object]]) -> str:
    rows = list(conflicts)
    sample = ", ".join(str(row["bot_id"]) for row in rows[:10])
    return (
        f"Agent preflight found {len(rows)} unresolved deterministic V5 "
        f"collision(s). First conflicts: {sample}. The batch was stopped "
        "before any migration shards ran."
    )


def _deterministic(value: str) -> str:
    return str(deterministic_uuid_v4_py(NAMESPACE_UUID, value))


def _parse_values(value: object) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip().strip("{}")
    if not text:
        return []
    return [
        item.strip().strip('"')
        for item in text.split(",")
        if item.strip().strip('"')
    ]


def _normalize_folder_id(value: object) -> Optional[str]:
    try:
        return str(int(float(str(value).strip())))
    except (ValueError, TypeError):
        text = str(value or "").strip()
        return text or None


def _ids_by_parent(
    frame: pd.DataFrame,
    parent_column: str,
) -> defaultdict[str, list[str]]:
    values: defaultdict[str, list[str]] = defaultdict(list)
    for row in frame.to_dict("records"):
        values[str(row[parent_column])].append(str(row["id"]))
    return values


def _detail(
    planned: Mapping[str, object],
    reason: str,
    existing: Optional[Mapping[str, object]],
    mapping: Optional[Mapping[str, object]],
) -> dict:
    return {
        "bot_id": str(planned["bot_id"]),
        "agent_id": str(planned["agent_id"]),
        "reason": reason,
        "expected_owner_id": str(planned["expected_owner_id"]),
        "actual_owner_id": (
            str(existing.get("user_id")) if existing else None
        ),
        "mapping_owner_run": (
            str(mapping.get("mapping_owner_run"))
            if mapping and mapping.get("mapping_owner_run")
            else None
        ),
    }
