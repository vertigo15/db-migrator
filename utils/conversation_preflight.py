"""Batch-wide preflight for existing V5 conversation UUIDs."""
from __future__ import annotations

import uuid
from collections import defaultdict
from typing import Iterable, Mapping, Optional

import pandas as pd

from utils.db import ConnectionConfig, execute_query
from utils.migration_tracking import config_for_database
from utils.sql_generator import (
    CONVERSATION_MESSAGES_NAMESPACE_UUID,
    USER_NAMESPACE_UUID,
    deterministic_uuid_v4_py,
)


def build_conversation_manifest(
    logs_df: pd.DataFrame,
    user_id_overrides: Optional[Mapping[str, str]] = None,
) -> list[dict]:
    """Describe the owner and deterministic child IDs for planned chats."""
    overrides = {
        str(old_id): str(new_id)
        for old_id, new_id in (user_id_overrides or {}).items()
    }
    manifest = []
    if logs_df.empty:
        return manifest

    normalized = logs_df.copy()
    normalized["legacy_chat_id"] = normalized["chat_id"].apply(
        _normalize_legacy_chat_id
    )
    normalized["chat_id"] = normalized["legacy_chat_id"].apply(
        _target_chat_uuid
    )
    normalized = normalized[
        normalized["chat_id"].notna() & normalized["user_id"].notna()
    ]
    for chat_id, rows in normalized.groupby("chat_id"):
        legacy_chat_ids = {
            str(value) for value in rows["legacy_chat_id"]
        }
        if len(legacy_chat_ids) != 1:
            continue
        legacy_chat_id = legacy_chat_ids.pop()
        owners = {str(value) for value in rows["user_id"]}
        if len(owners) != 1:
            continue
        legacy_owner_id = owners.pop()
        expected_owner_id = overrides.get(legacy_owner_id) or str(
            deterministic_uuid_v4_py(USER_NAMESPACE_UUID, legacy_owner_id)
        )
        message_ids = []
        block_ids = []
        for legacy_log_id in rows["id"]:
            for role in ("user", "assistant"):
                message_ids.append(str(deterministic_uuid_v4_py(
                    CONVERSATION_MESSAGES_NAMESPACE_UUID,
                    f"{legacy_log_id}-{role}",
                )))
                block_ids.append(str(deterministic_uuid_v4_py(
                    CONVERSATION_MESSAGES_NAMESPACE_UUID,
                    f"{legacy_log_id}-{role}-block-0",
                )))
        manifest.append({
            "chat_id": str(chat_id),
            "legacy_chat_id": legacy_chat_id,
            "legacy_owner_id": legacy_owner_id,
            "expected_owner_id": expected_owner_id,
            "message_ids": sorted(message_ids),
            "block_ids": sorted(block_ids),
        })
    return manifest


def inspect_conversation_conflicts(
    base_target_config: ConnectionConfig,
    manifest: Iterable[Mapping[str, object]],
    collision_policy: str,
) -> dict:
    """Inspect all planned chat UUIDs and classify them before queueing."""
    planned = [dict(row) for row in manifest]
    result = {"will_adopt": [], "will_replace": [], "conflicts": []}
    if not planned:
        return result

    completion_config = config_for_database(
        base_target_config, "completion_db"
    )
    chat_ids = [str(row["chat_id"]) for row in planned]
    legacy_chat_ids = [
        str(row.get("legacy_chat_id") or row["chat_id"])
        for row in planned
    ]
    mappings = execute_query(
        completion_config,
        """
        SELECT old_id, new_id::text, migration_run_id::text AS mapping_owner_run
        FROM migration.id_mappings
        WHERE table_name = 'conversations'
          AND (old_id = ANY(%s::text[]) OR new_id = ANY(%s::uuid[]))
        """,
        (legacy_chat_ids, chat_ids),
    )
    conversations = execute_query(
        completion_config,
        """
        SELECT id::text, user_id::text
        FROM public.conversations
        WHERE id = ANY(%s::uuid[])
        """,
        (chat_ids,),
    )
    messages = execute_query(
        completion_config,
        """
        SELECT id::text, conversation_id::text
        FROM public.messages
        WHERE conversation_id = ANY(%s::uuid[])
        """,
        (chat_ids,),
    )
    blocks = execute_query(
        completion_config,
        """
        SELECT block.id::text, message.conversation_id::text
        FROM public.message_content_blocks block
        JOIN public.messages message ON message.id = block.message_id
        WHERE message.conversation_id = ANY(%s::uuid[])
        """,
        (chat_ids,),
    )

    mapping_by_old = {
        str(row["old_id"]): row for row in mappings.to_dict("records")
    }
    mapping_by_new = {
        str(row["new_id"]): row for row in mappings.to_dict("records")
    }
    conversation_by_id = {
        str(row["id"]): row for row in conversations.to_dict("records")
    }
    message_ids_by_chat = _ids_by_chat(messages)
    block_ids_by_chat = _ids_by_chat(blocks)

    for row in planned:
        chat_id = str(row["chat_id"])
        legacy_chat_id = str(row.get("legacy_chat_id") or chat_id)
        expected_owner_id = str(row["expected_owner_id"])
        mapping = (
            mapping_by_old.get(legacy_chat_id)
            or mapping_by_new.get(chat_id)
        )
        existing = conversation_by_id.get(chat_id)
        if mapping:
            if (
                str(mapping["old_id"]) != legacy_chat_id
                or str(mapping["new_id"]) != chat_id
            ):
                result["conflicts"].append(_conflict(
                    row, "canonical_mapping_mismatch", existing, mapping
                ))
            elif not existing or str(existing.get("user_id")) != expected_owner_id:
                result["conflicts"].append(_conflict(
                    row, "canonical_owner_mismatch", existing, mapping
                ))
            continue
        if not existing:
            continue

        exact = (
            str(existing.get("user_id")) == expected_owner_id
            and set(message_ids_by_chat[chat_id])
            == {str(value) for value in row.get("message_ids", [])}
            and set(block_ids_by_chat[chat_id])
            == {str(value) for value in row.get("block_ids", [])}
        )
        detail = _conflict(
            row,
            "unmapped_exact_copy" if exact else "unmapped_content_mismatch",
            existing,
            None,
        )
        if collision_policy == "replace_unmapped":
            result["will_replace"].append(detail)
        elif collision_policy == "adopt_exact" and exact:
            result["will_adopt"].append(detail)
        else:
            result["conflicts"].append(detail)
    return result


def conversation_conflict_message(conflicts: Iterable[Mapping[str, object]]) -> str:
    rows = list(conflicts)
    sample = ", ".join(str(row["chat_id"]) for row in rows[:10])
    return (
        f"Conversation preflight found {len(rows)} unresolved V5 UUID "
        f"collision(s). First conflicts: {sample}. The batch was stopped before "
        "any migration shards ran."
    )


def _normalize_legacy_chat_id(value: object) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip().lower()
    return text or None


def _target_chat_uuid(value: object) -> Optional[str]:
    legacy_chat_id = _normalize_legacy_chat_id(value)
    if not legacy_chat_id:
        return None
    try:
        return str(uuid.UUID(legacy_chat_id))
    except ValueError:
        return str(deterministic_uuid_v4_py(
            CONVERSATION_MESSAGES_NAMESPACE_UUID,
            f"conversation-{legacy_chat_id}",
        ))


def _ids_by_chat(frame: pd.DataFrame) -> defaultdict[str, list[str]]:
    values: defaultdict[str, list[str]] = defaultdict(list)
    for row in frame.to_dict("records"):
        values[str(row["conversation_id"])].append(str(row["id"]))
    return values


def _conflict(
    planned: Mapping[str, object],
    reason: str,
    existing: Optional[Mapping[str, object]],
    mapping: Optional[Mapping[str, object]],
) -> dict:
    return {
        "chat_id": str(planned["chat_id"]),
        "legacy_chat_id": str(
            planned.get("legacy_chat_id") or planned["chat_id"]
        ),
        "reason": reason,
        "expected_owner_id": str(planned["expected_owner_id"]),
        "actual_owner_id": (
            str(existing.get("user_id")) if existing else None
        ),
        "expected_message_count": len(planned.get("message_ids", [])),
        "expected_block_count": len(planned.get("block_ids", [])),
        "mapping_owner_run": (
            str(mapping.get("mapping_owner_run"))
            if mapping and mapping.get("mapping_owner_run")
            else None
        ),
    }
