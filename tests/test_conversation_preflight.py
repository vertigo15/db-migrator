import pandas as pd

from utils.conversation_preflight import (
    build_conversation_manifest,
    inspect_conversation_conflicts,
)
from utils.db import ConnectionConfig
from utils.sql_generator import (
    CONVERSATION_MESSAGES_NAMESPACE_UUID,
    deterministic_uuid_v4_py,
)


TARGET = ConnectionConfig("target", 5432, "user_db", "user", "password")
OWNER_ID = "22222222-2222-4222-8222-222222222222"


def test_preflight_reports_all_adoptable_and_mismatched_conversations(
    monkeypatch,
):
    exact_chat = "11111111-1111-4111-8111-111111111111"
    mismatch_chat = "33333333-3333-4333-8333-333333333333"
    logs = pd.DataFrame([
        {
            "id": "exact-log",
            "user_id": "legacy-user",
            "chat_id": exact_chat,
        },
        {
            "id": "mismatch-log",
            "user_id": "legacy-user",
            "chat_id": mismatch_chat,
        },
    ])
    manifest = build_conversation_manifest(
        logs, {"legacy-user": OWNER_ID}
    )
    by_chat = {row["chat_id"]: row for row in manifest}

    def fake_query(_config, query, _params=None):
        if "FROM migration.id_mappings" in query:
            return pd.DataFrame(columns=[
                "old_id", "new_id", "mapping_owner_run"
            ])
        if "FROM public.conversations" in query:
            return pd.DataFrame([
                {"id": exact_chat, "user_id": OWNER_ID},
                {"id": mismatch_chat, "user_id": OWNER_ID},
            ])
        if "FROM public.messages" in query:
            rows = [
                {"id": message_id, "conversation_id": exact_chat}
                for message_id in by_chat[exact_chat]["message_ids"]
            ]
            rows.append({
                "id": "44444444-4444-4444-8444-444444444444",
                "conversation_id": mismatch_chat,
            })
            return pd.DataFrame(rows)
        if "FROM public.message_content_blocks" in query:
            return pd.DataFrame([
                {"id": block_id, "conversation_id": exact_chat}
                for block_id in by_chat[exact_chat]["block_ids"]
            ])
        raise AssertionError(query)

    monkeypatch.setattr(
        "utils.conversation_preflight.execute_query", fake_query
    )

    adoption = inspect_conversation_conflicts(
        TARGET, manifest, "adopt_exact"
    )
    assert [row["chat_id"] for row in adoption["will_adopt"]] == [
        exact_chat
    ]
    assert [row["chat_id"] for row in adoption["conflicts"]] == [
        mismatch_chat
    ]

    replacement = inspect_conversation_conflicts(
        TARGET, manifest, "replace_unmapped"
    )
    assert {row["chat_id"] for row in replacement["will_replace"]} == {
        exact_chat,
        mismatch_chat,
    }
    assert replacement["conflicts"] == []


def test_manifest_preserves_non_uuid_legacy_chat_id():
    legacy_chat_id = "agent-chat-legacy"
    manifest = build_conversation_manifest(
        pd.DataFrame([{
            "id": "legacy-log",
            "user_id": "legacy-user",
            "chat_id": legacy_chat_id,
        }]),
        {"legacy-user": OWNER_ID},
    )

    assert manifest == [{
        "chat_id": str(deterministic_uuid_v4_py(
            CONVERSATION_MESSAGES_NAMESPACE_UUID,
            f"conversation-{legacy_chat_id}",
        )),
        "legacy_chat_id": legacy_chat_id,
        "legacy_owner_id": "legacy-user",
        "expected_owner_id": OWNER_ID,
        "message_ids": manifest[0]["message_ids"],
        "block_ids": manifest[0]["block_ids"],
    }]
