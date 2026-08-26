import json
from unittest.mock import MagicMock

import pandas as pd
import pytest

from utils import queue_ui
from utils.db import ConnectionConfig
from utils.extraction import (
    ExtractionEngine,
    build_conversation_scope_cte,
    resolve_existing_user_overrides,
    validate_target_organization,
)
from utils.rollback import (
    _ensure_document_rollback_index,
    _pending_step_is_definitively_unexecuted,
)
from utils.step_verification import verify_step
from utils.sql_generator import (
    CONVERSATION_MESSAGES_NAMESPACE_UUID,
    NAMESPACE_UUID,
    USER_NAMESPACE_UUID,
    _topologically_sort_folders,
    deterministic_uuid_v4_py,
    generate_agent_insert,
    generate_chunks_embeddings_migration_sql,
    generate_conversations_logs_migration_sql,
    generate_documents_migration_sql,
    generate_folder_insert,
    generate_migration_schema_setup,
    generate_user_insert,
    truncate_embedding_vector,
)


SOURCE = ConnectionConfig("localhost", 5432, "source", "test", "test")
TARGET = ConnectionConfig("localhost", 5432, "user_db", "test", "test")


def test_document_rollback_creates_missing_processing_index(monkeypatch):
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(True,), (False,)]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    monkeypatch.setattr("utils.rollback.get_connection", lambda _config: conn)

    _ensure_document_rollback_index(TARGET)

    statements = [str(call.args[0]) for call in cursor.execute.call_args_list]
    assert any("SET statement_timeout = 0" in sql for sql in statements)
    assert any(
        "CREATE INDEX CONCURRENTLY idx_chunks_document_processing_id_rollback"
        in sql
        for sql in statements
    )
    assert conn.autocommit is True
    conn.close.assert_called_once()


def test_cancelled_never_started_pending_step_is_safe_to_skip():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        (2, 2),  # Both shards were cancelled without starting.
        (None,),  # Older schemas may not have step-entity tracking.
        (0,),  # No run-owned target mappings exist.
    ]

    assert _pending_step_is_definitively_unexecuted(
        cursor,
        "07_conversions",
        "11111111-1111-4111-8111-111111111111",
        {"mapping_table": "conversions"},
    )


def test_attempted_pending_step_is_not_safe_to_skip():
    cursor = MagicMock()
    cursor.fetchone.return_value = (2, 1)

    assert not _pending_step_is_definitively_unexecuted(
        cursor,
        "07_conversions",
        "11111111-1111-4111-8111-111111111111",
        {"mapping_table": "conversions"},
    )


def test_pending_step_with_run_owned_mapping_is_not_safe_to_skip():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        (1, 1),
        (None,),
        (1,),
    ]

    assert not _pending_step_is_definitively_unexecuted(
        cursor,
        "07_conversions",
        "11111111-1111-4111-8111-111111111111",
        {"mapping_table": "conversions"},
    )


def test_enqueue_run_stages_downstream_steps_before_users(monkeypatch):
    order = []
    monkeypatch.setattr(
        queue_ui,
        "load_manifest_for_file",
        lambda _path: {"shards": [{"shard_index": 1}]},
    )
    monkeypatch.setattr(
        queue_ui,
        "enqueue_shards",
        lambda _base, _run, step, _manifest, owner_emails=None: (
            order.append(step) or 1
        ),
    )

    enqueued = queue_ui.enqueue_run(
        TARGET,
        "11111111-1111-4111-8111-111111111111",
        {
            "01_users_": "users.sql",
            "02_folders_": "folders.sql",
            "03_documents_": "documents.sql",
        },
    )

    assert order == ["03_documents", "02_folders", "01_users"]
    assert enqueued == {
        "03_documents": 1,
        "02_folders": 1,
        "01_users": 1,
    }


def test_schema_is_migrated_removes_mapping_when_target_row_is_missing():
    ddl = generate_migration_schema_setup()

    assert "EXECUTE format(" in ddl
    assert "SELECT EXISTS (SELECT 1 FROM public.%I WHERE id = $1)" in ddl
    assert "DELETE FROM migration.id_mappings" in ddl
    assert "AND new_id = v_new_id" in ddl


def test_generic_step_verification_accepts_created_and_reused_entities():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        (2, {}),
        (2,),
        (2, 1, 1, 0),
    ]

    affected, details = verify_step(
        cursor,
        "02_folders",
        "11111111-1111-4111-8111-111111111111",
    )

    assert affected == 2
    assert details == {
        "actual_mappings": 2,
        "created_entities": 1,
        "reused_entities": 1,
        "invalid_entities": 0,
    }
    verification_sql = str(cursor.execute.call_args_list[2].args[0])
    assert "migration.migration_step_entities" in verification_sql
    assert "LEFT JOIN public.folders" in verification_sql


def test_existing_users_are_resolved_by_normalized_email(monkeypatch):
    source_df = pd.DataFrame([
        {"legacy_user_id": "legacy-existing", "email": " Existing@Example.com "},
        {"legacy_user_id": "legacy-new", "email": "new@example.com"},
    ])
    target_df = pd.DataFrame([
        {
            "v5_user_id": "11111111-1111-4111-8111-111111111111",
            "email": "existing@example.com",
            "organization_id": "22222222-2222-4222-8222-222222222222",
        }
    ])

    monkeypatch.setattr(
        "utils.extraction.execute_query",
        lambda config, *_args, **_kwargs: (
            source_df.copy() if config.database == "source" else target_df.copy()
        ),
    )

    result = resolve_existing_user_overrides(
        SOURCE,
        TARGET,
        "jeen_dev",
        ["existing@example.com", "new@example.com"],
    )

    assert result["overrides"] == {
        "legacy-existing": "11111111-1111-4111-8111-111111111111",
        "legacy-new": str(
            deterministic_uuid_v4_py(USER_NAMESPACE_UUID, "legacy-new")
        ),
    }
    assert result["existing_overrides"] == {
        "legacy-existing": "11111111-1111-4111-8111-111111111111",
    }
    users = {row["legacy_user_id"]: row for row in result["users"]}
    assert users["legacy-existing"]["action"] == "reused"
    assert users["legacy-new"]["action"] == "created"
    assert users["legacy-new"]["v5_user_id"] == str(
        deterministic_uuid_v4_py(USER_NAMESPACE_UUID, "legacy-new")
    )


def test_user_sql_only_reuses_users_proven_to_exist(
    monkeypatch,
    tmp_path,
):
    planned_new_id = str(
        deterministic_uuid_v4_py(USER_NAMESPACE_UUID, "legacy-new")
    )
    captured = {}

    def fake_query(_config, query, _params=None):
        if "information_schema.columns" in query:
            return pd.DataFrame({"column_name": ["id", "email"]})
        return pd.DataFrame([
            {
                "id": "legacy-new",
                "email": "new@example.com",
                "name": "New",
            },
            {
                "id": "legacy-existing",
                "email": "existing@example.com",
                "name": "Existing",
            },
        ])

    def fake_generate(**kwargs):
        captured.update(kwargs)
        return {}

    monkeypatch.setattr("utils.extraction.execute_query", fake_query)
    monkeypatch.setattr(
        "utils.extraction.generate_users_migration_sql",
        fake_generate,
    )
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path / "extract"),
        generate_sql=True,
        export_csv=False,
        user_id_overrides={
            "legacy-new": planned_new_id,
            "legacy-existing": "11111111-1111-4111-8111-111111111111",
        },
        existing_user_overrides={
            "legacy-existing": "11111111-1111-4111-8111-111111111111",
        },
    )

    engine.extract_users(["new@example.com", "existing@example.com"])

    assert captured["user_id_overrides"] == {
        "legacy-existing": "11111111-1111-4111-8111-111111111111",
    }
    assert engine.user_id_overrides["legacy-new"] == planned_new_id
    default_engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path / "default"),
        generate_sql=False,
        export_csv=False,
        user_id_overrides={"legacy-new": planned_new_id},
    )
    assert default_engine.existing_user_overrides == {}


def test_normalized_email_ambiguity_fails_closed(monkeypatch):
    source_df = pd.DataFrame([
        {"legacy_user_id": "one", "email": "same@example.com"},
        {"legacy_user_id": "two", "email": " SAME@example.com "},
    ])
    monkeypatch.setattr(
        "utils.extraction.execute_query",
        lambda *_args, **_kwargs: source_df.copy(),
    )

    with pytest.raises(ValueError, match="Ambiguous V4 email"):
        resolve_existing_user_overrides(
            SOURCE, TARGET, "jeen_dev", ["same@example.com"]
        )


@pytest.mark.parametrize(
    ("manual_target_rows", "expected_error"),
    [
        ([], "missing V5 users"),
        (
            [{
                "v5_user_id": "11111111-1111-4111-8111-111111111111",
                "email": "somebody-else@example.com",
                "organization_id": None,
            }],
            "different email",
        ),
    ],
)
def test_manual_user_overrides_require_existing_matching_email(
    monkeypatch,
    manual_target_rows,
    expected_error,
):
    source_df = pd.DataFrame([{
        "legacy_user_id": "legacy-user",
        "email": "user@example.com",
    }])

    def fake_query(config, query, _params=None):
        if config.database == "source":
            return source_df.copy()
        if "WHERE id = ANY" in query:
            return pd.DataFrame(
                manual_target_rows,
                columns=["v5_user_id", "email", "organization_id"],
            )
        return pd.DataFrame(
            columns=["v5_user_id", "email", "organization_id"]
        )

    monkeypatch.setattr("utils.extraction.execute_query", fake_query)

    with pytest.raises(ValueError, match=expected_error):
        resolve_existing_user_overrides(
            SOURCE,
            TARGET,
            "jeen_dev",
            ["user@example.com"],
            manual_overrides={
                "legacy-user": "11111111-1111-4111-8111-111111111111"
            },
        )


def test_inactive_or_missing_organization_fails_closed(monkeypatch):
    monkeypatch.setattr(
        "utils.extraction.execute_query",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )
    with pytest.raises(ValueError, match="does not exist or is not active"):
        validate_target_organization(
            ConnectionConfig("localhost", 5432, "admin_db", "test", "test"),
            "22222222-2222-4222-8222-222222222222",
        )


def _agent_frame():
    return pd.DataFrame([
        {
            "bot_id": "bot-1",
            "user_id": "owner-1",
            "docs_chosen": ["stale-doc", "cross-doc"],
            "chosen_docs_folders": [98, 99],
            "folder_id": 99,
        }
    ])


def test_agent_stale_refs_drop_and_cross_owner_content_is_reassigned(monkeypatch, tmp_path):
    def fake_query(_config, query, _params=None):
        if "custom_documents" in query:
            return pd.DataFrame([
                {
                    "doc_id": "cross-doc",
                    "owner_id": "owner-2",
                    "created_at": None,
                    "doc_name_origin": "cross.txt",
                    "doc_title": "cross",
                    "doc_size": 1,
                    "folder_id": None,
                    "doc_description": None,
                    "doc_type": "txt",
                    "vector_methods": None,
                    "doc_summery": None,
                    "doc_summery_modified_by": None,
                    "doc_summery_modified_at": None,
                    "tags": [],
                    "embedding_model": None,
                    "blob_source": None,
                    "version": None,
                    "doc_checksum": None,
                    "data_integration_doc_metadata": None,
                }
            ])
        if "embeddings" in query:
            return pd.DataFrame(
                columns=["id", "external_id", "collection", "document", "metadata", "embeddings"]
            )
        if "folders" in query:
            return pd.DataFrame([
                {
                    "id": 99,
                    "folder_name": "cross",
                    "owner_id": "owner-2",
                    "parent_id": None,
                    "created_at": None,
                    "folder_type": "default",
                }
            ])
        raise AssertionError(query)

    monkeypatch.setattr("utils.extraction.execute_query", fake_query)
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path),
        generate_sql=False,
        export_csv=False,
        cross_owner_policy="reassign",
        include_chunkless_documents=True,
    )
    agents = _agent_frame()
    empty_docs = pd.DataFrame(columns=["doc_id", "owner_id"])
    empty_embeddings = pd.DataFrame(
        columns=["id", "external_id", "collection", "document", "metadata", "embeddings"]
    )
    empty_folders = pd.DataFrame(columns=["id", "owner_id"])

    updated_docs, _, updated_folders, report = engine._topup_agent_documents(
        agents,
        empty_docs,
        empty_embeddings,
        empty_folders,
        selected_user_ids=["owner-1"],
    )

    assert agents.iloc[0]["docs_chosen"] == ["cross-doc"]
    assert agents.iloc[0]["chosen_docs_folders"] == ["99"]
    assert str(agents.iloc[0]["folder_id"]) == "99"
    assert updated_docs.iloc[0]["owner_id"] == "owner-1"
    assert updated_folders.iloc[0]["owner_id"] == "owner-1"
    assert set(report["removed_doc_ids"]) == {"stale-doc"}
    assert set(report["removed_folder_ids"]) == {"98"}
    assert report["reassigned_doc_ids"] == ["cross-doc"]
    assert report["reassigned_folder_ids"] == ["99"]


def test_cross_owner_agent_dependency_is_dropped_by_default(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(
        "utils.extraction.execute_query",
        lambda _config, query, _params=None: (
            pd.DataFrame([{
                "doc_id": "cross-doc",
                "owner_id": "owner-2",
                "created_at": None,
                "doc_name_origin": "cross.txt",
                "doc_title": "cross",
                "doc_size": 1,
                "folder_id": None,
                "doc_description": None,
                "doc_type": "txt",
                "vector_methods": None,
                "doc_summery": None,
                "doc_summery_modified_by": None,
                "doc_summery_modified_at": None,
                "tags": [],
                "embedding_model": None,
                "blob_source": None,
                "version": None,
                "doc_checksum": None,
                "data_integration_doc_metadata": None,
            }])
            if "custom_documents" in query
            else pd.DataFrame()
        ),
    )
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path),
        generate_sql=False,
        export_csv=False,
        include_chunkless_documents=True,
    )
    agents = _agent_frame()
    agents.at[0, "chosen_docs_folders"] = []
    agents.at[0, "folder_id"] = None

    docs, _, _, report = engine._topup_agent_documents(
        agents,
        pd.DataFrame(columns=["doc_id", "owner_id"]),
        pd.DataFrame(columns=["metadata"]),
        pd.DataFrame(columns=["id", "owner_id"]),
        selected_user_ids=["owner-1"],
    )

    assert docs.empty
    assert agents.iloc[0]["docs_chosen"] == []
    assert report["dropped_cross_owner_doc_ids"] == ["cross-doc"]
    assert "cross-doc" in report["removed_doc_ids"]


def test_owned_only_strips_existing_cross_owner_agent_references(tmp_path):
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path),
        generate_sql=False,
        export_csv=False,
    )
    agents = pd.DataFrame([{
        "bot_id": "bot-1",
        "user_id": "owner-1",
        "docs_chosen": ["cross-doc"],
        "chosen_docs_folders": [99],
        "folder_id": 99,
    }])

    docs, embeddings, folders, report = engine._topup_agent_documents(
        agents,
        pd.DataFrame([{
            "doc_id": "cross-doc",
            "owner_id": "owner-2",
            "folder_id": 99,
        }]),
        pd.DataFrame(columns=["metadata"]),
        pd.DataFrame([{
            "id": 99,
            "owner_id": "owner-2",
            "parent_id": None,
        }]),
        selected_user_ids=["owner-1"],
    )

    assert docs.empty
    assert embeddings.empty
    assert folders.empty
    assert agents.iloc[0]["docs_chosen"] == []
    assert agents.iloc[0]["chosen_docs_folders"] == []
    assert pd.isna(agents.iloc[0]["folder_id"])
    assert report["dropped_cross_owner_doc_ids"] == ["cross-doc"]
    assert report["dropped_cross_owner_folder_ids"] == ["99"]


def test_agent_csv_is_written_after_owned_only_sanitization(
    monkeypatch,
    tmp_path,
):
    def fake_query(_config, query, _params=None):
        if "playground_bot_generator_config" in query:
            return pd.DataFrame([{
                "bot_id": "bot-1",
                "user_id": "owner-1",
                "docs_chosen": ["cross-doc"],
                "chosen_docs_folders": [],
                "folder_id": None,
            }])
        if "custom_documents" in query:
            return pd.DataFrame([{
                "doc_id": "cross-doc",
                "owner_id": "owner-2",
                "blob_source": None,
            }])
        raise AssertionError(query)

    monkeypatch.setattr("utils.extraction.execute_query", fake_query)
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path),
        generate_sql=False,
        export_csv=True,
        include_chunkless_documents=True,
    )

    agents, csv_path = engine.extract_agents(
        ["owner-1"],
        docs_df=pd.DataFrame(columns=["doc_id", "owner_id"]),
        folders_df=pd.DataFrame(columns=["id", "owner_id"]),
        embeddings_df=pd.DataFrame(columns=["metadata"]),
    )

    assert agents.iloc[0]["docs_chosen"] == []
    with open(csv_path, encoding="utf-8") as handle:
        assert "cross-doc" not in handle.read()


def test_chunkless_agent_document_is_excluded_and_reference_removed(
    monkeypatch, tmp_path
):
    def fake_query(_config, query, _params=None):
        if "custom_documents" in query:
            return pd.DataFrame([{
                "doc_id": "chunkless-doc",
                "owner_id": "owner-1",
                "created_at": None,
                "doc_name_origin": "empty.txt",
                "doc_title": "empty",
                "doc_size": 1,
                "folder_id": None,
                "doc_description": None,
                "doc_type": "txt",
                "vector_methods": None,
                "doc_summery": None,
                "doc_summery_modified_by": None,
                "doc_summery_modified_at": None,
                "tags": [],
                "embedding_model": None,
                "blob_source": None,
                "version": None,
                "doc_checksum": None,
                "data_integration_doc_metadata": None,
            }])
        return pd.DataFrame(
            columns=[
                "id", "external_id", "collection", "document",
                "metadata", "embeddings",
            ]
        )

    monkeypatch.setattr("utils.extraction.execute_query", fake_query)
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path),
        generate_sql=False,
        export_csv=False,
    )
    agents = pd.DataFrame([{
        "bot_id": "bot-1",
        "user_id": "owner-1",
        "docs_chosen": ["chunkless-doc"],
        "chosen_docs_folders": [],
        "folder_id": None,
    }])

    docs, embeddings, _, report = engine._topup_agent_documents(
        agents,
        pd.DataFrame(columns=["doc_id", "owner_id"]),
        pd.DataFrame(columns=["metadata"]),
        pd.DataFrame(columns=["id", "owner_id"]),
        selected_user_ids=["owner-1"],
    )

    assert docs.empty
    assert embeddings.empty
    assert agents.iloc[0]["docs_chosen"] == []
    assert report["chunkless_doc_ids"] == ["chunkless-doc"]
    assert report["removed_doc_ids"] == ["chunkless-doc"]


def test_sharepoint_agent_document_is_never_topped_up(monkeypatch, tmp_path):
    def fake_query(_config, query, _params=None):
        if "custom_documents" not in query:
            raise AssertionError("SharePoint exclusion should avoid chunk extraction")
        return pd.DataFrame([{
            "doc_id": "sharepoint-doc",
            "owner_id": "owner-1",
            "created_at": None,
            "doc_name_origin": "sharepoint.docx",
            "doc_title": "SharePoint",
            "doc_size": 1,
            "folder_id": None,
            "doc_description": None,
            "doc_type": "docx",
            "vector_methods": None,
            "doc_summery": None,
            "doc_summery_modified_by": None,
            "doc_summery_modified_at": None,
            "tags": [],
            "embedding_model": None,
            "blob_source": "application_sharepoint",
            "version": None,
            "doc_checksum": None,
            "data_integration_doc_metadata": {"site": "example"},
        }])

    monkeypatch.setattr("utils.extraction.execute_query", fake_query)
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path),
        generate_sql=False,
        export_csv=False,
        include_chunkless_documents=True,
    )
    agents = pd.DataFrame([{
        "bot_id": "bot-1",
        "user_id": "owner-1",
        "docs_chosen": ["sharepoint-doc"],
        "chosen_docs_folders": [],
        "folder_id": None,
    }])

    docs, embeddings, _, report = engine._topup_agent_documents(
        agents,
        pd.DataFrame(columns=["doc_id", "owner_id"]),
        pd.DataFrame(columns=["metadata"]),
        pd.DataFrame(columns=["id", "owner_id"]),
        selected_user_ids=["owner-1"],
    )

    assert docs.empty
    assert embeddings.empty
    assert agents.iloc[0]["docs_chosen"] == []
    assert report["excluded_sharepoint_doc_ids"] == ["sharepoint-doc"]
    assert report["stale_doc_ids"] == []
    assert report["removed_doc_ids"] == ["sharepoint-doc"]


def test_document_extraction_enforces_sharepoint_exclusion(monkeypatch, tmp_path):
    captured = {}

    def fake_query(_config, query, params=None):
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr("utils.extraction.execute_query", fake_query)
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path),
        generate_sql=False,
        export_csv=False,
    )

    engine.extract_documents(
        ["owner-1"],
        selected_doc_ids=["sharepoint-doc"],
    )

    assert "COALESCE(blob_source, '') <> %s" in captured["query"]
    assert "owner_id IN" in captured["query"]
    assert captured["params"][0] == "application_sharepoint"
    assert "sharepoint-doc" in captured["params"]
    assert "owner-1" in captured["params"]


def test_owned_document_is_detached_from_unmigrated_folder(tmp_path):
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path),
        generate_sql=False,
        export_csv=False,
    )

    documents, detached = engine._detach_unmigrated_document_folders(
        pd.DataFrame([{
            "doc_id": "owned-doc",
            "owner_id": "owner-1",
            "folder_id": 99,
        }]),
        pd.DataFrame([{
            "id": 1,
            "owner_id": "owner-1",
        }]),
    )

    assert pd.isna(documents.iloc[0]["folder_id"])
    assert detached == ["owned-doc"]


def test_full_extraction_excludes_selected_chunkless_document(
    monkeypatch, tmp_path
):
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path),
        generate_sql=False,
        export_csv=False,
    )
    documents = pd.DataFrame([
        {"doc_id": "chunked-doc", "owner_id": "owner-1"},
        {"doc_id": "chunkless-doc", "owner_id": "owner-1"},
    ])
    chunks = pd.DataFrame([{
        "id": "chunk-1",
        "external_id": "external-1",
        "collection": "test",
        "document": json.dumps({"page_content": "hello"}),
        "metadata": {"doc_id": "chunked-doc", "type": "chunk-data"},
        "embeddings": "[0.1,0.2]",
    }])
    captured = {}

    monkeypatch.setattr(
        engine,
        "extract_users",
        lambda _emails: (pd.DataFrame([{"id": "owner-1"}]), "users.csv"),
    )
    monkeypatch.setattr(
        engine,
        "extract_folders",
        lambda _user_ids: (pd.DataFrame(columns=["id"]), "folders.csv"),
    )
    monkeypatch.setattr(
        engine,
        "extract_documents",
        lambda *_args, **_kwargs: (documents.copy(), "documents.csv"),
    )
    monkeypatch.setattr(
        engine,
        "extract_embeddings",
        lambda *_args, **_kwargs: (chunks.copy(), "embeddings.csv"),
    )

    def fake_extract_agents(
        _user_ids,
        _selected_agent_ids=None,
        docs_df=None,
        folders_df=None,
        embeddings_df=None,
    ):
        captured["doc_ids"] = docs_df["doc_id"].astype(str).tolist()
        return pd.DataFrame([{
            "bot_id": "legacy-agent",
            "user_id": "owner-1",
            "docs_chosen": [],
            "chosen_docs_folders": [],
        }]), "agents.csv"

    monkeypatch.setattr(engine, "extract_agents", fake_extract_agents)

    def fake_extract_logs(*_args, **kwargs):
        captured["agent_id_by_bot_id"] = kwargs["agent_id_by_bot_id"]
        return pd.DataFrame([{
            "id": "legacy-log",
            "user_id": "owner-1",
            "chat_id": "agent-chat",
            "bot_id": "legacy-agent",
        }]), "logs.csv"

    monkeypatch.setattr(engine, "extract_logs", fake_extract_logs)

    results = engine.run_full_extraction(
        ["user@example.com"],
        extract_conversions=False,
    )

    assert results["errors"] == []
    assert results["summary"]["documents"] == 1
    assert results["document_filter_report"]["chunkless_doc_ids"] == [
        "chunkless-doc"
    ]
    assert captured["doc_ids"] == ["chunked-doc"]
    assert captured["agent_id_by_bot_id"] == {
        "legacy-agent": str(deterministic_uuid_v4_py(
            NAMESPACE_UUID, "legacy-agent-agent"
        ))
    }
    assert results["agent_conversation_report"]["unlinked_bot_ids"] == []


def test_documents_start_pending_and_step_four_reconciles_readiness(tmp_path):
    run_id = "33333333-3333-4333-8333-333333333333"
    documents = pd.DataFrame([{
        "doc_id": "doc-1",
        "owner_id": "legacy-user",
        "doc_name_origin": "doc.txt",
        "doc_title": "Doc",
        "doc_size": 10,
        "blob_source": None,
        "doc_type": "txt",
        "folder_id": None,
        "created_at": None,
        "tags": [],
        "vector_methods": None,
        "data_integration_doc_metadata": None,
    }])
    document_file = tmp_path / "03_documents.sql"
    generate_documents_migration_sql(
        documents,
        str(document_file),
        "test",
        migration_run_id=run_id,
    )
    document_sql = document_file.read_text()
    assert "'PENDING'::public.document_processing_status_enum" in document_sql
    assert "migration_run_id" in document_sql
    assert "record_action" in document_sql
    assert "mapped.user_id = v_user_id" in document_sql
    assert "Canonical document owner mismatch" in document_sql

    chunks = pd.DataFrame([{
        "id": "chunk-1",
        "external_id": "external-1",
        "collection": "test",
        "document": json.dumps({"page_content": "hello"}),
        "metadata": {"doc_id": "doc-1", "type": "chunk-data"},
        "embeddings": "[0.1,0.2]",
    }])
    chunk_file = tmp_path / "04_chunks.sql"
    generate_chunks_embeddings_migration_sql(
        chunks,
        str(chunk_file),
        "test",
        migration_run_id=run_id,
    )
    chunk_sql = chunk_file.read_text()
    assert "SET status = 'COMPLETED'" in chunk_sql
    assert "NOT EXISTS" in chunk_sql
    assert run_id in chunk_sql
    assert "SELECT id INTO v_document_processing_id" in chunk_sql
    assert "document_processing_id," in chunk_sql
    assert "v_document_processing_id," in chunk_sql
    assert "c.document_processing_id = dp.id" in chunk_sql
    assert "c.document_processing_id IS DISTINCT FROM dp.id" in chunk_sql
    assert "Missing run-scoped document tracking" in chunk_sql
    assert "'04_chunks_embeddings', 'chunks'" in chunk_sql
    assert "'04_chunks_embeddings', 'embeddings'" in chunk_sql
    assert "resume its owning run instead" in chunk_sql

    readiness = ExtractionEngine.evaluate_document_readiness(
        pd.DataFrame([{"doc_id": "doc-1"}, {"doc_id": "doc-without-chunks"}]),
        chunks,
    )
    assert readiness["ready_document_ids"] == ["doc-1"]
    assert readiness["documents_requiring_reprocessing"] == [
        "doc-without-chunks"
    ]


def test_user_sql_records_explicit_created_and_reused_actions():
    row = pd.Series({
        "id": "legacy-user",
        "email": "user@example.com",
        "name": "User",
    })
    run_id = "33333333-3333-4333-8333-333333333333"

    created_sql = generate_user_insert(
        row,
        migration_run_id=run_id,
    )
    reused_sql = generate_user_insert(
        row,
        user_id_overrides={
            "legacy-user": "11111111-1111-4111-8111-111111111111"
        },
        migration_run_id=run_id,
    )

    assert "v_action VARCHAR := 'created'" in created_sql
    assert "record_action" in created_sql
    assert "'reused'" in reused_sql
    assert "Resolved existing V5 user is missing" in reused_sql
    assert "migration.migration_step_entities" in created_sql
    assert "migration.migration_step_entities" in reused_sql
    assert "WHEN m.migration_run_id =" in reused_sql
    assert "ELSE 'reused'" in reused_sql
    assert "Canonical users mapping mismatch" in reused_sql
    assert "migration.migration_run_users" in created_sql
    assert "Runtime V5 user resolution changed" in created_sql
    assert "lower(trim(email)) = lower(trim(v_email))" in created_sql
    assert run_id in created_sql and run_id in reused_sql


def test_embedding_truncation_preserves_source_or_requires_zero_padding():
    assert truncate_embedding_vector("[1,2,0,0]", 2) == "[1,2]"
    with pytest.raises(ValueError, match="Refusing destructive"):
        truncate_embedding_vector("[1,2,0.5,0]", 2)


def test_extract_embeddings_all_mode_is_not_preview_capped(monkeypatch, tmp_path):
    rows = pd.DataFrame([
        {
            "id": f"chunk-{index}",
            "external_id": None,
            "collection": "test",
            "document": "content",
            "metadata": {"doc_id": "doc-1", "type": "chunk-data"},
            "embeddings": "[0.1,0.2]",
        }
        for index in range(5001)
    ])

    def fake_query_chunked(_config, query, params=None, chunk_size=5000):
        assert "LIMIT" not in query.upper()
        assert "metadata->>'type' = 'chunk-data'" in query
        assert tuple(params) == ("doc-1",)
        for start in range(0, len(rows), chunk_size):
            yield rows.iloc[start:start + chunk_size].copy()

    monkeypatch.setattr("utils.extraction.execute_query_chunked", fake_query_chunked)
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path),
        generate_sql=False,
        export_csv=False,
    )
    extracted, _ = engine.extract_embeddings(["doc-1"], None)
    assert len(extracted) == 5001


def test_extract_embeddings_rejects_partial_chunk_selection(tmp_path):
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path),
        generate_sql=False,
        export_csv=False,
    )
    with pytest.raises(ValueError, match="Partial chunk selection"):
        engine.extract_embeddings(["doc-1"], ["chunk-1"])


def test_folder_sort_is_topological_and_rejects_bad_graphs():
    folders = pd.DataFrame([
        {"id": 1, "parent_id": 2},
        {"id": 2, "parent_id": None},
        {"id": 3, "parent_id": 1},
    ])
    assert _topologically_sort_folders(folders)["id"].tolist() == [2, 1, 3]

    with pytest.raises(ValueError, match="missing parent"):
        _topologically_sort_folders(
            pd.DataFrame([{"id": 1, "parent_id": 99}])
        )
    with pytest.raises(ValueError, match="cycle"):
        _topologically_sort_folders(
            pd.DataFrame([
                {"id": 1, "parent_id": 2},
                {"id": 2, "parent_id": 1},
            ])
        )


def test_folder_insert_normalizes_float_loaded_ids_and_tracks_current_run():
    sql = generate_folder_insert(pd.Series({
        "id": 1106.0,
        "folder_name": "YAM2",
        "owner_id": "yam-owner",
        "parent_id": 1105.0,
        "folder_type": "default",
        "created_at": None,
    }), migration_run_id="11111111-1111-4111-8111-111111111111")

    assert "v_old_folder_id VARCHAR := '1106';" in sql
    assert (
        "v_parent_id uuid := migration.deterministic_uuid_v4("
        "'0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '1105');"
    ) in sql
    assert "1105.0" not in sql
    assert "migration.migration_step_entities" in sql
    assert "'02_folders', 'folders'" in sql
    assert "'reused'" in sql
    assert "'created'" in sql
    assert "mapped.user_id = v_user_id" in sql
    assert "Canonical folder owner mismatch" in sql
    assert "DELETE FROM migration.id_mappings" not in sql
    assert str(deterministic_uuid_v4_py(
        "0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b",
        "1105",
    )) == "3efe543f-49e4-4659-ac8f-641c879ddd7e"


def test_folder_ancestor_closure_fetches_and_reassigns_parent(monkeypatch, tmp_path):
    def fake_query(_config, query, params=None):
        assert "folders" in query
        assert tuple(params) == ("2",)
        return pd.DataFrame([{
            "id": 2,
            "folder_name": "parent",
            "owner_id": "other-owner",
            "parent_id": None,
            "created_at": None,
            "folder_type": "default",
        }])

    monkeypatch.setattr("utils.extraction.execute_query", fake_query)
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path),
        generate_sql=False,
        export_csv=False,
        cross_owner_policy="reassign",
    )
    result = engine._resolve_folder_ancestor_closure(
        pd.DataFrame([{
            "id": 1,
            "folder_name": "child",
            "owner_id": "selected-owner",
            "parent_id": 2,
            "created_at": None,
            "folder_type": "default",
        }]),
        ["selected-owner"],
    )
    assert set(result["id"].astype(str)) == {"1", "2"}
    assert result.loc[result["id"] == 2, "owner_id"].iloc[0] == "selected-owner"
    assert engine._folder_hierarchy_report["reassigned_ancestor_ids"] == ["2"]


def test_folder_ancestor_owned_by_other_user_is_detached_by_default(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(
        "utils.extraction.execute_query",
        lambda _config, query, params=None: pd.DataFrame([{
            "id": 2,
            "folder_name": "shared-parent",
            "owner_id": "other-owner",
            "parent_id": None,
            "created_at": None,
            "folder_type": "default",
        }]),
    )
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path),
        generate_sql=False,
        export_csv=False,
    )

    result = engine._resolve_folder_ancestor_closure(
        pd.DataFrame([{
            "id": 1,
            "folder_name": "owned-child",
            "owner_id": "selected-owner",
            "parent_id": 2,
            "created_at": None,
            "folder_type": "default",
        }]),
        ["selected-owner"],
    )

    assert result["id"].astype(str).tolist() == ["1"]
    assert pd.isna(result.iloc[0]["parent_id"])
    assert engine._folder_hierarchy_report["detached_folder_ids"] == ["1"]
    assert engine._folder_hierarchy_report[
        "dropped_cross_owner_parent_ids"
    ] == ["2"]


def test_missing_folder_parent_is_detached_and_reported(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "utils.extraction.execute_query",
        lambda *_args, **_kwargs: pd.DataFrame(columns=[
            "id", "folder_name", "owner_id", "parent_id",
            "created_at", "folder_type",
        ]),
    )
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path),
        generate_sql=False,
        export_csv=False,
    )
    result = engine._resolve_folder_ancestor_closure(
        pd.DataFrame([{
            "id": 941,
            "folder_name": "orphan",
            "owner_id": "selected-owner",
            "parent_id": 939,
            "created_at": None,
            "folder_type": "document",
        }]),
        ["selected-owner"],
    )
    assert pd.isna(result.iloc[0]["parent_id"])
    assert engine._folder_hierarchy_report["stale_parent_ids"] == ["939"]
    assert engine._folder_hierarchy_report["detached_folder_ids"] == ["941"]


def test_active_owner_canonical_folder_is_excluded_and_dependents_detached(
    tmp_path,
):
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path / "csv"),
        generate_sql=False,
        export_csv=False,
    )
    engine._last_scope_frames = {
        "folders": pd.DataFrame([
            {
                "id": 565,
                "owner_id": "legacy-user",
                "parent_id": None,
            },
            {
                "id": 566,
                "owner_id": "legacy-user",
                "parent_id": 565,
            },
        ]),
        "documents": pd.DataFrame([{
            "doc_id": "doc-1",
            "owner_id": "legacy-user",
            "folder_id": 565,
        }]),
        "embeddings": pd.DataFrame(),
        "agents": pd.DataFrame([{
            "bot_id": "bot-1",
            "user_id": "legacy-user",
            "docs_chosen": [],
            "chosen_docs_folders": [565],
            "folder_id": 565,
        }]),
    }
    results = {
        "files": {},
        "sql_files": {},
        "summary": {"folders": 2, "documents": 1, "agents": 1},
        "folder_hierarchy_report": {"detached_folder_ids": []},
        "document_filter_report": {
            "detached_document_folder_ids": []
        },
    }

    report = engine.exclude_canonical_folder_conflicts(["565"], results)
    frames = engine._last_scope_frames

    assert frames["folders"]["id"].tolist() == [566]
    assert frames["folders"]["parent_id"].isna().all()
    assert frames["documents"]["folder_id"].isna().all()
    assert frames["agents"]["chosen_docs_folders"].iloc[0] == []
    assert pd.isna(frames["agents"]["folder_id"].iloc[0])
    assert report == {
        "excluded_folder_ids": ["565"],
        "detached_folder_ids": ["566"],
        "detached_document_ids": ["doc-1"],
    }
    assert results["summary"]["folders"] == 1
    assert results["ownership_manifest"]["folders"] == [{
        "old_id": "566",
        "owner_id": "legacy-user",
    }]


def test_conversation_generator_skips_blank_and_remaps_non_uuid_chat_ids(
    tmp_path,
):
    valid_chat = "11111111-1111-4111-8111-111111111111"
    logs = pd.DataFrame([
        {
            "id": "valid-log",
            "user_id": "legacy-user",
            "chat_id": valid_chat,
            "question": "hello",
            "answer": "world",
            "created_at": pd.Timestamp("2026-01-01"),
            "token_amount": 1,
            "words_amount": 1,
        },
        {"id": "blank-log", "user_id": "legacy-user", "chat_id": ""},
        {"id": "invalid-log", "user_id": "legacy-user", "chat_id": "not-a-uuid"},
        {"id": "null-log", "user_id": "legacy-user", "chat_id": None},
    ])
    output = tmp_path / "05_conversations.sql"
    result = generate_conversations_logs_migration_sql(
        logs,
        str(output),
        "test",
        migration_run_id="33333333-3333-4333-8333-333333333333",
    )
    assert result["conversations_processed"] == 2
    assert result["skipped_invalid_chat_id"] == 2
    assert result["remapped_non_uuid_chat_ids"] == 1
    sql = output.read_text()
    assert valid_chat in sql
    assert "'not-a-uuid'" in sql
    assert "migration.migration_step_entities" in sql
    assert "ELSE 'reused' END" in sql
    assert "m.migration_run_id = '33333333-3333-4333-8333-333333333333'::uuid" in sql
    assert "m.record_action = 'created'" in sql


@pytest.mark.parametrize(
    ("policy", "expected_sql", "unexpected_sql"),
    [
        (
            "adopt_exact",
            "Conversation exact adoption content mismatch",
            "Replaced unmapped V5 conversation",
        ),
        (
            "replace_unmapped",
            "Replaced unmapped V5 conversation",
            "Conversation exact adoption content mismatch",
        ),
        (
            "block",
            "Conversation UUID collision for legacy conversation",
            "Adopted exact previously migrated conversation",
        ),
    ],
)
def test_conversation_collision_policy_is_embedded_in_shard_sql(
    tmp_path, policy, expected_sql, unexpected_sql
):
    logs = pd.DataFrame([{
        "id": "legacy-log",
        "user_id": "legacy-user",
        "chat_id": "11111111-1111-4111-8111-111111111111",
        "question": "hello",
        "answer": "world",
        "created_at": pd.Timestamp("2025-01-01"),
        "token_amount": 1,
        "words_amount": 1,
    }])
    output = tmp_path / f"05_conversations_{policy}.sql"

    generate_conversations_logs_migration_sql(
        logs,
        str(output),
        "test",
        user_id_overrides={
            "legacy-user": "22222222-2222-4222-8222-222222222222"
        },
        migration_run_id="33333333-3333-4333-8333-333333333333",
        conversation_collision_policy=policy,
    )

    sql = output.read_text()
    assert expected_sql in sql
    assert unexpected_sql not in sql
    assert "Canonical conversation mapping mismatch" in sql
    assert "Canonical conversation owner mismatch" in sql
    assert (
        "v_expected_user_id uuid := "
        "'22222222-2222-4222-8222-222222222222'::uuid"
    ) in sql
    if policy == "replace_unmapped":
        block_delete = sql.index("DELETE FROM message_content_blocks")
        message_delete = sql.index("DELETE FROM messages")
        conversation_delete = sql.index("DELETE FROM conversations")
        conversation_insert = sql.index("-- Conversations INSERT")
        assert (
            block_delete
            < message_delete
            < conversation_delete
            < conversation_insert
        )


def test_conversation_collision_policy_rejects_unknown_value(tmp_path):
    logs = pd.DataFrame([{
        "id": "legacy-log",
        "user_id": "legacy-user",
        "chat_id": "11111111-1111-4111-8111-111111111111",
    }])

    with pytest.raises(ValueError, match="conversation_collision_policy"):
        generate_conversations_logs_migration_sql(
            logs,
            str(tmp_path / "05_conversations.sql"),
            "test",
            conversation_collision_policy="unsafe",
        )


def test_conversation_scope_filters_and_limits_unique_chat_ids():
    date_from = pd.Timestamp("2025-12-01")
    date_to = pd.Timestamp("2025-12-31 23:59:59")

    sql, params = build_conversation_scope_cte(
        "jeen_dev_logs",
        ["user-a", "user-b"],
        date_from=date_from,
        date_to=date_to,
        max_per_user=25,
    )

    assert "GROUP BY l.user_id, lower(btrim(l.chat_id::text))" in sql
    assert "MIN(l.created_at) >= %s" in sql
    assert "MIN(l.created_at) <= %s" in sql
    assert "WHERE conversation_rank <= %s" in sql
    assert "NULLIF(btrim(l.chat_id::text), '') IS NOT NULL" in sql
    assert "chat_id::text) ~ %s" not in sql
    assert params[:2] == ("user-a", "user-b")
    assert params[-3:] == (date_from, date_to, 25)


def test_agent_conversation_gets_v5_agent_metadata(tmp_path):
    bot_id = "legacy-agent"
    agent_id = str(deterministic_uuid_v4_py(
        NAMESPACE_UUID, f"{bot_id}-agent"
    ))
    logs = pd.DataFrame([{
        "id": "agent-log",
        "user_id": "legacy-user",
        "chat_id": "11111111-1111-4111-8111-111111111111",
        "question": "hello",
        "answer": "world",
        "bot_id": bot_id,
        "type": "playground",
        "created_at": pd.Timestamp("2026-01-01"),
        "token_amount": 1,
        "words_amount": 1,
    }])
    output = tmp_path / "05_agent_conversation.sql"

    generate_conversations_logs_migration_sql(
        logs,
        str(output),
        "test",
        agent_id_by_bot_id={bot_id: agent_id},
    )

    sql = output.read_text()
    assert '"initiatedByAgentId": "' + agent_id + '"' in sql
    assert '"legacyBotId": "legacy-agent"' in sql
    assert '"legacyAgentLinkMissing": false' in sql
    assert "Conversation agent association mismatch" in sql
    assert "INSERT INTO conversations" in sql
    assert "user_id, metadata)" in sql


def test_non_uuid_conversation_id_is_deterministically_migrated(tmp_path):
    legacy_chat_id = "legacy-agent-chat"
    expected_chat_id = str(deterministic_uuid_v4_py(
        CONVERSATION_MESSAGES_NAMESPACE_UUID,
        f"conversation-{legacy_chat_id}",
    ))
    logs = pd.DataFrame([{
        "id": "agent-log",
        "user_id": "legacy-user",
        "chat_id": legacy_chat_id,
        "question": "hello",
        "answer": "world",
        "created_at": pd.Timestamp("2026-01-01"),
        "token_amount": 1,
        "words_amount": 1,
    }])
    output = tmp_path / "05_non_uuid_conversation.sql"

    result = generate_conversations_logs_migration_sql(
        logs,
        str(output),
        "test",
    )

    sql = output.read_text()
    assert result["conversations_processed"] == 1
    assert result["remapped_non_uuid_chat_ids"] == 1
    assert f"'{expected_chat_id}'::uuid" in sql
    assert (
        "VALUES ('conversations', 'legacy-agent-chat', "
        f"'{expected_chat_id}'::uuid"
    ) in sql


def test_extract_logs_fetches_every_row_for_selected_conversations(
    monkeypatch, tmp_path
):
    captured = {}

    def fake_query(_config, query, params=None):
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame(columns=["chat_id"])

    monkeypatch.setattr("utils.extraction.execute_query", fake_query)
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path),
        generate_sql=False,
        export_csv=False,
    )

    engine.extract_logs(
        ["legacy-user"],
        date_from=pd.Timestamp("2025-12-01"),
        max_per_user=10,
        selected_chat_ids=["AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE"],
    )

    assert "JOIN selected_conversations selected" in captured["query"]
    assert "selected.chat_id = lower(btrim(l.chat_id::text))" in captured["query"]
    assert "WHERE selected.chat_id = ANY(%s)" in captured["query"]
    assert captured["params"][-1] == [
        "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
    ]


def test_conversation_generator_truncates_titles_to_v5_limit(tmp_path):
    long_title = "כ" * 300
    logs = pd.DataFrame([{
        "id": "long-title-log",
        "user_id": "legacy-user",
        "chat_id": "22222222-2222-4222-8222-222222222222",
        "question": "hello",
        "answer": "world",
        "title": long_title,
        "created_at": pd.Timestamp("2026-01-01"),
        "token_amount": 1,
        "words_amount": 1,
    }])
    output = tmp_path / "05_conversations.sql"

    result = generate_conversations_logs_migration_sql(
        logs,
        str(output),
        "test",
        migration_run_id="33333333-3333-4333-8333-333333333333",
    )

    sql = output.read_text()
    conversation_insert = (
        sql.split("-- Conversations INSERT", 1)[1]
        .split("INSERT INTO migration.id_mappings", 1)[0]
    )
    assert result["truncated_titles"] == 1
    assert long_title[:256] in conversation_insert
    assert long_title not in conversation_insert


def test_extraction_routes_collision_policy_only_to_conversations(
    monkeypatch,
    tmp_path,
):
    captured = {}

    def fake_query(_config, query, _params=None):
        if "FROM public.jeen_dev_folders" in query:
            return pd.DataFrame([{
                "id": 7,
                "folder_name": "Owned",
                "owner_id": "legacy-user",
                "parent_id": None,
                "created_at": None,
                "folder_type": "default",
            }])
        if "FROM public.jeen_dev_logs" in query:
            return pd.DataFrame([{
                "id": "legacy-log",
                "user_id": "legacy-user",
                "chat_id": "11111111-1111-4111-8111-111111111111",
                "question": "hello",
                "answer": "world",
                "created_at": pd.Timestamp("2026-01-01"),
            }])
        raise AssertionError(query)

    def fake_folders(**kwargs):
        captured["folders"] = kwargs
        return {}

    def fake_conversations(**kwargs):
        captured["conversations"] = kwargs
        return {}

    monkeypatch.setattr("utils.extraction.execute_query", fake_query)
    monkeypatch.setattr(
        "utils.extraction.generate_folders_migration_sql",
        fake_folders,
    )
    monkeypatch.setattr(
        "utils.extraction.generate_conversations_logs_migration_sql",
        fake_conversations,
    )
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path / "extract"),
        generate_sql=True,
        export_csv=False,
        conversation_collision_policy="replace_unmapped",
    )

    engine.extract_folders(["legacy-user"])
    engine.extract_logs(
        ["legacy-user"],
        agent_id_by_bot_id={
            "legacy-agent": "22222222-2222-4222-8222-222222222222"
        },
    )

    assert "conversation_collision_policy" not in captured["folders"]
    assert (
        captured["conversations"]["conversation_collision_policy"]
        == "replace_unmapped"
    )
    assert captured["conversations"]["agent_id_by_bot_id"] == {
        "legacy-agent": "22222222-2222-4222-8222-222222222222"
    }


def test_agent_sql_reconciles_kb_count_and_tracks_helper_ownership():
    sql = generate_agent_insert(
        pd.Series({
            "bot_id": "bot-1",
            "user_id": "owner-1",
            "bot_data": {"bot_name": "Agent"},
            "toolkit_settings": {"is_active": "No"},
            "character_prompts": {},
            "docs_chosen": ["doc-1", "doc-2"],
            "chosen_docs_folders": [7],
        }),
        migration_run_id="33333333-3333-4333-8333-333333333333",
    )
    assert "SET total_document_count = (" in sql
    assert "'knowledge_bases'" in sql
    assert "'knowledge_base_assignments'" in sql
    assert "'knowledge_base_items'" in sql
    assert "false," in sql
    assert "Canonical agent owner mismatch" in sql
    assert "Agent exact adoption helper mismatch" in sql
    assert "Adopted exact previously migrated agent" in sql


def test_agent_collision_block_policy_disables_exact_adoption():
    sql = generate_agent_insert(
        pd.Series({
            "bot_id": "bot-1",
            "user_id": "owner-1",
            "bot_data": {"bot_name": "Agent"},
            "toolkit_settings": {},
            "character_prompts": {},
            "docs_chosen": [],
            "chosen_docs_folders": [],
        }),
        agent_collision_policy="block",
    )

    assert "Agent UUID collision for legacy bot" in sql
    assert "Adopted exact previously migrated agent" not in sql
