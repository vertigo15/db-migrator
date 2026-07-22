import json

import pandas as pd
import pytest

from utils.db import ConnectionConfig
from utils.extraction import (
    ExtractionEngine,
    resolve_existing_user_overrides,
    validate_target_organization,
)
from utils.sql_generator import (
    USER_NAMESPACE_UUID,
    _topologically_sort_folders,
    deterministic_uuid_v4_py,
    generate_agent_insert,
    generate_chunks_embeddings_migration_sql,
    generate_conversations_logs_migration_sql,
    generate_documents_migration_sql,
    generate_user_insert,
    truncate_embedding_vector,
)


SOURCE = ConnectionConfig("localhost", 5432, "source", "test", "test")
TARGET = ConnectionConfig("localhost", 5432, "user_db", "test", "test")


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
        "legacy-existing": "11111111-1111-4111-8111-111111111111"
    }
    users = {row["legacy_user_id"]: row for row in result["users"]}
    assert users["legacy-existing"]["action"] == "reused"
    assert users["legacy-new"]["action"] == "created"
    assert users["legacy-new"]["v5_user_id"] == str(
        deterministic_uuid_v4_py(USER_NAMESPACE_UUID, "legacy-new")
    )


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


def test_cross_owner_agent_dependency_blocks_by_default(monkeypatch, tmp_path):
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

    with pytest.raises(ValueError, match="outside the selected batch"):
        engine._topup_agent_documents(
            agents,
            pd.DataFrame(columns=["doc_id", "owner_id"]),
            pd.DataFrame(columns=["metadata"]),
            pd.DataFrame(columns=["id", "owner_id"]),
            selected_user_ids=["owner-1"],
        )


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
        return pd.DataFrame(columns=["bot_id"]), "agents.csv"

    monkeypatch.setattr(engine, "extract_agents", fake_extract_agents)
    monkeypatch.setattr(
        engine,
        "extract_logs",
        lambda *_args, **_kwargs: (pd.DataFrame(columns=["chat_id"]), "logs.csv"),
    )

    results = engine.run_full_extraction(["user@example.com"])

    assert results["errors"] == []
    assert results["summary"]["documents"] == 1
    assert results["document_filter_report"]["chunkless_doc_ids"] == [
        "chunkless-doc"
    ]
    assert captured["doc_ids"] == ["chunked-doc"]


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

    def fake_query(_config, query, params=None):
        assert "LIMIT" not in query.upper()
        assert "metadata->>'type' = 'chunk-data'" in query
        assert tuple(params) == ("doc-1",)
        return rows.copy()

    monkeypatch.setattr("utils.extraction.execute_query", fake_query)
    engine = ExtractionEngine(
        SOURCE,
        "jeen_dev",
        str(tmp_path),
        generate_sql=False,
        export_csv=False,
    )
    extracted, _ = engine.extract_embeddings(["doc-1"], None)
    assert len(extracted) == 5001


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


def test_conversation_generator_skips_blank_and_invalid_chat_ids(tmp_path):
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
    assert result["conversations_processed"] == 1
    assert result["skipped_invalid_chat_id"] == 3
    sql = output.read_text()
    assert valid_chat in sql
    assert "not-a-uuid" not in sql


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
