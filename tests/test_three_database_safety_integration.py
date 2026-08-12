"""Three-database append-only/rollback proof using an ephemeral PostgreSQL."""
import json
import os

import pandas as pd
import psycopg2
import pytest

from utils.db import ConnectionConfig
from utils.migration_tracking import (
    create_distributed_run,
    reconcile_rollback_status,
    record_step_result,
)
from utils.rollback import (
    rollback_all_migrations,
    rollback_migration,
    rollback_tracked_user,
)
from utils.shard_queue import enqueue_shards
from utils.step_verification import verify_step
from utils.sql_generator import (
    CONVERSIONS_NAMESPACE_UUID,
    DOC_NAMESPACE_UUID,
    NAMESPACE_UUID,
    deterministic_uuid_v4_py,
    generate_migration_schema_setup,
    generate_user_insert,
)


def _connect(base, database):
    return psycopg2.connect(
        host=base.host,
        port=base.port,
        dbname=database,
        user=base.username,
    )


def _setup_databases(base):
    ddl = {
        "user_db": """
            CREATE TABLE users (
                id uuid PRIMARY KEY,
                email text UNIQUE NOT NULL,
                payload jsonb NOT NULL
            );
        """,
        "document_db": """
            CREATE TABLE folders (
                id uuid PRIMARY KEY, user_id uuid NOT NULL, payload jsonb NOT NULL,
                parent_id uuid
            );
            CREATE TABLE documents (
                id uuid PRIMARY KEY, user_id uuid NOT NULL, folder_id uuid,
                payload jsonb NOT NULL
            );
            CREATE TABLE chunks (
                id uuid PRIMARY KEY, document_id uuid NOT NULL, payload jsonb NOT NULL
            );
            CREATE TABLE embeddings (
                id uuid PRIMARY KEY, chunk_id uuid NOT NULL, document_id uuid NOT NULL,
                payload jsonb NOT NULL
            );
        """,
        "completion_db": """
            CREATE TABLE agents (
                id uuid PRIMARY KEY, user_id uuid NOT NULL, folder_id uuid,
                payload jsonb NOT NULL
            );
            CREATE TABLE agent_settings (
                id uuid PRIMARY KEY, agent_id uuid NOT NULL
            );
            CREATE TABLE knowledge_bases (id uuid PRIMARY KEY);
            CREATE TABLE knowledge_base_assignments (
                id uuid PRIMARY KEY, knowledge_base_id uuid NOT NULL,
                assigned_to_id uuid NOT NULL,
                assigned_to_type text NOT NULL DEFAULT 'agent'
            );
            CREATE TABLE knowledge_base_items (
                id uuid PRIMARY KEY, knowledge_base_id uuid NOT NULL,
                item_id uuid NOT NULL
            );
            CREATE TABLE legacy_bot_to_agent_mapping (
                old_bot_id text PRIMARY KEY, new_agent_id uuid NOT NULL
            );
            CREATE TABLE conversations (
                id uuid PRIMARY KEY, user_id uuid NOT NULL, payload jsonb NOT NULL
            );
            CREATE TABLE messages (
                id uuid PRIMARY KEY, conversation_id uuid NOT NULL
            );
            CREATE TABLE message_content_blocks (
                id uuid PRIMARY KEY, message_id uuid NOT NULL
            );
            CREATE TABLE conversions (
                id uuid PRIMARY KEY, user_id uuid NOT NULL
            );
            CREATE TABLE agent_conversions (
                agent_id uuid NOT NULL, conversion_id uuid NOT NULL
            );
        """,
    }
    for database, database_ddl in ddl.items():
        conn = _connect(base, database)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
                cursor.execute(generate_migration_schema_setup())
                cursor.execute(database_ddl)
        finally:
            conn.close()


def test_cross_run_user_reuse_is_tracked_without_reassigning_mapping(
    prepared_cluster,
):
    base = prepared_cluster
    old_run_id = "81000000-0000-4000-8000-000000000001"
    current_run_id = "81000000-0000-4000-8000-000000000002"
    user_id = "82000000-0000-4000-8000-000000000001"
    legacy_user_id = "legacy-cross-run-user"
    email = "cross-run@example.com"
    selected_user = {
        "email": email,
        "legacy_user_id": legacy_user_id,
        "v5_user_id": user_id,
        "action": "reused",
    }
    create_distributed_run(base, old_run_id, [selected_user], {"database": "v4"})
    create_distributed_run(
        base, current_run_id, [selected_user], {"database": "v4"}
    )

    conn = _connect(base, "user_db")
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO users (id, email, payload) "
                    "VALUES (%s::uuid, %s, '{}'::jsonb)",
                    (user_id, email),
                )
                cursor.execute(
                    """
                    INSERT INTO migration.id_mappings (
                        table_name, old_id, new_id, migration_batch,
                        migration_run_id, record_action
                    )
                    VALUES (
                        'users', %s, %s::uuid, 'older-run',
                        %s::uuid, 'reused'
                    )
                    """,
                    (legacy_user_id, user_id, old_run_id),
                )
                sql = generate_user_insert(
                    pd.Series({
                        "id": legacy_user_id,
                        "email": email,
                        "name": "Cross Run",
                    }),
                    user_id_overrides={legacy_user_id: user_id},
                    migration_run_id=current_run_id,
                )
                cursor.execute(sql)
                cursor.execute(
                    """
                    UPDATE migration.migration_steps
                    SET expected_count = 1
                    WHERE migration_run_id = %s::uuid
                      AND step_key = '01_users'
                    """,
                    (current_run_id,),
                )

                affected, details = verify_step(
                    cursor, "01_users", current_run_id
                )
                assert affected == 1
                assert details["created_entities"] == 0
                assert details["reused_entities"] == 1

                cursor.execute(
                    """
                    SELECT migration_run_id, record_action
                    FROM migration.id_mappings
                    WHERE table_name = 'users' AND old_id = %s
                    """,
                    (legacy_user_id,),
                )
                mapping_run_id, action = cursor.fetchone()
                assert str(mapping_run_id) == old_run_id
                assert action == "reused"
    finally:
        conn.close()


def test_force_removes_cross_database_kb_item_before_document(
    prepared_cluster,
):
    base = prepared_cluster
    run_id = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    user_id = "22000000-0000-4000-8000-000000000001"
    folder_id = "32000000-0000-4000-8000-000000000001"
    document_id = "42000000-0000-4000-8000-000000000001"
    kb_id = "52000000-0000-4000-8000-000000000009"
    kb_item_id = "54000000-0000-4000-8000-000000000009"

    create_distributed_run(
        base,
        run_id,
        [{
            "email": "force-kb@example.com",
            "legacy_user_id": "legacy-force-kb",
            "v5_user_id": user_id,
            "action": "created",
        }],
        {"database": "ephemeral-v4"},
    )

    conn = _connect(base, "user_db")
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO users VALUES (%s, %s, '{\"kind\":\"force-kb\"}')",
                    (user_id, "force-kb@example.com"),
                )
    finally:
        conn.close()

    conn = _connect(base, "document_db")
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO folders VALUES (%s, %s, '{\"kind\":\"force-kb\"}', NULL)",
                    (folder_id, user_id),
                )
                cursor.execute(
                    "INSERT INTO documents VALUES (%s, %s, %s, '{\"kind\":\"force-kb\"}')",
                    (document_id, user_id, folder_id),
                )
                cursor.execute(
                    """
                    INSERT INTO migration.id_mappings
                        (table_name, old_id, new_id, migration_batch,
                         migration_run_id, record_action)
                    VALUES ('documents', 'legacy-force-doc', %s, %s, %s, 'created')
                    """,
                    (document_id, run_id, run_id),
                )
    finally:
        conn.close()

    conn = _connect(base, "completion_db")
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO knowledge_bases VALUES (%s)",
                    (kb_id,),
                )
                cursor.execute(
                    """
                    INSERT INTO knowledge_base_assignments
                    VALUES (gen_random_uuid(), %s, gen_random_uuid(), 'agent')
                    """,
                    (kb_id,),
                )
                cursor.execute(
                    "INSERT INTO knowledge_base_items VALUES (%s, %s, %s)",
                    (kb_item_id, kb_id, document_id),
                )
    finally:
        conn.close()

    record_step_result(
        base, run_id, "03_documents", "document_db", True, 1
    )
    config = ConnectionConfig(
        base.host, base.port, "document_db", base.username, base.password
    )
    user_scope = {
        "email": "force-kb@example.com",
        "legacy_user_id": "legacy-force-kb",
        "v5_user_id": user_id,
    }

    success, message, _ = rollback_migration(
        config,
        "03_documents_test.sql",
        "document_db",
        run_id,
        user_scope=user_scope,
    )
    assert not success
    assert "Enable Force" in message

    success, message, _ = rollback_migration(
        config,
        "03_documents_test.sql",
        "document_db",
        run_id,
        user_scope=user_scope,
        force=True,
    )
    assert success, message

    conn = _connect(base, "completion_db")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM knowledge_base_items WHERE id = %s",
                (kb_item_id,),
            )
            assert cursor.fetchone()[0] == 0
            cursor.execute(
                "SELECT COUNT(*) FROM knowledge_bases WHERE id = %s",
                (kb_id,),
            )
            assert cursor.fetchone()[0] == 1
    finally:
        conn.close()

    conn = _connect(base, "document_db")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM documents WHERE id = %s",
                (document_id,),
            )
            assert cursor.fetchone()[0] == 0
    finally:
        conn.close()


def test_rollback_is_blocked_while_worker_shards_are_active(prepared_cluster):
    base = prepared_cluster
    run_id = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    create_distributed_run(
        base,
        run_id,
        [{
            "email": "active-worker@example.com",
            "legacy_user_id": "legacy-active-worker",
            "v5_user_id": "23000000-0000-4000-8000-000000000001",
            "action": "created",
        }],
        {"database": "ephemeral-v4"},
    )
    enqueue_shards(
        base,
        run_id,
        "03_documents",
        {
            "shards": [{
                "shard_index": 1,
                "file_path": "/tmp/active-document-shard.sql",
                "expected_rows": 1,
                "byte_size": 10,
                "checksum": "active",
            }]
        },
        owner_emails=["active-worker@example.com"],
    )
    config = ConnectionConfig(
        base.host, base.port, "document_db", base.username, base.password
    )
    success, message, rows = rollback_migration(
        config,
        "03_documents_active.sql",
        "document_db",
        run_id,
    )
    assert success is False
    assert rows == 0
    assert "still queued or running" in message


def test_is_migrated_repairs_stale_mapping_but_preserves_live_mapping(
    prepared_cluster,
):
    base = prepared_cluster
    stale_id = "35000000-0000-4000-8000-000000000001"
    live_id = "35000000-0000-4000-8000-000000000002"
    conn = _connect(base, "document_db")
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO folders (id, user_id, payload, parent_id)
                    VALUES (
                        %s, '25000000-0000-4000-8000-000000000001',
                        '{"kind":"live-mapping"}', NULL
                    )
                    """,
                    (live_id,),
                )
                cursor.execute(
                    """
                    INSERT INTO migration.id_mappings
                        (table_name, old_id, new_id, record_action)
                    VALUES
                        ('folders', 'stale-folder-integration', %s, 'created'),
                        ('folders', 'live-folder-integration', %s, 'created')
                    """,
                    (stale_id, live_id),
                )
                cursor.execute(
                    """
                    SELECT migration.is_migrated(
                        'folders', 'stale-folder-integration'
                    )
                    """
                )
                assert cursor.fetchone()[0] is False
                cursor.execute(
                    """
                    SELECT migration.is_migrated(
                        'folders', 'live-folder-integration'
                    )
                    """
                )
                assert cursor.fetchone()[0] is True
                cursor.execute(
                    """
                    SELECT old_id
                    FROM migration.id_mappings
                    WHERE old_id IN (
                        'stale-folder-integration',
                        'live-folder-integration'
                    )
                    ORDER BY old_id
                    """
                )
                assert cursor.fetchall() == [("live-folder-integration",)]
    finally:
        conn.close()


def test_rollback_marks_never_started_cancelled_step_skipped(prepared_cluster):
    base = prepared_cluster
    run_id = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
    create_distributed_run(
        base,
        run_id,
        [{
            "email": "cancelled-worker@example.com",
            "legacy_user_id": "legacy-cancelled-worker",
            "v5_user_id": "24000000-0000-4000-8000-000000000001",
            "action": "created",
        }],
        {"database": "ephemeral-v4"},
    )
    for database in ("user_db", "document_db", "completion_db"):
        conn = _connect(base, database)
        try:
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE migration.migration_steps
                        SET status = 'skipped'
                        WHERE migration_run_id = %s
                          AND step_key <> '07_conversions'
                        """,
                        (run_id,),
                    )
        finally:
            conn.close()

    conn = _connect(base, "completion_db")
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO migration.migration_shards (
                        migration_run_id, step_key, step_order,
                        target_database, shard_index, total_shards,
                        file_path, checksum, status
                    )
                    VALUES (%s, '07_conversions', 7, 'completion_db',
                            1, 1, '/tmp/cancelled-conversion.sql',
                            'cancelled', 'cancelled')
                    """,
                    (run_id,),
                )
    finally:
        conn.close()

    success, results, message = rollback_all_migrations(
        base,
        [{
            "filename": "07_conversions_cancelled.sql",
            "target_db": "completion_db",
        }],
        run_id,
    )
    assert success is True
    assert message == "All produced steps rolled back successfully"
    assert results[0]["rows"] == 0
    assert "never started" in results[0]["message"]

    conn = _connect(base, "completion_db")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT status
                FROM migration.migration_steps
                WHERE migration_run_id = %s
                  AND step_key = '07_conversions'
                """,
                (run_id,),
            )
            assert cursor.fetchone()[0] == "skipped"
    finally:
        conn.close()


@pytest.fixture(scope="module")
def prepared_cluster(postgres_cluster):
    _setup_databases(postgres_cluster)
    return postgres_cluster


def test_distributed_run_persists_user_resolution_in_every_target(
    prepared_cluster,
):
    run_id = "fafafafa-fafa-4afa-8afa-fafafafafafa"
    user = {
        "email": "resolved@example.com",
        "legacy_user_id": "legacy-resolved",
        "v5_user_id": "21000000-0000-4000-8000-000000000099",
        "action": "reused",
    }
    create_distributed_run(
        prepared_cluster,
        run_id,
        [user],
        {"database": "ephemeral-v4"},
    )

    for database in ("user_db", "document_db", "completion_db"):
        conn = _connect(prepared_cluster, database)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT legacy_user_id, v5_user_id::text, user_action
                    FROM migration.migration_run_users
                    WHERE migration_run_id = %s::uuid
                    """,
                    (run_id,),
                )
                assert cursor.fetchone() == (
                    "legacy-resolved",
                    user["v5_user_id"],
                    "reused",
                )
        finally:
            conn.close()


def _snapshot_existing(base):
    queries = {
        "user_db": "SELECT jsonb_agg(to_jsonb(t) ORDER BY email) FROM users t WHERE payload->>'kind' = 'existing'",
        "document_db": """
            SELECT jsonb_build_object(
                'folders', (SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM folders t WHERE payload->>'kind' = 'existing'),
                'documents', (SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM documents t WHERE payload->>'kind' = 'existing')
            )
        """,
        "completion_db": """
            SELECT jsonb_build_object(
                'agents', (SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM agents t WHERE payload->>'kind' = 'existing'),
                'conversations', (SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM conversations t WHERE payload->>'kind' = 'existing')
            )
        """,
    }
    snapshot = {}
    for database, query in queries.items():
        conn = _connect(base, database)
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                snapshot[database] = json.dumps(cursor.fetchone()[0], sort_keys=True, default=str)
        finally:
            conn.close()
    return snapshot


def test_append_only_migration_and_batch_scoped_rollback(prepared_cluster):
    base = prepared_cluster
    run_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    existing_users = [
        f"10000000-0000-4000-8000-00000000000{i}" for i in range(1, 5)
    ]
    new_user = "20000000-0000-4000-8000-000000000001"

    user_conn = _connect(base, "user_db")
    try:
        with user_conn:
            with user_conn.cursor() as cursor:
                for index, user_id in enumerate(existing_users, 1):
                    cursor.execute(
                        "INSERT INTO users VALUES (%s, %s, %s::jsonb)",
                        (user_id, f"existing{index}@example.com", '{"kind":"existing"}'),
                    )
                cursor.execute(
                    "INSERT INTO users VALUES (%s, %s, %s::jsonb)",
                    (new_user, "new@example.com", '{"kind":"migrated"}'),
                )
    finally:
        user_conn.close()

    existing_folder = "30000000-0000-4000-8000-000000000001"
    existing_doc = "40000000-0000-4000-8000-000000000001"
    new_folder = str(deterministic_uuid_v4_py(NAMESPACE_UUID, "legacy-folder"))
    new_doc = str(deterministic_uuid_v4_py(DOC_NAMESPACE_UUID, "legacy-doc"))
    doc_conn = _connect(base, "document_db")
    try:
        with doc_conn:
            with doc_conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO folders (id, user_id, payload) VALUES (%s, %s, '{\"kind\":\"existing\"}')",
                    (existing_folder, existing_users[0]),
                )
                cursor.execute(
                    "INSERT INTO documents VALUES (%s, %s, %s, '{\"kind\":\"existing\"}')",
                    (existing_doc, existing_users[0], existing_folder),
                )
                cursor.execute(
                    "INSERT INTO folders (id, user_id, payload) VALUES (%s, %s, '{\"kind\":\"migrated\"}')",
                    (new_folder, new_user),
                )
                cursor.execute(
                    "INSERT INTO documents VALUES (%s, %s, %s, '{\"kind\":\"migrated\"}')",
                    (new_doc, new_user, new_folder),
                )
                cursor.execute(
                    "INSERT INTO chunks VALUES (gen_random_uuid(), %s, '{\"kind\":\"migrated\"}')",
                    (new_doc,),
                )
                cursor.execute(
                    """
                    INSERT INTO embeddings
                    SELECT gen_random_uuid(), id, document_id, '{"kind":"migrated"}'
                    FROM chunks WHERE document_id = %s
                    """,
                    (new_doc,),
                )
    finally:
        doc_conn.close()

    existing_agent = "50000000-0000-4000-8000-000000000001"
    existing_conversation = "60000000-0000-4000-8000-000000000001"
    bot_id = "legacy-bot"
    new_agent = str(deterministic_uuid_v4_py(NAMESPACE_UUID, f"{bot_id}-agent"))
    new_kb = str(deterministic_uuid_v4_py(NAMESPACE_UUID, f"{bot_id}-kb"))
    new_conversation = "70000000-0000-4000-8000-000000000001"
    conversion_old_id = f"conv-source-{bot_id}"
    new_conversion = str(
        deterministic_uuid_v4_py(CONVERSIONS_NAMESPACE_UUID, conversion_old_id)
    )
    completion_conn = _connect(base, "completion_db")
    try:
        with completion_conn:
            with completion_conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO agents VALUES (%s, %s, %s, '{\"kind\":\"existing\"}')",
                    (existing_agent, existing_users[0], existing_folder),
                )
                cursor.execute(
                    "INSERT INTO conversations VALUES (%s, %s, '{\"kind\":\"existing\"}')",
                    (existing_conversation, existing_users[0]),
                )
                cursor.execute(
                    "INSERT INTO agents VALUES (%s, %s, %s, '{\"kind\":\"migrated\"}')",
                    (new_agent, new_user, new_folder),
                )
                cursor.execute(
                    "INSERT INTO agent_settings VALUES (gen_random_uuid(), %s)",
                    (new_agent,),
                )
                cursor.execute("INSERT INTO knowledge_bases VALUES (%s)", (new_kb,))
                cursor.execute(
                    "INSERT INTO knowledge_base_assignments VALUES (gen_random_uuid(), %s, %s)",
                    (new_kb, new_agent),
                )
                cursor.execute(
                    "INSERT INTO knowledge_base_items VALUES (gen_random_uuid(), %s, %s)",
                    (new_kb, new_doc),
                )
                cursor.execute(
                    "INSERT INTO legacy_bot_to_agent_mapping VALUES (%s, %s)",
                    (bot_id, new_agent),
                )
                cursor.execute(
                    "INSERT INTO conversations VALUES (%s, %s, '{\"kind\":\"migrated\"}')",
                    (new_conversation, new_user),
                )
                cursor.execute(
                    "INSERT INTO messages VALUES (gen_random_uuid(), %s) RETURNING id",
                    (new_conversation,),
                )
                message_id = cursor.fetchone()[0]
                cursor.execute(
                    "INSERT INTO message_content_blocks VALUES (gen_random_uuid(), %s)",
                    (message_id,),
                )
                cursor.execute(
                    "INSERT INTO conversions VALUES (%s, %s)",
                    (new_conversion, new_user),
                )
                cursor.execute(
                    "INSERT INTO agent_conversions VALUES (%s, %s)",
                    (new_agent, new_conversion),
                )
    finally:
        completion_conn.close()

    before = _snapshot_existing(base)
    users_for_tracking = [
        {
            "email": f"existing{index}@example.com",
            "legacy_user_id": f"legacy-existing-{index}",
            "v5_user_id": user_id,
            "action": "reused",
        }
        for index, user_id in enumerate(existing_users, 1)
    ] + [{
        "email": "new@example.com",
        "legacy_user_id": "legacy-new",
        "v5_user_id": new_user,
        "action": "created",
    }]
    create_distributed_run(
        base, run_id, users_for_tracking, {"database": "ephemeral-v4"}
    )

    mappings = {
        "user_db": [
            ("users", f"legacy-existing-{i}", user_id, "reused")
            for i, user_id in enumerate(existing_users, 1)
        ] + [("users", "legacy-new", new_user, "created")],
        "document_db": [
            ("folders", "legacy-folder", new_folder, "created"),
            ("documents", "legacy-doc", new_doc, "created"),
        ],
        "completion_db": [
            ("agents", bot_id, new_agent, "created"),
            ("conversations", new_conversation, new_conversation, "created"),
            ("conversions", conversion_old_id, new_conversion, "created"),
        ],
    }
    for database, rows in mappings.items():
        conn = _connect(base, database)
        try:
            with conn:
                with conn.cursor() as cursor:
                    for table_name, old_id, new_id, action in rows:
                        cursor.execute(
                            """
                            INSERT INTO migration.id_mappings
                                (table_name, old_id, new_id, migration_batch,
                                 migration_run_id, record_action)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (table_name, old_id, new_id, run_id, run_id, action),
                        )
        finally:
            conn.close()


    steps = [
        ("01_users", "user_db"),
        ("02_folders", "document_db"),
        ("03_documents", "document_db"),
        ("04_chunks_embeddings", "document_db"),
        ("05_conversations", "completion_db"),
        ("06_agents", "completion_db"),
        ("07_conversions", "completion_db"),
    ]
    for step, database in steps:
        record_step_result(base, run_id, step, database, True, 1)

    premature_user_config = ConnectionConfig(
        base.host, base.port, "user_db", base.username, base.password
    )
    success, message, _ = rollback_migration(
        premature_user_config,
        "01_users_test.sql",
        "user_db",
        run_id,
    )
    assert not success
    assert "Rollback order violation" in message

    migration_files = [
        {"filename": "01_users_test.sql", "target_db": "user_db"},
        {"filename": "02_folders_test.sql", "target_db": "document_db"},
        {"filename": "03_documents_test.sql", "target_db": "document_db"},
        {"filename": "04_chunks_embeddings_test.sql", "target_db": "document_db"},
        {"filename": "05_conversations_test.sql", "target_db": "completion_db"},
        {"filename": "06_agents_test.sql", "target_db": "completion_db"},
        {"filename": "07_conversions_test.sql", "target_db": "completion_db"},
    ]
    success, rollback_results, message = rollback_all_migrations(
        base, migration_files, run_id
    )
    assert success, message
    assert [result["filename"] for result in rollback_results] == [
        "07_conversions_test.sql",
        "06_agents_test.sql",
        "05_conversations_test.sql",
        "04_chunks_embeddings_test.sql",
        "03_documents_test.sql",
        "02_folders_test.sql",
        "01_users_test.sql",
    ]

    assert reconcile_rollback_status(base, run_id) == "rolled_back"
    assert _snapshot_existing(base) == before

    absence_checks = {
        "user_db": ("users", new_user),
        "document_db": ("documents", new_doc),
        "completion_db": ("agents", new_agent),
    }
    for database, (table, entity_id) in absence_checks.items():
        conn = _connect(base, database)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE id = %s", (entity_id,)
                )
                assert cursor.fetchone()[0] == 0
                cursor.execute(
                    "SELECT COUNT(*) FROM migration.id_mappings WHERE migration_run_id = %s",
                    (run_id,),
                )
                assert cursor.fetchone()[0] == 0
                cursor.execute(
                    "SELECT status FROM migration.migration_runs WHERE id = %s",
                    (run_id,),
                )
                assert cursor.fetchone()[0] == "rolled_back"
        finally:
            conn.close()


def test_per_user_rollback_preserves_other_users_in_batch(prepared_cluster):
    base = prepared_cluster
    run_id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    users = [
        {
            "email": "scope-a@example.com",
            "legacy_user_id": "legacy-scope-a",
            "v5_user_id": "21000000-0000-4000-8000-000000000001",
            "action": "created",
        },
        {
            "email": "scope-b@example.com",
            "legacy_user_id": "legacy-scope-b",
            "v5_user_id": "21000000-0000-4000-8000-000000000002",
            "action": "created",
        },
    ]
    create_distributed_run(
        base, run_id, users, {"database": "ephemeral-v4"}
    )

    owned = {}
    for index, user in enumerate(users, 1):
        suffix = str(index)
        owned[user["email"]] = {
            "user": user["v5_user_id"],
            "folder": f"31000000-0000-4000-8000-00000000000{suffix}",
            "document": f"41000000-0000-4000-8000-00000000000{suffix}",
            "agent": f"51000000-0000-4000-8000-00000000000{suffix}",
            "kb": f"52000000-0000-4000-8000-00000000000{suffix}",
            "kb_assignment": f"53000000-0000-4000-8000-00000000000{suffix}",
            "kb_item": f"54000000-0000-4000-8000-00000000000{suffix}",
            "conversation": f"61000000-0000-4000-8000-00000000000{suffix}",
            "conversion": f"71000000-0000-4000-8000-00000000000{suffix}",
        }

    for user in users:
        ids = owned[user["email"]]
        conn = _connect(base, "user_db")
        try:
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO users VALUES (%s, %s, '{\"kind\":\"scoped\"}')",
                        (ids["user"], user["email"]),
                    )
                    cursor.execute(
                        """
                        INSERT INTO migration.id_mappings
                            (table_name, old_id, new_id, migration_batch,
                             migration_run_id, record_action)
                        VALUES ('users', %s, %s, %s, %s, 'created')
                        """,
                        (
                            user["legacy_user_id"],
                            ids["user"],
                            run_id,
                            run_id,
                        ),
                    )
        finally:
            conn.close()
        conn = _connect(base, "document_db")
        try:
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO folders VALUES (%s, %s, '{\"kind\":\"scoped\"}', NULL)",
                        (ids["folder"], ids["user"]),
                    )
                    cursor.execute(
                        "INSERT INTO documents VALUES (%s, %s, %s, '{\"kind\":\"scoped\"}')",
                        (ids["document"], ids["user"], ids["folder"]),
                    )
                    cursor.execute(
                        "INSERT INTO chunks VALUES (gen_random_uuid(), %s, '{\"kind\":\"scoped\"}')",
                        (ids["document"],),
                    )
                    cursor.execute(
                        """
                        INSERT INTO embeddings
                        SELECT gen_random_uuid(), id, document_id, '{"kind":"scoped"}'
                        FROM chunks WHERE document_id = %s
                        """,
                        (ids["document"],),
                    )
                    for table_name, old_id, new_id in (
                        ("folders", f"folder-{user['legacy_user_id']}", ids["folder"]),
                        ("documents", f"doc-{user['legacy_user_id']}", ids["document"]),
                    ):
                        cursor.execute(
                            """
                            INSERT INTO migration.id_mappings
                                (table_name, old_id, new_id, migration_batch,
                                 migration_run_id, record_action)
                            VALUES (%s, %s, %s, %s, %s, 'created')
                            """,
                            (table_name, old_id, new_id, run_id, run_id),
                        )
        finally:
            conn.close()

        conn = _connect(base, "completion_db")
        try:
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO agents VALUES (%s, %s, %s, '{\"kind\":\"scoped\"}')",
                        (ids["agent"], ids["user"], ids["folder"]),
                    )
                    cursor.execute(
                        "INSERT INTO knowledge_bases VALUES (%s)",
                        (ids["kb"],),
                    )
                    cursor.execute(
                        "INSERT INTO knowledge_base_assignments VALUES (%s, %s, %s, 'agent')",
                        (ids["kb_assignment"], ids["kb"], ids["agent"]),
                    )
                    cursor.execute(
                        "INSERT INTO knowledge_base_items VALUES (%s, %s, %s)",
                        (ids["kb_item"], ids["kb"], ids["document"]),
                    )
                    cursor.execute(
                        "INSERT INTO conversations VALUES (%s, %s, '{\"kind\":\"scoped\"}')",
                        (ids["conversation"], ids["user"]),
                    )
                    cursor.execute(
                        "INSERT INTO conversions VALUES (%s, %s)",
                        (ids["conversion"], ids["user"]),
                    )
                    cursor.execute(
                        "INSERT INTO agent_conversions VALUES (%s, %s)",
                        (ids["agent"], ids["conversion"]),
                    )
                    for table_name, old_id, new_id in (
                        ("agents", f"agent-{user['legacy_user_id']}", ids["agent"]),
                        ("knowledge_bases", f"agent-{user['legacy_user_id']}", ids["kb"]),
                        (
                            "knowledge_base_assignments",
                            f"assignment-{user['legacy_user_id']}",
                            ids["kb_assignment"],
                        ),
                        (
                            "knowledge_base_items",
                            f"item-{user['legacy_user_id']}",
                            ids["kb_item"],
                        ),
                        (
                            "conversations",
                            f"conversation-{user['legacy_user_id']}",
                            ids["conversation"],
                        ),
                        (
                            "conversions",
                            f"conversion-{user['legacy_user_id']}",
                            ids["conversion"],
                        ),
                    ):
                        cursor.execute(
                            """
                            INSERT INTO migration.id_mappings
                                (table_name, old_id, new_id, migration_batch,
                                 migration_run_id, record_action)
                            VALUES (%s, %s, %s, %s, %s, 'created')
                            """,
                            (table_name, old_id, new_id, run_id, run_id),
                        )
        finally:
            conn.close()

    for step, database in (
        ("01_users", "user_db"),
        ("02_folders", "document_db"),
        ("03_documents", "document_db"),
        ("04_chunks_embeddings", "document_db"),
        ("05_conversations", "completion_db"),
        ("06_agents", "completion_db"),
        ("07_conversions", "completion_db"),
    ):
        record_step_result(base, run_id, step, database, True, 2)

    success, _, message = rollback_tracked_user(
        base, run_id, users[0]["email"]
    )
    assert success, message

    for database, table, entity_key in (
        ("user_db", "users", "user"),
        ("document_db", "documents", "document"),
        ("completion_db", "agents", "agent"),
        ("completion_db", "conversations", "conversation"),
        ("completion_db", "conversions", "conversion"),
    ):
        conn = _connect(base, database)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE id = %s",
                    (owned[users[0]["email"]][entity_key],),
                )
                assert cursor.fetchone()[0] == 0
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE id = %s",
                    (owned[users[1]["email"]][entity_key],),
                )
                assert cursor.fetchone()[0] == 1
        finally:
            conn.close()

    conn = _connect(base, "user_db")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT email, result
                FROM migration.migration_user_results
                WHERE batch_id = %s
                ORDER BY email
                """,
                (run_id,),
            )
            assert cursor.fetchall() == [
                ("scope-a@example.com", "rolled_back"),
                ("scope-b@example.com", "pending"),
            ]
            cursor.execute(
                "SELECT status FROM migration.migration_batches WHERE id = %s",
                (run_id,),
            )
            assert cursor.fetchone()[0] == "partial"
    finally:
        conn.close()

    success, _, message = rollback_tracked_user(
        base, run_id, users[1]["email"]
    )
    assert success, message
    for database in ("user_db", "document_db", "completion_db"):
        conn = _connect(base, database)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT status FROM migration.migration_runs WHERE id = %s",
                    (run_id,),
                )
                assert cursor.fetchone()[0] == "rolled_back"
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM migration.id_mappings
                    WHERE migration_run_id = %s
                    """,
                    (run_id,),
                )
                assert cursor.fetchone()[0] == 0
        finally:
            conn.close()
