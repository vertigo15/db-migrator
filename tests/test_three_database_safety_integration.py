"""Three-database append-only/rollback proof using an ephemeral PostgreSQL."""
import importlib.util
import json
import os
import shutil
import socket
import subprocess
from pathlib import Path

import psycopg2
import pytest

from utils.db import ConnectionConfig
from utils.migration_tracking import (
    create_distributed_run,
    reconcile_rollback_status,
    record_step_result,
)
from utils.sql_generator import (
    CONVERSIONS_NAMESPACE_UUID,
    DOC_NAMESPACE_UUID,
    NAMESPACE_UUID,
    deterministic_uuid_v4_py,
    generate_migration_schema_setup,
)


def _load_run_page():
    path = Path(__file__).parents[1] / "pages" / "4_run_migrations.py"
    spec = importlib.util.spec_from_file_location("run_migrations_page", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


RUN_PAGE = _load_run_page()


@pytest.fixture(scope="module")
def postgres_cluster(tmp_path_factory):
    required = ["initdb", "pg_ctl", "psql"]
    if any(shutil.which(binary) is None for binary in required):
        pytest.skip("Local PostgreSQL binaries are required for integration proof")

    root = tmp_path_factory.mktemp("migration-postgres")
    data = root / "data"
    log = root / "postgres.log"
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    postgres_env = {**os.environ, "LC_ALL": "C", "LANG": "C"}

    subprocess.run(
        ["initdb", "-D", str(data), "-A", "trust", "-U", "postgres", "--no-locale"],
        check=True,
        capture_output=True,
        text=True,
        env=postgres_env,
    )
    subprocess.run(
        [
            "pg_ctl",
            "-D",
            str(data),
            "-l",
            str(log),
            "-o",
            f"-F -p {port} -h 127.0.0.1",
            "start",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=postgres_env,
    )
    try:
        for database in ("user_db", "document_db", "completion_db"):
            subprocess.run(
                [
                    "psql",
                    "-h",
                    "127.0.0.1",
                    "-p",
                    str(port),
                    "-U",
                    "postgres",
                    "-d",
                    "postgres",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-c",
                    f"CREATE DATABASE {database}",
                ],
                check=True,
                capture_output=True,
                text=True,
                env=postgres_env,
            )
        yield ConnectionConfig(
            "127.0.0.1", port, "user_db", "postgres", ""
        )
    finally:
        subprocess.run(
            ["pg_ctl", "-D", str(data), "stop", "-m", "fast"],
            check=False,
            capture_output=True,
            text=True,
            env=postgres_env,
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


def test_append_only_migration_and_batch_scoped_rollback(postgres_cluster):
    base = postgres_cluster
    _setup_databases(base)
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
    success, message, _ = RUN_PAGE.rollback_migration(
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
    success, rollback_results, message = RUN_PAGE.rollback_all_migrations(
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
