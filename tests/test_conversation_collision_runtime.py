import pandas as pd
import psycopg2

from utils.sql_generator import (
    _conversation_collision_sql,
    generate_migration_schema_setup,
)


CHAT_ID = "11111111-1111-4111-8111-111111111111"
OWNER_ID = "22222222-2222-4222-8222-222222222222"
NAMESPACE = "0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b"


def _connect(base):
    return psycopg2.connect(
        host=base.host,
        port=base.port,
        dbname="completion_db",
        user=base.username,
    )


def _conversation():
    return {
        "chat_id": CHAT_ID,
        "user_id": "legacy-user",
        "logs": pd.DataFrame([{"id": "legacy-log"}]),
    }


def _prepare_tables(cursor):
    cursor.execute(
        """
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        DROP TABLE IF EXISTS message_content_blocks, messages, conversations
        CASCADE;
        DROP SCHEMA IF EXISTS migration CASCADE;
        CREATE TABLE conversations (
            id uuid PRIMARY KEY,
            user_id uuid NOT NULL
        );
        CREATE TABLE messages (
            id uuid PRIMARY KEY,
            conversation_id uuid NOT NULL
        );
        CREATE TABLE message_content_blocks (
            id uuid PRIMARY KEY,
            message_id uuid NOT NULL
        );
        """
    )
    cursor.execute(generate_migration_schema_setup())


def _expected_ids(cursor):
    cursor.execute(
        """
        SELECT
            migration.deterministic_uuid_v4(%s::uuid, 'legacy-log-user'),
            migration.deterministic_uuid_v4(%s::uuid, 'legacy-log-assistant'),
            migration.deterministic_uuid_v4(
                %s::uuid, 'legacy-log-user-block-0'
            ),
            migration.deterministic_uuid_v4(
                %s::uuid, 'legacy-log-assistant-block-0'
            )
        """,
        (NAMESPACE, NAMESPACE, NAMESPACE, NAMESPACE),
    )
    return cursor.fetchone()


def test_exact_legacy_conversation_is_adopted(postgres_cluster):
    conn = _connect(postgres_cluster)
    try:
        with conn.cursor() as cursor:
            _prepare_tables(cursor)
            user_message, assistant_message, user_block, assistant_block = (
                _expected_ids(cursor)
            )
            cursor.execute(
                "INSERT INTO conversations (id, user_id) VALUES (%s, %s)",
                (CHAT_ID, OWNER_ID),
            )
            cursor.executemany(
                "INSERT INTO messages (id, conversation_id) VALUES (%s, %s)",
                [(user_message, CHAT_ID), (assistant_message, CHAT_ID)],
            )
            cursor.executemany(
                """
                INSERT INTO message_content_blocks (id, message_id)
                VALUES (%s, %s)
                """,
                [(user_block, user_message), (assistant_block, assistant_message)],
            )
            cursor.execute(
                _conversation_collision_sql(
                    _conversation(),
                    NAMESPACE,
                    {"legacy-user": OWNER_ID},
                    "adopt_exact",
                )
            )
            cursor.execute(
                """
                SELECT new_id::text, migration_run_id, record_action
                FROM migration.id_mappings
                WHERE table_name = 'conversations' AND old_id = %s
                """,
                (CHAT_ID,),
            )
            assert cursor.fetchone() == (CHAT_ID, None, "reused")
        conn.rollback()
    finally:
        conn.close()


def test_v4_authoritative_policy_removes_unmapped_collision(postgres_cluster):
    conn = _connect(postgres_cluster)
    try:
        with conn.cursor() as cursor:
            _prepare_tables(cursor)
            cursor.execute(
                "INSERT INTO conversations (id, user_id) VALUES (%s, %s)",
                (CHAT_ID, "33333333-3333-4333-8333-333333333333"),
            )
            cursor.execute(
                """
                INSERT INTO messages (id, conversation_id)
                VALUES ('44444444-4444-4444-8444-444444444444', %s)
                """,
                (CHAT_ID,),
            )
            cursor.execute(
                """
                INSERT INTO message_content_blocks (id, message_id)
                VALUES (
                    '55555555-5555-4555-8555-555555555555',
                    '44444444-4444-4444-8444-444444444444'
                )
                """
            )
            cursor.execute(
                _conversation_collision_sql(
                    _conversation(),
                    NAMESPACE,
                    {"legacy-user": OWNER_ID},
                    "replace_unmapped",
                )
            )
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM conversations),
                    (SELECT COUNT(*) FROM messages),
                    (SELECT COUNT(*) FROM message_content_blocks)
                """
            )
            assert cursor.fetchone() == (0, 0, 0)
        conn.rollback()
    finally:
        conn.close()
