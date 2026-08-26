import pandas as pd
import psycopg2

from utils.sql_generator import (
    NAMESPACE_UUID,
    deterministic_uuid_v4_py,
    generate_agent_insert,
    generate_migration_schema_setup,
)


OWNER_ID = "22222222-2222-4222-8222-222222222222"


def _connect(base):
    return psycopg2.connect(
        host=base.host,
        port=base.port,
        dbname="completion_db",
        user=base.username,
    )


def test_exact_unmapped_agent_is_adopted(postgres_cluster):
    bot_id = "legacy-bot"
    agent_id = str(deterministic_uuid_v4_py(
        NAMESPACE_UUID, f"{bot_id}-agent"
    ))
    settings_id = str(deterministic_uuid_v4_py(
        NAMESPACE_UUID, f"{bot_id}-settings"
    ))
    knowledge_base_id = str(deterministic_uuid_v4_py(
        NAMESPACE_UUID, f"{bot_id}-kb"
    ))
    assignment_id = str(deterministic_uuid_v4_py(
        NAMESPACE_UUID, f"{bot_id}-kb-assignment"
    ))
    item_id = str(deterministic_uuid_v4_py(
        NAMESPACE_UUID, f"{bot_id}-kb-item-doc-1"
    ))
    conn = _connect(postgres_cluster)
    conn.set_client_encoding("UTF8")
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
                DROP TABLE IF EXISTS legacy_bot_to_agent_mapping,
                    knowledge_base_items, knowledge_base_assignments,
                    knowledge_bases, agent_settings, agents CASCADE;
                DROP SCHEMA IF EXISTS migration CASCADE;
                DO $$
                BEGIN
                    CREATE TYPE agents_type_enum AS ENUM ('cortex');
                EXCEPTION WHEN duplicate_object THEN NULL;
                END $$;
                CREATE TABLE agents (
                    id uuid PRIMARY KEY,
                    name text,
                    description text,
                    type agents_type_enum,
                    user_id uuid,
                    avatar_url text,
                    is_active boolean,
                    is_public boolean,
                    is_prebuilt boolean,
                    is_draft boolean,
                    folder_id uuid,
                    created_at timestamptz,
                    updated_at timestamptz,
                    last_interacted_at timestamptz,
                    deleted_at timestamptz
                );
                CREATE TABLE agent_settings (
                    id uuid PRIMARY KEY,
                    agent_id uuid,
                    model text,
                    instructions text,
                    enabled_tools jsonb,
                    conversation_starters jsonb,
                    workflow_flow_id uuid,
                    base_answers_on_files_only boolean,
                    combines_multiple_answers boolean,
                    retrieved_context_size integer,
                    re_rank_score numeric,
                    query_instructions text,
                    search_in_english boolean,
                    show_source_links boolean,
                    show_source_text boolean,
                    follow_up_questions boolean,
                    additional_links boolean
                );
                CREATE TABLE knowledge_base_assignments (
                    id uuid PRIMARY KEY,
                    knowledge_base_id uuid,
                    assigned_to_id uuid,
                    assigned_to_type text
                );
                CREATE TABLE knowledge_bases (
                    id uuid PRIMARY KEY
                );
                CREATE TABLE knowledge_base_items (
                    id uuid PRIMARY KEY,
                    knowledge_base_id uuid
                );
                CREATE TABLE legacy_bot_to_agent_mapping (
                    old_bot_id text PRIMARY KEY,
                    new_agent_id uuid,
                    agent_type text,
                    bot_name text
                );
                """
            )
            cursor.execute(generate_migration_schema_setup())
            cursor.execute(
                """
                INSERT INTO agents (id, name, type, user_id)
                VALUES (%s, 'Legacy agent', 'cortex', %s)
                """,
                (agent_id, OWNER_ID),
            )
            cursor.execute(
                """
                INSERT INTO agent_settings (id, agent_id)
                VALUES (%s, %s)
                """,
                (settings_id, agent_id),
            )
            cursor.execute(
                "INSERT INTO knowledge_bases (id) VALUES (%s)",
                (knowledge_base_id,),
            )
            cursor.execute(
                """
                INSERT INTO knowledge_base_assignments (
                    id, knowledge_base_id, assigned_to_id, assigned_to_type
                ) VALUES (%s, %s, %s, 'agent')
                """,
                (assignment_id, knowledge_base_id, agent_id),
            )
            cursor.execute(
                """
                INSERT INTO knowledge_base_items (id, knowledge_base_id)
                VALUES (%s, %s)
                """,
                (item_id, knowledge_base_id),
            )
            sql = generate_agent_insert(
                pd.Series({
                    "bot_id": bot_id,
                    "user_id": "legacy-user",
                    "bot_data": {"bot_name": "Legacy agent"},
                    "toolkit_settings": {},
                    "character_prompts": {},
                    "docs_chosen": ["doc-1"],
                    "chosen_docs_folders": [],
                }),
                user_id_overrides={"legacy-user": OWNER_ID},
            )
            cursor.execute(sql)
            cursor.execute(
                """
                SELECT new_id::text, migration_run_id, record_action
                FROM migration.id_mappings
                WHERE table_name = 'agents' AND old_id = %s
                """,
                (bot_id,),
            )
            assert cursor.fetchone() == (agent_id, None, "reused")
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM migration.id_mappings
                WHERE record_action = 'reused'
                  AND migration_run_id IS NULL
                """
            )
            assert cursor.fetchone()[0] == 4
        conn.rollback()
    finally:
        conn.close()
