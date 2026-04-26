-- ============================================================
-- AGENTS MIGRATION SQL (from playground_bot_generator_config)
-- ============================================================
-- Generated: 2026-04-26T14:40:46.948019
-- Source: test_source
-- Destination: agents + agent_settings + knowledge_bases
-- Source rows: 1
-- 
-- IMPORTANT: This script will INSERT data into 5 tables!
-- IMPORTANT: Run users, folders, and documents migrations first!
--
-- Creates:
--   1. agents (main agent record with deterministic UUID)
--   2. agent_settings (1:1 settings for each agent)
--   3. knowledge_bases (one per agent with documents)
--   4. knowledge_base_assignments (links KB to agent)
--   5. knowledge_base_items (links KB to documents/folders)
--   6. legacy_bot_to_agent_mapping (tracking table)
--
-- Uses deterministic UUID generation (deterministic_uuid_v4).
-- Namespace UUID: 0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b
-- ============================================================

-- Ensure PostgreSQL interprets this file as UTF-8 (required for Hebrew/multilingual content)
SET client_encoding = 'UTF8';

-- Ensure uuid-ossp extension is available
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- MIGRATION MAPPING TABLE SETUP (idempotent)
-- ============================================================
-- Creates schema and tables for tracking ID mappings
-- Safe to run multiple times - uses IF NOT EXISTS
-- ============================================================

-- Create migration schema
CREATE SCHEMA IF NOT EXISTS migration;

-- Create ID mappings table
CREATE TABLE IF NOT EXISTS migration.id_mappings (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    old_id VARCHAR(255) NOT NULL,
    new_id UUID NOT NULL,
    migration_batch VARCHAR(50),
    migrated_at TIMESTAMP DEFAULT now(),
    notes TEXT,
    CONSTRAINT uq_table_old_id UNIQUE (table_name, old_id),
    CONSTRAINT uq_table_new_id UNIQUE (table_name, new_id)
);

-- Create indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_mappings_table_old_id 
    ON migration.id_mappings(table_name, old_id);
CREATE INDEX IF NOT EXISTS idx_mappings_table_new_id 
    ON migration.id_mappings(table_name, new_id);
CREATE INDEX IF NOT EXISTS idx_mappings_batch 
    ON migration.id_mappings(migration_batch);

-- Create batch tracking table
CREATE TABLE IF NOT EXISTS migration.batch_log (
    batch_id VARCHAR(50) PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    started_at TIMESTAMP DEFAULT now(),
    completed_at TIMESTAMP,
    record_count INTEGER,
    status VARCHAR(20) DEFAULT 'in_progress',
    error_message TEXT,
    source_info JSONB
);

-- Helper function: Get new ID from old ID
CREATE OR REPLACE FUNCTION migration.get_new_id(
    p_table_name VARCHAR,
    p_old_id VARCHAR
) RETURNS UUID AS $$
DECLARE
    v_new_id UUID;
BEGIN
    SELECT new_id INTO v_new_id
    FROM migration.id_mappings
    WHERE table_name = p_table_name AND old_id = p_old_id;
    RETURN v_new_id;
END;
$$ LANGUAGE plpgsql;

-- Helper function: Check if record already migrated
CREATE OR REPLACE FUNCTION migration.is_migrated(
    p_table_name VARCHAR,
    p_old_id VARCHAR
) RETURNS BOOLEAN AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1 FROM migration.id_mappings
        WHERE table_name = p_table_name AND old_id = p_old_id
    );
END;
$$ LANGUAGE plpgsql;

-- Helper function: Deterministic UUID that passes v4 validation
-- Uses uuid_generate_v5 internally for determinism, then overwrites the
-- version nibble (position 15) from '5' to '4' so the result passes
-- any UUID-v4 format check the target application performs.
CREATE OR REPLACE FUNCTION migration.deterministic_uuid_v4(
    ns uuid,
    input text
) RETURNS uuid AS $$
  SELECT overlay(uuid_generate_v5(ns, input)::text placing '4' from 15 for 1)::uuid;
$$ LANGUAGE sql IMMUTABLE;

-- Progress summary view
CREATE OR REPLACE VIEW migration.progress_summary AS
SELECT 
    table_name,
    COUNT(*) as migrated_count,
    MIN(migrated_at) as first_migrated,
    MAX(migrated_at) as last_migrated,
    COUNT(DISTINCT migration_batch) as batch_count
FROM migration.id_mappings
GROUP BY table_name
ORDER BY table_name;

-- ============================================================
-- MIGRATION MAPPING TABLE SETUP COMPLETE
-- ============================================================


-- ============================================================
-- CREATE MAPPING TABLE FOR TRACKING
-- ============================================================
CREATE TABLE IF NOT EXISTS legacy_bot_to_agent_mapping (
    old_bot_id VARCHAR(255) PRIMARY KEY,
    new_agent_id UUID NOT NULL,
    agent_type VARCHAR(50),
    bot_name VARCHAR(255),
    migrated_at TIMESTAMP DEFAULT now()
);


-- Agent: Test Agent (bot_id: test_bot_123)
DO $agent_fn$
DECLARE
    v_agent_id uuid := migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'test_bot_123-agent');
    v_settings_id uuid := migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'test_bot_123-settings');
    v_user_id uuid := migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'test_user_456');
    v_kb_id uuid := migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'test_bot_123-kb');
    v_kb_assignment_id uuid := migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'test_bot_123-kb-assignment');
BEGIN
    -- Insert agent if not exists
    IF NOT EXISTS (SELECT 1 FROM agents WHERE id = v_agent_id) THEN
        INSERT INTO agents (
            id, name, description, type, user_id, avatar_url,
            is_active, is_public, is_prebuilt, is_draft,
            folder_id, created_at, updated_at, last_interacted_at, deleted_at
        ) VALUES (
            v_agent_id,
            'Test Agent',
            'Test Description',
            'cortex'::agents_type_enum,
            v_user_id,
            NULL,
            true,
            false,
            false,
            false,
            migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '1'),
            '2024-01-01T00:00:00'::timestamptz,
            '2024-01-02T00:00:00'::timestamptz,
            '2024-01-03T00:00:00'::timestamptz,
            NULL::timestamp
        );
    END IF;
    
    -- Insert agent settings if not exists
    IF NOT EXISTS (SELECT 1 FROM agent_settings WHERE agent_id = v_agent_id) THEN
        INSERT INTO agent_settings (
            id, agent_id, model, instructions, enabled_tools, conversation_starters,
            workflow_flow_id, base_answers_on_files_only, combines_multiple_answers,
            retrieved_context_size, re_rank_score, query_instructions,
            search_in_english, show_source_links, show_source_text,
            follow_up_questions, additional_links
        ) VALUES (
            v_settings_id,
            v_agent_id,
            'gpt-4',
            $INSTR$You are a helpful assistant$INSTR$,
            '[]'::jsonb,
            '["Hello! How can I help?"]'::jsonb,
            NULL::uuid,
            false,
            true,
            NULL::integer,
            NULL::numeric,
            NULL::text,
            false,
            false,
            false,
            false,
            false
        );
    END IF;

    -- Create knowledge base for this agent
    INSERT INTO knowledge_bases (
        id, name, description, similarity_top_k, re_rank_score,
        combines_multiple_answers, query_instructions,
        document_count_threshold, total_document_count, is_active, created_at, updated_at
    )
    SELECT
        v_kb_id,
        LEFT('Test Agent' || ' Knowledge Base', 128),
        NULL,
        NULL,
        NULL,
        true,
        NULL,
        20,
        4,
        true,
        now(),
        now()
    WHERE NOT EXISTS (
        SELECT 1 FROM knowledge_bases WHERE id = v_kb_id
    );

    -- Assign knowledge base to agent
    INSERT INTO knowledge_base_assignments (
        id, knowledge_base_id, assigned_to_id, assigned_to_type, is_active, created_at
    )
    SELECT
        v_kb_assignment_id,
        v_kb_id,
        v_agent_id,
        'agent'::knowledge_base_assignments_assigned_to_type_enum,
        true,
        now()
    WHERE NOT EXISTS (
        SELECT 1 FROM knowledge_base_assignments
        WHERE assigned_to_id = v_agent_id AND assigned_to_type = 'agent'
    );

    -- KB item (document): doc1
    INSERT INTO knowledge_base_items (id, knowledge_base_id, item_id, item_type, is_active, created_at)
    SELECT
        migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'test_bot_123-kb-item-doc1'),
        v_kb_id,
        migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, 'doc1'),
        'document'::knowledge_base_items_item_type_enum,
        true,
        now()
    WHERE NOT EXISTS (
        SELECT 1 FROM knowledge_base_items
        WHERE id = migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'test_bot_123-kb-item-doc1')
    );

    -- KB item (document): doc2
    INSERT INTO knowledge_base_items (id, knowledge_base_id, item_id, item_type, is_active, created_at)
    SELECT
        migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'test_bot_123-kb-item-doc2'),
        v_kb_id,
        migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, 'doc2'),
        'document'::knowledge_base_items_item_type_enum,
        true,
        now()
    WHERE NOT EXISTS (
        SELECT 1 FROM knowledge_base_items
        WHERE id = migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'test_bot_123-kb-item-doc2')
    );

    -- KB item (folder): 1
    INSERT INTO knowledge_base_items (id, knowledge_base_id, item_id, item_type, is_active, created_at)
    SELECT
        migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'test_bot_123-kb-item-folder-1'),
        v_kb_id,
        migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '1'),
        'folder'::knowledge_base_items_item_type_enum,
        true,
        now()
    WHERE NOT EXISTS (
        SELECT 1 FROM knowledge_base_items
        WHERE id = migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'test_bot_123-kb-item-folder-1')
    );

    -- KB item (folder): 2
    INSERT INTO knowledge_base_items (id, knowledge_base_id, item_id, item_type, is_active, created_at)
    SELECT
        migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'test_bot_123-kb-item-folder-2'),
        v_kb_id,
        migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '2'),
        'folder'::knowledge_base_items_item_type_enum,
        true,
        now()
    WHERE NOT EXISTS (
        SELECT 1 FROM knowledge_base_items
        WHERE id = migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'test_bot_123-kb-item-folder-2')
    );

    -- Track in migration.id_mappings
    INSERT INTO migration.id_mappings (table_name, old_id, new_id, migration_batch, notes)
    VALUES ('agents', 'test_bot_123', v_agent_id, 'agents_migration',
            'Type: cortex. KB items: 4')
    ON CONFLICT (table_name, old_id) DO NOTHING;
    
    -- Track in legacy mapping table
    INSERT INTO legacy_bot_to_agent_mapping (old_bot_id, new_agent_id, agent_type, bot_name)
    VALUES ('test_bot_123', v_agent_id, 'cortex', 'Test Agent')
    ON CONFLICT (old_bot_id) DO NOTHING;
    
    RAISE NOTICE 'Migrated agent: % (%) → %', 'Test Agent', 'cortex', v_agent_id;
END $agent_fn$;


-- ============================================================
-- MIGRATION SUMMARY
-- ============================================================
-- Agents processed: 1
-- Skipped (no bot_id): 0
-- ============================================================
