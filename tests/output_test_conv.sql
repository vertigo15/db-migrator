-- ============================================================
-- CONVERSATIONS, MESSAGES & MESSAGE_CONTENT_BLOCKS MIGRATION SQL
-- ============================================================
-- Generated: 2026-04-26T14:40:46.960034
-- Source: test-source
-- Destination: conversations + messages + message_content_blocks
-- Source rows: 3
-- 
-- IMPORTANT: This script will INSERT data into 3 tables!
-- IMPORTANT: Run users migration first.
--
-- Each source row creates entries in 3 tables:
--   1. conversations (aggregated per chat_id)
--   2. messages (user + assistant per row)
--   3. message_content_blocks (one per message)
--
-- Uses deterministic UUID generation (deterministic_uuid_v4).
-- Namespace UUID: 0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b
-- Multi-INSERT format: grouped by user, max 50 conversations per INSERT
-- ============================================================

-- Ensure PostgreSQL interprets this file as UTF-8 (required for Hebrew/multilingual content)
SET client_encoding = 'UTF8';

-- Ensure uuid-ossp extension is available
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- CONFIRMATION PROMPT
DO $$
DECLARE
    user_confirmation TEXT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'CONVERSATIONS/MESSAGES MIGRATION - CONFIRMATION REQUIRED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'This script will migrate conversations and messages';
    RAISE NOTICE 'Source rows: 3';
    RAISE NOTICE 'Namespace UUID: 0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b';
    RAISE NOTICE 'Generated: 2026-04-26T14:40:46.960274';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'PREREQUISITE: Users must be migrated first!';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';
    
    user_confirmation := NULL;
    
    IF current_setting('is_superuser') = 'off' THEN
        RAISE NOTICE 'Ready to proceed. Press Ctrl+C to cancel or Enter to continue...';
    END IF;
    
    RAISE NOTICE 'Starting migration...';
    RAISE NOTICE '';
END $$;

-- Uncomment for manual confirmation
-- \\prompt 'Type YES to confirm: ' user_confirmation
-- \\if :'user_confirmation' != 'YES'
--   \\echo 'Migration cancelled.'
--   \\quit
-- \\endif

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



-- User: user-hash-abc123 (Batch 1, 1 conversations)

-- Conversations INSERT
INSERT INTO conversations (id, title, message_count, total_tokens, is_active, deleted_at, created_at, updated_at, last_interacted_at, user_id)
SELECT * FROM (
  VALUES
    ('aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'::uuid, 'Test conversation', 6, 150, true, NULL::timestamp, '2026-01-01T10:00:00'::timestamptz, '2026-01-01T10:02:00'::timestamptz, '2026-01-01T10:02:00'::timestamptz, migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'user-hash-abc123'))
) AS v(id, title, message_count, total_tokens, is_active, deleted_at, created_at, updated_at, last_interacted_at, user_id)
WHERE NOT EXISTS (SELECT 1 FROM conversations WHERE id = v.id);

-- Messages INSERT
INSERT INTO messages (id, conversation_id, parent_message_id, role, has_tool_calls, iteration_count, content_block_count, finish_reason, created_at, updated_at, deleted_at, user_id, metadata)
SELECT * FROM (
  VALUES
    (migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0000-user'), 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'::uuid, NULL::uuid, 'user'::messages_role_enum, false, 1, 1, NULL::text, '2026-01-01T10:00:00'::timestamptz - interval '1 second', '2026-01-01T10:00:00'::timestamptz - interval '1 second', NULL::timestamp, migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'user-hash-abc123'), '{"message_order": 0, "turn_index": 0}'::jsonb),
    (migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0000-assistant'), 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'::uuid, migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0000-user'), 'assistant'::messages_role_enum, false, 1, 1, 'stop', '2026-01-01T10:00:00'::timestamptz, '2026-01-01T10:00:00'::timestamptz, NULL::timestamp, migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'user-hash-abc123'), '{"model": "gpt-4", "type": "chat", "bot_id": "bot-001", "is_like": ["positive"], "token_amount": 50, "words_amount": 10, "calculated_time": 120, "category": "general", "sentiment": "neutral", "message_order": 1, "turn_index": 0, "legacyData": {"legacy_log_id": "log-id-0000", "title": "Test conversation", "toolkit_settings": {"model": "gpt-4"}, "sourcetext": null, "sourcelink": null, "webpagelink": null, "documents_selected": null}}'::jsonb),
    (migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0001-user'), 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'::uuid, migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0000-assistant'), 'user'::messages_role_enum, false, 1, 1, NULL::text, '2026-01-01T10:01:00'::timestamptz - interval '1 second', '2026-01-01T10:01:00'::timestamptz - interval '1 second', NULL::timestamp, migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'user-hash-abc123'), '{"message_order": 2, "turn_index": 1}'::jsonb),
    (migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0001-assistant'), 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'::uuid, migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0001-user'), 'assistant'::messages_role_enum, false, 1, 1, 'stop', '2026-01-01T10:01:00'::timestamptz, '2026-01-01T10:01:00'::timestamptz, NULL::timestamp, migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'user-hash-abc123'), '{"model": "gpt-4", "type": "chat", "bot_id": "bot-001", "is_like": ["positive"], "token_amount": 50, "words_amount": 10, "calculated_time": 120, "category": "general", "sentiment": "neutral", "message_order": 3, "turn_index": 1, "legacyData": {"legacy_log_id": "log-id-0001", "title": "Test conversation", "toolkit_settings": {"model": "gpt-4"}, "sourcetext": null, "sourcelink": null, "webpagelink": null, "documents_selected": null}}'::jsonb),
    (migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0002-user'), 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'::uuid, migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0001-assistant'), 'user'::messages_role_enum, false, 1, 1, NULL::text, '2026-01-01T10:02:00'::timestamptz - interval '1 second', '2026-01-01T10:02:00'::timestamptz - interval '1 second', NULL::timestamp, migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'user-hash-abc123'), '{"message_order": 4, "turn_index": 2}'::jsonb),
    (migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0002-assistant'), 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'::uuid, migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0002-user'), 'assistant'::messages_role_enum, false, 1, 1, 'stop', '2026-01-01T10:02:00'::timestamptz, '2026-01-01T10:02:00'::timestamptz, NULL::timestamp, migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'user-hash-abc123'), '{"model": "gpt-4", "type": "chat", "bot_id": "bot-001", "is_like": ["positive"], "token_amount": 50, "words_amount": 10, "calculated_time": 120, "category": "general", "sentiment": "neutral", "message_order": 5, "turn_index": 2, "legacyData": {"legacy_log_id": "log-id-0002", "title": "Test conversation", "toolkit_settings": {"model": "gpt-4"}, "sourcetext": null, "sourcelink": null, "webpagelink": null, "documents_selected": null}}'::jsonb)
) AS v(id, conversation_id, parent_message_id, role, has_tool_calls, iteration_count, content_block_count, finish_reason, created_at, updated_at, deleted_at, user_id, metadata)
WHERE NOT EXISTS (SELECT 1 FROM messages WHERE id = v.id);

-- Message Content Blocks INSERT
INSERT INTO message_content_blocks (id, message_id, sequence, type, content, execution_time_ms, created_at)
SELECT * FROM (
  VALUES
    (migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0000-user-block-0'), migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0000-user'), 0, 'message'::message_content_blocks_type_enum, '{"role": "user", "type": "message", "content": [{"text": "Question 0", "type": "text"}]}'::jsonb, NULL::integer, '2026-01-01T10:00:00'::timestamptz - interval '1 second'),
    (migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0000-assistant-block-0'), migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0000-assistant'), 0, 'message'::message_content_blocks_type_enum, '{"role": "assistant", "type": "message", "content": [{"text": "Answer number 0", "type": "text"}]}'::jsonb, 120, '2026-01-01T10:00:00'::timestamptz),
    (migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0001-user-block-0'), migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0001-user'), 0, 'message'::message_content_blocks_type_enum, '{"role": "user", "type": "message", "content": [{"text": "Question 1", "type": "text"}]}'::jsonb, NULL::integer, '2026-01-01T10:01:00'::timestamptz - interval '1 second'),
    (migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0001-assistant-block-0'), migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0001-assistant'), 0, 'message'::message_content_blocks_type_enum, '{"role": "assistant", "type": "message", "content": [{"text": "Answer number 1", "type": "text"}]}'::jsonb, 120, '2026-01-01T10:01:00'::timestamptz),
    (migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0002-user-block-0'), migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0002-user'), 0, 'message'::message_content_blocks_type_enum, '{"role": "user", "type": "message", "content": [{"text": "Question 2", "type": "text"}]}'::jsonb, NULL::integer, '2026-01-01T10:02:00'::timestamptz - interval '1 second'),
    (migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0002-assistant-block-0'), migration.deterministic_uuid_v4('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'log-id-0002-assistant'), 0, 'message'::message_content_blocks_type_enum, '{"role": "assistant", "type": "message", "content": [{"text": "Answer number 2", "type": "text"}]}'::jsonb, 120, '2026-01-01T10:02:00'::timestamptz)
) AS v(id, message_id, sequence, type, content, execution_time_ms, created_at)
WHERE NOT EXISTS (SELECT 1 FROM message_content_blocks WHERE id = v.id);


-- ============================================================
-- MIGRATION SUMMARY
-- ============================================================
-- Users processed: 1
-- Conversations processed: 1
-- Messages processed: 6
-- Content blocks processed: 6
-- ============================================================
