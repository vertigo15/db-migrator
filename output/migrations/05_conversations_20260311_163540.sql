-- ============================================================
-- CONVERSATIONS, MESSAGES & MESSAGE_CONTENT_BLOCKS MIGRATION SQL
-- ============================================================
-- Generated: 2026-03-11T16:35:46.956810
-- Source: jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)
-- Destination: conversations + messages + message_content_blocks
-- Source rows: 12
-- 
-- IMPORTANT: This script will INSERT data into 3 tables!
-- IMPORTANT: Run users migration first.
--
-- Each source row creates entries in 3 tables:
--   1. conversations (aggregated per chat_id)
--   2. messages (user + assistant per row)
--   3. message_content_blocks (one per message)
--
-- Uses deterministic UUID generation (uuid_generate_v5).
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
    RAISE NOTICE 'Source rows: 12';
    RAISE NOTICE 'Namespace UUID: 0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b';
    RAISE NOTICE 'Generated: 2026-03-11T16:35:46.956991';
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



-- User: de0ff05457533c93fdf3e0d1cdd0f808 (Batch 1, 2 conversations)

-- Conversations INSERT
INSERT INTO conversations (id, title, message_count, total_tokens, is_active, deleted_at, created_at, updated_at, last_interacted_at, user_id)
SELECT * FROM (
  VALUES
    ('7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, 'Conversation 7929aa9a', 16, 440, true, NULL::timestamp, '2025-08-25T11:50:33.993556'::timestamptz, '2025-08-25T11:51:45.649515'::timestamptz, '2025-08-25T11:51:45.649515'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808')),
    ('a6030c5e-8ab0-42db-8dda-ddb30d53088a'::uuid, 'Conversation a6030c5e', 8, 198, true, NULL::timestamp, '2025-08-25T11:53:51.440362'::timestamptz, '2025-08-25T11:58:19.268601'::timestamptz, '2025-08-25T11:58:19.268601'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'))
) AS v(id, title, message_count, total_tokens, is_active, deleted_at, created_at, updated_at, last_interacted_at, user_id)
WHERE NOT EXISTS (SELECT 1 FROM conversations WHERE id = v.id);

