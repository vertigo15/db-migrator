-- ============================================================
-- USERS MIGRATION SQL
-- ============================================================
-- Generated: 2026-03-02T14:15:34.154602
-- Source: jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)
-- Destination: user_db.public.users
-- Records to migrate: 1
-- 
-- IMPORTANT: This script will INSERT records into the target database!
-- IMPORTANT: Review organization_id and other constants before execution!
--
-- Each INSERT checks if record already exists before inserting.
-- Uses migration.id_mappings table for fast ID lookups and tracking.
-- ============================================================

-- Ensure PostgreSQL interprets this file as UTF-8 (required for Hebrew/multilingual content)
SET client_encoding = 'UTF8';

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

-- CONFIRMATION PROMPT: User must confirm before execution
DO $$
DECLARE
    user_confirmation TEXT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'USERS MIGRATION - CONFIRMATION REQUIRED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'This script will migrate 1 records to: user_db.public.users';
    RAISE NOTICE 'Organization ID: 356b50f7-bcbd-42aa-9392-e1605f42f7a1';
    RAISE NOTICE 'Generated: 2026-03-02T14:15:34.154602';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';
    
    user_confirmation := NULL;
    
    IF current_setting('is_superuser') = 'off' THEN
        RAISE NOTICE 'Ready to proceed. Press Ctrl+C to cancel or Enter to continue...';
    END IF;
    
    RAISE NOTICE 'Starting migration...';
    RAISE NOTICE '';
END $$;

-- Uncomment the lines below to require manual confirmation (recommended for first run)
-- Note: These are psql meta-commands that work in interactive psql sessions
-- \\prompt 'Type YES to confirm and continue with migration: ' user_confirmation
-- \\if :'user_confirmation' != 'YES'
--   \\echo 'Migration cancelled by user.'
--   \\quit
-- \\endif

-- Ensure UUID extensions are available
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Start batch tracking
INSERT INTO migration.batch_log (batch_id, table_name, record_count, source_info)
VALUES ('users_20260302_141534', 'users', 1, '{"source": "jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)"}'::jsonb)
ON CONFLICT (batch_id) DO NOTHING;


-- User: adi@jeen.ai
DO $$
DECLARE
    v_old_id VARCHAR := 'de0ff05457533c93fdf3e0d1cdd0f808';
    v_email VARCHAR := 'adi@jeen.ai';
    v_new_id UUID;
BEGIN
    -- Check if already migrated using mapping table (FAST)
    IF migration.is_migrated('users', v_old_id) THEN
        RAISE NOTICE 'User % already migrated (old_id: %)', v_email, v_old_id;
        RETURN;
    END IF;
    
    -- Generate deterministic UUID (same namespace+input = same UUID across all databases)
    v_new_id := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, v_old_id);
    
    -- Insert user
    INSERT INTO user_db.public.users (
        id,
        email,
        first_name,
        last_name,
        username,
        avatar_url,
        metadata,
        created_at,
        updated_at,
        deleted_at,
        zitadel_user_id,
        organization_id,
        is_owner,
        preferred_language
    ) VALUES (
        v_new_id,
        'adi@jeen.ai',
        'adi',
        NULL,
        'adi',
        NULL,
        '{"legacyData": {"id": "de0ff05457533c93fdf3e0d1cdd0f808", "job": null, "model": ["gemini-2.5-pro-preview-06-05", "gpt-oss-120b", "gpt-5", "gpt-5-mini", "gpt-5-nano", "gpt-5.1", "gpt-4o"], "group_id": "1", "azure_oid": null, "department": null, "token_used": "287", "words_used": "141", "subfeatures": {"reasoning": false, "web_search": true, "control_panel": true, "reasoning_web": true, "see_all_agents": false, "create_new_agent": true, "read_aloud_message": false, "organizational_files": false}, "token_limit": "1000000", "company_name": null, "phone_number": null, "last_connected": "1770025989837", "letter_checkbox": null, "times_connected": "11", "enabled_features": ["admin", "voice", "sources", "automation", "chat", "workflow", "interactive"], "history_categories": ["tech", "tools", "ai"], "company_name_in_hebrew": null}}'::jsonb,
        '2025-08-25T07:15:18.828417',
        now(),
        NULL,
        NULL,
        '356b50f7-bcbd-42aa-9392-e1605f42f7a1'::uuid,
        false,
        NULL
    );
    
    -- Store ID mapping for fast future lookups
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch,
        notes
    ) VALUES (
        'users',
        v_old_id,
        v_new_id,
        'users_20260302_141534',
        'Migrated from V4 users table'
    );
    
    RAISE NOTICE 'Migrated user %: % → %', v_email, v_old_id, v_new_id;
END $$;

-- Complete batch tracking
UPDATE migration.batch_log 
SET completed_at = now(), status = 'completed' 
WHERE batch_id = 'users_20260302_141534';

-- Total records processed: 1
-- Skipped (no email): 0
