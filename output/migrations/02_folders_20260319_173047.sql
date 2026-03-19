-- ============================================================
-- FOLDERS MIGRATION SQL
-- ============================================================
-- Generated: 2026-03-19T17:30:49.875788
-- Source: jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)
-- Destination: document_db.public.folders
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
    RAISE NOTICE 'FOLDERS MIGRATION - CONFIRMATION REQUIRED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'This script will migrate 1 records to: document_db.public.folders';
    
    RAISE NOTICE 'Generated: 2026-03-19T17:30:49.875788';
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

-- Ensure uuid-ossp extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Start batch tracking
INSERT INTO migration.batch_log (batch_id, table_name, record_count, source_info)
VALUES ('folders_20260319_173049', 'folders', 1, '{"source": "jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)"}'::jsonb)
ON CONFLICT (batch_id) DO NOTHING;


-- Folder: Ariel TEsttt (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_folder_id VARCHAR := '1398';
    v_old_owner_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708';
    v_folder_id uuid := uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, v_old_folder_id);
    v_user_id uuid := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'e994b100cd7b6327b45618f254d1b708');
BEGIN
    -- Check if folder already migrated using mapping table (FAST)
    IF migration.is_migrated('folders', v_old_folder_id) THEN
        RAISE NOTICE 'Folder % already migrated', v_old_folder_id;
        RETURN;
    END IF;
    
    -- Insert folder
    INSERT INTO public.folders (
        id,
        folder_name,
        parent_id,
        folder_type,
        user_id,
        created_at,
        updated_at,
        deleted_at
    ) VALUES (
        v_folder_id,
        'Ariel TEsttt',
        NULL,
        'document'::public.folders_folder_type_enum,
        v_user_id,
        '2026-03-17T14:27:15.221381',
        now(),
        NULL
    )
    ON CONFLICT (id) DO NOTHING;
    
    -- Store folder ID mapping
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch
    ) VALUES (
        'folders',
        v_old_folder_id,
        v_folder_id,
        'folders_20260319_173049'
    );
    
    RAISE NOTICE 'Migrated folder: % → %', v_old_folder_id, v_folder_id;
END $$;

-- Complete batch tracking
UPDATE migration.batch_log 
SET completed_at = now(), status = 'completed' 
WHERE batch_id = 'folders_20260319_173049';

-- Total folders processed: 1
-- Skipped (no ID): 0
-- Note: Folders inserted in parent-first order using deterministic UUIDs (uuid_generate_v5)
-- Namespace UUID: 0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b
