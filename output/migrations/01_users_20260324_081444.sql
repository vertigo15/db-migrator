-- ============================================================
-- USERS MIGRATION SQL
-- ============================================================
-- Generated: 2026-03-24T08:14:45.480970
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
    RAISE NOTICE 'Organization ID: d8578dff-3465-4b81-8b0f-ce1a83efc21b';
    RAISE NOTICE 'Generated: 2026-03-24T08:14:45.480970';
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
VALUES ('users_20260324_081445', 'users', 1, '{"source": "jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)"}'::jsonb)
ON CONFLICT (batch_id) DO NOTHING;


-- User: arielgur99@gmail.com
DO $$
DECLARE
    v_old_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708';
    v_email VARCHAR := 'arielgur99@gmail.com';
    v_new_id UUID;
BEGIN
    -- Check if already migrated using mapping table (FAST)
    IF migration.is_migrated('users', v_old_id) THEN
        RAISE NOTICE 'User % already migrated (old_id: %)', v_email, v_old_id;
        RETURN;
    END IF;
    
    -- Generate deterministic UUID (same namespace+input = same UUID across all databases)
    v_new_id := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, v_old_id);
    
    -- Insert user (handle all unique constraint conflicts)
    BEGIN
        INSERT INTO public.users (
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
            organization_id
        ) VALUES (
            v_new_id,
            'arielgur99@gmail.com',
            'arielgur99',
            NULL,
            'arielgur99',
            NULL,
            '{"legacyData": {"id": "e994b100cd7b6327b45618f254d1b708", "job": null, "model": ["gpt-5", "gpt-5-mini", "gpt-5-nano", "gpt-5.1", "gpt-4o"], "group_id": "27", "azure_oid": null, "department": null, "token_used": "56932", "words_used": "19278", "subfeatures": {"reasoning": false, "control_panel": true, "reasoning_web": true, "see_all_agents": false, "internet_access": false, "create_new_agent": false, "read_aloud_message": false, "organizational_files": false}, "token_limit": "2500000", "company_name": null, "phone_number": null, "last_connected": "1774275323038", "letter_checkbox": null, "times_connected": "40", "enabled_features": ["chat", "admin", "voice", "sources", "interactive", "workflow"], "history_categories": ["tech", "tools", "ai"], "company_name_in_hebrew": null}}'::jsonb,
            '2026-02-23T11:50:16.848092',
            now(),
            NULL,
            NULL,
            'd8578dff-3465-4b81-8b0f-ce1a83efc21b'::uuid
        )
        ON CONFLICT (email) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            metadata = EXCLUDED.metadata,
            updated_at = now()
        RETURNING id INTO v_new_id;
    EXCEPTION WHEN unique_violation THEN
        -- Username conflict — check if this user already exists by email
        SELECT id INTO v_new_id FROM public.users WHERE email = v_email;
        IF v_new_id IS NOT NULL THEN
            RAISE NOTICE 'User % already exists (matched by email), reusing id %', v_email, v_new_id;
        ELSE
            -- User doesn't exist yet, username is taken — retry with email as username
            v_new_id := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, v_old_id);
            RAISE NOTICE 'User %: username conflict, using email as username instead', v_email;
            INSERT INTO public.users (
                id, email, first_name, last_name, username, avatar_url,
                metadata, created_at, updated_at, deleted_at, zitadel_user_id,
                organization_id
            ) VALUES (
                v_new_id,
                'arielgur99@gmail.com',
                'arielgur99',
                NULL,
                'arielgur99@gmail.com',
                NULL, '{"legacyData": {"id": "e994b100cd7b6327b45618f254d1b708", "job": null, "model": ["gpt-5", "gpt-5-mini", "gpt-5-nano", "gpt-5.1", "gpt-4o"], "group_id": "27", "azure_oid": null, "department": null, "token_used": "56932", "words_used": "19278", "subfeatures": {"reasoning": false, "control_panel": true, "reasoning_web": true, "see_all_agents": false, "internet_access": false, "create_new_agent": false, "read_aloud_message": false, "organizational_files": false}, "token_limit": "2500000", "company_name": null, "phone_number": null, "last_connected": "1774275323038", "letter_checkbox": null, "times_connected": "40", "enabled_features": ["chat", "admin", "voice", "sources", "interactive", "workflow"], "history_categories": ["tech", "tools", "ai"], "company_name_in_hebrew": null}}'::jsonb, '2026-02-23T11:50:16.848092', now(), NULL, NULL,
                'd8578dff-3465-4b81-8b0f-ce1a83efc21b'::uuid
            )
            ON CONFLICT (email) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                metadata = EXCLUDED.metadata,
                updated_at = now()
            RETURNING id INTO v_new_id;
        END IF;
    END;
    
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
        'users_20260324_081445',
        'Migrated from V4 users table'
    );
    
    RAISE NOTICE 'Migrated user %: % → %', v_email, v_old_id, v_new_id;
END $$;

-- Complete batch tracking
UPDATE migration.batch_log 
SET completed_at = now(), status = 'completed' 
WHERE batch_id = 'users_20260324_081445';

-- Total records processed: 1
-- Skipped (no email): 0
