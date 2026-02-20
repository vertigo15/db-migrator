-- ============================================================
-- DOCUMENTS MIGRATION SQL
-- ============================================================
-- Generated: 2026-02-19T15:36:57.962677
-- Source: jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)
-- Destination: user_db.public.documents
-- Records to migrate: 1
-- 
-- IMPORTANT: This script will INSERT documents into the target database!
-- IMPORTANT: Run users and folders migrations first (documents reference both).
--
-- Uses deterministic UUID generation (uuid_generate_v5) for folder_id conversion.
-- Namespace UUID: 0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b
-- Each INSERT checks if document already exists before inserting.
-- ============================================================

-- Ensure uuid-ossp extension is available
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- CONFIRMATION PROMPT: User must confirm before execution
DO $$
DECLARE
    user_confirmation TEXT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'DOCUMENTS MIGRATION - CONFIRMATION REQUIRED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'This script will migrate 1 documents to: user_db.public.documents';
    RAISE NOTICE 'Namespace UUID: 0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b';
    RAISE NOTICE 'Generated: 2026-02-19T15:36:57.962724';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'PREREQUISITE: Users and folders must be migrated first!';
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

