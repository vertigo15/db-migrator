-- ============================================================
-- FOLDERS MIGRATION SQL
-- ============================================================
-- Generated: 2026-02-19T15:36:57.276936
-- Source: jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)
-- Destination: user_db.public.folders
-- Records to migrate: 1
-- 
-- IMPORTANT: This script will INSERT folders into the target database!
-- IMPORTANT: Folders are inserted in parent-first order to maintain relationships.
--
-- Uses deterministic UUID generation (uuid_generate_v5) to preserve parent-child links.
-- Namespace UUID: 0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b
-- Each INSERT checks if folder already exists before inserting.
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
    RAISE NOTICE 'FOLDERS MIGRATION - CONFIRMATION REQUIRED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'This script will migrate 1 folders to: user_db.public.folders';
    RAISE NOTICE 'Namespace UUID: 0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b';
    RAISE NOTICE 'Generated: 2026-02-19T15:36:57.276977';
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


-- Folder: adi test (owner: de0ff05457533c93fdf3e0d1cdd0f808)
DO $$
DECLARE
    v_folder_id uuid := uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '1168');
    v_user_id uuid;
BEGIN
    -- Lookup user_id from migrated users via legacy owner_id
    SELECT id INTO v_user_id
    FROM user_db.public.users
    WHERE metadata->'legacyData'->>'id' = 'de0ff05457533c93fdf3e0d1cdd0f808';
    
    -- Skip if user not found
    IF v_user_id IS NULL THEN
        RAISE NOTICE 'Skipping folder % - user % not found', '1168', 'de0ff05457533c93fdf3e0d1cdd0f808';
        RETURN;
    END IF;
    
    -- Insert folder if not exists
    IF NOT EXISTS (
        SELECT 1 FROM user_db.public.folders
        WHERE id = v_folder_id
    ) THEN
        INSERT INTO user_db.public.folders (
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
            'adi test',
            NULL,
            'document'::public.folders_folder_type_enum,
            v_user_id,
            '2025-08-26T11:25:49.823742',
            now(),
            NULL
        );
    END IF;
END $$;


-- Total folders processed: 1
-- Skipped (no ID): 0
