-- ============================================================
-- FOLDERS MIGRATION SQL
-- ============================================================
-- Generated: 2026-02-17T12:58:09.385252
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
    RAISE NOTICE 'Generated: 2026-02-17T12:58:09.385314';
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


-- Folder: חדש פלקס וטקסט (owner: f2442a210e4377ab4ecf6a636cda59ed)
DO $$
DECLARE
    v_folder_id uuid := uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '605');
    v_user_id uuid;
BEGIN
    -- Lookup user_id from migrated users via legacy owner_id
    SELECT id INTO v_user_id
    FROM user_db.public.users
    WHERE metadata->'legacyData'->>'id' = 'f2442a210e4377ab4ecf6a636cda59ed';
    
    -- Skip if user not found
    IF v_user_id IS NULL THEN
        RAISE NOTICE 'Skipping folder % - user % not found', '605', 'f2442a210e4377ab4ecf6a636cda59ed';
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
            'חדש פלקס וטקסט',
            NULL,
            'document'::public.folders_folder_type_enum,
            v_user_id,
            '2024-11-14T12:39:24.319464',
            now(),
            NULL
        );
    END IF;
END $$;


-- Total folders processed: 1
-- Skipped (no ID): 0
