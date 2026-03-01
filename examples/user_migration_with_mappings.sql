-- ============================================================
-- EXAMPLE: USER MIGRATION WITH ID MAPPING TABLE
-- ============================================================
-- This example shows how to use migration.id_mappings table
-- for improved performance and tracking
-- ============================================================

-- STEP 1: Ensure mapping table exists
CREATE SCHEMA IF NOT EXISTS migration;
-- (Run schemas/migration_id_mappings.sql first)

-- STEP 2: Modified user insert with mapping table
-- User: john.doe@example.com
DO $$
DECLARE
    v_old_id VARCHAR := 'legacy_hash_abc123';
    v_email VARCHAR := 'john.doe@example.com';
    v_new_id UUID;
    v_org_id UUID := '356b50f7-bcbd-42aa-9392-e1605f42f7a1'::uuid;
BEGIN
    -- Check if already migrated using mapping table (FAST!)
    IF migration.is_migrated('users', v_old_id) THEN
        RAISE NOTICE 'User % already migrated (old_id: %)', v_email, v_old_id;
        RETURN;
    END IF;
    
    -- Generate new UUID
    v_new_id := gen_random_uuid();
    
    -- Insert user
    INSERT INTO user_db.public.users (
        id,
        email,
        first_name,
        last_name,
        username,
        metadata,
        organization_id,
        created_at,
        updated_at,
        is_owner
    ) VALUES (
        v_new_id,
        v_email,
        'John',
        'Doe',
        'johndoe',
        jsonb_build_object(
            'legacyData', jsonb_build_object(
                'id', v_old_id,
                'phone_number', '555-1234'
                -- ... other legacy fields
            )
        ),
        v_org_id,
        '2024-01-15T10:30:00'::timestamp,
        now(),
        false
    );
    
    -- Store mapping (CRUCIAL STEP!)
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
        'batch_20260228_001',
        'Migrated from V4 jeen_dev_users'
    );
    
    RAISE NOTICE 'Migrated user %: % → %', v_email, v_old_id, v_new_id;
END $$;

-- ============================================================
-- STEP 3: Folder migration using mapping for user lookup
-- ============================================================
DO $$
DECLARE
    v_old_folder_id VARCHAR := 'folder_xyz789';
    v_old_owner_id VARCHAR := 'legacy_hash_abc123';  -- User's old ID
    v_new_folder_id UUID;
    v_new_owner_id UUID;
    v_namespace UUID := '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid;
BEGIN
    -- Check if folder already migrated
    IF migration.is_migrated('folders', v_old_folder_id) THEN
        RAISE NOTICE 'Folder % already migrated', v_old_folder_id;
        RETURN;
    END IF;
    
    -- Lookup user's new ID from mapping table (FAST!)
    v_new_owner_id := migration.get_new_id('users', v_old_owner_id);
    
    IF v_new_owner_id IS NULL THEN
        RAISE NOTICE 'Skipping folder % - owner % not migrated', v_old_folder_id, v_old_owner_id;
        RETURN;
    END IF;
    
    -- Generate deterministic folder ID
    v_new_folder_id := uuid_generate_v5(v_namespace, v_old_folder_id);
    
    -- Insert folder
    INSERT INTO user_db.public.folders (
        id,
        folder_name,
        user_id,
        folder_type,
        created_at,
        updated_at
    ) VALUES (
        v_new_folder_id,
        'My Documents',
        v_new_owner_id,  -- Uses mapped user ID
        'default'::public.folders_folder_type_enum,
        '2024-01-15T11:00:00'::timestamp,
        now()
    );
    
    -- Store mapping
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch
    ) VALUES (
        'folders',
        v_old_folder_id,
        v_new_folder_id,
        'batch_20260228_001'
    );
    
    RAISE NOTICE 'Migrated folder: % → %', v_old_folder_id, v_new_folder_id;
END $$;

-- ============================================================
-- STEP 4: Document migration with BOTH user and folder lookups
-- ============================================================
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'doc_qwerty456';
    v_old_owner_id VARCHAR := 'legacy_hash_abc123';
    v_old_folder_id VARCHAR := 'folder_xyz789';
    v_new_doc_id UUID;
    v_new_owner_id UUID;
    v_new_folder_id UUID;
BEGIN
    -- Check if document already migrated
    IF migration.is_migrated('documents', v_old_doc_id) THEN
        RAISE NOTICE 'Document % already migrated', v_old_doc_id;
        RETURN;
    END IF;
    
    -- Lookup using mapping table - much faster than JSONB search!
    v_new_owner_id := migration.get_new_id('users', v_old_owner_id);
    v_new_folder_id := migration.get_new_id('folders', v_old_folder_id);
    
    IF v_new_owner_id IS NULL THEN
        RAISE NOTICE 'Skipping document % - owner not migrated', v_old_doc_id;
        RETURN;
    END IF;
    
    -- Generate new doc ID
    v_new_doc_id := gen_random_uuid();
    
    -- Insert document
    INSERT INTO user_db.public.documents (
        id,
        file_name,
        file_size,
        status,
        user_id,
        folder_id,
        storage_path,
        metadata,
        created_at,
        updated_at
    ) VALUES (
        v_new_doc_id,
        'report.pdf',
        1048576,
        'PROCESSED'::public.documents_status_enum,
        v_new_owner_id,     -- Mapped user ID
        v_new_folder_id,    -- Mapped folder ID
        v_old_doc_id,
        jsonb_build_object(
            'legacyData', jsonb_build_object(
                'doc_id', v_old_doc_id
            )
        ),
        '2024-01-15T12:00:00'::timestamp,
        now()
    );
    
    -- Store mapping
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch
    ) VALUES (
        'documents',
        v_old_doc_id,
        v_new_doc_id,
        'batch_20260228_001'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;

-- ============================================================
-- VERIFICATION QUERIES
-- ============================================================

-- Check migration progress
SELECT * FROM migration.progress_summary;

-- Find a specific user by old ID
SELECT 
    m.old_id as legacy_id,
    m.new_id as new_uuid,
    u.email,
    u.first_name,
    m.migrated_at
FROM migration.id_mappings m
JOIN user_db.public.users u ON u.id = m.new_id
WHERE m.table_name = 'users' AND m.old_id = 'legacy_hash_abc123';

-- Find all documents for a legacy user
SELECT 
    d.file_name,
    d.created_at,
    user_map.old_id as legacy_user_id,
    doc_map.old_id as legacy_doc_id
FROM user_db.public.documents d
JOIN migration.id_mappings user_map 
    ON d.user_id = user_map.new_id AND user_map.table_name = 'users'
JOIN migration.id_mappings doc_map 
    ON d.id = doc_map.new_id AND doc_map.table_name = 'documents'
WHERE user_map.old_id = 'legacy_hash_abc123';

-- Check for orphaned records (no mapping)
SELECT 
    u.id,
    u.email,
    u.created_at
FROM user_db.public.users u
LEFT JOIN migration.id_mappings m 
    ON u.id = m.new_id AND m.table_name = 'users'
WHERE m.id IS NULL
    AND u.metadata ? 'legacyData'  -- Should have been migrated
LIMIT 10;
