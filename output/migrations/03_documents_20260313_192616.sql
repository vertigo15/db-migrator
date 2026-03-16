-- ============================================================
-- DOCUMENTS MIGRATION SQL
-- ============================================================
-- Generated: 2026-03-13T19:26:19.689014
-- Source: jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)
-- Destination: document_db.public.documents
-- Records to migrate: 13
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
    RAISE NOTICE 'DOCUMENTS MIGRATION - CONFIRMATION REQUIRED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'This script will migrate 13 records to: document_db.public.documents';
    
    RAISE NOTICE 'Generated: 2026-03-13T19:26:19.689014';
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
-- Note: gen_random_uuid() is built-in for PostgreSQL 13+
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Start batch tracking
INSERT INTO migration.batch_log (batch_id, table_name, record_count, source_info)
VALUES ('documents_20260313_192619', 'documents', 13, '{"source": "jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)", "namespace_uuid": "0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b"}'::jsonb)
ON CONFLICT (batch_id) DO NOTHING;

-- IMPORTANT: Users and folders must be migrated FIRST!
-- Documents reference both users (owner_id) and folders (folder_id)


-- Document: Galaxy S23 Ultra.docx (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708/data/1771853596192-Galaxy S23 Ultra.docx';
    v_old_owner_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := uuid_generate_v5('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'e994b100cd7b6327b45618f254d1b708');
    v_folder_id UUID;
BEGIN
    -- Check if document already migrated using mapping table (FAST)
    IF migration.is_migrated('documents', v_old_doc_id) THEN
        RAISE NOTICE 'Document % already migrated', v_old_doc_id;
        RETURN;
    END IF;
    
    -- Lookup folder via mapping table if folder specified (same DB - document_db)
    IF v_old_folder_id IS NOT NULL THEN
        v_folder_id := migration.get_new_id('folders', v_old_folder_id);
    END IF;
    
    -- Insert document
    INSERT INTO document_db.public.documents (
        id,
        status,
        file_name,
        file_size,
        storage_type,
        storage_path,
        storage_id,
        metadata,
        created_at,
        updated_at,
        deleted_at,
        folder_id,
        user_id,
        content_type,
        parsing_technique_id,
        source_type,
        organization_id
    ) VALUES (
        v_new_doc_id,
        'PROCESSED'::public.documents_status_enum,
        'Galaxy S23 Ultra.docx',
        30348,
        'azure',
        'e994b100cd7b6327b45618f254d1b708/data/1771853596192-Galaxy S23 Ultra.docx',
        NULL,
        '{"name": "Galaxy S23 Ultra.docx", "source": "legacy-migration", "legacyData": {"doc_id": "e994b100cd7b6327b45618f254d1b708/data/1771853596192-Galaxy S23 Ultra.docx", "doc_title": "Galaxy S23 Ultra.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "1ea69035241fe0b825988b5f678022ec6504436115e37d438d2b3c8d6dc76199", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-02-23T13:33:10.503649',
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    )
    ON CONFLICT (id) DO NOTHING;
    
    -- Store document ID mapping
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch
    ) VALUES (
        'documents',
        v_old_doc_id,
        v_new_doc_id,
        'documents_20260313_192619'
    )
    ON CONFLICT (table_name, old_id) DO NOTHING;
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: Galaxy S23.docx (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708/data/1771853597373-Galaxy S23.docx';
    v_old_owner_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := uuid_generate_v5('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'e994b100cd7b6327b45618f254d1b708');
    v_folder_id UUID;
BEGIN
    -- Check if document already migrated using mapping table (FAST)
    IF migration.is_migrated('documents', v_old_doc_id) THEN
        RAISE NOTICE 'Document % already migrated', v_old_doc_id;
        RETURN;
    END IF;
    
    -- Lookup folder via mapping table if folder specified (same DB - document_db)
    IF v_old_folder_id IS NOT NULL THEN
        v_folder_id := migration.get_new_id('folders', v_old_folder_id);
    END IF;
    
    -- Insert document
    INSERT INTO document_db.public.documents (
        id,
        status,
        file_name,
        file_size,
        storage_type,
        storage_path,
        storage_id,
        metadata,
        created_at,
        updated_at,
        deleted_at,
        folder_id,
        user_id,
        content_type,
        parsing_technique_id,
        source_type,
        organization_id
    ) VALUES (
        v_new_doc_id,
        'PROCESSED'::public.documents_status_enum,
        'Galaxy S23.docx',
        29369,
        'azure',
        'e994b100cd7b6327b45618f254d1b708/data/1771853597373-Galaxy S23.docx',
        NULL,
        '{"name": "Galaxy S23.docx", "source": "legacy-migration", "legacyData": {"doc_id": "e994b100cd7b6327b45618f254d1b708/data/1771853597373-Galaxy S23.docx", "doc_title": "Galaxy S23.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "271713bbab705836ae4a056a1e8d73da120a0197d12b7d5c5de75af30f6adebc", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-02-23T13:33:11.187161',
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    )
    ON CONFLICT (id) DO NOTHING;
    
    -- Store document ID mapping
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch
    ) VALUES (
        'documents',
        v_old_doc_id,
        v_new_doc_id,
        'documents_20260313_192619'
    )
    ON CONFLICT (table_name, old_id) DO NOTHING;
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: Galaxy S23+.docx (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708/data/1771853597998-Galaxy S23+.docx';
    v_old_owner_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := uuid_generate_v5('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'e994b100cd7b6327b45618f254d1b708');
    v_folder_id UUID;
BEGIN
    -- Check if document already migrated using mapping table (FAST)
    IF migration.is_migrated('documents', v_old_doc_id) THEN
        RAISE NOTICE 'Document % already migrated', v_old_doc_id;
        RETURN;
    END IF;
    
    -- Lookup folder via mapping table if folder specified (same DB - document_db)
    IF v_old_folder_id IS NOT NULL THEN
        v_folder_id := migration.get_new_id('folders', v_old_folder_id);
    END IF;
    
    -- Insert document
    INSERT INTO document_db.public.documents (
        id,
        status,
        file_name,
        file_size,
        storage_type,
        storage_path,
        storage_id,
        metadata,
        created_at,
        updated_at,
        deleted_at,
        folder_id,
        user_id,
        content_type,
        parsing_technique_id,
        source_type,
        organization_id
    ) VALUES (
        v_new_doc_id,
        'PROCESSED'::public.documents_status_enum,
        'Galaxy S23+.docx',
        30591,
        'azure',
        'e994b100cd7b6327b45618f254d1b708/data/1771853597998-Galaxy S23+.docx',
        NULL,
        '{"name": "Galaxy S23+.docx", "source": "legacy-migration", "legacyData": {"doc_id": "e994b100cd7b6327b45618f254d1b708/data/1771853597998-Galaxy S23+.docx", "doc_title": "Galaxy S23+.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "4ac1c01e4ede1df7e897b743c3e674288607f9e9d41c86211d1fcc2a69b029c8", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-02-23T13:33:11.805318',
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    )
    ON CONFLICT (id) DO NOTHING;
    
    -- Store document ID mapping
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch
    ) VALUES (
        'documents',
        v_old_doc_id,
        v_new_doc_id,
        'documents_20260313_192619'
    )
    ON CONFLICT (table_name, old_id) DO NOTHING;
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: Galaxy S24 plus.docx (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708/data/1771853598604-Galaxy S24 plus.docx';
    v_old_owner_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := uuid_generate_v5('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'e994b100cd7b6327b45618f254d1b708');
    v_folder_id UUID;
BEGIN
    -- Check if document already migrated using mapping table (FAST)
    IF migration.is_migrated('documents', v_old_doc_id) THEN
        RAISE NOTICE 'Document % already migrated', v_old_doc_id;
        RETURN;
    END IF;
    
    -- Lookup folder via mapping table if folder specified (same DB - document_db)
    IF v_old_folder_id IS NOT NULL THEN
        v_folder_id := migration.get_new_id('folders', v_old_folder_id);
    END IF;
    
    -- Insert document
    INSERT INTO document_db.public.documents (
        id,
        status,
        file_name,
        file_size,
        storage_type,
        storage_path,
        storage_id,
        metadata,
        created_at,
        updated_at,
        deleted_at,
        folder_id,
        user_id,
        content_type,
        parsing_technique_id,
        source_type,
        organization_id
    ) VALUES (
        v_new_doc_id,
        'PROCESSED'::public.documents_status_enum,
        'Galaxy S24 plus.docx',
        35270,
        'azure',
        'e994b100cd7b6327b45618f254d1b708/data/1771853598604-Galaxy S24 plus.docx',
        NULL,
        '{"name": "Galaxy S24 plus.docx", "source": "legacy-migration", "legacyData": {"doc_id": "e994b100cd7b6327b45618f254d1b708/data/1771853598604-Galaxy S24 plus.docx", "doc_title": "Galaxy S24 plus.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "1fe3501e635c001395aeccd0989ea003ae5da3e84eb58420e6ed5b3618a0fb1e", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-02-23T13:33:12.369588',
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    )
    ON CONFLICT (id) DO NOTHING;
    
    -- Store document ID mapping
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch
    ) VALUES (
        'documents',
        v_old_doc_id,
        v_new_doc_id,
        'documents_20260313_192619'
    )
    ON CONFLICT (table_name, old_id) DO NOTHING;
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: Galaxy S24 Ultra.docx (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708/data/1771853599203-Galaxy S24 Ultra.docx';
    v_old_owner_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := uuid_generate_v5('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'e994b100cd7b6327b45618f254d1b708');
    v_folder_id UUID;
BEGIN
    -- Check if document already migrated using mapping table (FAST)
    IF migration.is_migrated('documents', v_old_doc_id) THEN
        RAISE NOTICE 'Document % already migrated', v_old_doc_id;
        RETURN;
    END IF;
    
    -- Lookup folder via mapping table if folder specified (same DB - document_db)
    IF v_old_folder_id IS NOT NULL THEN
        v_folder_id := migration.get_new_id('folders', v_old_folder_id);
    END IF;
    
    -- Insert document
    INSERT INTO document_db.public.documents (
        id,
        status,
        file_name,
        file_size,
        storage_type,
        storage_path,
        storage_id,
        metadata,
        created_at,
        updated_at,
        deleted_at,
        folder_id,
        user_id,
        content_type,
        parsing_technique_id,
        source_type,
        organization_id
    ) VALUES (
        v_new_doc_id,
        'PROCESSED'::public.documents_status_enum,
        'Galaxy S24 Ultra.docx',
        35995,
        'azure',
        'e994b100cd7b6327b45618f254d1b708/data/1771853599203-Galaxy S24 Ultra.docx',
        NULL,
        '{"name": "Galaxy S24 Ultra.docx", "source": "legacy-migration", "legacyData": {"doc_id": "e994b100cd7b6327b45618f254d1b708/data/1771853599203-Galaxy S24 Ultra.docx", "doc_title": "Galaxy S24 Ultra.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "f32921a05a95a62245915dd7a1481128b7a1b8bfe93a9ea9677c1453dac7dbca", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-02-23T13:33:13.063778',
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    )
    ON CONFLICT (id) DO NOTHING;
    
    -- Store document ID mapping
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch
    ) VALUES (
        'documents',
        v_old_doc_id,
        v_new_doc_id,
        'documents_20260313_192619'
    )
    ON CONFLICT (table_name, old_id) DO NOTHING;
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: Galaxy S24.docx (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708/data/1771853599857-Galaxy S24.docx';
    v_old_owner_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := uuid_generate_v5('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'e994b100cd7b6327b45618f254d1b708');
    v_folder_id UUID;
BEGIN
    -- Check if document already migrated using mapping table (FAST)
    IF migration.is_migrated('documents', v_old_doc_id) THEN
        RAISE NOTICE 'Document % already migrated', v_old_doc_id;
        RETURN;
    END IF;
    
    -- Lookup folder via mapping table if folder specified (same DB - document_db)
    IF v_old_folder_id IS NOT NULL THEN
        v_folder_id := migration.get_new_id('folders', v_old_folder_id);
    END IF;
    
    -- Insert document
    INSERT INTO document_db.public.documents (
        id,
        status,
        file_name,
        file_size,
        storage_type,
        storage_path,
        storage_id,
        metadata,
        created_at,
        updated_at,
        deleted_at,
        folder_id,
        user_id,
        content_type,
        parsing_technique_id,
        source_type,
        organization_id
    ) VALUES (
        v_new_doc_id,
        'PROCESSED'::public.documents_status_enum,
        'Galaxy S24.docx',
        34922,
        'azure',
        'e994b100cd7b6327b45618f254d1b708/data/1771853599857-Galaxy S24.docx',
        NULL,
        '{"name": "Galaxy S24.docx", "source": "legacy-migration", "legacyData": {"doc_id": "e994b100cd7b6327b45618f254d1b708/data/1771853599857-Galaxy S24.docx", "doc_title": "Galaxy S24.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "659997c6f21a911a16337cf7d60cd0303487efedd219c66cda64d9e55b71fa48", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-02-23T13:33:13.581975',
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    )
    ON CONFLICT (id) DO NOTHING;
    
    -- Store document ID mapping
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch
    ) VALUES (
        'documents',
        v_old_doc_id,
        v_new_doc_id,
        'documents_20260313_192619'
    )
    ON CONFLICT (table_name, old_id) DO NOTHING;
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: Galaxy Z Flip6.docx (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708/data/1771853600571-Galaxy Z Flip6.docx';
    v_old_owner_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := uuid_generate_v5('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'e994b100cd7b6327b45618f254d1b708');
    v_folder_id UUID;
BEGIN
    -- Check if document already migrated using mapping table (FAST)
    IF migration.is_migrated('documents', v_old_doc_id) THEN
        RAISE NOTICE 'Document % already migrated', v_old_doc_id;
        RETURN;
    END IF;
    
    -- Lookup folder via mapping table if folder specified (same DB - document_db)
    IF v_old_folder_id IS NOT NULL THEN
        v_folder_id := migration.get_new_id('folders', v_old_folder_id);
    END IF;
    
    -- Insert document
    INSERT INTO document_db.public.documents (
        id,
        status,
        file_name,
        file_size,
        storage_type,
        storage_path,
        storage_id,
        metadata,
        created_at,
        updated_at,
        deleted_at,
        folder_id,
        user_id,
        content_type,
        parsing_technique_id,
        source_type,
        organization_id
    ) VALUES (
        v_new_doc_id,
        'PROCESSED'::public.documents_status_enum,
        'Galaxy Z Flip6.docx',
        32352,
        'azure',
        'e994b100cd7b6327b45618f254d1b708/data/1771853600571-Galaxy Z Flip6.docx',
        NULL,
        '{"name": "Galaxy Z Flip6.docx", "source": "legacy-migration", "legacyData": {"doc_id": "e994b100cd7b6327b45618f254d1b708/data/1771853600571-Galaxy Z Flip6.docx", "doc_title": "Galaxy Z Flip6.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "2afbac003a827f734f3393489b0e8e3f3c7a1f2d92e5df609b2a0333174e913d", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-02-23T13:33:14.444292',
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    )
    ON CONFLICT (id) DO NOTHING;
    
    -- Store document ID mapping
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch
    ) VALUES (
        'documents',
        v_old_doc_id,
        v_new_doc_id,
        'documents_20260313_192619'
    )
    ON CONFLICT (table_name, old_id) DO NOTHING;
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: Galaxy Z Fold6.docx (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708/data/1771853601236-Galaxy Z Fold6.docx';
    v_old_owner_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := uuid_generate_v5('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'e994b100cd7b6327b45618f254d1b708');
    v_folder_id UUID;
BEGIN
    -- Check if document already migrated using mapping table (FAST)
    IF migration.is_migrated('documents', v_old_doc_id) THEN
        RAISE NOTICE 'Document % already migrated', v_old_doc_id;
        RETURN;
    END IF;
    
    -- Lookup folder via mapping table if folder specified (same DB - document_db)
    IF v_old_folder_id IS NOT NULL THEN
        v_folder_id := migration.get_new_id('folders', v_old_folder_id);
    END IF;
    
    -- Insert document
    INSERT INTO document_db.public.documents (
        id,
        status,
        file_name,
        file_size,
        storage_type,
        storage_path,
        storage_id,
        metadata,
        created_at,
        updated_at,
        deleted_at,
        folder_id,
        user_id,
        content_type,
        parsing_technique_id,
        source_type,
        organization_id
    ) VALUES (
        v_new_doc_id,
        'PROCESSED'::public.documents_status_enum,
        'Galaxy Z Fold6.docx',
        32404,
        'azure',
        'e994b100cd7b6327b45618f254d1b708/data/1771853601236-Galaxy Z Fold6.docx',
        NULL,
        '{"name": "Galaxy Z Fold6.docx", "source": "legacy-migration", "legacyData": {"doc_id": "e994b100cd7b6327b45618f254d1b708/data/1771853601236-Galaxy Z Fold6.docx", "doc_title": "Galaxy Z Fold6.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "beb95c40d8c8c339dc5f574527b6ff81305d01fd5591195d1066c662dada4502", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-02-23T13:33:15.010088',
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    )
    ON CONFLICT (id) DO NOTHING;
    
    -- Store document ID mapping
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch
    ) VALUES (
        'documents',
        v_old_doc_id,
        v_new_doc_id,
        'documents_20260313_192619'
    )
    ON CONFLICT (table_name, old_id) DO NOTHING;
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: AI Solution Engineer Test.pdf (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708/data/1773232870049-AI Solution Engineer Test.pdf';
    v_old_owner_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708';
    v_old_folder_id VARCHAR := '1393.0';
    v_new_doc_id UUID := uuid_generate_v5('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'e994b100cd7b6327b45618f254d1b708');
    v_folder_id UUID;
BEGIN
    -- Check if document already migrated using mapping table (FAST)
    IF migration.is_migrated('documents', v_old_doc_id) THEN
        RAISE NOTICE 'Document % already migrated', v_old_doc_id;
        RETURN;
    END IF;
    
    -- Lookup folder via mapping table if folder specified (same DB - document_db)
    IF v_old_folder_id IS NOT NULL THEN
        v_folder_id := migration.get_new_id('folders', v_old_folder_id);
    END IF;
    
    -- Insert document
    INSERT INTO document_db.public.documents (
        id,
        status,
        file_name,
        file_size,
        storage_type,
        storage_path,
        storage_id,
        metadata,
        created_at,
        updated_at,
        deleted_at,
        folder_id,
        user_id,
        content_type,
        parsing_technique_id,
        source_type,
        organization_id
    ) VALUES (
        v_new_doc_id,
        'PROCESSED'::public.documents_status_enum,
        'AI Solution Engineer Test.pdf',
        63275,
        'azure',
        'e994b100cd7b6327b45618f254d1b708/data/1773232870049-AI Solution Engineer Test.pdf',
        NULL,
        '{"name": "AI Solution Engineer Test.pdf", "source": "legacy-migration", "legacyData": {"doc_id": "e994b100cd7b6327b45618f254d1b708/data/1773232870049-AI Solution Engineer Test.pdf", "doc_title": "AI Solution Engineer Test.pdf", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "f6c9ecef7521d02e7cb599f9f9daa5562b07066b20e541e0b12c74ba50744a93", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-03-11T12:41:11.273383',
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        'application/pdf',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    )
    ON CONFLICT (id) DO NOTHING;
    
    -- Store document ID mapping
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch
    ) VALUES (
        'documents',
        v_old_doc_id,
        v_new_doc_id,
        'documents_20260313_192619'
    )
    ON CONFLICT (table_name, old_id) DO NOTHING;
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: AI Solution Engineer Test.pdf (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708/data/1773232946025-AI Solution Engineer Test.pdf';
    v_old_owner_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708';
    v_old_folder_id VARCHAR := '1393.0';
    v_new_doc_id UUID := uuid_generate_v5('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'e994b100cd7b6327b45618f254d1b708');
    v_folder_id UUID;
BEGIN
    -- Check if document already migrated using mapping table (FAST)
    IF migration.is_migrated('documents', v_old_doc_id) THEN
        RAISE NOTICE 'Document % already migrated', v_old_doc_id;
        RETURN;
    END IF;
    
    -- Lookup folder via mapping table if folder specified (same DB - document_db)
    IF v_old_folder_id IS NOT NULL THEN
        v_folder_id := migration.get_new_id('folders', v_old_folder_id);
    END IF;
    
    -- Insert document
    INSERT INTO document_db.public.documents (
        id,
        status,
        file_name,
        file_size,
        storage_type,
        storage_path,
        storage_id,
        metadata,
        created_at,
        updated_at,
        deleted_at,
        folder_id,
        user_id,
        content_type,
        parsing_technique_id,
        source_type,
        organization_id
    ) VALUES (
        v_new_doc_id,
        'PROCESSED'::public.documents_status_enum,
        'AI Solution Engineer Test.pdf',
        63275,
        'azure',
        'e994b100cd7b6327b45618f254d1b708/data/1773232946025-AI Solution Engineer Test.pdf',
        NULL,
        '{"name": "AI Solution Engineer Test.pdf", "source": "legacy-migration", "legacyData": {"doc_id": "e994b100cd7b6327b45618f254d1b708/data/1773232946025-AI Solution Engineer Test.pdf", "doc_title": "AI Solution Engineer Test.pdf", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "f6c9ecef7521d02e7cb599f9f9daa5562b07066b20e541e0b12c74ba50744a93", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-03-11T12:42:26.871016',
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        'application/pdf',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    )
    ON CONFLICT (id) DO NOTHING;
    
    -- Store document ID mapping
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch
    ) VALUES (
        'documents',
        v_old_doc_id,
        v_new_doc_id,
        'documents_20260313_192619'
    )
    ON CONFLICT (table_name, old_id) DO NOTHING;
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: דוח הקצאה רני צים לבורסה.pdf (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708/data/1773234425181-דוח הקצאה רני צים לבורסה.pdf';
    v_old_owner_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := uuid_generate_v5('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'e994b100cd7b6327b45618f254d1b708');
    v_folder_id UUID;
BEGIN
    -- Check if document already migrated using mapping table (FAST)
    IF migration.is_migrated('documents', v_old_doc_id) THEN
        RAISE NOTICE 'Document % already migrated', v_old_doc_id;
        RETURN;
    END IF;
    
    -- Lookup folder via mapping table if folder specified (same DB - document_db)
    IF v_old_folder_id IS NOT NULL THEN
        v_folder_id := migration.get_new_id('folders', v_old_folder_id);
    END IF;
    
    -- Insert document
    INSERT INTO document_db.public.documents (
        id,
        status,
        file_name,
        file_size,
        storage_type,
        storage_path,
        storage_id,
        metadata,
        created_at,
        updated_at,
        deleted_at,
        folder_id,
        user_id,
        content_type,
        parsing_technique_id,
        source_type,
        organization_id
    ) VALUES (
        v_new_doc_id,
        'PROCESSED'::public.documents_status_enum,
        'דוח הקצאה רני צים לבורסה.pdf',
        184596,
        'azure',
        'e994b100cd7b6327b45618f254d1b708/data/1773234425181-דוח הקצאה רני צים לבורסה.pdf',
        NULL,
        '{"name": "דוח הקצאה רני צים לבורסה.pdf", "source": "legacy-migration", "legacyData": {"doc_id": "e994b100cd7b6327b45618f254d1b708/data/1773234425181-דוח הקצאה רני צים לבורסה.pdf", "doc_title": "דוח הקצאה רני צים לבורסה.pdf", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "5ae8d83e204e66a5f0a7fb920c366470f13e8ab428a5c60f6e585eb21d6450aa", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-03-11T13:07:06.057881',
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        'application/pdf',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    )
    ON CONFLICT (id) DO NOTHING;
    
    -- Store document ID mapping
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch
    ) VALUES (
        'documents',
        v_old_doc_id,
        v_new_doc_id,
        'documents_20260313_192619'
    )
    ON CONFLICT (table_name, old_id) DO NOTHING;
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: Your_Pokeball.csv (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708/data/1773307578335-Your_Pokeball.csv';
    v_old_owner_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := uuid_generate_v5('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'e994b100cd7b6327b45618f254d1b708');
    v_folder_id UUID;
BEGIN
    -- Check if document already migrated using mapping table (FAST)
    IF migration.is_migrated('documents', v_old_doc_id) THEN
        RAISE NOTICE 'Document % already migrated', v_old_doc_id;
        RETURN;
    END IF;
    
    -- Lookup folder via mapping table if folder specified (same DB - document_db)
    IF v_old_folder_id IS NOT NULL THEN
        v_folder_id := migration.get_new_id('folders', v_old_folder_id);
    END IF;
    
    -- Insert document
    INSERT INTO document_db.public.documents (
        id,
        status,
        file_name,
        file_size,
        storage_type,
        storage_path,
        storage_id,
        metadata,
        created_at,
        updated_at,
        deleted_at,
        folder_id,
        user_id,
        content_type,
        parsing_technique_id,
        source_type,
        organization_id
    ) VALUES (
        v_new_doc_id,
        'PROCESSED'::public.documents_status_enum,
        'Your_Pokeball.csv',
        73,
        'azure',
        'e994b100cd7b6327b45618f254d1b708/data/1773307578335-Your_Pokeball.csv',
        NULL,
        '{"name": "Your_Pokeball.csv", "source": "legacy-migration", "legacyData": {"doc_id": "e994b100cd7b6327b45618f254d1b708/data/1773307578335-Your_Pokeball.csv", "doc_title": "Your_Pokeball.csv", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "8cbeff2b6cc887416ac8df8614672e6fb6c1f901b0a99c95da1c8db985be5d73", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-03-12T09:26:19.202393',
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        'text/csv',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    )
    ON CONFLICT (id) DO NOTHING;
    
    -- Store document ID mapping
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch
    ) VALUES (
        'documents',
        v_old_doc_id,
        v_new_doc_id,
        'documents_20260313_192619'
    )
    ON CONFLICT (table_name, old_id) DO NOTHING;
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: First Assignment.pdf (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708/data/1773307631582-First Assignment.pdf';
    v_old_owner_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708';
    v_old_folder_id VARCHAR := '1396.0';
    v_new_doc_id UUID := uuid_generate_v5('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'e994b100cd7b6327b45618f254d1b708');
    v_folder_id UUID;
BEGIN
    -- Check if document already migrated using mapping table (FAST)
    IF migration.is_migrated('documents', v_old_doc_id) THEN
        RAISE NOTICE 'Document % already migrated', v_old_doc_id;
        RETURN;
    END IF;
    
    -- Lookup folder via mapping table if folder specified (same DB - document_db)
    IF v_old_folder_id IS NOT NULL THEN
        v_folder_id := migration.get_new_id('folders', v_old_folder_id);
    END IF;
    
    -- Insert document
    INSERT INTO document_db.public.documents (
        id,
        status,
        file_name,
        file_size,
        storage_type,
        storage_path,
        storage_id,
        metadata,
        created_at,
        updated_at,
        deleted_at,
        folder_id,
        user_id,
        content_type,
        parsing_technique_id,
        source_type,
        organization_id
    ) VALUES (
        v_new_doc_id,
        'PROCESSED'::public.documents_status_enum,
        'First Assignment.pdf',
        43058,
        'azure',
        'e994b100cd7b6327b45618f254d1b708/data/1773307631582-First Assignment.pdf',
        NULL,
        '{"name": "First Assignment.pdf", "source": "legacy-migration", "legacyData": {"doc_id": "e994b100cd7b6327b45618f254d1b708/data/1773307631582-First Assignment.pdf", "doc_title": "First Assignment.pdf", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "de4bd7eebec575598262b305d2d720c861b95c6042c6d3d05c5aa505994c487d", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-03-12T09:27:12.530142',
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        'application/pdf',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    )
    ON CONFLICT (id) DO NOTHING;
    
    -- Store document ID mapping
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch
    ) VALUES (
        'documents',
        v_old_doc_id,
        v_new_doc_id,
        'documents_20260313_192619'
    )
    ON CONFLICT (table_name, old_id) DO NOTHING;
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;

-- Complete batch tracking
UPDATE migration.batch_log 
SET completed_at = now(), status = 'completed' 
WHERE batch_id = 'documents_20260313_192619';

-- Total documents processed: 13
-- Skipped (no doc_id): 0


