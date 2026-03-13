-- ============================================================
-- DOCUMENTS MIGRATION SQL
-- ============================================================
-- Generated: 2026-03-13T14:54:47.106677
-- Source: jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)
-- Destination: document_db.public.documents
-- Records to migrate: 4
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
    RAISE NOTICE 'This script will migrate 4 records to: document_db.public.documents';
    
    RAISE NOTICE 'Generated: 2026-03-13T14:54:47.106677';
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
VALUES ('documents_20260313_145447', 'documents', 4, '{"source": "jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)", "namespace_uuid": "0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b"}'::jsonb)
ON CONFLICT (batch_id) DO NOTHING;

-- IMPORTANT: Users and folders must be migrated FIRST!
-- Documents reference both users (owner_id) and folders (folder_id)


-- Document: P1719330-00.pdf (owner: 306916b02924f0142a236cc978b0d013)
DO $$
DECLARE
    v_old_doc_id VARCHAR := '306916b02924f0142a236cc978b0d013/data/1773232302673-P1719330-00.pdf';
    v_old_owner_id VARCHAR := '306916b02924f0142a236cc978b0d013';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := uuid_generate_v5('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013');
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
        'P1719330-00.pdf',
        368437,
        'azure',
        '306916b02924f0142a236cc978b0d013/data/1773232302673-P1719330-00.pdf',
        NULL,
        '{"name": "P1719330-00.pdf", "source": "legacy-migration", "legacyData": {"doc_id": "306916b02924f0142a236cc978b0d013/data/1773232302673-P1719330-00.pdf", "doc_title": "P1719330-00.pdf", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "809f11f2e287999228e1b28b07964288a441c3113317733c60319961c11c3019", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-03-11T12:31:44.156436',
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        'application/pdf',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    );
    
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
        'documents_20260313_145447'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: P1722570-00.pdf (owner: 306916b02924f0142a236cc978b0d013)
DO $$
DECLARE
    v_old_doc_id VARCHAR := '306916b02924f0142a236cc978b0d013/data/1773320287516-P1722570-00.pdf';
    v_old_owner_id VARCHAR := '306916b02924f0142a236cc978b0d013';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := uuid_generate_v5('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013');
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
        'P1722570-00.pdf',
        308648,
        'azure',
        '306916b02924f0142a236cc978b0d013/data/1773320287516-P1722570-00.pdf',
        NULL,
        '{"name": "P1722570-00.pdf", "source": "legacy-migration", "legacyData": {"doc_id": "306916b02924f0142a236cc978b0d013/data/1773320287516-P1722570-00.pdf", "doc_title": "P1722570-00.pdf", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "e3666eba494da6dabf4ca911f9a168b984d883f57083f771919a232aece998e3", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-03-12T12:58:08.590813',
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        'application/pdf',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    );
    
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
        'documents_20260313_145447'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: P1722949-00.pdf (owner: 306916b02924f0142a236cc978b0d013)
DO $$
DECLARE
    v_old_doc_id VARCHAR := '306916b02924f0142a236cc978b0d013/data/1773320288176-P1722949-00.pdf';
    v_old_owner_id VARCHAR := '306916b02924f0142a236cc978b0d013';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := uuid_generate_v5('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013');
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
        'P1722949-00.pdf',
        241534,
        'azure',
        '306916b02924f0142a236cc978b0d013/data/1773320288176-P1722949-00.pdf',
        NULL,
        '{"name": "P1722949-00.pdf", "source": "legacy-migration", "legacyData": {"doc_id": "306916b02924f0142a236cc978b0d013/data/1773320288176-P1722949-00.pdf", "doc_title": "P1722949-00.pdf", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "ccd86ec627f748a21acf686163cabd8900d2fb5282883e96bb9ebb8cd4cbbdbb", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-03-12T12:58:09.419379',
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        'application/pdf',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    );
    
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
        'documents_20260313_145447'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: P1722962-00.pdf (owner: 306916b02924f0142a236cc978b0d013)
DO $$
DECLARE
    v_old_doc_id VARCHAR := '306916b02924f0142a236cc978b0d013/data/1773320288984-P1722962-00.pdf';
    v_old_owner_id VARCHAR := '306916b02924f0142a236cc978b0d013';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := uuid_generate_v5('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013');
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
        'P1722962-00.pdf',
        241534,
        'azure',
        '306916b02924f0142a236cc978b0d013/data/1773320288984-P1722962-00.pdf',
        NULL,
        '{"name": "P1722962-00.pdf", "source": "legacy-migration", "legacyData": {"doc_id": "306916b02924f0142a236cc978b0d013/data/1773320288984-P1722962-00.pdf", "doc_title": "P1722962-00.pdf", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "ccd86ec627f748a21acf686163cabd8900d2fb5282883e96bb9ebb8cd4cbbdbb", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-03-12T12:58:10.279153',
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        'application/pdf',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    );
    
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
        'documents_20260313_145447'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;

-- Complete batch tracking
UPDATE migration.batch_log 
SET completed_at = now(), status = 'completed' 
WHERE batch_id = 'documents_20260313_145447';

-- Total documents processed: 4
-- Skipped (no doc_id): 0
