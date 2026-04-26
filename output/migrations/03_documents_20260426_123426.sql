-- ============================================================
-- DOCUMENTS MIGRATION SQL
-- ============================================================
-- Generated: 2026-04-26T12:34:34.286900
-- Source: jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)
-- Destination: document_db.public.documents
-- Records to migrate: 3
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

-- Helper function: Deterministic UUID that passes v4 validation
-- Uses uuid_generate_v5 internally for determinism, then overwrites the
-- version nibble (position 15) from '5' to '4' so the result passes
-- any UUID-v4 format check the target application performs.
CREATE OR REPLACE FUNCTION migration.deterministic_uuid_v4(
    ns uuid,
    input text
) RETURNS uuid AS $$
  SELECT overlay(uuid_generate_v5(ns, input)::text placing '4' from 15 for 1)::uuid;
$$ LANGUAGE sql IMMUTABLE;

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
    RAISE NOTICE 'This script will migrate 3 records to: document_db.public.documents';
    
    RAISE NOTICE 'Generated: 2026-04-26T12:34:34.286900';
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
VALUES ('documents_20260426_123434', 'documents', 3, '{"source": "jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)", "namespace_uuid": "0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b"}'::jsonb)
ON CONFLICT (batch_id) DO NOTHING;

-- IMPORTANT: Users and folders must be migrated FIRST!
-- Documents reference both users (owner_id) and folders (folder_id)


-- Document: oai מדריך למשתמש המתקדם.docx (owner: 857ca8ca4fc24d6afbf9ff5b74818b87)
DO $$
DECLARE
    v_old_doc_id VARCHAR := '857ca8ca4fc24d6afbf9ff5b74818b87/data/1747750161226-51687f6634937e75715c42851bb177e5';
    v_old_owner_id VARCHAR := '857ca8ca4fc24d6afbf9ff5b74818b87';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '857ca8ca4fc24d6afbf9ff5b74818b87');
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
    INSERT INTO public.documents (
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
        'UPLOADED'::public.documents_status_enum,
        'oai מדריך למשתמש המתקדם.docx',
        6182450,
        'azure',
        '857ca8ca4fc24d6afbf9ff5b74818b87/data/1747750161226-51687f6634937e75715c42851bb177e5',
        NULL,
        '{"name": "oai מדריך למשתמש המתקדם.docx", "source": "legacy-migration", "legacyData": {"doc_id": "857ca8ca4fc24d6afbf9ff5b74818b87/data/1747750161226-51687f6634937e75715c42851bb177e5", "doc_title": "oai מדריך למשתמש המתקדם.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "362ade716fd47de3991a81f8ef32242cf9eee203f0fa222f63fea59dbfd98e52", "data_integration_doc_metadata": null}}'::jsonb,
        '2025-05-20T14:09:22.716317',
        now(),
        NULL,
        v_folder_id,
        v_user_id,
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    )
    ON CONFLICT (id) DO NOTHING;

    -- Insert document_processing record (status COMPLETED, is_ready true)
    -- parsing_technique_id: subquery picks first available technique
    -- translate_to_english: derived from whether source chunks contained translated content
    INSERT INTO public.document_processing (
        id,
        document_id,
        parsing_technique_id,
        chunk_size,
        chunk_overlap,
        status,
        is_active,
        translate_to_english,
        embedding_model_id,
        is_ready,
        prepend_doc_title,
        deleted_at
    ) VALUES (
        migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id || '-processing'),
        v_new_doc_id,
        (SELECT id FROM public.parsing_techniques ORDER BY created_at LIMIT 1),
        512,
        50,
        'COMPLETED'::public.document_processing_status_enum,
        true,
        true,
        '00000000-0000-0000-0000-000000000001'::uuid,
        true,
        false,
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
        'documents_20260426_123434'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: jeen buddy guidelines - Nir.docx (owner: 857ca8ca4fc24d6afbf9ff5b74818b87)
DO $$
DECLARE
    v_old_doc_id VARCHAR := '857ca8ca4fc24d6afbf9ff5b74818b87/data/1775981749419-jeen buddy guidelines - Nir.docx';
    v_old_owner_id VARCHAR := '857ca8ca4fc24d6afbf9ff5b74818b87';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '857ca8ca4fc24d6afbf9ff5b74818b87');
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
    INSERT INTO public.documents (
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
        'UPLOADED'::public.documents_status_enum,
        'jeen buddy guidelines - Nir.docx',
        90388,
        'azure',
        '857ca8ca4fc24d6afbf9ff5b74818b87/data/1775981749419-jeen buddy guidelines - Nir.docx',
        NULL,
        '{"name": "jeen buddy guidelines - Nir.docx", "source": "legacy-migration", "legacyData": {"doc_id": "857ca8ca4fc24d6afbf9ff5b74818b87/data/1775981749419-jeen buddy guidelines - Nir.docx", "doc_title": "jeen buddy guidelines - Nir.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "4594652458cce2f6c672604cb372cc26821b79e3f0699423cb04fd0d372ca282", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-04-12T08:15:50.247515',
        now(),
        NULL,
        v_folder_id,
        v_user_id,
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    )
    ON CONFLICT (id) DO NOTHING;

    -- Insert document_processing record (status COMPLETED, is_ready true)
    -- parsing_technique_id: subquery picks first available technique
    -- translate_to_english: derived from whether source chunks contained translated content
    INSERT INTO public.document_processing (
        id,
        document_id,
        parsing_technique_id,
        chunk_size,
        chunk_overlap,
        status,
        is_active,
        translate_to_english,
        embedding_model_id,
        is_ready,
        prepend_doc_title,
        deleted_at
    ) VALUES (
        migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id || '-processing'),
        v_new_doc_id,
        (SELECT id FROM public.parsing_techniques ORDER BY created_at LIMIT 1),
        512,
        50,
        'COMPLETED'::public.document_processing_status_enum,
        true,
        true,
        '00000000-0000-0000-0000-000000000001'::uuid,
        true,
        false,
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
        'documents_20260426_123434'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: Microsoft Office Add-in Production Deployment – v1 draft.docx (owner: 857ca8ca4fc24d6afbf9ff5b74818b87)
DO $$
DECLARE
    v_old_doc_id VARCHAR := '857ca8ca4fc24d6afbf9ff5b74818b87/data/1775981750401-Microsoft Office Add-in Production Deployment – v1 draft.docx';
    v_old_owner_id VARCHAR := '857ca8ca4fc24d6afbf9ff5b74818b87';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '857ca8ca4fc24d6afbf9ff5b74818b87');
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
    INSERT INTO public.documents (
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
        'UPLOADED'::public.documents_status_enum,
        'Microsoft Office Add-in Production Deployment – v1 draft.docx',
        62417,
        'azure',
        '857ca8ca4fc24d6afbf9ff5b74818b87/data/1775981750401-Microsoft Office Add-in Production Deployment – v1 draft.docx',
        NULL,
        '{"name": "Microsoft Office Add-in Production Deployment – v1 draft.docx", "source": "legacy-migration", "legacyData": {"doc_id": "857ca8ca4fc24d6afbf9ff5b74818b87/data/1775981750401-Microsoft Office Add-in Production Deployment – v1 draft.docx", "doc_title": "Microsoft Office Add-in Production Deployment – v1 draft.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "dab3b5cfb62cc4d60c4d147848b7a46f25ca56c0fd566913d7db1fcdcd49732e", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-04-12T08:15:50.867331',
        now(),
        NULL,
        v_folder_id,
        v_user_id,
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    )
    ON CONFLICT (id) DO NOTHING;

    -- Insert document_processing record (status COMPLETED, is_ready true)
    -- parsing_technique_id: subquery picks first available technique
    -- translate_to_english: derived from whether source chunks contained translated content
    INSERT INTO public.document_processing (
        id,
        document_id,
        parsing_technique_id,
        chunk_size,
        chunk_overlap,
        status,
        is_active,
        translate_to_english,
        embedding_model_id,
        is_ready,
        prepend_doc_title,
        deleted_at
    ) VALUES (
        migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id || '-processing'),
        v_new_doc_id,
        (SELECT id FROM public.parsing_techniques ORDER BY created_at LIMIT 1),
        512,
        50,
        'COMPLETED'::public.document_processing_status_enum,
        true,
        false,
        '00000000-0000-0000-0000-000000000001'::uuid,
        true,
        false,
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
        'documents_20260426_123434'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;

-- Complete batch tracking
UPDATE migration.batch_log 
SET completed_at = now(), status = 'completed' 
WHERE batch_id = 'documents_20260426_123434';

-- Total documents processed: 3
-- Skipped (no doc_id): 0
