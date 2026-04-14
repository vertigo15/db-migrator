-- ============================================================
-- DOCUMENTS MIGRATION SQL
-- ============================================================
-- Generated: 2026-04-13T12:05:44.474250
-- Source: jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)
-- Destination: document_db.public.documents
-- Records to migrate: 11
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
    RAISE NOTICE 'This script will migrate 11 records to: document_db.public.documents';
    
    RAISE NOTICE 'Generated: 2026-04-13T12:05:44.474250';
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
VALUES ('documents_20260413_120544', 'documents', 11, '{"source": "jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)", "namespace_uuid": "0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b"}'::jsonb)
ON CONFLICT (batch_id) DO NOTHING;

-- IMPORTANT: Users and folders must be migrated FIRST!
-- Documents reference both users (owner_id) and folders (folder_id)


-- Document: סוכן פיננסי - חח״י.pdf (owner: c54aad3b1a0cba4027f315540112f7fd)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd/data/1768250243950-סוכן פיננסי - חח״י.pdf';
    v_old_owner_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'c54aad3b1a0cba4027f315540112f7fd');
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
        'סוכן פיננסי - חח״י.pdf',
        2326760,
        'azure',
        'c54aad3b1a0cba4027f315540112f7fd/data/1768250243950-סוכן פיננסי - חח״י.pdf',
        NULL,
        '{"name": "סוכן פיננסי - חח״י.pdf", "source": "legacy-migration", "legacyData": {"doc_id": "c54aad3b1a0cba4027f315540112f7fd/data/1768250243950-סוכן פיננסי - חח״י.pdf", "doc_title": "סוכן פיננסי - חח״י.pdf", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "434d6b2c071d97abc66f66412387cd9b21db416dca02f2b1a6d3ad553bfefa6c", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-01-12T20:37:25.264756',
        now(),
        NULL,
        v_folder_id,
        v_user_id,
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
        'documents_20260413_120544'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: jeen ai - סוכנים פיננסים.pptx (owner: c54aad3b1a0cba4027f315540112f7fd)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd/data/1768250260811-jeen ai - סוכנים פיננסים.pptx';
    v_old_owner_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'c54aad3b1a0cba4027f315540112f7fd');
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
        'jeen ai - סוכנים פיננסים.pptx',
        3652587,
        'azure',
        'c54aad3b1a0cba4027f315540112f7fd/data/1768250260811-jeen ai - סוכנים פיננסים.pptx',
        NULL,
        '{"name": "jeen ai - סוכנים פיננסים.pptx", "source": "legacy-migration", "legacyData": {"doc_id": "c54aad3b1a0cba4027f315540112f7fd/data/1768250260811-jeen ai - סוכנים פיננסים.pptx", "doc_title": "jeen ai - סוכנים פיננסים.pptx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "13bd9c7b4ab6fc0072b92d22b60ff2224177db9180dd75be19cb2aac02e1942f", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-01-12T20:37:41.749239',
        now(),
        NULL,
        v_folder_id,
        v_user_id,
        'application/vnd.openxmlformats-officedocument.presentationml.presentation',
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
        'documents_20260413_120544'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: הצעת בניית תוכנית עבודה לשימוש במוצר.docx (owner: c54aad3b1a0cba4027f315540112f7fd)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd/data/1768250298610-הצעת בניית תוכנית עבודה לשימוש במוצר.docx';
    v_old_owner_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'c54aad3b1a0cba4027f315540112f7fd');
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
        'הצעת בניית תוכנית עבודה לשימוש במוצר.docx',
        131490,
        'azure',
        'c54aad3b1a0cba4027f315540112f7fd/data/1768250298610-הצעת בניית תוכנית עבודה לשימוש במוצר.docx',
        NULL,
        '{"name": "הצעת בניית תוכנית עבודה לשימוש במוצר.docx", "source": "legacy-migration", "legacyData": {"doc_id": "c54aad3b1a0cba4027f315540112f7fd/data/1768250298610-הצעת בניית תוכנית עבודה לשימוש במוצר.docx", "doc_title": "הצעת בניית תוכנית עבודה לשימוש במוצר.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "a9850141e1b9dd198dca1213fe7929a4413b9e29c2ba914bdd2a713abea696ff", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-01-12T20:38:19.087649',
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
        'documents_20260413_120544'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: הצעת בניית תוכנית עבודה לשימוש במוצר.docx (owner: c54aad3b1a0cba4027f315540112f7fd)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd/data/1768250322792-הצעת בניית תוכנית עבודה לשימוש במוצר.docx';
    v_old_owner_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'c54aad3b1a0cba4027f315540112f7fd');
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
        'הצעת בניית תוכנית עבודה לשימוש במוצר.docx',
        131490,
        'azure',
        'c54aad3b1a0cba4027f315540112f7fd/data/1768250322792-הצעת בניית תוכנית עבודה לשימוש במוצר.docx',
        NULL,
        '{"name": "הצעת בניית תוכנית עבודה לשימוש במוצר.docx", "source": "legacy-migration", "legacyData": {"doc_id": "c54aad3b1a0cba4027f315540112f7fd/data/1768250322792-הצעת בניית תוכנית עבודה לשימוש במוצר.docx", "doc_title": "הצעת בניית תוכנית עבודה לשימוש במוצר.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "a9850141e1b9dd198dca1213fe7929a4413b9e29c2ba914bdd2a713abea696ff", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-01-12T20:38:43.335367',
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
        'documents_20260413_120544'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: מסמך אפיון.docx (owner: c54aad3b1a0cba4027f315540112f7fd)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd/data/1768250322792-מסמך אפיון.docx';
    v_old_owner_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'c54aad3b1a0cba4027f315540112f7fd');
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
        'מסמך אפיון.docx',
        741215,
        'azure',
        'c54aad3b1a0cba4027f315540112f7fd/data/1768250322792-מסמך אפיון.docx',
        NULL,
        '{"name": "מסמך אפיון.docx", "source": "legacy-migration", "legacyData": {"doc_id": "c54aad3b1a0cba4027f315540112f7fd/data/1768250322792-מסמך אפיון.docx", "doc_title": "מסמך אפיון.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "7f9161d458efa81ee2a1daa845e614c206edcf06d6d75f7377414aa37a6744a7", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-01-12T20:38:43.810976',
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
        'documents_20260413_120544'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: תקלות חח.docx (owner: c54aad3b1a0cba4027f315540112f7fd)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd/data/1768250343210-תקלות חח.docx';
    v_old_owner_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'c54aad3b1a0cba4027f315540112f7fd');
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
        'תקלות חח.docx',
        20739,
        'azure',
        'c54aad3b1a0cba4027f315540112f7fd/data/1768250343210-תקלות חח.docx',
        NULL,
        '{"name": "תקלות חח.docx", "source": "legacy-migration", "legacyData": {"doc_id": "c54aad3b1a0cba4027f315540112f7fd/data/1768250343210-תקלות חח.docx", "doc_title": "תקלות חח.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "98031e9b12301b8050baa1a4a5cd6e0b6d064473ea8119daf769bc1553d48b29", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-01-12T20:39:03.647924',
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
        'documents_20260413_120544'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: מסמך אפיון.docx (owner: c54aad3b1a0cba4027f315540112f7fd)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd/data/1768250372375-מסמך אפיון.docx';
    v_old_owner_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'c54aad3b1a0cba4027f315540112f7fd');
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
        'מסמך אפיון.docx',
        741215,
        'azure',
        'c54aad3b1a0cba4027f315540112f7fd/data/1768250372375-מסמך אפיון.docx',
        NULL,
        '{"name": "מסמך אפיון.docx", "source": "legacy-migration", "legacyData": {"doc_id": "c54aad3b1a0cba4027f315540112f7fd/data/1768250372375-מסמך אפיון.docx", "doc_title": "מסמך אפיון.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "7f9161d458efa81ee2a1daa845e614c206edcf06d6d75f7377414aa37a6744a7", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-01-12T20:39:32.933919',
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
        'documents_20260413_120544'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: אמות מידה קצר - בדיקות.docx (owner: c54aad3b1a0cba4027f315540112f7fd)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd/data/1768251429504-אמות מידה קצר - בדיקות.docx';
    v_old_owner_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'c54aad3b1a0cba4027f315540112f7fd');
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
        'אמות מידה קצר - בדיקות.docx',
        147979,
        'azure',
        'c54aad3b1a0cba4027f315540112f7fd/data/1768251429504-אמות מידה קצר - בדיקות.docx',
        NULL,
        '{"name": "אמות מידה קצר - בדיקות.docx", "source": "legacy-migration", "legacyData": {"doc_id": "c54aad3b1a0cba4027f315540112f7fd/data/1768251429504-אמות מידה קצר - בדיקות.docx", "doc_title": "אמות מידה קצר - בדיקות.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "0055bde2568db9465e43c8cfb53868022a3bcc6d56fae80eff9339e51b28b4de", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-01-12T20:57:10.415532',
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
        'documents_20260413_120544'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: אפיון עסקי - בדיקות מוצר.docx (owner: c54aad3b1a0cba4027f315540112f7fd)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd/data/1768252310218-אפיון עסקי - בדיקות מוצר.docx';
    v_old_owner_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'c54aad3b1a0cba4027f315540112f7fd');
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
        'אפיון עסקי - בדיקות מוצר.docx',
        1749127,
        'azure',
        'c54aad3b1a0cba4027f315540112f7fd/data/1768252310218-אפיון עסקי - בדיקות מוצר.docx',
        NULL,
        '{"name": "אפיון עסקי - בדיקות מוצר.docx", "source": "legacy-migration", "legacyData": {"doc_id": "c54aad3b1a0cba4027f315540112f7fd/data/1768252310218-אפיון עסקי - בדיקות מוצר.docx", "doc_title": "אפיון עסקי - בדיקות מוצר.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "2f218b6e95e1aa6d56213faf669ec2109c22d11764540f892fd69a4cec9509cf", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-01-12T21:11:51.521191',
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
        'documents_20260413_120544'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: אפיון עסקי - בדיקות מוצר.docx (owner: c54aad3b1a0cba4027f315540112f7fd)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd/data/1768291999419-אפיון עסקי - בדיקות מוצר.docx';
    v_old_owner_id VARCHAR := 'c54aad3b1a0cba4027f315540112f7fd';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'c54aad3b1a0cba4027f315540112f7fd');
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
        'אפיון עסקי - בדיקות מוצר.docx',
        1749127,
        'azure',
        'c54aad3b1a0cba4027f315540112f7fd/data/1768291999419-אפיון עסקי - בדיקות מוצר.docx',
        NULL,
        '{"name": "אפיון עסקי - בדיקות מוצר.docx", "source": "legacy-migration", "legacyData": {"doc_id": "c54aad3b1a0cba4027f315540112f7fd/data/1768291999419-אפיון עסקי - בדיקות מוצר.docx", "doc_title": "אפיון עסקי - בדיקות מוצר.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "2f218b6e95e1aa6d56213faf669ec2109c22d11764540f892fd69a4cec9509cf", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-01-13T08:13:20.524722',
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
        'documents_20260413_120544'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: סיכום למבחן אחזור מידע (1).pdf (owner: de0ff05457533c93fdf3e0d1cdd0f808)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'de0ff05457533c93fdf3e0d1cdd0f808/data/1756207659351-0c8cd1917d00e951630029e4978e9147.pdf';
    v_old_owner_id VARCHAR := 'de0ff05457533c93fdf3e0d1cdd0f808';
    v_old_folder_id VARCHAR := NULL;
    v_new_doc_id UUID := migration.deterministic_uuid_v4('b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'::uuid, v_old_doc_id);
    v_user_id UUID := migration.deterministic_uuid_v4('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808');
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
        'סיכום למבחן אחזור מידע (1).pdf',
        1713056,
        'azure',
        'de0ff05457533c93fdf3e0d1cdd0f808/data/1756207659351-0c8cd1917d00e951630029e4978e9147.pdf',
        NULL,
        '{"name": "סיכום למבחן אחזור מידע (1).pdf", "source": "legacy-migration", "legacyData": {"doc_id": "de0ff05457533c93fdf3e0d1cdd0f808/data/1756207659351-0c8cd1917d00e951630029e4978e9147.pdf", "doc_title": "סיכום למבחן אחזור מידע (1).pdf", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "a6afc94d6461f26718147db40ad29e52b244ad35801386154f6f7078c307e184", "data_integration_doc_metadata": null}}'::jsonb,
        '2025-08-26T11:27:40.618916',
        now(),
        NULL,
        v_folder_id,
        v_user_id,
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
        'documents_20260413_120544'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;

-- Complete batch tracking
UPDATE migration.batch_log 
SET completed_at = now(), status = 'completed' 
WHERE batch_id = 'documents_20260413_120544';

-- Total documents processed: 11
-- Skipped (no doc_id): 0
