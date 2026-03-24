-- ============================================================
-- DOCUMENTS MIGRATION SQL
-- ============================================================
-- Generated: 2026-03-24T08:14:47.425974
-- Source: jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)
-- Destination: document_db.public.documents
-- Records to migrate: 5
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
    RAISE NOTICE 'This script will migrate 5 records to: document_db.public.documents';
    
    RAISE NOTICE 'Generated: 2026-03-24T08:14:47.425974';
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
VALUES ('documents_20260324_081447', 'documents', 5, '{"source": "jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)", "namespace_uuid": "0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b"}'::jsonb)
ON CONFLICT (batch_id) DO NOTHING;

-- IMPORTANT: Users and folders must be migrated FIRST!
-- Documents reference both users (owner_id) and folders (folder_id)


-- Document: jeen_ai_Login_and_Dashboard_Features_Guide_20260323_083725.docx (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := '3e2f16c9dd877a11be89685ecf9b2267/data/1774275267701-jeen_ai_Login_and_Dashboard_Features_Guide_20260323_083725.docx';
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
        'jeen_ai_Login_and_Dashboard_Features_Guide_20260323_083725.docx',
        443369,
        'azure',
        '3e2f16c9dd877a11be89685ecf9b2267/data/1774275267701-jeen_ai_Login_and_Dashboard_Features_Guide_20260323_083725.docx',
        NULL,
        '{"name": "jeen_ai_Login_and_Dashboard_Features_Guide_20260323_083725.docx", "source": "legacy-migration", "legacyData": {"doc_id": "3e2f16c9dd877a11be89685ecf9b2267/data/1774275267701-jeen_ai_Login_and_Dashboard_Features_Guide_20260323_083725.docx", "doc_title": "jeen_ai_Login_and_Dashboard_Features_Guide_20260323_083725.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "e8a19a43cb9d84eb3bf76ead0e0c47b5259bfcdaf6b7dc9116333bae1f1e6c68", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-03-23T14:14:28.819626',
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
        'documents_20260324_081447'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: התחברות למערכת אדמירל.docx (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := '3e2f16c9dd877a11be89685ecf9b2267/data/1774275269129-התחברות למערכת אדמירל.docx';
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
        'התחברות למערכת אדמירל.docx',
        2403042,
        'azure',
        '3e2f16c9dd877a11be89685ecf9b2267/data/1774275269129-התחברות למערכת אדמירל.docx',
        NULL,
        '{"name": "התחברות למערכת אדמירל.docx", "source": "legacy-migration", "legacyData": {"doc_id": "3e2f16c9dd877a11be89685ecf9b2267/data/1774275269129-התחברות למערכת אדמירל.docx", "doc_title": "התחברות למערכת אדמירל.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "b1622dc12489edf5586ca1be6bff6fa8df7b081401bbc88b4de7d9087ed02466", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-03-23T14:14:31.589449',
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
        'documents_20260324_081447'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: table-d7d95045-4bef-4496-91fa-900ea1289d35.csv (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708/data/1773757657741-table-d7d95045-4bef-4496-91fa-900ea1289d35.csv';
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
        'table-d7d95045-4bef-4496-91fa-900ea1289d35.csv',
        1309,
        'azure',
        'e994b100cd7b6327b45618f254d1b708/data/1773757657741-table-d7d95045-4bef-4496-91fa-900ea1289d35.csv',
        NULL,
        '{"name": "table-d7d95045-4bef-4496-91fa-900ea1289d35.csv", "source": "legacy-migration", "legacyData": {"doc_id": "e994b100cd7b6327b45618f254d1b708/data/1773757657741-table-d7d95045-4bef-4496-91fa-900ea1289d35.csv", "doc_title": "table-d7d95045-4bef-4496-91fa-900ea1289d35.csv", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "cad50a50b64cd9b645ccae7e84818aa2838784d87accff6555666a172b645959", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-03-17T14:27:38.072869',
        now(),
        NULL,
        v_folder_id,
        v_user_id,
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
        'documents_20260324_081447'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: הקצאה_פרטית_לעובדים_רני_צים_24-11-2025_20260310_113707.docx (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708/data/1773757658572-הקצאה_פרטית_לעובדים_רני_צים_24-11-2025_20260310_113707.docx';
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
        'הקצאה_פרטית_לעובדים_רני_צים_24-11-2025_20260310_113707.docx',
        38043,
        'azure',
        'e994b100cd7b6327b45618f254d1b708/data/1773757658572-הקצאה_פרטית_לעובדים_רני_צים_24-11-2025_20260310_113707.docx',
        NULL,
        '{"name": "הקצאה_פרטית_לעובדים_רני_צים_24-11-2025_20260310_113707.docx", "source": "legacy-migration", "legacyData": {"doc_id": "e994b100cd7b6327b45618f254d1b708/data/1773757658572-הקצאה_פרטית_לעובדים_רני_צים_24-11-2025_20260310_113707.docx", "doc_title": "הקצאה_פרטית_לעובדים_רני_צים_24-11-2025_20260310_113707.docx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "2f5004abd47e19da9a3c10a1bd80cb47078b10b1ce4b1fe4d0425489070f9ed7", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-03-17T14:27:38.651420',
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
        'documents_20260324_081447'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;


-- Document: Insurance Company Profit and Loss - Duable Header 2   (3).xlsx (owner: e994b100cd7b6327b45618f254d1b708)
DO $$
DECLARE
    v_old_doc_id VARCHAR := 'e994b100cd7b6327b45618f254d1b708/data/1773840038748-Insurance Company Profit and Loss - Duable Header 2   (3).xlsx';
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
        'Insurance Company Profit and Loss - Duable Header 2   (3).xlsx',
        117343,
        'azure',
        'e994b100cd7b6327b45618f254d1b708/data/1773840038748-Insurance Company Profit and Loss - Duable Header 2   (3).xlsx',
        NULL,
        '{"name": "Insurance Company Profit and Loss - Duable Header 2   (3).xlsx", "source": "legacy-migration", "legacyData": {"doc_id": "e994b100cd7b6327b45618f254d1b708/data/1773840038748-Insurance Company Profit and Loss - Duable Header 2   (3).xlsx", "doc_title": "Insurance Company Profit and Loss - Duable Header 2   (3).xlsx", "doc_description": null, "doc_summery": null, "doc_summery_modified_by": null, "doc_summery_modified_at": null, "tags": [], "embedding_model": null, "vector_methods": null, "version": "2", "doc_checksum": "f81610101037925e4ed1eb27a817a8e05e12f4df1576ca06b18c7f5a702be552", "data_integration_doc_metadata": null}}'::jsonb,
        '2026-03-18T13:20:39.389879',
        now(),
        NULL,
        v_folder_id,
        v_user_id,
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
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
        'documents_20260324_081447'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;

-- Complete batch tracking
UPDATE migration.batch_log 
SET completed_at = now(), status = 'completed' 
WHERE batch_id = 'documents_20260324_081447';

-- Total documents processed: 5
-- Skipped (no doc_id): 0
