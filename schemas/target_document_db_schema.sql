-- ============================================================
-- TARGET SCHEMA: document_db
-- ============================================================
-- Expected V5 schema for document_db (document-service).
-- Run this BEFORE migrating data.
--
-- NOTE: Cannot create FKs to user_db.users (cross-database)
--       These relationships must be validated at application level.
--
-- Requires: CREATE EXTENSION IF NOT EXISTS vector;
-- ============================================================

-- Enum types
CREATE TYPE IF NOT EXISTS public.folders_folder_type_enum AS ENUM ('regular', 'public', 'shared', 'system');
CREATE TYPE IF NOT EXISTS public.documents_status_enum AS ENUM ('PENDING', 'UPLOADED', 'PROCESSING', 'COMPLETED', 'FAILED', 'DELETED');
CREATE TYPE IF NOT EXISTS public.documents_source_type_enum AS ENUM ('upload', 'external');
CREATE TYPE IF NOT EXISTS public.documents_content_type_enum AS ENUM ('pdf', 'docx', 'doc', 'txt', 'csv', 'xlsx', 'xls', 'pptx', 'html', 'md', 'json', 'xml', 'rtf', 'odt', 'ods', 'odp', 'epub', 'msg', 'eml', 'png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff', 'svg', 'webp', 'mp3', 'wav', 'mp4', 'avi', 'mkv', 'webm', 'url', 'link', 'other');
CREATE TYPE IF NOT EXISTS public.chunks_content_type_enum AS ENUM ('text', 'table', 'image');
CREATE TYPE IF NOT EXISTS public.document_processing_status_enum AS ENUM ('PENDING', 'PROCESSING', 'READY', 'COMPLETED', 'FAILED', 'CANCELLED');

-- ── folders ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.folders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    folder_name VARCHAR(255) NOT NULL,
    user_id UUID,
    parent_id UUID,
    folder_type public.folders_folder_type_enum DEFAULT 'regular',
    source_type VARCHAR(50),
    organization_id UUID,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    deleted_at TIMESTAMPTZ,

    CONSTRAINT fk_folders_parent
        FOREIGN KEY (parent_id)
        REFERENCES public.folders(id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_folders_user ON public.folders(user_id);
CREATE INDEX IF NOT EXISTS idx_folders_parent ON public.folders(parent_id);

-- ── documents ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status public.documents_status_enum NOT NULL DEFAULT 'PENDING',
    file_name VARCHAR(512),
    file_size BIGINT,
    storage_type VARCHAR(64),
    storage_path TEXT,
    storage_id VARCHAR(512),
    metadata JSONB,
    content_type public.documents_content_type_enum,
    source_type public.documents_source_type_enum DEFAULT 'upload',
    parsing_technique_id UUID,
    organization_id UUID,
    user_id UUID,
    folder_id UUID,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    deleted_at TIMESTAMPTZ,

    CONSTRAINT fk_documents_folder
        FOREIGN KEY (folder_id)
        REFERENCES public.folders(id)
        ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_documents_user ON public.documents(user_id);
CREATE INDEX IF NOT EXISTS idx_documents_folder ON public.documents(folder_id);
CREATE INDEX IF NOT EXISTS idx_documents_created ON public.documents(created_at);

-- ── document_processing ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.document_processing (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL REFERENCES public.documents(id) ON DELETE CASCADE,
    parsing_technique_id UUID,
    chunk_size INTEGER,
    chunk_overlap INTEGER,
    status public.document_processing_status_enum NOT NULL DEFAULT 'PENDING',
    is_active BOOLEAN NOT NULL DEFAULT true,
    translate_to_english BOOLEAN NOT NULL DEFAULT false,
    embedding_model_id UUID,
    is_ready BOOLEAN NOT NULL DEFAULT false,
    prepend_doc_title BOOLEAN NOT NULL DEFAULT false,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_doc_processing_document ON public.document_processing(document_id);

-- ── chunks ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.chunks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL REFERENCES public.documents(id) ON DELETE CASCADE,
    chunk_index INTEGER,
    content TEXT,
    content_hash VARCHAR(128),
    content_type public.chunks_content_type_enum DEFAULT 'text',
    page_number INTEGER,
    char_count INTEGER,
    word_count INTEGER,
    metadata JSONB,
    translated_content TEXT,
    link_url TEXT,
    link_title TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_chunks_document ON public.chunks(document_id);

-- ── embeddings ──────────────────────────────────────────────
-- V5 uses untyped vector + per-row dimension column with
-- partial HNSW indexes for each supported dimension.
CREATE TABLE IF NOT EXISTS public.embeddings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chunk_id UUID NOT NULL REFERENCES public.chunks(id) ON DELETE CASCADE,
    document_id UUID NOT NULL REFERENCES public.documents(id) ON DELETE CASCADE,
    embedding vector NOT NULL,
    dimension SMALLINT NOT NULL,
    model_name VARCHAR(256),
    created_at TIMESTAMPTZ DEFAULT now(),

    CONSTRAINT uq_embeddings_chunk_model UNIQUE (chunk_id, model_name)
);

CREATE INDEX IF NOT EXISTS idx_embeddings_chunk ON public.embeddings(chunk_id);
CREATE INDEX IF NOT EXISTS idx_embeddings_document ON public.embeddings(document_id);
CREATE INDEX IF NOT EXISTS idx_embeddings_dim_docid ON public.embeddings(dimension, document_id);

-- Partial HNSW indexes for common dimensions
CREATE INDEX IF NOT EXISTS idx_embeddings_hnsw_1024
    ON public.embeddings USING hnsw ((embedding::vector(1024)) vector_cosine_ops)
    WHERE dimension = 1024;

CREATE INDEX IF NOT EXISTS idx_embeddings_hnsw_1536
    ON public.embeddings USING hnsw ((embedding::vector(1536)) vector_cosine_ops)
    WHERE dimension = 1536;

-- ── Comments ────────────────────────────────────────────────
COMMENT ON TABLE public.folders IS 'Document folders/hierarchy';
COMMENT ON TABLE public.documents IS 'Document metadata and storage references';
COMMENT ON TABLE public.document_processing IS 'Processing pipeline records per document';
COMMENT ON TABLE public.chunks IS 'Document chunks (text segments for RAG)';
COMMENT ON TABLE public.embeddings IS 'Vector embeddings per chunk (untyped vector + dimension column)';

COMMENT ON COLUMN public.folders.user_id IS 'References user_db.users.id (cross-database, no FK)';
COMMENT ON COLUMN public.documents.user_id IS 'References user_db.users.id (cross-database, no FK)';
COMMENT ON COLUMN public.embeddings.dimension IS 'Dimensionality of the embedding vector (e.g. 1024, 1536)';

-- ============================================================
-- DOCUMENT_DB SCHEMA COMPLETE
-- ============================================================
-- Next steps:
-- 1. Ensure pgvector extension: CREATE EXTENSION IF NOT EXISTS vector;
-- 2. Run migration SQL files in order:
--    - 02_folders_*.sql
--    - 03_documents_*.sql
--    - 04_chunks_embeddings_*.sql
--
-- IMPORTANT: User IDs must exist in user_db BEFORE migrating!
-- ============================================================
