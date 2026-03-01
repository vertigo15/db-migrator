-- ============================================================
-- TARGET SCHEMA: document_db
-- ============================================================
-- This script creates the target database schema for document_db
-- Run this BEFORE migrating data
-- 
-- NOTE: Cannot create FKs to user_db.users (cross-database)
--       These relationships must be validated at application level
-- ============================================================

-- Create folders table
CREATE TABLE IF NOT EXISTS public.folders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    folder_name VARCHAR(255) NOT NULL,
    user_id UUID NOT NULL,  -- References user_db.users.id (NO FK possible)
    parent_id UUID,         -- Self-referencing FK within same table
    created_at TIMESTAMP DEFAULT now(),
    folder_type VARCHAR(50),
    
    -- Self-referencing FK (within same table)
    CONSTRAINT fk_folders_parent 
        FOREIGN KEY (parent_id) 
        REFERENCES public.folders(id) 
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_folders_user ON public.folders(user_id);
CREATE INDEX IF NOT EXISTS idx_folders_parent ON public.folders(parent_id);
CREATE INDEX IF NOT EXISTS idx_folders_name ON public.folders(folder_name);

-- Create documents table
CREATE TABLE IF NOT EXISTS public.documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    blob_name VARCHAR(255),
    indexer_type VARCHAR(255),
    user_id UUID NOT NULL,     -- References user_db.users.id (NO FK possible)
    folder_id UUID,            -- References folders.id (FK possible!)
    tags JSONB,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now(),
    
    -- Foreign Key to folders (within same database)
    CONSTRAINT fk_documents_folder 
        FOREIGN KEY (folder_id) 
        REFERENCES public.folders(id) 
        ON DELETE SET NULL
        ON UPDATE CASCADE
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_documents_user ON public.documents(user_id);
CREATE INDEX IF NOT EXISTS idx_documents_folder ON public.documents(folder_id);
CREATE INDEX IF NOT EXISTS idx_documents_created ON public.documents(created_at);

-- Create chunks table (embeddings)
CREATE TABLE IF NOT EXISTS public.chunks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL,  -- References documents.id (FK possible!)
    chunk_text TEXT,
    chunk_index INTEGER,
    embedding vector(1536),     -- Adjust dimension as needed
    metadata JSONB,
    created_at TIMESTAMP DEFAULT now(),
    
    -- Foreign Key to documents (within same database)
    CONSTRAINT fk_chunks_document 
        FOREIGN KEY (document_id) 
        REFERENCES public.documents(id) 
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_chunks_document ON public.chunks(document_id);
CREATE INDEX IF NOT EXISTS idx_chunks_created ON public.chunks(created_at);

-- Optional: Create vector similarity index if using pgvector
-- CREATE INDEX IF NOT EXISTS idx_chunks_embedding ON public.chunks 
--     USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- Comments for documentation
COMMENT ON TABLE public.folders IS 'Document folders/hierarchy';
COMMENT ON TABLE public.documents IS 'Document metadata';
COMMENT ON TABLE public.chunks IS 'Document chunks with embeddings';

COMMENT ON COLUMN public.folders.user_id IS 'References user_db.users.id (cross-database, no FK)';
COMMENT ON COLUMN public.documents.user_id IS 'References user_db.users.id (cross-database, no FK)';
COMMENT ON COLUMN public.documents.folder_id IS 'References folders.id (same database, FK enforced)';
COMMENT ON COLUMN public.chunks.document_id IS 'References documents.id (same database, FK enforced)';

COMMENT ON CONSTRAINT fk_folders_parent ON public.folders IS 'Self-referencing FK for folder hierarchy';
COMMENT ON CONSTRAINT fk_documents_folder ON public.documents IS 'FK to folders within same database';
COMMENT ON CONSTRAINT fk_chunks_document ON public.chunks IS 'FK to documents within same database - CASCADE delete';

-- ============================================================
-- DOCUMENT_DB SCHEMA CREATION COMPLETE
-- ============================================================
-- Next steps:
-- 1. Ensure pgvector extension is installed if using embeddings:
--    CREATE EXTENSION IF NOT EXISTS vector;
-- 2. Review vector dimension (default: 1536 for OpenAI)
-- 3. Run migration SQL files in order:
--    - 02_folders_*.sql
--    - 03_documents_*.sql
--    - 04_chunks_embeddings_*.sql
-- 
-- IMPORTANT: User IDs must exist in user_db BEFORE migrating!
-- ============================================================
