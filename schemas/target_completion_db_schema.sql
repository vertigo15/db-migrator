-- ============================================================
-- TARGET SCHEMA: completion_db
-- ============================================================
-- This script creates the target database schema for completion_db
-- Run this BEFORE migrating data
-- 
-- NOTE: Cannot create FKs to user_db.users or document_db.* (cross-database)
--       These relationships must be validated at application level
-- ============================================================

-- Create agents table
CREATE TABLE IF NOT EXISTS public.agents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    user_id UUID NOT NULL,      -- References user_db.users.id (NO FK possible)
    bot_data JSONB,              -- Agent configuration/settings
    tags JSONB,
    folder_id UUID,              -- References document_db.folders.id (NO FK possible)
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now(),
    is_active BOOLEAN DEFAULT true
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_agents_user ON public.agents(user_id);
CREATE INDEX IF NOT EXISTS idx_agents_folder ON public.agents(folder_id);
CREATE INDEX IF NOT EXISTS idx_agents_name ON public.agents(name);
CREATE INDEX IF NOT EXISTS idx_agents_active ON public.agents(is_active);

-- Create agent_settings table (optional, for per-user agent settings)
CREATE TABLE IF NOT EXISTS public.agent_settings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL,  -- References agents.id (FK possible!)
    user_id UUID NOT NULL,   -- References user_db.users.id (NO FK possible)
    settings JSONB,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now(),
    
    -- Composite unique constraint
    CONSTRAINT uq_agent_user UNIQUE (agent_id, user_id),
    
    -- Foreign Key to agents (within same database)
    CONSTRAINT fk_agent_settings_agent 
        FOREIGN KEY (agent_id) 
        REFERENCES public.agents(id) 
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_agent_settings_agent ON public.agent_settings(agent_id);
CREATE INDEX IF NOT EXISTS idx_agent_settings_user ON public.agent_settings(user_id);

-- Create agent_documents table (links agents to documents)
CREATE TABLE IF NOT EXISTS public.agent_documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL,     -- References agents.id (FK possible!)
    document_id UUID NOT NULL,  -- References document_db.documents.id (NO FK possible)
    added_at TIMESTAMP DEFAULT now(),
    
    -- Composite unique constraint
    CONSTRAINT uq_agent_document UNIQUE (agent_id, document_id),
    
    -- Foreign Key to agents (within same database)
    CONSTRAINT fk_agent_documents_agent 
        FOREIGN KEY (agent_id) 
        REFERENCES public.agents(id) 
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_agent_documents_agent ON public.agent_documents(agent_id);
CREATE INDEX IF NOT EXISTS idx_agent_documents_document ON public.agent_documents(document_id);

-- Comments for documentation
COMMENT ON TABLE public.agents IS 'AI agent configurations';
COMMENT ON TABLE public.agent_settings IS 'Per-user agent settings/overrides';
COMMENT ON TABLE public.agent_documents IS 'Agent-document associations';

COMMENT ON COLUMN public.agents.user_id IS 'References user_db.users.id (cross-database, no FK)';
COMMENT ON COLUMN public.agents.folder_id IS 'References document_db.folders.id (cross-database, no FK)';
COMMENT ON COLUMN public.agent_settings.agent_id IS 'References agents.id (same database, FK enforced)';
COMMENT ON COLUMN public.agent_settings.user_id IS 'References user_db.users.id (cross-database, no FK)';
COMMENT ON COLUMN public.agent_documents.agent_id IS 'References agents.id (same database, FK enforced)';
COMMENT ON COLUMN public.agent_documents.document_id IS 'References document_db.documents.id (cross-database, no FK)';

COMMENT ON CONSTRAINT fk_agent_settings_agent ON public.agent_settings IS 'FK to agents within same database - CASCADE delete';
COMMENT ON CONSTRAINT fk_agent_documents_agent ON public.agent_documents IS 'FK to agents within same database - CASCADE delete';

-- ============================================================
-- KNOWLEDGE BASE TABLES (V5 agent-document architecture)
-- ============================================================
-- These tables replace agent_documents as the primary link between
-- agents and their documents/folders in V5.

-- Enum types for knowledge base tables
CREATE TYPE IF NOT EXISTS public.knowledge_base_items_item_type_enum AS ENUM ('document', 'folder');
CREATE TYPE IF NOT EXISTS public.knowledge_base_assignments_assigned_to_type_enum AS ENUM ('agent', 'conversation', 'project');

-- knowledge_bases: one per agent, holds RAG retrieval settings
CREATE TABLE IF NOT EXISTS public.knowledge_bases (
    id UUID PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    description TEXT,
    similarity_top_k INTEGER,
    threshold REAL,
    re_ranker_top_k INTEGER,
    re_rank_score REAL,
    combines_multiple_answers BOOLEAN NOT NULL DEFAULT true,
    query_instructions TEXT,
    document_count_threshold INTEGER NOT NULL DEFAULT 20,
    total_document_count INTEGER NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- knowledge_base_assignments: links a KB to an agent (or conversation/project)
CREATE TABLE IF NOT EXISTS public.knowledge_base_assignments (
    id UUID PRIMARY KEY,
    knowledge_base_id UUID NOT NULL REFERENCES public.knowledge_bases(id) ON DELETE CASCADE,
    assigned_to_id UUID NOT NULL,
    assigned_to_type public.knowledge_base_assignments_assigned_to_type_enum NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_knowledge_base_assignments_target_kb UNIQUE (assigned_to_id, assigned_to_type, knowledge_base_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_one_kb_per_agent
    ON public.knowledge_base_assignments (assigned_to_id)
    WHERE assigned_to_type = 'agent';

-- knowledge_base_items: links a KB to documents/folders
CREATE TABLE IF NOT EXISTS public.knowledge_base_items (
    id UUID PRIMARY KEY,
    knowledge_base_id UUID NOT NULL REFERENCES public.knowledge_bases(id) ON DELETE CASCADE,
    item_id UUID NOT NULL,
    item_type public.knowledge_base_items_item_type_enum NOT NULL DEFAULT 'document',
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_knowledge_base_items_kb_doc_type UNIQUE (knowledge_base_id, item_id, item_type)
);

CREATE INDEX IF NOT EXISTS idx_knowledge_base_items_item_id ON public.knowledge_base_items(item_id);
CREATE INDEX IF NOT EXISTS idx_knowledge_base_items_kb_id ON public.knowledge_base_items(knowledge_base_id);
CREATE INDEX IF NOT EXISTS idx_knowledge_base_assignments_target ON public.knowledge_base_assignments(assigned_to_id, assigned_to_type);
CREATE INDEX IF NOT EXISTS idx_knowledge_base_assignments_kb_id ON public.knowledge_base_assignments(knowledge_base_id);

COMMENT ON TABLE public.knowledge_bases IS 'Logical collection of documents used for RAG retrieval. Owned by agents, conversations, or projects.';
COMMENT ON TABLE public.knowledge_base_assignments IS 'Polymorphic assignment table linking knowledge bases to agents, conversations, or projects.';
COMMENT ON TABLE public.knowledge_base_items IS 'Junction table linking knowledge bases to documents or folders from the document service.';

-- ============================================================
-- COMPLETION_DB SCHEMA CREATION COMPLETE
-- ============================================================
-- Next steps:
-- 1. Review agent_data JSONB structure
-- 2. Run migration SQL files:
--    - 05_agents_*.sql (or similar naming)
-- 
-- IMPORTANT: 
-- - User IDs must exist in user_db BEFORE migrating!
-- - Folder IDs must exist in document_db BEFORE migrating!
-- - Document IDs must exist in document_db for agent_documents!
-- - Knowledge base tables must exist for agent-document links!
-- ============================================================
