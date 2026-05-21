-- ============================================================
-- TARGET SCHEMA: completion_db
-- ============================================================
-- This script creates the target database schema for completion_db
-- Run this BEFORE migrating data
--
-- NOTE: Cannot create FKs to user_db.users or document_db.* (cross-database)
--       These relationships must be validated at application level
-- ============================================================

-- Enum types
CREATE TYPE IF NOT EXISTS public.agents_type_enum AS ENUM ('system', 'spark', 'cortex', 'workflow');
CREATE TYPE IF NOT EXISTS public.knowledge_base_items_item_type_enum AS ENUM ('document', 'folder');
CREATE TYPE IF NOT EXISTS public.knowledge_base_assignments_assigned_to_type_enum AS ENUM ('agent', 'conversation', 'project');
CREATE TYPE IF NOT EXISTS public.guardrails_type_enum AS ENUM ('input', 'output', 'pii');
CREATE TYPE IF NOT EXISTS public.conversations_async_status_enum AS ENUM ('STREAMING', 'COMPLETED_UNREAD');

-- ── agents ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.agents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description VARCHAR(512),
    type public.agents_type_enum NOT NULL DEFAULT 'cortex',
    avatar_url VARCHAR(512),
    is_active BOOLEAN NOT NULL DEFAULT true,
    is_public BOOLEAN NOT NULL DEFAULT false,
    deleted_at TIMESTAMP,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_interacted_at TIMESTAMPTZ,
    user_id UUID,                    -- References user_db.users.id (NO FK possible)
    folder_id UUID,                  -- Organizational folder
    is_prebuilt BOOLEAN NOT NULL DEFAULT false,
    is_draft BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS idx_agents_user ON public.agents(user_id);
CREATE INDEX IF NOT EXISTS idx_agents_folder ON public.agents(folder_id);
CREATE INDEX IF NOT EXISTS idx_agents_name ON public.agents(name);
CREATE INDEX IF NOT EXISTS idx_agents_active ON public.agents(is_active);

-- ── agent_settings ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.agent_settings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL REFERENCES public.agents(id) ON DELETE CASCADE,
    model VARCHAR(256),
    instructions TEXT,
    enabled_tools JSONB,
    conversation_starters JSONB,
    workflow_flow_id VARCHAR(256),
    combines_multiple_answers BOOLEAN NOT NULL DEFAULT true,
    retrieved_context_size INTEGER,
    query_instructions TEXT,
    search_in_english BOOLEAN NOT NULL DEFAULT false,
    show_source_links BOOLEAN NOT NULL DEFAULT false,
    show_source_text BOOLEAN NOT NULL DEFAULT false,
    follow_up_questions BOOLEAN NOT NULL DEFAULT false,
    additional_links BOOLEAN NOT NULL DEFAULT false,
    base_answers_on_files_only BOOLEAN NOT NULL DEFAULT false,
    re_rank_score REAL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_agent_settings_agent ON public.agent_settings(agent_id);

-- ── knowledge_bases ─────────────────────────────────────────
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

-- ── knowledge_base_assignments ──────────────────────────────
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

CREATE INDEX IF NOT EXISTS idx_knowledge_base_assignments_target ON public.knowledge_base_assignments(assigned_to_id, assigned_to_type);
CREATE INDEX IF NOT EXISTS idx_knowledge_base_assignments_kb_id ON public.knowledge_base_assignments(knowledge_base_id);

-- ── knowledge_base_items ────────────────────────────────────
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

-- ── guardrails ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.guardrails (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type public.guardrails_type_enum NOT NULL,
    prompt TEXT,
    response TEXT,
    keywords JSONB NOT NULL DEFAULT '[]',
    topics JSONB NOT NULL DEFAULT '[]',
    rules JSONB NOT NULL DEFAULT '[]',
    settings JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── conversations ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.conversations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(512) NOT NULL,
    message_count INTEGER NOT NULL DEFAULT 0,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT true,
    deleted_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    last_interacted_at TIMESTAMPTZ,
    user_id UUID,
    metadata JSONB,
    async_status public.conversations_async_status_enum,
    view_count INTEGER NOT NULL DEFAULT 0,
    forked_from_conversation_id UUID,
    forked_from_message_id UUID
);

CREATE INDEX IF NOT EXISTS idx_conversations_user ON public.conversations(user_id);

-- ── agent_conversions ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.agent_conversions (
    agent_id UUID NOT NULL,
    conversion_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_active BOOLEAN NOT NULL DEFAULT true,
    PRIMARY KEY (agent_id, conversion_id)
);

-- ── agent_skills ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.agent_skills (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL REFERENCES public.agents(id) ON DELETE CASCADE,
    skill_id UUID NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true
);

-- ── agent_sub_agents ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.agent_sub_agents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    brain_agent_id UUID NOT NULL REFERENCES public.agents(id) ON DELETE CASCADE,
    sub_agent_id UUID NOT NULL REFERENCES public.agents(id) ON DELETE CASCADE,
    alias VARCHAR(256),
    description TEXT
);

-- ── agent_drafts ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.agent_drafts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_agent_id UUID NOT NULL REFERENCES public.agents(id) ON DELETE CASCADE,
    draft_agent_id UUID NOT NULL REFERENCES public.agents(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Comments ────────────────────────────────────────────────
COMMENT ON TABLE public.agents IS 'AI agent configurations';
COMMENT ON TABLE public.agent_settings IS 'Agent settings (model, instructions, tools, RAG params)';
COMMENT ON TABLE public.knowledge_bases IS 'Logical collection of documents used for RAG retrieval';
COMMENT ON TABLE public.knowledge_base_assignments IS 'Links knowledge bases to agents, conversations, or projects';
COMMENT ON TABLE public.knowledge_base_items IS 'Links knowledge bases to documents or folders';
COMMENT ON TABLE public.guardrails IS 'Input/output/PII guardrail rules for agents';
COMMENT ON TABLE public.conversations IS 'Chat conversations';

COMMENT ON COLUMN public.agents.user_id IS 'References user_db.users.id (cross-database, no FK)';
COMMENT ON COLUMN public.conversations.user_id IS 'References user_db.users.id (cross-database, no FK)';

-- ============================================================
-- COMPLETION_DB SCHEMA CREATION COMPLETE
-- ============================================================
-- Next steps:
-- 1. Run migration SQL files in order:
--    - 05_agents_*.sql
--    - 06_knowledge_bases_*.sql
--
-- IMPORTANT:
-- - User IDs must exist in user_db BEFORE migrating!
-- - Document/Folder IDs must exist in document_db for knowledge_base_items!
-- ============================================================
