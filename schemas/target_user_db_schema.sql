-- ============================================================
-- TARGET SCHEMA: user_db
-- ============================================================
-- This script creates the target database schema for user_db
-- Run this BEFORE migrating data
-- ============================================================

-- Create users table
CREATE TABLE IF NOT EXISTS public.users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255),
    username VARCHAR(255) NOT NULL,
    avatar_url VARCHAR(512),
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    zitadel_user_id VARCHAR(255),
    preferred_language VARCHAR(10),
    organization_id UUID,

    CONSTRAINT uq_username UNIQUE (username)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_users_email ON public.users(email);
CREATE INDEX IF NOT EXISTS idx_users_org ON public.users(organization_id);
CREATE INDEX IF NOT EXISTS idx_users_zitadel ON public.users(zitadel_user_id);

-- Comments for documentation
COMMENT ON TABLE public.users IS 'User accounts';
COMMENT ON COLUMN public.users.organization_id IS 'Organization UUID';
COMMENT ON COLUMN public.users.zitadel_user_id IS 'External identity provider user ID';

-- ============================================================
-- USER_DB SCHEMA CREATION COMPLETE
-- ============================================================
-- Next steps:
-- 1. Review constraints and indexes
-- 2. Run migration SQL files in order:
--    - 01_users_*.sql
-- ============================================================
