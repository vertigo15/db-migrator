-- ============================================================
-- TARGET SCHEMA: user_db
-- ============================================================
-- This script creates the target database schema for user_db
-- Run this BEFORE migrating data
-- ============================================================

-- Create users table
CREATE TABLE IF NOT EXISTS public.users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    firstname VARCHAR(255),
    lastname VARCHAR(255),
    email VARCHAR(255) NOT NULL,
    mobile_user_id VARCHAR(255),
    organization_id UUID NOT NULL,
    group_id UUID,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now(),
    last_connected TIMESTAMP,
    
    -- Constraints
    CONSTRAINT uq_email UNIQUE (email)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_users_email ON public.users(email);
CREATE INDEX IF NOT EXISTS idx_users_org ON public.users(organization_id);
CREATE INDEX IF NOT EXISTS idx_users_group ON public.users(group_id);

-- Comments for documentation
COMMENT ON TABLE public.users IS 'User accounts';
COMMENT ON COLUMN public.users.organization_id IS 'Organization UUID (from Azure AD)';

-- ============================================================
-- USER_DB SCHEMA CREATION COMPLETE
-- ============================================================
-- Next steps:
-- 1. Review constraints and indexes
-- 2. Run migration SQL files in order:
--    - 01_users_*.sql
-- ============================================================
