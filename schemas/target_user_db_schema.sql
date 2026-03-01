-- ============================================================
-- TARGET SCHEMA: user_db
-- ============================================================
-- This script creates the target database schema for user_db
-- Run this BEFORE migrating data
-- ============================================================

-- Create users_groups table
CREATE TABLE IF NOT EXISTS public.users_groups (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    group_name VARCHAR(255) NOT NULL,
    default_model VARCHAR(255),
    default_max_tokens_per_user INTEGER,
    enabled_features JSONB,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now(),
    CONSTRAINT uq_group_name UNIQUE (group_name)
);

-- Create index on group_name for fast lookups
CREATE INDEX IF NOT EXISTS idx_users_groups_name ON public.users_groups(group_name);

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
    CONSTRAINT uq_email UNIQUE (email),
    
    -- Foreign Keys (within same database)
    CONSTRAINT fk_users_group 
        FOREIGN KEY (group_id) 
        REFERENCES public.users_groups(id) 
        ON DELETE SET NULL
        ON UPDATE CASCADE
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_users_email ON public.users(email);
CREATE INDEX IF NOT EXISTS idx_users_org ON public.users(organization_id);
CREATE INDEX IF NOT EXISTS idx_users_group ON public.users(group_id);

-- Comments for documentation
COMMENT ON TABLE public.users_groups IS 'User groups/organizations';
COMMENT ON TABLE public.users IS 'User accounts';
COMMENT ON COLUMN public.users.organization_id IS 'Organization UUID (from Azure AD)';
COMMENT ON CONSTRAINT fk_users_group ON public.users IS 'FK to users_groups within same database';

-- ============================================================
-- USER_DB SCHEMA CREATION COMPLETE
-- ============================================================
-- Next steps:
-- 1. Review constraints and indexes
-- 2. Run migration SQL files in order:
--    - 01_users_groups_*.sql
--    - 02_users_*.sql
-- ============================================================
