-- ============================================================
-- MIGRATION ID MAPPINGS TABLE
-- ============================================================
-- Purpose: Store mappings between V4 legacy IDs and V5 UUIDs
-- Benefits:
--   - Fast indexed lookups during migration
--   - Audit trail of migrated records
--   - Simplifies foreign key resolution
--   - Enables incremental migration and rollback
-- ============================================================

CREATE SCHEMA IF NOT EXISTS migration;

-- Main mapping table
CREATE TABLE IF NOT EXISTS migration.id_mappings (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    old_id VARCHAR(255) NOT NULL,
    new_id UUID NOT NULL,
    migration_batch VARCHAR(50),
    migration_run_id UUID,
    record_action VARCHAR(20) NOT NULL DEFAULT 'created',
    migrated_at TIMESTAMP DEFAULT now(),
    notes TEXT,
    
    -- Composite unique constraint
    CONSTRAINT uq_table_old_id UNIQUE (table_name, old_id),
    CONSTRAINT uq_table_new_id UNIQUE (table_name, new_id)
);

ALTER TABLE migration.id_mappings
    ADD COLUMN IF NOT EXISTS migration_run_id UUID;
ALTER TABLE migration.id_mappings
    ADD COLUMN IF NOT EXISTS record_action VARCHAR(20) NOT NULL DEFAULT 'created';

-- Indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_mappings_table_old_id 
    ON migration.id_mappings(table_name, old_id);

CREATE INDEX IF NOT EXISTS idx_mappings_table_new_id 
    ON migration.id_mappings(table_name, new_id);

CREATE INDEX IF NOT EXISTS idx_mappings_batch 
    ON migration.id_mappings(migration_batch);

CREATE INDEX IF NOT EXISTS idx_mappings_migrated_at 
    ON migration.id_mappings(migrated_at);
CREATE INDEX IF NOT EXISTS idx_mappings_run_action
    ON migration.id_mappings(migration_run_id, record_action);

CREATE TABLE IF NOT EXISTS migration.migration_runs (
    id UUID PRIMARY KEY,
    status VARCHAR(30) NOT NULL DEFAULT 'running',
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    total_users INTEGER NOT NULL DEFAULT 0,
    source_info JSONB,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS migration.migration_steps (
    migration_run_id UUID NOT NULL
        REFERENCES migration.migration_runs(id) ON DELETE CASCADE,
    step_key VARCHAR(50) NOT NULL,
    target_database VARCHAR(100) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'pending',
    expected_count INTEGER,
    affected_count INTEGER,
    verification_details JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (migration_run_id, step_key)
);

ALTER TABLE migration.migration_steps
    ADD COLUMN IF NOT EXISTS verification_details JSONB NOT NULL DEFAULT '{}'::jsonb;

-- Migration batch tracking
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

-- Migration result tracking tables
CREATE TABLE IF NOT EXISTS migration.migration_batches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    total_users INTEGER NOT NULL DEFAULT 0,
    source_info JSONB,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS migration.migration_user_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id UUID NOT NULL REFERENCES migration.migration_batches(id) ON DELETE CASCADE,
    email VARCHAR(255) NOT NULL,
    legacy_user_id VARCHAR(255),
    v5_user_id UUID,
    user_action VARCHAR(20) NOT NULL DEFAULT 'created',
    result VARCHAR(50) NOT NULL DEFAULT 'pending',
    failed_step VARCHAR(100),
    error_message TEXT,
    steps_completed JSONB DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT uq_migration_user_result UNIQUE (batch_id, email)
);

ALTER TABLE migration.migration_user_results
    ADD COLUMN IF NOT EXISTS user_action VARCHAR(20) NOT NULL DEFAULT 'created';
CREATE UNIQUE INDEX IF NOT EXISTS uq_migration_user_result_batch_email
    ON migration.migration_user_results(batch_id, email);

CREATE INDEX IF NOT EXISTS idx_user_results_batch
    ON migration.migration_user_results(batch_id);
CREATE INDEX IF NOT EXISTS idx_user_results_email
    ON migration.migration_user_results(email);
CREATE INDEX IF NOT EXISTS idx_user_results_result
    ON migration.migration_user_results(result);

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

-- Helper function: Get old ID from new ID (reverse lookup)
CREATE OR REPLACE FUNCTION migration.get_old_id(
    p_table_name VARCHAR,
    p_new_id UUID
) RETURNS VARCHAR AS $$
DECLARE
    v_old_id VARCHAR;
BEGIN
    SELECT old_id INTO v_old_id
    FROM migration.id_mappings
    WHERE table_name = p_table_name AND new_id = p_new_id;
    
    RETURN v_old_id;
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

-- View: Migration progress summary
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

-- Example usage queries:
COMMENT ON TABLE migration.id_mappings IS 
'Stores mappings between V4 legacy IDs and V5 UUIDs for all migrated records.

Example usage:
-- Insert mapping after creating new record
INSERT INTO migration.id_mappings (table_name, old_id, new_id, migration_batch)
VALUES (''users'', ''legacy_hash_123'', ''a1b2c3d4-uuid-here'', ''batch_20260228'');

-- Lookup new ID during migration
SELECT migration.get_new_id(''users'', ''legacy_hash_123'');

-- Check migration progress
SELECT * FROM migration.progress_summary;

-- Find all documents for a migrated user
SELECT d.* 
FROM documents d
JOIN migration.id_mappings m ON d.user_id = m.new_id
WHERE m.table_name = ''users'' AND m.old_id = ''legacy_user_123'';
';

-- Grant permissions (adjust as needed)
-- GRANT USAGE ON SCHEMA migration TO migration_user;
-- GRANT SELECT, INSERT ON migration.id_mappings TO migration_user;
-- GRANT SELECT, INSERT, UPDATE ON migration.batch_log TO migration_user;
