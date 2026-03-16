"""
SQL migration generator - generates INSERT statements directly from database data.
Integrated with the extraction engine to create SQL files alongside CSV exports.
"""
import os
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import pandas as pd
from pathlib import Path
import glob


# ============================================================
# Deterministic Namespace UUIDs for cross-database UUID generation
# ============================================================
# Each entity type uses a SEPARATE namespace to prevent UUID collisions.
# uuid_generate_v5(namespace, old_id) always produces the same UUID
# for the same inputs, allowing independent computation across databases.
# ============================================================
USER_NAMESPACE_UUID = 'a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'   # Users (step 01)
DOC_NAMESPACE_UUID  = 'b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'   # Documents (step 03)
NAMESPACE_UUID      = '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'   # Folders, agents, chunks, embeddings


def resolve_user_id_sql(old_id: str, overrides: Optional[Dict[str, str]] = None) -> str:
    """
    Return a SQL expression that resolves a V4 user hash ID to a V5 user UUID.

    If old_id is present in the overrides dict (user already exists in V5 with
    a known UUID), the real V5 UUID is embedded directly into the SQL at
    generation time — no runtime lookup required.

    Otherwise falls back to the deterministic uuid_generate_v5 formula, which
    is the standard path for users being migrated fresh.

    This approach avoids cross-database issues: each SQL file runs against a
    different target DB (user_db / document_db / completion_db) and only has
    access to its own local migration.id_mappings.  Resolving at generation
    time means the correct UUID is always embedded regardless of which DB the
    script runs against.

    Args:
        old_id:    V4 legacy hash ID (e.g. 'de0ff05457533c93fdf3e0d1cdd0f808')
        overrides: Optional dict mapping v4_old_id -> existing_v5_uuid string

    Returns:
        SQL expression string, e.g.:
          "'7a1b2c3d-...'::uuid"                          (override path)
          "uuid_generate_v5('...'::uuid, 'de0ff054...')" (default path)
    """
    if overrides and old_id and old_id in overrides:
        return f"'{overrides[old_id]}'::uuid"
    return f"uuid_generate_v5('{USER_NAMESPACE_UUID}'::uuid, {escape_sql_string(old_id)})"


def cleanup_old_migration_files(output_file: str, file_prefix: str):
    """
    Delete old migration SQL files with the same prefix to avoid confusion.
    
    Args:
        output_file: The new file being created (e.g., 'output/migrations/06_agents_20260228_123456.sql')
        file_prefix: The file prefix to match (e.g., '06_agents_')
        
    Example:
        cleanup_old_migration_files('output/migrations/06_agents_20260228_123456.sql', '06_agents_')
        # Deletes: 06_agents_20260228_120000.sql, 06_agents_20260227_*.sql, etc.
    """
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        return
    
    # Find all files matching the pattern
    pattern = os.path.join(output_dir, f"{file_prefix}*.sql")
    old_files = glob.glob(pattern)
    
    deleted_count = 0
    for old_file in old_files:
        # Don't delete the file we're about to create
        if os.path.abspath(old_file) != os.path.abspath(output_file):
            try:
                os.remove(old_file)
                deleted_count += 1
                print(f"Deleted old migration file: {os.path.basename(old_file)}")
            except Exception as e:
                print(f"Warning: Could not delete {old_file}: {e}")
    
    if deleted_count > 0:
        print(f"Cleaned up {deleted_count} old migration file(s) with prefix '{file_prefix}'")


def clean_string(val):
    """Clean and trim string values."""
    if val is None or pd.isna(val):
        return None
    cleaned = str(val).strip()
    return cleaned if cleaned else None


def escape_sql_string(val):
    """Escape single quotes for SQL string literals."""
    if val is None or pd.isna(val):
        return 'NULL'
    return f"'{str(val).replace(chr(39), chr(39)+chr(39))}'"


def escape_sql_string_with_dollar_quotes(val, tag='CONTENT'):
    """Escape string using PostgreSQL dollar quotes for content with special characters.
    
    This is safer for large text content that might contain quotes, backslashes, or $$ symbols.
    Uses a dollar-quote tag like $CONTENT$ to avoid conflicts with nested $$ in the content.
    
    PostgreSQL dollar-quoting syntax:  $tag$content$tag$
    """
    if val is None or pd.isna(val):
        return 'NULL'
    
    # Convert to string
    content = str(val)
    
    # If content is empty, use NULL
    if not content:
        return 'NULL'
    
    # Use dollar quoting with a unique tag
    # Ensure the delimiter $tag$ does not appear inside the content
    while f'${tag}$' in content:
        tag = tag + '_'
    
    return f'${tag}${content}${tag}$'


def _safe_json_default(obj):
    """Custom JSON encoder for pandas/numpy types."""
    import numpy as np
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        if np.isnan(obj):
            return None
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if hasattr(obj, 'item'):  # numpy scalar
        return obj.item()
    return str(obj)


def _is_scalar_na(val):
    """Safely check if a scalar value is NA/NaN (without blowing up on lists/dicts)."""
    if val is None:
        return True
    if isinstance(val, (list, dict, tuple)):
        return False
    try:
        return pd.isna(val)
    except (ValueError, TypeError):
        return False


def escape_json_for_sql(json_data):
    """Escape JSON data for inclusion in SQL."""
    if json_data is None:
        return 'NULL'
    json_str = json.dumps(json_data, ensure_ascii=False, default=_safe_json_default)
    # Escape single quotes in the JSON string
    return f"'{json_str.replace(chr(39), chr(39)+chr(39))}'::jsonb"


def generate_username(email):
    """Generate username from email (part before @)."""
    if not email:
        return None
    username = email.split('@')[0].lower().replace('.', '')
    return username


def generate_migration_schema_setup() -> str:
    """
    Generate SQL to create migration schema and mapping tables.
    This will be included in every migration SQL file.
    Uses IF NOT EXISTS so it's idempotent - safe to run multiple times.
    
    Returns:
        SQL setup string
    """
    setup_sql = """-- ============================================================
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

"""
    return setup_sql


def generate_sql_header(
    table_name: str,
    target_schema: str,
    target_table: str,
    source_info: str,
    record_count: int,
    org_id: Optional[str] = None,
    include_mapping_setup: bool = True
) -> str:
    """
    Generate SQL file header with confirmation prompt and migration setup.
    
    Args:
        table_name: Logical table name (e.g., 'users')
        target_schema: Target schema (e.g., 'user_db')
        target_table: Target table (e.g., 'public.users')
        source_info: Source database info
        record_count: Number of records to migrate
        org_id: Optional organization ID
        include_mapping_setup: Include migration.id_mappings table setup
        
    Returns:
        SQL header string
    """
    full_target = f"{target_schema}.{target_table}"
    timestamp = datetime.now().isoformat()
    
    org_notice = f"RAISE NOTICE 'Organization ID: {org_id}';" if org_id else ""
    
    header = f"""-- ============================================================
-- {table_name.upper()} MIGRATION SQL
-- ============================================================
-- Generated: {timestamp}
-- Source: {source_info}
-- Destination: {full_target}
-- Records to migrate: {record_count}
-- 
-- IMPORTANT: This script will INSERT records into the target database!
-- IMPORTANT: Review organization_id and other constants before execution!
--
-- Each INSERT checks if record already exists before inserting.
-- Uses migration.id_mappings table for fast ID lookups and tracking.
-- ============================================================

-- Ensure PostgreSQL interprets this file as UTF-8 (required for Hebrew/multilingual content)
SET client_encoding = 'UTF8';

"""
    
    # Add migration schema setup if requested
    if include_mapping_setup:
        header += generate_migration_schema_setup()
    
    header += f"""-- CONFIRMATION PROMPT: User must confirm before execution
DO $$
DECLARE
    user_confirmation TEXT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '{table_name.upper()} MIGRATION - CONFIRMATION REQUIRED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'This script will migrate {record_count} records to: {full_target}';
    {org_notice}
    RAISE NOTICE 'Generated: {timestamp}';
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
-- \\\\prompt 'Type YES to confirm and continue with migration: ' user_confirmation
-- \\\\if :'user_confirmation' != 'YES'
--   \\\\echo 'Migration cancelled by user.'
--   \\\\quit
-- \\\\endif

"""
    return header


def generate_user_insert(
    row: pd.Series,
    org_id: str = '356b50f7-bcbd-42aa-9392-e1605f42f7a1',
    user_id_overrides: Optional[Dict[str, str]] = None
) -> Optional[str]:
    """
    Generate INSERT statement for a single user.

    If user_id_overrides contains an entry for this user's V4 id, the user
    already exists in V5 with a different UUID.  In that case we skip the
    INSERT and only register the mapping in migration.id_mappings for audit.

    Args:
        row: Pandas Series with user data
        org_id: Organization UUID
        user_id_overrides: Optional dict {v4_old_id: existing_v5_uuid}

    Returns:
        SQL INSERT statement or None to skip
    """
    # Extract and clean fields
    old_id = clean_string(row.get('id'))
    email = clean_string(row.get('email'))
    first_name = clean_string(row.get('name'))
    last_name = clean_string(row.get('last_name'))

    # Skip if no email
    if not email:
        return None

    # --- OVERRIDE PATH: user already exists in V5 with a known UUID ---
    if user_id_overrides and old_id and old_id in user_id_overrides:
        v5_uuid = user_id_overrides[old_id]
        return f"""
-- User: {email} (OVERRIDE — already exists in V5)
-- V4 old_id: {old_id}  →  V5 UUID: {v5_uuid}
DO $$
BEGIN
    -- User already exists in V5; skip INSERT.
    -- Register mapping in migration.id_mappings for audit and downstream tracking.
    INSERT INTO migration.id_mappings (table_name, old_id, new_id, migration_batch, notes)
    VALUES ('users', {escape_sql_string(old_id)}, '{v5_uuid}'::uuid,
            'user_overrides', 'Pre-existing V5 user — override supplied at migration time')
    ON CONFLICT (table_name, old_id) DO NOTHING;
    RAISE NOTICE 'Skipped INSERT for user % — already exists in V5 as %',
                 {escape_sql_string(email)}, '{v5_uuid}';
END $$;
"""

    # Generate username
    username = generate_username(email)
    
    # Parse numeric/JSON fields
    try:
        token_used = int(float(row.get('token_used', 0) or 0))
    except:
        token_used = 0
        
    try:
        words_used = int(float(row.get('words_used', 0) or 0))
    except:
        words_used = 0
        
    try:
        last_connected = int(float(row.get('last_connected', 0) or 0))
    except:
        last_connected = 0
        
    try:
        times_connected = int(float(row.get('times_connected', 0) or 0))
    except:
        times_connected = 0
    
    # Parse JSON fields
    model = row.get('model')
    if isinstance(model, str):
        try:
            model = json.loads(model.replace("'", '"'))
        except:
            model = None
    
    history_categories = row.get('history_categories')
    if isinstance(history_categories, str):
        try:
            history_categories = json.loads(history_categories.replace("'", '"'))
        except:
            history_categories = None
    
    enabled_features = row.get('enabled_features')
    if isinstance(enabled_features, str):
        try:
            enabled_features = json.loads(enabled_features.replace("'", '"'))
        except:
            enabled_features = None
    
    subfeatures = row.get('subfeatures')
    if isinstance(subfeatures, str):
        try:
            subfeatures = json.loads(subfeatures.replace("'", '"'))
        except:
            subfeatures = None
    
    # Parse created_at
    created_at_val = row.get('created_at')
    if pd.notna(created_at_val):
        try:
            if isinstance(created_at_val, str):
                created_at_dt = pd.to_datetime(created_at_val)
            else:
                created_at_dt = created_at_val
            created_at_sql = f"'{created_at_dt.isoformat()}'"
        except:
            created_at_sql = 'now()'
    else:
        created_at_sql = 'now()'
    
    # Build metadata JSON object
    metadata = {
        'legacyData': {
            'id': old_id,
            'job': clean_string(row.get('job')),
            'model': model,
            'group_id': clean_string(row.get('__group_id__')),
            'azure_oid': clean_string(row.get('azure_oid')),
            'department': clean_string(row.get('department')),
            'token_used': str(token_used),
            'words_used': str(words_used),
            'subfeatures': subfeatures,
            'token_limit': clean_string(row.get('token_limit')),
            'company_name': clean_string(row.get('company_name')),
            'phone_number': clean_string(row.get('phone_number')),
            'last_connected': str(last_connected),
            'letter_checkbox': clean_string(row.get('letter_checkbox')),
            'times_connected': str(times_connected),
            'enabled_features': enabled_features,
            'history_categories': history_categories,
            'company_name_in_hebrew': clean_string(row.get('company_name_in_hebrew'))
        }
    }
    
    metadata_sql = escape_json_for_sql(metadata)
    
    # Generate SQL with mapping table integration
    sql = f"""
-- User: {email}
DO $$
DECLARE
    v_old_id VARCHAR := {escape_sql_string(old_id)};
    v_email VARCHAR := {escape_sql_string(email)};
    v_new_id UUID;
BEGIN
    -- Check if already migrated using mapping table (FAST)
    IF migration.is_migrated('users', v_old_id) THEN
        RAISE NOTICE 'User % already migrated (old_id: %)', v_email, v_old_id;
        RETURN;
    END IF;
    
    -- Generate deterministic UUID (same namespace+input = same UUID across all databases)
    v_new_id := uuid_generate_v5('{USER_NAMESPACE_UUID}'::uuid, v_old_id);
    
    -- Insert user (handle all unique constraint conflicts)
    BEGIN
        INSERT INTO user_db.public.users (
            id,
            email,
            first_name,
            last_name,
            username,
            avatar_url,
            metadata,
            created_at,
            updated_at,
            deleted_at,
            zitadel_user_id,
            organization_id,
            is_owner,
            preferred_language
        ) VALUES (
            v_new_id,
            {escape_sql_string(email)},
            {escape_sql_string(first_name)},
            {escape_sql_string(last_name)},
            {escape_sql_string(username)},
            NULL,
            {metadata_sql},
            {created_at_sql},
            now(),
            NULL,
            NULL,
            '{org_id}'::uuid,
            false,
            NULL
        )
        ON CONFLICT (email) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            metadata = EXCLUDED.metadata,
            updated_at = now()
        RETURNING id INTO v_new_id;
    EXCEPTION WHEN unique_violation THEN
        -- Username conflict — check if this user already exists by email
        SELECT id INTO v_new_id FROM user_db.public.users WHERE email = {escape_sql_string(email)};
        IF v_new_id IS NOT NULL THEN
            RAISE NOTICE 'User % already exists (matched by email), reusing id %', v_email, v_new_id;
        ELSE
            -- User doesn't exist yet, username is taken — retry with email as username
            v_new_id := uuid_generate_v5('{USER_NAMESPACE_UUID}'::uuid, v_old_id);
            RAISE NOTICE 'User %: username conflict, using email as username instead', v_email;
            INSERT INTO user_db.public.users (
                id, email, first_name, last_name, username, avatar_url,
                metadata, created_at, updated_at, deleted_at, zitadel_user_id,
                organization_id, is_owner, preferred_language
            ) VALUES (
                v_new_id,
                {escape_sql_string(email)},
                {escape_sql_string(first_name)},
                {escape_sql_string(last_name)},
                {escape_sql_string(email)},
                NULL, {metadata_sql}, {created_at_sql}, now(), NULL, NULL,
                '{org_id}'::uuid, false, NULL
            )
            ON CONFLICT (email) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                metadata = EXCLUDED.metadata,
                updated_at = now()
            RETURNING id INTO v_new_id;
        END IF;
    END;
    
    -- Store ID mapping for fast future lookups
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch,
        notes
    ) VALUES (
        'users',
        v_old_id,
        v_new_id,
        'batch_{{{{TIMESTAMP}}}}',
        'Migrated from V4 users table'
    );
    
    RAISE NOTICE 'Migrated user %: % → %', v_email, v_old_id, v_new_id;
END $$;
"""
    
    return sql


def generate_users_migration_sql(
    users_df: pd.DataFrame,
    output_file: str,
    source_info: str,
    org_id: str = '356b50f7-bcbd-42aa-9392-e1605f42f7a1',
    user_id_overrides: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Generate SQL migration file for users table.
    
    Args:
        users_df: DataFrame with user data
        output_file: Path to output SQL file
        source_info: Source database info string
        org_id: Organization UUID
        
    Returns:
        Dictionary with generation stats
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Clean up old users migration files
    cleanup_old_migration_files(output_file, '01_users_')
    
    # Generate batch ID for tracking
    batch_id = f"users_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    record_count = 0
    skipped_count = 0
    
    with open(output_file, 'w', encoding='utf-8') as sql_file:
        # Write header
        header = generate_sql_header(
            table_name='users',
            target_schema='user_db',
            target_table='public.users',
            source_info=source_info,
            record_count=len(users_df),
            org_id=org_id
        )
        sql_file.write(header)
        
        # Write UUID extension and batch tracking start
        source_json = json.dumps({"source": source_info})
        batch_start = f"""-- Ensure UUID extensions are available
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Start batch tracking
INSERT INTO migration.batch_log (batch_id, table_name, record_count, source_info)
VALUES ('{batch_id}', 'users', {len(users_df)}, '{source_json}'::jsonb)
ON CONFLICT (batch_id) DO NOTHING;

"""
        sql_file.write(batch_start)
        
        # Write individual INSERT statements with batch_id substitution
        for _, row in users_df.iterrows():
            sql = generate_user_insert(row, org_id, user_id_overrides)
            if sql:
                # Replace batch placeholder with actual batch_id
                sql = sql.replace('batch_{{TIMESTAMP}}', batch_id)
                sql_file.write(sql)
                sql_file.write('\n')
                record_count += 1
            else:
                skipped_count += 1
        
        # Write batch completion and footer
        footer = f"""-- Complete batch tracking
UPDATE migration.batch_log 
SET completed_at = now(), status = 'completed' 
WHERE batch_id = '{batch_id}';

-- Total records processed: {record_count}
-- Skipped (no email): {skipped_count}
"""
        sql_file.write(footer)
    
    return {
        'file': output_file,
        'processed': record_count,
        'skipped': skipped_count,
        'batch_id': batch_id
    }


def generate_folder_insert(
    row: pd.Series,
    namespace_uuid: str = '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b',
    user_id_overrides: Optional[Dict[str, str]] = None
) -> Optional[str]:
    """
    Generate INSERT statement for a single folder.
    Uses deterministic UUID generation to maintain parent-child relationships.
    
    Args:
        row: Pandas Series with folder data
        namespace_uuid: Fixed namespace UUID for deterministic ID generation
        
    Returns:
        SQL INSERT statement or None to skip
    """
    # Extract and clean fields
    old_id = clean_string(row.get('id'))
    folder_name = clean_string(row.get('folder_name'))
    parent_id = clean_string(row.get('parent_id'))
    owner_id = clean_string(row.get('owner_id'))  # Legacy hash ID
    # Map V4 folder_type to valid V5 enum values
    # V5 enum: 'default', 'agent' (TypeORM folders_folder_type_enum)
    raw_folder_type = clean_string(row.get('folder_type')) or 'default'
    FOLDER_TYPE_MAP = {
        'default': 'default',
        'bot': 'agent',           # V4 'bot' → V5 'agent'
        'agent': 'agent',
        'document': 'document',   # V4 'document' → V5 'document'
    }
    folder_type = FOLDER_TYPE_MAP.get(raw_folder_type, 'default')
    
    # Skip if no ID
    if not old_id:
        return None
    
    # Parse created_at
    created_at_val = row.get('created_at')
    if pd.notna(created_at_val):
        try:
            if isinstance(created_at_val, str):
                created_at_dt = pd.to_datetime(created_at_val)
            else:
                created_at_dt = created_at_val
            created_at_sql = f"'{created_at_dt.isoformat()}'"
        except:
            created_at_sql = 'now()'
    else:
        created_at_sql = 'now()'
    
    # Generate parent_id SQL (deterministic UUID or NULL)
    if parent_id:
        parent_id_sql = f"uuid_generate_v5('{namespace_uuid}'::uuid, '{parent_id}')"
    else:
        parent_id_sql = 'NULL'
    
    # Generate SQL with mapping table integration
    sql = f"""
-- Folder: {folder_name or old_id} (owner: {owner_id})
DO $$
DECLARE
    v_old_folder_id VARCHAR := {escape_sql_string(old_id)};
    v_old_owner_id VARCHAR := {escape_sql_string(owner_id)};
    v_folder_id uuid := uuid_generate_v5('{namespace_uuid}'::uuid, v_old_folder_id);
    v_user_id uuid := {resolve_user_id_sql(owner_id, user_id_overrides)};
BEGIN
    -- Check if folder already migrated using mapping table (FAST)
    IF migration.is_migrated('folders', v_old_folder_id) THEN
        RAISE NOTICE 'Folder % already migrated', v_old_folder_id;
        RETURN;
    END IF;
    
    -- Insert folder
    INSERT INTO document_db.public.folders (
        id,
        folder_name,
        parent_id,
        folder_type,
        user_id,
        created_at,
        updated_at,
        deleted_at
    ) VALUES (
        v_folder_id,
        {escape_sql_string(folder_name)},
        {parent_id_sql},
        '{folder_type}'::public.folders_folder_type_enum,
        v_user_id,
        {created_at_sql},
        now(),
        NULL
    );
    
    -- Store folder ID mapping
    INSERT INTO migration.id_mappings (
        table_name,
        old_id,
        new_id,
        migration_batch
    ) VALUES (
        'folders',
        v_old_folder_id,
        v_folder_id,
        'batch_{{{{TIMESTAMP}}}}'
    );
    
    RAISE NOTICE 'Migrated folder: % → %', v_old_folder_id, v_folder_id;
END $$;
"""
    
    return sql


def generate_folders_migration_sql(
    folders_df: pd.DataFrame,
    output_file: str,
    source_info: str,
    namespace_uuid: str = '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b',
    user_id_overrides: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Generate SQL migration file for folders table.
    Folders are sorted to insert parents before children.
    
    Args:
        folders_df: DataFrame with folder data
        output_file: Path to output SQL file
        source_info: Source database info string
        namespace_uuid: Fixed namespace UUID for deterministic ID generation
        
    Returns:
        Dictionary with generation stats
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Clean up old folders migration files
    cleanup_old_migration_files(output_file, '02_folders_')
    
    # Generate batch ID
    batch_id = f"folders_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Sort folders: parents (NULL parent_id) first, then by parent_id, then by id
    folders_sorted = folders_df.copy()
    folders_sorted['parent_id_sort'] = folders_sorted['parent_id'].fillna('')
    folders_sorted = folders_sorted.sort_values(['parent_id_sort', 'id'])
    
    record_count = 0
    skipped_count = 0
    
    with open(output_file, 'w', encoding='utf-8') as sql_file:
        # Write header using standard function
        header = generate_sql_header(
            table_name='folders',
            target_schema='document_db',
            target_table='public.folders',
            source_info=source_info,
            record_count=len(folders_df)
        )
        sql_file.write(header)
        
        # Write extension and batch tracking
        source_json = json.dumps({"source": source_info})
        batch_start = f"""-- Ensure uuid-ossp extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Start batch tracking
INSERT INTO migration.batch_log (batch_id, table_name, record_count, source_info)
VALUES ('{batch_id}', 'folders', {len(folders_df)}, '{source_json}'::jsonb)
ON CONFLICT (batch_id) DO NOTHING;

"""
        sql_file.write(batch_start)
        
        # Write individual INSERT statements (sorted order)
        for _, row in folders_sorted.iterrows():
            sql = generate_folder_insert(row, namespace_uuid, user_id_overrides)
            if sql:
                # Replace batch placeholder with actual batch_id
                sql = sql.replace('batch_{{TIMESTAMP}}', batch_id)
                sql_file.write(sql)
                sql_file.write('\n')
                record_count += 1
            else:
                skipped_count += 1
        
        # Write batch completion and footer
        footer = f"""-- Complete batch tracking
UPDATE migration.batch_log 
SET completed_at = now(), status = 'completed' 
WHERE batch_id = '{batch_id}';

-- Total folders processed: {record_count}
-- Skipped (no ID): {skipped_count}
-- Note: Folders inserted in parent-first order using deterministic UUIDs (uuid_generate_v5)
-- Namespace UUID: {namespace_uuid}
"""
        sql_file.write(footer)
    
    return {
        'file': output_file,
        'processed': record_count,
        'skipped': skipped_count,
        'batch_id': batch_id
    }


def get_content_type(doc_type: Optional[str]) -> str:
    """Map document type to MIME content type."""
    if not doc_type:
        return 'application/octet-stream'
    
    doc_type = doc_type.strip().lower()
    
    mime_types = {
        'pdf': 'application/pdf',
        'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'doc': 'application/msword',
        'ppt': 'application/vnd.ms-powerpoint',
        'xls': 'application/vnd.ms-excel',
        'txt': 'text/plain',
        'csv': 'text/csv',
        'html': 'text/html',
        'json': 'application/json',
        'png': 'image/png',
        'jpg': 'image/jpeg',
        'jpeg': 'image/jpeg',
        'gif': 'image/gif',
        'svg': 'image/svg+xml',
        'mp3': 'audio/mpeg',
        'mp4': 'video/mp4'
    }
    
    return mime_types.get(doc_type, 'application/octet-stream')


def generate_document_insert(
    row: pd.Series,
    namespace_uuid: str = '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b',
    user_id_overrides: Optional[Dict[str, str]] = None
) -> Optional[str]:
    """
    Generate INSERT statement for a single document.
    
    Args:
        row: Pandas Series with document data
        namespace_uuid: Fixed namespace UUID for folder_id conversion
        
    Returns:
        SQL INSERT statement or None to skip
    """
    # Extract and clean fields
    doc_id = clean_string(row.get('doc_id'))
    owner_id = clean_string(row.get('owner_id'))
    doc_name_origin = clean_string(row.get('doc_name_origin'))
    doc_title = clean_string(row.get('doc_title'))
    doc_size = row.get('doc_size', 0)
    blob_source = clean_string(row.get('blob_source'))
    doc_type = clean_string(row.get('doc_type'))

    # folder_id is int4 in V4 — pandas may load it as float (e.g. 1393.0).
    # Normalise to plain integer string so migration.id_mappings lookup matches.
    _doc_folder_id_raw = row.get('folder_id')
    if _doc_folder_id_raw is not None and not (isinstance(_doc_folder_id_raw, float) and pd.isna(_doc_folder_id_raw)):
        try:
            folder_id = str(int(float(_doc_folder_id_raw)))
        except (ValueError, TypeError):
            folder_id = clean_string(_doc_folder_id_raw)
    else:
        folder_id = None
    
    # Skip if no doc_id
    if not doc_id:
        return None
    
    # Determine file_name
    file_name = doc_name_origin or doc_title or 'unnamed'
    
    # Parse file_size
    try:
        file_size = int(float(doc_size or 0))
    except:
        file_size = 0
    
    # Map storage_type
    if blob_source == 'azure_blob':
        storage_type = 'azure'
    elif blob_source:
        storage_type = blob_source
    else:
        storage_type = None
    
    # Parse created_at
    created_at_val = row.get('created_at')
    if pd.notna(created_at_val):
        try:
            if isinstance(created_at_val, str):
                created_at_dt = pd.to_datetime(created_at_val)
            else:
                created_at_dt = created_at_val
            created_at_sql = f"'{created_at_dt.isoformat()}'"
        except:
            created_at_sql = 'now()'
    else:
        created_at_sql = 'now()'
    
    # Get content type
    content_type = get_content_type(doc_type)
    
    # Parse JSON fields for metadata
    tags = row.get('tags')
    if isinstance(tags, str):
        try:
            tags = json.loads(tags.replace("'", '"'))
        except:
            tags = []
    elif isinstance(tags, (list, dict)):
        pass  # already parsed from JSONB
    elif _is_scalar_na(tags):
        tags = []
    else:
        tags = []
    
    vector_methods = row.get('vector_methods')
    if isinstance(vector_methods, str):
        try:
            vector_methods = json.loads(vector_methods.replace("'", '"'))
        except:
            vector_methods = None
    elif isinstance(vector_methods, (list, dict)):
        pass  # already parsed from JSONB
    elif _is_scalar_na(vector_methods):
        vector_methods = None
    
    data_integration_doc_metadata = row.get('data_integration_doc_metadata')
    if isinstance(data_integration_doc_metadata, str):
        try:
            data_integration_doc_metadata = json.loads(data_integration_doc_metadata.replace("'", '"'))
        except:
            data_integration_doc_metadata = None
    elif isinstance(data_integration_doc_metadata, (list, dict)):
        pass  # already parsed from JSONB
    elif _is_scalar_na(data_integration_doc_metadata):
        data_integration_doc_metadata = None
    
    # Build metadata
    metadata = {
        'name': file_name,
        'source': 'legacy-migration',
        'legacyData': {
            'doc_id': doc_id,
            'doc_title': doc_title,
            'doc_description': clean_string(row.get('doc_description')),
            'doc_summery': clean_string(row.get('doc_summery')),
            'doc_summery_modified_by': clean_string(row.get('doc_summery_modified_by')),
            'doc_summery_modified_at': clean_string(row.get('doc_summery_modified_at')),
            'tags': tags,
            'embedding_model': clean_string(row.get('embedding_model')),
            'vector_methods': vector_methods,
            'version': clean_string(row.get('version')),
            'doc_checksum': clean_string(row.get('doc_checksum')),
            'data_integration_doc_metadata': data_integration_doc_metadata
        }
    }
    
    metadata_sql = escape_json_for_sql(metadata)
    
    # Generate SQL with mapping table integration
    sql = f"""
-- Document: {file_name} (owner: {owner_id})
DO $$
DECLARE
    v_old_doc_id VARCHAR := {escape_sql_string(doc_id)};
    v_old_owner_id VARCHAR := {escape_sql_string(owner_id)};
    v_old_folder_id VARCHAR := {escape_sql_string(folder_id) if folder_id else 'NULL'};
    v_new_doc_id UUID := uuid_generate_v5('{DOC_NAMESPACE_UUID}'::uuid, v_old_doc_id);
    v_user_id UUID := {resolve_user_id_sql(owner_id, user_id_overrides)};
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
    INSERT INTO document_db.public.documents (
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
        'PROCESSED'::public.documents_status_enum,
        {escape_sql_string(file_name)},
        {file_size},
        {escape_sql_string(storage_type) if storage_type else 'NULL'},
        '{doc_id}',
        NULL,
        {metadata_sql},
        {created_at_sql},
        now(),
        NULL,
        v_folder_id,
        v_user_id::varchar(255),
        '{content_type}',
        NULL,
        'upload'::public.documents_source_type_enum,
        NULL
    );
    
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
        'batch_{{{{TIMESTAMP}}}}'
    );
    
    RAISE NOTICE 'Migrated document: % → %', v_old_doc_id, v_new_doc_id;
END $$;
"""
    
    return sql


def generate_documents_migration_sql(
    documents_df: pd.DataFrame,
    output_file: str,
    source_info: str,
    namespace_uuid: str = '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b',
    user_id_overrides: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Generate SQL migration file for documents table.
    
    Args:
        documents_df: DataFrame with document data
        output_file: Path to output SQL file
        source_info: Source database info string
        namespace_uuid: Fixed namespace UUID for folder_id conversion
        
    Returns:
        Dictionary with generation stats
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Clean up old documents migration files
    cleanup_old_migration_files(output_file, '03_documents_')
    
    record_count = 0
    skipped_count = 0
    
    # Generate batch ID for tracking
    batch_id = f"documents_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    with open(output_file, 'w', encoding='utf-8') as sql_file:
        # Write header with migration setup
        header = generate_sql_header(
            table_name='documents',
            target_schema='document_db',
            target_table='public.documents',
            source_info=source_info,
            record_count=len(documents_df),
            include_mapping_setup=True
        )
        sql_file.write(header)
        
        # Write UUID extensions and batch tracking
        source_json = json.dumps({"source": source_info, "namespace_uuid": namespace_uuid})
        batch_start = f"""-- Ensure UUID extensions are available
-- Note: gen_random_uuid() is built-in for PostgreSQL 13+
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Start batch tracking
INSERT INTO migration.batch_log (batch_id, table_name, record_count, source_info)
VALUES ('{batch_id}', 'documents', {len(documents_df)}, '{source_json}'::jsonb)
ON CONFLICT (batch_id) DO NOTHING;

-- IMPORTANT: Users and folders must be migrated FIRST!
-- Documents reference both users (owner_id) and folders (folder_id)

"""
        sql_file.write(batch_start)
        
        # Write individual INSERT statements with batch_id substitution
        for _, row in documents_df.iterrows():
            try:
                sql = generate_document_insert(row, namespace_uuid, user_id_overrides)
                if sql:
                    # Replace batch placeholder with actual batch_id
                    sql = sql.replace('batch_{{TIMESTAMP}}', batch_id)
                    sql_file.write(sql)
                    sql_file.write('\n')
                    record_count += 1
                else:
                    skipped_count += 1
            except Exception as e:
                import sys, traceback
                doc_id = row.get('doc_id', 'unknown')
                print(f"Warning: Failed to generate SQL for document {doc_id}: {e}", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
                skipped_count += 1
        
        # Write batch completion and footer
        footer = f"""-- Complete batch tracking
UPDATE migration.batch_log 
SET completed_at = now(), status = 'completed' 
WHERE batch_id = '{batch_id}';

-- Total documents processed: {record_count}
-- Skipped (no doc_id): {skipped_count}
"""
        sql_file.write(footer)
    
    return {
        'file': output_file,
        'processed': record_count,
        'skipped': skipped_count,
        'batch_id': batch_id
    }


def truncate_embedding_vector(
    embeddings_value: str,
    target_dim: int = 1024,
    warn_nonzero_tail: bool = True
) -> str:
    """
    Truncate an embedding vector string from source dimension to target dimension.
    
    Used when the source embeddings were produced by a smaller model (e.g. E5-large
    at 1024 dims) and zero-padded to a larger size (e.g. 1536) for storage.
    Truncation strips the trailing zero-padded dimensions.
    
    Args:
        embeddings_value: pgvector-format string, e.g. "[0.1,0.2,...,0.0]"
        target_dim: Target dimension count (default 1024)
        warn_nonzero_tail: If True, print a warning when truncated dims are non-zero
        
    Returns:
        Truncated pgvector-format string with target_dim dimensions
    """
    if not embeddings_value:
        return embeddings_value
    
    # Strip surrounding brackets and whitespace
    cleaned = embeddings_value.strip()
    if cleaned.startswith('['):
        cleaned = cleaned[1:]
    if cleaned.endswith(']'):
        cleaned = cleaned[:-1]
    
    # Split into individual float strings
    parts = [p.strip() for p in cleaned.split(',') if p.strip()]
    source_dim = len(parts)
    
    # If already at or below target dim, return as-is
    if source_dim <= target_dim:
        return embeddings_value
    
    # Warn if truncated tail contains non-zero values
    if warn_nonzero_tail:
        tail = parts[target_dim:]
        nonzero_count = sum(1 for v in tail if abs(float(v)) > 1e-9)
        if nonzero_count > 0:
            import sys
            print(
                f"WARNING: Truncating embedding from {source_dim} to {target_dim} dims, "
                f"but {nonzero_count}/{len(tail)} truncated values are non-zero. "
                f"This may indicate the vector was NOT zero-padded.",
                file=sys.stderr
            )
    
    # Truncate and re-serialize
    truncated = parts[:target_dim]
    return '[' + ','.join(truncated) + ']'


def extract_content_from_document(document_text: str) -> tuple:
    """
    Extract original_content and translated_content from legacy document field.
    
    Format: "excerptKeywords: ...\n\ntranslated_content:\n...\n\noriginal_content:\n..."
    
    Returns:
        (original_content, translated_content) tuple
    """
    if not document_text:
        return ('', None)
    
    original_content = document_text
    translated_content = None
    
    # Extract original_content
    if 'original_content:' in document_text:
        parts = document_text.split('original_content:')
        if len(parts) > 1:
            original_content = parts[1].strip()
    
    # Extract translated_content
    if 'translated_content:' in document_text and 'original_content:' in document_text:
        try:
            start_idx = document_text.index('translated_content:') + len('translated_content:')
            end_idx = document_text.index('original_content:')
            translated_content = document_text[start_idx:end_idx].strip()
        except (ValueError, IndexError):
            translated_content = None
    
    return (original_content, translated_content)


def generate_chunk_and_embedding_inserts(
    row: pd.Series,
    chunk_index: int,
    namespace_uuid: str = '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b',
    default_embedding_model: str = 'text-embedding-ada-002',
    skip_empty_embeddings: bool = False,
    target_embedding_dim: Optional[int] = None
) -> Optional[str]:
    """
    Generate INSERT statements for BOTH chunk and embedding from a single source row.
    
    Args:
        row: Pandas Series with jeen_dev data
        chunk_index: The chunk index within its document
        namespace_uuid: Fixed namespace UUID for chunk_id generation
        default_embedding_model: Default model name if not specified
        skip_empty_embeddings: If True, skip rows without embeddings
        target_embedding_dim: If set, truncate embeddings to this dimension (e.g. 1024)
        
    Returns:
        SQL statements (both chunk and embedding) or None to skip
    """
    # Extract and clean fields
    legacy_id = clean_string(row.get('id'))
    external_id = clean_string(row.get('external_id'))
    collection = clean_string(row.get('collection'))
    document_text = row.get('document', '')
    embeddings = row.get('embeddings')
    
    # Parse metadata JSON
    metadata_raw = row.get('metadata')
    if isinstance(metadata_raw, str):
        try:
            metadata = json.loads(metadata_raw.replace("'", '"'))
        except:
            metadata = {}
    elif isinstance(metadata_raw, dict):
        metadata = metadata_raw
    else:
        metadata = {}
    
    doc_id = metadata.get('doc_id')
    user_id = metadata.get('user_id')
    meta_type = metadata.get('type')
    tags = metadata.get('tags')
    file_title = metadata.get('file_title')
    create_date = metadata.get('create_date')
    link_to_file = metadata.get('link_to_file')
    excerpt_keywords = metadata.get('excerptKeywords')
    
    # Skip if no id or not chunk-data type
    if not legacy_id or meta_type != 'chunk-data':
        return None
    
    # Skip if no doc_id (can't link to document)
    if not doc_id:
        return None
    
    # Skip if embeddings are null and skip_empty_embeddings is True
    if skip_empty_embeddings and (embeddings is None or pd.isna(embeddings)):
        return None
    
    # Extract content
    original_content, translated_content = extract_content_from_document(document_text)
    
    if not original_content:
        original_content = document_text  # Fallback to full document
    
    # Calculate content stats
    char_count = len(original_content)
    word_count = len(original_content.split()) if original_content else 0
    content_hash = f"md5('{original_content.replace(chr(39), chr(39)+chr(39))}')"
    
    # Determine file_type from file_title
    file_type = 'unknown'
    if file_title:
        file_title_lower = file_title.lower()
        if file_title_lower.endswith('.pdf'):
            file_type = 'pdf'
        elif file_title_lower.endswith('.docx'):
            file_type = 'docx'
        elif file_title_lower.endswith('.pptx'):
            file_type = 'pptx'
        elif file_title_lower.endswith('.xlsx'):
            file_type = 'xlsx'
        elif file_title_lower.endswith('.txt'):
            file_type = 'txt'
        elif file_title_lower.endswith('.csv'):
            file_type = 'csv'
        elif file_title_lower.endswith('.html'):
            file_type = 'html'
    
    # Build chunk metadata
    chunk_metadata = {
        'parser': 'legacy-migration',
        'file_name': file_title,
        'file_type': file_type,
        'legacyData': {
            'legacy_id': legacy_id,
            'external_id': external_id,
            'collection': collection,
            'type': meta_type,
            'tags': tags,
            'user_id': user_id,
            'create_date': create_date,
            'link_to_file': link_to_file,
            'excerptKeywords': excerpt_keywords
        }
    }
    
    chunk_metadata_sql = escape_json_for_sql(chunk_metadata)
    
    # Parse created_at
    if create_date:
        try:
            created_at_dt = pd.to_datetime(create_date)
            created_at_sql = f"'{created_at_dt.isoformat()}'"
        except:
            created_at_sql = 'now()'
    else:
        created_at_sql = 'now()'
    
    # Generate a unique tag for the outer DO block so that $$ inside
    # dollar-quoted content (e.g. Hebrew text containing "$$") cannot
    # prematurely close the PL/pgSQL block.
    outer_tag = 'chunk_fn'
    all_content = (original_content or '') + (translated_content or '')
    while f'${outer_tag}$' in all_content:
        outer_tag = outer_tag + '_'
    
    # Generate chunk INSERT
    chunk_sql = f"""
-- Chunk from legacy ID: {legacy_id} (doc_id: {doc_id})
DO ${outer_tag}$
DECLARE
    v_chunk_id uuid := uuid_generate_v5('{namespace_uuid}'::uuid, '{legacy_id}');
    v_old_doc_id VARCHAR := {escape_sql_string(doc_id)};
    v_document_id uuid;
BEGIN
    -- Lookup document_id from migration mapping table (FAST)
    v_document_id := migration.get_new_id('documents', v_old_doc_id);
    
    -- Skip if document not found in mapping
    IF v_document_id IS NULL THEN
        RAISE NOTICE 'Skipping chunk % - document % not migrated', '{legacy_id}', v_old_doc_id;
        RETURN;
    END IF;
    
    -- Verify document actually exists (mapping can be stale)
    IF NOT EXISTS (SELECT 1 FROM documents WHERE id = v_document_id) THEN
        RAISE NOTICE 'Skipping chunk % - document % has stale mapping (not in documents table)', '{legacy_id}', v_old_doc_id;
        RETURN;
    END IF;
    
    -- Insert chunk if not exists
    IF NOT EXISTS (
        SELECT 1 FROM chunks
        WHERE id = v_chunk_id
    ) THEN
        INSERT INTO chunks (
            id,
            document_id,
            chunk_index,
            content,
            content_hash,
            content_type,
            page_number,
            char_count,
            word_count,
            metadata,
            created_at,
            translated_content
        ) VALUES (
            v_chunk_id,
            v_document_id,
            {chunk_index},
            {escape_sql_string_with_dollar_quotes(original_content, 'ORIG')},
            {content_hash},
            'text'::chunks_content_type_enum,
            NULL,
            {char_count},
            {word_count},
            {chunk_metadata_sql},
            {created_at_sql},
            {escape_sql_string_with_dollar_quotes(translated_content, 'TRANS') if translated_content else 'NULL'}
        );
    END IF;
END ${outer_tag}$;
"""
    
    # Generate embedding INSERT (only if embeddings exist)
    embedding_sql = ''
    if embeddings is not None and not pd.isna(embeddings):
        # Convert embeddings to proper format if needed
        if isinstance(embeddings, str):
            embeddings_value = embeddings
        else:
            embeddings_value = str(embeddings)
        
        # Truncate embedding vector if target dimension is specified
        if target_embedding_dim is not None:
            embeddings_value = truncate_embedding_vector(
                embeddings_value, target_embedding_dim
            )
        
        # Use a named tag for the outer DO block (consistent with chunk block)
        emb_tag = 'emb_fn'
        
        embedding_sql = f"""
-- Embedding for chunk {legacy_id}
DO ${emb_tag}$
DECLARE
    v_chunk_id uuid := uuid_generate_v5('{namespace_uuid}'::uuid, '{legacy_id}');
    v_old_doc_id VARCHAR := {escape_sql_string(doc_id)};
    v_document_id uuid;
BEGIN
    -- Lookup document_id from migration mapping table (FAST)
    v_document_id := migration.get_new_id('documents', v_old_doc_id);
    
    IF v_document_id IS NULL THEN
        RETURN;
    END IF;
    
    -- Verify document actually exists
    IF NOT EXISTS (SELECT 1 FROM documents WHERE id = v_document_id) THEN
        RETURN;
    END IF;
    
    -- Insert embedding if not exists
    IF NOT EXISTS (
        SELECT 1 FROM embeddings
        WHERE chunk_id = v_chunk_id
    ) THEN
        INSERT INTO embeddings (
            id,
            chunk_id,
            document_id,
            embedding,
            model_name,
            created_at
        ) VALUES (
            gen_random_uuid(),
            v_chunk_id,
            v_document_id,
            '{embeddings_value}'::vector,
            '{default_embedding_model}',
            {created_at_sql}
        );
    END IF;
END ${emb_tag}$;
"""
    
    # Combine both SQLs
    combined_sql = chunk_sql
    if embedding_sql:
        combined_sql += '\n' + embedding_sql
    
    return combined_sql


def generate_chunks_embeddings_migration_sql(
    jeen_dev_df: pd.DataFrame,
    output_file: str,
    source_info: str,
    namespace_uuid: str = '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b',
    default_embedding_model: str = 'text-embedding-ada-002',
    skip_empty_embeddings: bool = False,
    target_embedding_dim: Optional[int] = None
) -> Dict[str, Any]:
    """
    Generate SQL migration file for chunks and embeddings tables.
    Each source row generates TWO inserts: one for chunk, one for embedding.
    
    Args:
        jeen_dev_df: DataFrame with jeen_dev data
        output_file: Path to output SQL file
        source_info: Source database info string
        namespace_uuid: Fixed namespace UUID for chunk_id generation
        default_embedding_model: Default embedding model name
        skip_empty_embeddings: If True, skip rows without embeddings
        target_embedding_dim: If set, truncate embeddings to this dimension (e.g. 1024)
        
    Returns:
        Dictionary with generation stats
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Clean up old chunks/embeddings migration files
    cleanup_old_migration_files(output_file, '04_chunks_embeddings_')
    
    # Group by doc_id to assign chunk_index
    jeen_dev_df['doc_id_from_metadata'] = jeen_dev_df['metadata'].apply(
        lambda x: json.loads(x.replace("'", '"')).get('doc_id') if isinstance(x, str) else x.get('doc_id') if isinstance(x, dict) else None
    )
    
    # Sort by doc_id and id for consistent chunk_index
    jeen_dev_sorted = jeen_dev_df.sort_values(['doc_id_from_metadata', 'id'])
    
    # Assign chunk_index per document
    jeen_dev_sorted['chunk_index'] = jeen_dev_sorted.groupby('doc_id_from_metadata').cumcount()
    
    chunk_count = 0
    embedding_count = 0
    skipped_count = 0
    
    with open(output_file, 'w', encoding='utf-8') as sql_file:
        # Write header
        header = f"""-- ============================================================
-- CHUNKS & EMBEDDINGS MIGRATION SQL
-- ============================================================
-- Generated: {datetime.now().isoformat()}
-- Source: {source_info}
-- Destination: chunks + embeddings tables
-- Records to migrate: {len(jeen_dev_df)}
-- 
-- IMPORTANT: This script will INSERT chunks AND embeddings!
-- IMPORTANT: Run users, folders, and documents migrations first.
--
-- Each legacy row creates TWO inserts:
--   1. chunks table - stores text content
--   2. embeddings table - stores vector (if available)
--
-- Uses deterministic UUID generation (uuid_generate_v5) for chunk_id.
-- Namespace UUID: {namespace_uuid}
-- Default embedding model: {default_embedding_model}
-- Skip rows without embeddings: {skip_empty_embeddings}
-- Target embedding dimension: {target_embedding_dim or 'None (keep original)'}
-- ============================================================

-- Ensure PostgreSQL interprets this file as UTF-8 (required for Hebrew/multilingual content)
SET client_encoding = 'UTF8';

-- Ensure uuid-ossp extension is available
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- CONFIRMATION PROMPT: User must confirm before execution
DO $$
DECLARE
    user_confirmation TEXT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'CHUNKS & EMBEDDINGS MIGRATION - CONFIRMATION REQUIRED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'This script will migrate {len(jeen_dev_df)} chunks/embeddings';
    RAISE NOTICE 'Namespace UUID: {namespace_uuid}';
    RAISE NOTICE 'Default embedding model: {default_embedding_model}';
    RAISE NOTICE 'Generated: {datetime.now().isoformat()}';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'PREREQUISITE: Users, folders, and documents must be migrated first!';
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
-- \\\\prompt 'Type YES to confirm and continue with migration: ' user_confirmation
-- \\\\if :'user_confirmation' != 'YES'
--   \\\\echo 'Migration cancelled by user.'
--   \\\\quit
-- \\\\endif

-- Ensure embeddings column matches target dimension ({target_embedding_dim or 'original'})
DO $$
DECLARE
    v_current_dim INTEGER;
BEGIN
    -- Check current vector dimension on the embeddings table
    SELECT atttypmod INTO v_current_dim
    FROM pg_attribute
    WHERE attrelid = 'public.embeddings'::regclass
      AND attname = 'embedding';

    IF v_current_dim IS NOT NULL AND v_current_dim != {target_embedding_dim or 0} THEN
        RAISE NOTICE '⚠️  DIMENSION TRIM: embeddings.embedding column resized from % to {target_embedding_dim or 'original'} dimensions. Source vectors will be truncated to fit.', v_current_dim;
        ALTER TABLE public.embeddings ALTER COLUMN embedding TYPE vector({target_embedding_dim or 1024});
    END IF;
END $$;

"""
        sql_file.write(header)
        
        # Include migration schema setup (idempotent - safe if already exists in this DB)
        sql_file.write(generate_migration_schema_setup())
        sql_file.write('\n')
        
        # Write individual INSERT statements
        for _, row in jeen_dev_sorted.iterrows():
            sql = generate_chunk_and_embedding_inserts(
                row,
                chunk_index=int(row['chunk_index']),
                namespace_uuid=namespace_uuid,
                default_embedding_model=default_embedding_model,
                skip_empty_embeddings=skip_empty_embeddings,
                target_embedding_dim=target_embedding_dim
            )
            if sql:
                sql_file.write(sql)
                sql_file.write('\n')
                chunk_count += 1
                # Check if embedding was included
                if 'INSERT INTO embeddings' in sql:
                    embedding_count += 1
            else:
                skipped_count += 1
        
        # Write footer
        sql_file.write(f'\n-- Total chunks processed: {chunk_count}\n')
        sql_file.write(f'-- Total embeddings processed: {embedding_count}\n')
        sql_file.write(f'-- Skipped: {skipped_count}\n')
    
    return {
        'file': output_file,
        'chunks_processed': chunk_count,
        'embeddings_processed': embedding_count,
        'skipped': skipped_count
    }


def extract_question_from_jsonb(question_data) -> str:
    """
    Extract user question from the question jsonb column.
    Format: question->[1]->>'value' (index 1 is current turn's user question)
    """
    if not question_data or pd.isna(question_data):
        return '[no question text]'
    
    try:
        if isinstance(question_data, str):
            question_json = json.loads(question_data.replace("'", '"'))
        else:
            question_json = question_data
        
        # Try to get question from index 1
        if isinstance(question_json, list) and len(question_json) > 1:
            if isinstance(question_json[1], dict) and 'value' in question_json[1]:
                return question_json[1]['value']
        
        return '[no question text]'
    except:
        return '[no question text]'


def generate_conversations_logs_migration_sql(
    logs_df: pd.DataFrame,
    output_file: str,
    source_info: str,
    namespace_uuid: str = '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b',
    max_records_per_insert: int = 50,
    user_id_overrides: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Generate SQL migration file for jeen_dev_logs.
    Creates multi-INSERT statements grouped by user for 3 tables:
      - conversations (aggregated per chat_id)
      - messages (user + assistant pairs)
      - message_content_blocks (one per message)
    
    Args:
        logs_df: DataFrame with jeen_dev_logs data
        output_file: Path to output SQL file
        source_info: Source database info string
        namespace_uuid: Fixed namespace UUID for deterministic IDs
        max_records_per_insert: Max conversations per INSERT (for batching)
        
    Returns:
        Dictionary with generation stats
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Clean up old conversations migration files
    cleanup_old_migration_files(output_file, '05_conversations_')
    
    # Filter: only rows with user_id and chat_id
    logs_df = logs_df[
        logs_df['user_id'].notna() & 
        logs_df['chat_id'].notna()
    ].copy()
    
    if len(logs_df) == 0:
        return {
            'file': output_file,
            'users_processed': 0,
            'conversations_processed': 0,
            'messages_processed': 0,
            'blocks_processed': 0
        }
    
    # Add question_number if not present (for ordering)
    if 'question_number' not in logs_df.columns:
        logs_df['question_number'] = logs_df.groupby('chat_id').cumcount()
    
    # Add message_index if not present
    if 'message_index' not in logs_df.columns:
        logs_df['message_index'] = logs_df['question_number']
    
    users_processed = 0
    conversations_processed = 0
    messages_processed = 0
    blocks_processed = 0
    
    with open(output_file, 'w', encoding='utf-8') as sql_file:
        # Write header
        header = f"""-- ============================================================
-- CONVERSATIONS, MESSAGES & MESSAGE_CONTENT_BLOCKS MIGRATION SQL
-- ============================================================
-- Generated: {datetime.now().isoformat()}
-- Source: {source_info}
-- Destination: conversations + messages + message_content_blocks
-- Source rows: {len(logs_df)}
-- 
-- IMPORTANT: This script will INSERT data into 3 tables!
-- IMPORTANT: Run users migration first.
--
-- Each source row creates entries in 3 tables:
--   1. conversations (aggregated per chat_id)
--   2. messages (user + assistant per row)
--   3. message_content_blocks (one per message)
--
-- Uses deterministic UUID generation (uuid_generate_v5).
-- Namespace UUID: {namespace_uuid}
-- Multi-INSERT format: grouped by user, max {max_records_per_insert} conversations per INSERT
-- ============================================================

-- Ensure PostgreSQL interprets this file as UTF-8 (required for Hebrew/multilingual content)
SET client_encoding = 'UTF8';

-- Ensure uuid-ossp extension is available
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- CONFIRMATION PROMPT
DO $$
DECLARE
    user_confirmation TEXT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'CONVERSATIONS/MESSAGES MIGRATION - CONFIRMATION REQUIRED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'This script will migrate conversations and messages';
    RAISE NOTICE 'Source rows: {len(logs_df)}';
    RAISE NOTICE 'Namespace UUID: {namespace_uuid}';
    RAISE NOTICE 'Generated: {datetime.now().isoformat()}';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'PREREQUISITE: Users must be migrated first!';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';
    
    user_confirmation := NULL;
    
    IF current_setting('is_superuser') = 'off' THEN
        RAISE NOTICE 'Ready to proceed. Press Ctrl+C to cancel or Enter to continue...';
    END IF;
    
    RAISE NOTICE 'Starting migration...';
    RAISE NOTICE '';
END $$;

-- Uncomment for manual confirmation
-- \\\\prompt 'Type YES to confirm: ' user_confirmation
-- \\\\if :'user_confirmation' != 'YES'
--   \\\\echo 'Migration cancelled.'
--   \\\\quit
-- \\\\endif

"""
        sql_file.write(header)
        
        # Include migration schema setup (idempotent - safe if already exists in this DB)
        sql_file.write(generate_migration_schema_setup())
        sql_file.write('\n')
        
        # Group by user_id
        for user_id, user_logs in logs_df.groupby('user_id'):
            users_processed += 1
            
            # Get user's conversations (aggregate by chat_id)
            conversations = []
            for chat_id, chat_logs in user_logs.groupby('chat_id'):
                # Sort by question_number/message_index for proper ordering
                chat_logs_sorted = chat_logs.sort_values(
                    by=['message_index', 'question_number', 'created_at'],
                    na_position='last'
                )
                
                # Aggregate conversation data
                latest_row = chat_logs_sorted.iloc[-1]
                title = clean_string(latest_row.get('title')) or f'Conversation {chat_id[:8]}'
                message_count = len(chat_logs_sorted) * 2  # user + assistant per row
                total_tokens = int(chat_logs_sorted['token_amount'].fillna(0).sum())
                created_at = chat_logs_sorted['created_at'].min()
                updated_at = chat_logs_sorted['created_at'].max()
                
                conversations.append({
                    'chat_id': chat_id,
                    'user_id': user_id,
                    'title': title,
                    'message_count': message_count,
                    'total_tokens': total_tokens,
                    'created_at': created_at,
                    'updated_at': updated_at,
                    'logs': chat_logs_sorted
                })
            
            # Batch conversations if needed
            for batch_idx, conv_batch in enumerate([conversations[i:i+max_records_per_insert] 
                                                     for i in range(0, len(conversations), max_records_per_insert)]):
                
                sql_file.write(f"\n-- User: {user_id} (Batch {batch_idx + 1}, {len(conv_batch)} conversations)\n\n")
                
                # Generate conversations INSERT
                conv_values = []
                for conv in conv_batch:
                    created_at_str = conv['created_at'].isoformat() if pd.notna(conv['created_at']) else 'now()'
                    updated_at_str = conv['updated_at'].isoformat() if pd.notna(conv['updated_at']) else 'now()'
                    
                    conv_values.append(
                        f"    ('{conv['chat_id']}'::uuid, {escape_sql_string(conv['title'])}, "
                        f"{conv['message_count']}, {conv['total_tokens']}, "
                        f"true, NULL::timestamp, '{created_at_str}'::timestamptz, '{updated_at_str}'::timestamptz, "
                        f"'{updated_at_str}'::timestamptz, "
                        f"{resolve_user_id_sql(str(user_id), user_id_overrides)})"
                    )
                
                conv_values_joined = ',\n'.join(conv_values)
                sql_file.write(f"""-- Conversations INSERT
INSERT INTO conversations (id, title, message_count, total_tokens, is_active, deleted_at, created_at, updated_at, last_interacted_at, user_id)
SELECT * FROM (
  VALUES
{conv_values_joined}
) AS v(id, title, message_count, total_tokens, is_active, deleted_at, created_at, updated_at, last_interacted_at, user_id)
WHERE NOT EXISTS (SELECT 1 FROM conversations WHERE id = v.id);

""")
                conversations_processed += len(conv_batch)
                
                # Generate messages and content blocks INSERT
                msg_values = []
                block_values = []
                
                for conv in conv_batch:
                    prev_assistant_msg_id = None
                    
                    for turn_idx, (_, log_row) in enumerate(conv['logs'].iterrows()):
                        legacy_id = clean_string(log_row['id'])
                        created_at = log_row['created_at']
                        created_at_str = created_at.isoformat() if pd.notna(created_at) else 'now()'
                        
                        # Generate deterministic message IDs
                        user_msg_id = f"uuid_generate_v5('{namespace_uuid}'::uuid, '{legacy_id}-user')"
                        assistant_msg_id = f"uuid_generate_v5('{namespace_uuid}'::uuid, '{legacy_id}-assistant')"
                        
                        # User message
                        user_parent = f"'{prev_assistant_msg_id}'" if prev_assistant_msg_id else 'NULL::uuid'
                        user_created_at = f"'{created_at_str}'::timestamptz - interval '1 second'" if pd.notna(created_at) else 'now()'
                        
                        msg_values.append(
                            f"    ({user_msg_id}, '{conv['chat_id']}'::uuid, {user_parent}, 'user'::messages_role_enum, "
                            f"false, 1, 1, NULL::text, {user_created_at}, {user_created_at}, NULL::timestamp, "
                            f"{resolve_user_id_sql(str(user_id), user_id_overrides)}, '{{}}'::jsonb)"
                        )
                        
                        # Assistant message
                        # Build metadata
                        toolkit_settings = log_row.get('toolkit_settings')
                        model_name = None
                        if pd.notna(toolkit_settings):
                            try:
                                ts_json = json.loads(str(toolkit_settings).replace("'", '"')) if isinstance(toolkit_settings, str) else toolkit_settings
                                model_name = ts_json.get('model') if isinstance(ts_json, dict) else None
                            except:
                                pass
                        
                        is_like = log_row.get('is_like')
                        is_like_json = None
                        if pd.notna(is_like) and str(is_like).strip():
                            try:
                                is_like_json = json.loads(str(is_like).replace("'", '"'))
                            except:
                                pass
                        
                        metadata = {
                            'model': model_name,
                            'type': clean_string(log_row.get('type')),
                            'bot_id': clean_string(log_row.get('bot_id')),
                            'is_like': is_like_json,
                            'token_amount': int(log_row.get('token_amount', 0)) if pd.notna(log_row.get('token_amount')) else None,
                            'words_amount': int(log_row.get('words_amount', 0)) if pd.notna(log_row.get('words_amount')) else None,
                            'calculated_time': int(log_row.get('calculated_time', 0)) if pd.notna(log_row.get('calculated_time')) else None,
                            'category': clean_string(log_row.get('category')),
                            'sentiment': clean_string(log_row.get('sentiment')),
                            'legacyData': {
                                'legacy_log_id': legacy_id,
                                'title': clean_string(log_row.get('title')),
                                'toolkit_settings': ts_json if 'ts_json' in locals() else None,
                                'sourcetext': clean_string(log_row.get('sourcetext')),
                                'sourcelink': clean_string(log_row.get('sourcelink')),
                                'webpagelink': clean_string(log_row.get('webpagelink')),
                                'documents_selected': clean_string(log_row.get('documents_selected'))
                            }
                        }
                        metadata_escaped = escape_json_for_sql(metadata)
                        
                        msg_values.append(
                            f"    ({assistant_msg_id}, '{conv['chat_id']}'::uuid, {user_msg_id}, 'assistant'::messages_role_enum, "
                            f"false, 1, 1, 'stop', '{created_at_str}'::timestamptz, '{created_at_str}'::timestamptz, NULL::timestamp, "
                            f"{resolve_user_id_sql(str(user_id), user_id_overrides)}, {metadata_escaped})"
                        )
                        
                        # Content blocks
                        # User content block
                        user_question = extract_question_from_jsonb(log_row.get('question'))
                        # Fallback to question_in_english
                        if user_question == '[no question text]':
                            user_question = clean_string(log_row.get('question_in_english')) or '[no question text]'
                        
                        user_content = {
                            'role': 'user',
                            'type': 'message',
                            'content': [{'text': user_question, 'type': 'text'}]
                        }
                        user_content_escaped = escape_json_for_sql(user_content)
                        
                        block_values.append(
                            f"    (uuid_generate_v5('{namespace_uuid}'::uuid, '{legacy_id}-user-block-0'), "
                            f"{user_msg_id}, 0, 'message'::message_content_blocks_type_enum, "
                            f"{user_content_escaped}, NULL::integer, {user_created_at})"
                        )
                        
                        # Assistant content block
                        assistant_answer = clean_string(log_row.get('answer')) or ''
                        assistant_content = {
                            'role': 'assistant',
                            'type': 'message',
                            'content': [{'text': assistant_answer, 'type': 'text'}]
                        }
                        assistant_content_escaped = escape_json_for_sql(assistant_content)
                        
                        calc_time = int(log_row.get('calculated_time', 0)) if pd.notna(log_row.get('calculated_time')) else None
                        exec_time_sql = str(calc_time) if calc_time is not None else 'NULL::integer'
                        
                        block_values.append(
                            f"    (uuid_generate_v5('{namespace_uuid}'::uuid, '{legacy_id}-assistant-block-0'), "
                            f"{assistant_msg_id}, 0, 'message'::message_content_blocks_type_enum, "
                            f"{assistant_content_escaped}, {exec_time_sql}, '{created_at_str}'::timestamptz)"
                        )
                        
                        # Update prev_assistant_msg_id for next turn
                        prev_assistant_msg_id = assistant_msg_id
                        messages_processed += 2
                        blocks_processed += 2
                
                # Write messages INSERT
                if msg_values:
                    msg_values_joined = ',\n'.join(msg_values)
                    sql_file.write(f"""-- Messages INSERT
INSERT INTO messages (id, conversation_id, parent_message_id, role, has_tool_calls, iteration_count, content_block_count, finish_reason, created_at, updated_at, deleted_at, user_id, metadata)
SELECT * FROM (
  VALUES
{msg_values_joined}
) AS v(id, conversation_id, parent_message_id, role, has_tool_calls, iteration_count, content_block_count, finish_reason, created_at, updated_at, deleted_at, user_id, metadata)
WHERE NOT EXISTS (SELECT 1 FROM messages WHERE id = v.id);

""")
                
                # Write content blocks INSERT
                if block_values:
                    block_values_joined = ',\n'.join(block_values)
                    sql_file.write(f"""-- Message Content Blocks INSERT
INSERT INTO message_content_blocks (id, message_id, sequence, type, content, execution_time_ms, created_at)
SELECT * FROM (
  VALUES
{block_values_joined}
) AS v(id, message_id, sequence, type, content, execution_time_ms, created_at)
WHERE NOT EXISTS (SELECT 1 FROM message_content_blocks WHERE id = v.id);

""")
        
        # Write footer
        sql_file.write(f"""\n-- ============================================================
-- MIGRATION SUMMARY
-- ============================================================
-- Users processed: {users_processed}
-- Conversations processed: {conversations_processed}
-- Messages processed: {messages_processed}
-- Content blocks processed: {blocks_processed}
-- ============================================================
""")
    
    return {
        'file': output_file,
        'users_processed': users_processed,
        'conversations_processed': conversations_processed,
        'messages_processed': messages_processed,
        'blocks_processed': blocks_processed
    }


def _parse_jsonb(val) -> Optional[dict]:
    """Parse a JSONB column from DataFrame (could be dict, JSON string, or None)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val.replace("'", '"'))
        except:
            return None
    return None


def _safe_get(d: Optional[dict], key: str, default=None):
    """Safely get a key from a dict that might be None."""
    if d is None:
        return default
    return d.get(key, default)


def generate_agent_insert(
    row: pd.Series,
    namespace_uuid: str = '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b',
    user_id_overrides: Optional[Dict[str, str]] = None
) -> Optional[str]:
    """
    Generate INSERT statements for a single agent (agents + agent_settings + agent_documents).
    
    Extracts all JSONB fields in Python and generates a self-contained DO block.
    
    Args:
        row: Pandas Series with playground_bot_generator_config data
        namespace_uuid: Fixed namespace UUID for deterministic IDs
        
    Returns:
        SQL statements or None to skip
    """
    bot_id = clean_string(row.get('bot_id'))
    user_id = clean_string(row.get('user_id'))

    # folder_id is int4 in V4 — pandas may load it as float (e.g. 1393.0).
    # Must normalise to plain integer string so migration.id_mappings lookup matches.
    _folder_id_raw = row.get('folder_id')
    if _folder_id_raw is not None and not (isinstance(_folder_id_raw, float) and pd.isna(_folder_id_raw)):
        try:
            folder_id = str(int(float(_folder_id_raw)))
        except (ValueError, TypeError):
            folder_id = clean_string(_folder_id_raw)
    else:
        folder_id = None
    
    if not bot_id:
        return None
    
    # Parse JSONB columns
    bot_data = _parse_jsonb(row.get('bot_data'))
    toolkit_settings = _parse_jsonb(row.get('toolkit_settings'))
    character_prompts = _parse_jsonb(row.get('character_prompts'))
    hack_prompt = _parse_jsonb(row.get('hack_prompt'))
    analysis_prompt = _parse_jsonb(row.get('analysis_prompt'))
    grade_prompt = _parse_jsonb(row.get('grade_prompt'))
    relevant_answer_prompt = _parse_jsonb(row.get('relevant_answer_prompt'))
    additional_links_title = _parse_jsonb(row.get('additional_links_title'))
    
    # Extract fields
    bot_name = clean_string(_safe_get(bot_data, 'bot_name')) or 'Unnamed Agent'
    bot_description = (_safe_get(bot_data, 'bot_description') or '')[:2048]
    first_message = clean_string(row.get('first_message'))
    
    # Derive agent_type — all legacy bots migrate as 'cortex'
    agent_type = 'cortex'
    
    # Avatar URL
    avatar_url = None
    if toolkit_settings:
        assistant_icon = _safe_get(toolkit_settings, 'assistantIcon')
        if isinstance(assistant_icon, dict):
            avatar_url = _safe_get(assistant_icon, 'url')
        if not avatar_url:
            avatar_url = _safe_get(toolkit_settings, 'logo_url')
    if avatar_url:
        avatar_url = str(avatar_url)[:512]
    
    # is_active
    is_active = True
    if toolkit_settings and _safe_get(toolkit_settings, 'is_active') == 'Yes':
        is_active = True
    elif toolkit_settings and _safe_get(toolkit_settings, 'is_active') is not None:
        is_active = _safe_get(toolkit_settings, 'is_active') == 'Yes'
    
    # Model (fallback chain)
    model = None
    for prompt_data in [character_prompts, hack_prompt, analysis_prompt, relevant_answer_prompt]:
        m = clean_string(_safe_get(prompt_data, 'model'))
        if m:
            model = m[:128]
            break
    
    # Instructions
    instructions = clean_string(_safe_get(character_prompts, 'content'))
    
    # Enabled tools
    enabled_tools = []
    ts_data = _safe_get(toolkit_settings, 'data') if toolkit_settings else None
    if isinstance(ts_data, dict):
        enabled_tools = [k for k, v in ts_data.items() if str(v).lower() == 'true']
    enabled_tools_sql = escape_json_for_sql(enabled_tools)
    
    # Conversation starters
    if first_message and first_message.strip():
        conversation_starters_sql = escape_json_for_sql([first_message])
    else:
        conversation_starters_sql = escape_json_for_sql([])
    
    # RAG settings
    base_answers_on_files_only = False
    for src in [toolkit_settings, relevant_answer_prompt]:
        val = _safe_get(src, 'isAnswerBasedOnBestGrade')
        if val is not None:
            base_answers_on_files_only = str(val).lower() == 'true'
            break
    
    retrieved_context_size = None
    for src_key in [('vectorsNumber', toolkit_settings), ('vectorsNumber', _safe_get(grade_prompt, 'vectors') if grade_prompt else None)]:
        val = _safe_get(src_key[1], src_key[0])
        if val is not None:
            try:
                retrieved_context_size = int(val)
            except:
                pass
            break
    
    re_rank_score = None
    for src_key in [('passingGrade', toolkit_settings), ('passingGrade', _safe_get(grade_prompt, 'vectors') if grade_prompt else None)]:
        val = _safe_get(src_key[1], src_key[0])
        if val is not None:
            try:
                re_rank_score = round(float(val) / 100.0, 4)
            except:
                pass
            break
    
    search_in_english = False
    if toolkit_settings and _safe_get(toolkit_settings, 'inputVectorsLanguage') == 'To English':
        search_in_english = True
    
    # Questions selected flags
    qs = _safe_get(toolkit_settings, 'questions_selected') if toolkit_settings else None
    show_source_links = False
    show_source_text = False
    follow_up_questions = False
    if isinstance(qs, (list, dict)):
        qs_items = qs if isinstance(qs, list) else list(qs.keys())
        show_source_links = 'Display the source link' in qs_items
        show_source_text = 'Display the source text' in qs_items
        follow_up_questions = 'Follow-up questions' in qs_items
    
    additional_links = False
    if additional_links_title and str(_safe_get(additional_links_title, 'is_selected', '')).lower() == 'true':
        additional_links = True
    
    # Parse timestamps
    created_at_val = row.get('created_at')
    if pd.notna(created_at_val):
        try:
            created_at_sql = f"'{pd.to_datetime(created_at_val).isoformat()}'::timestamptz"
        except:
            created_at_sql = 'now()'
    else:
        created_at_sql = 'now()'
    
    updated_at_val = row.get('updated_at')
    if pd.notna(updated_at_val):
        try:
            updated_at_sql = f"'{pd.to_datetime(updated_at_val).isoformat()}'::timestamptz"
        except:
            updated_at_sql = created_at_sql
    else:
        updated_at_sql = created_at_sql
    
    last_activity_val = row.get('last_activity')
    if pd.notna(last_activity_val):
        try:
            last_activity_sql = f"'{pd.to_datetime(last_activity_val).isoformat()}'::timestamptz"
        except:
            last_activity_sql = 'NULL::timestamp'
    else:
        last_activity_sql = 'NULL::timestamp'
    
    # Folder UUID SQL — use mapping lookup so agents land at root if folder wasn't migrated
    if folder_id:
        folder_id_sql = f"migration.get_new_id('folders', {escape_sql_string(folder_id)})"
    else:
        folder_id_sql = 'NULL::uuid'
    
    # Parse docs_chosen and chosen_docs_folders
    docs_chosen_raw = row.get('docs_chosen')
    docs_chosen = []
    if docs_chosen_raw is not None and not (isinstance(docs_chosen_raw, float) and pd.isna(docs_chosen_raw)):
        if isinstance(docs_chosen_raw, list):
            docs_chosen = [str(d).strip() for d in docs_chosen_raw if d and str(d).strip()]
        elif isinstance(docs_chosen_raw, str):
            # Could be PostgreSQL array format: {val1,val2,...}
            cleaned = docs_chosen_raw.strip('{}')
            if cleaned:
                docs_chosen = [d.strip().strip('"') for d in cleaned.split(',') if d.strip()]
    
    folders_chosen_raw = row.get('chosen_docs_folders')
    folders_chosen = []
    if folders_chosen_raw is not None and not (isinstance(folders_chosen_raw, float) and pd.isna(folders_chosen_raw)):
        if isinstance(folders_chosen_raw, list):
            # chosen_docs_folders is int4[] — normalise each element to plain integer string
            for f in folders_chosen_raw:
                if f is not None:
                    try:
                        folders_chosen.append(str(int(float(f))))
                    except (ValueError, TypeError):
                        v = str(f).strip()
                        if v:
                            folders_chosen.append(v)
        elif isinstance(folders_chosen_raw, str):
            cleaned = folders_chosen_raw.strip('{}')
            if cleaned:
                for f in cleaned.split(','):
                    f = f.strip()
                    if f:
                        try:
                            folders_chosen.append(str(int(float(f))))
                        except (ValueError, TypeError):
                            folders_chosen.append(f)
    
    # Build agent_documents SQL fragments
    doc_inserts = ''
    for doc_id in docs_chosen:
        doc_inserts += f"""
    -- Link document: {doc_id} (skip if document wasn't migrated)
    IF migration.get_new_id('documents', {escape_sql_string(doc_id)}) IS NOT NULL THEN
        INSERT INTO agent_documents (id, agent_id, document_id, is_active, type)
        SELECT
            uuid_generate_v5('{namespace_uuid}'::uuid, '{bot_id}-doc-{doc_id}'),
            v_agent_id,
            migration.get_new_id('documents', {escape_sql_string(doc_id)}),
            true,
            'document'::agent_documents_type_enum
        WHERE NOT EXISTS (
            SELECT 1 FROM agent_documents
            WHERE id = uuid_generate_v5('{namespace_uuid}'::uuid, '{bot_id}-doc-{doc_id}')
        );
        v_docs_linked := v_docs_linked + 1;
    ELSE
        RAISE NOTICE 'Agent {bot_id}: skipping document link {doc_id} — document not migrated';
    END IF;
"""
    
    for fid in folders_chosen:
        doc_inserts += f"""
    -- Link folder: {fid} (skip if folder wasn't migrated)
    IF migration.get_new_id('folders', {escape_sql_string(fid)}) IS NOT NULL THEN
        INSERT INTO agent_documents (id, agent_id, document_id, is_active, type)
        SELECT
            uuid_generate_v5('{namespace_uuid}'::uuid, '{bot_id}-folder-{fid}'),
            v_agent_id,
            migration.get_new_id('folders', {escape_sql_string(fid)}),
            true,
            'folder'::agent_documents_type_enum
        WHERE NOT EXISTS (
            SELECT 1 FROM agent_documents
            WHERE id = uuid_generate_v5('{namespace_uuid}'::uuid, '{bot_id}-folder-{fid}')
        );
        v_docs_linked := v_docs_linked + 1;
    ELSE
        RAISE NOTICE 'Agent {bot_id}: skipping folder link {fid} — folder not migrated';
    END IF;
"""
    
    # Build the DO block
    sql = f"""
-- Agent: {bot_name[:60]} (bot_id: {bot_id})
DO $agent_fn$
DECLARE
    v_agent_id uuid := uuid_generate_v5('{namespace_uuid}'::uuid, '{bot_id}-agent');
    v_settings_id uuid := uuid_generate_v5('{namespace_uuid}'::uuid, '{bot_id}-settings');
    v_user_id uuid := {resolve_user_id_sql(str(user_id), user_id_overrides)};
    v_docs_linked integer := 0;
BEGIN
    -- Insert agent if not exists
    IF NOT EXISTS (SELECT 1 FROM agents WHERE id = v_agent_id) THEN
        INSERT INTO agents (
            id, name, description, type, user_id, avatar_url,
            is_active, is_public, is_prebuilt, is_draft,
            folder_id, created_at, updated_at, last_interacted_at, deleted_at
        ) VALUES (
            v_agent_id,
            {escape_sql_string(bot_name[:128])},
            {escape_sql_string(bot_description) if bot_description else 'NULL'},
            '{agent_type}'::agents_type_enum,
            v_user_id,
            {escape_sql_string(avatar_url) if avatar_url else 'NULL'},
            {str(is_active).lower()},
            false,
            false,
            false,
            {folder_id_sql},
            {created_at_sql},
            {updated_at_sql},
            {last_activity_sql},
            NULL::timestamp
        );
    END IF;
    
    -- Insert agent settings if not exists
    IF NOT EXISTS (SELECT 1 FROM agent_settings WHERE agent_id = v_agent_id) THEN
        INSERT INTO agent_settings (
            id, agent_id, model, instructions, enabled_tools, conversation_starters,
            workflow_flow_id, base_answers_on_files_only, combines_multiple_answers,
            retrieved_context_size, re_rank_score, query_instructions,
            search_in_english, show_source_links, show_source_text,
            follow_up_questions, additional_links
        ) VALUES (
            v_settings_id,
            v_agent_id,
            {escape_sql_string(model) if model else 'NULL'},
            {escape_sql_string_with_dollar_quotes(instructions, 'INSTR') if instructions else 'NULL'},
            {enabled_tools_sql},
            {conversation_starters_sql},
            NULL::uuid,
            {str(base_answers_on_files_only).lower()},
            true,
            {str(retrieved_context_size) if retrieved_context_size is not None else 'NULL::integer'},
            {str(re_rank_score) if re_rank_score is not None else 'NULL::numeric'},
            NULL::text,
            {str(search_in_english).lower()},
            {str(show_source_links).lower()},
            {str(show_source_text).lower()},
            {str(follow_up_questions).lower()},
            {str(additional_links).lower()}
        );
    END IF;
{doc_inserts}
    -- Track in migration.id_mappings
    INSERT INTO migration.id_mappings (table_name, old_id, new_id, migration_batch, notes)
    VALUES ('agents', '{bot_id}', v_agent_id, 'agents_migration',
            'Type: {agent_type}. Docs linked: ' || v_docs_linked)
    ON CONFLICT (table_name, old_id) DO NOTHING;
    
    -- Track in legacy mapping table
    INSERT INTO legacy_bot_to_agent_mapping (old_bot_id, new_agent_id, agent_type, bot_name)
    VALUES ('{bot_id}', v_agent_id, '{agent_type}', {escape_sql_string(bot_name[:255])})
    ON CONFLICT (old_bot_id) DO NOTHING;
    
    RAISE NOTICE 'Migrated agent: % (%) → %', {escape_sql_string(bot_name[:60])}, '{agent_type}', v_agent_id;
END $agent_fn$;
"""
    return sql


def generate_agents_migration_sql(
    agents_df: pd.DataFrame,
    output_file: str,
    source_info: str,
    namespace_uuid: str = '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b',
    user_id_overrides: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Generate SQL migration file for agents from playground_bot_generator_config.
    
    Creates entries in 3 target tables:
      - agents (main agent record)
      - agent_settings (1:1 with agent)
      - agent_documents (many-to-many with documents/folders)
    
    Uses pre-extracted DataFrame data to generate self-contained INSERT statements.
    
    Args:
        agents_df: DataFrame with playground_bot_generator_config data
        output_file: Path to output SQL file
        source_info: Source database info string
        namespace_uuid: Fixed namespace UUID for deterministic IDs
        
    Returns:
        Dictionary with generation stats
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Clean up old agents migration files
    cleanup_old_migration_files(output_file, '06_agents_')
    
    if len(agents_df) == 0:
        return {
            'file': output_file,
            'agents_processed': 0,
            'settings_processed': 0,
            'documents_linked': 0
        }
    
    agents_processed = 0
    skipped_count = 0
    
    with open(output_file, 'w', encoding='utf-8') as sql_file:
        # Write header
        header = f"""-- ============================================================
-- AGENTS MIGRATION SQL (from playground_bot_generator_config)
-- ============================================================
-- Generated: {datetime.now().isoformat()}
-- Source: {source_info}
-- Destination: agents + agent_settings + agent_documents
-- Source rows: {len(agents_df)}
-- 
-- IMPORTANT: This script will INSERT data into 3 tables!
-- IMPORTANT: Run users, folders, and documents migrations first!
--
-- Creates:
--   1. agents (main agent record with deterministic UUID)
--   2. agent_settings (1:1 settings for each agent)
--   3. agent_documents (links to documents and folders)
--   4. legacy_bot_to_agent_mapping (tracking table)
--
-- Uses deterministic UUID generation (uuid_generate_v5).
-- Namespace UUID: {namespace_uuid}
-- ============================================================

-- Ensure PostgreSQL interprets this file as UTF-8 (required for Hebrew/multilingual content)
SET client_encoding = 'UTF8';

-- Ensure uuid-ossp extension is available
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

"""
        sql_file.write(header)
        
        # Include migration schema setup (idempotent)
        sql_file.write(generate_migration_schema_setup())
        sql_file.write('\n')
        
        # Create legacy mapping table
        sql_file.write("""-- ============================================================
-- CREATE MAPPING TABLE FOR TRACKING
-- ============================================================
CREATE TABLE IF NOT EXISTS legacy_bot_to_agent_mapping (
    old_bot_id VARCHAR(255) PRIMARY KEY,
    new_agent_id UUID NOT NULL,
    agent_type VARCHAR(50),
    bot_name VARCHAR(255),
    migrated_at TIMESTAMP DEFAULT now()
);

""")
        
        # Generate per-agent INSERT blocks
        for _, row in agents_df.iterrows():
            sql = generate_agent_insert(row, namespace_uuid, user_id_overrides)
            if sql:
                sql_file.write(sql)
                sql_file.write('\n')
                agents_processed += 1
            else:
                skipped_count += 1
        
        # Write summary footer
        sql_file.write(f"""\n-- ============================================================
-- MIGRATION SUMMARY
-- ============================================================
-- Agents processed: {agents_processed}
-- Skipped (no bot_id): {skipped_count}
-- ============================================================
""")
    
    return {
        'file': output_file,
        'agents_processed': agents_processed,
        'settings_processed': agents_processed,
        'documents_linked': 0  # Tracked per-agent at runtime via RAISE NOTICE
    }
