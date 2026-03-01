# Migration SQL Execution Guide

## ❌ CRITICAL ISSUE IDENTIFIED

### Problem 1: Migration Schema NOT in Generated SQL

**Current Situation:**
- The code has functions to create migration schema (`generate_migration_schema_setup()`)
- BUT: Generated SQL files use the **OLD approach** without mapping tables
- The files in `output/migrations/` do NOT contain:
  - `CREATE SCHEMA IF NOT EXISTS migration;`
  - `CREATE TABLE migration.id_mappings`
  - `migration.is_migrated()` checks
  - `migration.get_new_id()` lookups

**Evidence:**
```sql
-- What's ACTUALLY in the generated files:
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM user_db.public.users 
        WHERE email = 'adi@jeen.ai' OR metadata->'legacyData'->>'id' = 'de0ff05457533c93fdf3e0d1cdd0f808'
    ) THEN
        INSERT INTO user_db.public.users (...)
        VALUES (gen_random_uuid(), ...);  -- No mapping table insert!
    END IF;
END $$;
```

**What SHOULD be in the files:**
```sql
-- 1. Migration schema setup (at top of FIRST file)
CREATE SCHEMA IF NOT EXISTS migration;
CREATE TABLE IF NOT EXISTS migration.id_mappings (...);
CREATE TABLE IF NOT EXISTS migration.batch_log (...);
CREATE FUNCTION migration.is_migrated(...);
CREATE FUNCTION migration.get_new_id(...);

-- 2. Each user insert with mapping
DO $$
DECLARE
    v_old_id VARCHAR := 'de0ff05457533c93fdf3e0d1cdd0f808';
    v_new_id UUID;
BEGIN
    -- Check mapping table
    IF migration.is_migrated('users', v_old_id) THEN
        RETURN;
    END IF;
    
    v_new_id := gen_random_uuid();
    INSERT INTO user_db.public.users (...) VALUES (v_new_id, ...);
    
    -- Store mapping
    INSERT INTO migration.id_mappings (table_name, old_id, new_id, ...)
    VALUES ('users', v_old_id, v_new_id, ...);
END $$;
```

---

## 🔧 SOLUTION

### Step 1: Generate Migration Schema Setup SQL

**Create standalone file:**
```sql
-- File: 00_migration_schema_setup.sql
-- Run this FIRST before any migrations
-- ============================================================
-- MIGRATION MAPPING TABLE SETUP (idempotent)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS migration;

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

CREATE INDEX IF NOT EXISTS idx_mappings_table_old_id 
    ON migration.id_mappings(table_name, old_id);
CREATE INDEX IF NOT EXISTS idx_mappings_table_new_id 
    ON migration.id_mappings(table_name, new_id);
CREATE INDEX IF NOT EXISTS idx_mappings_batch 
    ON migration.id_mappings(migration_batch);

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
```

### Step 2: Fix Code to Generate Proper SQL

The code at line 192-193 in `sql_generator.py` should include the setup:
```python
if include_mapping_setup:
    header += generate_migration_schema_setup()
```

**BUT** this only runs for the header, and the actual user insert at line 232 uses the OLD non-mapping approach.

**You need to verify which version of `generate_user_insert()` is active:**
- Line 232-416 has the NEW version (with mapping table)
- But generated files show OLD version (no mapping table)

**Something is wrong with the code flow or there's an old cached version running!**

---

## 📋 EXECUTION ORDER

### Correct Execution Order on Destination DB:

```bash
# 1. FIRST: Create migration infrastructure (ONCE)
psql -h target-db -d user_db -f 00_migration_schema_setup.sql

# 2. Users (must be first - no dependencies)
psql -h target-db -d user_db -f migrate_users_20260228_HHMMSS.sql

# 3. Folders (depends on users via owner_id)
psql -h target-db -d user_db -f migrate_folders_20260228_HHMMSS.sql

# 4. Documents (depends on users via owner_id, folders via folder_id)
psql -h target-db -d user_db -f migrate_documents_20260228_HHMMSS.sql

# 5. Chunks & Embeddings (depends on documents via doc_id)
psql -h target-db -d user_db -f migrate_chunks_embeddings_20260228_HHMMSS.sql

# 6. Conversations (depends on users via user_id)
psql -h target-db -d user_db -f migrate_conversations_20260228_HHMMSS.sql
```

### Dependency Graph:
```
migration.id_mappings (infrastructure)
    ↓
users
    ├─→ folders → documents → chunks/embeddings
    └─→ conversations
```

### Re-run Safety:
All scripts are **idempotent** thanks to:
- `migration.is_migrated()` checks
- `ON CONFLICT DO NOTHING` clauses
- `IF NOT EXISTS` checks

You can safely re-run scripts multiple times - already migrated records will be skipped.

---

## 🔍 VERIFICATION QUERIES

After migration, verify with:

```sql
-- Check migration progress
SELECT * FROM migration.progress_summary;

-- Check specific table mappings
SELECT table_name, COUNT(*) as total_migrated
FROM migration.id_mappings
GROUP BY table_name
ORDER BY table_name;

-- Find a specific user's new ID
SELECT 
    old_id,
    new_id,
    migrated_at
FROM migration.id_mappings
WHERE table_name = 'users' 
  AND old_id = 'de0ff05457533c93fdf3e0d1cdd0f808';

-- Check batch status
SELECT 
    batch_id,
    table_name,
    record_count,
    status,
    started_at,
    completed_at,
    EXTRACT(EPOCH FROM (completed_at - started_at)) as duration_seconds
FROM migration.batch_log
ORDER BY started_at DESC;

-- Verify user count match
SELECT 
    (SELECT COUNT(*) FROM user_db.public.users) as v5_count,
    (SELECT COUNT(*) FROM migration.id_mappings WHERE table_name = 'users') as migrated_count;
```

---

## ⚠️ IMMEDIATE ACTION REQUIRED

1. **Check which code version is actually running:**
   ```bash
   # Restart the Streamlit app to clear any caches
   # Then regenerate a fresh SQL file and verify it contains mapping table code
   ```

2. **Create the `00_migration_schema_setup.sql` file manually** (content above)

3. **Run it FIRST on the destination database before any migrations**

4. **Verify generated SQL files contain mapping table inserts**

5. **If not, there's a code issue that needs debugging**
