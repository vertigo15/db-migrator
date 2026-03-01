# Foreign Key Constraint Strategy

## Overview
This document explains the FK (Foreign Key) constraint strategy for the V4→V5 migration across 3 separate PostgreSQL databases.

## Architecture Challenge

### 3 Separate Databases
```
jeen-dev-db-migration-test:5432
├── user_db      (users, users_groups)
├── document_db  (folders, documents, chunks)
└── completion_db (agents, agent_settings, agent_documents)
```

### PostgreSQL Limitation
**⚠️ PostgreSQL does NOT support foreign key constraints across databases.**

You cannot do this:
```sql
-- ❌ THIS WILL FAIL
-- In document_db:
ALTER TABLE public.folders 
ADD CONSTRAINT fk_folders_user 
FOREIGN KEY (user_id) 
REFERENCES user_db.public.users(id);  -- Cross-database reference
```

## FK Constraint Strategy

### 1. Within-Database FKs ✅

FKs **are enforced** when both tables are in the same database:

#### user_db
```sql
-- ✅ FK from users to users_groups (same database)
ALTER TABLE public.users
ADD CONSTRAINT fk_users_group 
FOREIGN KEY (group_id) 
REFERENCES public.users_groups(id);
```

#### document_db
```sql
-- ✅ FK from folders to folders (self-referencing)
ALTER TABLE public.folders
ADD CONSTRAINT fk_folders_parent 
FOREIGN KEY (parent_id) 
REFERENCES public.folders(id);

-- ✅ FK from documents to folders (same database)
ALTER TABLE public.documents
ADD CONSTRAINT fk_documents_folder 
FOREIGN KEY (folder_id) 
REFERENCES public.folders(id);

-- ✅ FK from chunks to documents (same database)
ALTER TABLE public.chunks
ADD CONSTRAINT fk_chunks_document 
FOREIGN KEY (document_id) 
REFERENCES public.documents(id);
```

#### completion_db
```sql
-- ✅ FK from agent_settings to agents (same database)
ALTER TABLE public.agent_settings
ADD CONSTRAINT fk_agent_settings_agent 
FOREIGN KEY (agent_id) 
REFERENCES public.agents(id);

-- ✅ FK from agent_documents to agents (same database)
ALTER TABLE public.agent_documents
ADD CONSTRAINT fk_agent_documents_agent 
FOREIGN KEY (agent_id) 
REFERENCES public.agents(id);
```

### 2. Cross-Database References ❌

FKs **cannot be enforced** when tables are in different databases:

```
user_db.users
    ↓ (NO FK)
document_db.folders.user_id

document_db.folders
    ↓ (NO FK)
completion_db.agents.folder_id
```

## Referential Integrity Strategy

Since cross-database FKs are not possible, we use these strategies:

### 1. Application-Level Validation ✅
The migration scripts validate references before insert:

```sql
-- Check if user exists before inserting folder
DO $$
DECLARE
    v_user_exists BOOLEAN;
BEGIN
    -- Query user_db to check if user exists
    -- (Requires dblink or application logic)
    v_user_exists := (SELECT EXISTS(
        SELECT 1 FROM user_db.public.users WHERE id = 'uuid-here'
    ));
    
    IF NOT v_user_exists THEN
        RAISE EXCEPTION 'User does not exist in user_db';
    END IF;
    
    -- Insert folder
    INSERT INTO public.folders (...) VALUES (...);
END;
$$;
```

### 2. Migration Mapping Table ✅
Use `migration.id_mappings` to track old_id → new_id relationships:

```sql
-- Get new user_id from old_id
SELECT migration.get_new_id('users', 'old_user_hash');

-- Use in folder insert
INSERT INTO public.folders (user_id, ...)
VALUES (
    migration.get_new_id('users', 'old_user_hash'),
    ...
);
```

### 3. Pre-Migration Validation ✅
Before migrating data, check that all referenced records exist:

```sql
-- Check if all folder user_ids exist in user_db
SELECT DISTINCT f.user_id
FROM document_db.public.folders f
WHERE NOT EXISTS (
    SELECT 1 FROM user_db.public.users u
    WHERE u.id = f.user_id
);
```

### 4. Load Order Enforcement ✅
Migrate tables in dependency order:

```
1. users_groups   → user_db      (no dependencies)
2. users          → user_db      (depends on users_groups)
3. folders        → document_db  (depends on users)
4. documents      → document_db  (depends on users, folders)
5. chunks         → document_db  (depends on documents)
6. agents         → completion_db (depends on users, folders)
```

## Schema Files

Created 3 schema files with FK constraints where possible:

### `schemas/target_user_db_schema.sql`
- ✅ FK: `users.group_id → users_groups.id`
- Creates: users_groups, users
- Indexes on: email, organization_id, group_id

### `schemas/target_document_db_schema.sql`
- ✅ FK: `folders.parent_id → folders.id` (self-referencing)
- ✅ FK: `documents.folder_id → folders.id`
- ✅ FK: `chunks.document_id → documents.id`
- ❌ NO FK: `folders.user_id → user_db.users.id` (cross-database)
- ❌ NO FK: `documents.user_id → user_db.users.id` (cross-database)
- Creates: folders, documents, chunks
- Includes pgvector extension note

### `schemas/target_completion_db_schema.sql`
- ✅ FK: `agent_settings.agent_id → agents.id`
- ✅ FK: `agent_documents.agent_id → agents.id`
- ❌ NO FK: `agents.user_id → user_db.users.id` (cross-database)
- ❌ NO FK: `agents.folder_id → document_db.folders.id` (cross-database)
- ❌ NO FK: `agent_documents.document_id → document_db.documents.id` (cross-database)
- Creates: agents, agent_settings, agent_documents

## FK Constraints Summary

| From Table | From Column | To Table | To Column | FK Status | Database |
|------------|-------------|----------|-----------|-----------|----------|
| users | group_id | users_groups | id | ✅ FK enforced | user_db |
| folders | parent_id | folders | id | ✅ FK enforced | document_db |
| folders | user_id | users | id | ❌ NO FK (cross-DB) | → user_db |
| documents | folder_id | folders | id | ✅ FK enforced | document_db |
| documents | user_id | users | id | ❌ NO FK (cross-DB) | → user_db |
| chunks | document_id | documents | id | ✅ FK enforced | document_db |
| agents | user_id | users | id | ❌ NO FK (cross-DB) | → user_db |
| agents | folder_id | folders | id | ❌ NO FK (cross-DB) | → document_db |
| agent_settings | agent_id | agents | id | ✅ FK enforced | completion_db |
| agent_settings | user_id | users | id | ❌ NO FK (cross-DB) | → user_db |
| agent_documents | agent_id | agents | id | ✅ FK enforced | completion_db |
| agent_documents | document_id | documents | id | ❌ NO FK (cross-DB) | → document_db |

**Summary**: 
- ✅ **7 FKs enforced** (within-database)
- ❌ **7 FKs not possible** (cross-database)

## Setup Instructions

### Step 1: Create Databases
```bash
psql -h jeen-dev-db-migration-test -U jeen_dev_db_admin -c "CREATE DATABASE user_db;"
psql -h jeen-dev-db-migration-test -U jeen_dev_db_admin -c "CREATE DATABASE document_db;"
psql -h jeen-dev-db-migration-test -U jeen_dev_db_admin -c "CREATE DATABASE completion_db;"
```

### Step 2: Install Extensions
```bash
# For document_db (if using pgvector for embeddings)
psql -h jeen-dev-db-migration-test -U jeen_dev_db_admin -d document_db -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

### Step 3: Create Schemas
```bash
# user_db schema
psql -h jeen-dev-db-migration-test -U jeen_dev_db_admin -d user_db \
  -f schemas/target_user_db_schema.sql

# document_db schema
psql -h jeen-dev-db-migration-test -U jeen_dev_db_admin -d document_db \
  -f schemas/target_document_db_schema.sql

# completion_db schema
psql -h jeen-dev-db-migration-test -U jeen_dev_db_admin -d completion_db \
  -f schemas/target_completion_db_schema.sql
```

### Step 4: Verify FK Constraints
```sql
-- Check FKs in user_db
SELECT 
    tc.constraint_name,
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY';
```

### Step 5: Run Migration
```bash
# In correct order
psql -h jeen-dev-db-migration-test -U jeen_dev_db_admin -d user_db \
  -f output/migrations/01_users_groups_*.sql

psql -h jeen-dev-db-migration-test -U jeen_dev_db_admin -d user_db \
  -f output/migrations/02_users_*.sql

psql -h jeen-dev-db-migration-test -U jeen_dev_db_admin -d document_db \
  -f output/migrations/03_folders_*.sql

# etc...
```

## Validation After Migration

### Check Within-Database FKs
```sql
-- user_db: Verify users → users_groups FK
SELECT u.id, u.email, u.group_id, ug.group_name
FROM public.users u
LEFT JOIN public.users_groups ug ON u.group_id = ug.id
WHERE u.group_id IS NOT NULL AND ug.id IS NULL;
-- Should return 0 rows (all group_ids valid)

-- document_db: Verify documents → folders FK
SELECT d.id, d.blob_name, d.folder_id
FROM public.documents d
LEFT JOIN public.folders f ON d.folder_id = f.id
WHERE d.folder_id IS NOT NULL AND f.id IS NULL;
-- Should return 0 rows (all folder_ids valid)

-- document_db: Verify chunks → documents FK
SELECT c.id, c.document_id
FROM public.chunks c
LEFT JOIN public.documents d ON c.document_id = d.id
WHERE c.document_id IS NOT NULL AND d.id IS NULL;
-- Should return 0 rows (all document_ids valid)
```

### Check Cross-Database References (Manual)
```sql
-- Check folders.user_id references user_db.users.id
-- (Requires dblink or application-level check)
SELECT DISTINCT f.user_id
FROM document_db.public.folders f
WHERE NOT EXISTS (
    SELECT 1 FROM user_db.public.users u
    WHERE u.id = f.user_id
);
-- Should return 0 rows (all user_ids exist in user_db)
```

## Benefits of This Approach

### Advantages ✅
1. **Within-database integrity**: FKs enforced where possible
2. **Cascade deletes**: Works for within-database relationships
3. **Query performance**: FKs create automatic indexes
4. **Data validation**: Prevents orphaned records within databases
5. **Clear documentation**: Schema files show intent

### Trade-offs ⚠️
1. **No cross-database enforcement**: Must validate at application level
2. **Manual validation needed**: For cross-database references
3. **Load order critical**: Must load in correct order
4. **Potential inconsistencies**: If application validation fails

## Alternative: Single Database

If cross-database FKs are critical, consider using **schemas in one database**:

```
single_database
├── user_db schema      (users, users_groups)
├── document_db schema  (folders, documents, chunks)
└── completion_db schema (agents, agent_settings, agent_documents)
```

Then change `.env`:
```bash
TARGET_SCHEMA_MODE=schemas  # Instead of "databases"
```

With this approach, ALL FKs can be enforced:
```sql
-- ✅ NOW POSSIBLE
ALTER TABLE document_db.folders
ADD CONSTRAINT fk_folders_user
FOREIGN KEY (user_id)
REFERENCES user_db.users(id);
```

---

**Status**: ✅ Schema files created with maximum FK enforcement  
**Files**: `schemas/target_*_schema.sql` (3 files)  
**FKs Enforced**: 7 within-database, 7 cross-database (application-level)  
**Last Updated**: 2026-02-28
