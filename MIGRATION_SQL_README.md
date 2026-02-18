# Migration SQL Generator - User Guide

This document explains how to generate and execute migration SQL scripts for moving data from Jeen V4 to V5.

## Overview

Each table migration follows this pattern:
1. **Extract** data from V4 database to CSV (using existing extraction tools)
2. **Generate** individual INSERT statements with existence checks
3. **Review** the generated SQL file
4. **Execute** the SQL file against the V5 database (with confirmation)

## Files

- `generate_user_migration.py` - User table migration generator (complete)
- `migration_generator_template.py` - Template for creating generators for other tables
- `output/migrations/*.sql` - Generated SQL migration files

## How It Works

### 1. Generate Migration SQL

Run the generator for a specific table:

```powershell
# Generate user migration SQL
python generate_user_migration.py
```

This will:
- Read the latest CSV from `output/extract/`
- Generate individual INSERT statements for each record
- Include existence checks (no duplicates)
- Save to `output/migrations/migrate_users_TIMESTAMP.sql`

### 2. Review Generated SQL

Each SQL file contains:

**Header Section:**
```sql
-- ============================================================
-- USER MIGRATION SQL
-- ============================================================
-- Generated: 2026-02-16T07:36:14
-- Source: users_20260215_210141.csv
-- Destination: user_db.public.users
-- IMPORTANT: This script will INSERT users into the target database!
-- ============================================================
```

**Confirmation Prompt:**
```sql
DO $$
BEGIN
    RAISE NOTICE 'USER MIGRATION - CONFIRMATION REQUIRED';
    RAISE NOTICE 'This script will migrate users to: user_db.public.users';
    -- ... displays migration info before proceeding
END $$;
```

**Individual INSERT Statements:**
```sql
-- User: user@example.com
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM user_db.public.users 
        WHERE email = 'user@example.com' OR metadata->'legacyData'->>'id' = 'abc123'
    ) THEN
        INSERT INTO user_db.public.users (
            email,
            first_name,
            -- ... all columns
        ) VALUES (
            'user@example.com',
            'John',
            -- ... all values
        );
    END IF;
END $$;
```

### 3. Execute Migration

**Option A: Direct execution (shows confirmation)**
```powershell
psql -h <host> -U <username> -d <database> -f output/migrations/migrate_users_TIMESTAMP.sql
```

When executed, you'll see:
```
NOTICE:  ============================================================
NOTICE:  USER MIGRATION - CONFIRMATION REQUIRED
NOTICE:  ============================================================
NOTICE:  This script will migrate users to: user_db.public.users
NOTICE:  Organization ID: 356b50f7-bcbd-42aa-9392-e1605f42f7a1
NOTICE:  ============================================================
NOTICE:  Ready to proceed. Press Ctrl+C to cancel or Enter to continue...
NOTICE:  Starting migration...
```

**Option B: With mandatory confirmation (recommended for first run)**

1. Open the SQL file
2. Uncomment these lines (remove `--` prefix):
   ```sql
   -- \\prompt 'Type YES to confirm and continue with migration: ' user_confirmation
   -- \\if :'user_confirmation' != 'YES'
   --   \\echo 'Migration cancelled by user.'
   --   \\quit
   -- \\endif
   ```

3. Execute as above - you'll be prompted to type "YES"

### 4. Verify Migration

After execution:

```sql
-- Check record count
SELECT COUNT(*) FROM user_db.public.users;

-- Check migrated users
SELECT email, first_name, last_name, created_at 
FROM user_db.public.users 
WHERE metadata ? 'legacyData';

-- Check specific legacy ID
SELECT * FROM user_db.public.users 
WHERE metadata->'legacyData'->>'id' = 'abc123';
```

## Safety Features

✅ **Existence Checks**: Each INSERT checks if record already exists (by email OR legacy ID)  
✅ **Idempotent**: Safe to run multiple times - skips existing records  
✅ **Confirmation Prompt**: Displays migration info before execution  
✅ **Legacy Data Preservation**: All V4 fields stored in `metadata.legacyData`  
✅ **Hard-coded Values**: No SQL injection risk - all values are embedded  

## Creating Generators for Other Tables

To create a migration generator for another table (folders, documents, etc.):

1. Copy the template:
   ```powershell
   Copy-Item migration_generator_template.py generate_TABLENAME_migration.py
   ```

2. Customize the configuration section:
   ```python
   TABLE_NAME = "folders"
   TARGET_SCHEMA = "document_db"
   TARGET_TABLE = "public.folders"
   ORGANIZATION_ID = "your-org-uuid"  # if needed
   ```

3. Customize the `generate_insert()` function:
   - Map CSV columns to target columns
   - Build metadata JSON with legacy fields
   - Define existence check logic
   - Return SQL INSERT statement

4. Run the generator:
   ```powershell
   python generate_TABLENAME_migration.py
   ```

## Example: Creating Folders Migration

```python
# generate_folders_migration.py
TABLE_NAME = "folders"
TARGET_SCHEMA = "document_db"
TARGET_TABLE = "public.folders"

def generate_insert(row):
    old_id = clean_string(row.get('id'))
    folder_name = clean_string(row.get('name'))
    
    if not old_id:
        return None
    
    metadata = {
        'legacyData': {
            'id': old_id,
            'original_name': folder_name,
            # ... other legacy fields
        }
    }
    
    metadata_json = json.dumps(metadata).replace("'", "''")
    
    sql = f"""
-- Folder: {folder_name or old_id}
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM {FULL_TARGET}
        WHERE metadata->'legacyData'->>'id' = '{old_id}'
    ) THEN
        INSERT INTO {FULL_TARGET} (
            name,
            metadata,
            id,
            created_at,
            updated_at
        ) VALUES (
            {escape_sql_string(folder_name)},
            '{metadata_json}'::jsonb,
            gen_random_uuid(),
            now(),
            now()
        );
    END IF;
END $$;
"""
    return sql
```

## Migration Order

Follow this order to respect foreign key dependencies:

1. ✅ **users_groups** (if needed)
2. ✅ **users** (DONE)
3. ⏳ **folders**
4. ⏳ **documents**
5. ⏳ **embeddings**
6. ⏳ **agents**

## Troubleshooting

### Error: "relation does not exist"
- Check that target schema/table exists in V5 database
- Verify connection to correct database

### Error: "duplicate key value"
- The existence check should prevent this
- Verify your existence check fields are correct
- Check for case sensitivity issues

### Error: "invalid input syntax for type uuid"
- Check organization_id is a valid UUID
- Verify UUID fields are properly formatted

### Warning: "syntax warning"
- Update to latest version of generator script
- Ensure using f-strings for SQL generation

## Best Practices

1. **Always review generated SQL** before execution
2. **Test on a staging database** first
3. **Backup target database** before migration
4. **Run generators in order** (respect foreign keys)
5. **Verify counts** after each table migration
6. **Keep generated SQL files** for audit trail

## Configuration

### Organization ID
Default: `356b50f7-bcbd-42aa-9392-e1605f42f7a1`

To change, update in the generator script:
```python
ORGANIZATION_ID = "your-org-uuid-here"
```

Or edit the generated SQL file before execution.

## Support

For issues or questions:
1. Check this README
2. Review the template file
3. Examine the users migration generator as reference
4. Test on staging database first
