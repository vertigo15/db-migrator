# SQL Migration Generation - Integration Guide

## Overview

The extraction process now automatically generates SQL migration files alongside CSV exports. When you click **"Start Extraction"** in the Streamlit app, it will:

1. Extract data from the source database → CSV files
2. Generate SQL INSERT statements from the same data → SQL files
3. Store both in the output directory

## Architecture

### Flow Diagram

```
User clicks "Start Extraction"
    ↓
ExtractionEngine initialized (with generate_sql=True)
    ↓
For each table (users, folders, documents, etc.):
    ├─→ Query database
    ├─→ Save to CSV (output/extract/)
    └─→ Generate SQL (output/migrations/)
```

### File Structure

```
db-migrator/
├── output/
│   ├── extract/                    # CSV files
│   │   ├── users_20260216_073207.csv
│   │   ├── folders_20260216_073207.csv
│   │   └── ...
│   └── migrations/                 # SQL files (NEW)
│       ├── migrate_users_20260216_073207.sql
│       ├── migrate_folders_20260216_073207.sql  (TODO)
│       └── ...
└── utils/
    ├── extraction.py               # Modified - calls SQL generator
    └── sql_generator.py            # NEW - generates SQL files
```

## Key Components

### 1. SQL Generator Module (`utils/sql_generator.py`)

**Purpose**: Generate SQL INSERT statements from pandas DataFrames

**Key Functions**:
- `generate_users_migration_sql()` - Creates SQL file for users table
- `generate_user_insert()` - Generates single user INSERT statement
- `generate_sql_header()` - Creates header with confirmation prompt

**Future**: Add generators for other tables (folders, documents, embeddings, agents)

### 2. Updated Extraction Engine (`utils/extraction.py`)

**New Parameters**:
```python
ExtractionEngine(
    config=connection_config,
    prefix="jeen_dev",
    output_dir="output/extract",
    generate_sql=True,           # NEW - enable SQL generation
    organization_id="uuid-here"  # NEW - for SQL generation
)
```

**Modified Methods**:
- `__init__()` - Accepts SQL generation parameters
- `extract_users()` - Calls SQL generator after saving CSV
- `run_full_extraction()` - Tracks SQL files in results

### 3. Streamlit UI Updates (`pages/2_select_data.py`)

**New UI Elements**:
- ✅ Checkbox: "Generate SQL migration files" (default: ON)
- 📝 Text input: Organization ID for SQL
- 📥 Download section for SQL files

**User Experience**:
1. User selects data to migrate
2. Toggles SQL generation ON/OFF
3. Enters organization ID (if needed)
4. Clicks "Start Extraction"
5. Downloads both CSV **and** SQL files

## SQL File Format

Each generated SQL file contains:

### 1. Header with Confirmation
```sql
-- ============================================================
-- USERS MIGRATION SQL
-- ============================================================
-- Generated: 2026-02-16T07:32:07
-- Source: localhost:5432/jeen_v4 (prefix: jeen_dev)
-- Destination: user_db.public.users
-- Records to migrate: 150
--
-- IMPORTANT: This script will INSERT records into the target database!
-- ============================================================

DO $$
BEGIN
    RAISE NOTICE 'USER MIGRATION - CONFIRMATION REQUIRED';
    RAISE NOTICE 'This script will migrate 150 records to: user_db.public.users';
    RAISE NOTICE 'Organization ID: 356b50f7-bcbd-42aa-9392-e1605f42f7a1';
    -- User must see this before execution continues
END $$;
```

### 2. Individual INSERT Statements
```sql
-- User: john@example.com
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM user_db.public.users 
        WHERE email = 'john@example.com' OR metadata->'legacyData'->>'id' = 'abc123'
    ) THEN
        INSERT INTO user_db.public.users (
            email, first_name, last_name, username,
            metadata, created_at, organization_id, ...
        ) VALUES (
            'john@example.com', 'John', 'Doe', 'john',
            '{"legacyData": {...}}'::jsonb, now(), 'org-uuid', ...
        );
    END IF;
END $$;
```

### 3. Footer
```sql
-- Total records processed: 150
-- Skipped (no email): 0
```

## Execution

### From Streamlit App
1. Navigate to **"Select Data"** page
2. Select users/documents
3. Enable **"Generate SQL migration files"**
4. Click **"Start Extraction"**
5. Download `.sql` files
6. Execute manually with `psql`

### Direct Execution
```bash
# Execute the SQL file
psql -h your-host -U your-user -d your-database -f output/migrations/migrate_users_20260216_073207.sql

# User will see confirmation prompt:
# NOTICE:  ============================================================
# NOTICE:  USER MIGRATION - CONFIRMATION REQUIRED
# NOTICE:  ============================================================
# NOTICE:  This script will migrate 150 records to: user_db.public.users
# NOTICE:  Ready to proceed. Press Ctrl+C to cancel or Enter to continue...
```

## Safety Features

✅ **Existence Checks**: Each INSERT checks if record already exists (by email OR legacy ID)
✅ **Idempotent**: Safe to run multiple times - skips existing records
✅ **Confirmation Prompt**: Displays migration info before execution
✅ **Legacy Preservation**: All V4 fields stored in `metadata.legacyData`
✅ **Hard-coded Values**: No SQL injection risk
✅ **Source Tracking**: Header includes source database info

## Configuration

### Organization ID
Default: `356b50f7-bcbd-42aa-9392-e1605f42f7a1`

**To Change**:
1. In Streamlit UI: Edit the "Org ID" field before extraction
2. In generated SQL: Edit the UUID in the SQL file before execution

### Enable/Disable SQL Generation

**In Streamlit App**:
- Uncheck "Generate SQL migration files" checkbox

**In Code**:
```python
engine = ExtractionEngine(
    config=config,
    prefix=prefix,
    output_dir=output_dir,
    generate_sql=False  # Disable SQL generation
)
```

## Extending to Other Tables

To add SQL generation for folders, documents, etc.:

### Step 1: Add Generator Function
In `utils/sql_generator.py`:

```python
def generate_folder_insert(row: pd.Series, org_id: str) -> Optional[str]:
    """Generate INSERT statement for a single folder."""
    old_id = clean_string(row.get('id'))
    folder_name = clean_string(row.get('folder_name'))
    
    if not old_id:
        return None
    
    # Build metadata
    metadata = {
        'legacyData': {
            'id': old_id,
            'folder_type': row.get('folder_type'),
            # ... other legacy fields
        }
    }
    
    sql = f"""
-- Folder: {folder_name or old_id}
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM document_db.public.folders 
        WHERE metadata->'legacyData'->>'id' = '{old_id}'
    ) THEN
        INSERT INTO document_db.public.folders (
            name, metadata, id, created_at, updated_at
        ) VALUES (
            {escape_sql_string(folder_name)},
            {escape_json_for_sql(metadata)},
            gen_random_uuid(),
            now(),
            now()
        );
    END IF;
END $$;
"""
    return sql


def generate_folders_migration_sql(
    folders_df: pd.DataFrame,
    output_file: str,
    source_info: str
) -> Dict[str, Any]:
    """Generate SQL migration file for folders table."""
    # Similar to generate_users_migration_sql()
    # ...
```

### Step 2: Call from Extraction Engine
In `utils/extraction.py`, method `extract_folders()`:

```python
def extract_folders(self, user_ids: List[str]) -> Tuple[pd.DataFrame, str]:
    # ... existing extraction code ...
    
    output_path = os.path.join(self.output_dir, f"folders_{self.timestamp}.csv")
    df.to_csv(output_path, index=False)
    
    # Generate SQL migration file if enabled
    if self.generate_sql and len(df) > 0:
        sql_output_path = os.path.join(self.sql_output_dir, f"migrate_folders_{self.timestamp}.sql")
        source_info = f"{self.config.host}:{self.config.port}/{self.config.database} (prefix: {self.prefix})"
        try:
            generate_folders_migration_sql(
                folders_df=df,
                output_file=sql_output_path,
                source_info=source_info
            )
        except Exception as e:
            print(f"Warning: Failed to generate SQL for folders: {str(e)}")
    
    return df, output_path
```

### Step 3: Track in Results
In `run_full_extraction()`:

```python
# 3. Extract folders
folders_df, folders_path = self.extract_folders(user_ids)
results["files"]["folders"] = folders_path
results["summary"]["folders"] = len(folders_df)

# Track SQL generation
if self.generate_sql and len(folders_df) > 0:
    sql_path = os.path.join(self.sql_output_dir, f"migrate_folders_{self.timestamp}.sql")
    if os.path.exists(sql_path):
        results["sql_files"]["folders"] = sql_path
```

## Benefits

### For Users
- ✅ Single-click extraction generates both CSV and SQL
- ✅ SQL files ready for direct execution
- ✅ No need to manually create migration scripts
- ✅ Consistent format across all tables

### For Developers
- ✅ Separation of concerns (extraction vs. SQL generation)
- ✅ Reusable SQL generator functions
- ✅ Easy to extend to new tables
- ✅ Testable components

### For DBAs
- ✅ Review SQL before execution
- ✅ Modify organization IDs or values as needed
- ✅ Execute migrations at convenient time
- ✅ Track changes in version control

## Testing

### Manual Test

1. Start Streamlit app:
   ```bash
   streamlit run app.py
   ```

2. Navigate to **"Connect"** page → Connect to source DB

3. Navigate to **"Select Data"** page:
   - Select 1-2 users
   - Enable "Generate SQL migration files"
   - Click "Start Extraction"

4. Verify outputs:
   ```bash
   ls output/extract/users_*.csv
   ls output/migrations/migrate_users_*.sql
   ```

5. Review SQL file:
   ```bash
   cat output/migrations/migrate_users_*.sql
   ```

6. Test execution (dry run):
   ```bash
   psql -h localhost -U postgres -d test_db -f output/migrations/migrate_users_*.sql
   ```

## Troubleshooting

### SQL files not generated
- Check that "Generate SQL migration files" is enabled
- Verify `output/migrations/` directory exists
- Check console for error messages

### SQL execution fails
- Verify target database schema exists
- Check organization_id UUID format
- Ensure target table has required columns
- Review metadata JSONB structure

### CSV exists but SQL doesn't
- SQL generation errors are logged but don't fail extraction
- Check console output for warnings
- Verify pandas DataFrame has required columns

## Next Steps

1. ✅ **Users table** - COMPLETE
2. ⏳ **Folders table** - Add generator
3. ⏳ **Documents table** - Add generator
4. ⏳ **Embeddings table** - Add generator
5. ⏳ **Agents table** - Add generator
6. ⏳ **Users_groups table** - Add generator

## Summary

The integrated SQL generation provides a seamless workflow:
1. **Extract** from V4 database
2. **Generate** migration SQL automatically
3. **Review** SQL files
4. **Execute** against V5 database

All in one click! 🚀
