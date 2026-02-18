"""
TEMPLATE: Generic migration SQL generator for any table.

This template shows the pattern for creating migration scripts for other tables.
Copy this file and customize for each table (folders, documents, embeddings, agents, etc.)

Key customization points:
1. TABLE_NAME - logical name of source table
2. TARGET_SCHEMA and TARGET_TABLE - destination in V5
3. ORGANIZATION_ID - if needed for the table
4. generate_insert() - mapping logic specific to the table
5. check_conditions - existence check logic (by email, id, etc.)
"""
import os
import csv
import json
from datetime import datetime
from pathlib import Path


# ============================================================
# CONFIGURATION - CUSTOMIZE THESE FOR EACH TABLE
# ============================================================
TABLE_NAME = "users"  # Source table name (for finding CSV file)
TARGET_SCHEMA = "user_db"
TARGET_TABLE = "public.users"
FULL_TARGET = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

# Optional: Organization ID if needed
ORGANIZATION_ID = "356b50f7-bcbd-42aa-9392-e1605f42f7a1"

# Existence check columns (customize per table)
# This checks if record already exists before inserting
EXISTENCE_CHECK_FIELDS = ["email", "metadata->'legacyData'->>'id'"]


# ============================================================
# HELPER FUNCTIONS (REUSABLE ACROSS ALL TABLES)
# ============================================================
def clean_string(val):
    """Clean and trim string values."""
    if not val:
        return None
    cleaned = val.strip()
    return cleaned if cleaned else None


def parse_json_field(val):
    """Parse JSON field from CSV."""
    if not val or val.strip() == '':
        return None
    try:
        val = val.replace("'", '"')
        return json.loads(val)
    except:
        return None


def escape_sql_string(val):
    """Escape single quotes for SQL string literals."""
    if val is None:
        return 'NULL'
    return f"'{str(val).replace(chr(39), chr(39)+chr(39))}'"


def escape_sql_identifier(val):
    """Escape double quotes for SQL identifiers."""
    if val is None:
        return 'NULL'
    return f'"{str(val).replace(chr(34), chr(34)+chr(34))}"'


# ============================================================
# TABLE-SPECIFIC LOGIC - CUSTOMIZE THIS SECTION
# ============================================================
def generate_insert(row):
    """
    Generate a single INSERT statement for a record.
    
    CUSTOMIZE THIS FUNCTION FOR EACH TABLE!
    
    Args:
        row: Dictionary with data from CSV
        
    Returns:
        SQL statement as string or None to skip
    """
    # Example for users table:
    old_id = clean_string(row.get('id'))
    email = clean_string(row.get('email'))
    first_name = clean_string(row.get('name'))
    
    # Skip if critical field is missing
    if not email:
        return None
    
    # Build metadata from legacy fields
    metadata = {
        'legacyData': {
            'id': old_id,
            # Add all other legacy fields here
        }
    }
    
    metadata_json = json.dumps(metadata).replace("'", "''")
    
    # Build existence check condition
    existence_check = f"email = '{email}' OR metadata->'legacyData'->>'id' = '{old_id}'"
    
    # Generate SQL
    sql = f"""
-- Record: {email or old_id}
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM {FULL_TARGET}
        WHERE {existence_check}
    ) THEN
        INSERT INTO {FULL_TARGET} (
            email,
            first_name,
            -- Add all columns here
            metadata
        ) VALUES (
            '{email}',
            {escape_sql_string(first_name)},
            -- Add all values here
            '{metadata_json}'::jsonb
        );
    END IF;
END $$;
"""
    
    return sql


# ============================================================
# MAIN FUNCTION (GENERIC - REUSABLE)
# ============================================================
def main():
    """Main function to generate migration SQL file."""
    script_dir = Path(__file__).parent
    extract_dir = script_dir / 'output' / 'extract'
    output_dir = script_dir / 'output' / 'migrations'
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find latest CSV for this table
    csv_files = sorted(extract_dir.glob(f'{TABLE_NAME}_*.csv'), reverse=True)
    if not csv_files:
        print(f"ERROR: No {TABLE_NAME} CSV file found in output/extract/")
        return
    
    # Filter out unwanted files (e.g., users_groups when looking for users)
    # Customize this filter per table if needed
    csv_files = [f for f in csv_files if 'groups' not in f.name]
    if not csv_files:
        print(f"ERROR: No valid {TABLE_NAME} CSV file found")
        return
    
    source_csv = csv_files[0]
    print(f"Processing: {source_csv.name}")
    
    # Output SQL file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = output_dir / f'migrate_{TABLE_NAME}_{timestamp}.sql'
    
    # Read CSV and generate SQL
    record_count = 0
    skipped_count = 0
    
    with open(source_csv, 'r', encoding='utf-8') as csv_file, \
         open(output_file, 'w', encoding='utf-8') as sql_file:
        
        # Write header with confirmation prompt
        header = f"""-- ============================================================
-- {TABLE_NAME.upper()} MIGRATION SQL
-- ============================================================
-- Generated: {datetime.now().isoformat()}
-- Source: {source_csv.name}
-- Destination: {FULL_TARGET}
-- 
-- IMPORTANT: This script will INSERT records into the target database!
-- IMPORTANT: Review organization_id and other constants before execution!
--
-- Each INSERT checks if record already exists before inserting.
-- ============================================================

-- CONFIRMATION PROMPT: User must confirm before execution
DO $$
DECLARE
    user_confirmation TEXT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '{TABLE_NAME.upper()} MIGRATION - CONFIRMATION REQUIRED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'This script will migrate records to: {FULL_TARGET}';
    RAISE NOTICE 'Generated: {datetime.now().isoformat()}';
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
        sql_file.write(header)
        
        reader = csv.DictReader(csv_file)
        
        for row in reader:
            sql = generate_insert(row)
            if sql:
                sql_file.write(sql)
                sql_file.write('\n')
                record_count += 1
            else:
                skipped_count += 1
        
        # Write footer
        sql_file.write(f'\n-- Total records processed: {record_count}\n')
        sql_file.write(f'-- Skipped: {skipped_count}\n')
    
    print(f"\n✓ Generated: {output_file}")
    print(f"  - Records processed: {record_count}")
    print(f"  - Skipped: {skipped_count}")
    print(f"\nTo execute:")
    print(f"  psql -h <host> -U <user> -d <database> -f {output_file}")


if __name__ == '__main__':
    main()
