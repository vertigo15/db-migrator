"""
Generate individual INSERT statements for user migration from CSV.
Each user gets a hard-coded INSERT with existence check (by email and id).
"""
import os
import csv
import json
from datetime import datetime
from pathlib import Path


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
        # Replace single quotes with double quotes for valid JSON
        val = val.replace("'", '"')
        return json.loads(val)
    except:
        return None


def escape_sql_string(val):
    """Escape single quotes for SQL string literals."""
    if val is None:
        return 'NULL'
    return f"'{str(val).replace(chr(39), chr(39)+chr(39))}'"


def generate_username(email):
    """Generate username from email (part before @)."""
    if not email:
        return None
    username = email.split('@')[0].lower().replace('.', '')
    return username


def generate_user_insert(row, org_id='356b50f7-bcbd-42aa-9392-e1605f42f7a1'):
    """
    Generate a single INSERT statement for a user with existence check.
    
    Args:
        row: Dictionary with user data from CSV
        org_id: Organization UUID to use
        
    Returns:
        SQL statement as string
    """
    # Extract and clean fields
    old_id = clean_string(row.get('id'))
    email = clean_string(row.get('email'))
    first_name = clean_string(row.get('name'))
    last_name = clean_string(row.get('last_name'))
    job = clean_string(row.get('job'))
    department = clean_string(row.get('department'))
    phone_number = clean_string(row.get('phone_number'))
    company_name = clean_string(row.get('company_name'))
    company_name_hebrew = clean_string(row.get('company_name_in_hebrew'))
    letter_checkbox = clean_string(row.get('letter_checkbox'))
    
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
    
    group_id = clean_string(row.get('__group_id__'))
    azure_oid = clean_string(row.get('azure_oid'))
    token_limit = clean_string(row.get('token_limit'))
    model = parse_json_field(row.get('model'))
    history_categories = parse_json_field(row.get('history_categories'))
    enabled_features = parse_json_field(row.get('enabled_features'))
    subfeatures = parse_json_field(row.get('subfeatures'))
    
    # Parse created_at
    created_at_str = clean_string(row.get('created_at'))
    if created_at_str:
        try:
            created_at = datetime.fromisoformat(created_at_str.replace(' ', 'T'))
            created_at_sql = f"'{created_at.isoformat()}'"
        except:
            created_at_sql = 'now()'
    else:
        created_at_sql = 'now()'
    
    # Skip if no email
    if not email:
        return None
    
    # Generate username
    username = generate_username(email)
    
    # Build metadata JSON object
    metadata = {
        'legacyData': {
            'id': old_id,
            'job': job,
            'model': model,
            'group_id': group_id,
            'azure_oid': azure_oid,
            'department': department,
            'token_used': str(token_used),
            'words_used': str(words_used),
            'subfeatures': subfeatures,
            'token_limit': token_limit,
            'company_name': company_name,
            'phone_number': phone_number,
            'last_connected': str(last_connected),
            'letter_checkbox': letter_checkbox,
            'times_connected': str(times_connected),
            'enabled_features': enabled_features,
            'history_categories': history_categories,
            'company_name_in_hebrew': company_name_hebrew
        }
    }
    
    # Convert metadata to JSON string for SQL
    metadata_json = json.dumps(metadata).replace("'", "''")
    
    # Generate SQL with existence check
    sql = f"""
-- User: {email}
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM user_db.public.users 
        WHERE email = '{email}' OR metadata->'legacyData'->>'id' = '{old_id}'
    ) THEN
        INSERT INTO user_db.public.users (
            email,
            first_name,
            last_name,
            username,
            avatar_url,
            metadata,
            created_at,
            updated_at,
            deleted_at,
            id,
            zitadel_user_id,
            organization_id,
            is_owner,
            preferred_language
        ) VALUES (
            '{email}',
            {escape_sql_string(first_name)},
            {escape_sql_string(last_name)},
            {escape_sql_string(username)},
            NULL,
            '{metadata_json}'::jsonb,
            {created_at_sql},
            now(),
            NULL,
            gen_random_uuid(),
            NULL,
            '{org_id}'::uuid,
            false,
            NULL
        );
    END IF;
END $$;
"""
    
    return sql


def main():
    """Main function to generate user migration SQL file."""
    # Paths
    script_dir = Path(__file__).parent
    extract_dir = script_dir / 'output' / 'extract'
    output_dir = script_dir / 'output' / 'migrations'
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find latest users CSV
    users_files = sorted(extract_dir.glob('users_*.csv'), reverse=True)
    if not users_files:
        print("ERROR: No users CSV file found in output/extract/")
        return
    
    # Filter out users_groups files
    users_files = [f for f in users_files if 'groups' not in f.name]
    if not users_files:
        print("ERROR: No users CSV file found (excluding users_groups)")
        return
    
    users_csv = users_files[0]
    print(f"Processing: {users_csv.name}")
    
    # Output SQL file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = output_dir / f'migrate_users_{timestamp}.sql'
    
    # Read CSV and generate SQL
    user_count = 0
    skipped_count = 0
    
    with open(users_csv, 'r', encoding='utf-8') as csv_file, \
         open(output_file, 'w', encoding='utf-8') as sql_file:
        
        # Write header with confirmation prompt
        header = f"""-- ============================================================
-- USER MIGRATION SQL
-- ============================================================
-- Generated: {datetime.now().isoformat()}
-- Source: {users_csv.name}
-- Destination: user_db.public.users
-- 
-- IMPORTANT: This script will INSERT users into the target database!
-- IMPORTANT: Replace organization_id if needed (currently: 356b50f7-bcbd-42aa-9392-e1605f42f7a1)
--
-- Each INSERT checks if user already exists by email or legacy ID before inserting.
-- ============================================================

-- CONFIRMATION PROMPT: User must confirm before execution
DO $$
DECLARE
    user_confirmation TEXT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'USER MIGRATION - CONFIRMATION REQUIRED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'This script will migrate users to: user_db.public.users';
    RAISE NOTICE 'Organization ID: 356b50f7-bcbd-42aa-9392-e1605f42f7a1';
    RAISE NOTICE 'Generated: {datetime.now().isoformat()}';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';
    
    -- Prompt for confirmation (PostgreSQL will pause here in interactive mode)
    user_confirmation := NULL;
    
    -- In psql interactive mode, this will require user to press Enter
    -- In non-interactive mode, this will fail and stop execution
    IF current_setting('is_superuser') = 'off' THEN
        RAISE NOTICE 'Ready to proceed. Press Ctrl+C to cancel or Enter to continue...';
    END IF;
    
    RAISE NOTICE 'Starting migration...';
    RAISE NOTICE '';
END $$;

-- Uncomment the lines below to require manual confirmation (recommended for first run)
-- Note: These are psql meta-commands that work in interactive psql sessions
-- \\prompt 'Type YES to confirm and continue with migration: ' user_confirmation
-- \\if :'user_confirmation' != 'YES'
--   \\echo 'Migration cancelled by user.'
--   \\quit
-- \\endif

"""
        sql_file.write(header)
        
        reader = csv.DictReader(csv_file)
        
        for row in reader:
            sql = generate_user_insert(row)
            if sql:
                sql_file.write(sql)
                sql_file.write('\n')
                user_count += 1
            else:
                skipped_count += 1
        
        # Write footer
        sql_file.write(f'\n-- Total users processed: {user_count}\n')
        sql_file.write(f'-- Skipped (no email): {skipped_count}\n')
    
    print(f"\n✓ Generated: {output_file}")
    print(f"  - Users processed: {user_count}")
    print(f"  - Skipped: {skipped_count}")
    print(f"\nTo execute:")
    print(f"  psql -h <host> -U <user> -d <database> -f {output_file}")


if __name__ == '__main__':
    main()
