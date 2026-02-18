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


def escape_json_for_sql(json_data):
    """Escape JSON data for inclusion in SQL."""
    if json_data is None:
        return 'NULL'
    json_str = json.dumps(json_data, ensure_ascii=False)
    # Escape single quotes in the JSON string
    return f"'{json_str.replace(chr(39), chr(39)+chr(39))}'::jsonb"


def generate_username(email):
    """Generate username from email (part before @)."""
    if not email:
        return None
    username = email.split('@')[0].lower().replace('.', '')
    return username


def generate_sql_header(
    table_name: str,
    target_schema: str,
    target_table: str,
    source_info: str,
    record_count: int,
    org_id: Optional[str] = None
) -> str:
    """
    Generate SQL file header with confirmation prompt.
    
    Args:
        table_name: Logical table name (e.g., 'users')
        target_schema: Target schema (e.g., 'user_db')
        target_table: Target table (e.g., 'public.users')
        source_info: Source database info
        record_count: Number of records to migrate
        org_id: Optional organization ID
        
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
-- ============================================================

-- CONFIRMATION PROMPT: User must confirm before execution
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


def generate_user_insert(row: pd.Series, org_id: str = '356b50f7-bcbd-42aa-9392-e1605f42f7a1') -> Optional[str]:
    """
    Generate INSERT statement for a single user.
    
    Args:
        row: Pandas Series with user data
        org_id: Organization UUID
        
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
            {metadata_sql},
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


def generate_users_migration_sql(
    users_df: pd.DataFrame,
    output_file: str,
    source_info: str,
    org_id: str = '356b50f7-bcbd-42aa-9392-e1605f42f7a1'
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
        
        # Write individual INSERT statements
        for _, row in users_df.iterrows():
            sql = generate_user_insert(row, org_id)
            if sql:
                sql_file.write(sql)
                sql_file.write('\n')
                record_count += 1
            else:
                skipped_count += 1
        
        # Write footer
        sql_file.write(f'\n-- Total records processed: {record_count}\n')
        sql_file.write(f'-- Skipped (no email): {skipped_count}\n')
    
    return {
        'file': output_file,
        'processed': record_count,
        'skipped': skipped_count
    }


def generate_folder_insert(row: pd.Series, namespace_uuid: str = '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b') -> Optional[str]:
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
    folder_type = clean_string(row.get('folder_type')) or 'default'
    
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
    
    # Generate SQL with existence check
    sql = f"""
-- Folder: {folder_name or old_id} (owner: {owner_id})
DO $$
DECLARE
    v_folder_id uuid := uuid_generate_v5('{namespace_uuid}'::uuid, '{old_id}');
    v_user_id uuid;
BEGIN
    -- Lookup user_id from migrated users via legacy owner_id
    SELECT id INTO v_user_id
    FROM user_db.public.users
    WHERE metadata->'legacyData'->>'id' = '{owner_id}';
    
    -- Skip if user not found
    IF v_user_id IS NULL THEN
        RAISE NOTICE 'Skipping folder % - user % not found', '{old_id}', '{owner_id}';
        RETURN;
    END IF;
    
    -- Insert folder if not exists
    IF NOT EXISTS (
        SELECT 1 FROM user_db.public.folders
        WHERE id = v_folder_id
    ) THEN
        INSERT INTO user_db.public.folders (
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
    END IF;
END $$;
"""
    
    return sql


def generate_folders_migration_sql(
    folders_df: pd.DataFrame,
    output_file: str,
    source_info: str,
    namespace_uuid: str = '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'
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
    
    # Sort folders: parents (NULL parent_id) first, then by parent_id, then by id
    folders_sorted = folders_df.copy()
    folders_sorted['parent_id_sort'] = folders_sorted['parent_id'].fillna('')
    folders_sorted = folders_sorted.sort_values(['parent_id_sort', 'id'])
    
    record_count = 0
    skipped_count = 0
    
    with open(output_file, 'w', encoding='utf-8') as sql_file:
        # Write header
        header = f"""-- ============================================================
-- FOLDERS MIGRATION SQL
-- ============================================================
-- Generated: {datetime.now().isoformat()}
-- Source: {source_info}
-- Destination: user_db.public.folders
-- Records to migrate: {len(folders_df)}
-- 
-- IMPORTANT: This script will INSERT folders into the target database!
-- IMPORTANT: Folders are inserted in parent-first order to maintain relationships.
--
-- Uses deterministic UUID generation (uuid_generate_v5) to preserve parent-child links.
-- Namespace UUID: {namespace_uuid}
-- Each INSERT checks if folder already exists before inserting.
-- ============================================================

-- Ensure uuid-ossp extension is available
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- CONFIRMATION PROMPT: User must confirm before execution
DO $$
DECLARE
    user_confirmation TEXT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'FOLDERS MIGRATION - CONFIRMATION REQUIRED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'This script will migrate {len(folders_df)} folders to: user_db.public.folders';
    RAISE NOTICE 'Namespace UUID: {namespace_uuid}';
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
        
        # Write individual INSERT statements (sorted order)
        for _, row in folders_sorted.iterrows():
            sql = generate_folder_insert(row, namespace_uuid)
            if sql:
                sql_file.write(sql)
                sql_file.write('\n')
                record_count += 1
            else:
                skipped_count += 1
        
        # Write footer
        sql_file.write(f'\n-- Total folders processed: {record_count}\n')
        sql_file.write(f'-- Skipped (no ID): {skipped_count}\n')
    
    return {
        'file': output_file,
        'processed': record_count,
        'skipped': skipped_count
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
    namespace_uuid: str = '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'
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
    folder_id = clean_string(row.get('folder_id'))
    doc_type = clean_string(row.get('doc_type'))
    
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
    
    # Generate folder_id SQL (deterministic UUID or NULL)
    if folder_id:
        folder_id_sql = f"uuid_generate_v5('{namespace_uuid}'::uuid, '{folder_id}')"
    else:
        folder_id_sql = 'NULL'
    
    # Get content type
    content_type = get_content_type(doc_type)
    
    # Parse JSON fields for metadata
    tags = row.get('tags')
    if isinstance(tags, str):
        try:
            tags = json.loads(tags.replace("'", '"'))
        except:
            tags = []
    elif pd.isna(tags):
        tags = []
    
    vector_methods = row.get('vector_methods')
    if isinstance(vector_methods, str):
        try:
            vector_methods = json.loads(vector_methods.replace("'", '"'))
        except:
            vector_methods = None
    
    data_integration_doc_metadata = row.get('data_integration_doc_metadata')
    if isinstance(data_integration_doc_metadata, str):
        try:
            data_integration_doc_metadata = json.loads(data_integration_doc_metadata.replace("'", '"'))
        except:
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
    
    # Generate SQL with existence check
    sql = f"""
-- Document: {file_name} (owner: {owner_id})
DO $$
DECLARE
    v_user_id varchar(255);
BEGIN
    -- Lookup user_id from migrated users via legacy owner_id
    SELECT id::varchar(255) INTO v_user_id
    FROM user_db.public.users
    WHERE metadata->'legacyData'->>'id' = '{owner_id}';
    
    -- Skip if user not found
    IF v_user_id IS NULL THEN
        RAISE NOTICE 'Skipping document % - user % not found', '{doc_id}', '{owner_id}';
        RETURN;
    END IF;
    
    -- Insert document if not exists
    IF NOT EXISTS (
        SELECT 1 FROM user_db.public.documents
        WHERE metadata->'legacyData'->>'doc_id' = '{doc_id}'
    ) THEN
        INSERT INTO user_db.public.documents (
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
            gen_random_uuid(),
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
            {folder_id_sql},
            v_user_id,
            '{content_type}',
            NULL,
            'upload'::public.documents_source_type_enum,
            NULL
        );
    END IF;
END $$;
"""
    
    return sql


def generate_documents_migration_sql(
    documents_df: pd.DataFrame,
    output_file: str,
    source_info: str,
    namespace_uuid: str = '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'
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
    
    record_count = 0
    skipped_count = 0
    
    with open(output_file, 'w', encoding='utf-8') as sql_file:
        # Write header
        header = f"""-- ============================================================
-- DOCUMENTS MIGRATION SQL
-- ============================================================
-- Generated: {datetime.now().isoformat()}
-- Source: {source_info}
-- Destination: user_db.public.documents
-- Records to migrate: {len(documents_df)}
-- 
-- IMPORTANT: This script will INSERT documents into the target database!
-- IMPORTANT: Run users and folders migrations first (documents reference both).
--
-- Uses deterministic UUID generation (uuid_generate_v5) for folder_id conversion.
-- Namespace UUID: {namespace_uuid}
-- Each INSERT checks if document already exists before inserting.
-- ============================================================

-- Ensure uuid-ossp extension is available
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- CONFIRMATION PROMPT: User must confirm before execution
DO $$
DECLARE
    user_confirmation TEXT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'DOCUMENTS MIGRATION - CONFIRMATION REQUIRED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'This script will migrate {len(documents_df)} documents to: user_db.public.documents';
    RAISE NOTICE 'Namespace UUID: {namespace_uuid}';
    RAISE NOTICE 'Generated: {datetime.now().isoformat()}';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'PREREQUISITE: Users and folders must be migrated first!';
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
        
        # Write individual INSERT statements
        for _, row in documents_df.iterrows():
            sql = generate_document_insert(row, namespace_uuid)
            if sql:
                sql_file.write(sql)
                sql_file.write('\n')
                record_count += 1
            else:
                skipped_count += 1
        
        # Write footer
        sql_file.write(f'\n-- Total documents processed: {record_count}\n')
        sql_file.write(f'-- Skipped (no doc_id): {skipped_count}\n')
    
    return {
        'file': output_file,
        'processed': record_count,
        'skipped': skipped_count
    }


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
    default_embedding_model: str = 'BAAI/bge-m3',
    skip_empty_embeddings: bool = False
) -> Optional[str]:
    """
    Generate INSERT statements for BOTH chunk and embedding from a single source row.
    
    Args:
        row: Pandas Series with jeen_dev data
        chunk_index: The chunk index within its document
        namespace_uuid: Fixed namespace UUID for chunk_id generation
        default_embedding_model: Default model name if not specified
        skip_empty_embeddings: If True, skip rows without embeddings
        
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
    
    # Generate chunk INSERT
    chunk_sql = f"""
-- Chunk from legacy ID: {legacy_id} (doc_id: {doc_id})
DO $$
DECLARE
    v_chunk_id uuid := uuid_generate_v5('{namespace_uuid}'::uuid, '{legacy_id}');
    v_document_id uuid;
BEGIN
    -- Lookup document_id from migrated documents via legacy doc_id
    SELECT id INTO v_document_id
    FROM documents
    WHERE metadata->'legacyData'->>'doc_id' = '{doc_id}';
    
    -- Skip if document not found
    IF v_document_id IS NULL THEN
        RAISE NOTICE 'Skipping chunk % - document % not found', '{legacy_id}', '{doc_id}';
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
            {escape_sql_string(original_content)},
            {content_hash},
            'text'::chunks_content_type_enum,
            NULL,
            {char_count},
            {word_count},
            {chunk_metadata_sql},
            {created_at_sql},
            {escape_sql_string(translated_content) if translated_content else 'NULL'}
        );
    END IF;
END $$;
"""
    
    # Generate embedding INSERT (only if embeddings exist)
    embedding_sql = ''
    if embeddings is not None and not pd.isna(embeddings):
        # Convert embeddings to proper format if needed
        if isinstance(embeddings, str):
            embeddings_value = embeddings
        else:
            embeddings_value = str(embeddings)
        
        embedding_sql = f"""
-- Embedding for chunk {legacy_id}
DO $$
DECLARE
    v_chunk_id uuid := uuid_generate_v5('{namespace_uuid}'::uuid, '{legacy_id}');
    v_document_id uuid;
BEGIN
    -- Get document_id (same as chunk)
    SELECT id INTO v_document_id
    FROM documents
    WHERE metadata->'legacyData'->>'doc_id' = '{doc_id}';
    
    IF v_document_id IS NULL THEN
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
END $$;
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
    default_embedding_model: str = 'BAAI/bge-m3',
    skip_empty_embeddings: bool = False
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
        
    Returns:
        Dictionary with generation stats
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
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
-- ============================================================

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

"""
        sql_file.write(header)
        
        # Write individual INSERT statements
        for _, row in jeen_dev_sorted.iterrows():
            sql = generate_chunk_and_embedding_inserts(
                row,
                chunk_index=int(row['chunk_index']),
                namespace_uuid=namespace_uuid,
                default_embedding_model=default_embedding_model,
                skip_empty_embeddings=skip_empty_embeddings
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
    max_records_per_insert: int = 50
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
                title = clean_string(latest_row.get('title', f'Conversation {chat_id[:8]}'))
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
                        f"true, NULL, '{created_at_str}'::timestamptz, '{updated_at_str}'::timestamptz, "
                        f"'{updated_at_str}'::timestamptz, "
                        f"(SELECT id FROM users WHERE metadata->'legacyData'->>'id' = '{user_id}'))"
                    )
                
                conv_values_joined = ',\n'.join(conv_values)
                sql_file.write(f"""-- Conversations INSERT
INSERT INTO conversations (id, title, message_count, total_tokens, is_active, deleted_at, created_at, updated_at, last_interacted_at, user_id)
SELECT * FROM (
  VALUES
{conv_values_joined}
) AS v(id, title, message_count, total_tokens, is_active, deleted_at, created_at, updated_at, last_interacted_at, user_id)
WHERE v.user_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM conversations WHERE id = v.id);

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
                        user_parent = f"'{prev_assistant_msg_id}'" if prev_assistant_msg_id else 'NULL'
                        user_created_at = f"'{created_at_str}'::timestamptz - interval '1 second'" if pd.notna(created_at) else 'now()'
                        
                        msg_values.append(
                            f"    ({user_msg_id}, '{conv['chat_id']}'::uuid, {user_parent}, 'user'::messages_role_enum, "
                            f"false, 1, 1, NULL, {user_created_at}, {user_created_at}, NULL, "
                            f"(SELECT id FROM users WHERE metadata->'legacyData'->>'id' = '{user_id}'), '{{}}'::jsonb)"
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
                            f"false, 1, 1, 'stop', '{created_at_str}'::timestamptz, '{created_at_str}'::timestamptz, NULL, "
                            f"(SELECT id FROM users WHERE metadata->'legacyData'->>'id' = '{user_id}'), {metadata_escaped})"
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
                            f"{user_content_escaped}, NULL, {user_created_at})"
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
                        exec_time_sql = str(calc_time) if calc_time is not None else 'NULL'
                        
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
WHERE v.user_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM messages WHERE id = v.id);

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


# TODO: Add agents migration if needed
