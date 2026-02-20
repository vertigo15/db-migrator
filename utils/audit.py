"""
Pre-Migration Audit Queries.
Run these BEFORE migration to capture baseline statistics and identify potential issues.
"""
from typing import Dict, Any, List
import pandas as pd
from utils.db import ConnectionConfig, execute_query


def get_table_name(logical_name: str, prefix: str) -> str:
    """Get actual table name with prefix."""
    if logical_name == "embeddings":
        return prefix  # The main embeddings table is just the prefix
    return f"{prefix}_{logical_name}"


# ============================================================================
# SECTION 2: USER ANALYTICS
# ============================================================================

def audit_top_users_by_logs(config: ConnectionConfig, prefix: str, limit: int = 10) -> pd.DataFrame:
    """Top users by log count (most chat activity)."""
    users_table = get_table_name("users", prefix)
    logs_table = get_table_name("logs", prefix)
    
    query = f"""
        SELECT
            u.id AS legacy_user_id,
            u.name AS user_name,
            TRIM(u.email) AS email,
            COUNT(l.id) AS total_log_entries,
            COUNT(DISTINCT l.chat_id) AS total_conversations
        FROM public.{logs_table} l
        JOIN public.{users_table} u ON u.id = l.user_id
        WHERE l.user_id IS NOT NULL
        GROUP BY u.id, u.name, u.email
        ORDER BY total_log_entries DESC
        LIMIT {limit}
    """
    return execute_query(config, query)


def audit_top_users_by_documents(config: ConnectionConfig, prefix: str, limit: int = 10) -> pd.DataFrame:
    """Top users by document count."""
    users_table = get_table_name("users", prefix)
    docs_table = get_table_name("custom_documents", prefix)
    
    query = f"""
        SELECT
            u.id AS legacy_user_id,
            u.name AS user_name,
            TRIM(u.email) AS email,
            COUNT(d.doc_id) AS total_documents,
            SUM(COALESCE(d.doc_size, 0)) AS total_doc_size_bytes
        FROM public.{docs_table} d
        JOIN public.{users_table} u ON u.id = d.owner_id
        GROUP BY u.id, u.name, u.email
        ORDER BY total_documents DESC
        LIMIT {limit}
    """
    return execute_query(config, query)


def audit_top_users_by_chunks(config: ConnectionConfig, prefix: str, limit: int = 10) -> pd.DataFrame:
    """Top users by chunk/embedding count."""
    users_table = get_table_name("users", prefix)
    embeddings_table = get_table_name("embeddings", prefix)
    
    query = f"""
        SELECT
            u.id AS legacy_user_id,
            u.name AS user_name,
            TRIM(u.email) AS email,
            COUNT(c.id) AS total_chunks
        FROM public.{embeddings_table} c
        JOIN public.{users_table} u ON u.id = c.metadata->>'user_id'
        WHERE c.metadata->>'type' = 'chunk-data'
        GROUP BY u.id, u.name, u.email
        ORDER BY total_chunks DESC
        LIMIT {limit}
    """
    return execute_query(config, query)


def audit_users_without_email(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Users with no email (will be SKIPPED by migration)."""
    users_table = get_table_name("users", prefix)
    
    query = f"""
        SELECT
            id AS legacy_user_id,
            name AS user_name,
            email
        FROM public.{users_table}
        WHERE TRIM(COALESCE(email, '')) = ''
    """
    return execute_query(config, query)


def audit_username_collisions(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Potential username collisions (same email prefix across domains)."""
    users_table = get_table_name("users", prefix)
    
    query = f"""
        SELECT
            SPLIT_PART(TRIM(email), '@', 1) AS username_prefix,
            COUNT(*) AS user_count,
            ARRAY_AGG(TRIM(email)) AS emails
        FROM public.{users_table}
        WHERE TRIM(COALESCE(email, '')) != ''
        GROUP BY SPLIT_PART(TRIM(email), '@', 1)
        HAVING COUNT(*) > 1
        ORDER BY user_count DESC
    """
    return execute_query(config, query)


# ============================================================================
# SECTION 3: FOLDER ANALYTICS
# ============================================================================

def audit_folder_hierarchy_depth(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Folder hierarchy depth analysis."""
    folders_table = get_table_name("folders", prefix)
    
    query = f"""
        WITH RECURSIVE folder_tree AS (
            SELECT id, folder_name, parent_id, 1 AS depth
            FROM public.{folders_table}
            WHERE parent_id IS NULL
            
            UNION ALL
            
            SELECT f.id, f.folder_name, f.parent_id, ft.depth + 1
            FROM public.{folders_table} f
            JOIN folder_tree ft ON f.parent_id = ft.id
        )
        SELECT depth, COUNT(*) AS folder_count
        FROM folder_tree
        GROUP BY depth
        ORDER BY depth
    """
    return execute_query(config, query)


def audit_folders_multilevel(config: ConnectionConfig, prefix: str, limit: int = 50) -> pd.DataFrame:
    """Folders with multi-level hierarchy (depth > 1) - will lose parent relationship."""
    folders_table = get_table_name("folders", prefix)
    
    query = f"""
        WITH RECURSIVE folder_tree AS (
            SELECT id, folder_name, parent_id, 1 AS depth
            FROM public.{folders_table} WHERE parent_id IS NULL
            UNION ALL
            SELECT f.id, f.folder_name, f.parent_id, ft.depth + 1
            FROM public.{folders_table} f JOIN folder_tree ft ON f.parent_id = ft.id
        )
        SELECT id, folder_name, parent_id, depth
        FROM folder_tree
        WHERE depth > 1
        ORDER BY depth DESC, folder_name
        LIMIT {limit}
    """
    return execute_query(config, query)


def audit_folder_type_distribution(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Folder type distribution."""
    folders_table = get_table_name("folders", prefix)
    
    query = f"""
        SELECT
            COALESCE(folder_type, '(null)') AS folder_type,
            COUNT(*) AS folder_count
        FROM public.{folders_table}
        GROUP BY folder_type
        ORDER BY folder_count DESC
    """
    return execute_query(config, query)


def audit_orphaned_folders(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Orphaned folders (parent_id references non-existent folder)."""
    folders_table = get_table_name("folders", prefix)
    
    query = f"""
        SELECT
            f.id,
            f.folder_name,
            f.parent_id AS missing_parent_id
        FROM public.{folders_table} f
        WHERE f.parent_id IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM public.{folders_table} p WHERE p.id = f.parent_id
          )
    """
    return execute_query(config, query)


# ============================================================================
# SECTION 4: DOCUMENT ANALYTICS
# ============================================================================

def audit_doc_type_distribution(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Document type distribution."""
    docs_table = get_table_name("custom_documents", prefix)
    
    query = f"""
        SELECT
            COALESCE(TRIM(doc_type), '(null)') AS doc_type,
            COUNT(*) AS doc_count
        FROM public.{docs_table}
        GROUP BY TRIM(doc_type)
        ORDER BY doc_count DESC
    """
    return execute_query(config, query)


def audit_problematic_doc_types(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Documents with problematic doc_type values (will map to application/octet-stream)."""
    docs_table = get_table_name("custom_documents", prefix)
    
    # Use subquery to get sample doc_ids (LIMIT not supported inside ARRAY_AGG)
    query = f"""
        SELECT
            TRIM(doc_type) AS doc_type,
            COUNT(*) AS doc_count,
            (SELECT ARRAY_AGG(sub.doc_id) FROM (
                SELECT doc_id FROM public.{docs_table} d2 
                WHERE TRIM(d2.doc_type) = TRIM(d.doc_type) 
                ORDER BY doc_id LIMIT 3
            ) sub) AS sample_doc_ids
        FROM public.{docs_table} d
        WHERE TRIM(LOWER(doc_type)) NOT IN (
            'pdf','docx','pptx','xlsx','doc','ppt','xls','txt','csv','html','json',
            'png','jpg','jpeg','gif','svg','webp','md','mp3','mp4',
            'application/pdf','image/png','image/jpeg'
        )
        GROUP BY TRIM(doc_type)
        ORDER BY doc_count DESC
    """
    return execute_query(config, query)


def audit_blob_source_distribution(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Blob source distribution."""
    docs_table = get_table_name("custom_documents", prefix)
    
    query = f"""
        SELECT
            COALESCE(blob_source, '(null)') AS blob_source,
            COUNT(*) AS doc_count
        FROM public.{docs_table}
        GROUP BY blob_source
        ORDER BY doc_count DESC
    """
    return execute_query(config, query)


def audit_orphaned_documents(config: ConnectionConfig, prefix: str) -> int:
    """Count of documents without an owner match in users table."""
    docs_table = get_table_name("custom_documents", prefix)
    users_table = get_table_name("users", prefix)
    
    query = f"""
        SELECT COUNT(*) AS orphaned_docs
        FROM public.{docs_table} d
        WHERE NOT EXISTS (
            SELECT 1 FROM public.{users_table} u WHERE u.id = d.owner_id
        )
    """
    df = execute_query(config, query)
    return int(df.iloc[0]['orphaned_docs']) if not df.empty else 0


def audit_docs_missing_folders(config: ConnectionConfig, prefix: str) -> int:
    """Count of documents referencing non-existent folders."""
    docs_table = get_table_name("custom_documents", prefix)
    folders_table = get_table_name("folders", prefix)
    
    query = f"""
        SELECT COUNT(*) AS docs_with_missing_folder
        FROM public.{docs_table} d
        WHERE d.folder_id IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM public.{folders_table} f WHERE f.id = d.folder_id
          )
    """
    df = execute_query(config, query)
    return int(df.iloc[0]['docs_with_missing_folder']) if not df.empty else 0


def audit_duplicate_doc_ids(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Duplicate doc_id values."""
    docs_table = get_table_name("custom_documents", prefix)
    
    query = f"""
        SELECT doc_id, COUNT(*) AS occurrences
        FROM public.{docs_table}
        GROUP BY doc_id
        HAVING COUNT(*) > 1
        ORDER BY occurrences DESC
    """
    return execute_query(config, query)


# ============================================================================
# SECTION 5: CHUNKS & EMBEDDINGS ANALYTICS
# ============================================================================

def audit_chunks_per_document(config: ConnectionConfig, prefix: str, limit: int = 20) -> pd.DataFrame:
    """Chunks per document distribution (top N)."""
    embeddings_table = get_table_name("embeddings", prefix)
    
    query = f"""
        SELECT
            metadata->>'doc_id' AS doc_id,
            COUNT(*) AS chunk_count
        FROM public.{embeddings_table}
        WHERE metadata->>'type' = 'chunk-data'
        GROUP BY metadata->>'doc_id'
        ORDER BY chunk_count DESC
        LIMIT {limit}
    """
    return execute_query(config, query)


def audit_orphaned_chunks(config: ConnectionConfig, prefix: str) -> Dict[str, int]:
    """Chunks referencing documents that don't exist."""
    embeddings_table = get_table_name("embeddings", prefix)
    docs_table = get_table_name("custom_documents", prefix)
    
    query = f"""
        SELECT
            COUNT(*) AS orphaned_chunks,
            COUNT(DISTINCT metadata->>'doc_id') AS orphaned_doc_ids
        FROM public.{embeddings_table} c
        WHERE c.metadata->>'type' = 'chunk-data'
          AND NOT EXISTS (
            SELECT 1 FROM public.{docs_table} d
            WHERE d.doc_id = c.metadata->>'doc_id'
          )
    """
    df = execute_query(config, query)
    if df.empty:
        return {'orphaned_chunks': 0, 'orphaned_doc_ids': 0}
    return {
        'orphaned_chunks': int(df.iloc[0]['orphaned_chunks']),
        'orphaned_doc_ids': int(df.iloc[0]['orphaned_doc_ids'])
    }


def audit_chunks_without_embeddings(config: ConnectionConfig, prefix: str) -> int:
    """Count of chunks with NULL embeddings."""
    embeddings_table = get_table_name("embeddings", prefix)
    
    query = f"""
        SELECT COUNT(*) AS chunks_without_embeddings
        FROM public.{embeddings_table}
        WHERE metadata->>'type' = 'chunk-data'
          AND embeddings IS NULL
    """
    df = execute_query(config, query)
    return int(df.iloc[0]['chunks_without_embeddings']) if not df.empty else 0


def audit_embedding_dimensions(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Embedding vector dimensions (sanity check)."""
    embeddings_table = get_table_name("embeddings", prefix)
    
    query = f"""
        SELECT
            array_length(embeddings::text::float[], 1) AS vector_dimension,
            COUNT(*) AS chunk_count
        FROM public.{embeddings_table}
        WHERE metadata->>'type' = 'chunk-data'
          AND embeddings IS NOT NULL
        GROUP BY array_length(embeddings::text::float[], 1)
        ORDER BY chunk_count DESC
        LIMIT 5
    """
    return execute_query(config, query)


def audit_chunk_type_distribution(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Chunk type distribution in embeddings table."""
    embeddings_table = get_table_name("embeddings", prefix)
    
    query = f"""
        SELECT
            COALESCE(metadata->>'type', '(null)') AS chunk_type,
            COUNT(*) AS row_count
        FROM public.{embeddings_table}
        GROUP BY metadata->>'type'
        ORDER BY row_count DESC
    """
    return execute_query(config, query)


# ============================================================================
# SECTION 6: CONVERSATION / LOGS ANALYTICS
# ============================================================================

def audit_top_users_by_conversations(config: ConnectionConfig, prefix: str, limit: int = 10) -> pd.DataFrame:
    """Top users by conversation count."""
    logs_table = get_table_name("logs", prefix)
    users_table = get_table_name("users", prefix)
    
    query = f"""
        SELECT
            l.user_id AS legacy_user_id,
            u.name AS user_name,
            COUNT(DISTINCT l.chat_id) AS conversation_count,
            COUNT(*) AS total_messages
        FROM public.{logs_table} l
        LEFT JOIN public.{users_table} u ON u.id = l.user_id
        WHERE l.user_id IS NOT NULL
        GROUP BY l.user_id, u.name
        ORDER BY total_messages DESC
        LIMIT {limit}
    """
    return execute_query(config, query)


def audit_conversation_size_distribution(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Conversation size distribution (messages per conversation)."""
    logs_table = get_table_name("logs", prefix)
    
    query = f"""
        SELECT
            CASE
                WHEN cnt = 1      THEN '1 turn'
                WHEN cnt <= 5     THEN '2-5 turns'
                WHEN cnt <= 20    THEN '6-20 turns'
                WHEN cnt <= 50    THEN '21-50 turns'
                WHEN cnt <= 100   THEN '51-100 turns'
                ELSE '100+ turns'
            END AS turn_range,
            COUNT(*) AS conversation_count
        FROM (
            SELECT chat_id, COUNT(*) AS cnt
            FROM public.{logs_table}
            WHERE user_id IS NOT NULL AND chat_id IS NOT NULL
            GROUP BY chat_id
        ) sub
        GROUP BY
            CASE
                WHEN cnt = 1      THEN '1 turn'
                WHEN cnt <= 5     THEN '2-5 turns'
                WHEN cnt <= 20    THEN '6-20 turns'
                WHEN cnt <= 50    THEN '21-50 turns'
                WHEN cnt <= 100   THEN '51-100 turns'
                ELSE '100+ turns'
            END
        ORDER BY MIN(cnt)
    """
    return execute_query(config, query)


def audit_logs_without_user(config: ConnectionConfig, prefix: str) -> Dict[str, int]:
    """Logs with NULL user_id (will be SKIPPED)."""
    logs_table = get_table_name("logs", prefix)
    
    query = f"""
        SELECT
            COUNT(*) AS logs_without_user,
            COUNT(DISTINCT chat_id) AS conversations_affected
        FROM public.{logs_table}
        WHERE user_id IS NULL
    """
    df = execute_query(config, query)
    if df.empty:
        return {'logs_without_user': 0, 'conversations_affected': 0}
    return {
        'logs_without_user': int(df.iloc[0]['logs_without_user']),
        'conversations_affected': int(df.iloc[0]['conversations_affected'])
    }


def audit_logs_without_chat_id(config: ConnectionConfig, prefix: str) -> int:
    """Logs with NULL or empty chat_id (will be SKIPPED)."""
    logs_table = get_table_name("logs", prefix)
    
    query = f"""
        SELECT COUNT(*) AS logs_without_chat_id
        FROM public.{logs_table}
        WHERE chat_id IS NULL OR TRIM(chat_id::text) = ''
    """
    df = execute_query(config, query)
    return int(df.iloc[0]['logs_without_chat_id']) if not df.empty else 0


def audit_invalid_chat_id_format(config: ConnectionConfig, prefix: str, limit: int = 20) -> pd.DataFrame:
    """chat_id format validation (should be valid UUID)."""
    logs_table = get_table_name("logs", prefix)
    
    query = f"""
        SELECT
            chat_id::text,
            'INVALID UUID FORMAT' AS issue
        FROM public.{logs_table}
        WHERE user_id IS NOT NULL
          AND chat_id IS NOT NULL
          AND chat_id::text !~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
        GROUP BY chat_id
        LIMIT {limit}
    """
    return execute_query(config, query)


def audit_question_json_extraction(config: ConnectionConfig, prefix: str, limit: int = 20) -> pd.DataFrame:
    """Question JSON extraction validation (catches rows where extraction fails)."""
    logs_table = get_table_name("logs", prefix)
    
    query = f"""
        SELECT
            id AS legacy_log_id,
            question::jsonb->1->>'value' AS extracted_user_question,
            LEFT(answer, 80) AS answer_preview,
            jsonb_array_length(question::jsonb) AS history_length
        FROM public.{logs_table}
        WHERE user_id IS NOT NULL
          AND chat_id IS NOT NULL
          AND (
            question::jsonb->1->>'value' IS NULL
            OR TRIM(question::jsonb->1->>'value') = ''
          )
        LIMIT {limit}
    """
    return execute_query(config, query)


def audit_orphaned_logs(config: ConnectionConfig, prefix: str) -> Dict[str, int]:
    """Logs referencing users that don't exist in users table."""
    logs_table = get_table_name("logs", prefix)
    users_table = get_table_name("users", prefix)
    
    query = f"""
        SELECT
            COUNT(*) AS orphaned_logs,
            COUNT(DISTINCT user_id) AS orphaned_user_ids
        FROM public.{logs_table} l
        WHERE l.user_id IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM public.{users_table} u WHERE u.id = l.user_id
          )
    """
    df = execute_query(config, query)
    if df.empty:
        return {'orphaned_logs': 0, 'orphaned_user_ids': 0}
    return {
        'orphaned_logs': int(df.iloc[0]['orphaned_logs']),
        'orphaned_user_ids': int(df.iloc[0]['orphaned_user_ids'])
    }


def audit_model_usage_distribution(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Model usage distribution."""
    logs_table = get_table_name("logs", prefix)
    
    query = f"""
        SELECT
            COALESCE(
                (toolkit_settings::jsonb->>'model'),
                '(unknown)'
            ) AS model_name,
            COUNT(*) AS usage_count
        FROM public.{logs_table}
        WHERE user_id IS NOT NULL
        GROUP BY (toolkit_settings::jsonb->>'model')
        ORDER BY usage_count DESC
    """
    return execute_query(config, query)


def audit_bot_usage_distribution(config: ConnectionConfig, prefix: str, limit: int = 20) -> pd.DataFrame:
    """Bot/agent usage distribution."""
    logs_table = get_table_name("logs", prefix)
    
    query = f"""
        SELECT
            COALESCE(bot_id::text, '(none)') AS bot_id,
            COALESCE(type, '(none)') AS conversation_type,
            COUNT(*) AS log_count,
            COUNT(DISTINCT chat_id) AS conversation_count
        FROM public.{logs_table}
        WHERE user_id IS NOT NULL
        GROUP BY bot_id, type
        ORDER BY log_count DESC
        LIMIT {limit}
    """
    return execute_query(config, query)


def audit_token_statistics(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Token usage statistics."""
    logs_table = get_table_name("logs", prefix)
    
    query = f"""
        SELECT
            COUNT(*) AS total_turns,
            SUM(token_amount)::bigint AS total_tokens,
            ROUND(AVG(token_amount), 0) AS avg_tokens_per_turn,
            MIN(token_amount) AS min_tokens,
            MAX(token_amount) AS max_tokens,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY token_amount) AS median_tokens,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY token_amount) AS p95_tokens
        FROM public.{logs_table}
        WHERE user_id IS NOT NULL
          AND token_amount IS NOT NULL
    """
    return execute_query(config, query)


# ============================================================================
# SECTION 7: CROSS-TABLE INTEGRITY CHECKS
# ============================================================================

def audit_missing_user_references(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Users referenced in other tables but missing from users table."""
    users_table = get_table_name("users", prefix)
    docs_table = get_table_name("custom_documents", prefix)
    folders_table = get_table_name("folders", prefix)
    logs_table = get_table_name("logs", prefix)
    embeddings_table = get_table_name("embeddings", prefix)
    
    query = f"""
        SELECT DISTINCT d.owner_id AS missing_user_id, 'documents' AS referenced_from
        FROM public.{docs_table} d
        WHERE NOT EXISTS (SELECT 1 FROM public.{users_table} u WHERE u.id = d.owner_id)
        
        UNION ALL
        
        SELECT DISTINCT f.owner_id, 'folders'
        FROM public.{folders_table} f
        WHERE NOT EXISTS (SELECT 1 FROM public.{users_table} u WHERE u.id = f.owner_id)
        
        UNION ALL
        
        SELECT DISTINCT l.user_id, 'logs'
        FROM public.{logs_table} l
        WHERE l.user_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM public.{users_table} u WHERE u.id = l.user_id)
        
        UNION ALL
        
        SELECT DISTINCT c.metadata->>'user_id', 'chunks'
        FROM public.{embeddings_table} c
        WHERE c.metadata->>'type' = 'chunk-data'
          AND NOT EXISTS (SELECT 1 FROM public.{users_table} u WHERE u.id = c.metadata->>'user_id')
        
        ORDER BY referenced_from, missing_user_id
    """
    return execute_query(config, query)


def audit_data_loss_risk_summary(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Summary: data loss risk per table - rows that will be SKIPPED."""
    users_table = get_table_name("users", prefix)
    docs_table = get_table_name("custom_documents", prefix)
    folders_table = get_table_name("folders", prefix)
    logs_table = get_table_name("logs", prefix)
    embeddings_table = get_table_name("embeddings", prefix)
    
    query = f"""
        SELECT
            'documents without valid user' AS risk,
            COUNT(*) AS rows_at_risk
        FROM public.{docs_table} d
        WHERE NOT EXISTS (SELECT 1 FROM public.{users_table} u WHERE u.id = d.owner_id)
        
        UNION ALL
        
        SELECT
            'folders without valid user',
            COUNT(*)
        FROM public.{folders_table} f
        WHERE NOT EXISTS (SELECT 1 FROM public.{users_table} u WHERE u.id = f.owner_id)
        
        UNION ALL
        
        SELECT
            'chunks without valid document',
            COUNT(*)
        FROM public.{embeddings_table} c
        WHERE c.metadata->>'type' = 'chunk-data'
          AND NOT EXISTS (
            SELECT 1 FROM public.{docs_table} d
            WHERE d.doc_id = c.metadata->>'doc_id'
          )
        
        UNION ALL
        
        SELECT
            'logs without valid user',
            COUNT(*)
        FROM public.{logs_table} l
        WHERE l.user_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM public.{users_table} u WHERE u.id = l.user_id)
        
        UNION ALL
        
        SELECT
            'users without email (skipped)',
            COUNT(*)
        FROM public.{users_table}
        WHERE TRIM(COALESCE(email, '')) = ''
        
        ORDER BY rows_at_risk DESC
    """
    return execute_query(config, query)


# ============================================================================
# MAIN AUDIT FUNCTION
# ============================================================================

def run_full_audit(config: ConnectionConfig, prefix: str) -> Dict[str, Any]:
    """
    Run all audit queries and return results.
    
    Returns:
        Dictionary with all audit results organized by section.
    """
    results = {
        'users': {},
        'folders': {},
        'documents': {},
        'chunks_embeddings': {},
        'conversations': {},
        'cross_table': {}
    }
    
    # Section 2: Users
    try:
        results['users']['top_by_logs'] = audit_top_users_by_logs(config, prefix)
        results['users']['top_by_documents'] = audit_top_users_by_documents(config, prefix)
        results['users']['top_by_chunks'] = audit_top_users_by_chunks(config, prefix)
        results['users']['without_email'] = audit_users_without_email(config, prefix)
        results['users']['username_collisions'] = audit_username_collisions(config, prefix)
    except Exception as e:
        results['users']['error'] = str(e)
    
    # Section 3: Folders
    try:
        results['folders']['hierarchy_depth'] = audit_folder_hierarchy_depth(config, prefix)
        results['folders']['multilevel'] = audit_folders_multilevel(config, prefix)
        results['folders']['type_distribution'] = audit_folder_type_distribution(config, prefix)
        results['folders']['orphaned'] = audit_orphaned_folders(config, prefix)
    except Exception as e:
        results['folders']['error'] = str(e)
    
    # Section 4: Documents
    try:
        results['documents']['type_distribution'] = audit_doc_type_distribution(config, prefix)
        results['documents']['problematic_types'] = audit_problematic_doc_types(config, prefix)
        results['documents']['blob_source_distribution'] = audit_blob_source_distribution(config, prefix)
        results['documents']['orphaned_count'] = audit_orphaned_documents(config, prefix)
        results['documents']['missing_folders_count'] = audit_docs_missing_folders(config, prefix)
        results['documents']['duplicate_ids'] = audit_duplicate_doc_ids(config, prefix)
    except Exception as e:
        results['documents']['error'] = str(e)
    
    # Section 5: Chunks & Embeddings
    try:
        results['chunks_embeddings']['per_document'] = audit_chunks_per_document(config, prefix)
        results['chunks_embeddings']['orphaned'] = audit_orphaned_chunks(config, prefix)
        results['chunks_embeddings']['without_embeddings'] = audit_chunks_without_embeddings(config, prefix)
        results['chunks_embeddings']['dimensions'] = audit_embedding_dimensions(config, prefix)
        results['chunks_embeddings']['type_distribution'] = audit_chunk_type_distribution(config, prefix)
    except Exception as e:
        results['chunks_embeddings']['error'] = str(e)
    
    # Section 6: Conversations
    try:
        results['conversations']['top_users'] = audit_top_users_by_conversations(config, prefix)
        results['conversations']['size_distribution'] = audit_conversation_size_distribution(config, prefix)
        results['conversations']['without_user'] = audit_logs_without_user(config, prefix)
        results['conversations']['without_chat_id'] = audit_logs_without_chat_id(config, prefix)
        results['conversations']['invalid_chat_ids'] = audit_invalid_chat_id_format(config, prefix)
        results['conversations']['question_extraction_issues'] = audit_question_json_extraction(config, prefix)
        results['conversations']['orphaned'] = audit_orphaned_logs(config, prefix)
        results['conversations']['model_usage'] = audit_model_usage_distribution(config, prefix)
        results['conversations']['bot_usage'] = audit_bot_usage_distribution(config, prefix)
        results['conversations']['token_stats'] = audit_token_statistics(config, prefix)
    except Exception as e:
        results['conversations']['error'] = str(e)
    
    # Section 7: Cross-Table Integrity
    try:
        results['cross_table']['missing_users'] = audit_missing_user_references(config, prefix)
        results['cross_table']['data_loss_risk'] = audit_data_loss_risk_summary(config, prefix)
    except Exception as e:
        results['cross_table']['error'] = str(e)
    
    return results
