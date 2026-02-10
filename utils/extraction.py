"""
Extraction engine for extracting data from source database.
"""
import os
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Callable
import pandas as pd

from utils.db import ConnectionConfig, execute_query, get_connection
from utils.config import (
    EXTRACTION_ORDER,
    get_table_name,
    get_query_for_table,
    TABLE_DEFINITIONS
)


class ExtractionEngine:
    """
    Engine for extracting data from source database based on user selections.
    """
    
    def __init__(
        self,
        config: ConnectionConfig,
        prefix: str,
        output_dir: str,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ):
        """
        Initialize extraction engine.
        
        Args:
            config: Database connection configuration
            prefix: Table prefix (e.g., 'jeen_dev')
            output_dir: Directory to save extracted CSV files
            progress_callback: Optional callback for progress updates (table_name, current, total)
        """
        self.config = config
        self.prefix = prefix
        self.output_dir = output_dir
        self.progress_callback = progress_callback
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
    
    def _report_progress(self, table_name: str, current: int, total: int):
        """Report progress if callback is set."""
        if self.progress_callback:
            self.progress_callback(table_name, current, total)
    
    def extract_users_groups(
        self,
        group_ids: Optional[List[str]] = None
    ) -> Tuple[pd.DataFrame, str]:
        """
        Extract user groups data.
        
        Args:
            group_ids: Optional list of specific group IDs to extract
            
        Returns:
            Tuple of (DataFrame, output_file_path)
        """
        table_name = get_table_name("users_groups", self.prefix)
        
        if group_ids:
            placeholders = ", ".join(["%s"] * len(group_ids))
            query = f"""
                SELECT id, group_name, default_model, default_max_tokens_per_user, enabled_features
                FROM public.{table_name}
                WHERE id IN ({placeholders})
            """
            df = execute_query(self.config, query, tuple(group_ids))
        else:
            query = f"""
                SELECT id, group_name, default_model, default_max_tokens_per_user, enabled_features
                FROM public.{table_name}
            """
            df = execute_query(self.config, query)
        
        output_path = os.path.join(self.output_dir, f"users_groups_{self.timestamp}.csv")
        df.to_csv(output_path, index=False)
        
        return df, output_path
    
    def extract_users(
        self,
        user_emails: Optional[List[str]] = None
    ) -> Tuple[pd.DataFrame, str]:
        """
        Extract users data.
        
        Args:
            user_emails: Optional list of specific user emails to extract
            
        Returns:
            Tuple of (DataFrame, output_file_path)
        """
        table_name = get_table_name("users", self.prefix)
        
        if user_emails:
            placeholders = ", ".join(["%s"] * len(user_emails))
            query = f"""
                SELECT id, name, letter_checkbox, created_at, last_connected, times_connected,
                       token_used, words_used, phone_number, company_name, company_name_in_hebrew,
                       job, department, email, __group_id__, token_limit, model, history_categories,
                       enabled_features, azure_oid, subfeatures, last_name
                FROM public.{table_name}
                WHERE email IN ({placeholders})
            """
            df = execute_query(self.config, query, tuple(user_emails))
        else:
            query = f"""
                SELECT id, name, letter_checkbox, created_at, last_connected, times_connected,
                       token_used, words_used, phone_number, company_name, company_name_in_hebrew,
                       job, department, email, __group_id__, token_limit, model, history_categories,
                       enabled_features, azure_oid, subfeatures, last_name
                FROM public.{table_name}
            """
            df = execute_query(self.config, query)
        
        output_path = os.path.join(self.output_dir, f"users_{self.timestamp}.csv")
        df.to_csv(output_path, index=False)
        
        return df, output_path
    
    def extract_folders(
        self,
        user_ids: List[str]
    ) -> Tuple[pd.DataFrame, str]:
        """
        Extract folders belonging to specified users.
        
        Args:
            user_ids: List of user IDs whose folders to extract
            
        Returns:
            Tuple of (DataFrame, output_file_path)
        """
        table_name = get_table_name("folders", self.prefix)
        placeholders = ", ".join(["%s"] * len(user_ids))
        
        query = f"""
            SELECT id, folder_name, owner_id, parent_id, created_at, folder_type
            FROM public.{table_name}
            WHERE owner_id IN ({placeholders})
        """
        df = execute_query(self.config, query, tuple(user_ids))
        
        output_path = os.path.join(self.output_dir, f"folders_{self.timestamp}.csv")
        df.to_csv(output_path, index=False)
        
        return df, output_path
    
    def extract_documents(
        self,
        user_ids: List[str],
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        max_doc_size: Optional[int] = None
    ) -> Tuple[pd.DataFrame, str]:
        """
        Extract documents with optional filters.
        
        Args:
            user_ids: List of user IDs (owner_id) to filter by
            date_from: Optional start date filter
            date_to: Optional end date filter
            max_doc_size: Optional maximum document size filter
            
        Returns:
            Tuple of (DataFrame, output_file_path)
        """
        table_name = get_table_name("custom_documents", self.prefix)
        placeholders = ", ".join(["%s"] * len(user_ids))
        
        query = f"""
            SELECT doc_id, created_at, owner_id, doc_name_origin, doc_title, doc_size,
                   folder_id, doc_description, doc_type, vector_methods, doc_summery,
                   doc_summery_modified_by, doc_summery_modified_at, tags, embedding_model,
                   blob_source, version, doc_checksum, data_integration_doc_metadata
            FROM public.{table_name}
            WHERE owner_id IN ({placeholders})
        """
        params = list(user_ids)
        
        if date_from:
            query += " AND created_at >= %s"
            params.append(date_from)
        
        if date_to:
            query += " AND created_at <= %s"
            params.append(date_to)
        
        if max_doc_size:
            query += " AND doc_size <= %s"
            params.append(max_doc_size)
        
        df = execute_query(self.config, query, tuple(params))
        
        output_path = os.path.join(self.output_dir, f"documents_{self.timestamp}.csv")
        df.to_csv(output_path, index=False)
        
        return df, output_path
    
    def extract_embeddings(
        self,
        doc_ids: List[str]
    ) -> Tuple[pd.DataFrame, str]:
        """
        Extract embeddings for specified documents.
        
        Args:
            doc_ids: List of document IDs to filter embeddings by (from metadata->>'doc_id')
            
        Returns:
            Tuple of (DataFrame, output_file_path)
        """
        table_name = get_table_name("embeddings", self.prefix)
        
        # Build query with JSONB filter
        # We need to filter by metadata->>'doc_id' IN (doc_ids)
        placeholders = ", ".join(["%s"] * len(doc_ids))
        
        query = f"""
            SELECT id, external_id, collection, document, metadata, embeddings
            FROM public.{table_name}
            WHERE metadata->>'doc_id' IN ({placeholders})
        """
        
        df = execute_query(self.config, query, tuple(doc_ids))
        
        output_path = os.path.join(self.output_dir, f"embeddings_{self.timestamp}.csv")
        df.to_csv(output_path, index=False)
        
        return df, output_path
    
    def extract_agents(
        self,
        user_ids: List[str]
    ) -> Tuple[pd.DataFrame, str]:
        """
        Extract agents belonging to specified users.
        
        Args:
            user_ids: List of user IDs whose agents to extract
            
        Returns:
            Tuple of (DataFrame, output_file_path)
        """
        table_name = get_table_name("agents", self.prefix)
        placeholders = ", ".join(["%s"] * len(user_ids))
        
        query = f"""
            SELECT bot_id, user_id, bot_data, tags, folder_id, created_at
            FROM public.{table_name}
            WHERE user_id IN ({placeholders})
        """
        df = execute_query(self.config, query, tuple(user_ids))
        
        output_path = os.path.join(self.output_dir, f"agents_{self.timestamp}.csv")
        df.to_csv(output_path, index=False)
        
        return df, output_path
    
    def run_full_extraction(
        self,
        user_emails: List[str],
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        max_doc_size: Optional[int] = None
    ) -> Dict:
        """
        Run full extraction pipeline.
        
        Args:
            user_emails: List of user emails to extract
            date_from: Optional document date filter (start)
            date_to: Optional document date filter (end)
            max_doc_size: Optional maximum document size
            
        Returns:
            Dictionary with extraction results
        """
        results = {
            "timestamp": self.timestamp,
            "files": {},
            "summary": {},
            "errors": []
        }
        
        total_steps = 6
        current_step = 0
        
        try:
            # 1. Extract users
            current_step += 1
            self._report_progress("users", current_step, total_steps)
            users_df, users_path = self.extract_users(user_emails)
            results["files"]["users"] = users_path
            results["summary"]["users"] = len(users_df)
            
            # Get user IDs for subsequent queries
            user_ids = users_df["id"].tolist()
            
            if not user_ids:
                results["errors"].append("No users found matching the selected emails.")
                return results
            
            # Get group IDs for users
            group_ids = users_df["__group_id__"].dropna().unique().tolist()
            
            # 2. Extract user groups
            current_step += 1
            self._report_progress("users_groups", current_step, total_steps)
            if group_ids:
                groups_df, groups_path = self.extract_users_groups(group_ids)
                results["files"]["users_groups"] = groups_path
                results["summary"]["users_groups"] = len(groups_df)
            else:
                results["summary"]["users_groups"] = 0
            
            # 3. Extract folders
            current_step += 1
            self._report_progress("folders", current_step, total_steps)
            folders_df, folders_path = self.extract_folders(user_ids)
            results["files"]["folders"] = folders_path
            results["summary"]["folders"] = len(folders_df)
            
            # 4. Extract documents
            current_step += 1
            self._report_progress("documents", current_step, total_steps)
            docs_df, docs_path = self.extract_documents(
                user_ids, date_from, date_to, max_doc_size
            )
            results["files"]["documents"] = docs_path
            results["summary"]["documents"] = len(docs_df)
            
            # Get doc_ids for embeddings
            doc_ids = docs_df["doc_id"].tolist() if len(docs_df) > 0 else []
            
            # 5. Extract embeddings
            current_step += 1
            self._report_progress("embeddings", current_step, total_steps)
            if doc_ids:
                embeddings_df, embeddings_path = self.extract_embeddings(doc_ids)
                results["files"]["embeddings"] = embeddings_path
                results["summary"]["embeddings"] = len(embeddings_df)
            else:
                results["summary"]["embeddings"] = 0
            
            # 6. Extract agents
            current_step += 1
            self._report_progress("agents", current_step, total_steps)
            agents_df, agents_path = self.extract_agents(user_ids)
            results["files"]["agents"] = agents_path
            results["summary"]["agents"] = len(agents_df)
            
        except Exception as e:
            results["errors"].append(f"Extraction failed: {str(e)}")
        
        return results


def get_document_count_preview(
    config: ConnectionConfig,
    prefix: str,
    user_ids: List[str],
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    max_doc_size: Optional[int] = None
) -> int:
    """
    Get a preview count of documents matching the filters.
    
    Args:
        config: Database connection
        prefix: Table prefix
        user_ids: List of user IDs to filter by
        date_from: Optional start date
        date_to: Optional end date
        max_doc_size: Optional max size
        
    Returns:
        Count of matching documents
    """
    table_name = get_table_name("custom_documents", prefix)
    placeholders = ", ".join(["%s"] * len(user_ids))
    
    query = f"""
        SELECT COUNT(*) as count
        FROM public.{table_name}
        WHERE owner_id IN ({placeholders})
    """
    params = list(user_ids)
    
    if date_from:
        query += " AND created_at >= %s"
        params.append(date_from)
    
    if date_to:
        query += " AND created_at <= %s"
        params.append(date_to)
    
    if max_doc_size:
        query += " AND doc_size <= %s"
        params.append(max_doc_size)
    
    df = execute_query(config, query, tuple(params))
    return int(df["count"].iloc[0]) if len(df) > 0 else 0


def get_related_counts(
    config: ConnectionConfig,
    prefix: str,
    user_ids: List[str],
    doc_ids: List[str]
) -> Dict[str, int]:
    """
    Get counts of related data for the selection summary.
    
    Args:
        config: Database connection
        prefix: Table prefix
        user_ids: List of selected user IDs
        doc_ids: List of selected document IDs
        
    Returns:
        Dictionary of table name to row count
    """
    counts = {}
    
    # Folders count
    try:
        folders_table = get_table_name("folders", prefix)
        placeholders = ", ".join(["%s"] * len(user_ids))
        query = f"SELECT COUNT(*) as count FROM public.{folders_table} WHERE owner_id IN ({placeholders})"
        df = execute_query(config, query, tuple(user_ids))
        counts["folders"] = int(df["count"].iloc[0])
    except:
        counts["folders"] = 0
    
    # Embeddings count
    try:
        if doc_ids:
            embeddings_table = get_table_name("embeddings", prefix)
            placeholders = ", ".join(["%s"] * len(doc_ids))
            query = f"SELECT COUNT(*) as count FROM public.{embeddings_table} WHERE metadata->>'doc_id' IN ({placeholders})"
            df = execute_query(config, query, tuple(doc_ids))
            counts["embeddings"] = int(df["count"].iloc[0])
        else:
            counts["embeddings"] = 0
    except:
        counts["embeddings"] = 0
    
    # Agents count
    try:
        agents_table = get_table_name("agents", prefix)
        placeholders = ", ".join(["%s"] * len(user_ids))
        query = f"SELECT COUNT(*) as count FROM public.{agents_table} WHERE user_id IN ({placeholders})"
        df = execute_query(config, query, tuple(user_ids))
        counts["agents"] = int(df["count"].iloc[0])
    except:
        counts["agents"] = 0
    
    return counts


def estimate_embeddings_size(
    config: ConnectionConfig,
    prefix: str,
    doc_ids: List[str]
) -> float:
    """
    Estimate the size of embeddings for selected documents.
    
    Args:
        config: Database connection
        prefix: Table prefix
        doc_ids: List of document IDs
        
    Returns:
        Estimated size in MB
    """
    if not doc_ids:
        return 0.0
    
    try:
        embeddings_table = get_table_name("embeddings", prefix)
        placeholders = ", ".join(["%s"] * len(doc_ids))
        
        # Use pg_column_size to estimate
        query = f"""
            SELECT SUM(pg_column_size(embeddings)) as total_size
            FROM public.{embeddings_table}
            WHERE metadata->>'doc_id' IN ({placeholders})
        """
        df = execute_query(config, query, tuple(doc_ids))
        total_bytes = df["total_size"].iloc[0] or 0
        return total_bytes / (1024 * 1024)  # Convert to MB
    except:
        return 0.0
