"""
Extraction engine for extracting data from source database.
"""
import os
import json
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Callable
import pandas as pd

from utils.db import ConnectionConfig, execute_query, execute_query_chunked, get_connection
from utils.config import (
    EXTRACTION_ORDER,
    SHAREPOINT_DOCUMENT_BLOB_SOURCE,
    get_table_name,
    get_query_for_table,
    TABLE_DEFINITIONS
)
from utils.sql_generator import (
    generate_users_migration_sql,
    generate_folders_migration_sql,
    generate_documents_migration_sql,
    generate_chunks_embeddings_migration_sql,
    generate_conversations_logs_migration_sql,
    generate_conversions_migration_sql,
    deterministic_uuid_v4_py,
    USER_NAMESPACE_UUID,
)

CONVERSATION_UUID_PATTERN = (
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
    r"[89aAbB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)


def normalize_email(value: object) -> str:
    """Return the canonical email key used for source/target matching."""
    return str(value or "").strip().lower()


def build_conversation_scope_cte(
    table_name: str,
    user_ids: List[str],
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    max_per_user: Optional[int] = None,
) -> Tuple[str, tuple]:
    """Build the canonical conversation-level filter shared by UI and extraction.

    A V4 conversation consists of one or more log rows sharing a ``chat_id``.
    Date and per-user limits select complete conversations; callers can then
    join the selected chat IDs back to the source table to fetch every log row.
    """
    if not user_ids:
        raise ValueError("At least one user ID is required for conversation scope")

    placeholders = ", ".join(["%s"] * len(user_ids))
    params: List[object] = list(user_ids) + [CONVERSATION_UUID_PATTERN]
    having = []
    if date_from is not None:
        having.append("MIN(l.created_at) >= %s")
        params.append(date_from)
    if date_to is not None:
        having.append("MIN(l.created_at) <= %s")
        params.append(date_to)
    having_sql = f"HAVING {' AND '.join(having)}" if having else ""
    limit_sql = ""
    if max_per_user is not None and int(max_per_user) > 0:
        limit_sql = "WHERE conversation_rank <= %s"
        params.append(int(max_per_user))

    cte = f"""
        WITH grouped_conversations AS (
            SELECT
                l.user_id,
                lower(btrim(l.chat_id::text)) AS chat_id,
                MIN(l.created_at) AS conversation_created_at,
                MAX(l.created_at) AS conversation_updated_at,
                COUNT(*)::bigint AS log_row_count
            FROM public.{table_name} l
            WHERE l.user_id IN ({placeholders})
              AND btrim(l.chat_id::text) ~ %s
            GROUP BY l.user_id, lower(btrim(l.chat_id::text))
            {having_sql}
        ),
        ranked_conversations AS (
            SELECT
                grouped_conversations.*,
                ROW_NUMBER() OVER (
                    PARTITION BY user_id
                    ORDER BY conversation_created_at DESC, chat_id
                ) AS conversation_rank
            FROM grouped_conversations
        ),
        selected_conversations AS (
            SELECT *
            FROM ranked_conversations
            {limit_sql}
        )
    """
    return cte, tuple(params)


def _get_extraction_stream_chunk_size() -> int:
    """Return the validated row count used for streamed extraction."""
    raw_value = os.getenv("EXTRACTION_STREAM_CHUNK_SIZE", "5000")
    try:
        chunk_size = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "EXTRACTION_STREAM_CHUNK_SIZE must be a positive integer; "
            f"got {raw_value!r}"
        ) from exc
    if chunk_size <= 0:
        raise ValueError(
            "EXTRACTION_STREAM_CHUNK_SIZE must be a positive integer; "
            f"got {raw_value!r}"
        )
    return chunk_size


def resolve_existing_user_overrides(
    source_config: ConnectionConfig,
    target_user_config: ConnectionConfig,
    prefix: str,
    user_emails: List[str],
    manual_overrides: Optional[Dict[str, str]] = None,
) -> Dict:
    """Resolve selected V4 users to existing V5 users, failing on ambiguity."""
    normalized = [normalize_email(email) for email in user_emails if normalize_email(email)]
    if not normalized:
        raise ValueError("No valid selected user emails were supplied.")

    users_table = get_table_name("users", prefix)
    placeholders = ", ".join(["%s"] * len(normalized))
    source_df = execute_query(
        source_config,
        f"""
        SELECT id::text AS legacy_user_id, email
        FROM public.{users_table}
        WHERE lower(trim(email)) IN ({placeholders})
        """,
        tuple(normalized),
    )
    if source_df.empty:
        raise ValueError("No selected users were found in the V4 source.")

    source_df["normalized_email"] = source_df["email"].apply(normalize_email)
    source_duplicates = source_df[
        source_df.duplicated("normalized_email", keep=False)
    ]["normalized_email"].unique().tolist()
    if source_duplicates:
        raise ValueError(
            "Ambiguous V4 email matches: " + ", ".join(sorted(source_duplicates))
        )

    target_df = execute_query(
        target_user_config,
        f"""
        SELECT id::text AS v5_user_id, email, organization_id::text
        FROM public.users
        WHERE lower(trim(email)) IN ({placeholders})
        """,
        tuple(normalized),
    )
    if not target_df.empty:
        target_df["normalized_email"] = target_df["email"].apply(normalize_email)
        target_duplicates = target_df[
            target_df.duplicated("normalized_email", keep=False)
        ]["normalized_email"].unique().tolist()
        if target_duplicates:
            raise ValueError(
                "Ambiguous V5 email matches: " + ", ".join(sorted(target_duplicates))
            )

    target_by_email = {
        row["normalized_email"]: row for _, row in target_df.iterrows()
    } if not target_df.empty else {}
    manual = manual_overrides or {}
    resolved: Dict[str, str] = {}
    users: List[Dict[str, str]] = []
    warnings: List[str] = []

    for _, source_row in source_df.iterrows():
        legacy_id = str(source_row["legacy_user_id"])
        email = str(source_row["email"]).strip()
        target_row = target_by_email.get(source_row["normalized_email"])
        automatic_id = str(target_row["v5_user_id"]) if target_row is not None else None
        manual_id = manual.get(legacy_id)
        if manual_id and automatic_id and str(manual_id) != automatic_id:
            raise ValueError(
                f"Manual override for {email} conflicts with its normalized-email V5 match."
            )

        v5_user_id = str(manual_id or automatic_id or deterministic_uuid_v4_py(
            USER_NAMESPACE_UUID, legacy_id
        ))
        action = "reused" if manual_id or automatic_id else "created"
        if action == "reused":
            resolved[legacy_id] = v5_user_id
        if target_row is not None and target_row.get("organization_id"):
            warnings.append(
                f"{email} already belongs to V5 organization "
                f"{target_row['organization_id']}; its organization will not be changed."
            )
        users.append({
            "email": email,
            "legacy_user_id": legacy_id,
            "v5_user_id": v5_user_id,
            "action": action,
        })

    unmatched_manual = set(manual) - {u["legacy_user_id"] for u in users}
    if unmatched_manual:
        raise ValueError(
            "Manual overrides reference unselected V4 users: "
            + ", ".join(sorted(unmatched_manual))
        )

    return {"overrides": resolved, "users": users, "warnings": warnings}


def validate_target_organization(
    target_admin_config: ConnectionConfig,
    organization_id: str,
) -> None:
    """Fail unless the selected organization exists and is active."""
    org_df = execute_query(
        target_admin_config,
        """
        SELECT id
        FROM public.organizations
        WHERE id = %s::uuid AND is_active = true
        """,
        (organization_id,),
    )
    if org_df.empty:
        raise ValueError(
            f"Organization {organization_id} does not exist or is not active in admin_db."
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
        progress_callback: Optional[Callable[[str, int, int], None]] = None,
        generate_sql: bool = True,
        export_csv: bool = True,
        organization_id: str = '356b50f7-bcbd-42aa-9392-e1605f42f7a1',
        embedding_model: str = 'text-embedding-ada-002',
        skip_empty_embeddings: bool = False,
        target_embedding_dim: Optional[int] = None,
        user_id_overrides: Optional[Dict[str, str]] = None,
        migration_run_id: Optional[str] = None,
        cross_owner_policy: str = "owned_only",
        include_chunkless_documents: bool = False,
    ):
        """
        Initialize extraction engine.
        
        Args:
            config: Database connection configuration
            prefix: Table prefix (e.g., 'jeen_dev')
            output_dir: Directory to save extracted CSV files
            progress_callback: Optional callback for progress updates (table_name, current, total)
            generate_sql: Whether to generate SQL migration files (default: True)
            export_csv: Whether to export CSV files (default: True)
            organization_id: Organization UUID for SQL generation
            embedding_model: Default embedding model name for chunks/embeddings
            skip_empty_embeddings: Skip rows without embeddings in chunks/embeddings migration
            target_embedding_dim: If set, truncate embeddings to this dimension (e.g. 1024)
            user_id_overrides: Optional mapping of {v4_user_id: existing_v5_uuid} for users
                               who already exist in V5 with a different UUID
            include_chunkless_documents: Preserve metadata-only documents that
                                         have no V4 chunk rows (default: False)
        """
        self.config = config
        self.prefix = prefix
        self.output_dir = output_dir
        self.progress_callback = progress_callback
        self.generate_sql = generate_sql
        self.export_csv = export_csv
        self.organization_id = organization_id
        self.embedding_model = embedding_model
        self.skip_empty_embeddings = skip_empty_embeddings
        self.target_embedding_dim = target_embedding_dim
        self.user_id_overrides = user_id_overrides or {}
        self.migration_run_id = migration_run_id or str(uuid.uuid4())
        if cross_owner_policy not in {"owned_only", "block", "reassign"}:
            raise ValueError(
                "cross_owner_policy must be 'owned_only', 'block', or 'reassign'"
            )
        self.cross_owner_policy = cross_owner_policy
        self.include_chunkless_documents = include_chunkless_documents
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Ensure output directories exist
        os.makedirs(output_dir, exist_ok=True)
        if self.generate_sql:
            self.sql_output_dir = os.path.join(os.path.dirname(output_dir), 'migrations')
            os.makedirs(self.sql_output_dir, exist_ok=True)
    
    def _report_progress(self, table_name: str, current: int, total: int):
        """Report progress if callback is set."""
        if self.progress_callback:
            self.progress_callback(table_name, current, total)

    @staticmethod
    def evaluate_document_readiness(
        documents_df: pd.DataFrame,
        embeddings_df: pd.DataFrame,
    ) -> Dict[str, List[str]]:
        """Classify planned documents from chunks/embeddings actually selected."""
        all_doc_ids = (
            {str(value) for value in documents_df.get("doc_id", [])}
            if not documents_df.empty
            else set()
        )
        chunk_counts: Dict[str, int] = {}
        embedding_counts: Dict[str, int] = {}
        if not embeddings_df.empty:
            for _, row in embeddings_df.iterrows():
                metadata = row.get("metadata")
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except (ValueError, TypeError):
                        metadata = {}
                if not isinstance(metadata, dict):
                    continue
                doc_id = metadata.get("doc_id")
                if not doc_id or metadata.get("type", "chunk-data") != "chunk-data":
                    continue
                doc_id = str(doc_id)
                chunk_counts[doc_id] = chunk_counts.get(doc_id, 0) + 1
                embedding = row.get("embeddings")
                has_embedding = embedding is not None
                if isinstance(embedding, float) and pd.isna(embedding):
                    has_embedding = False
                if isinstance(embedding, str) and not embedding.strip():
                    has_embedding = False
                if has_embedding:
                    embedding_counts[doc_id] = embedding_counts.get(doc_id, 0) + 1

        ready = {
            doc_id
            for doc_id in all_doc_ids
            if chunk_counts.get(doc_id, 0) > 0
            and chunk_counts[doc_id] == embedding_counts.get(doc_id, 0)
        }
        return {
            "ready_document_ids": sorted(ready),
            "documents_requiring_reprocessing": sorted(all_doc_ids - ready),
        }

    @staticmethod
    def document_ids_with_chunks(embeddings_df: pd.DataFrame) -> set[str]:
        """Return document IDs represented by at least one V4 chunk row."""
        chunked: set[str] = set()
        if embeddings_df.empty:
            return chunked
        for _, row in embeddings_df.iterrows():
            metadata = row.get("metadata")
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except (ValueError, TypeError):
                    metadata = {}
            if not isinstance(metadata, dict):
                continue
            doc_id = metadata.get("doc_id")
            if doc_id and metadata.get("type", "chunk-data") == "chunk-data":
                chunked.add(str(doc_id))
        return chunked

    def _get_users_group_column(self, users_table: str) -> Optional[str]:
        """Detect group-id column name in users table across environments."""
        cols_query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
        """
        cols_df = execute_query(self.config, cols_query, (users_table,))
        if cols_df.empty:
            return None
        available = set(cols_df["column_name"].tolist())
        for c in ["__group_id__", "group_id", "_group_id_", "groupid"]:
            if c in available:
                return c
        return None
    
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
        group_col = self._get_users_group_column(table_name)
        group_select = f'"{group_col}" AS "__group_id__",' if group_col else 'NULL::text AS "__group_id__",'
        
        if user_emails:
            placeholders = ", ".join(["%s"] * len(user_emails))
            query = f"""
                SELECT id, name, letter_checkbox, created_at, last_connected, times_connected,
                       token_used, words_used, phone_number, company_name, company_name_in_hebrew,
                       job, department, email, {group_select} token_limit, model, history_categories,
                       enabled_features, azure_oid, subfeatures, last_name
                FROM public.{table_name}
                WHERE email IN ({placeholders})
            """
            df = execute_query(self.config, query, tuple(user_emails))
        else:
            query = f"""
                SELECT id, name, letter_checkbox, created_at, last_connected, times_connected,
                       token_used, words_used, phone_number, company_name, company_name_in_hebrew,
                       job, department, email, {group_select} token_limit, model, history_categories,
                       enabled_features, azure_oid, subfeatures, last_name
                FROM public.{table_name}
            """
            df = execute_query(self.config, query)
        
        output_path = os.path.join(self.output_dir, f"users_{self.timestamp}.csv")
        if self.export_csv:
            df.to_csv(output_path, index=False)
        
        # Generate SQL migration file if enabled
        if self.generate_sql and len(df) > 0:
            sql_output_path = os.path.join(self.sql_output_dir, f"01_users_{self.timestamp}.sql")
            source_info = f"{self.config.host}:{self.config.port}/{self.config.database} (prefix: {self.prefix})"
            try:
                generate_users_migration_sql(
                    users_df=df,
                    output_file=sql_output_path,
                    source_info=source_info,
                    org_id=self.organization_id,
                    user_id_overrides=self.user_id_overrides,
                    migration_run_id=self.migration_run_id,
                )
            except Exception as e:
                raise RuntimeError(f"Failed to generate users SQL: {e}") from e
        
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
        df = self._resolve_folder_ancestor_closure(df, user_ids)
        
        output_path = os.path.join(self.output_dir, f"folders_{self.timestamp}.csv")
        if self.export_csv:
            df.to_csv(output_path, index=False)
        
        # Generate SQL migration file if enabled
        if self.generate_sql and len(df) > 0:
            sql_output_path = os.path.join(self.sql_output_dir, f"02_folders_{self.timestamp}.sql")
            source_info = f"{self.config.host}:{self.config.port}/{self.config.database} (prefix: {self.prefix})"
            try:
                generate_folders_migration_sql(
                    folders_df=df,
                    output_file=sql_output_path,
                    source_info=source_info,
                    user_id_overrides=self.user_id_overrides,
                    migration_run_id=self.migration_run_id,
                )
            except Exception as e:
                raise RuntimeError(f"Failed to generate folders SQL: {e}") from e
        
        return df, output_path
    
    def extract_documents(
        self,
        user_ids: List[str],
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        max_doc_size: Optional[int] = None,
        selected_doc_ids: Optional[List[str]] = None,
        generate_sql_now: bool = True,
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
        query = f"""
            SELECT doc_id, created_at, owner_id, doc_name_origin, doc_title, doc_size,
                   folder_id, doc_description, doc_type, vector_methods, doc_summery,
                   doc_summery_modified_by, doc_summery_modified_at, tags, embedding_model,
                   blob_source, version, doc_checksum, data_integration_doc_metadata
            FROM public.{table_name}
            WHERE COALESCE(blob_source, '') <> %s
        """
        params = [SHAREPOINT_DOCUMENT_BLOB_SOURCE]
        
        if selected_doc_ids is not None:
            if not selected_doc_ids:
                empty_df = pd.DataFrame(columns=[
                    "doc_id", "created_at", "owner_id", "doc_name_origin", "doc_title", "doc_size",
                    "folder_id", "doc_description", "doc_type", "vector_methods", "doc_summery",
                    "doc_summery_modified_by", "doc_summery_modified_at", "tags", "embedding_model",
                    "blob_source", "version", "doc_checksum", "data_integration_doc_metadata"
                ])
                output_path = os.path.join(self.output_dir, f"documents_{self.timestamp}.csv")
                if self.export_csv:
                    empty_df.to_csv(output_path, index=False)
                return empty_df, output_path
            placeholders = ", ".join(["%s"] * len(selected_doc_ids))
            query += f" AND doc_id IN ({placeholders})"
            params.extend(selected_doc_ids)
            if self.cross_owner_policy == "owned_only":
                owner_placeholders = ", ".join(["%s"] * len(user_ids))
                query += f" AND owner_id IN ({owner_placeholders})"
                params.extend(user_ids)
        else:
            placeholders = ", ".join(["%s"] * len(user_ids))
            query += f" AND owner_id IN ({placeholders})"
            params.extend(user_ids)
        
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
        if self.export_csv:
            df.to_csv(output_path, index=False)
        
        # Generate SQL migration file if enabled
        if self.generate_sql and generate_sql_now and len(df) > 0:
            sql_output_path = os.path.join(self.sql_output_dir, f"03_documents_{self.timestamp}.sql")
            source_info = f"{self.config.host}:{self.config.port}/{self.config.database} (prefix: {self.prefix})"
            try:
                generate_documents_migration_sql(
                    documents_df=df,
                    output_file=sql_output_path,
                    source_info=source_info,
                    user_id_overrides=self.user_id_overrides,
                    migration_run_id=self.migration_run_id,
                )
            except Exception as e:
                raise RuntimeError(f"Failed to generate documents SQL: {e}") from e
        
        return df, output_path
    
    def extract_embeddings(
        self,
        doc_ids: List[str],
        selected_embedding_ids: Optional[List[str]] = None
    ) -> Tuple[pd.DataFrame, str]:
        """
        Extract embeddings for specified documents.
        
        Args:
            doc_ids: List of document IDs to filter embeddings by (from metadata->>'doc_id')
            
        Returns:
            Tuple of (DataFrame, output_file_path)
        """
        if selected_embedding_ids is not None:
            raise ValueError(
                "Partial chunk selection is not supported. All chunk-data rows "
                "for the selected documents must be migrated."
            )
        chunk_size = _get_extraction_stream_chunk_size()
        table_name = get_table_name("embeddings", self.prefix)
        output_path = os.path.join(self.output_dir, f"embeddings_{self.timestamp}.csv")
        if not doc_ids:
            empty_df = pd.DataFrame(columns=["id", "external_id", "collection", "document", "metadata", "embeddings"])
            if self.export_csv:
                empty_df.to_csv(output_path, index=False)
            return empty_df, output_path

        placeholders = ", ".join(["%s"] * len(doc_ids))
        # ORDER BY doc_id, id lets the chunk generator assign chunk_index with
        # a running per-document counter while only holding one bounded chunk
        # of rows in memory at a time, instead of one DataFrame sized to the
        # user's entire chunk/embedding volume.
        query = f"""
            SELECT id, external_id, collection, document, metadata, embeddings
            FROM public.{table_name}
            WHERE metadata->>'type' = 'chunk-data'
              AND metadata->>'doc_id' IN ({placeholders})
            ORDER BY metadata->>'doc_id', id
        """
        collected_frames: List[pd.DataFrame] = []
        wrote_csv_header = False

        def _stream_and_collect():
            nonlocal wrote_csv_header
            for frame in execute_query_chunked(self.config, query, tuple(doc_ids), chunk_size=chunk_size):
                collected_frames.append(frame)
                if self.export_csv:
                    frame.to_csv(output_path, mode='a', index=False, header=not wrote_csv_header)
                    wrote_csv_header = True
                yield frame

        sql_output_path = None
        if self.generate_sql:
            sql_output_path = os.path.join(self.sql_output_dir, f"04_chunks_embeddings_{self.timestamp}.sql")
            source_info = f"{self.config.host}:{self.config.port}/{self.config.database} (table: {get_table_name('embeddings', self.prefix)})"
            try:
                gen_result = generate_chunks_embeddings_migration_sql(
                    jeen_dev_df=_stream_and_collect(),
                    output_file=sql_output_path,
                    source_info=source_info,
                    default_embedding_model=self.embedding_model,
                    skip_empty_embeddings=self.skip_empty_embeddings,
                    target_embedding_dim=self.target_embedding_dim,
                    migration_run_id=self.migration_run_id,
                )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to generate chunks/embeddings SQL: {e}"
                ) from e
        else:
            for _ in _stream_and_collect():
                pass

        df = (
            pd.concat(collected_frames, ignore_index=True)
            if collected_frames
            else pd.DataFrame(columns=["id", "external_id", "collection", "document", "metadata", "embeddings"])
        )

        if self.generate_sql and len(df) == 0:
            # The query matched no chunk-data rows; discard the shard(s) that
            # only contain preamble/epilogue so an empty step is never enqueued.
            for shard in gen_result.get('shards', []):
                if os.path.exists(shard):
                    os.remove(shard)
            manifest_file = sql_output_path + '.manifest.json'
            if os.path.exists(manifest_file):
                os.remove(manifest_file)
        if self.export_csv and not wrote_csv_header:
            # No rows were streamed; still produce an (empty) CSV for consistency.
            df.to_csv(output_path, index=False)

        return df, output_path
    
    def extract_agents(
        self,
        user_ids: List[str],
        selected_agent_ids: Optional[List[str]] = None,
        docs_df: Optional[pd.DataFrame] = None,
        folders_df: Optional[pd.DataFrame] = None,
        embeddings_df: Optional[pd.DataFrame] = None,
    ) -> Tuple[pd.DataFrame, str]:
        """
        Extract agents belonging to specified users.

        Prompt parts (tone, guardrail, response) are automatically concatenated
        into merged instructions during SQL generation.

        Returns:
            Tuple of (DataFrame, output_file_path)
        """
        table_name = get_table_name("agents", self.prefix)
        query = f"""
            SELECT bot_id, user_id, bot_data, toolkit_settings, character_prompts,
                   hack_prompt, analysis_prompt, grade_prompt, relevant_answer_prompt,
                   first_message, additional_links_title, docs_chosen, chosen_docs_folders,
                   folder_id, created_at, updated_at, last_activity, deleted_at, tags
            FROM public.{table_name}
            WHERE deleted_at IS NULL
        """
        params = []
        if selected_agent_ids is not None:
            if not selected_agent_ids:
                empty_df = pd.DataFrame(columns=[
                    "bot_id", "user_id", "bot_data", "toolkit_settings", "character_prompts",
                    "hack_prompt", "analysis_prompt", "grade_prompt", "relevant_answer_prompt",
                    "first_message", "additional_links_title", "docs_chosen", "chosen_docs_folders",
                    "folder_id", "created_at", "updated_at", "last_activity", "deleted_at", "tags"
                ])
                output_path = os.path.join(self.output_dir, f"agents_{self.timestamp}.csv")
                if self.export_csv:
                    empty_df.to_csv(output_path, index=False)
                return empty_df, output_path
            placeholders = ", ".join(["%s"] * len(selected_agent_ids))
            query += f" AND bot_id IN ({placeholders})"
            params.extend(selected_agent_ids)
            if self.cross_owner_policy == "owned_only":
                owner_placeholders = ", ".join(["%s"] * len(user_ids))
                query += f" AND user_id IN ({owner_placeholders})"
                params.extend(user_ids)
        else:
            placeholders = ", ".join(["%s"] * len(user_ids))
            query += f" AND user_id IN ({placeholders})"
            params.extend(user_ids)
        df = execute_query(self.config, query, tuple(params))

        output_path = os.path.join(self.output_dir, f"agents_{self.timestamp}.csv")
        
        # If dependent DataFrames are supplied, run topup before generating SQL
        # so that all agent-referenced documents/folders are guaranteed to be
        # present in migration SQL files when 06_agents_*.sql is executed.
        topup_report: Optional[Dict] = None
        if docs_df is not None and folders_df is not None and embeddings_df is not None and len(df) > 0:
            source_info_base = f"{self.config.host}:{self.config.port}/{self.config.database} (prefix: {self.prefix})"
            docs_df, embeddings_df, folders_df, topup_report = self._topup_agent_documents(
                agents_df=df,
                docs_df=docs_df,
                embeddings_df=embeddings_df,
                folders_df=folders_df,
                selected_user_ids=user_ids,
            )
            # Regenerate dependent SQL files if anything was topped up
            if self.generate_sql:
                if topup_report.get('added_folder_ids'):
                    folders_sql = os.path.join(self.sql_output_dir, f"02_folders_{self.timestamp}.sql")
                    try:
                        generate_folders_migration_sql(
                            folders_df=folders_df,
                            output_file=folders_sql,
                            source_info=source_info_base,
                            user_id_overrides=self.user_id_overrides,
                            migration_run_id=self.migration_run_id,
                        )
                    except Exception as e:
                        raise RuntimeError(
                            f"Failed to regenerate folders SQL after topup: {e}"
                        ) from e

                if topup_report.get('added_doc_ids'):
                    from utils.sql_generator import generate_documents_migration_sql, generate_chunks_embeddings_migration_sql
                    docs_sql = os.path.join(self.sql_output_dir, f"03_documents_{self.timestamp}.sql")
                    try:
                        generate_documents_migration_sql(
                            documents_df=docs_df,
                            output_file=docs_sql,
                            source_info=source_info_base,
                            user_id_overrides=self.user_id_overrides,
                            doc_source_labels=topup_report.get('doc_source_labels', {}),
                            embeddings_df=embeddings_df,
                            migration_run_id=self.migration_run_id,
                        )
                    except Exception as e:
                        raise RuntimeError(
                            f"Failed to regenerate documents SQL after topup: {e}"
                        ) from e

                    if len(embeddings_df) > 0:
                        emb_sql = os.path.join(self.sql_output_dir, f"04_chunks_embeddings_{self.timestamp}.sql")
                        try:
                            generate_chunks_embeddings_migration_sql(
                                jeen_dev_df=embeddings_df,
                                output_file=emb_sql,
                                source_info=f"{self.config.host}:{self.config.port}/{self.config.database} (table: {get_table_name('embeddings', self.prefix)})",
                                default_embedding_model=self.embedding_model,
                                skip_empty_embeddings=self.skip_empty_embeddings,
                                target_embedding_dim=self.target_embedding_dim,
                                migration_run_id=self.migration_run_id,
                            )
                        except Exception as e:
                            raise RuntimeError(
                                f"Failed to regenerate embeddings SQL after topup: {e}"
                            ) from e

        if self.export_csv:
            df.to_csv(output_path, index=False)

        # Expose topup_report for run_full_extraction to collect
        self._last_topup_report = topup_report

        # Generate SQL migration file if enabled
        if self.generate_sql and len(df) > 0:
            sql_output_path = os.path.join(self.sql_output_dir, f"06_agents_{self.timestamp}.sql")
            source_info = f"{self.config.host}:{self.config.port}/{self.config.database} (table: playground_bot_generator_config)"
            merged_instructions = self._build_merged_instructions(df)
            try:
                from utils.sql_generator import generate_agents_migration_sql
                generate_agents_migration_sql(
                    agents_df=df,
                    output_file=sql_output_path,
                    source_info=source_info,
                    user_id_overrides=self.user_id_overrides,
                    merged_instructions=merged_instructions,
                    migration_run_id=self.migration_run_id,
                )
            except Exception as e:
                raise RuntimeError(f"Failed to generate agents SQL: {e}") from e

        return df, output_path
    
    @staticmethod
    def _build_merged_instructions(agents_df: pd.DataFrame) -> Dict[str, str]:
        """Concatenate active prompt parts per agent into a single instruction.

        Only prompt features whose ``is_selected`` flag is ``True`` are
        included.  When multiple features are active they are concatenated
        with section headers ([Tone], [Guardrail], [Response]).
        """
        import json as _json

        def _parse_prompt(col_val):
            """Return (is_selected: bool, content: str|None) for a JSONB prompt column."""
            if col_val is None:
                return False, None
            if isinstance(col_val, str):
                try:
                    col_val = _json.loads(col_val)
                except Exception:
                    return False, col_val.strip() or None
            if isinstance(col_val, dict):
                selected = str(col_val.get("is_selected", False)).lower() == "true"
                content = (col_val.get("content") or "").strip() or None
                return selected, content
            return False, None

        merged: Dict[str, str] = {}
        for _, row in agents_df.iterrows():
            bot_id = str(row.get("bot_id", ""))

            tone_sel, tone = _parse_prompt(row.get("character_prompts"))
            guard_sel, guardrail = _parse_prompt(row.get("hack_prompt"))
            resp_sel, response = _parse_prompt(row.get("relevant_answer_prompt"))

            sections = []
            if tone_sel and tone:
                sections.append(f"[Tone]\n{tone}")
            if guard_sel and guardrail:
                sections.append(f"[Guardrail]\n{guardrail}")
            if resp_sel and response:
                sections.append(f"[Response]\n{response}")

            if sections:
                merged[bot_id] = "\n\n".join(sections)
        return merged

    @staticmethod
    def _normalise_folder_id(val) -> Optional[str]:
        """Normalise a V4 folder ID (int4, possibly float-loaded) to a plain integer string."""
        if val is None:
            return None
        if isinstance(val, float):
            if pd.isna(val):
                return None
            return str(int(val))
        try:
            return str(int(float(str(val).strip())))
        except (ValueError, TypeError):
            v = str(val).strip()
            return v if v else None

    def _detach_unmigrated_document_folders(
        self,
        documents_df: pd.DataFrame,
        folders_df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, List[str]]:
        """Detach owned documents from folders outside the migration scope."""
        if documents_df.empty or "folder_id" not in documents_df.columns:
            return documents_df, []
        migrated_folder_ids = {
            folder_id
            for folder_id in (
                self._normalise_folder_id(value)
                for value in folders_df.get("id", pd.Series(dtype=object))
            )
            if folder_id
        }
        result = documents_df.copy()
        detached = []
        for index, row in result.iterrows():
            folder_id = self._normalise_folder_id(row.get("folder_id"))
            if folder_id and folder_id not in migrated_folder_ids:
                result.at[index, "folder_id"] = None
                detached.append(str(row.get("doc_id")))
        return result, sorted(set(detached))

    def _build_ownership_manifest(
        self,
        documents_df: pd.DataFrame,
        folders_df: pd.DataFrame,
    ) -> Dict[str, List[Dict[str, str]]]:
        """Return the source owner expected for each planned primary entity."""
        folders = []
        for _, row in folders_df.iterrows():
            old_id = self._normalise_folder_id(row.get("id"))
            owner_id = str(row.get("owner_id") or "").strip()
            if old_id and owner_id:
                folders.append({"old_id": old_id, "owner_id": owner_id})
        documents = []
        for _, row in documents_df.iterrows():
            old_id = str(row.get("doc_id") or "").strip()
            owner_id = str(row.get("owner_id") or "").strip()
            if old_id and owner_id:
                documents.append({"old_id": old_id, "owner_id": owner_id})
        return {
            "folders": folders,
            "documents": documents,
        }

    def _resolve_folder_ancestor_closure(
        self,
        folders_df: pd.DataFrame,
        selected_user_ids: List[str],
    ) -> pd.DataFrame:
        """Include every source ancestor and reject orphaned or cyclic hierarchies."""
        if folders_df.empty:
            self._folder_hierarchy_report = {
                "added_ancestor_ids": [],
                "reassigned_ancestor_ids": [],
                "dropped_cross_owner_parent_ids": [],
                "stale_parent_ids": [],
                "detached_folder_ids": [],
            }
            return folders_df

        folders_table = get_table_name("folders", self.prefix)
        result = folders_df.copy()
        selected_owners = {str(value) for value in selected_user_ids}
        added: List[str] = []
        reassigned: List[str] = []
        dropped_cross_owner_parents: List[str] = []
        stale_parents: List[str] = []
        detached_folders: List[str] = []

        while True:
            known = {
                self._normalise_folder_id(value)
                for value in result["id"].tolist()
            }
            known.discard(None)
            requested_by: Dict[str, set] = {}
            for _, row in result.iterrows():
                parent_id = self._normalise_folder_id(row.get("parent_id"))
                if parent_id and parent_id not in known:
                    requested_by.setdefault(parent_id, set()).add(
                        str(row.get("owner_id"))
                    )
            if not requested_by:
                break

            missing_ids = sorted(requested_by)
            placeholders = ", ".join(["%s"] * len(missing_ids))
            ancestors = execute_query(
                self.config,
                f"""
                    SELECT id, folder_name, owner_id, parent_id, created_at, folder_type
                    FROM public.{folders_table}
                    WHERE id IN ({placeholders})
                """,
                tuple(missing_ids),
            )
            fetched = {
                self._normalise_folder_id(value)
                for value in ancestors.get("id", [])
            }
            fetched.discard(None)
            unresolved = set(missing_ids) - fetched
            if unresolved:
                stale_parents.extend(sorted(unresolved))
                for index, row in result.iterrows():
                    parent_id = self._normalise_folder_id(row.get("parent_id"))
                    if parent_id in unresolved:
                        result.at[index, "parent_id"] = None
                        folder_id = self._normalise_folder_id(row.get("id"))
                        if folder_id:
                            detached_folders.append(folder_id)

            cross_owner_parent_ids = set()
            for index, row in ancestors.iterrows():
                folder_id = self._normalise_folder_id(row.get("id"))
                owner_id = str(row.get("owner_id"))
                if owner_id in selected_owners:
                    continue
                if self.cross_owner_policy == "owned_only":
                    if folder_id:
                        cross_owner_parent_ids.add(folder_id)
                        dropped_cross_owner_parents.append(folder_id)
                    continue
                if self.cross_owner_policy == "block":
                    raise ValueError(
                        f"Folder ancestor {folder_id} belongs to unselected user "
                        f"{owner_id}. Include that owner or choose reassign."
                    )
                candidate_owners = requested_by.get(folder_id, set()) & selected_owners
                if not candidate_owners and len(selected_owners) == 1:
                    candidate_owners = set(selected_owners)
                if len(candidate_owners) != 1:
                    raise ValueError(
                        f"Cannot unambiguously reassign folder ancestor {folder_id}; "
                        "include its owner in the migration."
                    )
                ancestors.at[index, "owner_id"] = next(iter(candidate_owners))
                reassigned.append(folder_id)

            if cross_owner_parent_ids:
                for index, row in result.iterrows():
                    parent_id = self._normalise_folder_id(row.get("parent_id"))
                    if parent_id in cross_owner_parent_ids:
                        result.at[index, "parent_id"] = None
                        folder_id = self._normalise_folder_id(row.get("id"))
                        if folder_id:
                            detached_folders.append(folder_id)
                ancestors = ancestors[
                    ~ancestors["id"].apply(self._normalise_folder_id).isin(
                        cross_owner_parent_ids
                    )
                ].copy()

            accepted_ids = {
                self._normalise_folder_id(value)
                for value in ancestors.get("id", [])
            }
            accepted_ids.discard(None)
            added.extend(sorted(accepted_ids))
            result = pd.concat([result, ancestors], ignore_index=True)
            result = result.drop_duplicates(subset=["id"], keep="first")

        parents = {
            self._normalise_folder_id(row.get("id")):
            self._normalise_folder_id(row.get("parent_id"))
            for _, row in result.iterrows()
        }
        for folder_id in parents:
            seen = set()
            current = folder_id
            while current is not None:
                if current in seen:
                    raise ValueError(
                        f"Folder hierarchy cycle detected at source folder {current}"
                    )
                seen.add(current)
                current = parents.get(current)

        self._folder_hierarchy_report = {
            "added_ancestor_ids": sorted(set(added)),
            "reassigned_ancestor_ids": sorted(set(reassigned)),
            "dropped_cross_owner_parent_ids": sorted(
                set(dropped_cross_owner_parents)
            ),
            "stale_parent_ids": sorted(set(stale_parents)),
            "detached_folder_ids": sorted(set(detached_folders)),
        }
        return result

    # ---------------------------------------------------------------------- #
    # Helper: collect doc/folder IDs referenced by every agent in a DataFrame #
    # ---------------------------------------------------------------------- #
    @staticmethod
    def _collect_agent_refs(
        agents_df: pd.DataFrame,
    ) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
        """
        Return two dicts:
            agent_doc_map:    {bot_id: [doc_id, ...]}
            agent_folder_map: {bot_id: [folder_id, ...]}
        """
        agent_doc_map: Dict[str, List[str]] = {}
        agent_folder_map: Dict[str, List[str]] = {}

        for _, row in agents_df.iterrows():
            bot_id = str(row.get('bot_id') or '').strip()
            if not bot_id:
                continue

            doc_ids: List[str] = []
            raw = row.get('docs_chosen')
            if raw is not None and not (isinstance(raw, float) and pd.isna(raw)):
                if isinstance(raw, list):
                    doc_ids = [str(d).strip() for d in raw if d and str(d).strip()]
                elif isinstance(raw, str):
                    cleaned = raw.strip('{}')
                    if cleaned:
                        doc_ids = [d.strip().strip('"') for d in cleaned.split(',') if d.strip()]

            folder_ids: List[str] = []
            fraw = row.get('chosen_docs_folders')
            if fraw is not None and not (isinstance(fraw, float) and pd.isna(fraw)):
                if isinstance(fraw, list):
                    folder_ids = [ExtractionEngine._normalise_folder_id(f) for f in fraw if f is not None]
                    folder_ids = [f for f in folder_ids if f]
                elif isinstance(fraw, str):
                    cleaned = fraw.strip('{}')
                    if cleaned:
                        folder_ids = [
                            ExtractionEngine._normalise_folder_id(f.strip().strip('"'))
                            for f in cleaned.split(',') if f.strip()
                        ]
                        folder_ids = [f for f in folder_ids if f]
            agent_doc_map[bot_id] = doc_ids
            agent_folder_map[bot_id] = folder_ids

        return agent_doc_map, agent_folder_map

    @classmethod
    def _sanitize_agent_refs(
        cls,
        agents_df: pd.DataFrame,
        removed_doc_ids: set,
        removed_folder_ids: set,
    ) -> pd.DataFrame:
        """Remove unresolved references before Step 06 SQL is generated."""
        sanitized = agents_df.copy()
        doc_map, folder_map = cls._collect_agent_refs(sanitized)
        for index, row in sanitized.iterrows():
            bot_id = str(row.get("bot_id") or "").strip()
            sanitized.at[index, "docs_chosen"] = [
                doc_id for doc_id in doc_map.get(bot_id, [])
                if doc_id not in removed_doc_ids
            ]
            sanitized.at[index, "chosen_docs_folders"] = [
                folder_id for folder_id in folder_map.get(bot_id, [])
                if folder_id not in removed_folder_ids
            ]
            direct_folder = cls._normalise_folder_id(row.get("folder_id"))
            if direct_folder in removed_folder_ids:
                sanitized.at[index, "folder_id"] = None
        return sanitized

    def analyze_agent_dependencies(
        self,
        agents_df: pd.DataFrame,
        docs_df: pd.DataFrame,
        folders_df: pd.DataFrame,
        selected_user_ids: Optional[List[str]] = None,
    ) -> Dict:
        """
        Dry-run: classify every agent-referenced document and folder without
        mutating any DataFrame.  Useful for showing the user what will happen
        before extraction commits to regenerating SQL files.

        Returns a dict with keys:
            already_selected_doc_ids:  set — already in docs_df
            can_topup_doc_ids:         set — missing but found in V4
            stale_doc_ids:             set — missing and NOT in V4
            already_selected_folder_ids: set
            can_topup_folder_ids:      set
            stale_folder_ids:          set
            out_of_scope_owner_folder_ids: set — topped-up folders whose owner
                                           is not in selected_user_ids
            agent_doc_map:             {bot_id: [doc_ids]}
            agent_folder_map:          {bot_id: [folder_ids]}
        """
        agent_doc_map, agent_folder_map = self._collect_agent_refs(agents_df)

        all_agent_doc_ids: set = {d for docs in agent_doc_map.values() for d in docs}
        all_agent_folder_ids: set = {f for fids in agent_folder_map.values() for f in fids}
        all_agent_folder_ids.update(
            folder_id
            for folder_id in (
                self._normalise_folder_id(value)
                for value in agents_df.get("folder_id", pd.Series(dtype=object))
            )
            if folder_id
        )

        existing_doc_ids = set(docs_df['doc_id'].astype(str).tolist()) if len(docs_df) > 0 else set()
        existing_folder_ids = (
            set(folders_df['id'].apply(self._normalise_folder_id).dropna().tolist())
            if len(folders_df) > 0 else set()
        )

        missing_doc_ids = all_agent_doc_ids - existing_doc_ids
        missing_folder_ids = all_agent_folder_ids - existing_folder_ids

        # Query V4 for missing docs
        can_topup_doc_ids: set = set()
        stale_doc_ids: set = set()
        if missing_doc_ids:
            try:
                docs_table = get_table_name('custom_documents', self.prefix)
                ph = ', '.join(['%s'] * len(missing_doc_ids))
                found_df = execute_query(
                    self.config,
                    f"""
                        SELECT doc_id, blob_source
                        FROM public.{docs_table}
                        WHERE doc_id IN ({ph})
                    """,
                    tuple(missing_doc_ids)
                )
                found_ids = (
                    set(found_df["doc_id"].astype(str).tolist())
                    if len(found_df) > 0
                    else set()
                )
                excluded_sharepoint_doc_ids = (
                    set(
                        found_df.loc[
                            found_df["blob_source"] == SHAREPOINT_DOCUMENT_BLOB_SOURCE,
                            "doc_id",
                        ].astype(str)
                    )
                    if len(found_df) > 0
                    else set()
                )
                can_topup_doc_ids = found_ids - excluded_sharepoint_doc_ids
                stale_doc_ids = missing_doc_ids - found_ids
            except Exception:
                stale_doc_ids = missing_doc_ids
                excluded_sharepoint_doc_ids = set()
        else:
            excluded_sharepoint_doc_ids = set()

        # Query V4 for missing folders
        can_topup_folder_ids: set = set()
        stale_folder_ids: set = set()
        out_of_scope_owner_folder_ids: set = set()
        if missing_folder_ids:
            try:
                folders_table = get_table_name('folders', self.prefix)
                ph = ', '.join(['%s'] * len(missing_folder_ids))
                found_df = execute_query(
                    self.config,
                    f"SELECT id, owner_id FROM public.{folders_table} WHERE id IN ({ph})",
                    tuple(missing_folder_ids)
                )
                if len(found_df) > 0:
                    found_ids = set(found_df['id'].apply(self._normalise_folder_id).dropna().tolist())
                    can_topup_folder_ids = found_ids
                    stale_folder_ids = missing_folder_ids - found_ids
                    if selected_user_ids:
                        selected_set = set(str(u) for u in selected_user_ids)
                        for _, frow in found_df.iterrows():
                            if str(frow.get('owner_id', '')) not in selected_set:
                                fid = self._normalise_folder_id(frow['id'])
                                if fid:
                                    out_of_scope_owner_folder_ids.add(fid)
                else:
                    stale_folder_ids = missing_folder_ids
            except Exception:
                stale_folder_ids = missing_folder_ids

        return {
            'already_selected_doc_ids': existing_doc_ids & all_agent_doc_ids,
            'can_topup_doc_ids': can_topup_doc_ids,
            'stale_doc_ids': stale_doc_ids,
            'excluded_sharepoint_doc_ids': excluded_sharepoint_doc_ids,
            'already_selected_folder_ids': existing_folder_ids & all_agent_folder_ids,
            'can_topup_folder_ids': can_topup_folder_ids,
            'stale_folder_ids': stale_folder_ids,
            'out_of_scope_owner_folder_ids': out_of_scope_owner_folder_ids,
            'agent_doc_map': agent_doc_map,
            'agent_folder_map': agent_folder_map,
        }

    def _topup_agent_documents(
        self,
        agents_df: pd.DataFrame,
        docs_df: pd.DataFrame,
        embeddings_df: pd.DataFrame,
        folders_df: pd.DataFrame,
        selected_user_ids: Optional[List[str]] = None,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict]:
        """
        Auto-include documents AND folders referenced by agents that were not
        explicitly selected.

        - docs_chosen  → fetches missing documents + their embeddings.
        - chosen_docs_folders → fetches missing folders, then recursively
          fetches any ancestor folders needed to maintain the parent-child
          hierarchy so that folder INSERT statements don't reference
          non-existent parents.

        Returns:
            (updated_docs_df, updated_embeddings_df, updated_folders_df, report)

        report keys:
            added_doc_ids           – list of doc_ids fetched and added
            stale_doc_ids           – list of doc_ids not found in V4
            added_folder_ids        – list of folder ids fetched and added
            stale_folder_ids        – list of folder ids not found in V4
            out_of_scope_owner_folder_ids – folder ids whose owner_id is
                                           not in selected_user_ids
            doc_source_labels       – {doc_id: 'agent:<bot_id>'} for SQL annotation
        """
        # ------------------------------------------------------------------ #
        # 1. Collect every doc ID and folder ID referenced by any agent       #
        # ------------------------------------------------------------------ #
        agent_doc_map, agent_folder_map = self._collect_agent_refs(agents_df)
        agent_doc_ids: set = {d for docs in agent_doc_map.values() for d in docs}
        agent_folder_ids: set = {f for fids in agent_folder_map.values() for f in fids}
        agent_folder_ids.update(
            folder_id
            for folder_id in (
                self._normalise_folder_id(value)
                for value in agents_df.get("folder_id", pd.Series(dtype=object))
            )
            if folder_id
        )

        # Build reverse map: doc_id -> first bot_id that references it (for SQL label)
        doc_to_agent: Dict[str, str] = {}
        for bot_id, doc_ids in agent_doc_map.items():
            for d in doc_ids:
                if d not in doc_to_agent:
                    doc_to_agent[d] = bot_id
        agent_owners = {
            str(row.get("bot_id")): str(row.get("user_id"))
            for _, row in agents_df.iterrows()
            if row.get("bot_id") and row.get("user_id")
        }
        folder_to_agent: Dict[str, str] = {}
        for bot_id, folder_ids in agent_folder_map.items():
            for folder_id in folder_ids:
                folder_to_agent.setdefault(folder_id, bot_id)
        for _, row in agents_df.iterrows():
            direct_folder = self._normalise_folder_id(row.get("folder_id"))
            if direct_folder:
                folder_to_agent.setdefault(direct_folder, str(row.get("bot_id")))

        # ------------------------------------------------------------------ #
        # 2. Topup: missing documents                                          #
        # ------------------------------------------------------------------ #
        selected_set = {str(user_id) for user_id in (selected_user_ids or [])}
        added_doc_ids: List[str] = []
        stale_doc_ids: List[str] = []
        chunkless_doc_ids: List[str] = []
        excluded_sharepoint_doc_ids: List[str] = []
        out_of_scope_owner_doc_ids: List[str] = []
        doc_source_labels: Dict[str, str] = {}
        known_cross_owner_doc_ids: set[str] = set()

        if selected_set and len(docs_df) > 0:
            foreign_doc_mask = ~docs_df["owner_id"].astype(str).isin(selected_set)
            foreign_doc_ids = set(
                docs_df.loc[foreign_doc_mask, "doc_id"].astype(str).tolist()
            )
            known_cross_owner_doc_ids.update(foreign_doc_ids)
            out_of_scope_owner_doc_ids.extend(
                sorted(foreign_doc_ids & agent_doc_ids)
            )
            if self.cross_owner_policy == "owned_only" and foreign_doc_ids:
                docs_df = docs_df.loc[~foreign_doc_mask].copy()
                if "metadata" in embeddings_df.columns:
                    embeddings_df = embeddings_df[
                        ~embeddings_df["metadata"].apply(
                            lambda value: str((value or {}).get("doc_id"))
                            in foreign_doc_ids
                            if isinstance(value, dict)
                            else False
                        )
                    ].copy()
            elif self.cross_owner_policy == "reassign":
                for index, doc_row in docs_df.loc[foreign_doc_mask].iterrows():
                    doc_id = str(doc_row.get("doc_id"))
                    owner = agent_owners.get(doc_to_agent.get(doc_id, ""))
                    if not owner:
                        raise ValueError(
                            f"Cannot determine reassignment owner for document {doc_id}"
                        )
                    docs_df.at[index, "owner_id"] = owner

        existing_doc_ids = (
            set(docs_df["doc_id"].astype(str).tolist())
            if len(docs_df) > 0
            else set()
        )
        missing_doc_ids = (
            agent_doc_ids - existing_doc_ids - known_cross_owner_doc_ids
        )

        if missing_doc_ids:
            print(f"[topup] Fetching {len(missing_doc_ids)} agent-referenced document(s) not in selection...")
            docs_table = get_table_name("custom_documents", self.prefix)
            placeholders = ", ".join(["%s"] * len(missing_doc_ids))
            docs_query = f"""
                SELECT doc_id, created_at, owner_id, doc_name_origin, doc_title, doc_size,
                       folder_id, doc_description, doc_type, vector_methods, doc_summery,
                       doc_summery_modified_by, doc_summery_modified_at, tags, embedding_model,
                       blob_source, version, doc_checksum, data_integration_doc_metadata
                FROM public.{docs_table}
                WHERE doc_id IN ({placeholders})
            """
            new_docs_df = execute_query(self.config, docs_query, tuple(missing_doc_ids))

            source_found_ids = (
                set(new_docs_df["doc_id"].astype(str).tolist())
                if len(new_docs_df) > 0
                else set()
            )
            if selected_set and len(new_docs_df) > 0:
                foreign_new_doc_mask = ~new_docs_df["owner_id"].astype(str).isin(
                    selected_set
                )
                out_of_scope_owner_doc_ids.extend(
                    new_docs_df.loc[
                        foreign_new_doc_mask, "doc_id"
                    ].astype(str).tolist()
                )
                out_of_scope_owner_doc_ids = sorted(
                    set(out_of_scope_owner_doc_ids)
                )
                if self.cross_owner_policy == "owned_only":
                    new_docs_df = new_docs_df.loc[~foreign_new_doc_mask].copy()
                elif self.cross_owner_policy == "reassign":
                    for index, doc_row in new_docs_df.loc[
                        foreign_new_doc_mask
                    ].iterrows():
                        doc_id = str(doc_row["doc_id"])
                        owner = agent_owners.get(doc_to_agent.get(doc_id, ""))
                        if not owner:
                            raise ValueError(
                                "Cannot determine reassignment owner for "
                                f"document {doc_id}"
                            )
                        new_docs_df.at[index, "owner_id"] = owner

            if len(new_docs_df) > 0:
                sharepoint_mask = (
                    new_docs_df["blob_source"].fillna("")
                    == SHAREPOINT_DOCUMENT_BLOB_SOURCE
                )
                excluded_sharepoint_doc_ids = sorted(
                    new_docs_df.loc[sharepoint_mask, "doc_id"].astype(str).tolist()
                )
                new_docs_df = new_docs_df.loc[~sharepoint_mask].copy()
            found_ids = (
                set(new_docs_df["doc_id"].astype(str).tolist())
                if len(new_docs_df) > 0
                else set()
            )
            stale_doc_ids = list(missing_doc_ids - source_found_ids)

            if len(new_docs_df) == 0:
                print(
                    f"[topup] Warning: none of the {len(missing_doc_ids)} "
                    "referenced document ID(s) are eligible for migration."
                )
            else:
                # Fetch only actual chunk rows for newly discovered documents.
                # Metadata-only documents are excluded by default because V5
                # cannot use or reprocess them unless the original blob is
                # separately available.
                new_doc_ids = new_docs_df["doc_id"].astype(str).tolist()
                emb_table = get_table_name("embeddings", self.prefix)
                emb_placeholders = ", ".join(["%s"] * len(new_doc_ids))
                emb_query = f"""
                    SELECT id, external_id, collection, document, metadata, embeddings
                    FROM public.{emb_table}
                    WHERE metadata->>'doc_id' IN ({emb_placeholders})
                      AND metadata->>'type' = 'chunk-data'
                """
                new_emb_df = execute_query(self.config, emb_query, tuple(new_doc_ids))
                if not self.include_chunkless_documents:
                    chunked_ids = self.document_ids_with_chunks(new_emb_df)
                    chunkless_doc_ids = sorted(found_ids - chunked_ids)
                    if chunkless_doc_ids:
                        new_docs_df = new_docs_df[
                            ~new_docs_df["doc_id"].astype(str).isin(chunkless_doc_ids)
                        ].copy()

                added_doc_ids = new_docs_df["doc_id"].astype(str).tolist()
                # Build SQL annotation labels
                for doc_id in added_doc_ids:
                    agent_ref = doc_to_agent.get(str(doc_id), 'unknown')
                    doc_source_labels[str(doc_id)] = f'agent:{agent_ref[:16]}'

                if added_doc_ids:
                    docs_df = pd.concat([docs_df, new_docs_df], ignore_index=True)
                    # Excluded chunkless documents have no rows in new_emb_df,
                    # so every fetched chunk belongs to a retained document.
                    kept_emb_df = new_emb_df
                    if len(kept_emb_df) > 0:
                        embeddings_df = pd.concat(
                            [embeddings_df, kept_emb_df], ignore_index=True
                        )
                    print(
                        f"[topup] Added {len(added_doc_ids)} document(s) and "
                        f"{len(kept_emb_df)} embedding chunk(s)."
                    )
                if chunkless_doc_ids:
                    print(
                        f"[topup] Excluded {len(chunkless_doc_ids)} chunkless "
                        "agent-referenced document(s)."
                    )
                if excluded_sharepoint_doc_ids:
                    print(
                        f"[topup] Excluded {len(excluded_sharepoint_doc_ids)} "
                        "SharePoint-backed agent-referenced document(s)."
                    )
                if stale_doc_ids:
                    print(f"[topup] Warning: {len(stale_doc_ids)} agent-referenced doc ID(s) not found in V4 "
                          f"(stale references — agent-document links will be dropped): {stale_doc_ids[:5]}")

        # ------------------------------------------------------------------ #
        # 3. Topup: missing folders (with recursive ancestor resolution)       #
        # ------------------------------------------------------------------ #
        added_folder_ids: List[str] = []
        stale_folder_ids: List[str] = []
        out_of_scope_owner_folder_ids: List[str] = []
        detached_topup_folder_ids: List[str] = []
        all_new_folder_rows: List[pd.DataFrame] = []
        known_cross_owner_folder_ids: set[str] = set()
        if selected_set and len(folders_df) > 0:
            foreign_folder_mask = ~folders_df["owner_id"].astype(str).isin(
                selected_set
            )
            foreign_folder_ids = {
                folder_id
                for folder_id in (
                    self._normalise_folder_id(value)
                    for value in folders_df.loc[foreign_folder_mask, "id"]
                )
                if folder_id
            }
            known_cross_owner_folder_ids.update(foreign_folder_ids)
            out_of_scope_owner_folder_ids.extend(
                sorted(foreign_folder_ids & agent_folder_ids)
            )
            if self.cross_owner_policy == "owned_only" and foreign_folder_ids:
                folders_df = folders_df.loc[~foreign_folder_mask].copy()
            elif self.cross_owner_policy == "reassign":
                for index, folder_row in folders_df.loc[
                    foreign_folder_mask
                ].iterrows():
                    folder_id = self._normalise_folder_id(folder_row.get("id"))
                    owner = agent_owners.get(
                        folder_to_agent.get(folder_id or "", "")
                    )
                    if not owner:
                        raise ValueError(
                            f"Cannot determine reassignment owner for folder {folder_id}"
                        )
                    folders_df.at[index, "owner_id"] = owner

        existing_folder_ids = (
            set(folders_df["id"].apply(self._normalise_folder_id).dropna().tolist())
            if len(folders_df) > 0 else set()
        )
        missing_folder_ids = (
            agent_folder_ids
            - existing_folder_ids
            - known_cross_owner_folder_ids
        )

        if missing_folder_ids:
            print(f"[topup] Fetching {len(missing_folder_ids)} agent-referenced folder(s) not in selection "
                  "(will also resolve ancestor folders)...")
            folders_table = get_table_name("folders", self.prefix)
            known_ids = existing_folder_ids.copy()
            to_fetch = missing_folder_ids.copy()

            # Fetch iteratively until all ancestors are resolved
            while to_fetch:
                ph = ", ".join(["%s"] * len(to_fetch))
                folder_query = f"""
                    SELECT id, folder_name, owner_id, parent_id, created_at, folder_type
                    FROM public.{folders_table}
                    WHERE id IN ({ph})
                """
                batch_df = execute_query(self.config, folder_query, tuple(to_fetch))
                if len(batch_df) == 0:
                    # IDs left in to_fetch were not found — they are stale
                    stale_folder_ids.extend(list(to_fetch - known_ids))
                    break

                fetched_ids = set(batch_df["id"].apply(self._normalise_folder_id).dropna().tolist())
                # Any of to_fetch not returned are stale
                stale_folder_ids.extend(list(to_fetch - fetched_ids - known_ids))
                known_ids.update(fetched_ids)

                # Check out-of-scope owners
                foreign_batch_ids = set()
                if selected_set:
                    for _, frow in batch_df.iterrows():
                        if str(frow.get("owner_id", "")) not in selected_set:
                            fid = self._normalise_folder_id(frow["id"])
                            if fid:
                                foreign_batch_ids.add(fid)
                                if fid not in out_of_scope_owner_folder_ids:
                                    out_of_scope_owner_folder_ids.append(fid)

                accepted_batch = batch_df
                if (
                    self.cross_owner_policy == "owned_only"
                    and foreign_batch_ids
                ):
                    accepted_batch = batch_df[
                        ~batch_df["id"].apply(self._normalise_folder_id).isin(
                            foreign_batch_ids
                        )
                    ].copy()
                if len(accepted_batch) > 0:
                    all_new_folder_rows.append(accepted_batch)

                # Collect parent IDs that we don't have yet
                next_batch: set = set()
                for _, frow in accepted_batch.iterrows():
                    p = self._normalise_folder_id(frow.get('parent_id'))
                    if p and p not in known_ids:
                        next_batch.add(p)
                to_fetch = next_batch

            if all_new_folder_rows:
                new_folders_df = pd.concat(all_new_folder_rows, ignore_index=True)
                # Drop duplicates in case ancestors appeared in multiple batches
                new_folders_df = new_folders_df.drop_duplicates(subset=["id"])
                if self.cross_owner_policy == "reassign" and out_of_scope_owner_folder_ids:
                    for index, folder_row in new_folders_df.iterrows():
                        folder_id = self._normalise_folder_id(folder_row["id"])
                        if folder_id not in out_of_scope_owner_folder_ids:
                            continue
                        agent_id = folder_to_agent.get(folder_id)
                        owner = agent_owners.get(agent_id) if agent_id else None
                        if not owner and len(selected_set) == 1:
                            owner = next(iter(selected_set))
                        if not owner:
                            raise ValueError(
                                f"Cannot unambiguously reassign folder {folder_id}; "
                                "include its owner in the batch instead."
                            )
                        new_folders_df.at[index, "owner_id"] = owner
                if self.cross_owner_policy == "owned_only":
                    unavailable_parents = (
                        set(out_of_scope_owner_folder_ids)
                        | set(stale_folder_ids)
                    )
                    for index, folder_row in new_folders_df.iterrows():
                        parent_id = self._normalise_folder_id(
                            folder_row.get("parent_id")
                        )
                        if parent_id in unavailable_parents:
                            new_folders_df.at[index, "parent_id"] = None
                            folder_id = self._normalise_folder_id(
                                folder_row.get("id")
                            )
                            if folder_id:
                                detached_topup_folder_ids.append(folder_id)
                added_folder_ids = new_folders_df["id"].apply(self._normalise_folder_id).dropna().tolist()
                folders_df = pd.concat([folders_df, new_folders_df], ignore_index=True)
                print(f"[topup] Added {len(added_folder_ids)} folder(s) (including ancestors).")
                if out_of_scope_owner_folder_ids:
                    print(f"[topup] Warning: {len(out_of_scope_owner_folder_ids)} added folder(s) are owned by "
                          f"users not in the selected migration set.")
            elif missing_folder_ids:
                stale_folder_ids = sorted(
                    set(stale_folder_ids)
                    | (
                        set(missing_folder_ids)
                        - set(out_of_scope_owner_folder_ids)
                    )
                )

        cross_owner_doc_set = set(out_of_scope_owner_doc_ids) - set(chunkless_doc_ids)
        cross_owner_folder_set = set(out_of_scope_owner_folder_ids)
        if self.cross_owner_policy == "block" and (
            cross_owner_doc_set or cross_owner_folder_set
        ):
            raise ValueError(
                "Agent dependencies reference content owned by users outside the "
                "selected batch. Include those owners or explicitly choose the "
                "drop policy. Documents: "
                f"{sorted(cross_owner_doc_set)[:10]}; folders: "
                f"{sorted(cross_owner_folder_set)[:10]}"
            )

        removed_doc_ids = (
            set(stale_doc_ids)
            | set(chunkless_doc_ids)
            | set(excluded_sharepoint_doc_ids)
        )
        removed_folder_ids = set(stale_folder_ids)
        if self.cross_owner_policy == "owned_only":
            removed_doc_ids |= cross_owner_doc_set
            removed_folder_ids |= cross_owner_folder_set
        sanitized_agents = self._sanitize_agent_refs(
            agents_df, removed_doc_ids, removed_folder_ids
        )
        for column in ("docs_chosen", "chosen_docs_folders", "folder_id"):
            agents_df[column] = sanitized_agents[column]

        docs_df, detached_document_folder_ids = (
            self._detach_unmigrated_document_folders(docs_df, folders_df)
        )
        report: Dict = {
            'added_doc_ids': added_doc_ids,
            'stale_doc_ids': stale_doc_ids,
            'chunkless_doc_ids': chunkless_doc_ids,
            'excluded_sharepoint_doc_ids': excluded_sharepoint_doc_ids,
            'added_folder_ids': added_folder_ids,
            'stale_folder_ids': stale_folder_ids,
            'out_of_scope_owner_folder_ids': out_of_scope_owner_folder_ids,
            'out_of_scope_owner_doc_ids': out_of_scope_owner_doc_ids,
            'reassigned_doc_ids': (
                sorted(cross_owner_doc_set)
                if self.cross_owner_policy == "reassign"
                else []
            ),
            'reassigned_folder_ids': (
                sorted(cross_owner_folder_set)
                if self.cross_owner_policy == "reassign"
                else []
            ),
            'dropped_cross_owner_doc_ids': (
                sorted(cross_owner_doc_set)
                if self.cross_owner_policy == "owned_only"
                else []
            ),
            'dropped_cross_owner_folder_ids': (
                sorted(cross_owner_folder_set)
                if self.cross_owner_policy == "owned_only"
                else []
            ),
            'detached_topup_folder_ids': sorted(
                set(detached_topup_folder_ids)
            ),
            'detached_document_folder_ids': detached_document_folder_ids,
            'removed_doc_ids': sorted(removed_doc_ids),
            'removed_folder_ids': sorted(removed_folder_ids),
            'doc_source_labels': doc_source_labels,
            'ownership_manifest': self._build_ownership_manifest(
                docs_df,
                folders_df,
            ),
            'total_document_rows': len(docs_df),
            'total_folder_rows': len(folders_df),
            'total_chunk_rows': len(embeddings_df),
            'total_embedding_rows': int(
                embeddings_df["embeddings"].apply(
                    lambda value: value is not None
                    and not (isinstance(value, float) and pd.isna(value))
                    and not (isinstance(value, str) and not value.strip())
                ).sum()
            ) if "embeddings" in embeddings_df.columns else 0,
        }
        report["document_readiness"] = self.evaluate_document_readiness(
            docs_df, embeddings_df
        )
        return docs_df, embeddings_df, folders_df, report

    def extract_logs(
        self,
        user_ids: List[str],
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        max_per_user: Optional[int] = None,
        selected_chat_ids: Optional[List[str]] = None,
    ) -> Tuple[pd.DataFrame, str]:
        """
        Extract conversation logs belonging to specified users.

        Args:
            user_ids: List of user IDs whose logs to extract
            date_from: Optional conversation creation date filter (inclusive)
            date_to: Optional conversation creation date filter (inclusive)
            max_per_user: If set, keep only the most recent N conversations per user
            selected_chat_ids: Optional explicit subset of chat IDs from the preview

        Returns:
            Tuple of (DataFrame, output_file_path)
        """
        table_name = get_table_name("logs", self.prefix)

        cols = """id, user_id, chat_id, question, question_in_english, answer, created_at,
                   message_index, question_number, token_amount, words_amount, is_like,
                   type, bot_id, toolkit_settings, title, category, sentiment,
                   sourcetext, sourcelink, webpagelink, documents_selected, calculated_time"""
        scope_cte, params = build_conversation_scope_cte(
            table_name,
            user_ids,
            date_from=date_from,
            date_to=date_to,
            max_per_user=max_per_user,
        )
        selected_where = ""
        if selected_chat_ids is not None:
            normalized_chat_ids = [
                str(chat_id).strip().lower()
                for chat_id in selected_chat_ids
                if str(chat_id).strip()
            ]
            selected_where = "WHERE selected.chat_id = ANY(%s)"
            params += (normalized_chat_ids,)
        query = f"""
            {scope_cte}
            SELECT {', '.join(f'l.{column.strip()}' for column in cols.split(','))}
            FROM public.{table_name} l
            JOIN selected_conversations selected
              ON selected.user_id = l.user_id
             AND selected.chat_id = lower(btrim(l.chat_id::text))
            {selected_where}
            ORDER BY l.user_id, selected.chat_id, l.created_at, l.id
        """

        df = execute_query(self.config, query, params)
        
        output_path = os.path.join(self.output_dir, f"logs_{self.timestamp}.csv")
        if self.export_csv:
            df.to_csv(output_path, index=False)
        
        # Generate SQL migration file if enabled (conversations + messages + blocks)
        if self.generate_sql and len(df) > 0:
            sql_output_path = os.path.join(self.sql_output_dir, f"05_conversations_{self.timestamp}.sql")
            source_info = f"{self.config.host}:{self.config.port}/{self.config.database} (prefix: {self.prefix})"
            try:
                generate_conversations_logs_migration_sql(
                    logs_df=df,
                    output_file=sql_output_path,
                    source_info=source_info,
                    user_id_overrides=self.user_id_overrides,
                    migration_run_id=self.migration_run_id,
                )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to generate conversations SQL: {e}"
                ) from e
        
        return df, output_path
    
    def extract_translate(
        self,
        bot_ids: List[str]
    ) -> Tuple[pd.DataFrame, str]:
        """
        Extract conversion/translation rows linked to specified agents.

        Args:
            bot_ids: List of agent bot_id values whose conversions to extract

        Returns:
            Tuple of (DataFrame, output_file_path)
        """
        table_name = get_table_name("translate", self.prefix)
        placeholders = ", ".join(["%s"] * len(bot_ids))

        query = f"""
            SELECT t.id, t.bot_id, t.src, t.translated, t.type, t.last_updated, t.is_active,
                   a.user_id
            FROM public.{table_name} t
            LEFT JOIN public.playground_bot_generator_config a ON t.bot_id = a.bot_id
            WHERE t.bot_id IN ({placeholders})
        """
        df = execute_query(self.config, query, tuple(bot_ids))

        output_path = os.path.join(self.output_dir, f"translate_{self.timestamp}.csv")
        if self.export_csv:
            df.to_csv(output_path, index=False)

        if self.generate_sql and len(df) > 0:
            sql_output_path = os.path.join(
                self.sql_output_dir, f"07_conversions_{self.timestamp}.sql"
            )
            source_info = (
                f"{self.config.host}:{self.config.port}/{self.config.database}"
                f" (table: {table_name})"
            )
            try:
                generate_conversions_migration_sql(
                    translate_df=df,
                    output_file=sql_output_path,
                    source_info=source_info,
                    user_id_overrides=self.user_id_overrides,
                    migration_run_id=self.migration_run_id,
                )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to generate conversions SQL: {e}"
                ) from e

        return df, output_path

    def run_full_extraction(
        self,
        user_emails: List[str],
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        max_doc_size: Optional[int] = None,
        selected_doc_ids: Optional[List[str]] = None,
        selected_embedding_ids: Optional[List[str]] = None,
        selected_agent_ids: Optional[List[str]] = None,
        selected_conversation_chat_ids: Optional[List[str]] = None,
        extract_conversions: bool = True,
        conv_date_from: Optional[datetime] = None,
        conv_date_to: Optional[datetime] = None,
        conv_max_per_user: Optional[int] = None,
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
            "sql_files": {},
            "summary": {},
            "errors": []
        }
        
        total_steps = 8
        current_step = 0
        embeddings_df = pd.DataFrame(columns=["id", "external_id", "collection", "document", "metadata", "embeddings"])
        
        try:
            # 1. Extract users
            current_step += 1
            self._report_progress("users", current_step, total_steps)
            users_df, users_path = self.extract_users(user_emails)
            results["files"]["users"] = users_path
            results["summary"]["users"] = len(users_df)
            
            # Track SQL generation
            if self.generate_sql and len(users_df) > 0:
                sql_path = os.path.join(self.sql_output_dir, f"01_users_{self.timestamp}.sql")
                if os.path.exists(sql_path):
                    results["sql_files"]["users"] = sql_path
            
            # Get user IDs for subsequent queries
            user_ids = users_df["id"].tolist()
            
            if not user_ids:
                results["errors"].append("No users found matching the selected emails.")
                return results
            
            # 2. Extract folders
            current_step += 1
            self._report_progress("folders", current_step, total_steps)
            folders_df, folders_path = self.extract_folders(user_ids)
            results["files"]["folders"] = folders_path
            results["summary"]["folders"] = len(folders_df)
            results["folder_hierarchy_report"] = getattr(
                self, "_folder_hierarchy_report", {}
            )
            
            # Track SQL generation
            if self.generate_sql and len(folders_df) > 0:
                sql_path = os.path.join(self.sql_output_dir, f"02_folders_{self.timestamp}.sql")
                if os.path.exists(sql_path):
                    results["sql_files"]["folders"] = sql_path
            
            # 4. Extract documents
            current_step += 1
            self._report_progress("documents", current_step, total_steps)
            docs_df, docs_path = self.extract_documents(
                user_ids,
                date_from,
                date_to,
                max_doc_size,
                selected_doc_ids,
                generate_sql_now=False,
            )
            docs_df, detached_document_folder_ids = (
                self._detach_unmigrated_document_folders(
                    docs_df,
                    folders_df,
                )
            )
            results["files"]["documents"] = docs_path
            
            # Get doc_ids for embeddings
            doc_ids = docs_df["doc_id"].tolist() if len(docs_df) > 0 else []
            
            # 5. Extract embeddings (includes chunks data for SQL generation)
            current_step += 1
            self._report_progress("embeddings", current_step, total_steps)
            if doc_ids:
                embeddings_df, embeddings_path = self.extract_embeddings(doc_ids, selected_embedding_ids)
                results["files"]["embeddings"] = embeddings_path
                results["summary"]["embeddings"] = len(embeddings_df)
                results["summary"]["embedding_vectors"] = int(
                    embeddings_df["embeddings"].apply(
                        lambda value: value is not None
                        and not (isinstance(value, float) and pd.isna(value))
                        and not (isinstance(value, str) and not value.strip())
                    ).sum()
                ) if "embeddings" in embeddings_df.columns else 0

                # Track SQL generation (chunks + embeddings combined)
                if self.generate_sql and len(embeddings_df) > 0:
                    sql_path = os.path.join(self.sql_output_dir, f"04_chunks_embeddings_{self.timestamp}.sql")
                    if os.path.exists(sql_path):
                        results["sql_files"]["chunks_embeddings"] = sql_path

            else:
                results["summary"]["embeddings"] = 0
                results["summary"]["embedding_vectors"] = 0
                embeddings_df = pd.DataFrame(columns=["id", "external_id", "collection", "document", "metadata", "embeddings"])

            chunkless_doc_ids: List[str] = []
            if not self.include_chunkless_documents and len(docs_df) > 0:
                chunked_doc_ids = self.document_ids_with_chunks(embeddings_df)
                planned_doc_ids = set(docs_df["doc_id"].astype(str))
                chunkless_doc_ids = sorted(planned_doc_ids - chunked_doc_ids)
                if chunkless_doc_ids:
                    docs_df = docs_df[
                        ~docs_df["doc_id"].astype(str).isin(chunkless_doc_ids)
                    ].copy()

            results["summary"]["documents"] = len(docs_df)
            results["document_filter_report"] = {
                "chunkless_doc_ids": chunkless_doc_ids,
                "include_chunkless_documents": self.include_chunkless_documents,
                "detached_document_folder_ids": detached_document_folder_ids,
            }
            if self.export_csv:
                docs_df.to_csv(docs_path, index=False)

            # Generate document SQL only after chunk availability is known.
            # This prevents metadata-only records from entering V5 by default.
            if self.generate_sql and len(docs_df) > 0:
                docs_sql = os.path.join(
                    self.sql_output_dir, f"03_documents_{self.timestamp}.sql"
                )
                source_info = (
                    f"{self.config.host}:{self.config.port}/{self.config.database} "
                    f"(prefix: {self.prefix})"
                )
                try:
                    generate_documents_migration_sql(
                        documents_df=docs_df,
                        output_file=docs_sql,
                        source_info=source_info,
                        user_id_overrides=self.user_id_overrides,
                        embeddings_df=embeddings_df,
                        migration_run_id=self.migration_run_id,
                    )
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to generate documents SQL after chunk filtering: {e}"
                    ) from e
                results["sql_files"]["documents"] = docs_sql

            results["document_readiness"] = self.evaluate_document_readiness(
                docs_df, embeddings_df
            )
            results["ownership_manifest"] = self._build_ownership_manifest(
                docs_df,
                folders_df,
            )
            
            # 6. Extract agents — pass current docs/folders/embeddings so topup
            #    runs inside extract_agents() and SQL files are regenerated atomically.
            current_step += 1
            self._report_progress("agents", current_step, total_steps)
            agents_df, agents_path = self.extract_agents(
                user_ids, selected_agent_ids,
                docs_df=docs_df,
                folders_df=folders_df,
                embeddings_df=embeddings_df,
            )
            results["files"]["agents"] = agents_path
            results["summary"]["agents"] = len(agents_df)

            # Track SQL generation for agents
            if self.generate_sql and len(agents_df) > 0:
                sql_path = os.path.join(self.sql_output_dir, f"06_agents_{self.timestamp}.sql")
                if os.path.exists(sql_path):
                    results["sql_files"]["agents"] = sql_path

            # Collect topup report produced inside extract_agents() and update
            # summary counts + CSV exports to reflect any added records.
            # (extract_agents stores the report on self for retrieval here)
            topup_report = getattr(self, '_last_topup_report', None)
            if topup_report:
                results['topup_report'] = topup_report
                topup_chunkless = topup_report.get("chunkless_doc_ids", [])
                if topup_chunkless:
                    existing_chunkless = results["document_filter_report"].get(
                        "chunkless_doc_ids", []
                    )
                    results["document_filter_report"]["chunkless_doc_ids"] = sorted(
                        set(existing_chunkless) | set(topup_chunkless)
                    )
                results["summary"]["folders"] = topup_report["total_folder_rows"]
                results["summary"]["documents"] = topup_report["total_document_rows"]
                results["summary"]["embeddings"] = topup_report["total_chunk_rows"]
                results["summary"]["embedding_vectors"] = topup_report[
                    "total_embedding_rows"
                ]
                results["document_readiness"] = topup_report.get(
                    "document_readiness", results["document_readiness"]
                )
                results["ownership_manifest"] = topup_report.get(
                    "ownership_manifest",
                    results["ownership_manifest"],
                )
                results["document_filter_report"][
                    "detached_document_folder_ids"
                ] = sorted(
                    set(
                        results["document_filter_report"].get(
                            "detached_document_folder_ids", []
                        )
                    )
                    | set(
                        topup_report.get(
                            "detached_document_folder_ids", []
                        )
                    )
                )
                source_info_base = f"{self.config.host}:{self.config.port}/{self.config.database} (prefix: {self.prefix})"

                if topup_report.get('added_folder_ids'):
                    if self.generate_sql:
                        results["sql_files"]["folders"] = os.path.join(self.sql_output_dir, f"02_folders_{self.timestamp}.sql")

                if topup_report.get('added_doc_ids'):
                    if self.generate_sql:
                        results["sql_files"]["documents"] = os.path.join(self.sql_output_dir, f"03_documents_{self.timestamp}.sql")
                        results["sql_files"]["chunks_embeddings"] = os.path.join(self.sql_output_dir, f"04_chunks_embeddings_{self.timestamp}.sql")

            # 7. Extract logs (conversations/messages)
            current_step += 1
            self._report_progress("logs", current_step, total_steps)
            logs_df, logs_path = self.extract_logs(
                user_ids,
                date_from=conv_date_from,
                date_to=conv_date_to,
                max_per_user=conv_max_per_user,
                selected_chat_ids=selected_conversation_chat_ids,
            )
            results["files"]["logs"] = logs_path
            results["summary"]["logs"] = len(logs_df)
            valid_chat_ids = set()
            invalid_chat_rows = 0
            for value in logs_df.get("chat_id", []):
                if value is None or (
                    isinstance(value, float) and pd.isna(value)
                ):
                    invalid_chat_rows += 1
                    continue
                try:
                    valid_chat_ids.add(str(uuid.UUID(str(value).strip())))
                except (ValueError, TypeError, AttributeError):
                    invalid_chat_rows += 1
            results["summary"]["conversations"] = len(valid_chat_ids)
            results["summary"]["invalid_chat_rows"] = invalid_chat_rows
            
            # Track SQL generation (conversations + messages + content_blocks)
            if self.generate_sql and len(logs_df) > 0:
                sql_path = os.path.join(self.sql_output_dir, f"05_conversations_{self.timestamp}.sql")
                if os.path.exists(sql_path):
                    results["sql_files"]["conversations"] = sql_path

            # 8. Extract translate table (conversions)
            current_step += 1
            self._report_progress("translate", current_step, total_steps)
            if extract_conversions:
                bot_ids = agents_df["bot_id"].tolist() if len(agents_df) > 0 else []
                if bot_ids:
                    translate_df, translate_path = self.extract_translate(bot_ids)
                    results["files"]["translate"] = translate_path
                    results["summary"]["translate"] = len(translate_df)

                    if self.generate_sql and len(translate_df) > 0:
                        sql_path = os.path.join(
                            self.sql_output_dir, f"07_conversions_{self.timestamp}.sql"
                        )
                        if os.path.exists(sql_path):
                            results["sql_files"]["conversions"] = sql_path
                else:
                    results["summary"]["translate"] = 0
            else:
                results["summary"]["translate"] = 0

        except Exception as e:
            results["errors"].append(f"Extraction failed: {str(e)}")
            if self.generate_sql:
                for filename in os.listdir(self.sql_output_dir):
                    if self.timestamp in filename and filename.endswith(".sql"):
                        try:
                            os.remove(os.path.join(self.sql_output_dir, filename))
                        except OSError:
                            pass
        
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
          AND COALESCE(blob_source, '') <> %s
    """
    params = list(user_ids) + [SHAREPOINT_DOCUMENT_BLOB_SOURCE]
    
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
    if not user_ids:
        return {
            "folders": 0,
            "embeddings": 0,
            "embedding_bytes": 0,
            "agents": 0,
            "logs": 0,
        }
    folders_table = get_table_name("folders", prefix)
    embeddings_table = get_table_name("embeddings", prefix)
    agents_table = get_table_name("agents", prefix)
    logs_table = get_table_name("logs", prefix)
    user_placeholders = ", ".join(["%s"] * len(user_ids))
    params = list(user_ids)
    params.extend(user_ids)
    params.extend(user_ids)

    if doc_ids:
        doc_placeholders = ", ".join(["%s"] * len(doc_ids))
        embedding_counts = f"""
            SELECT COUNT(*)::bigint,
                   COALESCE(SUM(pg_column_size(embeddings)), 0)::bigint
            FROM public.{embeddings_table}
            WHERE metadata->>'doc_id' IN ({doc_placeholders})
              AND metadata->>'type' = 'chunk-data'
        """
        params.extend(doc_ids)
    else:
        embedding_counts = "SELECT 0::bigint, 0::bigint"

    query = f"""
        SELECT
            (SELECT COUNT(*) FROM public.{folders_table}
             WHERE owner_id IN ({user_placeholders}))::bigint AS folders,
            embedding_stats.embedding_count AS embeddings,
            embedding_stats.embedding_bytes,
            (SELECT COUNT(*) FROM public.{agents_table}
             WHERE user_id IN ({user_placeholders}))::bigint AS agents,
            (SELECT COUNT(*) FROM public.{logs_table}
             WHERE user_id IN ({user_placeholders}))::bigint AS logs
        FROM LATERAL ({embedding_counts}) AS embedding_stats(
            embedding_count, embedding_bytes
        )
    """
    try:
        df = execute_query(config, query, tuple(params))
        if df.empty:
            raise RuntimeError("Summary query returned no rows")
        row = df.iloc[0]
        return {
            key: int(row[key] or 0)
            for key in ("folders", "embeddings", "embedding_bytes", "agents", "logs")
        }
    except Exception:
        return {
            "folders": 0,
            "embeddings": 0,
            "embedding_bytes": 0,
            "agents": 0,
            "logs": 0,
        }


def get_selection_summary(
    config: ConnectionConfig,
    prefix: str,
    user_ids: List[str],
    filters: Optional[Dict] = None,
    include_chunkless_documents: bool = False,
) -> Dict[str, int]:
    """Fetch all selection-summary facts with one database round trip."""
    if not user_ids:
        return {
            "documents": 0,
            "folders": 0,
            "embeddings": 0,
            "embedding_bytes": 0,
            "agents": 0,
            "logs": 0,
        }

    filters = filters or {}
    documents_table = get_table_name("custom_documents", prefix)
    folders_table = get_table_name("folders", prefix)
    embeddings_table = get_table_name("embeddings", prefix)
    agents_table = get_table_name("agents", prefix)
    logs_table = get_table_name("logs", prefix)
    placeholders = ", ".join(["%s"] * len(user_ids))
    document_predicates = [
        f"d.owner_id IN ({placeholders})",
        "COALESCE(d.blob_source, '') <> %s",
    ]
    params = list(user_ids) + [SHAREPOINT_DOCUMENT_BLOB_SOURCE]
    if not include_chunkless_documents:
        document_predicates.append(
            f"""EXISTS (
                SELECT 1 FROM public.{embeddings_table} chunk_check
                WHERE chunk_check.metadata->>'doc_id' = d.doc_id
                  AND chunk_check.metadata->>'type' = 'chunk-data'
            )"""
        )
    if filters.get("date_from"):
        document_predicates.append("d.created_at >= %s")
        params.append(filters["date_from"])
    if filters.get("date_to"):
        document_predicates.append("d.created_at <= %s")
        params.append(filters["date_to"])
    if filters.get("max_size"):
        document_predicates.append("d.doc_size <= %s")
        params.append(filters["max_size"])

    params.extend(user_ids)
    params.extend(user_ids)
    params.extend(user_ids)
    query = f"""
        WITH selected_documents AS MATERIALIZED (
            SELECT d.doc_id
            FROM public.{documents_table} d
            WHERE {' AND '.join(document_predicates)}
        ),
        embedding_stats AS (
            SELECT COUNT(*)::bigint AS embedding_count,
                   COALESCE(SUM(pg_column_size(e.embeddings)), 0)::bigint
                       AS embedding_bytes
            FROM public.{embeddings_table} e
            JOIN selected_documents d
              ON d.doc_id = e.metadata->>'doc_id'
            WHERE e.metadata->>'type' = 'chunk-data'
        )
        SELECT
            (SELECT COUNT(*) FROM selected_documents)::bigint AS documents,
            (SELECT COUNT(*) FROM public.{folders_table}
             WHERE owner_id IN ({placeholders}))::bigint AS folders,
            embedding_stats.embedding_count AS embeddings,
            embedding_stats.embedding_bytes,
            (SELECT COUNT(*) FROM public.{agents_table}
             WHERE user_id IN ({placeholders}))::bigint AS agents,
            (SELECT COUNT(*) FROM public.{logs_table}
             WHERE user_id IN ({placeholders}))::bigint AS logs
        FROM embedding_stats
    """
    df = execute_query(config, query, tuple(params))
    if df.empty:
        raise RuntimeError("Selection summary query returned no rows")
    row = df.iloc[0]
    return {
        key: int(row[key] or 0)
        for key in (
            "documents",
            "folders",
            "embeddings",
            "embedding_bytes",
            "agents",
            "logs",
        )
    }


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
