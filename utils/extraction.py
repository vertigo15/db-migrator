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
from utils.sql_generator import (
    generate_users_migration_sql,
    generate_folders_migration_sql,
    generate_documents_migration_sql,
    generate_chunks_embeddings_migration_sql,
    generate_conversations_logs_migration_sql,
    generate_conversions_migration_sql,
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
        user_id_overrides: Optional[Dict[str, str]] = None
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
                    user_id_overrides=self.user_id_overrides
                )
            except Exception as e:
                # Log error but don't fail extraction
                print(f"Warning: Failed to generate SQL for users: {str(e)}")
        
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
                    user_id_overrides=self.user_id_overrides
                )
            except Exception as e:
                # Log error but don't fail extraction
                print(f"Warning: Failed to generate SQL for folders: {str(e)}")
        
        return df, output_path
    
    def extract_documents(
        self,
        user_ids: List[str],
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        max_doc_size: Optional[int] = None,
        selected_doc_ids: Optional[List[str]] = None
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
            WHERE 1=1
        """
        params = []
        
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
        if self.generate_sql and len(df) > 0:
            sql_output_path = os.path.join(self.sql_output_dir, f"03_documents_{self.timestamp}.sql")
            source_info = f"{self.config.host}:{self.config.port}/{self.config.database} (prefix: {self.prefix})"
            try:
                generate_documents_migration_sql(
                    documents_df=df,
                    output_file=sql_output_path,
                    source_info=source_info,
                    user_id_overrides=self.user_id_overrides
                )
            except Exception as e:
                # Log error but don't fail extraction
                import sys, traceback
                print(f"Warning: Failed to generate SQL for documents: {str(e)}", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
        
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
        table_name = get_table_name("embeddings", self.prefix)
        query = f"""
            SELECT id, external_id, collection, document, metadata, embeddings
            FROM public.{table_name}
            WHERE 1=1
        """
        params = []
        if selected_embedding_ids is not None:
            if not selected_embedding_ids:
                empty_df = pd.DataFrame(columns=["id", "external_id", "collection", "document", "metadata", "embeddings"])
                output_path = os.path.join(self.output_dir, f"embeddings_{self.timestamp}.csv")
                if self.export_csv:
                    empty_df.to_csv(output_path, index=False)
                return empty_df, output_path
            placeholders = ", ".join(["%s"] * len(selected_embedding_ids))
            query += f" AND id IN ({placeholders})"
            params.extend(selected_embedding_ids)
        elif doc_ids:
            placeholders = ", ".join(["%s"] * len(doc_ids))
            query += f" AND metadata->>'doc_id' IN ({placeholders})"
            params.extend(doc_ids)
        else:
            empty_df = pd.DataFrame(columns=["id", "external_id", "collection", "document", "metadata", "embeddings"])
            output_path = os.path.join(self.output_dir, f"embeddings_{self.timestamp}.csv")
            if self.export_csv:
                empty_df.to_csv(output_path, index=False)
            return empty_df, output_path
        
        df = execute_query(self.config, query, tuple(params))
        
        output_path = os.path.join(self.output_dir, f"embeddings_{self.timestamp}.csv")
        if self.export_csv:
            df.to_csv(output_path, index=False)
        
        # Generate SQL migration file if enabled (chunks + embeddings combined)
        if self.generate_sql and len(df) > 0:
            sql_output_path = os.path.join(self.sql_output_dir, f"04_chunks_embeddings_{self.timestamp}.sql")
            source_info = f"{self.config.host}:{self.config.port}/{self.config.database} (table: {get_table_name('embeddings', self.prefix)})"
            try:
                generate_chunks_embeddings_migration_sql(
                    jeen_dev_df=df,
                    output_file=sql_output_path,
                    source_info=source_info,
                    default_embedding_model=self.embedding_model,
                    skip_empty_embeddings=self.skip_empty_embeddings,
                    target_embedding_dim=self.target_embedding_dim
                )
            except Exception as e:
                # Log error but don't fail extraction
                print(f"Warning: Failed to generate SQL for chunks/embeddings: {str(e)}")
        
        return df, output_path
    
    def extract_agents(
        self,
        user_ids: List[str],
        selected_agent_ids: Optional[List[str]] = None,
        docs_df: Optional[pd.DataFrame] = None,
        folders_df: Optional[pd.DataFrame] = None,
        embeddings_df: Optional[pd.DataFrame] = None,
        merged_instructions: Optional[Dict[str, str]] = None,
    ) -> Tuple[pd.DataFrame, str]:
        """
        Extract agents belonging to specified users.
        
        Args:
            user_ids: List of user IDs whose agents to extract
            merged_instructions: Optional dict {bot_id: merged_instruction_text} from the
                                 prompt merger service (passed through to SQL generation)
            
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
        else:
            placeholders = ", ".join(["%s"] * len(user_ids))
            query += f" AND user_id IN ({placeholders})"
            params.extend(user_ids)
        df = execute_query(self.config, query, tuple(params))

        output_path = os.path.join(self.output_dir, f"agents_{self.timestamp}.csv")
        if self.export_csv:
            df.to_csv(output_path, index=False)
        
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
                            user_id_overrides=self.user_id_overrides
                        )
                    except Exception as e:
                        print(f"Warning: Failed to regenerate folders SQL after topup: {e}")

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
                        )
                    except Exception as e:
                        print(f"Warning: Failed to regenerate documents SQL after topup: {e}")

                    if len(embeddings_df) > 0:
                        emb_sql = os.path.join(self.sql_output_dir, f"04_chunks_embeddings_{self.timestamp}.sql")
                        try:
                            generate_chunks_embeddings_migration_sql(
                                jeen_dev_df=embeddings_df,
                                output_file=emb_sql,
                                source_info=f"{self.config.host}:{self.config.port}/{self.config.database} (table: {get_table_name('embeddings', self.prefix)})",
                                default_embedding_model=self.embedding_model,
                                skip_empty_embeddings=self.skip_empty_embeddings,
                                target_embedding_dim=self.target_embedding_dim
                            )
                        except Exception as e:
                            print(f"Warning: Failed to regenerate embeddings SQL after topup: {e}")

        # Expose topup_report for run_full_extraction to collect
        self._last_topup_report = topup_report

        # Generate SQL migration file if enabled
        if self.generate_sql and len(df) > 0:
            sql_output_path = os.path.join(self.sql_output_dir, f"06_agents_{self.timestamp}.sql")
            source_info = f"{self.config.host}:{self.config.port}/{self.config.database} (table: playground_bot_generator_config)"
            try:
                from utils.sql_generator import generate_agents_migration_sql
                generate_agents_migration_sql(
                    agents_df=df,
                    output_file=sql_output_path,
                    source_info=source_info,
                    user_id_overrides=self.user_id_overrides,
                    merged_instructions=merged_instructions,
                )
            except Exception as e:
                # Log error but don't fail extraction
                print(f"Warning: Failed to generate SQL for agents: {str(e)}")

        return df, output_path
    
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
                    f"SELECT doc_id FROM public.{docs_table} WHERE doc_id IN ({ph})",
                    tuple(missing_doc_ids)
                )
                found_ids = set(found_df['doc_id'].astype(str).tolist()) if len(found_df) > 0 else set()
                can_topup_doc_ids = found_ids
                stale_doc_ids = missing_doc_ids - found_ids
            except Exception:
                stale_doc_ids = missing_doc_ids

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

        # Build reverse map: doc_id -> first bot_id that references it (for SQL label)
        doc_to_agent: Dict[str, str] = {}
        for bot_id, doc_ids in agent_doc_map.items():
            for d in doc_ids:
                if d not in doc_to_agent:
                    doc_to_agent[d] = bot_id

        # ------------------------------------------------------------------ #
        # 2. Topup: missing documents                                          #
        # ------------------------------------------------------------------ #
        existing_doc_ids = set(docs_df["doc_id"].astype(str).tolist()) if len(docs_df) > 0 else set()
        missing_doc_ids = agent_doc_ids - existing_doc_ids

        added_doc_ids: List[str] = []
        stale_doc_ids: List[str] = []
        doc_source_labels: Dict[str, str] = {}

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

            found_ids = set(new_docs_df["doc_id"].astype(str).tolist()) if len(new_docs_df) > 0 else set()
            stale_doc_ids = list(missing_doc_ids - found_ids)

            if found_ids:
                stale_doc_ids = list(missing_doc_ids - found_ids)
            else:
                stale_doc_ids = list(missing_doc_ids)

            if len(new_docs_df) == 0:
                print(f"[topup] Warning: none of the {len(missing_doc_ids)} referenced doc ID(s) found "
                      "(may have been deleted from source).")
            else:
                added_doc_ids = new_docs_df["doc_id"].tolist()
                # Build SQL annotation labels
                for doc_id in added_doc_ids:
                    agent_ref = doc_to_agent.get(str(doc_id), 'unknown')
                    doc_source_labels[str(doc_id)] = f'agent:{agent_ref[:16]}'

                docs_df = pd.concat([docs_df, new_docs_df], ignore_index=True)

                # Fetch embeddings for the newly added documents
                new_doc_ids = new_docs_df["doc_id"].tolist()
                emb_table = get_table_name("embeddings", self.prefix)
                emb_placeholders = ", ".join(["%s"] * len(new_doc_ids))
                emb_query = f"""
                    SELECT id, external_id, collection, document, metadata, embeddings
                    FROM public.{emb_table}
                    WHERE metadata->>'doc_id' IN ({emb_placeholders})
                """
                new_emb_df = execute_query(self.config, emb_query, tuple(new_doc_ids))
                if len(new_emb_df) > 0:
                    embeddings_df = pd.concat([embeddings_df, new_emb_df], ignore_index=True)
                print(f"[topup] Added {len(added_doc_ids)} document(s) and {len(new_emb_df)} embedding chunk(s).")
                if stale_doc_ids:
                    print(f"[topup] Warning: {len(stale_doc_ids)} agent-referenced doc ID(s) not found in V4 "
                          f"(stale references — agent-document links will be dropped): {stale_doc_ids[:5]}")

        # ------------------------------------------------------------------ #
        # 3. Topup: missing folders (with recursive ancestor resolution)       #
        # ------------------------------------------------------------------ #
        existing_folder_ids = (
            set(folders_df["id"].apply(self._normalise_folder_id).dropna().tolist())
            if len(folders_df) > 0 else set()
        )
        missing_folder_ids = agent_folder_ids - existing_folder_ids
        added_folder_ids: List[str] = []
        stale_folder_ids: List[str] = []
        out_of_scope_owner_folder_ids: List[str] = []
        all_new_folder_rows: List[pd.DataFrame] = []

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

                all_new_folder_rows.append(batch_df)
                fetched_ids = set(batch_df["id"].apply(self._normalise_folder_id).dropna().tolist())
                # Any of to_fetch not returned are stale
                stale_folder_ids.extend(list(to_fetch - fetched_ids - known_ids))
                known_ids.update(fetched_ids)

                # Check out-of-scope owners
                if selected_user_ids:
                    sel_set = set(str(u) for u in selected_user_ids)
                    for _, frow in batch_df.iterrows():
                        if str(frow.get('owner_id', '')) not in sel_set:
                            fid = self._normalise_folder_id(frow['id'])
                            if fid and fid not in out_of_scope_owner_folder_ids:
                                out_of_scope_owner_folder_ids.append(fid)

                # Collect parent IDs that we don't have yet
                next_batch: set = set()
                for _, frow in batch_df.iterrows():
                    p = self._normalise_folder_id(frow.get('parent_id'))
                    if p and p not in known_ids:
                        next_batch.add(p)
                to_fetch = next_batch

            if all_new_folder_rows:
                new_folders_df = pd.concat(all_new_folder_rows, ignore_index=True)
                # Drop duplicates in case ancestors appeared in multiple batches
                new_folders_df = new_folders_df.drop_duplicates(subset=["id"])
                added_folder_ids = new_folders_df["id"].apply(self._normalise_folder_id).dropna().tolist()
                folders_df = pd.concat([folders_df, new_folders_df], ignore_index=True)
                print(f"[topup] Added {len(added_folder_ids)} folder(s) (including ancestors).")
                if out_of_scope_owner_folder_ids:
                    print(f"[topup] Warning: {len(out_of_scope_owner_folder_ids)} added folder(s) are owned by "
                          f"users not in the selected migration set.")
            elif missing_folder_ids:
                stale_folder_ids = list(missing_folder_ids)

        report: Dict = {
            'added_doc_ids': added_doc_ids,
            'stale_doc_ids': stale_doc_ids,
            'added_folder_ids': added_folder_ids,
            'stale_folder_ids': stale_folder_ids,
            'out_of_scope_owner_folder_ids': out_of_scope_owner_folder_ids,
            'doc_source_labels': doc_source_labels,
        }
        return docs_df, embeddings_df, folders_df, report

    def extract_logs(
        self,
        user_ids: List[str]
    ) -> Tuple[pd.DataFrame, str]:
        """
        Extract conversation logs belonging to specified users.
        
        Args:
            user_ids: List of user IDs whose logs to extract
            
        Returns:
            Tuple of (DataFrame, output_file_path)
        """
        table_name = get_table_name("logs", self.prefix)
        placeholders = ", ".join(["%s"] * len(user_ids))
        
        query = f"""
            SELECT id, user_id, chat_id, question, question_in_english, answer, created_at,
                   message_index, question_number, token_amount, words_amount, is_like,
                   type, bot_id, toolkit_settings, title, category, sentiment,
                   sourcetext, sourcelink, webpagelink, documents_selected, calculated_time
            FROM public.{table_name}
            WHERE user_id IN ({placeholders})
        """
        df = execute_query(self.config, query, tuple(user_ids))
        
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
                    user_id_overrides=self.user_id_overrides
                )
            except Exception as e:
                # Log error but don't fail extraction
                print(f"Warning: Failed to generate SQL for conversations/messages: {str(e)}")
        
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
                )
            except Exception as e:
                print(f"Warning: Failed to generate SQL for conversions: {str(e)}")

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
        merged_instructions: Optional[Dict[str, str]] = None,
        extract_conversions: bool = True,
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
            
            # Track SQL generation
            if self.generate_sql and len(folders_df) > 0:
                sql_path = os.path.join(self.sql_output_dir, f"02_folders_{self.timestamp}.sql")
                if os.path.exists(sql_path):
                    results["sql_files"]["folders"] = sql_path
            
            # 4. Extract documents
            current_step += 1
            self._report_progress("documents", current_step, total_steps)
            docs_df, docs_path = self.extract_documents(
                user_ids, date_from, date_to, max_doc_size, selected_doc_ids
            )
            results["files"]["documents"] = docs_path
            results["summary"]["documents"] = len(docs_df)
            
            # Track SQL generation
            if self.generate_sql and len(docs_df) > 0:
                sql_path = os.path.join(self.sql_output_dir, f"03_documents_{self.timestamp}.sql")
                if os.path.exists(sql_path):
                    results["sql_files"]["documents"] = sql_path
            
            # Get doc_ids for embeddings
            doc_ids = docs_df["doc_id"].tolist() if len(docs_df) > 0 else []
            
            # 5. Extract embeddings (includes chunks data for SQL generation)
            current_step += 1
            self._report_progress("embeddings", current_step, total_steps)
            if doc_ids:
                embeddings_df, embeddings_path = self.extract_embeddings(doc_ids, selected_embedding_ids)
                results["files"]["embeddings"] = embeddings_path
                results["summary"]["embeddings"] = len(embeddings_df)

                # Track SQL generation (chunks + embeddings combined)
                if self.generate_sql and len(embeddings_df) > 0:
                    sql_path = os.path.join(self.sql_output_dir, f"04_chunks_embeddings_{self.timestamp}.sql")
                    if os.path.exists(sql_path):
                        results["sql_files"]["chunks_embeddings"] = sql_path

                # Regenerate 03_documents_*.sql now that embeddings are known so that
                # translate_to_english is correctly set on each document_processing record.
                if self.generate_sql and len(docs_df) > 0:
                    _docs_sql = os.path.join(self.sql_output_dir, f"03_documents_{self.timestamp}.sql")
                    _src = f"{self.config.host}:{self.config.port}/{self.config.database} (prefix: {self.prefix})"
                    try:
                        generate_documents_migration_sql(
                            documents_df=docs_df,
                            output_file=_docs_sql,
                            source_info=_src,
                            user_id_overrides=self.user_id_overrides,
                            embeddings_df=embeddings_df,
                        )
                    except Exception as e:
                        print(f"Warning: Failed to regenerate documents SQL with translation info: {e}")
            else:
                results["summary"]["embeddings"] = 0
                embeddings_df = pd.DataFrame(columns=["id", "external_id", "collection", "document", "metadata", "embeddings"])
            
            # 6. Extract agents — pass current docs/folders/embeddings so topup
            #    runs inside extract_agents() and SQL files are regenerated atomically.
            current_step += 1
            self._report_progress("agents", current_step, total_steps)
            agents_df, agents_path = self.extract_agents(
                user_ids, selected_agent_ids,
                docs_df=docs_df,
                folders_df=folders_df,
                embeddings_df=embeddings_df,
                merged_instructions=merged_instructions,
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
                source_info_base = f"{self.config.host}:{self.config.port}/{self.config.database} (prefix: {self.prefix})"

                if topup_report.get('added_folder_ids'):
                    # Re-fetch updated folders_df from the file that was regenerated
                    results["summary"]["folders"] = results["summary"].get("folders", 0) + len(topup_report['added_folder_ids'])
                    if self.generate_sql:
                        results["sql_files"]["folders"] = os.path.join(self.sql_output_dir, f"02_folders_{self.timestamp}.sql")

                if topup_report.get('added_doc_ids'):
                    results["summary"]["documents"] = results["summary"].get("documents", 0) + len(topup_report['added_doc_ids'])
                    if self.generate_sql:
                        results["sql_files"]["documents"] = os.path.join(self.sql_output_dir, f"03_documents_{self.timestamp}.sql")
                        results["sql_files"]["chunks_embeddings"] = os.path.join(self.sql_output_dir, f"04_chunks_embeddings_{self.timestamp}.sql")

            # 7. Extract logs (conversations/messages)
            current_step += 1
            self._report_progress("logs", current_step, total_steps)
            logs_df, logs_path = self.extract_logs(user_ids)
            results["files"]["logs"] = logs_path
            results["summary"]["logs"] = len(logs_df)
            
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
    
    # Logs/conversations count
    try:
        logs_table = get_table_name("logs", prefix)
        placeholders = ", ".join(["%s"] * len(user_ids))
        query = f"SELECT COUNT(*) as count FROM public.{logs_table} WHERE user_id IN ({placeholders})"
        df = execute_query(config, query, tuple(user_ids))
        counts["logs"] = int(df["count"].iloc[0])
    except:
        counts["logs"] = 0
    
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
