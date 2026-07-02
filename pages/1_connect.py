"""
Page 1: Connect to Source Database.
Defaults are loaded from the project .env file.
"""
import os
from datetime import datetime
import streamlit as st
from dotenv import dotenv_values

from utils.db import (
    ConnectionConfig, 
    test_connection, 
    check_tables_exist, 
    run_pg_dump,
    get_table_row_count,
    execute_query,
)
from utils.storage import save_connection, save_to_storage
from utils.config import SessionKeys, get_all_table_names
from utils.audit import run_full_audit
from utils.pdf_export import generate_audit_pdf
import pandas as pd


def _fmt_df(df: pd.DataFrame) -> pd.DataFrame:
    """Format numeric DataFrame columns with comma thousand separators."""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df
    result = df.copy()
    for col in result.select_dtypes(include=['int64', 'int32', 'int16', 'int8']).columns:
        result[col] = result[col].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "")
    for col in result.select_dtypes(include=['float64', 'float32']).columns:
        result[col] = result[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "")
    return result


def _get_agent_knowledge_counts(config: ConnectionConfig, prefix: str) -> dict:
    """Return total agents split by whether they reference docs or folders."""
    agents_table = get_all_table_names(prefix).get("agents", "playground_bot_generator_config")
    query = f"""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (
                WHERE COALESCE(array_length(docs_chosen, 1), 0) > 0
                   OR COALESCE(array_length(chosen_docs_folders, 1), 0) > 0
            ) AS with_knowledge,
            COUNT(*) FILTER (
                WHERE COALESCE(array_length(docs_chosen, 1), 0) = 0
                  AND COALESCE(array_length(chosen_docs_folders, 1), 0) = 0
            ) AS without_knowledge
        FROM public.{agents_table}
    """
    try:
        df = execute_query(config, query)
        if df.empty:
            return {}
        row = df.iloc[0]
        return {
            "total": int(row.get("total") or 0),
            "with_knowledge": int(row.get("with_knowledge") or 0),
            "without_knowledge": int(row.get("without_knowledge") or 0),
        }
    except Exception:
        return {}


def _get_workflow_counts(config: ConnectionConfig, prefix: str) -> dict:
    """Return Langflow workflow usage counts.

    NOTE: the authoritative list of provisioned Langflow flows lives in a separate
    Langflow DB that this tool does not connect to. Every count below is derived
    purely from the source DB, using the langflow permissions table as a proxy for
    "all known workflows" -- so `granted` is flows that appear in permission grants,
    not the true provisioned total.

    - granted:      distinct flows that appear in the langflow permissions table
    - agents_using: non-deleted workflow agents that reference >=1 flow id
    - in_use:       distinct flows actually referenced by a live agent
    - dead:         granted flows referenced by no live agent (dead weight)
    - orphans:      flows referenced by live agents but absent from the perms table
                    (proves the perms table is not a complete universe; because of
                     these, in_use + dead = granted + orphans)

    On failure returns {"error": <message>} so the caller can distinguish a real
    zero from a query/schema problem.
    """
    names = get_all_table_names(prefix)
    agents_table = names.get("agents", "playground_bot_generator_config")
    perms_table = names.get("langflow_user_permissions", f"{prefix}_langflow_user_permissions")
    query = f"""
        WITH all_flows AS (
            SELECT DISTINCT (p->>'flowId') AS flow_id
            FROM public.{perms_table},
                 jsonb_array_elements(flow_permissions) AS p
            WHERE NULLIF(p->>'flowId', '') IS NOT NULL
        ),
        referenced AS (
            SELECT DISTINCT (f->>'id') AS flow_id, c.bot_id
            FROM public.{agents_table} c,
                 jsonb_array_elements(c.character_prompts->'agentFlow'->'flows') AS f
            WHERE lower(trim(c.character_prompts->>'model')) = 'workflow'
              AND c.deleted_at IS NULL
              AND NULLIF(f->>'id', '') IS NOT NULL
        ),
        referenced_flows AS (
            SELECT DISTINCT flow_id FROM referenced
        )
        SELECT
            (SELECT COUNT(*) FROM all_flows)                                AS granted,
            (SELECT COUNT(DISTINCT bot_id) FROM referenced)                 AS agents_using,
            (SELECT COUNT(*) FROM referenced_flows)                         AS in_use,
            (SELECT COUNT(*) FROM all_flows a
                 WHERE NOT EXISTS (
                     SELECT 1 FROM referenced_flows r WHERE r.flow_id = a.flow_id
                 ))                                                         AS dead,
            (SELECT COUNT(*) FROM referenced_flows r
                 WHERE NOT EXISTS (
                     SELECT 1 FROM all_flows a WHERE a.flow_id = r.flow_id
                 ))                                                         AS orphans
    """
    try:
        df = execute_query(config, query)
        if df.empty:
            return {}
        row = df.iloc[0]
        return {
            "granted": int(row.get("granted") or 0),
            "agents_using": int(row.get("agents_using") or 0),
            "in_use": int(row.get("in_use") or 0),
            "dead": int(row.get("dead") or 0),
            "orphans": int(row.get("orphans") or 0),
        }
    except Exception as e:
        return {"error": str(e)}


# Page config
st.set_page_config(page_title="Connect to Source DB", page_icon="🔌", layout="wide")
st.title("🔌 Connect to Source Database")

# Get the base directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKUP_DIR = os.path.join(BASE_DIR, "backups")


@st.cache_data
def load_defaults():
    """Load connection defaults from the project .env file."""
    env_path = os.path.join(BASE_DIR, ".env")
    if os.path.exists(env_path):
        config = dotenv_values(env_path)
        return {
            "host": config.get("SOURCE_DB_HOST", "localhost"),
            "port": int(config.get("SOURCE_DB_PORT", "5432")),
            "database": config.get("SOURCE_DB_DATABASE", ""),
            "username": config.get("SOURCE_DB_USERNAME", ""),
            "password": config.get("SOURCE_DB_PASSWORD", ""),
            "prefix": config.get("TABLE_PREFIX", "jeen_dev"),
        }
    
    # Fallback defaults when .env is missing
    return {
        "host": "localhost",
        "port": 5432,
        "database": "",
        "username": "",
        "password": "",
        "prefix": "jeen_dev",
    }


# Load defaults once
DEFAULTS = load_defaults()


def render_connection_form():
    """Render the database connection form."""
    st.subheader("Connection Details")
    current_prefix = (
        st.session_state[SessionKeys.TABLE_PREFIX]
        if SessionKeys.TABLE_PREFIX in st.session_state
        else DEFAULTS["prefix"]
    )
    
    with st.form("source_connection_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            host = st.text_input("Host", value=DEFAULTS["host"], placeholder="localhost")
            database = st.text_input("Database", value=DEFAULTS["database"], placeholder="my_database")
            username = st.text_input("Username", value=DEFAULTS["username"], placeholder="postgres")
        
        with col2:
            port = st.number_input("Port", value=int(DEFAULTS["port"]), min_value=1, max_value=65535)
            password = st.text_input("Password", type="password", value=DEFAULTS["password"], placeholder="••••••••")
            table_prefix = st.text_input(
                "Table Prefix",
                value=current_prefix,
                placeholder="jeen_dev",
                help="Prefix for table names (e.g., 'jeen_dev' for 'jeen_dev_users'). Leave empty for unprefixed tables.",
            )
        
        submitted = st.form_submit_button("🔗 Test Connection", type="primary", use_container_width=True)
        
        if submitted:
            if not all([host, database, username, password]):
                st.error("Please fill in all required fields.")
                return
            
            config = ConnectionConfig(
                host=host,
                port=port,
                database=database,
                username=username,
                password=password
            )
            
            with st.spinner("Testing connection..."):
                success, message = test_connection(config)
            
            if success:
                st.success(f"✅ {message}")
                
                conn_dict = config.to_dict()
                st.session_state[SessionKeys.SOURCE_CONNECTION] = conn_dict
                st.session_state[SessionKeys.TABLE_PREFIX] = table_prefix
                st.session_state["source_config"] = config
                
                save_connection("source", conn_dict)
                save_to_storage("table_prefix", table_prefix)
                
                st.rerun()
            else:
                st.error(f"❌ {message}")


def render_table_verification():
    """Render the table existence verification section."""
    if SessionKeys.SOURCE_CONNECTION not in st.session_state:
        return
    
    st.markdown("---")
    st.subheader("📋 Table Verification")
    
    conn_dict = st.session_state[SessionKeys.SOURCE_CONNECTION]
    prefix = st.session_state.get(SessionKeys.TABLE_PREFIX, "jeen_dev")
    
    if "source_config" not in st.session_state:
        st.warning("Please enter your password and test the connection to verify tables.")
        password = st.text_input("Enter password to verify tables:", type="password", key="verify_pwd")
        if st.button("Verify Tables"):
            config = ConnectionConfig(
                host=conn_dict["host"],
                port=conn_dict["port"],
                database=conn_dict["database"],
                username=conn_dict["username"],
                password=password
            )
            st.session_state["source_config"] = config
            st.rerun()
        return
    
    config = st.session_state["source_config"]
    
    with st.spinner("Checking tables..."):
        table_status = check_tables_exist(config, prefix)
    
    if not table_status:
        st.error("Failed to check tables. Please verify your connection.")
        return
    
    st.session_state[SessionKeys.RESOLVED_TABLES] = table_status
    st.markdown(f"**Resolved table names for prefix `{prefix}`:**")
    
    cols = st.columns(3)
    for i, (logical_name, info) in enumerate(table_status.items()):
        with cols[i % 3]:
            if info["exists"]:
                count = get_table_row_count(config, info["actual_name"])
                st.success(f"**{logical_name}**  \n`{info['actual_name']}`  \n{count:,} rows")
            else:
                st.error(f"**{logical_name}**  \n`{info['actual_name']}`  \n❌ Not found")
    
    existing_count = sum(1 for info in table_status.values() if info["exists"])
    total_count = len(table_status)
    
    if existing_count == total_count:
        st.success(f"✅ All {total_count} tables found!")
    else:
        st.warning(f"⚠️ {existing_count}/{total_count} tables found. Some tables may be missing.")


def render_audit_section():
    """Render the pre-migration audit section."""
    if SessionKeys.SOURCE_CONNECTION not in st.session_state or "source_config" not in st.session_state:
        return
    
    st.markdown("---")
    st.subheader("🔍 Pre-Migration Audit")
    st.info("Run this audit to identify potential data issues before migration. Results help you understand data quality and estimate migration risks.")
    
    config = st.session_state["source_config"]
    prefix = st.session_state.get(SessionKeys.TABLE_PREFIX, "jeen_dev")
    table_status = st.session_state.get(SessionKeys.RESOLVED_TABLES, {})
    
    # Section 1: Overall Counts Summary from Table Verification
    if table_status:
        st.markdown("### 📊 Section 1: Overall Counts")
        # Build summary data
        summary_items = []
        for logical_name, info in table_status.items():
            if info["exists"]:
                count = get_table_row_count(config, info["actual_name"])
                item = {"table": logical_name, "count": count}
                if logical_name == "agents":
                    item["knowledge_counts"] = _get_agent_knowledge_counts(config, prefix)
                summary_items.append(item)
        
        if summary_items:
            st.session_state["audit_counts"] = summary_items
            # Create a single row with all KPIs
            cols = st.columns(len(summary_items))
            for i, item in enumerate(summary_items):
                with cols[i]:
                    if item["table"] == "agents" and item.get("knowledge_counts"):
                        counts = item["knowledge_counts"]
                        st.markdown(
                            f"**{item['table']}**<br>"
                            f"**{item['count']:,}**<br>"
                            f"<small>With attached documents: **{counts['with_knowledge']:,}**<br>"
                            f"Without attached documents: **{counts['without_knowledge']:,}**</small>",
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown(f"**{item['table']}**<br>**{item['count']:,}**", unsafe_allow_html=True)

        # Workflow (Langflow) usage metrics
        wf = _get_workflow_counts(config, prefix)
        if wf:
            st.session_state["audit_workflow_counts"] = wf
            st.markdown("**🔀 Workflows (Langflow)**")
            if wf.get("error"):
                st.warning(
                    "Could not compute workflow stats (the langflow permissions "
                    f"table may be missing or have an unexpected shape): {wf['error']}"
                )
            else:
                wf_cols = st.columns(5)
                with wf_cols[0]:
                    st.metric("Workflows granted (perms)", f"{wf['granted']:,}")
                with wf_cols[1]:
                    st.metric("Agents using workflows", f"{wf['agents_using']:,}")
                with wf_cols[2]:
                    st.metric("Workflows in use", f"{wf['in_use']:,}")
                with wf_cols[3]:
                    st.metric("Unused workflows (dead)", f"{wf['dead']:,}")
                with wf_cols[4]:
                    st.metric("Orphan flows (not granted)", f"{wf['orphans']:,}")
                st.caption(
                    "Granted = distinct flows that appear in the langflow permissions "
                    "table (a proxy — the authoritative flow list lives in a separate "
                    "Langflow DB this tool does not query). Dead = granted flows no live "
                    "agent references. Orphans = flows live agents reference that are NOT "
                    "in the perms table. Reconciliation: in_use + dead = granted + orphans."
                )

    # Calculate button (secondary style - not red/green)
    if st.button("📊 Calculate Audit Statistics", type="secondary", use_container_width=True):
        with st.spinner("Running audit queries... This may take a few minutes for large databases."):
            try:
                results = run_full_audit(config, prefix)
                st.session_state["audit_results"] = results
            except Exception as e:
                st.error(f"Audit failed: {str(e)}")
                return
    
    # Display results if available
    if "audit_results" not in st.session_state:
        return
    
    results = st.session_state["audit_results"]

    # ── PDF Export ──────────────────────────────────────────────────────────
    _gap_col, _pdf_col = st.columns([3, 1])
    with _pdf_col:
        if st.button("📄 Export to PDF", use_container_width=True):
            with st.spinner("Generating PDF report..."):
                try:
                    pdf_bytes = generate_audit_pdf(
                        results,
                        prefix,
                        f"{config.host}/{config.database}",
                        st.session_state.get("audit_counts", []),
                    )
                    st.session_state["audit_pdf_bytes"] = pdf_bytes
                except Exception as pdf_err:
                    st.error(f"PDF generation failed: {pdf_err}")
        if "audit_pdf_bytes" in st.session_state:
            st.download_button(
                label="📥 Download PDF",
                data=st.session_state["audit_pdf_bytes"],
                file_name=f"audit_{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                mime="application/pdf",
                use_container_width=True,
            )

    # Section 2: Users
    st.markdown("### 👥 Section 2: User Analytics")
    with st.expander("View User Analytics", expanded=False):
        if 'error' in results.get('users', {}):
            st.error(f"Error: {results['users']['error']}")
        else:
            users = results.get('users', {})
            
            st.markdown("**Top 10 Users by Chat Activity**")
            if not users.get('top_by_logs', pd.DataFrame()).empty:
                st.dataframe(_fmt_df(users['top_by_logs']), use_container_width=True, hide_index=True)
            else:
                st.info("No data")
            
            st.markdown("**Top 10 Users by Documents**")
            if not users.get('top_by_documents', pd.DataFrame()).empty:
                st.dataframe(_fmt_df(users['top_by_documents']), use_container_width=True, hide_index=True)
            else:
                st.info("No data")
            
            st.markdown("**Top 10 Users by Chunks**")
            if not users.get('top_by_chunks', pd.DataFrame()).empty:
                st.dataframe(_fmt_df(users['top_by_chunks']), use_container_width=True, hide_index=True)
            else:
                st.info("No data")
            
            st.markdown("**⚠️ Users Without Email (will be SKIPPED)**")
            without_email = users.get('without_email', pd.DataFrame())
            if not without_email.empty:
                st.warning(f"{len(without_email)} users have no email and will be skipped!")
                st.dataframe(_fmt_df(without_email), use_container_width=True, hide_index=True)
            else:
                st.success("All users have email addresses ✓")
            
            st.markdown("**⚠️ Potential Username Collisions**")
            collisions = users.get('username_collisions', pd.DataFrame())
            if not collisions.empty:
                st.warning(f"{len(collisions)} email prefixes are shared across multiple users")
                st.dataframe(_fmt_df(collisions), use_container_width=True, hide_index=True)
            else:
                st.success("No username collisions detected ✓")
    
    # Section 3: Folders
    st.markdown("### 📁 Section 3: Folder Analytics")
    with st.expander("View Folder Analytics", expanded=False):
        if 'error' in results.get('folders', {}):
            st.error(f"Error: {results['folders']['error']}")
        else:
            folders = results.get('folders', {})
            
            st.markdown("**Folder Hierarchy Depth**")
            depth_df = folders.get('hierarchy_depth', pd.DataFrame())
            if not depth_df.empty:
                st.dataframe(_fmt_df(depth_df), use_container_width=True, hide_index=True)
                max_depth = depth_df['depth'].max() if 'depth' in depth_df.columns else 0
                if max_depth > 1:
                    st.warning(f"⚠️ Max depth is {max_depth}. Folders at depth > 1 will have parent_id set based on hierarchy.")
            else:
                st.info("No folders found")
            
            st.markdown("**Folder Type Distribution**")
            if not folders.get('type_distribution', pd.DataFrame()).empty:
                st.dataframe(_fmt_df(folders['type_distribution']), use_container_width=True, hide_index=True)
            
            st.markdown("**⚠️ Orphaned Folders (parent references non-existent folder)**")
            orphaned = folders.get('orphaned', pd.DataFrame())
            if not orphaned.empty:
                st.warning(f"{len(orphaned)} folders have orphaned parent references!")
                st.dataframe(_fmt_df(orphaned), use_container_width=True, hide_index=True)
            else:
                st.success("No orphaned folders ✓")
    
    # Section 4: Documents
    st.markdown("### 📄 Section 4: Document Analytics")
    with st.expander("View Document Analytics", expanded=False):
        if 'error' in results.get('documents', {}):
            st.error(f"Error: {results['documents']['error']}")
        else:
            docs = results.get('documents', {})
            
            st.markdown("**Document Type Distribution**")
            if not docs.get('type_distribution', pd.DataFrame()).empty:
                st.dataframe(_fmt_df(docs['type_distribution']), use_container_width=True, hide_index=True)
            
            st.markdown("**⚠️ Problematic Doc Types (will become application/octet-stream)**")
            problematic = docs.get('problematic_types', pd.DataFrame())
            if not problematic.empty:
                st.warning(f"{len(problematic)} document types will need manual mapping")
                st.dataframe(_fmt_df(problematic), use_container_width=True, hide_index=True)
            else:
                st.success("All document types are recognized ✓")
            
            st.markdown("**Blob Source Distribution**")
            if not docs.get('blob_source_distribution', pd.DataFrame()).empty:
                st.dataframe(_fmt_df(docs['blob_source_distribution']), use_container_width=True, hide_index=True)
            
            col1, col2 = st.columns(2)
            with col1:
                orphaned_count = docs.get('orphaned_count', 0)
                if orphaned_count > 0:
                    st.error(f"⚠️ {orphaned_count:,} documents without valid owner")
                else:
                    st.success("All documents have valid owners ✓")
            with col2:
                missing_folders = docs.get('missing_folders_count', 0)
                if missing_folders > 0:
                    st.warning(f"⚠️ {missing_folders:,} documents reference missing folders")
                else:
                    st.success("All folder references valid ✓")
            
            st.markdown("**Duplicate doc_id Values**")
            duplicates = docs.get('duplicate_ids', pd.DataFrame())
            if not duplicates.empty:
                st.error(f"⚠️ {len(duplicates)} duplicate doc_ids found!")
                st.dataframe(_fmt_df(duplicates), use_container_width=True, hide_index=True)
            else:
                st.success("No duplicate doc_ids ✓")
    
    # Section 5: Chunks & Embeddings
    st.markdown("### 🧮 Section 5: Chunks & Embeddings Analytics")
    with st.expander("View Chunks & Embeddings Analytics", expanded=False):
        if 'error' in results.get('chunks_embeddings', {}):
            st.error(f"Error: {results['chunks_embeddings']['error']}")
        else:
            chunks = results.get('chunks_embeddings', {})
            
            st.markdown("**Top Documents by Chunk Count**")
            if not chunks.get('per_document', pd.DataFrame()).empty:
                st.dataframe(_fmt_df(chunks['per_document']), use_container_width=True, hide_index=True)
            
            st.markdown("**Chunk Type Distribution**")
            if not chunks.get('type_distribution', pd.DataFrame()).empty:
                st.dataframe(_fmt_df(chunks['type_distribution']), use_container_width=True, hide_index=True)
            
            st.markdown("**Embedding Vector Dimensions**")
            if not chunks.get('dimensions', pd.DataFrame()).empty:
                st.dataframe(_fmt_df(chunks['dimensions']), use_container_width=True, hide_index=True)
            
            st.markdown("**Embeddings by Model**")
            if not chunks.get('by_model', pd.DataFrame()).empty:
                st.dataframe(_fmt_df(chunks['by_model']), use_container_width=True, hide_index=True)
            
            orphaned = chunks.get('orphaned', {})
            if orphaned.get('orphaned_chunks', 0) > 0:
                st.warning(f"⚠️ {orphaned['orphaned_chunks']:,} chunks reference non-existent documents ({orphaned['orphaned_doc_ids']} unique doc_ids)")
            else:
                st.success("All chunks have valid document references ✓")
            
            without_emb = chunks.get('without_embeddings', 0)
            if without_emb > 0:
                st.info(f"ℹ️ {without_emb:,} chunks have NULL embeddings")
    
    # Section 6: Conversations
    st.markdown("### 💬 Section 6: Conversation Analytics")
    with st.expander("View Conversation Analytics", expanded=False):
        if 'error' in results.get('conversations', {}):
            st.error(f"Error: {results['conversations']['error']}")
        else:
            convs = results.get('conversations', {})
            
            st.markdown("**Top 10 Users by Conversations**")
            if not convs.get('top_users', pd.DataFrame()).empty:
                st.dataframe(_fmt_df(convs['top_users']), use_container_width=True, hide_index=True)
            
            st.markdown("**Conversation Size Distribution**")
            if not convs.get('size_distribution', pd.DataFrame()).empty:
                st.dataframe(_fmt_df(convs['size_distribution']), use_container_width=True, hide_index=True)
            
            st.markdown("**Model Usage Distribution**")
            if not convs.get('model_usage', pd.DataFrame()).empty:
                st.dataframe(_fmt_df(convs['model_usage']), use_container_width=True, hide_index=True)
            
            st.markdown("**Bot/Agent Usage**")
            if not convs.get('bot_usage', pd.DataFrame()).empty:
                st.dataframe(_fmt_df(convs['bot_usage']), use_container_width=True, hide_index=True)
            
            st.markdown("**Token Statistics**")
            if not convs.get('token_stats', pd.DataFrame()).empty:
                st.dataframe(_fmt_df(convs['token_stats']), use_container_width=True, hide_index=True)
            
            # Issues
            without_user = convs.get('without_user', {})
            if without_user.get('logs_without_user', 0) > 0:
                st.warning(f"⚠️ {without_user['logs_without_user']:,} logs have NULL user_id ({without_user['conversations_affected']:,} conversations affected)")
            
            without_chat = convs.get('without_chat_id', 0)
            if without_chat > 0:
                st.warning(f"⚠️ {without_chat:,} logs have NULL/empty chat_id")
            
            invalid_uuids = convs.get('invalid_chat_ids', pd.DataFrame())
            if not invalid_uuids.empty:
                st.warning(f"⚠️ {len(invalid_uuids)} chat_ids have invalid UUID format")
                st.dataframe(_fmt_df(invalid_uuids), use_container_width=True, hide_index=True)
            
            question_issues = convs.get('question_extraction_issues', pd.DataFrame())
            if not question_issues.empty:
                st.warning(f"⚠️ {len(question_issues)} logs have question extraction issues")
                with st.expander("View question extraction issues"):
                    st.dataframe(_fmt_df(question_issues), use_container_width=True, hide_index=True)
            
            orphaned = convs.get('orphaned', {})
            if orphaned.get('orphaned_logs', 0) > 0:
                st.error(f"⚠️ {orphaned['orphaned_logs']:,} logs reference non-existent users ({orphaned['orphaned_user_ids']} unique user_ids)")
    
    # Section 7: Cross-Table Integrity (Most Critical)
    st.markdown("### ⚠️ Section 7: Cross-Table Integrity (DATA LOSS RISK)")
    with st.expander("View Data Loss Risk Analysis", expanded=True):
        if 'error' in results.get('cross_table', {}):
            st.error(f"Error: {results['cross_table']['error']}")
        else:
            cross = results.get('cross_table', {})
            
            st.markdown("**🚨 Data Loss Risk Summary**")
            st.caption("Rows that will be SKIPPED due to missing foreign key references")
            
            risk_df = cross.get('data_loss_risk', pd.DataFrame())
            if not risk_df.empty:
                # Highlight rows with risk > 0
                total_risk = risk_df['rows_at_risk'].sum() if 'rows_at_risk' in risk_df.columns else 0
                if total_risk > 0:
                    st.error(f"⚠️ Total rows at risk: {total_risk:,}")
                else:
                    st.success("No data loss risk detected ✓")
                st.dataframe(_fmt_df(risk_df), use_container_width=True, hide_index=True)
            
            st.markdown("**Missing User References by Table**")
            missing_users = cross.get('missing_users', pd.DataFrame())
            if not missing_users.empty:
                st.dataframe(_fmt_df(missing_users), use_container_width=True, hide_index=True)
            else:
                st.success("All user references are valid ✓")


def render_backup_section():
    """Render the pg_dump backup section."""
    if SessionKeys.SOURCE_CONNECTION not in st.session_state or "source_config" not in st.session_state:
        return
    
    st.markdown("---")
    st.subheader("💾 Backup Source Database (optional)")
    
    config = st.session_state["source_config"]
    prefix = st.session_state.get(SessionKeys.TABLE_PREFIX, "jeen_dev")
    table_status = st.session_state.get(SessionKeys.RESOLVED_TABLES, {})
    
    backup_type = st.radio("Backup Type", ["Full Database", "Selected Tables Only"], horizontal=True)
    
    tables_to_backup = None
    if backup_type == "Selected Tables Only":
        existing_tables = [info["actual_name"] for info in table_status.values() if info["exists"]]
        if existing_tables:
            tables_to_backup = st.multiselect("Select tables to backup", options=existing_tables, default=existing_tables)
        else:
            st.warning("No tables found to backup.")
            return
    
    compress = st.checkbox("Compress backup (gzip)", value=True)
    
    if st.button("🗄️ Create Backup", type="secondary"):
        os.makedirs(BACKUP_DIR, exist_ok=True)
        
        with st.spinner("Creating backup... This may take a while for large databases."):
            success, message, output_path = run_pg_dump(config, BACKUP_DIR, tables=tables_to_backup, compress=compress)
        
        if success:
            st.success(f"✅ {message}")
            if output_path and os.path.exists(output_path):
                with open(output_path, "rb") as f:
                    st.download_button(
                        label="📥 Download Backup",
                        data=f,
                        file_name=os.path.basename(output_path),
                        mime="application/gzip" if compress else "application/sql"
                    )
        else:
            st.error(f"❌ {message}")
    
    if os.path.exists(BACKUP_DIR):
        backups = [f for f in os.listdir(BACKUP_DIR) if f.endswith(('.sql', '.sql.gz'))]
        if backups:
            with st.expander("📁 Existing Backups"):
                for backup in sorted(backups, reverse=True)[:10]:
                    backup_path = os.path.join(BACKUP_DIR, backup)
                    size_mb = os.path.getsize(backup_path) / (1024 * 1024)
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.text(f"{backup} ({size_mb:.2f} MB)")
                    with col2:
                        with open(backup_path, "rb") as f:
                            st.download_button(label="📥", data=f, file_name=backup, key=f"dl_{backup}")


def main():
    """Main page function."""
    render_connection_form()
    render_table_verification()
    render_audit_section()
    render_backup_section()
    
    if SessionKeys.RESOLVED_TABLES in st.session_state:
        st.markdown("---")
        st.info("👉 **Next Step:** Go to **Select Data** page to choose users and documents to migrate.")


# Run main
main()
