"""
Page 1: Connect to Source Database.
Defaults are loaded from the project .env file.
"""
import os
import streamlit as st
from dotenv import dotenv_values

from utils.db import (
    ConnectionConfig, 
    test_connection, 
    check_tables_exist, 
    run_pg_dump,
    get_table_row_count
)
from utils.storage import save_connection, save_to_storage
from utils.config import SessionKeys, get_all_table_names
from utils.audit import run_full_audit
import pandas as pd

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
    
    with st.form("source_connection_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            host = st.text_input("Host", value=DEFAULTS["host"], placeholder="localhost")
            database = st.text_input("Database", value=DEFAULTS["database"], placeholder="my_database")
            username = st.text_input("Username", value=DEFAULTS["username"], placeholder="postgres")
        
        with col2:
            port = st.number_input("Port", value=int(DEFAULTS["port"]), min_value=1, max_value=65535)
            password = st.text_input("Password", type="password", value=DEFAULTS["password"], placeholder="••••••••")
            table_prefix = st.text_input("Table Prefix", value=DEFAULTS["prefix"], placeholder="jeen_dev",
                                        help="Prefix for table names (e.g., 'jeen_dev' for 'jeen_dev_users')")
        
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
                summary_items.append({"table": logical_name, "count": count})
        
        if summary_items:
            # Create a single row with all KPIs
            cols = st.columns(len(summary_items))
            for i, item in enumerate(summary_items):
                with cols[i]:
                    st.markdown(f"**{item['table']}**<br>**{item['count']:,}**", unsafe_allow_html=True)
    
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
    
    # Section 2: Users
    st.markdown("### 👥 Section 2: User Analytics")
    with st.expander("View User Analytics", expanded=False):
        if 'error' in results.get('users', {}):
            st.error(f"Error: {results['users']['error']}")
        else:
            users = results.get('users', {})
            
            st.markdown("**Top 10 Users by Chat Activity**")
            if not users.get('top_by_logs', pd.DataFrame()).empty:
                st.dataframe(users['top_by_logs'], use_container_width=True, hide_index=True)
            else:
                st.info("No data")
            
            st.markdown("**Top 10 Users by Documents**")
            if not users.get('top_by_documents', pd.DataFrame()).empty:
                st.dataframe(users['top_by_documents'], use_container_width=True, hide_index=True)
            else:
                st.info("No data")
            
            st.markdown("**Top 10 Users by Chunks**")
            if not users.get('top_by_chunks', pd.DataFrame()).empty:
                st.dataframe(users['top_by_chunks'], use_container_width=True, hide_index=True)
            else:
                st.info("No data")
            
            st.markdown("**⚠️ Users Without Email (will be SKIPPED)**")
            without_email = users.get('without_email', pd.DataFrame())
            if not without_email.empty:
                st.warning(f"{len(without_email)} users have no email and will be skipped!")
                st.dataframe(without_email, use_container_width=True, hide_index=True)
            else:
                st.success("All users have email addresses ✓")
            
            st.markdown("**⚠️ Potential Username Collisions**")
            collisions = users.get('username_collisions', pd.DataFrame())
            if not collisions.empty:
                st.warning(f"{len(collisions)} email prefixes are shared across multiple users")
                st.dataframe(collisions, use_container_width=True, hide_index=True)
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
                st.dataframe(depth_df, use_container_width=True, hide_index=True)
                max_depth = depth_df['depth'].max() if 'depth' in depth_df.columns else 0
                if max_depth > 1:
                    st.warning(f"⚠️ Max depth is {max_depth}. Folders at depth > 1 will have parent_id set based on hierarchy.")
            else:
                st.info("No folders found")
            
            st.markdown("**Folder Type Distribution**")
            if not folders.get('type_distribution', pd.DataFrame()).empty:
                st.dataframe(folders['type_distribution'], use_container_width=True, hide_index=True)
            
            st.markdown("**⚠️ Orphaned Folders (parent references non-existent folder)**")
            orphaned = folders.get('orphaned', pd.DataFrame())
            if not orphaned.empty:
                st.warning(f"{len(orphaned)} folders have orphaned parent references!")
                st.dataframe(orphaned, use_container_width=True, hide_index=True)
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
                st.dataframe(docs['type_distribution'], use_container_width=True, hide_index=True)
            
            st.markdown("**⚠️ Problematic Doc Types (will become application/octet-stream)**")
            problematic = docs.get('problematic_types', pd.DataFrame())
            if not problematic.empty:
                st.warning(f"{len(problematic)} document types will need manual mapping")
                st.dataframe(problematic, use_container_width=True, hide_index=True)
            else:
                st.success("All document types are recognized ✓")
            
            st.markdown("**Blob Source Distribution**")
            if not docs.get('blob_source_distribution', pd.DataFrame()).empty:
                st.dataframe(docs['blob_source_distribution'], use_container_width=True, hide_index=True)
            
            col1, col2 = st.columns(2)
            with col1:
                orphaned_count = docs.get('orphaned_count', 0)
                if orphaned_count > 0:
                    st.error(f"⚠️ {orphaned_count} documents without valid owner")
                else:
                    st.success("All documents have valid owners ✓")
            with col2:
                missing_folders = docs.get('missing_folders_count', 0)
                if missing_folders > 0:
                    st.warning(f"⚠️ {missing_folders} documents reference missing folders")
                else:
                    st.success("All folder references valid ✓")
            
            st.markdown("**Duplicate doc_id Values**")
            duplicates = docs.get('duplicate_ids', pd.DataFrame())
            if not duplicates.empty:
                st.error(f"⚠️ {len(duplicates)} duplicate doc_ids found!")
                st.dataframe(duplicates, use_container_width=True, hide_index=True)
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
                st.dataframe(chunks['per_document'], use_container_width=True, hide_index=True)
            
            st.markdown("**Chunk Type Distribution**")
            if not chunks.get('type_distribution', pd.DataFrame()).empty:
                st.dataframe(chunks['type_distribution'], use_container_width=True, hide_index=True)
            
            st.markdown("**Embedding Vector Dimensions**")
            if not chunks.get('dimensions', pd.DataFrame()).empty:
                st.dataframe(chunks['dimensions'], use_container_width=True, hide_index=True)
            
            orphaned = chunks.get('orphaned', {})
            if orphaned.get('orphaned_chunks', 0) > 0:
                st.warning(f"⚠️ {orphaned['orphaned_chunks']} chunks reference non-existent documents ({orphaned['orphaned_doc_ids']} unique doc_ids)")
            else:
                st.success("All chunks have valid document references ✓")
            
            without_emb = chunks.get('without_embeddings', 0)
            if without_emb > 0:
                st.info(f"ℹ️ {without_emb} chunks have NULL embeddings")
    
    # Section 6: Conversations
    st.markdown("### 💬 Section 6: Conversation Analytics")
    with st.expander("View Conversation Analytics", expanded=False):
        if 'error' in results.get('conversations', {}):
            st.error(f"Error: {results['conversations']['error']}")
        else:
            convs = results.get('conversations', {})
            
            st.markdown("**Top 10 Users by Conversations**")
            if not convs.get('top_users', pd.DataFrame()).empty:
                st.dataframe(convs['top_users'], use_container_width=True, hide_index=True)
            
            st.markdown("**Conversation Size Distribution**")
            if not convs.get('size_distribution', pd.DataFrame()).empty:
                st.dataframe(convs['size_distribution'], use_container_width=True, hide_index=True)
            
            st.markdown("**Model Usage Distribution**")
            if not convs.get('model_usage', pd.DataFrame()).empty:
                st.dataframe(convs['model_usage'], use_container_width=True, hide_index=True)
            
            st.markdown("**Bot/Agent Usage**")
            if not convs.get('bot_usage', pd.DataFrame()).empty:
                st.dataframe(convs['bot_usage'], use_container_width=True, hide_index=True)
            
            st.markdown("**Token Statistics**")
            if not convs.get('token_stats', pd.DataFrame()).empty:
                st.dataframe(convs['token_stats'], use_container_width=True, hide_index=True)
            
            # Issues
            without_user = convs.get('without_user', {})
            if without_user.get('logs_without_user', 0) > 0:
                st.warning(f"⚠️ {without_user['logs_without_user']} logs have NULL user_id ({without_user['conversations_affected']} conversations affected)")
            
            without_chat = convs.get('without_chat_id', 0)
            if without_chat > 0:
                st.warning(f"⚠️ {without_chat} logs have NULL/empty chat_id")
            
            invalid_uuids = convs.get('invalid_chat_ids', pd.DataFrame())
            if not invalid_uuids.empty:
                st.warning(f"⚠️ {len(invalid_uuids)} chat_ids have invalid UUID format")
                st.dataframe(invalid_uuids, use_container_width=True, hide_index=True)
            
            question_issues = convs.get('question_extraction_issues', pd.DataFrame())
            if not question_issues.empty:
                st.warning(f"⚠️ {len(question_issues)} logs have question extraction issues")
                with st.expander("View question extraction issues"):
                    st.dataframe(question_issues, use_container_width=True, hide_index=True)
            
            orphaned = convs.get('orphaned', {})
            if orphaned.get('orphaned_logs', 0) > 0:
                st.error(f"⚠️ {orphaned['orphaned_logs']} logs reference non-existent users ({orphaned['orphaned_user_ids']} unique user_ids)")
    
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
                st.dataframe(risk_df, use_container_width=True, hide_index=True)
            
            st.markdown("**Missing User References by Table**")
            missing_users = cross.get('missing_users', pd.DataFrame())
            if not missing_users.empty:
                st.dataframe(missing_users, use_container_width=True, hide_index=True)
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
