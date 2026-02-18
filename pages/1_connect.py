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
    render_backup_section()
    
    if SessionKeys.RESOLVED_TABLES in st.session_state:
        st.markdown("---")
        st.info("👉 **Next Step:** Go to **Select Data** page to choose users and documents to migrate.")


# Run main
main()
