"""
Page 4: Target Configuration & Load

Features:
- Target database connection
- Schema/database mode selection
- Load mode per table (truncate, upsert)
- Dry-run toggle
- Load execution with progress
"""
import os
import streamlit as st
import pandas as pd

from utils.db import ConnectionConfig, test_connection
from utils.storage import save_connection, load_connection
from utils.config import SessionKeys
from utils.loader import DataLoader, get_target_table_info, TARGET_TABLES, LOAD_ORDER

# Page config
st.set_page_config(page_title="Target", page_icon="🎯", layout="wide")
st.title("🎯 Target Configuration & Load")

# Directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRANSFORM_DIR = os.path.join(BASE_DIR, "output", "transform")


def init_session_state():
    """Initialize session state from localStorage."""
    if "target_form_loaded" not in st.session_state:
        st.session_state.target_form_loaded = True
        
        # Try to load from localStorage
        saved_conn = load_connection("target")
        if saved_conn:
            st.session_state[SessionKeys.TARGET_CONNECTION] = saved_conn


def render_target_connection():
    """Render target database connection form."""
    st.subheader("🔌 Target Database Connection")
    
    # Get saved values
    saved_conn = st.session_state.get(SessionKeys.TARGET_CONNECTION, {})
    
    with st.form("target_connection_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            host = st.text_input(
                "Host",
                value=saved_conn.get("host", "localhost"),
                placeholder="localhost"
            )
            database = st.text_input(
                "Database",
                value=saved_conn.get("database", ""),
                placeholder="target_database"
            )
            username = st.text_input(
                "Username",
                value=saved_conn.get("username", ""),
                placeholder="postgres"
            )
        
        with col2:
            port = st.number_input(
                "Port",
                value=int(saved_conn.get("port", 5432)),
                min_value=1,
                max_value=65535
            )
            password = st.text_input(
                "Password",
                type="password",
                placeholder="••••••••"
            )
            schema_mode = st.selectbox(
                "Target Structure",
                options=["schemas", "databases"],
                index=0 if saved_conn.get("schema_mode", "schemas") == "schemas" else 1,
                help="'schemas' = user_db, document_db, completion_db as schemas in one database. 'databases' = separate databases."
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
                
                # Save to session state
                conn_dict = config.to_dict()
                conn_dict["schema_mode"] = schema_mode
                st.session_state[SessionKeys.TARGET_CONNECTION] = conn_dict
                st.session_state["target_config"] = config
                st.session_state["target_schema_mode"] = schema_mode
                
                # Save to localStorage (without password)
                save_connection("target", conn_dict)
                
                st.rerun()
            else:
                st.error(f"❌ {message}")


def render_target_tables_status():
    """Render target tables status."""
    if "target_config" not in st.session_state:
        return
    
    st.markdown("---")
    st.subheader("📋 Target Tables Status")
    
    config = st.session_state["target_config"]
    schema_mode = st.session_state.get("target_schema_mode", "schemas")
    
    with st.spinner("Checking target tables..."):
        table_info = get_target_table_info(config, schema_mode)
    
    if not table_info:
        st.warning("Could not retrieve target table information.")
        return
    
    # Display table status
    status_data = []
    for name in LOAD_ORDER:
        info = table_info.get(name, {})
        status_data.append({
            "Table": name,
            "Full Name": info.get("full_name", "N/A"),
            "Exists": "✅" if info.get("exists") else "❌",
            "Rows": info.get("row_count", 0) if info.get("exists") else "-",
            "Columns": len(info.get("columns", [])) if info.get("exists") else "-",
        })
    
    st.dataframe(pd.DataFrame(status_data), hide_index=True, use_container_width=True)
    
    # Schema inspector
    with st.expander("🔍 Schema Inspector"):
        for name in LOAD_ORDER:
            info = table_info.get(name, {})
            if info.get("exists") and info.get("columns"):
                st.markdown(f"**{info.get('full_name')}**")
                cols_df = pd.DataFrame(info["columns"])
                st.dataframe(cols_df, hide_index=True, use_container_width=True)


def render_load_configuration():
    """Render load configuration section."""
    if "target_config" not in st.session_state:
        return
    
    # Check if transformation has been done
    transformed_data = st.session_state.get(SessionKeys.TRANSFORMED_DATA)
    if not transformed_data:
        st.warning("⚠️ No transformed data found. Please run transformation first on the Transform page.")
        return
    
    st.markdown("---")
    st.subheader("⚙️ Load Configuration")
    
    st.info(f"Using transformation from: {transformed_data.get('timestamp', 'N/A')}")
    
    # Initialize load modes
    if "load_modes" not in st.session_state:
        st.session_state.load_modes = {name: "truncate" for name in LOAD_ORDER}
    
    # Toggles
    col1, col2 = st.columns(2)
    with col1:
        dry_run = st.toggle("🔍 Dry Run Mode", value=True, help="Preview SQL without executing")
    with col2:
        strict_mode = st.toggle("⚠️ Strict Mode", value=True, help="Stop on first error")
    
    st.session_state["load_dry_run"] = dry_run
    st.session_state["load_strict_mode"] = strict_mode
    
    # Per-table configuration
    st.markdown("**Load Mode per Table:**")
    
    config_data = []
    for name in LOAD_ORDER:
        transformed_count = transformed_data.get("summary", {}).get(name, 0)
        config_data.append({
            "table": name,
            "rows": transformed_count,
            "mode": st.session_state.load_modes.get(name, "truncate"),
        })
    
    # Create editable config
    edited_config = st.data_editor(
        pd.DataFrame(config_data),
        column_config={
            "table": st.column_config.TextColumn("Table", disabled=True),
            "rows": st.column_config.NumberColumn("Rows to Load", disabled=True),
            "mode": st.column_config.SelectboxColumn(
                "Load Mode",
                options=["truncate", "upsert"],
                help="truncate = DELETE all then INSERT. upsert = INSERT ... ON CONFLICT UPDATE"
            ),
        },
        hide_index=True,
        use_container_width=True,
        key="load_config_editor"
    )
    
    # Update load modes from edited config
    for _, row in edited_config.iterrows():
        st.session_state.load_modes[row["table"]] = row["mode"]


def render_load_execution():
    """Render load execution section."""
    if "target_config" not in st.session_state:
        return
    
    transformed_data = st.session_state.get(SessionKeys.TRANSFORMED_DATA)
    if not transformed_data:
        return
    
    st.markdown("---")
    st.subheader("🚀 Execute Load")
    
    dry_run = st.session_state.get("load_dry_run", True)
    strict_mode = st.session_state.get("load_strict_mode", True)
    load_modes = st.session_state.get("load_modes", {})
    
    if dry_run:
        button_label = "🔍 Preview Load (Dry Run)"
        button_type = "secondary"
    else:
        button_label = "⚡ Execute Load"
        button_type = "primary"
    
    if st.button(button_label, type=button_type, use_container_width=True):
        # Create progress containers
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def progress_callback(table_name: str, current: int, total: int, status: str):
            progress_bar.progress(current / total)
            status_text.text(f"Loading {table_name}... ({current}/{total}) - {status}")
        
        # Create loader
        loader = DataLoader(
            config=st.session_state["target_config"],
            input_dir=TRANSFORM_DIR,
            schema_mode=st.session_state.get("target_schema_mode", "schemas"),
            progress_callback=progress_callback
        )
        
        # Execute load
        with st.spinner("Loading data..."):
            results = loader.load_all(
                load_modes=load_modes,
                dry_run=dry_run,
                strict_mode=strict_mode
            )
        
        progress_bar.progress(1.0)
        status_text.text("Load complete!")
        
        # Show results
        if dry_run:
            st.info("🔍 **Dry Run Results** - No data was actually loaded")
        else:
            if results["errors"]:
                st.error("⚠️ Some errors occurred during load")
            else:
                st.success("✅ Load completed successfully!")
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Tables Succeeded", results["summary"]["tables_succeeded"])
        with col2:
            st.metric("Tables Failed", results["summary"]["tables_failed"])
        with col3:
            st.metric("Rows Loaded", results["summary"]["total_loaded"])
        with col4:
            st.metric("Rows Failed", results["summary"]["total_failed"])
        
        # Per-table results
        st.markdown("**Per-Table Results:**")
        table_results = []
        for name in LOAD_ORDER:
            result = results["tables"].get(name, {})
            table_results.append({
                "Table": name,
                "Status": result.get("status", "N/A"),
                "Rows Loaded": result.get("rows_loaded", 0),
                "Rows Failed": result.get("rows_failed", 0),
                "Error": result.get("error", "") or "",
            })
        
        st.dataframe(pd.DataFrame(table_results), hide_index=True, use_container_width=True)
        
        # SQL Preview (for dry run)
        if dry_run:
            st.markdown("**SQL Preview:**")
            for name in LOAD_ORDER:
                result = results["tables"].get(name, {})
                if result.get("sql_preview"):
                    with st.expander(f"📄 {name}"):
                        st.code(result["sql_preview"], language="sql")
        
        # Errors
        if results["errors"]:
            st.markdown("**Errors:**")
            for error in results["errors"]:
                st.error(error)


def main():
    """Main page function."""
    init_session_state()
    
    # Target connection
    render_target_connection()
    
    # Target tables status
    render_target_tables_status()
    
    # Load configuration
    render_load_configuration()
    
    # Load execution
    render_load_execution()
    
    # Next step hint
    st.markdown("---")
    st.info("👉 **Next Step:** Go to **Run** page to execute the full migration pipeline with validation.")


if __name__ == "__main__":
    main()
