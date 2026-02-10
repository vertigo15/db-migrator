"""
Page 3: Configure Transformations

Features:
- Visual mapping editor for V4 to V5 schema
- Flagged field highlighting
- Constant column addition
- Save/load YAML configs
- Transform execution with preview
"""
import os
import streamlit as st
import pandas as pd

from utils.config import SessionKeys, DEFAULT_MAPPINGS
from utils.storage import save_mapping_config as save_mapping_storage, load_mapping_config as load_mapping_storage
from utils.transformation import (
    TransformationEngine,
    get_default_mapping_config,
    save_mapping_config,
    load_mapping_config,
    mapping_to_dataframe,
    dataframe_to_mapping,
    get_flagged_fields
)

# Page config
st.set_page_config(page_title="Transform", page_icon="🔄", layout="wide")
st.title("🔄 Configure Transformations")

# Directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIGS_DIR = os.path.join(BASE_DIR, "configs")
EXTRACT_DIR = os.path.join(BASE_DIR, "output", "extract")
TRANSFORM_DIR = os.path.join(BASE_DIR, "output", "transform")

# Ensure directories exist
os.makedirs(CONFIGS_DIR, exist_ok=True)
os.makedirs(TRANSFORM_DIR, exist_ok=True)


def init_mapping_config():
    """Initialize mapping configuration in session state."""
    if SessionKeys.MAPPING_CONFIG not in st.session_state:
        # Try to load from localStorage first
        saved = load_mapping_storage()
        if saved:
            st.session_state[SessionKeys.MAPPING_CONFIG] = saved
        else:
            st.session_state[SessionKeys.MAPPING_CONFIG] = get_default_mapping_config()


def render_table_mapping(table_name: str, mapping: dict):
    """Render the mapping editor for a single table."""
    table_config = mapping.get(table_name, {})
    
    # Table header with metadata
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        st.markdown(f"**Source:** `{table_config.get('source_table', 'N/A')}`")
    with col2:
        st.markdown(f"**Target:** `{table_config.get('target_schema', '')}.{table_config.get('target_table', 'N/A')}`")
    with col3:
        if table_config.get("flag"):
            st.warning(f"⚠️ {table_config['flag']}")
    
    # Get mapping as dataframe
    df = mapping_to_dataframe(mapping, table_name)
    
    if df.empty:
        st.info("No column mappings defined.")
        return mapping
    
    # Style flagged rows
    def highlight_flagged(row):
        if row.get("flag"):
            return ['background-color: #fff3cd'] * len(row)
        return [''] * len(row)
    
    # Editable dataframe
    edited_df = st.data_editor(
        df,
        column_config={
            "source_col": st.column_config.TextColumn("Source Column", width="medium"),
            "target_col": st.column_config.TextColumn("Target Column", width="medium"),
            "type": st.column_config.TextColumn("Type", width="small"),
            "flag": st.column_config.TextColumn("Notes/Flags", width="large"),
        },
        hide_index=True,
        use_container_width=True,
        num_rows="dynamic",
        key=f"mapping_{table_name}"
    )
    
    # Update mapping with edited values
    updated_mapping = dataframe_to_mapping(edited_df, mapping, table_name)
    
    return updated_mapping


def render_constant_columns():
    """Render the constant columns configuration section."""
    st.subheader("➕ Constant Columns")
    st.caption("Add columns with constant values to be added to every row during transformation.")
    
    # Get existing constant columns from session state
    if "constant_columns" not in st.session_state:
        st.session_state.constant_columns = {}
    
    # Display existing constant columns
    if st.session_state.constant_columns:
        for table, columns in st.session_state.constant_columns.items():
            for col_name, col_value in columns.items():
                col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
                with col1:
                    st.text(table)
                with col2:
                    st.text(col_name)
                with col3:
                    st.text(str(col_value))
                with col4:
                    if st.button("🗑️", key=f"del_{table}_{col_name}"):
                        del st.session_state.constant_columns[table][col_name]
                        if not st.session_state.constant_columns[table]:
                            del st.session_state.constant_columns[table]
                        st.rerun()
    
    # Add new constant column
    with st.expander("Add New Constant Column"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            new_table = st.selectbox(
                "Table",
                options=["users", "folders", "documents", "embeddings", "agents", "users_groups"],
                key="const_table"
            )
        with col2:
            new_col_name = st.text_input("Column Name", placeholder="source_system", key="const_col_name")
        with col3:
            new_col_value = st.text_input("Value", placeholder="v4", key="const_col_value")
        
        if st.button("Add Constant Column"):
            if new_col_name and new_col_value:
                if new_table not in st.session_state.constant_columns:
                    st.session_state.constant_columns[new_table] = {}
                st.session_state.constant_columns[new_table][new_col_name] = new_col_value
                st.success(f"Added {new_col_name}={new_col_value} to {new_table}")
                st.rerun()
            else:
                st.error("Please provide both column name and value.")


def render_config_management():
    """Render save/load configuration section."""
    st.subheader("💾 Configuration Management")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Save Configuration**")
        config_name = st.text_input("Configuration Name", placeholder="my_migration_config")
        
        if st.button("Save Config", type="secondary"):
            if config_name:
                filepath = os.path.join(CONFIGS_DIR, f"{config_name}.yaml")
                config_to_save = {
                    "mapping": st.session_state.get(SessionKeys.MAPPING_CONFIG, {}),
                    "constant_columns": st.session_state.get("constant_columns", {})
                }
                if save_mapping_config(config_to_save, filepath):
                    # Also save to localStorage
                    save_mapping_storage(st.session_state.get(SessionKeys.MAPPING_CONFIG, {}))
                    st.success(f"Saved to {config_name}.yaml")
                else:
                    st.error("Failed to save configuration.")
            else:
                st.error("Please provide a configuration name.")
    
    with col2:
        st.markdown("**Load Configuration**")
        
        # List available configs
        config_files = [f for f in os.listdir(CONFIGS_DIR) if f.endswith('.yaml')]
        
        if config_files:
            selected_config = st.selectbox("Select Configuration", options=config_files)
            
            if st.button("Load Config", type="secondary"):
                filepath = os.path.join(CONFIGS_DIR, selected_config)
                loaded = load_mapping_config(filepath)
                if loaded:
                    if "mapping" in loaded:
                        st.session_state[SessionKeys.MAPPING_CONFIG] = loaded["mapping"]
                    else:
                        st.session_state[SessionKeys.MAPPING_CONFIG] = loaded
                    
                    if "constant_columns" in loaded:
                        st.session_state.constant_columns = loaded["constant_columns"]
                    
                    st.success(f"Loaded {selected_config}")
                    st.rerun()
                else:
                    st.error("Failed to load configuration.")
        else:
            st.info("No saved configurations found.")
    
    # Upload config
    uploaded_file = st.file_uploader("Or upload a YAML config file", type=["yaml", "yml"])
    if uploaded_file:
        import yaml
        try:
            loaded = yaml.safe_load(uploaded_file)
            if loaded:
                if "mapping" in loaded:
                    st.session_state[SessionKeys.MAPPING_CONFIG] = loaded["mapping"]
                else:
                    st.session_state[SessionKeys.MAPPING_CONFIG] = loaded
                
                if "constant_columns" in loaded:
                    st.session_state.constant_columns = loaded["constant_columns"]
                
                st.success("Configuration loaded from uploaded file!")
                st.rerun()
        except Exception as e:
            st.error(f"Failed to parse YAML: {e}")


def render_flagged_fields_summary():
    """Render summary of all flagged fields."""
    mapping = st.session_state.get(SessionKeys.MAPPING_CONFIG, {})
    flagged = get_flagged_fields(mapping)
    
    if flagged:
        st.subheader("⚠️ Flagged Fields Summary")
        st.caption("These fields require attention or manual mapping:")
        
        flagged_df = pd.DataFrame(flagged)
        st.dataframe(
            flagged_df,
            column_config={
                "table": st.column_config.TextColumn("Table"),
                "column": st.column_config.TextColumn("Column"),
                "flag": st.column_config.TextColumn("Issue"),
            },
            hide_index=True,
            use_container_width=True
        )


def render_transformation_section():
    """Render the transformation execution section."""
    st.markdown("---")
    st.subheader("🚀 Run Transformation")
    
    # Check if extraction has been done
    extracted_data = st.session_state.get(SessionKeys.EXTRACTED_DATA)
    
    if not extracted_data:
        st.warning("⚠️ No extracted data found. Please run extraction first on the Select Data page.")
        return
    
    st.info(f"Using extraction from: {extracted_data.get('timestamp', 'N/A')}")
    
    # Show summary of what will be transformed
    st.markdown("**Files to transform:**")
    for table, count in extracted_data.get("summary", {}).items():
        st.caption(f"• {table}: {count} rows")
    
    if st.button("🔄 Run Transform", type="primary", use_container_width=True):
        # Create progress containers
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def progress_callback(table_name: str, current: int, total: int):
            progress_bar.progress(current / total)
            status_text.text(f"Transforming {table_name}... ({current}/{total})")
        
        # Create transformation engine
        engine = TransformationEngine(
            mapping_config=st.session_state.get(SessionKeys.MAPPING_CONFIG, {}),
            input_dir=EXTRACT_DIR,
            output_dir=TRANSFORM_DIR,
            constant_columns=st.session_state.get("constant_columns", {}),
            progress_callback=progress_callback
        )
        
        # Run transformation
        with st.spinner("Transforming data..."):
            results = engine.run_full_transformation()
        
        progress_bar.progress(1.0)
        status_text.text("Transformation complete!")
        
        # Store results
        st.session_state[SessionKeys.TRANSFORMED_DATA] = results
        
        # Show results
        if results.get("errors"):
            for error in results["errors"]:
                st.error(error)
        else:
            st.success(f"✅ Transformation complete! Timestamp: {results['timestamp']}")
        
        # Show summary
        st.markdown("**Transformation Summary:**")
        summary_data = [
            {"Table": table, "Rows Transformed": count}
            for table, count in results.get("summary", {}).items()
        ]
        st.dataframe(pd.DataFrame(summary_data), hide_index=True)
        
        # Preview transformed data
        st.markdown("**👁️ Preview Transformed Data:**")
        for table, filepath in results.get("files", {}).items():
            if os.path.exists(filepath):
                with st.expander(f"📄 {table} ({results['summary'].get(table, 0)} rows)"):
                    preview_df = pd.read_csv(filepath, nrows=100)
                    st.dataframe(preview_df, use_container_width=True)
                    
                    # Download button
                    with open(filepath, "rb") as f:
                        st.download_button(
                            label=f"📥 Download {table}.csv",
                            data=f,
                            file_name=os.path.basename(filepath),
                            mime="text/csv",
                            key=f"dl_transform_{table}"
                        )


def main():
    """Main page function."""
    init_mapping_config()
    
    # Flagged fields summary at top
    render_flagged_fields_summary()
    
    st.markdown("---")
    
    # Table mapping editors
    st.subheader("📊 Column Mappings")
    
    mapping = st.session_state.get(SessionKeys.MAPPING_CONFIG, {})
    tables = ["users", "folders", "custom_documents", "embeddings", "agents", "users_groups"]
    
    # Create tabs for each table
    tabs = st.tabs([t.replace("_", " ").title() for t in tables])
    
    for i, table_name in enumerate(tables):
        with tabs[i]:
            updated_mapping = render_table_mapping(table_name, mapping)
            st.session_state[SessionKeys.MAPPING_CONFIG] = updated_mapping
    
    st.markdown("---")
    
    # Constant columns
    render_constant_columns()
    
    st.markdown("---")
    
    # Configuration management
    render_config_management()
    
    # Transformation execution
    render_transformation_section()
    
    # Next step hint
    if SessionKeys.TRANSFORMED_DATA in st.session_state:
        st.markdown("---")
        st.info("👉 **Next Step:** Go to **Target** page to configure target database and load data.")


if __name__ == "__main__":
    main()
