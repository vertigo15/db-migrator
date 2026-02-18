"""
DB Migrator - Main Application Entry Point

A Streamlit-based database migration tool for migrating data from V4 to V5 schema.
"""
import os
import streamlit as st
from utils.config import SessionKeys
from utils.storage import clear_all_storage

# Page configuration
st.set_page_config(
    page_title="DB Migrator",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)


# =============================================================================
# LOAD .ENV DEFAULTS INTO SESSION STATE AT APPLICATION STARTUP
# This runs ONCE when the app starts, before any pages are loaded
# =============================================================================
def load_env_to_session_state():
    """Load .env values into session state if not already loaded."""
    if "env_loaded" not in st.session_state:
        st.session_state.env_loaded = True
        
        # Load .env file explicitly
        from dotenv import load_dotenv
        base_dir = os.path.dirname(os.path.abspath(__file__))
        env_path = os.path.join(base_dir, ".env")
        load_dotenv(env_path, override=True)
        
        # Store source connection defaults in session state
        if SessionKeys.SOURCE_CONNECTION not in st.session_state:
            st.session_state[SessionKeys.SOURCE_CONNECTION] = {
                "host": os.getenv("SOURCE_DB_HOST", "localhost"),
                "port": os.getenv("SOURCE_DB_PORT", "5432"),
                "database": os.getenv("SOURCE_DB_DATABASE", ""),
                "username": os.getenv("SOURCE_DB_USERNAME", ""),
                "password": os.getenv("SOURCE_DB_PASSWORD", ""),
            }
        
        # Store target connection defaults in session state  
        if SessionKeys.TARGET_CONNECTION not in st.session_state:
            st.session_state[SessionKeys.TARGET_CONNECTION] = {
                "host": os.getenv("TARGET_DB_HOST", "localhost"),
                "port": os.getenv("TARGET_DB_PORT", "5432"),
                "database": os.getenv("TARGET_DB_DATABASE", ""),
                "username": os.getenv("TARGET_DB_USERNAME", ""),
                "password": os.getenv("TARGET_DB_PASSWORD", ""),
            }
        
        # Store table prefix
        if SessionKeys.TABLE_PREFIX not in st.session_state:
            st.session_state[SessionKeys.TABLE_PREFIX] = os.getenv("TABLE_PREFIX", "jeen_dev")


# Call immediately - this loads .env into session state at app startup
load_env_to_session_state()


# Custom CSS for RTL support and styling
st.markdown("""
<style>
    /* RTL support for Hebrew text */
    .rtl-text {
        direction: rtl;
        text-align: right;
    }
    
    /* Status indicators */
    .status-connected {
        color: #28a745;
        font-weight: bold;
    }
    .status-disconnected {
        color: #dc3545;
    }
    
    /* Flagged field highlighting */
    .flag-warning {
        background-color: #fff3cd;
        border-left: 3px solid #ffc107;
        padding: 5px 10px;
    }
    
    /* Metric cards */
    .metric-card {
        background-color: #f8f9fa;
        border-radius: 5px;
        padding: 10px;
        margin: 5px 0;
    }
</style>
""", unsafe_allow_html=True)


def render_sidebar_status():
    """Render the sidebar status section."""
    st.sidebar.title("🔄 DB Migrator")
    st.sidebar.markdown("---")
    
    # Source DB Status
    st.sidebar.subheader("Source Database")
    if SessionKeys.SOURCE_CONNECTION in st.session_state and st.session_state[SessionKeys.SOURCE_CONNECTION]:
        conn = st.session_state[SessionKeys.SOURCE_CONNECTION]
        st.sidebar.markdown(f"✅ **{conn.get('database', 'N/A')}**")
        st.sidebar.caption(f"{conn.get('host', 'N/A')}:{conn.get('port', 'N/A')}")
        if SessionKeys.TABLE_PREFIX in st.session_state:
            st.sidebar.caption(f"Prefix: `{st.session_state[SessionKeys.TABLE_PREFIX]}`")
    else:
        st.sidebar.markdown("❌ Not connected")
    
    # Target DB Status
    st.sidebar.subheader("Target Database")
    if SessionKeys.TARGET_CONNECTION in st.session_state and st.session_state[SessionKeys.TARGET_CONNECTION]:
        conn = st.session_state[SessionKeys.TARGET_CONNECTION]
        st.sidebar.markdown(f"✅ **{conn.get('database', 'N/A')}**")
        st.sidebar.caption(f"{conn.get('host', 'N/A')}:{conn.get('port', 'N/A')}")
    else:
        st.sidebar.markdown("❌ Not connected")
    
    # Selection Summary
    st.sidebar.markdown("---")
    st.sidebar.subheader("Selection Summary")
    
    if SessionKeys.SELECTED_USERS in st.session_state and st.session_state[SessionKeys.SELECTED_USERS]:
        user_count = len(st.session_state[SessionKeys.SELECTED_USERS])
        st.sidebar.metric("Selected Users", user_count)
    else:
        st.sidebar.caption("No users selected")
    
    # Extraction Status
    if SessionKeys.EXTRACTED_DATA in st.session_state and st.session_state[SessionKeys.EXTRACTED_DATA]:
        st.sidebar.markdown("---")
        st.sidebar.subheader("Last Extraction")
        extracted = st.session_state[SessionKeys.EXTRACTED_DATA]
        if "timestamp" in extracted:
            st.sidebar.caption(f"📅 {extracted['timestamp']}")
        if "summary" in extracted:
            for table, count in extracted["summary"].items():
                st.sidebar.caption(f"• {table}: {count} rows")
    
    # Reset button
    st.sidebar.markdown("---")
    if st.sidebar.button("🗑️ Reset All Settings", type="secondary", use_container_width=True):
        # Clear localStorage
        clear_all_storage()
        # Clear session state
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.sidebar.success("Settings cleared!")
        st.rerun()


def main():
    """Main application entry point."""
    render_sidebar_status()
    
    # Welcome message on main page
    st.title("Welcome to DB Migrator")
    st.markdown("""
    This tool helps you migrate data from a V4 database schema to V5.
    
    ## Quick Start
    
    1. **Connect** - Set up your source database connection
    2. **Select Data** - Choose users and documents to migrate
    3. **Transform** - Configure column mappings
    4. **Target** - Connect to your target database
    5. **Run** - Execute the migration
    
    Use the sidebar to navigate between pages, or click on a page in the left navigation panel.
    """)
    
    # Quick status cards
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info("### 📊 Source DB\nConfigure your source database connection and verify tables.")
        
    with col2:
        st.info("### 🔄 Transform\nMap V4 columns to V5 schema with visual editor.")
        
    with col3:
        st.info("### 🚀 Migrate\nRun the full migration pipeline with validation.")


if __name__ == "__main__":
    main()
