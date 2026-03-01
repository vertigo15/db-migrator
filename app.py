"""
DB Migrator - Main Application Entry Point

A Streamlit-based database migration tool for migrating data from V4 to V5 schema.
"""
import os
import streamlit as st
import streamlit.components.v1 as components
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
    3. **Target** - Connect to your target database
    4. **Run** - Execute the migration
    
    Use the sidebar to navigate between pages, or click on a page in the left navigation panel.
    """)
    
    # Quick status cards
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info("### 🔌 Source DB\nConfigure your source database connection and verify tables.")
        
    with col2:
        st.info("### 📋 Select Data\nChoose users and documents to migrate with SQL generation.")
        
    with col3:
        st.info("### 🎯 Target\nConnect to your target database for migration execution.")
    
    # =========================================================================
    # DB LINEAGE CHART: V4 → V5 Migration
    # =========================================================================
    st.markdown("---")
    st.header("📊 Database Lineage: V4 → V5")
    st.caption("Migration flow from the legacy single-database schema to the new multi-database architecture.")
    
    mermaid_chart = """
    <style>
        body { margin: 0; padding: 0; background: transparent; }
    </style>
    <div class="mermaid">
    flowchart LR
        subgraph V4["V4 Source &middot; Single Database"]
            v4u["{prefix}_users"]
            v4f["{prefix}_folders"]
            v4d["{prefix}_custom_documents"]
            v4e["{prefix} &middot; collection"]
            v4l["{prefix}_logs"]
            v4b["playground_bot_generator_config"]
        end

        subgraph MAP["Migration Infrastructure"]
            mid["migration.id_mappings<br/>old_id &#10148; new UUID"]
        end

        subgraph V5U["V5 &middot; user_db"]
            u["users"]
        end

        subgraph V5D["V5 &middot; document_db"]
            f["folders"]
            d["documents"]
            c["chunks"]
            e["embeddings"]
        end

        subgraph V5C["V5 &middot; completion_db"]
            ag["agents"]
            as2["agent_settings"]
            ad["agent_documents"]
            cv["conversations"]
            ms["messages"]
            mb["message_content_blocks"]
        end

        v4u -- "Step 1" --> u
        v4f -- "Step 2" --> f
        v4d -- "Step 3" --> d
        v4e -- "Step 4" --> c
        v4e -- "Step 4" --> e
        v4l -- "Step 5" --> cv
        v4l -- "Step 5" --> ms
        v4l -- "Step 5" --> mb
        v4b -- "Step 6" --> ag
        v4b -- "Step 6" --> as2
        v4b -- "Step 6" --> ad

        mid -.-> u
        mid -.-> f
        mid -.-> d
        mid -.-> c
        mid -.-> ag
    </div>
    <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
    <script>
        mermaid.initialize({
            startOnLoad: true,
            theme: 'base',
            themeVariables: {
                primaryColor: '#4A90D9',
                primaryTextColor: '#fff',
                primaryBorderColor: '#3A7BC8',
                lineColor: '#5C6BC0',
                secondaryColor: '#E8F5E9',
                tertiaryColor: '#FFF3E0',
                clusterBkg: '#f0f4fa',
                clusterBorder: '#90CAF9'
            },
            flowchart: { curve: 'basis', useMaxWidth: true }
        });
    </script>
    """
    components.html(mermaid_chart, height=520, scrolling=True)
    
    # =========================================================================
    # MIGRATION STEPS DETAIL TABLE
    # =========================================================================
    st.subheader("🔀 Migration Steps & Transformation Logic")
    
    steps = [
        {
            "step": "01",
            "name": "Users",
            "source": "`{prefix}_users`",
            "target": "`user_db.users`",
            "logic": (
                "Hash-based IDs replaced with new UUIDs. "
                "Legacy fields (`token_used`, `words_used`, `model`, `subfeatures`, etc.) "
                "are preserved inside a `metadata` JSONB column under `legacyData`. "
                "Username derived from email. Each mapping is stored in `migration.id_mappings`."
            ),
        },
        {
            "step": "02",
            "name": "Folders",
            "source": "`{prefix}_folders`",
            "target": "`document_db.folders`",
            "logic": (
                "Deterministic UUIDs generated via `uuid_generate_v5` to preserve parent → child hierarchy. "
                "`owner_id` resolved to V5 `user_id` through `migration.id_mappings`. "
                "Parents inserted before children (topological sort)."
            ),
        },
        {
            "step": "03",
            "name": "Documents",
            "source": "`{prefix}_custom_documents`",
            "target": "`document_db.documents`",
            "logic": (
                "New UUID per document. `owner_id` → `user_id` and `folder_id` resolved via mapping table. "
                "File type mapped to MIME `content_type`. Legacy metadata (`tags`, `doc_summery`, "
                "`embedding_model`, `vector_methods`, etc.) stored in `metadata` JSONB."
            ),
        },
        {
            "step": "04",
            "name": "Chunks & Embeddings",
            "source": "`{prefix}` (collection)",
            "target": "`document_db.chunks` + `embeddings`",
            "logic": (
                "Single source table **split** into two target tables. "
                "`document` text separated into `original_content` and `translated_content`. "
                "Chunk IDs generated deterministically; `chunk_index` assigned per document. "
                "Embedding vectors inserted with model name. `document_id` resolved via mapping."
            ),
        },
        {
            "step": "05",
            "name": "Conversations",
            "source": "`{prefix}_logs`",
            "target": "`completion_db.conversations` + `messages` + `message_content_blocks`",
            "logic": (
                "Logs **aggregated by `chat_id`** into conversations. Each log row produces a "
                "user + assistant message pair with deterministic IDs. Content blocks store "
                "question/answer text as structured JSONB. Token counts, sentiment, and "
                "toolkit settings preserved in message metadata."
            ),
        },
        {
            "step": "06",
            "name": "Agents",
            "source": "`playground_bot_generator_config`",
            "target": "`completion_db.agents` + `agent_settings` + `agent_documents`",
            "logic": (
                "Agent type derived from config (`spark` / `cortex` / `workflow`). "
                "JSONB fields (`bot_data`, `toolkit_settings`, prompts) decomposed into "
                "normalized columns. `docs_chosen` and `chosen_docs_folders` expanded "
                "into `agent_documents` link table. RAG settings extracted and mapped."
            ),
        },
    ]
    
    for s in steps:
        with st.expander(f"**Step {s['step']}** — {s['name']}  ·  {s['source']}  →  {s['target']}"):
            st.markdown(f"**Source:** {s['source']}")
            st.markdown(f"**Target:** {s['target']}")
            st.markdown(f"**Logic:** {s['logic']}")
    
    # ID Mapping infrastructure note
    st.info(
        "**🔗 migration.id_mappings** — Central lookup table that stores every "
        "`old_id → new_id (UUID)` mapping. Used by steps 2-6 to resolve foreign keys "
        "across tables. Enables idempotent re-runs (records are skipped if already migrated) "
        "and provides a real-time progress summary via `migration.progress_summary`."
    )


if __name__ == "__main__":
    main()
