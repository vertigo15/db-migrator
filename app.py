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
    5. **Erase User Data V5** - Delete specific user data from a V5 instance
    
    Use the sidebar to navigate between pages, or click on a page in the left navigation panel.
    """)
    
    # Quick status cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.info("### 🔌 Source DB\nConfigure your source database connection and verify tables.")
        
    with col2:
        st.info("### 📋 Select Data\nChoose users and documents to migrate with SQL generation.")
        
    with col3:
        st.info("### 🎯 Target\nConnect to your target database for migration execution.")
    
    with col4:
        st.info("### 🗑️ Erase V5\nDelete specific user data from a V5 database instance.")
    
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
        mid -.-> ad
        mid -.-> cv
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
    st.caption("See `SOURCE_TO_TARGET_MAPPING.md` for the full column-level mapping reference.")

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
            "columns": [
                ("id", "id", "uuid_generate_v5(USER_NAMESPACE, id)"),
                ("email", "email", "Direct"),
                ("first_name", "name", "Direct"),
                ("last_name", "last_name", "Direct"),
                ("username", "email", "email.split('@')[0].lower().replace('.', '')"),
                ("avatar_url", "—", "NULL"),
                ("metadata", "Multiple", "JSONB legacyData: id, job, model, group_id, azure_oid, department, token_used, words_used, subfeatures, token_limit, company_name, phone_number, last_connected, letter_checkbox, times_connected, enabled_features, history_categories, company_name_in_hebrew"),
                ("created_at", "created_at", "Direct"),
                ("updated_at", "—", "now()"),
                ("deleted_at", "—", "NULL"),
                ("zitadel_user_id", "—", "NULL"),
                ("organization_id", "—", "Fixed config org UUID"),
                ("is_owner", "—", "false"),
                ("preferred_language", "—", "NULL"),
            ],
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
            "columns": [
                ("id", "id", "uuid_generate_v5(NAMESPACE, id)"),
                ("folder_name", "folder_name", "Direct"),
                ("parent_id", "parent_id", "uuid_generate_v5(NAMESPACE, parent_id) or NULL"),
                ("folder_type", "folder_type", "Default 'default', cast to folders_folder_type_enum"),
                ("user_id", "owner_id", "uuid_generate_v5(USER_NAMESPACE, owner_id)"),
                ("created_at", "created_at", "Direct"),
                ("updated_at", "—", "now()"),
                ("deleted_at", "—", "NULL"),
            ],
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
            "columns": [
                ("id", "doc_id", "uuid_generate_v5(DOC_NAMESPACE, doc_id)"),
                ("status", "—", "Always 'PROCESSED'"),
                ("file_name", "doc_name_origin / doc_title", "doc_name_origin → doc_title → 'unnamed'"),
                ("file_size", "doc_size", "Integer cast"),
                ("storage_type", "blob_source", "'azure_blob' → 'azure'; else direct"),
                ("storage_path", "doc_id", "Old doc_id as path reference"),
                ("storage_id", "—", "NULL"),
                ("metadata", "Multiple", "JSONB: name, source='legacy-migration', legacyData: {doc_id, doc_title, doc_description, doc_summery, tags, embedding_model, vector_methods, version, doc_checksum, data_integration_doc_metadata}"),
                ("created_at", "created_at", "Direct"),
                ("updated_at", "—", "now()"),
                ("deleted_at", "—", "NULL"),
                ("folder_id", "folder_id", "uuid_generate_v5(NAMESPACE, folder_id) or NULL"),
                ("user_id", "owner_id", "uuid_generate_v5(USER_NAMESPACE, owner_id)"),
                ("content_type", "doc_type", "Mapped to MIME (pdf → application/pdf, etc.)"),
                ("parsing_technique_id", "—", "NULL"),
                ("source_type", "—", "Always 'upload'"),
                ("organization_id", "—", "NULL"),
            ],
        },
        {
            "step": "04",
            "name": "Chunks & Embeddings",
            "source": "`{prefix}_embeddings`",
            "target": "`document_db.chunks` + `embeddings`",
            "logic": (
                "Single source table **split** into two target tables. "
                "`document` text separated into `original_content` and `translated_content`. "
                "Chunk IDs generated deterministically; `chunk_index` assigned per document. "
                "Embedding vectors inserted with model name. `document_id` resolved via mapping. "
                "Only rows where `metadata.type = 'chunk-data'` are processed."
            ),
            "columns": [
                ("chunks.id", "id", "uuid_generate_v5(NAMESPACE, id)"),
                ("chunks.document_id", "metadata.doc_id", "migration.get_new_id('documents', doc_id)"),
                ("chunks.chunk_index", "—", "Computed: cumcount() per doc_id (0-based)"),
                ("chunks.content", "document", "Text after 'original_content:' prefix; fallback full document"),
                ("chunks.content_hash", "document", "md5(content)"),
                ("chunks.content_type", "—", "Always 'text'"),
                ("chunks.char_count", "document", "len(content)"),
                ("chunks.word_count", "document", "len(content.split())"),
                ("chunks.metadata", "Multiple", "JSONB: parser, file_name, file_type, legacyData: {legacy_id, external_id, collection, tags, user_id, create_date, link_to_file, excerptKeywords}"),
                ("chunks.created_at", "metadata.create_date", "Parsed as timestamptz"),
                ("chunks.translated_content", "document", "Text between 'translated_content:' and 'original_content:'; NULL if absent"),
                ("embeddings.id", "—", "gen_random_uuid()"),
                ("embeddings.chunk_id", "id", "uuid_generate_v5(NAMESPACE, id)"),
                ("embeddings.embedding", "embeddings", "Cast to vector; optionally truncated to target_embedding_dim"),
                ("embeddings.model_name", "—", "Config value (default: text-embedding-ada-002)"),
                ("embeddings.created_at", "metadata.create_date", "Parsed as timestamptz"),
            ],
        },
        {
            "step": "05",
            "name": "Conversations",
            "source": "`{prefix}_logs`",
            "target": "`conversations` + `messages` + `message_content_blocks`",
            "logic": (
                "Logs **aggregated by `chat_id`** into conversations. Each log row produces a "
                "user + assistant message pair with deterministic IDs. Content blocks store "
                "question/answer text as structured JSONB. Token counts, sentiment, and "
                "toolkit settings preserved in message metadata."
            ),
            "columns": [
                ("conversations.id", "chat_id", "Direct UUID"),
                ("conversations.title", "title", "Last row's title; fallback 'Conversation {chat_id[:8]}'"),
                ("conversations.message_count", "—", "count(rows) × 2"),
                ("conversations.total_tokens", "token_amount", "sum(token_amount) per chat"),
                ("conversations.created_at", "created_at", "min(created_at) in chat"),
                ("conversations.updated_at / last_interacted_at", "created_at", "max(created_at) in chat"),
                ("conversations.user_id", "user_id", "uuid_generate_v5(USER_NAMESPACE, user_id)"),
                ("messages[user].id", "id", "uuid_generate_v5(NAMESPACE, '{id}-user')"),
                ("messages[user].role", "—", "'user'"),
                ("messages[user].created_at", "created_at", "created_at - interval '1 second'"),
                ("messages[user].metadata", "—", "Empty {}"),
                ("messages[asst].id", "id", "uuid_generate_v5(NAMESPACE, '{id}-assistant')"),
                ("messages[asst].role", "—", "'assistant'"),
                ("messages[asst].finish_reason", "—", "'stop'"),
                ("messages[asst].metadata", "Multiple", "JSONB: model, type, bot_id, is_like, token_amount, words_amount, calculated_time, category, sentiment, legacyData: {legacy_log_id, title, toolkit_settings, sourcetext, sourcelink, webpagelink, documents_selected}"),
                ("blocks[user].content", "question / question_in_english", "question[1].value → fallback question_in_english → '[no question text]'"),
                ("blocks[user].execution_time_ms", "—", "NULL"),
                ("blocks[asst].content", "answer", "Direct plain text"),
                ("blocks[asst].execution_time_ms", "calculated_time", "Direct integer or NULL"),
            ],
        },
        {
            "step": "06",
            "name": "Agents",
            "source": "`playground_bot_generator_config`",
            "target": "`agents` + `agent_settings` + `agent_documents`",
            "logic": (
                "Agent type derived from config (`spark` / `cortex` / `workflow`). "
                "JSONB fields (`bot_data`, `toolkit_settings`, prompts) decomposed into "
                "normalized columns. `docs_chosen` and `chosen_docs_folders` expanded "
                "into `agent_documents` link table. RAG settings extracted and mapped."
            ),
            "columns": [
                ("agents.id", "bot_id", "uuid_generate_v5(NAMESPACE, '{bot_id}-agent')"),
                ("agents.name", "bot_data.bot_name", "Max 128 chars; fallback 'Unnamed Agent'"),
                ("agents.description", "bot_data.bot_description", "Max 2048 chars"),
                ("agents.type", "toolkit_settings / prompts", "'workflow' / 'cortex' / 'spark' derived"),
                ("agents.user_id", "user_id", "uuid_generate_v5(USER_NAMESPACE, user_id)"),
                ("agents.avatar_url", "toolkit_settings", "assistantIcon.url or logo_url"),
                ("agents.folder_id", "folder_id", "migration.get_new_id('folders', folder_id)"),
                ("agents.last_interacted_at", "last_activity", "Direct"),
                ("agent_settings.model", "Prompts", "First non-null model from character/hack/analysis/relevant prompts"),
                ("agent_settings.instructions", "character_prompts.content", "Direct text"),
                ("agent_settings.enabled_tools", "toolkit_settings.data", "Keys where value == 'true'"),
                ("agent_settings.conversation_starters", "first_message", "[first_message] or []"),
                ("agent_settings.base_answers_on_files_only", "toolkit_settings.isAnswerBasedOnBestGrade", "Boolean"),
                ("agent_settings.retrieved_context_size", "toolkit_settings.vectorsNumber", "Integer"),
                ("agent_settings.re_rank_score", "toolkit_settings.passingGrade", "passingGrade / 100"),
                ("agent_settings.search_in_english", "toolkit_settings.inputVectorsLanguage", "== 'To English'"),
                ("agent_settings.show_source_links", "toolkit_settings.questions_selected", "Contains 'Display the source link'"),
                ("agent_settings.show_source_text", "toolkit_settings.questions_selected", "Contains 'Display the source text'"),
                ("agent_settings.follow_up_questions", "toolkit_settings.questions_selected", "Contains 'Follow-up questions'"),
                ("agent_settings.additional_links", "additional_links_title.is_selected", "== 'true'"),
                ("agent_documents.document_id", "docs_chosen", "uuid_generate_v5(DOC_NAMESPACE, doc_id)"),
                ("agent_documents.document_id", "chosen_docs_folders", "migration.get_new_id('folders', fid)"),
                ("agent_documents.type", "—", "'document' or 'folder'"),
            ],
        },
    ]

    for s in steps:
        with st.expander(f"**Step {s['step']}** — {s['name']}  ·  {s['source']}  →  {s['target']}"):
            st.markdown(f"**Source:** {s['source']}")
            st.markdown(f"**Target:** {s['target']}")
            st.markdown(f"**Logic:** {s['logic']}")
            st.markdown("**Column Mappings:**")
            col_header = "| Target Column | Source Column | Transformation |\n|---|---|---|\n"
            col_rows = "".join(
                f"| `{t}` | `{src}` | {xform} |\n"
                for t, src, xform in s["columns"]
            )
            st.markdown(col_header + col_rows)

    # ID Mapping infrastructure note
    st.info(
        "**🔗 migration.id_mappings** — Central lookup table that stores every "
        "`old_id → new_id (UUID)` mapping. Used by steps 2-6 to resolve foreign keys "
        "across tables. Enables idempotent re-runs (records are skipped if already migrated) "
        "and provides a real-time progress summary via `migration.progress_summary`."
    )

    st.markdown(
        "📄 **Full column-level mapping reference:** [`SOURCE_TO_TARGET_MAPPING.md`](SOURCE_TO_TARGET_MAPPING.md)"
    )


if __name__ == "__main__":
    main()
