"""
Page 2: Select Data to Migrate

Features:
- User selection with searchable dataframe
- Document filters (date range, max size)
- Related data counts (folders, embeddings, agents)
- Extraction with progress
- CSV preview and download
"""
import os
import json
from datetime import datetime, date
import streamlit as st
import pandas as pd

from utils.db import ConnectionConfig, execute_query
from utils.storage import (
    save_selected_users, load_selected_users,
    save_document_filters, load_document_filters
)
from utils.config import SessionKeys, get_table_name, get_env_org_id, get_env_embedding_model, get_env_target_defaults, EMBEDDING_MODEL_OPTIONS
from utils.db import test_connection
from utils.extraction import (
    ExtractionEngine,
    get_document_count_preview,
    get_related_counts,
    estimate_embeddings_size
)

# Page config
st.set_page_config(page_title="Select Data", page_icon="📋", layout="wide")
st.title("📋 Select Data to Migrate")

# Output directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(BASE_DIR, "output", "extract")


# ─── Destination DB lookup helpers ──────────────────────────────────────────

def _make_target_config(database: str):
    """Return a ConnectionConfig for a specific target database, or None if
    no target connection is available in session state."""
    target: ConnectionConfig = st.session_state.get("target_config")
    if target is None:
        td = st.session_state.get(SessionKeys.TARGET_CONNECTION, {})
        if not td.get("host") or not td.get("password"):
            return None
        try:
            target = ConnectionConfig.from_dict(td)
        except Exception:
            return None
    return ConnectionConfig(
        host=target.host,
        port=target.port,
        database=database,
        username=target.username,
        password=target.password,
    )


def fetch_dest_org_ids() -> list:
    """Query target user_db for distinct organization_ids."""
    cfg = _make_target_config("user_db")
    if cfg is None:
        return []
    try:
        df = execute_query(
            cfg,
            "SELECT DISTINCT organization_id::text AS org_id "
            "FROM public.users "
            "WHERE organization_id IS NOT NULL "
            "ORDER BY org_id"
        )
        return df["org_id"].tolist() if not df.empty else []
    except Exception:
        return []


def fetch_dest_embedding_models() -> list:
    """Query target document_db for distinct embedding model_names."""
    cfg = _make_target_config("document_db")
    if cfg is None:
        return []
    try:
        df = execute_query(
            cfg,
            "SELECT DISTINCT model_name "
            "FROM public.embeddings "
            "WHERE model_name IS NOT NULL "
            "ORDER BY model_name"
        )
        return df["model_name"].tolist() if not df.empty else []
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────


def convert_timestamp_to_datetime(ts):
    """Convert Unix timestamp (float/int) or string to datetime."""
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return None
    if isinstance(ts, str):
        try:
            return pd.to_datetime(ts)
        except:
            return None
    if isinstance(ts, (int, float)):
        try:
            return pd.to_datetime(ts, unit='s')
        except:
            return None
    return ts


def check_connection():
    """Check if source connection is available."""
    if "source_config" not in st.session_state:
        st.warning("⚠️ Please connect to the source database first.")
        st.page_link("pages/1_connect.py", label="Go to Connect Page", icon="🔌")
        return False
    return True


def load_users_data(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Load users from the source database."""
    table_name = get_table_name("users", prefix)
    query = f"""
        SELECT id, name, last_name, email, company_name, created_at, last_connected
        FROM public.{table_name}
        ORDER BY email
    """
    df = execute_query(config, query)
    
    # Convert timestamp columns to datetime
    if not df.empty:
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], unit='s', errors='coerce')
        if 'last_connected' in df.columns:
            df['last_connected'] = pd.to_datetime(df['last_connected'], unit='s', errors='coerce')
    
    return df


def render_user_selection(config: ConnectionConfig, prefix: str):
    """Render the user selection section."""
    st.subheader("👥 Select Users")
    
    # Load users
    with st.spinner("Loading users..."):
        users_df = load_users_data(config, prefix)
    
    if users_df.empty:
        st.warning("No users found in the database.")
        return
    
    st.caption(f"Found {len(users_df)} users in `{get_table_name('users', prefix)}`")
    
    # Load previously selected users from localStorage
    saved_emails = load_selected_users()
    if not isinstance(saved_emails, list):
        saved_emails = []
    else:
        saved_emails = [e for e in saved_emails if isinstance(e, str)]
    
    # Select all checkbox
    col1, col2 = st.columns([1, 4])
    with col1:
        select_all = st.checkbox("Select All", value=False)
    
    # Search filter
    with col2:
        search = st.text_input("🔍 Search users", placeholder="Search by name or email...")
    
    # Filter dataframe
    if search:
        mask = (
            users_df["name"].str.contains(search, case=False, na=False) |
            users_df["last_name"].str.contains(search, case=False, na=False) |
            users_df["email"].str.contains(search, case=False, na=False) |
            users_df["company_name"].str.contains(search, case=False, na=False)
        )
        filtered_df = users_df[mask].copy()
    else:
        filtered_df = users_df.copy()
    
    # Add selection column
    if select_all:
        filtered_df["selected"] = True
    else:
        filtered_df["selected"] = filtered_df["email"].isin(saved_emails)
    
    # Reorder columns
    display_cols = ["selected", "name", "last_name", "email", "company_name", "created_at", "last_connected"]
    filtered_df = filtered_df[display_cols]
    
    # Display editable dataframe
    edited_df = st.data_editor(
        filtered_df,
        column_config={
            "selected": st.column_config.CheckboxColumn(
                "Select",
                help="Select users to migrate",
                default=False
            ),
            "name": st.column_config.TextColumn("First Name"),
            "last_name": st.column_config.TextColumn("Last Name"),
            "email": st.column_config.TextColumn("Email"),
            "company_name": st.column_config.TextColumn("Company"),
            "created_at": st.column_config.DatetimeColumn("Created", format="YYYY-MM-DD"),
            "last_connected": st.column_config.DatetimeColumn("Last Connected", format="YYYY-MM-DD"),
        },
        hide_index=True,
        use_container_width=True,
        height=400
    )
    
    # Get selected emails
    selected_emails = edited_df[edited_df["selected"] == True]["email"].tolist()
    
    # Store selection
    st.session_state[SessionKeys.SELECTED_USERS] = selected_emails
    
    # Get selected user IDs
    selected_user_ids = users_df[users_df["email"].isin(selected_emails)]["id"].tolist()
    st.session_state[SessionKeys.SELECTED_USER_IDS] = selected_user_ids
    
    # Auto-save selection (no button needed)
    save_selected_users(selected_emails)
    
    # Selection summary
    st.metric("Selected Users", len(selected_emails))
    
    return selected_emails, selected_user_ids

def render_user_groups_under_users(config: ConnectionConfig, prefix: str, user_ids: list):
    """Show user groups for currently selected users with selection capability."""
    st.subheader("👥 User Groups")
    if not user_ids:
        st.info("Select users to view their groups.")
        st.session_state["selected_group_ids"] = []
        return []
    users_table = get_table_name("users", prefix)
    groups_table = get_table_name("users_groups", prefix)
    # Detect the group-id column in users table (schema may vary between environments)
    cols_query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
    """
    cols_df = execute_query(config, cols_query, (users_table,))
    available_cols = set(cols_df["column_name"].tolist()) if not cols_df.empty else set()
    candidate_cols = ["__group_id__", "group_id", "_group_id_", "groupid"]
    group_col = next((c for c in candidate_cols if c in available_cols), None)
    if group_col is None:
        st.info("No group-id column found in users table for this environment.")
        st.session_state["selected_group_ids"] = []
        return []
    placeholders = ", ".join(["%s"] * len(user_ids))
    group_ids_query = f"""
        SELECT DISTINCT "{group_col}" AS group_id
        FROM public.{users_table}
        WHERE id IN ({placeholders}) AND "{group_col}" IS NOT NULL
    """
    gids_df = execute_query(config, group_ids_query, tuple(user_ids))
    if gids_df.empty:
        st.info("No user groups found for selected users.")
        st.session_state["selected_group_ids"] = []
        return []
    group_ids = gids_df["group_id"].astype(str).tolist()
    gp = ", ".join(["%s"] * len(group_ids))
    groups_query = f"""
        SELECT id, group_name, default_model, default_max_tokens_per_user
        FROM public.{groups_table}
        WHERE id IN ({gp})
        ORDER BY group_name
    """
    groups_df = execute_query(config, groups_query, tuple(group_ids))
    if groups_df.empty:
        st.info("No user groups found.")
        st.session_state["selected_group_ids"] = []
        return []
    
    st.caption(f"Groups found: {len(groups_df)}")
    
    # Select all checkbox for groups
    select_all_groups = st.checkbox("Select all groups", value=True, key="select_all_groups")
    
    # Previous selection
    previous = st.session_state.get("selected_group_ids")
    if select_all_groups:
        groups_df["selected"] = True
    else:
        if isinstance(previous, list):
            groups_df["selected"] = groups_df["id"].astype(str).isin(previous)
        else:
            groups_df["selected"] = True
    
    # Reorder columns
    groups_df = groups_df[["selected", "id", "group_name", "default_model", "default_max_tokens_per_user"]]
    
    edited_df = st.data_editor(
        groups_df,
        hide_index=True,
        use_container_width=True,
        height=250,
        column_config={
            "selected": st.column_config.CheckboxColumn("Select", default=True),
        },
        key="groups_editor",
    )
    
    selected_group_ids = edited_df[edited_df["selected"] == True]["id"].astype(str).tolist()
    st.session_state["selected_group_ids"] = selected_group_ids
    st.metric("Selected Groups", len(selected_group_ids))
    return selected_group_ids


def render_document_filters(config: ConnectionConfig, prefix: str, user_ids: list):
    """Render document filter section."""
    if not user_ids:
        st.info("Select users above to filter documents.")
        return None, None, None
    
    # Load saved filters
    saved_filters = load_document_filters()
    if not isinstance(saved_filters, dict):
        saved_filters = {}
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        date_from = st.date_input(
            "Created After",
            value=saved_filters.get("date_from"),
            key="doc_date_from"
        )
    
    with col2:
        date_to = st.date_input(
            "Created Before",
            value=saved_filters.get("date_to"),
            key="doc_date_to"
        )
    
    with col3:
        max_size = st.number_input(
            "Max Document Size (bytes)",
            value=saved_filters.get("max_size", 0),
            min_value=0,
            step=1000000,
            help="0 = no limit"
        )
    
    # Convert date to datetime if set
    date_from_dt = datetime.combine(date_from, datetime.min.time()) if date_from else None
    date_to_dt = datetime.combine(date_to, datetime.max.time()) if date_to else None
    max_size_val = max_size if max_size > 0 else None
    
    # Get preview count
    with st.spinner("Counting matching documents..."):
        doc_count = get_document_count_preview(
            config, prefix, user_ids,
            date_from_dt, date_to_dt, max_size_val
        )
    
    st.metric("📝 Matching Documents", f"{doc_count:,}")
    
    # Save filters
    if st.button("💾 Save Filters", type="secondary", key="save_filters"):
        filters = {
            "date_from": str(date_from) if date_from else None,
            "date_to": str(date_to) if date_to else None,
            "max_size": max_size
        }
        save_document_filters(filters)
        st.success("Filters saved!")
    
    # Store in session state
    st.session_state[SessionKeys.DOCUMENT_FILTERS] = {
        "date_from": date_from_dt,
        "date_to": date_to_dt,
        "max_size": max_size_val
    }
    
    return date_from_dt, date_to_dt, max_size_val
def _load_documents_df(config: ConnectionConfig, prefix: str, user_ids: list, filters: dict) -> pd.DataFrame:
    """Load documents for selected users + current filters."""
    if not user_ids:
        return pd.DataFrame()
    doc_table = get_table_name("custom_documents", prefix)
    placeholders = ", ".join(["%s"] * len(user_ids))
    query = f"""
        SELECT doc_id, owner_id, doc_title, doc_name_origin, doc_size, created_at, folder_id, doc_type
        FROM public.{doc_table}
        WHERE owner_id IN ({placeholders})
    """
    params = list(user_ids)
    if filters.get("date_from"):
        query += " AND created_at >= %s"
        params.append(filters["date_from"])
    if filters.get("date_to"):
        query += " AND created_at <= %s"
        params.append(filters["date_to"])
    if filters.get("max_size"):
        query += " AND doc_size <= %s"
        params.append(filters["max_size"])
    query += " ORDER BY created_at DESC"
    df = execute_query(config, query, tuple(params))
    if not df.empty and "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], unit="s", errors="coerce")
    return df

def render_document_selection(config: ConnectionConfig, prefix: str, user_ids: list):
    """Render document filters + selectable documents list (default all selected)."""
    st.markdown("---")
    st.subheader("📚 Documents (Filters + Selection)")
    if not user_ids:
        st.info("Select users first.")
        st.session_state["selected_doc_ids"] = []
        return []
    # Keep all filters above the table
    render_document_filters(config, prefix, user_ids)
    filters = st.session_state.get(SessionKeys.DOCUMENT_FILTERS, {})
    with st.spinner("Loading documents..."):
        docs_df = _load_documents_df(config, prefix, user_ids, filters)
    if docs_df.empty:
        st.info("No matching documents found.")
        st.session_state["selected_doc_ids"] = []
        return []
    st.caption(f"Found {len(docs_df)} matching documents")
    owner_options = sorted(docs_df["owner_id"].dropna().astype(str).unique().tolist())
    selected_owners = st.multiselect("Filter by owner", options=owner_options, default=owner_options, key="doc_owner_filter")
    search = st.text_input("🔍 Search documents", placeholder="Search by doc id/title/name...", key="doc_search")
    filtered_df = docs_df.copy()
    if selected_owners:
        filtered_df = filtered_df[filtered_df["owner_id"].astype(str).isin(selected_owners)]
    if search:
        mask = (
            filtered_df["doc_id"].astype(str).str.contains(search, case=False, na=False)
            | filtered_df["doc_title"].astype(str).str.contains(search, case=False, na=False)
            | filtered_df["doc_name_origin"].astype(str).str.contains(search, case=False, na=False)
        )
        filtered_df = filtered_df[mask]
    select_all_docs = st.checkbox("Select all documents in current list", value=True, key="select_all_docs")
    previous = st.session_state.get("selected_doc_ids")
    if select_all_docs:
        filtered_df["selected"] = True
    else:
        if isinstance(previous, list):
            filtered_df["selected"] = filtered_df["doc_id"].isin(previous)
        else:
            filtered_df["selected"] = True
    filtered_df = filtered_df[["selected", "doc_id", "owner_id", "doc_title", "doc_name_origin", "doc_size", "created_at", "folder_id", "doc_type"]]
    edited_df = st.data_editor(
        filtered_df,
        hide_index=True,
        use_container_width=True,
        height=350,
        column_config={
            "selected": st.column_config.CheckboxColumn("Select", default=True),
            "created_at": st.column_config.DatetimeColumn("Created", format="YYYY-MM-DD"),
        },
        key="documents_editor",
    )
    selected_doc_ids = edited_df[edited_df["selected"] == True]["doc_id"].astype(str).tolist()
    st.session_state["selected_doc_ids"] = selected_doc_ids
    st.metric("Selected Documents", len(selected_doc_ids))
    return selected_doc_ids

def _extract_doc_id_from_metadata(value):
    if isinstance(value, dict):
        return value.get("doc_id")
    if isinstance(value, str):
        try:
            obj = json.loads(value)
            if isinstance(obj, dict):
                return obj.get("doc_id")
        except Exception:
            return None
    return None

def render_embeddings_selection(config: ConnectionConfig, prefix: str, doc_ids: list):
    """Render selectable embeddings list (default all selected)."""
    st.markdown("---")
    st.subheader("🧮 Select Embeddings")
    if not doc_ids:
        st.info("No selected documents, so no embeddings to select.")
        st.session_state["selected_embedding_ids"] = []
        return []
    embeddings_table = get_table_name("embeddings", prefix)
    placeholders = ", ".join(["%s"] * len(doc_ids))
    query = f"""
        SELECT id, external_id, collection, metadata
        FROM public.{embeddings_table}
        WHERE metadata->>'doc_id' IN ({placeholders})
        LIMIT 5000
    """
    with st.spinner("Loading embeddings..."):
        emb_df = execute_query(config, query, tuple(doc_ids))
    if emb_df.empty:
        st.info("No embeddings found for selected documents.")
        st.session_state["selected_embedding_ids"] = []
        return []
    emb_df["doc_id"] = emb_df["metadata"].apply(_extract_doc_id_from_metadata)
    search = st.text_input("🔍 Search embeddings", placeholder="Search by id/external_id/collection/doc_id...", key="emb_search")
    filtered_df = emb_df.copy()
    if search:
        mask = (
            filtered_df["id"].astype(str).str.contains(search, case=False, na=False)
            | filtered_df["external_id"].astype(str).str.contains(search, case=False, na=False)
            | filtered_df["collection"].astype(str).str.contains(search, case=False, na=False)
            | filtered_df["doc_id"].astype(str).str.contains(search, case=False, na=False)
        )
        filtered_df = filtered_df[mask]
    select_all_embeddings = st.checkbox("Select all embeddings in current list", value=True, key="select_all_embeddings")
    previous = st.session_state.get("selected_embedding_ids")
    if select_all_embeddings:
        filtered_df["selected"] = True
    else:
        if isinstance(previous, list):
            filtered_df["selected"] = filtered_df["id"].isin(previous)
        else:
            filtered_df["selected"] = True
    filtered_df = filtered_df[["selected", "id", "external_id", "collection", "doc_id"]]
    edited_df = st.data_editor(filtered_df, hide_index=True, use_container_width=True, height=320, key="embeddings_editor")
    selected_embedding_ids = edited_df[edited_df["selected"] == True]["id"].astype(str).tolist()
    st.session_state["selected_embedding_ids"] = selected_embedding_ids
    st.metric("Selected Embeddings", len(selected_embedding_ids))
    return selected_embedding_ids

def render_conversations_selection(config: ConnectionConfig, prefix: str, user_ids: list):
    """Render selectable conversations list (default all selected)."""
    st.markdown("---")
    st.subheader("💬 Select Conversations")
    if not user_ids:
        st.info("No selected users, so no conversations to select.")
        st.session_state["selected_conversation_ids"] = []
        return []
    logs_table = get_table_name("logs", prefix)
    placeholders = ", ".join(["%s"] * len(user_ids))
    query = f"""
        SELECT id, user_id, chat_id, question, answer, created_at, type, bot_id
        FROM public.{logs_table}
        WHERE user_id IN ({placeholders})
        ORDER BY created_at DESC
        LIMIT 5000
    """
    with st.spinner("Loading conversations..."):
        convs_df = execute_query(config, query, tuple(user_ids))
    if not convs_df.empty and "created_at" in convs_df.columns:
        convs_df["created_at"] = pd.to_datetime(convs_df["created_at"], unit="s", errors="coerce")
    if convs_df.empty:
        st.info("No conversations found for selected users.")
        st.session_state["selected_conversation_ids"] = []
        return []
    
    # Truncate question/answer for display
    convs_df["question_preview"] = convs_df["question"].astype(str).str[:100] + "..."
    convs_df["answer_preview"] = convs_df["answer"].astype(str).str[:100] + "..."
    
    search = st.text_input("🔍 Search conversations", placeholder="Search by id/user_id/question...", key="conv_search")
    filtered_df = convs_df.copy()
    if search:
        mask = (
            filtered_df["id"].astype(str).str.contains(search, case=False, na=False)
            | filtered_df["user_id"].astype(str).str.contains(search, case=False, na=False)
            | filtered_df["chat_id"].astype(str).str.contains(search, case=False, na=False)
            | filtered_df["question"].astype(str).str.contains(search, case=False, na=False)
        )
        filtered_df = filtered_df[mask]
    select_all_convs = st.checkbox("Select all conversations in current list", value=True, key="select_all_convs")
    previous = st.session_state.get("selected_conversation_ids")
    if select_all_convs:
        filtered_df["selected"] = True
    else:
        if isinstance(previous, list):
            filtered_df["selected"] = filtered_df["id"].isin(previous)
        else:
            filtered_df["selected"] = True
    filtered_df = filtered_df[["selected", "id", "user_id", "chat_id", "type", "question_preview", "answer_preview", "created_at"]]
    edited_df = st.data_editor(
        filtered_df,
        hide_index=True,
        use_container_width=True,
        height=320,
        column_config={
            "created_at": st.column_config.DatetimeColumn("Created", format="YYYY-MM-DD"),
            "question_preview": st.column_config.TextColumn("Question"),
            "answer_preview": st.column_config.TextColumn("Answer"),
            "chat_id": st.column_config.TextColumn("Chat ID"),
            "type": st.column_config.TextColumn("Type"),
        },
        key="conversations_editor",
    )
    selected_conversation_ids = edited_df[edited_df["selected"] == True]["id"].astype(str).tolist()
    st.session_state["selected_conversation_ids"] = selected_conversation_ids
    st.metric("Selected Conversations", len(selected_conversation_ids))
    return selected_conversation_ids


def render_agents_selection(config: ConnectionConfig, prefix: str, user_ids: list):
    """Render selectable agents list (default all selected)."""
    st.markdown("---")
    st.subheader("🤖 Select Agents")
    if not user_ids:
        st.info("No selected users, so no agents to select.")
        st.session_state["selected_agent_ids"] = []
        return []
    agents_table = get_table_name("agents", prefix)
    placeholders = ", ".join(["%s"] * len(user_ids))
    query = f"""
        SELECT bot_id, user_id, folder_id, created_at,
               COALESCE(array_length(docs_chosen, 1), 0) AS docs,
               COALESCE(array_length(chosen_docs_folders, 1), 0) AS folders,
               array_to_string(docs_chosen, ', ') AS doc_ids
        FROM public.{agents_table}
        WHERE user_id IN ({placeholders})
        ORDER BY created_at DESC
        LIMIT 5000
    """
    with st.spinner("Loading agents..."):
        agents_df = execute_query(config, query, tuple(user_ids))
    if not agents_df.empty and "created_at" in agents_df.columns:
        agents_df["created_at"] = pd.to_datetime(agents_df["created_at"], unit="s", errors="coerce")
    if agents_df.empty:
        st.info("No agents found for selected users.")
        st.session_state["selected_agent_ids"] = []
        return []
    search = st.text_input("🔍 Search agents", placeholder="Search by bot_id/user_id/folder_id...", key="agent_search")
    filtered_df = agents_df.copy()
    if search:
        mask = (
            filtered_df["bot_id"].astype(str).str.contains(search, case=False, na=False)
            | filtered_df["user_id"].astype(str).str.contains(search, case=False, na=False)
            | filtered_df["folder_id"].astype(str).str.contains(search, case=False, na=False)
        )
        filtered_df = filtered_df[mask]
    select_all_agents = st.checkbox("Select all agents in current list", value=True, key="select_all_agents")
    previous = st.session_state.get("selected_agent_ids")
    if select_all_agents:
        filtered_df["selected"] = True
    else:
        if isinstance(previous, list):
            filtered_df["selected"] = filtered_df["bot_id"].isin(previous)
        else:
            filtered_df["selected"] = True
    filtered_df = filtered_df[["selected", "bot_id", "user_id", "folder_id", "docs", "doc_ids", "folders", "created_at"]]
    edited_df = st.data_editor(
        filtered_df,
        hide_index=True,
        use_container_width=True,
        height=320,
        column_config={"created_at": st.column_config.DatetimeColumn("Created", format="YYYY-MM-DD")},
        key="agents_editor",
    )
    selected_agent_ids = edited_df[edited_df["selected"] == True]["bot_id"].astype(str).tolist()
    st.session_state["selected_agent_ids"] = selected_agent_ids
    st.metric("Selected Agents", len(selected_agent_ids))
    return selected_agent_ids


def render_related_counts(config: ConnectionConfig, prefix: str, user_ids: list, doc_count: int):
    """Render related data counts."""
    st.markdown("---")
    st.subheader("📊 Related Data Summary")
    
    if not user_ids:
        st.info("Select users to see related data counts.")
        return
    
    # Get document IDs for embedding count (if we have the preview)
    # For now, we'll show estimated counts
    
    with st.spinner("Calculating related data..."):
        # First get document IDs for the current filters
        filters = st.session_state.get(SessionKeys.DOCUMENT_FILTERS, {})
        doc_table = get_table_name("custom_documents", prefix)
        placeholders = ", ".join(["%s"] * len(user_ids))
        
        query = f"SELECT doc_id FROM public.{doc_table} WHERE owner_id IN ({placeholders})"
        params = list(user_ids)
        
        if filters.get("date_from"):
            query += " AND created_at >= %s"
            params.append(filters["date_from"])
        if filters.get("date_to"):
            query += " AND created_at <= %s"
            params.append(filters["date_to"])
        if filters.get("max_size"):
            query += " AND doc_size <= %s"
            params.append(filters["max_size"])
        
        doc_ids_df = execute_query(config, query, tuple(params))
        doc_ids = doc_ids_df["doc_id"].tolist() if not doc_ids_df.empty else []
        
        # Get counts
        counts = get_related_counts(config, prefix, user_ids, doc_ids)
    
    # Display as metric cards
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.metric("👥 Users", len(user_ids))
    with col2:
        st.metric("📄 Documents", f"{doc_count:,}")
    with col3:
        st.metric("📁 Folders", f"{counts.get('folders', 0):,}")
    with col4:
        st.metric("🧮 Embeddings", f"{counts.get('embeddings', 0):,}")
    with col5:
        st.metric("🤖 Agents", f"{counts.get('agents', 0):,}")
    with col6:
        st.metric("💬 Conversations", f"{counts.get('logs', 0):,}")
    
    # Embedding size warning
    if doc_ids:
        est_size = estimate_embeddings_size(config, prefix, doc_ids)
        if est_size > 500:
            st.warning(f"⚠️ Estimated embeddings size: {est_size:.1f} MB. Consider batched extraction for large datasets.")
    
    # Summary bar
    total_items = len(user_ids) + doc_count + counts.get("folders", 0) + counts.get("embeddings", 0) + counts.get("agents", 0) + counts.get("logs", 0)
    st.success(f"**Ready to migrate:** {len(user_ids)} users, {doc_count:,} documents, {counts.get('embeddings', 0):,} embeddings, {counts.get('folders', 0):,} folders, {counts.get('agents', 0):,} agents, {counts.get('logs', 0):,} conversations")

def render_copy_preview(config: ConnectionConfig, prefix: str, user_ids: list):
    """Optional preview of folders and embeddings that will be copied."""
    st.markdown("---")
    st.subheader("🔎 Preview Data to Copy")

    if not user_ids:
        st.info("Select users to preview folders and embeddings.")
        return

    show_preview = st.toggle("Show folders and embeddings preview", value=False)
    if not show_preview:
        return

    filters = st.session_state.get(SessionKeys.DOCUMENT_FILTERS, {})
    doc_table = get_table_name("custom_documents", prefix)
    folders_table = get_table_name("folders", prefix)
    embeddings_table = get_table_name("embeddings", prefix)

    placeholders = ", ".join(["%s"] * len(user_ids))
    doc_query = f"SELECT doc_id FROM public.{doc_table} WHERE owner_id IN ({placeholders})"
    doc_params = list(user_ids)

    if filters.get("date_from"):
        doc_query += " AND created_at >= %s"
        doc_params.append(filters["date_from"])
    if filters.get("date_to"):
        doc_query += " AND created_at <= %s"
        doc_params.append(filters["date_to"])
    if filters.get("max_size"):
        doc_query += " AND doc_size <= %s"
        doc_params.append(filters["max_size"])

    doc_ids_df = execute_query(config, doc_query, tuple(doc_params))
    doc_ids = doc_ids_df["doc_id"].tolist() if not doc_ids_df.empty else []

    with st.expander("📁 Folders that will be copied", expanded=True):
        folders_query = f"""
            SELECT id, folder_name, owner_id, parent_id, created_at, folder_type
            FROM public.{folders_table}
            WHERE owner_id IN ({placeholders})
            ORDER BY created_at DESC
            LIMIT 200
        """
        folders_df = execute_query(config, folders_query, tuple(user_ids))
        st.caption(f"Rows shown: {len(folders_df)} (max 200)")
        if folders_df.empty:
            st.info("No folders found for selected users.")
        else:
            st.dataframe(folders_df, use_container_width=True, hide_index=True)

    with st.expander("🧮 Embeddings that will be copied", expanded=True):
        if not doc_ids:
            st.info("No matching documents found for current filters, so no embeddings to preview.")
        else:
            emb_placeholders = ", ".join(["%s"] * len(doc_ids))
            embeddings_query = f"""
                SELECT id, external_id, collection, metadata
                FROM public.{embeddings_table}
                WHERE metadata->>'doc_id' IN ({emb_placeholders})
                LIMIT 200
            """
            embeddings_df = execute_query(config, embeddings_query, tuple(doc_ids))
            st.caption(f"Rows shown: {len(embeddings_df)} (max 200)")
            if embeddings_df.empty:
                st.info("No embeddings found for selected documents.")
            else:
                st.dataframe(embeddings_df, use_container_width=True, hide_index=True)


def render_extraction_section(config: ConnectionConfig, prefix: str, user_emails: list):
    """Render the extraction section."""
    st.markdown("---")
    st.subheader("📥 Extract Data")
    
    if not user_emails:
        st.info("Select users above to enable extraction.")
        return
    
    filters = st.session_state.get(SessionKeys.DOCUMENT_FILTERS, {})
    
    # Export options
    col1, col2 = st.columns([1, 1])
    with col1:
        generate_sql = st.checkbox(
            "📝 Generate SQL migration files",
            value=True,
            help="Generate SQL INSERT statements for direct database execution"
        )
    with col2:
        export_csv = st.checkbox(
            "📄 Export CSV files",
            value=False,
            help="Export data as CSV files (can be disabled if you only need SQL)"
        )
    
    # SQL-specific options (shown only if SQL generation is enabled)
    _has_target = "target_config" in st.session_state
    if generate_sql:

        # ── Inline destination connection (to load Org IDs & Embedding Models) ──
        _target_label = (
            "✅ Destination DB connected — use buttons below to load Org IDs & Models"
            if _has_target
            else "🔌 Connect to destination DB — load Org IDs & Embedding Models"
        )
        with st.expander(_target_label, expanded=not _has_target):
            if _has_target:
                _tc = st.session_state["target_config"]
                st.caption(f"Connected: `{_tc.host}:{_tc.port}` (set via Target page or quick-connect below)")
                if st.button("❌ Disconnect", key="_dest_disconnect"):
                    for _k in ("target_config", "_dest_org_ids", "_dest_embedding_models"):
                        st.session_state.pop(_k, None)
                    st.rerun()
            else:
                st.caption(
                    "Enter destination credentials to fetch Org IDs from `user_db` and "
                    "Embedding Models from `document_db`. Credentials are used only for "
                    "these lookups and are stored in session state only."
                )
                _env_t = get_env_target_defaults()
                _dc1, _dc2, _dc3 = st.columns([3, 1, 2])
                with _dc1:
                    _t_host = st.text_input(
                        "Host", value=_env_t.get("host", ""),
                        key="_dest_host", placeholder="db.example.com"
                    )
                with _dc2:
                    _t_port = st.number_input(
                        "Port", value=int(_env_t.get("port", 5432)),
                        min_value=1, max_value=65535, key="_dest_port"
                    )
                with _dc3:
                    _t_user = st.text_input(
                        "Username", value=_env_t.get("username", ""),
                        key="_dest_user", placeholder="postgres"
                    )
                _t_pass = st.text_input(
                    "Password", type="password",
                    value=_env_t.get("password", ""),
                    key="_dest_pass", placeholder="••••••••"
                )
                if st.button(
                    "📡 Connect & load Org IDs and Embedding Models",
                    key="_dest_connect", type="primary", use_container_width=True
                ):
                    if not all([_t_host, _t_user, _t_pass]):
                        st.error("Please fill in host, username, and password.")
                    else:
                        _probe = ConnectionConfig(
                            host=_t_host, port=int(_t_port),
                            database="user_db",
                            username=_t_user, password=_t_pass
                        )
                        with st.spinner("Connecting…"):
                            _ok, _msg = test_connection(_probe)
                        if not _ok:
                            st.error(f"❌ {_msg}")
                        else:
                            st.session_state["target_config"] = _probe
                            _has_target = True
                            # Fetch both values immediately
                            with st.spinner("Loading Org IDs from user_db…"):
                                _org_ids = fetch_dest_org_ids()
                            with st.spinner("Loading Embedding Models from document_db…"):
                                _emb_models = fetch_dest_embedding_models()
                            if _org_ids:
                                st.session_state["_dest_org_ids"] = _org_ids
                            if _emb_models:
                                st.session_state["_dest_embedding_models"] = _emb_models
                            st.success(
                                f"✅ Connected! Loaded {len(_org_ids)} Org ID(s) and "
                                f"{len(_emb_models)} Embedding Model(s)."
                            )
                            st.rerun()

        col3, col4, col5, col6 = st.columns([2, 2, 1, 1])

        # ── Org ID ──────────────────────────────────────────────────────────
        with col3:
            _dest_org_ids = st.session_state.get("_dest_org_ids", [])

            if _dest_org_ids:
                # Destination values loaded — show selectbox
                org_id = st.selectbox(
                    "Org ID",
                    options=_dest_org_ids,
                    index=(
                        _dest_org_ids.index(get_env_org_id())
                        if get_env_org_id() in _dest_org_ids
                        else 0
                    ),
                    help="Organization IDs found in destination `user_db.users`. "
                         "Clear with the button below to enter manually."
                )
                if st.button("✕ Clear / enter manually", key="_clear_org_ids",
                             use_container_width=True):
                    del st.session_state["_dest_org_ids"]
                    st.rerun()
            else:
                # Manual entry (default)
                org_id = st.text_input(
                    "Org ID",
                    value=get_env_org_id(),
                    help="Organization UUID for SQL generation "
                         "(set DEFAULT_ORG_ID in .env to change default)"
                )
                if _has_target:
                    if st.button("📡 Load from destination", key="_load_org_ids",
                                 use_container_width=True):
                        with st.spinner("Querying user_db.users…"):
                            _fetched = fetch_dest_org_ids()
                        if _fetched:
                            st.session_state["_dest_org_ids"] = _fetched
                            st.rerun()
                        else:
                            st.warning("No organization_ids found in destination user_db.")

        # ── Embedding Model ─────────────────────────────────────────────────
        with col4:
            _env_model    = get_env_embedding_model()
            _custom_label = "Custom..."
            _dest_models  = st.session_state.get("_dest_embedding_models", [])

            # Merge presets + destination models (deduplicated, presets first)
            _combined = list(dict.fromkeys(EMBEDDING_MODEL_OPTIONS + _dest_models + [_custom_label]))

            _default_index = (
                _combined.index(_env_model)
                if _env_model in _combined
                else _combined.index(_custom_label)
            )
            _selected_model = st.selectbox(
                "Embedding Model",
                options=_combined,
                index=_default_index,
                help="Model name written into the embeddings table. "
                     "Destination models (if loaded) are merged with the preset list."
            )
            if _selected_model == _custom_label:
                embedding_model = st.text_input(
                    "Custom model name",
                    value=_env_model if _env_model not in EMBEDDING_MODEL_OPTIONS else "",
                    placeholder="e.g. my-custom-model",
                    label_visibility="collapsed"
                )
            else:
                embedding_model = _selected_model

            if _has_target:
                _btn_label = (
                    f"📡 Reload from destination ({len(_dest_models)} loaded)"
                    if _dest_models else "📡 Load from destination"
                )
                if st.button(_btn_label, key="_load_embedding_models",
                             use_container_width=True):
                    with st.spinner("Querying document_db.embeddings…"):
                        _fetched = fetch_dest_embedding_models()
                    if _fetched:
                        st.session_state["_dest_embedding_models"] = _fetched
                        st.rerun()
                    else:
                        st.warning("No model_names found in destination document_db.")
        with col5:
            skip_empty_embeddings = st.checkbox(
                "Skip empty",
                value=False,
                help="Skip rows without embeddings"
            )
        with col6:
            target_embedding_dim = st.number_input(
                "Target dim",
                min_value=0,
                max_value=4096,
                value=1024,
                step=1,
                help="Target embedding dimension. Source 1536 will be truncated to this value. Set to 0 to keep original dimension."
            )
            if target_embedding_dim == 0:
                target_embedding_dim = None
        
        # User ID overrides for users who already exist in V5 with a different UUID
        with st.expander("👤 User ID Overrides *(optional)*"):
            st.caption(
                "Use this when a user exists in both V4 and V5 but with different UUIDs. "
                "Their content will be migrated and linked to their existing V5 UUID instead of generating a new one. "
                "Enter one mapping per line in the format: `v4_uuid=v5_uuid`"
            )
            overrides_text = st.text_area(
                "V4 UUID → V5 UUID mappings",
                value="",
                height=100,
                placeholder="3fa85f64-5717-4562-b3fc-2c963f66afa6=7c9e6679-7425-40de-944b-e07fc1f90ae7",
                label_visibility="collapsed"
            )
        user_id_overrides = {}
        if overrides_text.strip():
            for _line in overrides_text.strip().splitlines():
                _line = _line.strip()
                if '=' in _line:
                    _parts = _line.split('=', 1)
                    if len(_parts) == 2:
                        _v4, _v5 = _parts[0].strip(), _parts[1].strip()
                        if _v4 and _v5:
                            user_id_overrides[_v4] = _v5
            if user_id_overrides:
                st.info(f"🔀 {len(user_id_overrides)} user ID override(s) configured.")
    else:
        org_id = "356b50f7-bcbd-42aa-9392-e1605f42f7a1"
        embedding_model = get_env_embedding_model()
        skip_empty_embeddings = False
        target_embedding_dim = None
        user_id_overrides = {}
    
    if st.button("🚀 Start Extraction", type="primary", use_container_width=True):
        # Create progress containers
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def progress_callback(table_name: str, current: int, total: int):
            progress_bar.progress(current / total)
            status_text.text(f"Extracting {table_name}... ({current}/{total})")
        
        # Create extraction engine
        engine = ExtractionEngine(
            config=config,
            prefix=prefix,
            output_dir=OUT_DIR,
            progress_callback=progress_callback,
            generate_sql=generate_sql,
            export_csv=export_csv,
            organization_id=org_id if generate_sql else None,
            embedding_model=embedding_model if generate_sql else 'text-embedding-ada-002',
            skip_empty_embeddings=skip_empty_embeddings if generate_sql else False,
            target_embedding_dim=target_embedding_dim if generate_sql else None,
            user_id_overrides=user_id_overrides if generate_sql else {}
        )
        
        # Run extraction
        with st.spinner("Extracting data..."):
            results = engine.run_full_extraction(
                user_emails=user_emails,
                date_from=filters.get("date_from"),
                date_to=filters.get("date_to"),
                max_doc_size=filters.get("max_size"),
                selected_doc_ids=st.session_state.get("selected_doc_ids"),
                selected_embedding_ids=st.session_state.get("selected_embedding_ids"),
                selected_agent_ids=st.session_state.get("selected_agent_ids"),
            )
        
        progress_bar.progress(1.0)
        status_text.text("Extraction complete!")
        
        # Store results
        st.session_state[SessionKeys.EXTRACTED_DATA] = results
        
        # Show results
        if results.get("errors"):
            for error in results["errors"]:
                st.error(error)
        else:
            st.success(f"✅ Extraction complete! Timestamp: {results['timestamp']}")

        # ── Agent-document topup report ──────────────────────────────────────
        topup = results.get("topup_report")
        if topup:
            added_docs    = topup.get("added_doc_ids", [])
            stale_docs    = topup.get("stale_doc_ids", [])
            added_folders = topup.get("added_folder_ids", [])
            stale_folders = topup.get("stale_folder_ids", [])
            oos_folders   = topup.get("out_of_scope_owner_folder_ids", [])

            st.subheader("🤖 Agent Document Coverage")

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("📄 Docs auto-added",    len(added_docs),
                      help="Documents not in original selection but required by agents — fetched automatically.")
            c2.metric("⚠️ Stale doc refs",     len(stale_docs),
                      help="Agent-referenced documents no longer found in V4. These links will be dropped.")
            c3.metric("📁 Folders auto-added", len(added_folders),
                      help="Folders (including ancestors) fetched because agents reference them.")
            c4.metric("⚠️ Stale folder refs",  len(stale_folders),
                      help="Agent-referenced folders no longer found in V4. These links will be dropped.")

            if added_docs:
                st.success(
                    f"✅ **{len(added_docs)} document(s)** were automatically added to the migration because "
                    f"selected agents depend on them. They appear in `03_documents_*.sql` annotated with "
                    f"`[agent-topup]`."
                )

            if stale_docs:
                with st.expander(f"⚠️ {len(stale_docs)} stale document reference(s) — links will be dropped"):
                    st.warning(
                        "These documents are referenced by agents in V4 but no longer exist in the source "
                        "database. The agent-document links cannot be migrated."
                    )
                    st.dataframe(
                        pd.DataFrame({"Stale doc_id": stale_docs}),
                        hide_index=True, use_container_width=True
                    )

            if stale_folders:
                with st.expander(f"⚠️ {len(stale_folders)} stale folder reference(s) — links will be dropped"):
                    st.warning("These folders are referenced by agents but no longer exist in V4.")
                    st.dataframe(
                        pd.DataFrame({"Stale folder_id": stale_folders}),
                        hide_index=True, use_container_width=True
                    )

            if oos_folders:
                with st.expander(
                    f"⚠️ {len(oos_folders)} auto-added folder(s) owned by users outside the migration scope"
                ):
                    st.warning(
                        "These folders were fetched because agents reference them, but their owner is not "
                        "among the selected users. They will be inserted into `document_db` without a "
                        "matching user record — verify this is acceptable before executing the SQL."
                    )
                    st.dataframe(
                        pd.DataFrame({"Folder id (out-of-scope owner)": oos_folders}),
                        hide_index=True, use_container_width=True
                    )

        # ── Extraction summary table ─────────────────────────────────────────
        # Show summary
        st.subheader("📊 Extraction Summary")
        summary_data = [
            {"Table": table, "Rows Extracted": count}
            for table, count in results.get("summary", {}).items()
        ]
        st.dataframe(pd.DataFrame(summary_data), hide_index=True)
        
        # Download buttons for CSV files
        st.subheader("📥 Download CSV Files")
        cols = st.columns(3)
        for i, (table, filepath) in enumerate(results.get("files", {}).items()):
            if os.path.exists(filepath):
                with cols[i % 3]:
                    with open(filepath, "rb") as f:
                        st.download_button(
                            label=f"📄 {table}.csv",
                            data=f,
                            file_name=os.path.basename(filepath),
                            mime="text/csv",
                            key=f"dl_csv_{table}"
                        )
        
        # Download buttons for SQL files (if generated)
        if results.get("sql_files"):
            st.subheader("📥 Download SQL Migration Files")
            st.info("💡 These SQL files can be executed directly with: `psql -h <host> -U <user> -d <database> -f <file>.sql`")
            
            # Show the host folder path (volume mounted)
            # Container path: /app/output/migrations -> Host path: ./output/migrations
            host_folder = "output/migrations"  # Relative to project root on host
            st.info(f"📂 **SQL files location (copy to File Explorer):**")
            st.code(host_folder, language=None)
            cols_sql = st.columns(3)
            for i, (table, filepath) in enumerate(results.get("sql_files", {}).items()):
                if os.path.exists(filepath):
                    with cols_sql[i % 3]:
                        with open(filepath, "rb") as f:
                            # Use actual filename with numbered prefix
                            display_name = os.path.basename(filepath)
                            st.download_button(
                                label=f"🗃️ {display_name}",
                                data=f,
                                file_name=display_name,
                                mime="text/plain",
                                key=f"dl_sql_{table}"
                            )
            
            # SQL file preview expanders
            st.subheader("👁️ SQL Files Preview")
            for table, filepath in results.get("sql_files", {}).items():
                if os.path.exists(filepath):
                    file_size = os.path.getsize(filepath)
                    size_str = f"{file_size / 1024:.1f} KB" if file_size < 1024 * 1024 else f"{file_size / (1024 * 1024):.1f} MB"
                    # Use actual filename with numbered prefix
                    display_name = os.path.basename(filepath)
                    
                    # Expander with inline download button
                    col_exp, col_btn = st.columns([10, 1])
                    with col_exp:
                        expander_label = f"🗃️ {display_name} ({size_str})"
                    with col_btn:
                        with open(filepath, "rb") as f:
                            st.download_button(
                                label="💾",
                                data=f,
                                file_name=display_name,
                                mime="text/plain",
                                key=f"save_sql_{table}",
                                help="Save SQL file"
                            )
                    
                    with st.expander(expander_label):
                        with open(filepath, "r", encoding="utf-8") as f:
                            # Read first 50KB for preview (large files truncated)
                            content = f.read(50000)
                            if file_size > 50000:
                                content += "\n\n-- [TRUNCATED - File too large for full preview] --"
                        st.code(content, language="sql")


def main():
    """Main page function."""
    if not check_connection():
        return
    
    config = st.session_state["source_config"]
    prefix = st.session_state.get(SessionKeys.TABLE_PREFIX, "jeen_dev")
    
    # User selection
    result = render_user_selection(config, prefix)
    if result is None:
        return
    
    selected_emails, selected_user_ids = result
    
    # User groups should appear under select users
    render_user_groups_under_users(config, prefix, selected_user_ids)
    
    # Documents filters + selection (default all selected)
    selected_doc_ids = render_document_selection(config, prefix, selected_user_ids)
    
    # Embeddings selection (default all selected)
    selected_embedding_ids = render_embeddings_selection(config, prefix, selected_doc_ids)
    
    # Agents selection (default all selected)
    selected_agent_ids = render_agents_selection(config, prefix, selected_user_ids)
    
    # Conversations selection (default all selected)
    selected_conversation_ids = render_conversations_selection(config, prefix, selected_user_ids)
    
    # Get current doc count for summary
    filters = st.session_state.get(SessionKeys.DOCUMENT_FILTERS, {})
    if selected_user_ids:
        doc_count = get_document_count_preview(
            config, prefix, selected_user_ids,
            filters.get("date_from"), filters.get("date_to"), filters.get("max_size")
        )
    else:
        doc_count = 0
    if isinstance(selected_doc_ids, list):
        doc_count = len(selected_doc_ids)
    
    # Related counts
    render_related_counts(config, prefix, selected_user_ids, doc_count)
    
    # Optional preview of folders/embeddings that will be copied
    render_copy_preview(config, prefix, selected_user_ids)
    
    # Extraction
    render_extraction_section(config, prefix, selected_emails)
    
    # Next step hint
    if SessionKeys.EXTRACTED_DATA in st.session_state:
        st.markdown("---")
        st.info("👉 **Next Step:** Go to **Transform** page to configure column mappings.")


if __name__ == "__main__":
    main()
