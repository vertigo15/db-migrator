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
from utils.config import SessionKeys, get_table_name
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
    
    # Save selection button
    if st.button("💾 Save Selection", type="secondary"):
        save_selected_users(selected_emails)
        st.success(f"Saved {len(selected_emails)} selected users!")
    
    # Selection summary
    st.metric("Selected Users", len(selected_emails))
    
    return selected_emails, selected_user_ids

def render_user_groups_under_users(config: ConnectionConfig, prefix: str, user_ids: list):
    """Show user groups for currently selected users (under user selection)."""
    st.subheader("👥 User Groups")
    if not user_ids:
        st.info("Select users to view their groups.")
        return
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
        return
    placeholders = ", ".join(["%s"] * len(user_ids))
    group_ids_query = f"""
        SELECT DISTINCT "{group_col}" AS group_id
        FROM public.{users_table}
        WHERE id IN ({placeholders}) AND "{group_col}" IS NOT NULL
    """
    gids_df = execute_query(config, group_ids_query, tuple(user_ids))
    if gids_df.empty:
        st.info("No user groups found for selected users.")
        return
    group_ids = gids_df["group_id"].astype(str).tolist()
    gp = ", ".join(["%s"] * len(group_ids))
    groups_query = f"""
        SELECT id, group_name, default_model, default_max_tokens_per_user
        FROM public.{groups_table}
        WHERE id IN ({gp})
        ORDER BY group_name
    """
    groups_df = execute_query(config, groups_query, tuple(group_ids))
    st.caption(f"Groups found: {len(groups_df)}")
    st.dataframe(groups_df, use_container_width=True, hide_index=True)


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
        SELECT bot_id, user_id, folder_id, created_at
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
    filtered_df = filtered_df[["selected", "bot_id", "user_id", "folder_id", "created_at"]]
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
            value=True,
            help="Export data as CSV files (can be disabled if you only need SQL)"
        )
    
    # SQL-specific options (shown only if SQL generation is enabled)
    if generate_sql:
        col3, col4, col5 = st.columns([2, 2, 1])
        with col3:
            org_id = st.text_input(
                "Org ID",
                value="356b50f7-bcbd-42aa-9392-e1605f42f7a1",
                help="Organization UUID for SQL generation"
            )
        with col4:
            embedding_model = st.text_input(
                "Embedding Model",
                value="BAAI/bge-m3",
                help="Default embedding model name for chunks/embeddings migration"
            )
        with col5:
            skip_empty_embeddings = st.checkbox(
                "Skip empty",
                value=False,
                help="Skip rows without embeddings"
            )
    else:
        org_id = "356b50f7-bcbd-42aa-9392-e1605f42f7a1"
        embedding_model = "BAAI/bge-m3"
        skip_empty_embeddings = False
    
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
            embedding_model=embedding_model if generate_sql else 'BAAI/bge-m3',
            skip_empty_embeddings=skip_empty_embeddings if generate_sql else False
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
            cols_sql = st.columns(3)
            for i, (table, filepath) in enumerate(results.get("sql_files", {}).items()):
                if os.path.exists(filepath):
                    with cols_sql[i % 3]:
                        with open(filepath, "rb") as f:
                            st.download_button(
                                label=f"🗃️ {table}.sql",
                                data=f,
                                file_name=os.path.basename(filepath),
                                mime="text/plain",
                                key=f"dl_sql_{table}"
                            )
        
        # Preview expanders
        st.subheader("👁️ Data Preview")
        for table, filepath in results.get("files", {}).items():
            if os.path.exists(filepath):
                with st.expander(f"📄 {table} ({results['summary'].get(table, 0)} rows)"):
                    preview_df = pd.read_csv(filepath, nrows=100)
                    st.dataframe(preview_df, use_container_width=True)


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
