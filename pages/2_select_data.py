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
import importlib.util
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
import requests

# Page config
st.set_page_config(page_title="Select Data", page_icon="📋", layout="wide")
st.title("📋 Select Data to Migrate")

# Output directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(BASE_DIR, "output", "extract")


def _source_scope_key(config: ConnectionConfig, prefix: str) -> str:
    """Stable key for caching reads from the source DB (unchanged when only SQL/org UI changes)."""
    d = config.to_dict()
    return f"{d['host']}:{d['port']}:{d['database']}:{d['username']}:{prefix}"


def _filters_fingerprint(filters: dict) -> str:
    """Stable fingerprint for document filter dict (may contain datetimes)."""
    if not filters:
        return "none"

    def _conv(v):
        if isinstance(v, datetime):
            return v.isoformat()
        if isinstance(v, date):
            return v.isoformat()
        return v

    try:
        return json.dumps(
            {str(k): _conv(v) for k, v in sorted(filters.items(), key=lambda kv: str(kv[0]))},
            sort_keys=True,
        )
    except TypeError:
        return repr(filters)


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
    """Query target admin_db for organizations (id + name).
    Falls back to user_db.users if admin_db is unavailable.
    Returns list of dicts: [{"id": "uuid", "name": "Org Name"}, ...]
    """
    cfg = _make_target_config("admin_db")
    if cfg is not None:
        try:
            df = execute_query(
                cfg,
                "SELECT id::text AS org_id, name AS org_name "
                "FROM public.organizations "
                "WHERE is_active = true "
                "ORDER BY name"
            )
            if not df.empty:
                return [{"id": r["org_id"], "name": r["org_name"]} for _, r in df.iterrows()]
        except Exception:
            pass

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
        return [{"id": r["org_id"], "name": None} for _, r in df.iterrows()] if not df.empty else []
    except Exception:
        return []


def _org_name_label(org: dict) -> str:
    """Label for org name selectbox (name from DB; fallback if missing)."""
    name = (org.get("name") or "").strip()
    if name:
        return name
    oid = str(org.get("id") or "")
    return oid if len(oid) <= 48 else f"{oid[:8]}…{oid[-4:]}"


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

    _usk = _source_scope_key(config, prefix)
    if st.session_state.get("_p2_users_scope") != _usk:
        with st.spinner("Loading users..."):
            users_df = load_users_data(config, prefix)
        st.session_state["_p2_users_scope"] = _usk
        st.session_state["_p2_users_df"] = users_df
    else:
        users_df = st.session_state["_p2_users_df"]
    
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

    _filters_snap = {"date_from": date_from_dt, "date_to": date_to_dt, "max_size": max_size_val}
    # Get preview count (cached — avoids re-query on unrelated widget changes e.g. org select)
    _fp = _source_scope_key(config, prefix)
    _uids = tuple(sorted(str(u) for u in user_ids))
    _dcc_key = f"{_fp}|{_uids}|{_filters_fingerprint(_filters_snap)}"
    if st.session_state.get("_p2_doccount_key") == _dcc_key:
        doc_count = st.session_state["_p2_doccount_val"]
    else:
        with st.spinner("Counting matching documents..."):
            doc_count = get_document_count_preview(
                config, prefix, user_ids,
                date_from_dt, date_to_dt, max_size_val
            )
        st.session_state["_p2_doccount_key"] = _dcc_key
        st.session_state["_p2_doccount_val"] = doc_count
    
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
    _dk = f"{_source_scope_key(config, prefix)}|{tuple(sorted(str(u) for u in user_ids))}|{_filters_fingerprint(filters)}"
    if st.session_state.get("_p2_docs_df_key") == _dk:
        docs_df = st.session_state["_p2_docs_df"]
    else:
        with st.spinner("Loading documents..."):
            docs_df = _load_documents_df(config, prefix, user_ids, filters)
        st.session_state["_p2_docs_df_key"] = _dk
        st.session_state["_p2_docs_df"] = docs_df.copy()
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
    _ek = f"{_source_scope_key(config, prefix)}|{tuple(sorted(str(d) for d in doc_ids))}"
    if st.session_state.get("_p2_emb_df_key") == _ek:
        emb_df = st.session_state["_p2_emb_df"]
    else:
        with st.spinner("Loading embeddings..."):
            emb_df = execute_query(config, query, tuple(doc_ids))
        st.session_state["_p2_emb_df_key"] = _ek
        st.session_state["_p2_emb_df"] = emb_df.copy()
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
    _ck = f"{_source_scope_key(config, prefix)}|{tuple(sorted(str(u) for u in user_ids))}"
    if st.session_state.get("_p2_conv_df_key") == _ck:
        convs_df = st.session_state["_p2_conv_df"]
    else:
        with st.spinner("Loading conversations..."):
            convs_df = execute_query(config, query, tuple(user_ids))
        st.session_state["_p2_conv_df_key"] = _ck
        st.session_state["_p2_conv_df"] = convs_df.copy()
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
    _ak = f"{_source_scope_key(config, prefix)}|{tuple(sorted(str(u) for u in user_ids))}"
    if st.session_state.get("_p2_agents_df_key") == _ak:
        agents_df = st.session_state["_p2_agents_df"]
    else:
        with st.spinner("Loading agents..."):
            agents_df = execute_query(config, query, tuple(user_ids))
        st.session_state["_p2_agents_df_key"] = _ak
        st.session_state["_p2_agents_df"] = agents_df.copy()
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


PROMPT_MERGER_URL = os.getenv("PROMPT_MERGER_URL", "http://localhost:8100")
PROMPTS_MODULE_PATH = os.path.join(BASE_DIR, "prompt-merger", "prompts.py")


def _load_prompt_constants():
    """Load prompt merger constants from prompt-merger/prompts.py."""
    spec = importlib.util.spec_from_file_location("prompt_merger_prompts", PROMPTS_MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_template_prompt() -> str:
    """Load the template prompt shown to users and sent to the prompt merger."""
    try:
        return _load_prompt_constants().TEMPLATE_PROMPT
    except Exception:
        return ""


def _load_company_name_options() -> list:
    """Load company name options used by the prompt merger UI."""
    try:
        return list(_load_prompt_constants().COMPANY_NAME_OPTIONS)
    except Exception:
        return ["IAI", "Isracard", "Maccabi"]


def _text_or_empty(value) -> str:
    return value if isinstance(value, str) and value.strip() else ""


def render_merged_prompt_review():
    """Show what was extracted, the template, and the final merged prompt per agent."""
    review_items = st.session_state.get("merged_prompt_review") or []
    if not review_items:
        return

    st.markdown("#### 👁️ Review Prompt Merge Results")
    st.caption(
        "Review the extracted V4 prompt parts, the template sent to the LLM, "
        "and the final prompt that will be injected into SQL generation."
    )

    summary_df = pd.DataFrame([
        {
            "bot_id": item.get("bot_id"),
            "status": item.get("status"),
            "has_tone": bool(item.get("tone")),
            "has_guardrail": bool(item.get("guardrail")),
            "has_response": bool(item.get("response")),
            "merged_chars": len(item.get("merged_instruction") or ""),
        }
        for item in review_items
    ])
    st.dataframe(summary_df, hide_index=True, use_container_width=True)

    bot_ids = [item["bot_id"] for item in review_items]
    selected_bot_id = st.selectbox(
        "Agent to review",
        options=bot_ids,
        key="merged_prompt_review_agent",
    )
    selected = next(item for item in review_items if item["bot_id"] == selected_bot_id)

    status = selected.get("status", "unknown")
    if status == "ok":
        st.success("This merged prompt came from the LLM and is ready for SQL generation.")
    elif status == "template_only":
        st.info("This agent had no prompt parts, so the company-injected template will be used.")
    elif status == "fallback":
        st.warning(
            "The LLM call failed for this agent. The fallback concatenation will be injected "
            "unless you rerun the merge successfully."
        )
        if selected.get("error_message"):
            st.caption(selected["error_message"])

    extracted_tab, template_tab, merged_tab = st.tabs([
        "Extracted V4 Parts",
        "Template",
        "New V5 Prompt",
    ])
    with extracted_tab:
        st.text_area("Tone", value=selected.get("tone") or "(not provided)", height=180, disabled=True)
        st.text_area("Guardrail", value=selected.get("guardrail") or "(not provided)", height=180, disabled=True)
        st.text_area("Response", value=selected.get("response") or "(not provided)", height=180, disabled=True)
    with template_tab:
        st.text_area(
            "Template sent to prompt merger",
            value=selected.get("template") or "(template not available)",
            height=420,
            disabled=True,
        )
    with merged_tab:
        st.text_area(
            "New prompt injected during SQL generation",
            value=selected.get("merged_instruction") or "(no merged prompt returned)",
            height=520,
            disabled=True,
        )


def _extract_prompt_parts(config: ConnectionConfig, prefix: str, bot_ids: list) -> dict:
    """Fetch the 3 prompt parts (tone/guardrail/response) for each agent from source DB.

    Returns {bot_id: {"tone": ..., "guardrail": ..., "response": ...}}
    """
    if not bot_ids:
        return {}

    agents_table = get_table_name("agents", prefix)
    placeholders = ", ".join(["%s"] * len(bot_ids))
    query = f"""
        SELECT bot_id, character_prompts, hack_prompt, relevant_answer_prompt
        FROM public.{agents_table}
        WHERE bot_id IN ({placeholders})
    """
    df = execute_query(config, query, tuple(bot_ids))
    result = {}
    for _, row in df.iterrows():
        bid = str(row.get("bot_id", ""))

        def _get_content(col_val):
            if col_val is None:
                return None
            if isinstance(col_val, str):
                try:
                    col_val = json.loads(col_val)
                except Exception:
                    return col_val.strip() or None
            if isinstance(col_val, dict):
                return (col_val.get("content") or "").strip() or None
            return None

        result[bid] = {
            "tone": _get_content(row.get("character_prompts")),
            "guardrail": _get_content(row.get("hack_prompt")),
            "response": _get_content(row.get("relevant_answer_prompt")),
        }
    return result


def render_prompt_merger_section(config: ConnectionConfig, prefix: str, agent_ids: list):
    """Render the prompt merger UI: company selector + merge button + progress."""
    st.markdown("---")
    st.subheader("🔀 Merge Agent Prompts (V4 → V5)")
    st.caption(
        "Combine each agent's Tone, Guardrail, and Response prompts into a single "
        "structured V5 instruction using the on-prem LLM."
    )

    if not agent_ids:
        st.info("Select agents above to enable prompt merging.")
        return

    col1, col2 = st.columns([2, 3], vertical_alignment="bottom")
    with col1:
        company_name_options = _load_company_name_options()
        company_name = st.selectbox(
            "Company Name",
            options=company_name_options,
            index=0,
            help="Injected into the template where [Company/Brand Name] appears.",
            key="company_name_select",
        )
        st.session_state["company_name"] = company_name

    with col2:
        st.info(f"**{len(agent_ids)}** agents selected for prompt merging.")

    if st.button("🚀 Merge Agent Prompts", type="primary", use_container_width=False, key="merge_prompts_btn"):
        progress_bar = st.progress(0)
        status_text = st.empty()

        status_text.text("Extracting prompt parts from source DB...")
        prompt_parts = _extract_prompt_parts(config, prefix, agent_ids)
        template_text = _load_template_prompt()

        merge_requests = []
        for bot_id in agent_ids:
            parts = prompt_parts.get(bot_id, {})
            merge_requests.append({
                "bot_id": bot_id,
                "tone": parts.get("tone"),
                "guardrail": parts.get("guardrail"),
                "response": parts.get("response"),
            })

        status_text.text(f"Sending {len(merge_requests)} agents to prompt merger service...")
        progress_bar.progress(0.1)

        request_url = f"{PROMPT_MERGER_URL}/merge-prompts/batch"

        try:
            resp = requests.post(
                request_url,
                json={
                    "agents": merge_requests,
                    "company_name": company_name,
                    "template": template_text,
                },
                timeout=600,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.ConnectionError as exc:
            st.error(
                f"Could not connect to prompt merger service at `{PROMPT_MERGER_URL}`. "
                "Make sure it is running: `cd db-migrator/prompt-merger && uvicorn main:app --port 8100`"
            )
            return
        except Exception as exc:
            st.error(f"Prompt merger request failed: {exc}")
            return

        progress_bar.progress(1.0)
        status_text.text("Prompt merging complete!")

        merged = {}
        results_by_bot = {}
        for r in data.get("results", []):
            merged[r["bot_id"]] = r["merged_instruction"]
            results_by_bot[r["bot_id"]] = r
        st.session_state["merged_instructions"] = merged
        st.session_state["merged_prompt_review"] = [
            {
                "bot_id": bot_id,
                "tone": _text_or_empty(prompt_parts.get(bot_id, {}).get("tone")),
                "guardrail": _text_or_empty(prompt_parts.get(bot_id, {}).get("guardrail")),
                "response": _text_or_empty(prompt_parts.get(bot_id, {}).get("response")),
                "template": template_text,
                "merged_instruction": results_by_bot.get(bot_id, {}).get("merged_instruction", ""),
                "status": results_by_bot.get(bot_id, {}).get("status", "missing"),
                "error_message": results_by_bot.get(bot_id, {}).get("error_message"),
            }
            for bot_id in agent_ids
        ]

        ok_count = data.get("succeeded", 0)
        fail_count = data.get("failed", 0)
        total = data.get("total", 0)

        c1, c2, c3 = st.columns(3)
        c1.metric("Total", total)
        c2.metric("Succeeded", ok_count)
        c3.metric("Fallback / Failed", fail_count)

        statuses = {}
        for r in data.get("results", []):
            s = r.get("status", "unknown")
            statuses[s] = statuses.get(s, 0) + 1
        if statuses:
            st.caption("Status breakdown: " + ", ".join(f"{k}: {v}" for k, v in statuses.items()))

        st.success(
            f"Merged instructions stored for **{len(merged)}** agent(s). "
            "They will be used during extraction."
        )

    if st.session_state.get("merged_instructions"):
        n = len(st.session_state["merged_instructions"])
        st.success(f"✅ {n} merged instruction(s) ready in session. They will be injected during SQL generation.")
        render_merged_prompt_review()


def render_related_counts(config: ConnectionConfig, prefix: str, user_ids: list, doc_count: int):
    """Render related data counts."""
    st.markdown("---")
    st.subheader("📊 Related Data Summary")
    
    if not user_ids:
        st.info("Select users to see related data counts.")
        return
    
    filters = st.session_state.get(SessionKeys.DOCUMENT_FILTERS, {})
    _rk = f"{_source_scope_key(config, prefix)}|{tuple(sorted(str(u) for u in user_ids))}|{_filters_fingerprint(filters)}"
    if st.session_state.get("_p2_related_key") == _rk:
        doc_ids = st.session_state["_p2_related_doc_ids"]
        counts = st.session_state["_p2_related_counts"]
        est_size = st.session_state.get("_p2_related_est_size", 0.0)
    else:
        with st.spinner("Calculating related data..."):
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

            counts = get_related_counts(config, prefix, user_ids, doc_ids)
            est_size = (
                estimate_embeddings_size(config, prefix, doc_ids) if doc_ids else 0.0
            )

        st.session_state["_p2_related_key"] = _rk
        st.session_state["_p2_related_doc_ids"] = doc_ids
        st.session_state["_p2_related_counts"] = counts
        st.session_state["_p2_related_est_size"] = est_size
    
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
    if doc_ids and est_size > 500:
        st.warning(
            f"⚠️ Estimated embeddings size: {est_size:.1f} MB. "
            "Consider batched extraction for large datasets."
        )
    
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
                            with st.spinner("Loading Organizations…"):
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

        col3, col4, col5 = st.columns([2, 2, 1], vertical_alignment="bottom")

        # ── Organization (name + UUID for SQL) ─────────────────────────────
        with col3:
            _dest_orgs = st.session_state.get("_dest_org_ids", [])

            if _dest_orgs:
                _env_org = get_env_org_id()
                _default_idx = 0
                for _i, _o in enumerate(_dest_orgs):
                    if str(_o.get("id")) == str(_env_org):
                        _default_idx = _i
                        break
                _org_row = st.selectbox(
                    "Org Name",
                    options=list(range(len(_dest_orgs))),
                    index=_default_idx,
                    format_func=lambda i: _org_name_label(_dest_orgs[i]),
                    key="_extract_sql_org_row",
                    help="From destination `admin_db.organizations` (or user_db fallback). "
                         "Only the name is shown here; UUID is used in generated SQL.",
                )
                org_id = str(_dest_orgs[int(_org_row)]["id"])
                # Key includes org_id so this field refreshes when selection changes
                # (a fixed-key disabled text_input can show a stale UUID in Streamlit).
                st.text_input(
                    "Org UUID",
                    value=org_id,
                    disabled=True,
                    key=f"_extract_org_uuid_{org_id}",
                    help="Organization id written into migration SQL.",
                )

                if st.button("✕ Clear / enter manually", key="_clear_org_ids",
                             use_container_width=True):
                    del st.session_state["_dest_org_ids"]
                    st.rerun()
            else:
                org_id = st.text_input(
                    "Org UUID",
                    value=get_env_org_id(),
                    help="Organization UUID for SQL generation "
                         "(set DEFAULT_ORG_ID in .env to change default)"
                )
                if _has_target:
                    if st.button("📡 Load from destination", key="_load_org_ids",
                                 use_container_width=True):
                        with st.spinner("Querying admin_db.organizations…"):
                            _fetched = fetch_dest_org_ids()
                        if _fetched:
                            st.session_state["_dest_org_ids"] = _fetched
                            st.rerun()
                        else:
                            st.warning("No organizations found in destination.")

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
            skip_empty_embeddings = st.checkbox(
                "Skip empty",
                value=False,
                help="Skip rows without embeddings"
            )
        
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
        org_id = get_env_org_id()
        embedding_model = get_env_embedding_model()
        skip_empty_embeddings = False
        target_embedding_dim = None
        user_id_overrides = {}
    
    if st.button("🚀 Start Extraction", type="primary", use_container_width=True):
        if generate_sql and not org_id:
            st.error("Please select an organization before starting extraction.")
            st.stop()
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
                merged_instructions=st.session_state.get("merged_instructions"),
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
    
    # Documents filters + selection (default all selected)
    selected_doc_ids = render_document_selection(config, prefix, selected_user_ids)
    
    # Embeddings selection (default all selected)
    selected_embedding_ids = render_embeddings_selection(config, prefix, selected_doc_ids)
    
    # Agents selection (default all selected)
    selected_agent_ids = render_agents_selection(config, prefix, selected_user_ids)

    # Prompt merger (optional step — merge V4 prompt parts into V5 template)
    render_prompt_merger_section(config, prefix, selected_agent_ids or [])

    # Conversations selection (default all selected)
    selected_conversation_ids = render_conversations_selection(config, prefix, selected_user_ids)
    
    # Doc count for summary (reuse cache from document filters when possible)
    filters = st.session_state.get(SessionKeys.DOCUMENT_FILTERS, {})
    if selected_user_ids:
        _dc_main_k = (
            f"{_source_scope_key(config, prefix)}|"
            f"{tuple(sorted(str(u) for u in selected_user_ids))}|"
            f"{_filters_fingerprint(filters)}"
        )
        if st.session_state.get("_p2_doccount_key") == _dc_main_k:
            doc_count = st.session_state["_p2_doccount_val"]
        else:
            doc_count = get_document_count_preview(
                config, prefix, selected_user_ids,
                filters.get("date_from"), filters.get("date_to"), filters.get("max_size"),
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
