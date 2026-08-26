"""
Page 5: Erase User Data V5

Features:
- Connect to a V5 database instance (user_db, document_db, completion_db)
- Select one or more users to erase
- Generate a deletion plan showing affected rows per table
- Execute deletion respecting FK dependencies (CASCADE)
- Post-delete verification across all tables
"""
import streamlit as st
import pandas as pd
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass

from utils.db import (
    ConnectionConfig,
    execute_query,
    get_connection,
    pooled_read_connection,
    test_connection,
)
from utils.config import SessionKeys, get_env_target_defaults
from utils.ui_performance import resolve_lazy_value

# Page config
st.set_page_config(page_title="Erase User Data V5", page_icon="🗑️", layout="wide")
st.title("🗑️ Erase User Data V5")

# ─────────────────────────────────────────────────────────────────────────────
# V5 database names (separate databases)
# ─────────────────────────────────────────────────────────────────────────────
V5_DATABASES = {
    "user_db": {
        "label": "User DB",
        "tables": {
            "user_roles": {"pk": "id", "user_col": "user_id"},
            "users":      {"pk": "id", "user_col": "id"},
        },
    },
    "document_db": {
        "label": "Document DB",
        "tables": {
            "folders":   {"pk": "id", "user_col": "user_id"},
            "documents": {"pk": "id", "user_col": "user_id"},
            "chunks":    {"pk": "id", "user_col": None, "parent": "documents", "fk": "document_id"},
        },
    },
    "completion_db": {
        "label": "Completion DB",
        "tables": {
            "conversations":          {"pk": "id", "user_col": "user_id"},
            "messages":               {"pk": "id", "user_col": "user_id", "parent": "conversations", "fk": "conversation_id"},
            "message_content_blocks": {"pk": "id", "user_col": None, "parent": "messages", "fk": "message_id"},
            "agents":                 {"pk": "id", "user_col": "user_id"},
            "agent_settings":         {"pk": "id", "user_col": None, "parent": "agents", "fk": "agent_id"},
            "knowledge_bases":        {"pk": "id", "user_col": None},
            "knowledge_base_assignments": {"pk": "id", "user_col": None},
            "knowledge_base_items":   {"pk": "id", "user_col": None},
        },
    },
}

# Deletion order: children first, then parents, then user record last.
# Within each database, respect CASCADE so we only need to delete the top-level
# rows — but we still count children for the plan display.
DELETION_STEPS = [
    # (database, table, description, query_template)
    # --- Migration tracking tables (delete BEFORE data so subqueries still work) ---
    ("completion_db", "legacy_bot_to_agent_mapping", "Legacy bot-to-agent tracking",
     "DELETE FROM public.legacy_bot_to_agent_mapping WHERE new_agent_id IN (SELECT id FROM public.agents WHERE user_id IN ({placeholders}))"),
    ("completion_db", "migration.id_mappings (conversations)", "Migration mappings for conversations",
     "DELETE FROM migration.id_mappings WHERE table_name = 'conversations' AND new_id IN (SELECT id FROM public.conversations WHERE user_id IN ({placeholders}))"),
    ("completion_db", "migration.id_mappings (messages)", "Migration mappings for messages",
     "DELETE FROM migration.id_mappings WHERE table_name = 'messages' AND new_id IN (SELECT id FROM public.messages WHERE user_id IN ({placeholders}))"),
    ("completion_db", "migration.id_mappings (agents)", "Migration mappings for agents",
     "DELETE FROM migration.id_mappings WHERE table_name = 'agents' AND new_id IN (SELECT id FROM public.agents WHERE user_id IN ({placeholders}))"),
    ("completion_db", "migration.id_mappings (conversions)", "Migration mappings for conversions",
     "DELETE FROM migration.id_mappings WHERE table_name = 'conversions' AND new_id IN (SELECT id FROM public.conversions WHERE user_id IN ({placeholders}))"),
    ("document_db", "migration.id_mappings (documents)", "Migration mappings for documents",
     "DELETE FROM migration.id_mappings WHERE table_name = 'documents' AND new_id IN (SELECT id FROM public.documents WHERE user_id IN ({placeholders}))"),
    ("document_db", "migration.id_mappings (folders)", "Migration mappings for folders",
     "DELETE FROM migration.id_mappings WHERE table_name = 'folders' AND new_id IN (SELECT id FROM public.folders WHERE user_id IN ({placeholders}))"),
    ("user_db", "migration.id_mappings (users)", "Migration mappings for users",
     "DELETE FROM migration.id_mappings WHERE table_name = 'users' AND new_id::text IN ({placeholders})"),
    ("user_db", "migration.migration_user_results", "Mark migration history as erased",
     "UPDATE migration.migration_user_results SET result = 'erased', completed_at = now() "
     "WHERE v5_user_id::text IN ({placeholders})"),
    # --- completion_db — message_content_blocks → messages → conversations (children first) ---
    ("completion_db", "message_content_blocks", "Message content blocks",
     "DELETE FROM public.message_content_blocks WHERE message_id IN (SELECT id FROM public.messages WHERE user_id IN ({placeholders}))"),
    ("completion_db", "messages", "Messages",
     "DELETE FROM public.messages WHERE user_id IN ({placeholders})"),
    ("completion_db", "conversations", "Conversations",
     "DELETE FROM public.conversations WHERE user_id IN ({placeholders})"),
    ("completion_db", "conversions", "Conversions",
     "DELETE FROM public.conversions WHERE user_id IN ({placeholders})"),
    # --- completion_db — agent knowledge-base links, then agents ---
    # Delete whole knowledge bases only when they are exclusively assigned to
    # selected agents. Shared KBs keep their items and only lose the agent assignment.
    ("completion_db", "knowledge_bases", "Knowledge bases exclusively assigned to agents",
     "WITH selected_agents AS (SELECT id FROM public.agents WHERE user_id IN ({placeholders})) "
     "DELETE FROM public.knowledge_bases kb WHERE EXISTS ("
     "SELECT 1 FROM public.knowledge_base_assignments kba "
     "WHERE kba.knowledge_base_id = kb.id "
     "AND kba.assigned_to_type = 'agent'::public.knowledge_base_assignments_assigned_to_type_enum "
     "AND kba.assigned_to_id IN (SELECT id FROM selected_agents)"
     ") AND NOT EXISTS ("
     "SELECT 1 FROM public.knowledge_base_assignments other_kba "
     "WHERE other_kba.knowledge_base_id = kb.id "
     "AND NOT ("
     "other_kba.assigned_to_type = 'agent'::public.knowledge_base_assignments_assigned_to_type_enum "
     "AND other_kba.assigned_to_id IN (SELECT id FROM selected_agents)"
     "))"),
    ("completion_db", "knowledge_base_assignments", "Knowledge base assignments for agents",
     "DELETE FROM public.knowledge_base_assignments "
     "WHERE assigned_to_type = 'agent'::public.knowledge_base_assignments_assigned_to_type_enum "
     "AND assigned_to_id IN (SELECT id FROM public.agents WHERE user_id IN ({placeholders}))"),
    ("completion_db", "agent_settings", "Agent settings",
     "DELETE FROM public.agent_settings WHERE agent_id IN (SELECT id FROM public.agents WHERE user_id IN ({placeholders}))"),
    ("completion_db", "agents", "Agents",
     "DELETE FROM public.agents WHERE user_id IN ({placeholders})"),
    # --- document_db — chunks CASCADE from documents ---
    ("document_db", "chunks", "Chunks & embeddings",
     "DELETE FROM public.chunks WHERE document_id IN (SELECT id FROM public.documents WHERE user_id IN ({placeholders}))"),
    ("document_db", "documents", "Documents",
     "DELETE FROM public.documents WHERE user_id IN ({placeholders})"),
    ("document_db", "folders", "Folders",
     "DELETE FROM public.folders WHERE user_id IN ({placeholders})"),
    # --- user_db — dynamic FK cleanup + user record (handled specially in execute) ---
    ("user_db", "users", "User account",
     "DELETE FROM public.users WHERE id IN ({placeholders})"),
]

# Counting queries for the plan / verification (same order as DELETION_STEPS)
COUNT_QUERIES = [
    ("completion_db", "legacy_bot_to_agent_mapping", "Legacy bot-to-agent tracking",
     "SELECT COUNT(*) FROM public.legacy_bot_to_agent_mapping WHERE new_agent_id IN (SELECT id FROM public.agents WHERE user_id IN ({placeholders}))"),
    ("completion_db", "migration.id_mappings (conversations)", "Migration mappings for conversations",
     "SELECT COUNT(*) FROM migration.id_mappings WHERE table_name = 'conversations' AND new_id IN (SELECT id FROM public.conversations WHERE user_id IN ({placeholders}))"),
    ("completion_db", "migration.id_mappings (messages)", "Migration mappings for messages",
     "SELECT COUNT(*) FROM migration.id_mappings WHERE table_name = 'messages' AND new_id IN (SELECT id FROM public.messages WHERE user_id IN ({placeholders}))"),
    ("completion_db", "migration.id_mappings (agents)", "Migration mappings for agents",
     "SELECT COUNT(*) FROM migration.id_mappings WHERE table_name = 'agents' AND new_id IN (SELECT id FROM public.agents WHERE user_id IN ({placeholders}))"),
    ("completion_db", "migration.id_mappings (conversions)", "Migration mappings for conversions",
     "SELECT COUNT(*) FROM migration.id_mappings WHERE table_name = 'conversions' AND new_id IN (SELECT id FROM public.conversions WHERE user_id IN ({placeholders}))"),
    ("document_db", "migration.id_mappings (documents)", "Migration mappings for documents",
     "SELECT COUNT(*) FROM migration.id_mappings WHERE table_name = 'documents' AND new_id IN (SELECT id FROM public.documents WHERE user_id IN ({placeholders}))"),
    ("document_db", "migration.id_mappings (folders)", "Migration mappings for folders",
     "SELECT COUNT(*) FROM migration.id_mappings WHERE table_name = 'folders' AND new_id IN (SELECT id FROM public.folders WHERE user_id IN ({placeholders}))"),
    ("user_db", "migration.id_mappings (users)", "Migration mappings for users",
     "SELECT COUNT(*) FROM migration.id_mappings WHERE table_name = 'users' AND new_id::text IN ({placeholders})"),
    ("user_db", "migration.migration_user_results", "Migration history rows to mark erased",
     "SELECT COUNT(*) FROM migration.migration_user_results WHERE v5_user_id::text IN ({placeholders})"),
    ("completion_db", "message_content_blocks", "Message content blocks",
     "SELECT COUNT(*) FROM public.message_content_blocks WHERE message_id IN (SELECT id FROM public.messages WHERE user_id IN ({placeholders}))"),
    ("completion_db", "messages", "Messages",
     "SELECT COUNT(*) FROM public.messages WHERE user_id IN ({placeholders})"),
    ("completion_db", "conversations", "Conversations",
     "SELECT COUNT(*) FROM public.conversations WHERE user_id IN ({placeholders})"),
    ("completion_db", "conversions", "Conversions",
     "SELECT COUNT(*) FROM public.conversions WHERE user_id IN ({placeholders})"),
    ("completion_db", "knowledge_base_items", "Knowledge base items for exclusively assigned agent KBs",
     "WITH selected_agents AS (SELECT id FROM public.agents WHERE user_id IN ({placeholders})) "
     "SELECT COUNT(*) FROM public.knowledge_base_items WHERE knowledge_base_id IN ("
     "SELECT kb.id FROM public.knowledge_bases kb WHERE EXISTS ("
     "SELECT 1 FROM public.knowledge_base_assignments kba "
     "WHERE kba.knowledge_base_id = kb.id "
     "AND kba.assigned_to_type = 'agent'::public.knowledge_base_assignments_assigned_to_type_enum "
     "AND kba.assigned_to_id IN (SELECT id FROM selected_agents)"
     ") AND NOT EXISTS ("
     "SELECT 1 FROM public.knowledge_base_assignments other_kba "
     "WHERE other_kba.knowledge_base_id = kb.id "
     "AND NOT ("
     "other_kba.assigned_to_type = 'agent'::public.knowledge_base_assignments_assigned_to_type_enum "
     "AND other_kba.assigned_to_id IN (SELECT id FROM selected_agents)"
     ")))"),
    ("completion_db", "knowledge_base_assignments", "Knowledge base assignments for agents",
     "SELECT COUNT(*) FROM public.knowledge_base_assignments "
     "WHERE assigned_to_type = 'agent'::public.knowledge_base_assignments_assigned_to_type_enum "
     "AND assigned_to_id IN (SELECT id FROM public.agents WHERE user_id IN ({placeholders}))"),
    ("completion_db", "knowledge_bases", "Knowledge bases exclusively assigned to agents",
     "WITH selected_agents AS (SELECT id FROM public.agents WHERE user_id IN ({placeholders})) "
     "SELECT COUNT(*) FROM public.knowledge_bases kb WHERE EXISTS ("
     "SELECT 1 FROM public.knowledge_base_assignments kba "
     "WHERE kba.knowledge_base_id = kb.id "
     "AND kba.assigned_to_type = 'agent'::public.knowledge_base_assignments_assigned_to_type_enum "
     "AND kba.assigned_to_id IN (SELECT id FROM selected_agents)"
     ") AND NOT EXISTS ("
     "SELECT 1 FROM public.knowledge_base_assignments other_kba "
     "WHERE other_kba.knowledge_base_id = kb.id "
     "AND NOT ("
     "other_kba.assigned_to_type = 'agent'::public.knowledge_base_assignments_assigned_to_type_enum "
     "AND other_kba.assigned_to_id IN (SELECT id FROM selected_agents)"
     "))"),
    ("completion_db", "agent_settings", "Agent settings",
     "SELECT COUNT(*) FROM public.agent_settings WHERE agent_id IN (SELECT id FROM public.agents WHERE user_id IN ({placeholders}))"),
    ("completion_db", "agents", "Agents",
     "SELECT COUNT(*) FROM public.agents WHERE user_id IN ({placeholders})"),
    ("document_db", "chunks", "Chunks & embeddings",
     "SELECT COUNT(*) FROM public.chunks WHERE document_id IN (SELECT id FROM public.documents WHERE user_id IN ({placeholders}))"),
    ("document_db", "documents", "Documents",
     "SELECT COUNT(*) FROM public.documents WHERE user_id IN ({placeholders})"),
    ("document_db", "folders", "Folders",
     "SELECT COUNT(*) FROM public.folders WHERE user_id IN ({placeholders})"),
    ("user_db", "users", "User account",
     "SELECT COUNT(*) FROM public.users WHERE id IN ({placeholders})"),
]

# These are migration artifacts, not core V5 entities. A partial migration can
# legitimately leave them absent because every generated SQL file is atomic.
OPTIONAL_RELATIONS = {
    ("completion_db", "public.legacy_bot_to_agent_mapping"),
    ("completion_db", "migration.id_mappings"),
    ("document_db", "migration.id_mappings"),
    ("user_db", "migration.id_mappings"),
    ("user_db", "migration.migration_user_results"),
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _make_config(base: ConnectionConfig, database: str) -> ConnectionConfig:
    """Clone a ConnectionConfig, switching only the database name."""
    return ConnectionConfig(
        host=base.host,
        port=base.port,
        database=database,
        username=base.username,
        password=base.password,
    )


def _relation_name(table_label: str) -> str:
    """Resolve a display label to its underlying schema-qualified relation."""
    base_name = table_label.split(" (", 1)[0]
    return base_name if "." in base_name else f"public.{base_name}"


def _relation_exists(
    base_config: ConnectionConfig,
    db_name: str,
    relation_name: str,
) -> Tuple[bool, Optional[str]]:
    """Check relation availability without causing a failed transaction."""
    config = _make_config(base_config, db_name)
    try:
        conn = get_connection(config)
        cur = conn.cursor()
        cur.execute("SELECT to_regclass(%s)", (relation_name,))
        exists = cur.fetchone()[0] is not None
        cur.close()
        conn.close()
        return exists, None
    except Exception as exc:
        return False, str(exc)


def _scan_relations(
    base_config: ConnectionConfig,
) -> Tuple[Dict[Tuple[str, str], bool], List[str]]:
    """Discover required/optional relations once before planning or deletion."""
    relation_keys = {
        (db_name, _relation_name(table))
        for db_name, table, _description, _query in COUNT_QUERIES
    }
    availability: Dict[Tuple[str, str], bool] = {}
    blockers: List[str] = []
    for db_name in V5_DATABASES:
        relations = sorted(
            relation_name
            for relation_db, relation_name in relation_keys
            if relation_db == db_name
        )
        try:
            with pooled_read_connection(_make_config(base_config, db_name)) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT relation_name, to_regclass(relation_name)::text
                        FROM unnest(%s::text[]) relation_name
                        """,
                        (relations,),
                    )
                    found = {name: regclass is not None for name, regclass in cur.fetchall()}
            for relation_name in relations:
                exists = found.get(relation_name, False)
                availability[(db_name, relation_name)] = exists
                if (
                    not exists
                    and (db_name, relation_name) not in OPTIONAL_RELATIONS
                ):
                    blockers.append(
                        f"Required relation {db_name}.{relation_name} does not exist"
                    )
        except Exception as exc:
            for relation_name in relations:
                availability[(db_name, relation_name)] = False
            blockers.append(f"Could not inspect {db_name} relations: {exc}")
    return availability, blockers


def _run_count(
    base_config: ConnectionConfig,
    db_name: str,
    query_template: str,
    user_ids: List[str],
) -> Tuple[Optional[int], Optional[str]]:
    """Run a COUNT query, returning (count, error) without rendering UI."""
    placeholders = ", ".join(["%s"] * len(user_ids))
    query = query_template.format(placeholders=placeholders)
    config = _make_config(base_config, db_name)
    try:
        with pooled_read_connection(config) as conn:
            with conn.cursor() as cur:
                cur.execute(query, tuple(user_ids))
                count = cur.fetchone()[0]
        return count, None
    except Exception as exc:
        return None, str(exc)


def _discover_user_fk_deps(cur) -> List[Tuple[str, str]]:
    """Query user_db information_schema to find all tables with FK refs to users(id).
    Returns list of (table_name, fk_column_name) excluding 'users' itself."""
    query = """
        SELECT DISTINCT
            kcu.table_name,
            kcu.column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND kcu.table_schema = 'public'
          AND ccu.table_name = 'users'
          AND ccu.column_name = 'id'
          AND kcu.table_name != 'users'
        ORDER BY kcu.table_name;
    """
    cur.execute(query)
    return cur.fetchall()


def _delete_user_fk_deps(
    cur,
    user_ids: List[str],
    log_lines: List[str],
    log_area,
) -> int:
    """Delete user_db FK dependents using the caller's transaction."""
    deps = _discover_user_fk_deps(cur)
    total_deleted = 0
    for table_name, col_name in deps:
        # Quote column name to handle camelCase (TypeORM)
        placeholders = ", ".join(["%s"] * len(user_ids))
        query = (
            f'DELETE FROM public."{table_name}" '
            f'WHERE "{col_name}" IN ({placeholders})'
        )
        log_lines.append(f"▶ Deleting from **user_db.{table_name}** (FK dep on users)...")
        log_area.markdown("\n\n".join(log_lines))
        cur.execute(query, tuple(user_ids))
        rows = cur.rowcount
        total_deleted += rows
        log_lines[-1] += f" ✅ {rows} rows staged"
        log_area.markdown("\n\n".join(log_lines))
    return total_deleted


def _build_deletion_plan(
    base_config: ConnectionConfig,
    user_ids: List[str],
) -> Dict:
    """Build a schema-aware plan and identify blockers before any mutation."""
    availability, blockers = _scan_relations(base_config)
    rows = []
    skipped_steps: Set[Tuple[str, str]] = set()
    total = 0

    for db_name, table, description, query_template in COUNT_QUERIES:
        relation_name = _relation_name(table)
        relation_key = (db_name, relation_name)
        step_key = (db_name, table)
        is_optional = relation_key in OPTIONAL_RELATIONS

        if not availability.get(relation_key, False):
            rows.append({
                "Database": db_name,
                "Table": table,
                "Description": description,
                "Rows to Affect": "NOT PRESENT" if is_optional else "BLOCKED",
                "Status": "Optional — skipped" if is_optional else "Required — missing",
            })
            skipped_steps.add(step_key)
            continue

        count, error = _run_count(
            base_config, db_name, query_template, user_ids
        )
        if error:
            if is_optional:
                skipped_steps.add(step_key)
                rows.append({
                    "Database": db_name,
                    "Table": table,
                    "Description": description,
                    "Rows to Affect": "SKIPPED",
                    "Status": f"Optional query incompatible: {error}",
                })
            else:
                blockers.append(
                    f"Required count failed on {db_name}.{table}: {error}"
                )
                rows.append({
                    "Database": db_name,
                    "Table": table,
                    "Description": description,
                    "Rows to Affect": "ERROR",
                    "Status": "Required query failed",
                })
            continue

        rows.append({
            "Database": db_name,
            "Table": table,
            "Description": description,
            "Rows to Affect": count,
            "Status": "Ready",
        })
        total += count or 0

    return {
        "rows": rows,
        "total": total,
        "availability": availability,
        "skipped_steps": skipped_steps,
        "blockers": blockers,
    }


# ─────────────────────────────────────────────────────────────────────────────
# UI Sections
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60, show_spinner=False)
def _load_v5_users(host, port, username, password, refresh_token):
    config = ConnectionConfig(host, int(port), "user_db", username, password)
    return execute_query(
        config,
        """
        SELECT id, first_name, last_name, email, organization_id, created_at
        FROM public.users
        ORDER BY email
        """,
    )


def _available_users_fingerprint(
    config: ConnectionConfig,
    users_df: pd.DataFrame,
) -> Tuple:
    """Identify the exact connection and user dataset shown to the operator."""
    columns = [
        column
        for column in ("id", "email", "organization_id", "created_at")
        if column in users_df.columns
    ]
    rows = tuple(
        tuple("" if pd.isna(value) else str(value) for value in row)
        for row in users_df[columns].itertuples(index=False, name=None)
    )
    return (
        config.host,
        config.port,
        config.username,
        rows,
    )


def _deletion_plan_fingerprint(
    base: ConnectionConfig,
    user_ids: List[str],
    dataset_fingerprint=None,
) -> Tuple:
    """Bind a deletion plan to one connection, dataset, and exact ID set."""
    return (
        base.host,
        base.port,
        base.username,
        dataset_fingerprint,
        tuple(sorted(str(user_id) for user_id in user_ids)),
    )


def _invalidate_deletion_plan() -> None:
    """Remove every confirmation derived from an older selection."""
    st.session_state.pop("v5_erase_plan", None)
    st.session_state.pop("v5_erase_plan_key", None)


def _clear_active_erasure_selection() -> None:
    """Clear active destructive state while retaining last-delete verification."""
    st.session_state.pop("v5_erase_selected_ids", None)
    _invalidate_deletion_plan()
    st.session_state["_v5_erase_editor_epoch"] = (
        st.session_state.get("_v5_erase_editor_epoch", 0) + 1
    )


def render_connection_form():
    """Render connection form for the V5 instance."""
    st.subheader("🔌 Connect to V5 Instance")

    st.info(
        "Provide the connection details for the **V5 PostgreSQL server**. "
        "The tool will connect to **user_db**, **document_db**, and **completion_db** "
        "automatically using the same host/port/credentials."
    )

    # Pre-fill from .env target defaults, then override with session state if available
    saved = get_env_target_defaults()
    session_conn = st.session_state.get(SessionKeys.TARGET_CONNECTION, {})
    for k, v in session_conn.items():
        if v:
            saved[k] = v

    with st.form("v5_erase_connection_form"):
        col1, col2 = st.columns(2)
        with col1:
            host = st.text_input("Host", value=saved.get("host", "localhost"))
            username = st.text_input("Username", value=saved.get("username", ""))
        with col2:
            port = st.number_input("Port", value=int(saved.get("port", 5432)),
                                   min_value=1, max_value=65535)
            password = st.text_input("Password", type="password",
                                     value=saved.get("password", ""))

        submitted = st.form_submit_button("🔗 Test V5 Connection", type="primary",
                                          use_container_width=True)

        if submitted:
            if not all([host, username, password]):
                st.error("Please fill in all required fields.")
                return

            # Test connectivity to each V5 database
            base = ConnectionConfig(host=host, port=port, database="user_db",
                                    username=username, password=password)
            results = {}
            all_ok = True
            for db_name in V5_DATABASES:
                cfg = _make_config(base, db_name)
                ok, msg = test_connection(cfg)
                results[db_name] = (ok, msg)
                if not ok:
                    all_ok = False

            # Store in session state
            if all_ok:
                st.session_state["v5_erase_config"] = base
                st.session_state["v5_erase_results"] = results
                st.session_state["_v5_erase_refresh_token"] = (
                    st.session_state.get("_v5_erase_refresh_token", 0) + 1
                )
                st.rerun()
            else:
                for db, (ok, msg) in results.items():
                    if ok:
                        st.success(f"✅ {db}: {msg}")
                    else:
                        st.error(f"❌ {db}: {msg}")

    # Show previous test results
    if "v5_erase_results" in st.session_state:
        for db, (ok, msg) in st.session_state["v5_erase_results"].items():
            if ok:
                st.success(f"✅ **{db}** — connected")
            else:
                st.error(f"❌ **{db}** — {msg}")


def render_user_selection():
    """Fetch users from user_db and let the operator pick who to erase."""
    if "v5_erase_config" not in st.session_state:
        return None

    st.markdown("---")
    st.subheader("👥 Select Users to Erase")

    base = st.session_state["v5_erase_config"]
    config = _make_config(base, "user_db")

    try:
        df = _load_v5_users(
            config.host,
            config.port,
            config.username,
            config.password,
            st.session_state.get("_v5_erase_refresh_token", 0),
        )
    except Exception as e:
        st.error(f"Failed to load users from user_db: {e}")
        return None

    if df.empty:
        st.warning("No users found in user_db.users.")
        _clear_active_erasure_selection()
        return None

    st.caption(f"Found **{len(df)}** users in `user_db.users`")

    dataset_fingerprint = _available_users_fingerprint(config, df)
    previous_dataset = st.session_state.get("_v5_erase_dataset_fingerprint")
    if (
        previous_dataset is not None
        and previous_dataset != dataset_fingerprint
    ):
        _clear_active_erasure_selection()
        st.info(
            "The available-user dataset changed. The prior selection and "
            "deletion plan were cleared."
        )
    st.session_state["_v5_erase_dataset_fingerprint"] = dataset_fingerprint

    prev_ids = st.session_state.get("v5_erase_selected_ids", [])
    search = st.text_input(
        "🔍 Search users",
        placeholder="Search by name or email...",
        key="erase_user_search",
    )
    filtered = df.copy()
    if search:
        mask = (
            filtered["first_name"].astype(str).str.contains(search, case=False, na=False)
            | filtered["last_name"].astype(str).str.contains(search, case=False, na=False)
            | filtered["email"].astype(str).str.contains(search, case=False, na=False)
        )
        filtered = filtered[mask]

    filtered["selected"] = filtered["id"].astype(str).isin(prev_ids)
    display_cols = ["selected", "id", "first_name", "last_name", "email", "organization_id", "created_at"]
    filtered = filtered[display_cols]
    edited = st.data_editor(
        filtered,
        column_config={
            "selected": st.column_config.CheckboxColumn("Select", default=False),
            "id": st.column_config.TextColumn("User ID"),
            "created_at": st.column_config.DatetimeColumn("Created", format="YYYY-MM-DD"),
        },
        hide_index=True,
        use_container_width=True,
        height=400,
        key=(
            "erase_users_editor_"
            f"{st.session_state.get('_v5_erase_editor_epoch', 0)}"
        ),
        disabled=[column for column in display_cols if column != "selected"],
    )
    selected_ids = (
        edited[edited["selected"] == True]["id"].astype(str).tolist()
    )
    if selected_ids != prev_ids:
        _invalidate_deletion_plan()
    st.session_state["v5_erase_selected_ids"] = selected_ids
    st.metric("Selected for Erasure", len(selected_ids))
    return selected_ids


def render_deletion_plan(user_ids: List[str]):
    """Show what will be deleted across all V5 databases."""
    if not user_ids:
        return None

    st.markdown("---")
    st.subheader("📋 Deletion Plan")
    st.warning(
        f"This permanently erases **all V5 data** for **{len(user_ids)}** "
        "selected user(s), including data that existed before migration."
    )
    st.info(
        "Migration mappings are cleaned up when present. Missing optional "
        "tracking tables are skipped. Batch/run history is preserved as audit data."
    )

    base = st.session_state["v5_erase_config"]
    plan_key = _deletion_plan_fingerprint(
        base,
        user_ids,
        st.session_state.get("_v5_erase_dataset_fingerprint"),
    )
    requested = st.button(
        "Build / Refresh deletion plan",
        type="secondary",
        use_container_width=True,
    )

    def _build():
        with st.spinner("Scanning tables..."):
            return _build_deletion_plan(base, user_ids)

    plan = resolve_lazy_value(
        st.session_state,
        value_key="v5_erase_plan",
        fingerprint_key="v5_erase_plan_key",
        fingerprint=plan_key,
        requested=requested,
        builder=_build,
    )

    if plan is None:
        st.info(
            "The expensive cross-database deletion plan is generated only on request."
        )
        return None

    plan_df = pd.DataFrame(plan["rows"])
    st.dataframe(plan_df, hide_index=True, use_container_width=True)
    st.metric("Total Rows to Affect", f"{plan['total']:,}")

    if plan["blockers"]:
        st.error(
            "Deletion is blocked. No data will be changed until these required "
            "preflight failures are resolved:"
        )
        for blocker in plan["blockers"]:
            st.error(blocker)
    else:
        skipped = len(plan["skipped_steps"])
        if skipped:
            st.warning(
                f"{skipped} optional migration cleanup step(s) will be skipped "
                "because their tracking relations are absent or incompatible."
            )

    return plan


def render_execute_deletion(user_ids: List[str]):
    """Execute the deletion with a confirmation gate."""
    if not user_ids or "v5_erase_plan" not in st.session_state:
        return

    st.markdown("---")
    st.subheader("⚡ Execute Deletion")

    base = st.session_state["v5_erase_config"]
    expected_plan_key = _deletion_plan_fingerprint(
        base,
        user_ids,
        st.session_state.get("_v5_erase_dataset_fingerprint"),
    )
    if st.session_state.get("v5_erase_plan_key") != expected_plan_key:
        st.error(
            "The deletion plan does not match the currently displayed "
            "selection. Build a fresh plan before execution."
        )
        return

    plan = st.session_state["v5_erase_plan"]
    if plan.get("blockers"):
        st.error("Execution is disabled because the deletion preflight is blocked.")
        return

    confirmed = st.checkbox(
        "I understand this erases the complete V5 user and pre-existing data, "
        "not only migration-created records.",
        key=f"confirm_full_v5_erasure_{abs(hash(expected_plan_key))}",
    )
    if not st.button(
        "🗑️ DELETE ALL SELECTED USER DATA",
        type="primary",
        use_container_width=True,
        disabled=not confirmed,
    ):
        return

    # Re-run preflight immediately before opening mutation transactions.
    plan = _build_deletion_plan(base, user_ids)
    st.session_state["v5_erase_plan"] = plan
    if plan["blockers"]:
        st.error("Preflight changed; deletion was cancelled before any mutation.")
        for blocker in plan["blockers"]:
            st.error(blocker)
        return

    progress = st.progress(0)
    log_area = st.empty()
    log_lines: List[str] = []
    connections = {}
    cursors = {}
    all_ok = False

    try:
        for db_name in V5_DATABASES:
            conn = get_connection(_make_config(base, db_name))
            conn.autocommit = False
            connections[db_name] = conn
            cursors[db_name] = conn.cursor()

        total_steps = len(DELETION_STEPS) + 1
        for index, (db_name, table, description, query_template) in enumerate(
            DELETION_STEPS
        ):
            progress.progress((index + 1) / total_steps)
            relation_key = (db_name, _relation_name(table))
            step_key = (db_name, table)

            if (
                step_key in plan["skipped_steps"]
                or not plan["availability"].get(relation_key, False)
            ):
                log_lines.append(
                    f"⏭️ Skipped optional **{db_name}.{table}** — relation not present."
                )
                log_area.markdown("\n\n".join(log_lines))
                continue

            cur = cursors[db_name]
            if db_name == "user_db" and table == "users":
                log_lines.append(
                    "▶ Discovering and staging deletion of user_db FK dependencies..."
                )
                log_area.markdown("\n\n".join(log_lines))
                fk_rows = _delete_user_fk_deps(
                    cur, user_ids, log_lines, log_area
                )
                log_lines.append(
                    f"✅ Staged {fk_rows} dynamic FK-dependent row(s)."
                )

            placeholders = ", ".join(["%s"] * len(user_ids))
            query = query_template.format(placeholders=placeholders)
            action = "Updating" if query.lstrip().upper().startswith("UPDATE") else "Deleting from"
            log_lines.append(
                f"▶ {action} **{db_name}.{table}** ({description})..."
            )
            log_area.markdown("\n\n".join(log_lines))
            cur.execute(query, tuple(user_ids))
            log_lines[-1] += f" ✅ {cur.rowcount} row(s) staged"
            log_area.markdown("\n\n".join(log_lines))

        # No database is committed until every statement in every database has
        # succeeded. User deletion commits last.
        for db_name in ("completion_db", "document_db", "user_db"):
            connections[db_name].commit()
            log_lines.append(f"✅ Committed **{db_name}** transaction.")
            log_area.markdown("\n\n".join(log_lines))
        all_ok = True
    except Exception as exc:
        for conn in connections.values():
            try:
                if not conn.closed:
                    conn.rollback()
            except Exception:
                pass
        log_lines.append(
            f"❌ FAILED: {exc}. All uncommitted database transactions were rolled back."
        )
        log_area.markdown("\n\n".join(log_lines))
    finally:
        for cur in cursors.values():
            try:
                cur.close()
            except Exception:
                pass
        for conn in connections.values():
            try:
                if not conn.closed:
                    conn.close()
            except Exception:
                pass

    progress.progress(1.0)
    if all_ok:
        st.success("✅ Complete V5 user erasure committed successfully.")
        st.session_state["_v5_erase_last_deleted_ids"] = list(user_ids)
        st.session_state["v5_erase_executed"] = True
        st.session_state["_v5_erase_refresh_token"] = (
            st.session_state.get("_v5_erase_refresh_token", 0) + 1
        )
        _clear_active_erasure_selection()
    else:
        st.error("Deletion failed. See the transaction log above.")


def render_verification(user_ids: List[str]):
    """Post-delete verification: re-count rows to confirm everything is gone."""
    if not user_ids:
        return
    if not st.session_state.get("v5_erase_executed"):
        return

    st.markdown("---")
    st.subheader("✅ Post-Delete Verification")

    if st.button("🔍 Verify Deletion", type="secondary", use_container_width=True):
        base = st.session_state["v5_erase_config"]

        with st.spinner("Verifying..."):
            verification = _build_deletion_plan(base, user_ids)

        verify_rows = []
        all_clean = not verification["blockers"]
        for row in verification["rows"]:
            remaining = row["Rows to Affect"]
            if isinstance(remaining, int):
                status = "✅ Clean" if remaining == 0 else "❌ REMAINING"
                if remaining != 0:
                    all_clean = False
            elif row["Status"].startswith("Optional"):
                status = "⏭️ Optional — skipped"
            else:
                status = "⚠️ Error"
                all_clean = False
            verify_rows.append({
                "Database": row["Database"],
                "Table": row["Table"],
                "Remaining Rows": remaining,
                "Status": status,
            })

        verify_df = pd.DataFrame(verify_rows)
        st.dataframe(verify_df, hide_index=True, use_container_width=True)

        if all_clean:
            st.success("🎉 Verification passed — all user data has been fully erased.")
        else:
            st.error(
                "⚠️ Some data remains. This may indicate FK constraints prevented deletion. "
                "Check the table details above."
            )


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    render_connection_form()

    selected_ids = render_user_selection()
    if selected_ids:
        render_deletion_plan(selected_ids)
        render_execute_deletion(selected_ids)

    last_deleted_ids = st.session_state.get("_v5_erase_last_deleted_ids", [])
    render_verification(last_deleted_ids)


main()
