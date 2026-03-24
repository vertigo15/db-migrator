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
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from utils.db import ConnectionConfig, get_connection, test_connection
from utils.config import SessionKeys, get_env_target_defaults

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
            "agent_documents":        {"pk": "id", "user_col": None, "parent": "agents", "fk": "agent_id"},
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
    ("document_db", "migration.id_mappings (documents)", "Migration mappings for documents",
     "DELETE FROM migration.id_mappings WHERE table_name = 'documents' AND new_id IN (SELECT id FROM public.documents WHERE user_id IN ({placeholders}))"),
    ("document_db", "migration.id_mappings (folders)", "Migration mappings for folders",
     "DELETE FROM migration.id_mappings WHERE table_name = 'folders' AND new_id IN (SELECT id FROM public.folders WHERE user_id IN ({placeholders}))"),
    ("user_db", "migration.id_mappings (users)", "Migration mappings for users",
     "DELETE FROM migration.id_mappings WHERE table_name = 'users' AND new_id::text IN ({placeholders})"),
    # --- completion_db — message_content_blocks → messages → conversations (children first) ---
    ("completion_db", "message_content_blocks", "Message content blocks",
     "DELETE FROM public.message_content_blocks WHERE message_id IN (SELECT id FROM public.messages WHERE user_id IN ({placeholders}))"),
    ("completion_db", "messages", "Messages",
     "DELETE FROM public.messages WHERE user_id IN ({placeholders})"),
    ("completion_db", "conversations", "Conversations",
     "DELETE FROM public.conversations WHERE user_id IN ({placeholders})"),
    # --- completion_db — agents CASCADE to agent_settings + agent_documents ---
    ("completion_db", "agent_documents", "Agent ↔ Document links",
     "DELETE FROM public.agent_documents WHERE agent_id IN (SELECT id FROM public.agents WHERE user_id IN ({placeholders}))"),
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
    # --- Migration batch logs (clean up after all data is deleted) ---
    ("user_db", "migration.batch_log", "Migration batch log (user_db)",
     "DELETE FROM migration.batch_log WHERE table_name = 'users'"),
    ("document_db", "migration.batch_log", "Migration batch log (document_db)",
     "DELETE FROM migration.batch_log WHERE table_name IN ('folders', 'documents', 'chunks_embeddings')"),
    ("completion_db", "migration.batch_log", "Migration batch log (completion_db)",
     "DELETE FROM migration.batch_log WHERE table_name IN ('agents', 'conversations', 'messages', 'message_content_blocks')"),
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
    ("document_db", "migration.id_mappings (documents)", "Migration mappings for documents",
     "SELECT COUNT(*) FROM migration.id_mappings WHERE table_name = 'documents' AND new_id IN (SELECT id FROM public.documents WHERE user_id IN ({placeholders}))"),
    ("document_db", "migration.id_mappings (folders)", "Migration mappings for folders",
     "SELECT COUNT(*) FROM migration.id_mappings WHERE table_name = 'folders' AND new_id IN (SELECT id FROM public.folders WHERE user_id IN ({placeholders}))"),
    ("user_db", "migration.id_mappings (users)", "Migration mappings for users",
     "SELECT COUNT(*) FROM migration.id_mappings WHERE table_name = 'users' AND new_id::text IN ({placeholders})"),
    ("completion_db", "message_content_blocks", "Message content blocks",
     "SELECT COUNT(*) FROM public.message_content_blocks WHERE message_id IN (SELECT id FROM public.messages WHERE user_id IN ({placeholders}))"),
    ("completion_db", "messages", "Messages",
     "SELECT COUNT(*) FROM public.messages WHERE user_id IN ({placeholders})"),
    ("completion_db", "conversations", "Conversations",
     "SELECT COUNT(*) FROM public.conversations WHERE user_id IN ({placeholders})"),
    ("completion_db", "agent_documents", "Agent ↔ Document links",
     "SELECT COUNT(*) FROM public.agent_documents WHERE agent_id IN (SELECT id FROM public.agents WHERE user_id IN ({placeholders}))"),
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
    ("user_db", "migration.batch_log", "Migration batch log (user_db)",
     "SELECT COUNT(*) FROM migration.batch_log WHERE table_name = 'users'"),
    ("document_db", "migration.batch_log", "Migration batch log (document_db)",
     "SELECT COUNT(*) FROM migration.batch_log WHERE table_name IN ('folders', 'documents', 'chunks_embeddings')"),
    ("completion_db", "migration.batch_log", "Migration batch log (completion_db)",
     "SELECT COUNT(*) FROM migration.batch_log WHERE table_name IN ('agents', 'conversations', 'messages', 'message_content_blocks')"),
]


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


def _run_count(base_config: ConnectionConfig, db_name: str, query_template: str,
               user_ids: List[str]) -> int:
    """Run a COUNT(*) query against the given database, returning the integer count."""
    placeholders = ", ".join(["%s"] * len(user_ids))
    query = query_template.format(placeholders=placeholders)
    config = _make_config(base_config, db_name)
    try:
        conn = get_connection(config)
        cur = conn.cursor()
        cur.execute(query, tuple(user_ids))
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except Exception as e:
        st.error(f"Count query failed on {db_name}: {e}")
        return -1


def _run_delete(base_config: ConnectionConfig, db_name: str, query_template: str,
                user_ids: List[str]) -> Tuple[bool, int, Optional[str]]:
    """Execute a DELETE statement. Returns (success, rows_affected, error_msg)."""
    placeholders = ", ".join(["%s"] * len(user_ids))
    query = query_template.format(placeholders=placeholders)
    config = _make_config(base_config, db_name)
    conn = None
    try:
        conn = get_connection(config)
        cur = conn.cursor()
        cur.execute(query, tuple(user_ids))
        rows = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        return True, rows, None
    except Exception as e:
        if conn:
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass
        return False, 0, str(e)


def _discover_user_fk_deps(base_config: ConnectionConfig) -> List[Tuple[str, str]]:
    """Query user_db information_schema to find all tables with FK refs to users(id).
    Returns list of (table_name, fk_column_name) excluding 'users' itself."""
    config = _make_config(base_config, "user_db")
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
          AND ccu.table_name = 'users'
          AND ccu.column_name = 'id'
          AND kcu.table_name != 'users'
        ORDER BY kcu.table_name;
    """
    try:
        conn = get_connection(config)
        cur = conn.cursor()
        cur.execute(query)
        deps = cur.fetchall()  # [(table, column), ...]
        cur.close()
        conn.close()
        return deps
    except Exception as e:
        st.warning(f"Could not discover FK deps on users: {e}")
        return []


def _delete_user_fk_deps(base_config: ConnectionConfig, user_ids: List[str],
                         log_lines: List[str], log_area) -> bool:
    """Dynamically discover and delete all FK dependencies on users table.
    Returns True if all succeeded."""
    deps = _discover_user_fk_deps(base_config)
    if not deps:
        return True

    all_ok = True
    for table_name, col_name in deps:
        # Quote column name to handle camelCase (TypeORM)
        query_tpl = f'DELETE FROM public."{table_name}" WHERE "{col_name}" IN ({{placeholders}})'
        log_lines.append(f"▶ Deleting from **user_db.{table_name}** (FK dep on users)...")
        log_area.markdown("\n\n".join(log_lines))

        ok, rows, err = _run_delete(base_config, "user_db", query_tpl, user_ids)
        if ok:
            log_lines[-1] += f" ✅ {rows} rows deleted"
        else:
            log_lines[-1] += f" ❌ FAILED: {err}"
            all_ok = False
        log_area.markdown("\n\n".join(log_lines))

    return all_ok


# ─────────────────────────────────────────────────────────────────────────────
# UI Sections
# ─────────────────────────────────────────────────────────────────────────────
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
        conn = get_connection(config)
        df = pd.read_sql_query(
            "SELECT id, first_name, last_name, email, organization_id, created_at "
            "FROM public.users ORDER BY email",
            conn,
        )
        conn.close()
    except Exception as e:
        st.error(f"Failed to load users from user_db: {e}")
        return None

    if df.empty:
        st.warning("No users found in user_db.users.")
        return None

    st.caption(f"Found **{len(df)}** users in `user_db.users`")

    # Search
    search = st.text_input("🔍 Search users", placeholder="Search by name or email...",
                           key="erase_user_search")
    filtered = df.copy()
    if search:
        mask = (
            filtered["first_name"].astype(str).str.contains(search, case=False, na=False)
            | filtered["last_name"].astype(str).str.contains(search, case=False, na=False)
            | filtered["email"].astype(str).str.contains(search, case=False, na=False)
        )
        filtered = filtered[mask]

    # Selection column
    prev_ids = st.session_state.get("v5_erase_selected_ids", [])
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
        key="erase_users_editor",
    )

    selected_ids = edited[edited["selected"] == True]["id"].astype(str).tolist()
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
        f"The following data will be **permanently deleted** for "
        f"**{len(user_ids)}** selected user(s). Review carefully."
    )

    base = st.session_state["v5_erase_config"]

    plan_rows = []
    total = 0
    with st.spinner("Scanning tables..."):
        for db_name, table, description, query_tpl in COUNT_QUERIES:
            count = _run_count(base, db_name, query_tpl, user_ids)
            plan_rows.append({
                "Database": db_name,
                "Table": table,
                "Description": description,
                "Rows to Delete": count if count >= 0 else "ERROR",
            })
            if count > 0:
                total += count

    plan_df = pd.DataFrame(plan_rows)
    st.dataframe(plan_df, hide_index=True, use_container_width=True)
    st.metric("Total Rows to Delete", f"{total:,}")

    # Store plan for reference
    st.session_state["v5_erase_plan"] = plan_rows
    return plan_rows


def render_execute_deletion(user_ids: List[str]):
    """Execute the deletion with a confirmation gate."""
    if not user_ids or "v5_erase_plan" not in st.session_state:
        return

    st.markdown("---")
    st.subheader("⚡ Execute Deletion")

    if not st.button("🗑️ DELETE ALL SELECTED USER DATA", type="primary", use_container_width=True):
        return

    # --- Proceed with deletion ---
    if True:
        base = st.session_state["v5_erase_config"]
        progress = st.progress(0)
        log_area = st.empty()
        log_lines: List[str] = []

        total_steps = len(DELETION_STEPS) + 1  # +1 for dynamic FK cleanup
        all_ok = True

        for i, (db_name, table, description, query_tpl) in enumerate(DELETION_STEPS):
            # Before deleting users, dynamically clean up all FK deps
            if db_name == "user_db" and table == "users":
                progress.progress(i / total_steps)
                log_lines.append("▶ **Discovering FK dependencies on users table...**")
                log_area.markdown("\n\n".join(log_lines))
                fk_ok = _delete_user_fk_deps(base, user_ids, log_lines, log_area)
                if not fk_ok:
                    all_ok = False
                    log_lines.append("❌ **Aborting: FK dependencies could not be fully removed. Users NOT deleted.**")
                    log_area.markdown("\n\n".join(log_lines))
                    break  # Stop — can't delete users with remaining FK refs

            progress.progress((i + 1) / total_steps)
            log_lines.append(f"▶ Deleting from **{db_name}.{table}** ({description})...")
            log_area.markdown("\n\n".join(log_lines))

            ok, rows, err = _run_delete(base, db_name, query_tpl, user_ids)

            if ok:
                log_lines[-1] += f" ✅ {rows} rows deleted"
            else:
                log_lines[-1] += f" ❌ FAILED: {err}"
                all_ok = False

            log_area.markdown("\n\n".join(log_lines))

        progress.progress(1.0)

        if all_ok:
            st.success("✅ All deletion steps completed successfully!")
        else:
            st.error("⚠️ Some deletion steps failed. See log above.")

        # Store result for verification
        st.session_state["v5_erase_executed"] = True


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
        verify_rows = []
        all_clean = True

        with st.spinner("Verifying..."):
            for db_name, table, description, query_tpl in COUNT_QUERIES:
                count = _run_count(base, db_name, query_tpl, user_ids)
                status = "✅ Clean" if count == 0 else ("❌ REMAINING" if count > 0 else "⚠️ Error")
                verify_rows.append({
                    "Database": db_name,
                    "Table": table,
                    "Remaining Rows": count if count >= 0 else "ERROR",
                    "Status": status,
                })
                if count != 0:
                    all_clean = False

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
    if not selected_ids:
        return

    plan = render_deletion_plan(selected_ids)
    render_execute_deletion(selected_ids)
    render_verification(selected_ids)


main()
