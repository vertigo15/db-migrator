"""
Page 6: Erase Orphan Data V5

Scans V5 databases for records with broken FK references and offers
targeted deletion:

  Intra-DB orphans  — FK violations within the same database
                      (e.g. messages with no conversation)

  Cross-DB orphans  — Records whose user_id no longer exists in user_db
                      (e.g. conversations left behind after a user was deleted)

Deletion always processes children before parents to avoid FK violations.
"""
import streamlit as st
import pandas as pd
from typing import Dict, List, Optional, Tuple

from utils.db import (
    ConnectionConfig,
    execute_query,
    get_connection,
    pooled_read_connection,
    test_connection,
)
from utils.config import SessionKeys, get_env_target_defaults

# ─── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="Erase Orphan Data V5", page_icon="🧹", layout="wide")
st.title("🧹 Erase Orphan Data V5")
st.caption(
    "Detect and remove records with broken FK references across the V5 databases. "
    "Intra-DB checks find broken FK links within a single database. "
    "Cross-DB checks find records referencing users that no longer exist in user_db."
)

# ─── Orphan check catalogue ───────────────────────────────────────────────────
#
# Each entry describes one type of orphan.
#   key          – unique string used as session-state key
#   db           – which V5 database to query
#   label        – short human-readable name
#   description  – one-line explanation
#   category     – grouping for display
#   count_sql    – SELECT COUNT(*) … (no params needed for intra-DB checks)
#   delete_sql   – DELETE … matching count_sql
#   delete_order – lower = deleted first (children before parents)
#   needs_user_ids – True if the SQL needs a NOT IN (user_ids) clause appended
#                    at runtime via {user_ids_ph} placeholder
#
# For needs_user_ids=True entries, count_sql / delete_sql contain a literal
# placeholder string "{user_ids_ph}" which is replaced at runtime with the
# correct number of %s params.
# ─────────────────────────────────────────────────────────────────────────────

ORPHAN_CHECKS = [
    # ── Intra-DB: completion_db ──────────────────────────────────────────────
    {
        "key": "blocks_no_message",
        "db": "completion_db",
        "label": "message_content_blocks → messages",
        "description": "Content blocks whose parent message does not exist",
        "category": "🔗 Intra-DB (completion_db)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.message_content_blocks "
            "WHERE message_id NOT IN (SELECT id FROM public.messages)"
        ),
        "delete_sql": (
            "DELETE FROM public.message_content_blocks "
            "WHERE message_id NOT IN (SELECT id FROM public.messages)"
        ),
        "delete_order": 1,
        "needs_user_ids": False,
    },
    {
        "key": "messages_no_conv",
        "db": "completion_db",
        "label": "messages → conversations",
        "description": "Messages whose conversation does not exist",
        "category": "🔗 Intra-DB (completion_db)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.messages "
            "WHERE conversation_id NOT IN (SELECT id FROM public.conversations)"
        ),
        "delete_sql": (
            "DELETE FROM public.messages "
            "WHERE conversation_id NOT IN (SELECT id FROM public.conversations)"
        ),
        "delete_order": 2,
        "needs_user_ids": False,
    },
    {
        "key": "agent_settings_no_agent",
        "db": "completion_db",
        "label": "agent_settings → agents",
        "description": "Agent settings whose agent does not exist",
        "category": "🔗 Intra-DB (completion_db)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.agent_settings "
            "WHERE agent_id NOT IN (SELECT id FROM public.agents)"
        ),
        "delete_sql": (
            "DELETE FROM public.agent_settings "
            "WHERE agent_id NOT IN (SELECT id FROM public.agents)"
        ),
        "delete_order": 3,
        "needs_user_ids": False,
    },
    {
        "key": "agent_kb_items_no_agent",
        "db": "completion_db",
        "label": "knowledge_base_items (via missing agents)",
        "description": "Knowledge base items tied to assignments whose agent does not exist",
        "category": "🔗 Intra-DB (completion_db)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.knowledge_base_items "
            "WHERE knowledge_base_id IN ("
            "  SELECT knowledge_base_id FROM public.knowledge_base_assignments "
            "  WHERE assigned_to_type = 'agent'::public.knowledge_base_assignments_assigned_to_type_enum "
            "  AND assigned_to_id NOT IN (SELECT id FROM public.agents)"
            ")"
        ),
        "delete_sql": (
            "DELETE FROM public.knowledge_base_items "
            "WHERE knowledge_base_id IN ("
            "  SELECT knowledge_base_id FROM public.knowledge_base_assignments "
            "  WHERE assigned_to_type = 'agent'::public.knowledge_base_assignments_assigned_to_type_enum "
            "  AND assigned_to_id NOT IN (SELECT id FROM public.agents)"
            ")"
        ),
        "delete_order": 4,
        "needs_user_ids": False,
    },
    {
        "key": "agent_kbs_no_agent",
        "db": "completion_db",
        "label": "knowledge_bases → agents",
        "description": "Knowledge bases assigned to agents that do not exist",
        "category": "🔗 Intra-DB (completion_db)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.knowledge_bases "
            "WHERE id IN ("
            "  SELECT knowledge_base_id FROM public.knowledge_base_assignments "
            "  WHERE assigned_to_type = 'agent'::public.knowledge_base_assignments_assigned_to_type_enum "
            "  AND assigned_to_id NOT IN (SELECT id FROM public.agents)"
            ")"
        ),
        "delete_sql": (
            "DELETE FROM public.knowledge_bases "
            "WHERE id IN ("
            "  SELECT knowledge_base_id FROM public.knowledge_base_assignments "
            "  WHERE assigned_to_type = 'agent'::public.knowledge_base_assignments_assigned_to_type_enum "
            "  AND assigned_to_id NOT IN (SELECT id FROM public.agents)"
            ")"
        ),
        "delete_order": 5,
        "needs_user_ids": False,
    },
    {
        "key": "kb_assignments_no_kb",
        "db": "completion_db",
        "label": "knowledge_base_assignments → knowledge_bases",
        "description": "Knowledge base assignments whose knowledge base does not exist",
        "category": "🔗 Intra-DB (completion_db)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.knowledge_base_assignments "
            "WHERE knowledge_base_id NOT IN (SELECT id FROM public.knowledge_bases)"
        ),
        "delete_sql": (
            "DELETE FROM public.knowledge_base_assignments "
            "WHERE knowledge_base_id NOT IN (SELECT id FROM public.knowledge_bases)"
        ),
        "delete_order": 6,
        "needs_user_ids": False,
    },
    # ── Intra-DB: document_db ────────────────────────────────────────────────
    {
        "key": "embeddings_no_chunk",
        "db": "document_db",
        "label": "embeddings → chunks",
        "description": "Embeddings whose chunk does not exist",
        "category": "🔗 Intra-DB (document_db)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.embeddings "
            "WHERE chunk_id NOT IN (SELECT id FROM public.chunks)"
        ),
        "delete_sql": (
            "DELETE FROM public.embeddings "
            "WHERE chunk_id NOT IN (SELECT id FROM public.chunks)"
        ),
        "delete_order": 5,
        "needs_user_ids": False,
    },
    {
        "key": "chunks_no_doc",
        "db": "document_db",
        "label": "chunks → documents",
        "description": "Chunks whose document does not exist",
        "category": "🔗 Intra-DB (document_db)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.chunks "
            "WHERE document_id NOT IN (SELECT id FROM public.documents)"
        ),
        "delete_sql": (
            "DELETE FROM public.chunks "
            "WHERE document_id NOT IN (SELECT id FROM public.documents)"
        ),
        "delete_order": 6,
        "needs_user_ids": False,
    },
    # ── Cross-DB: records belonging to users that no longer exist in user_db ─
    # Delete order: cascade children first so FK constraints are not violated.
    {
        "key": "conv_blocks_orphan_user",
        "db": "completion_db",
        "label": "message_content_blocks (via user-orphan messages)",
        "description": "Content blocks tied to messages from non-existent users",
        "category": "👤 Cross-DB (user reference)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.message_content_blocks "
            "WHERE message_id IN ("
            "  SELECT id FROM public.messages WHERE user_id NOT IN ({user_ids_ph})"
            ")"
        ),
        "delete_sql": (
            "DELETE FROM public.message_content_blocks "
            "WHERE message_id IN ("
            "  SELECT id FROM public.messages WHERE user_id NOT IN ({user_ids_ph})"
            ")"
        ),
        "delete_order": 7,
        "needs_user_ids": True,
    },
    {
        "key": "messages_orphan_user",
        "db": "completion_db",
        "label": "messages → users",
        "description": "Messages belonging to users that no longer exist in user_db",
        "category": "👤 Cross-DB (user reference)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.messages "
            "WHERE user_id NOT IN ({user_ids_ph})"
        ),
        "delete_sql": (
            "DELETE FROM public.messages "
            "WHERE user_id NOT IN ({user_ids_ph})"
        ),
        "delete_order": 8,
        "needs_user_ids": True,
    },
    {
        "key": "convs_orphan_user",
        "db": "completion_db",
        "label": "conversations → users",
        "description": "Conversations belonging to users that no longer exist in user_db",
        "category": "👤 Cross-DB (user reference)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.conversations "
            "WHERE user_id NOT IN ({user_ids_ph})"
        ),
        "delete_sql": (
            "DELETE FROM public.conversations "
            "WHERE user_id NOT IN ({user_ids_ph})"
        ),
        "delete_order": 9,
        "needs_user_ids": True,
    },
    {
        "key": "agent_settings_orphan_user",
        "db": "completion_db",
        "label": "agent_settings (via user-orphan agents)",
        "description": "Agent settings tied to agents from non-existent users",
        "category": "👤 Cross-DB (user reference)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.agent_settings "
            "WHERE agent_id IN ("
            "  SELECT id FROM public.agents WHERE user_id NOT IN ({user_ids_ph})"
            ")"
        ),
        "delete_sql": (
            "DELETE FROM public.agent_settings "
            "WHERE agent_id IN ("
            "  SELECT id FROM public.agents WHERE user_id NOT IN ({user_ids_ph})"
            ")"
        ),
        "delete_order": 10,
        "needs_user_ids": True,
    },
    {
        "key": "agent_kb_items_orphan_user",
        "db": "completion_db",
        "label": "knowledge_base_items (via user-orphan agents)",
        "description": "Knowledge base items tied to agents from non-existent users",
        "category": "👤 Cross-DB (user reference)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.knowledge_base_items "
            "WHERE knowledge_base_id IN ("
            "  SELECT knowledge_base_id FROM public.knowledge_base_assignments "
            "  WHERE assigned_to_type = 'agent'::public.knowledge_base_assignments_assigned_to_type_enum "
            "  AND assigned_to_id IN ("
            "    SELECT id FROM public.agents WHERE user_id NOT IN ({user_ids_ph})"
            "  )"
            ")"
        ),
        "delete_sql": (
            "DELETE FROM public.knowledge_base_items "
            "WHERE knowledge_base_id IN ("
            "  SELECT knowledge_base_id FROM public.knowledge_base_assignments "
            "  WHERE assigned_to_type = 'agent'::public.knowledge_base_assignments_assigned_to_type_enum "
            "  AND assigned_to_id IN ("
            "    SELECT id FROM public.agents WHERE user_id NOT IN ({user_ids_ph})"
            "  )"
            ")"
        ),
        "delete_order": 11,
        "needs_user_ids": True,
    },
    {
        "key": "agent_kbs_orphan_user",
        "db": "completion_db",
        "label": "knowledge_bases (via user-orphan agents)",
        "description": "Knowledge bases assigned to agents from non-existent users",
        "category": "👤 Cross-DB (user reference)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.knowledge_bases "
            "WHERE id IN ("
            "  SELECT knowledge_base_id FROM public.knowledge_base_assignments "
            "  WHERE assigned_to_type = 'agent'::public.knowledge_base_assignments_assigned_to_type_enum "
            "  AND assigned_to_id IN ("
            "    SELECT id FROM public.agents WHERE user_id NOT IN ({user_ids_ph})"
            "  )"
            ")"
        ),
        "delete_sql": (
            "DELETE FROM public.knowledge_bases "
            "WHERE id IN ("
            "  SELECT knowledge_base_id FROM public.knowledge_base_assignments "
            "  WHERE assigned_to_type = 'agent'::public.knowledge_base_assignments_assigned_to_type_enum "
            "  AND assigned_to_id IN ("
            "    SELECT id FROM public.agents WHERE user_id NOT IN ({user_ids_ph})"
            "  )"
            ")"
        ),
        "delete_order": 12,
        "needs_user_ids": True,
    },
    {
        "key": "agents_orphan_user",
        "db": "completion_db",
        "label": "agents → users",
        "description": "Agents belonging to users that no longer exist in user_db",
        "category": "👤 Cross-DB (user reference)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.agents "
            "WHERE user_id NOT IN ({user_ids_ph})"
        ),
        "delete_sql": (
            "DELETE FROM public.agents "
            "WHERE user_id NOT IN ({user_ids_ph})"
        ),
        "delete_order": 12,
        "needs_user_ids": True,
    },
    {
        "key": "embeddings_orphan_user",
        "db": "document_db",
        "label": "embeddings (via user-orphan documents)",
        "description": "Embeddings tied to documents from non-existent users",
        "category": "👤 Cross-DB (user reference)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.embeddings "
            "WHERE document_id IN ("
            "  SELECT id FROM public.documents WHERE user_id NOT IN ({user_ids_ph})"
            ")"
        ),
        "delete_sql": (
            "DELETE FROM public.embeddings "
            "WHERE document_id IN ("
            "  SELECT id FROM public.documents WHERE user_id NOT IN ({user_ids_ph})"
            ")"
        ),
        "delete_order": 13,
        "needs_user_ids": True,
    },
    {
        "key": "chunks_orphan_user",
        "db": "document_db",
        "label": "chunks (via user-orphan documents)",
        "description": "Chunks tied to documents from non-existent users",
        "category": "👤 Cross-DB (user reference)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.chunks "
            "WHERE document_id IN ("
            "  SELECT id FROM public.documents WHERE user_id NOT IN ({user_ids_ph})"
            ")"
        ),
        "delete_sql": (
            "DELETE FROM public.chunks "
            "WHERE document_id IN ("
            "  SELECT id FROM public.documents WHERE user_id NOT IN ({user_ids_ph})"
            ")"
        ),
        "delete_order": 14,
        "needs_user_ids": True,
    },
    {
        "key": "docs_orphan_user",
        "db": "document_db",
        "label": "documents → users",
        "description": "Documents belonging to users that no longer exist in user_db",
        "category": "👤 Cross-DB (user reference)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.documents "
            "WHERE user_id NOT IN ({user_ids_ph})"
        ),
        "delete_sql": (
            "DELETE FROM public.documents "
            "WHERE user_id NOT IN ({user_ids_ph})"
        ),
        "delete_order": 15,
        "needs_user_ids": True,
    },
    {
        "key": "folders_orphan_user",
        "db": "document_db",
        "label": "folders → users",
        "description": "Folders belonging to users that no longer exist in user_db",
        "category": "👤 Cross-DB (user reference)",
        "count_sql": (
            "SELECT COUNT(*) FROM public.folders "
            "WHERE user_id NOT IN ({user_ids_ph})"
        ),
        "delete_sql": (
            "DELETE FROM public.folders "
            "WHERE user_id NOT IN ({user_ids_ph})"
        ),
        "delete_order": 16,
        "needs_user_ids": True,
    },
]

# Lookup by key for fast access
CHECKS_BY_KEY: Dict[str, dict] = {c["key"]: c for c in ORPHAN_CHECKS}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _make_config(base: ConnectionConfig, database: str) -> ConnectionConfig:
    return ConnectionConfig(
        host=base.host,
        port=base.port,
        database=database,
        username=base.username,
        password=base.password,
    )


def _build_sql(sql_template: str, user_ids: List[str]) -> Tuple[str, tuple]:
    """
    Replace {user_ids_ph} placeholder with the correct number of %s params
    and return (final_sql, params_tuple).
    """
    ph = ", ".join(["%s"] * len(user_ids))
    sql = sql_template.replace("{user_ids_ph}", ph)
    return sql, tuple(user_ids)


def _run_count(base: ConnectionConfig, check: dict, user_ids: List[str]) -> int:
    """Execute a COUNT query for a single orphan check. Returns -1 on error."""
    db = check["db"]
    config = _make_config(base, db)
    try:
        conn = get_connection(config)
        cur = conn.cursor()
        if check["needs_user_ids"]:
            sql, params = _build_sql(check["count_sql"], user_ids)
        else:
            sql, params = check["count_sql"], ()
        cur.execute(sql, params)
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return int(count)
    except Exception as e:
        st.warning(f"Count failed for **{check['label']}** on `{db}`: {e}")
        return -1


def _run_delete(base: ConnectionConfig, check: dict,
                user_ids: List[str]) -> Tuple[bool, int, Optional[str]]:
    """Execute a DELETE for a single orphan check. Returns (success, rows, error)."""
    db = check["db"]
    config = _make_config(base, db)
    conn = None
    try:
        conn = get_connection(config)
        cur = conn.cursor()
        if check["needs_user_ids"]:
            sql, params = _build_sql(check["delete_sql"], user_ids)
        else:
            sql, params = check["delete_sql"], ()
        cur.execute(sql, params)
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


def _preview_sql(check: dict, user_ids: List[str]) -> str:
    """
    Return a human-readable version of the DELETE SQL for display.
    For cross-DB checks, replaces {user_ids_ph} with the actual UUIDs
    (up to MAX_SHOW inline; remainder summarised as a comment).
    """
    MAX_SHOW = 5
    sql = check["delete_sql"]
    if not check["needs_user_ids"] or not user_ids:
        return sql

    if len(user_ids) <= MAX_SHOW:
        shown = ", ".join(f"'{u}'" for u in user_ids)
        return sql.replace("{user_ids_ph}", shown)
    else:
        shown = ", ".join(f"'{u}'" for u in user_ids[:MAX_SHOW])
        remainder = len(user_ids) - MAX_SHOW
        return sql.replace(
            "{user_ids_ph}",
            f"{shown}\n    -- … and {remainder} more valid user UUID(s)",
        )


def _fetch_valid_user_ids(base: ConnectionConfig) -> List[str]:
    """Load all user UUIDs from user_db.public.users."""
    config = _make_config(base, "user_db")
    try:
        frame = execute_query(config, "SELECT id::text AS id FROM public.users")
        return frame["id"].tolist() if not frame.empty else []
    except Exception as e:
        st.error(f"Failed to load user IDs from user_db: {e}")
        return []


def _run_counts_grouped(
    base: ConnectionConfig,
    user_ids: List[str],
) -> Tuple[Dict[str, int], List[str]]:
    """Run all orphan counts with one pooled connection per database."""
    results: Dict[str, int] = {}
    errors: List[str] = []
    for db_name in ("completion_db", "document_db"):
        checks = [check for check in ORPHAN_CHECKS if check["db"] == db_name]
        try:
            with pooled_read_connection(_make_config(base, db_name)) as conn:
                for check in checks:
                    if check["needs_user_ids"] and not user_ids:
                        results[check["key"]] = -2
                        continue
                    try:
                        sql, params = (
                            _build_sql(check["count_sql"], user_ids)
                            if check["needs_user_ids"]
                            else (check["count_sql"], ())
                        )
                        with conn.cursor() as cur:
                            cur.execute(sql, params)
                            results[check["key"]] = int(cur.fetchone()[0])
                    except Exception as exc:
                        results[check["key"]] = -1
                        errors.append(f"{db_name}.{check['label']}: {exc}")
        except Exception as exc:
            errors.append(f"{db_name}: {exc}")
            for check in checks:
                results.setdefault(check["key"], -1)
    return results, errors


# ─── UI sections ──────────────────────────────────────────────────────────────

def render_connection():
    st.subheader("🔌 Connect to V5 Instance")
    st.info(
        "Provide connection details for the **V5 PostgreSQL server**. "
        "The same host/port/credentials are used to connect to "
        "`user_db`, `document_db`, and `completion_db`."
    )

    saved = get_env_target_defaults()
    session_conn = st.session_state.get(SessionKeys.TARGET_CONNECTION, {})
    for k, v in session_conn.items():
        if v:
            saved[k] = v

    with st.form("orphan_connection_form"):
        col1, col2 = st.columns(2)
        with col1:
            host = st.text_input("Host", value=saved.get("host", "localhost"))
            username = st.text_input("Username", value=saved.get("username", ""))
        with col2:
            port = st.number_input("Port", value=int(saved.get("port", 5432)),
                                   min_value=1, max_value=65535)
            password = st.text_input("Password", type="password",
                                     value=saved.get("password", ""))

        submitted = st.form_submit_button("🔗 Test Connection", type="primary",
                                          use_container_width=True)

        if submitted:
            if not all([host, username, password]):
                st.error("Please fill in all required fields.")
                return

            base = ConnectionConfig(host=host, port=port, database="user_db",
                                    username=username, password=password)
            all_ok = True
            msgs = {}
            for db in ["user_db", "document_db", "completion_db"]:
                ok, msg = test_connection(_make_config(base, db))
                msgs[db] = (ok, msg)
                if not ok:
                    all_ok = False

            if all_ok:
                st.session_state["orphan_base_config"] = base
                st.session_state["orphan_conn_msgs"] = msgs
                st.rerun()
            else:
                for db, (ok, msg) in msgs.items():
                    (st.success if ok else st.error)(
                        f"{'✅' if ok else '❌'} **{db}**: {msg}"
                    )

    if "orphan_conn_msgs" in st.session_state:
        for db, (ok, msg) in st.session_state["orphan_conn_msgs"].items():
            (st.success if ok else st.error)(
                f"{'✅' if ok else '❌'} **{db}** — {'connected' if ok else msg}"
            )


def render_scan():
    if "orphan_base_config" not in st.session_state:
        return

    st.markdown("---")
    st.subheader("🔍 Scan for Orphans")

    if st.button("▶ Run Orphan Scan", type="primary", use_container_width=True):
        base: ConnectionConfig = st.session_state["orphan_base_config"]

        # Load valid user IDs once (needed for cross-DB checks)
        with st.spinner("Loading user IDs from user_db…"):
            valid_user_ids = _fetch_valid_user_ids(base)

        if not valid_user_ids:
            st.warning(
                "⚠️ No users found in `user_db.public.users`. "
                "Cross-DB orphan checks require at least one user and will be skipped."
            )

        with st.spinner("Running grouped orphan counts..."):
            results, errors = _run_counts_grouped(base, valid_user_ids)
        for error in errors:
            st.warning(f"Count failed for {error}")

        st.session_state["orphan_scan_results"] = results
        st.session_state["orphan_valid_user_ids"] = valid_user_ids
        st.rerun()


def render_results():
    if "orphan_scan_results" not in st.session_state:
        return

    results: Dict[str, int] = st.session_state["orphan_scan_results"]
    total_orphans = sum(v for v in results.values() if v > 0)

    st.markdown("---")
    st.subheader("📊 Scan Results")

    if total_orphans == 0:
        st.success("✅ No orphaned records found across all V5 databases.")
    else:
        st.warning(f"⚠️ Found **{total_orphans:,}** orphaned record(s) across V5 databases.")

    # Build display table
    rows = []
    prev_category = None
    for check in ORPHAN_CHECKS:
        count = results.get(check["key"], -1)
        if count == -2:
            count_display = "⏭ Skipped (no users)"
            status = "⏭"
        elif count < 0:
            count_display = "❌ Error"
            status = "❌"
        elif count == 0:
            count_display = "0"
            status = "✅ Clean"
        else:
            count_display = f"{count:,}"
            status = "⚠️ Orphans found"

        rows.append({
            "Category": check["category"],
            "Check": check["label"],
            "Description": check["description"],
            "DB": check["db"],
            "Orphan Count": count_display,
            "Status": status,
        })

    df = pd.DataFrame(rows)
    st.dataframe(df, hide_index=True, use_container_width=True,
                 column_config={
                     "Category": st.column_config.TextColumn(width="medium"),
                     "Check": st.column_config.TextColumn(width="large"),
                     "Description": st.column_config.TextColumn(width="large"),
                     "DB": st.column_config.TextColumn(width="small"),
                     "Orphan Count": st.column_config.TextColumn(width="small"),
                     "Status": st.column_config.TextColumn(width="medium"),
                 })

    st.metric("Total Orphaned Rows", f"{total_orphans:,}")
    return total_orphans


def render_delete_section(total_orphans: int):
    if not st.session_state.get("orphan_scan_results"):
        return
    if total_orphans == 0:
        return

    results: Dict[str, int] = st.session_state["orphan_scan_results"]
    valid_user_ids: List[str] = st.session_state.get("orphan_valid_user_ids", [])

    st.markdown("---")
    st.subheader("🗑️ Delete Orphans")

    st.warning(
        "Select the orphan categories to erase. "
        "Deletion is **permanent** and **cannot be undone**. "
        "Only checks with orphan count > 0 are selectable."
    )

    # ── Selection table ───────────────────────────────────────────────────────
    selectable_checks = [
        c for c in ORPHAN_CHECKS
        if results.get(c["key"], 0) > 0
    ]

    if not selectable_checks:
        st.info("No checks with orphaned records to select.")
        return

    st.caption("Select which orphan types to delete:")

    # Build a data editor with a 'delete' checkbox column
    sel_rows = []
    for check in selectable_checks:
        sel_rows.append({
            "delete": False,
            "category": check["category"],
            "check": check["label"],
            "db": check["db"],
            "rows": results[check["key"]],
            "_key": check["key"],
        })

    sel_df = pd.DataFrame(sel_rows)

    col_select_all, _ = st.columns([1, 5])
    if col_select_all.button("☑ Select All", key="orphan_select_all"):
        for row in sel_rows:
            row["delete"] = True
        sel_df = pd.DataFrame(sel_rows)

    edited = st.data_editor(
        sel_df,
        column_config={
            "delete": st.column_config.CheckboxColumn("Delete?", default=False),
            "category": st.column_config.TextColumn("Category"),
            "check": st.column_config.TextColumn("Orphan Type"),
            "db": st.column_config.TextColumn("Database"),
            "rows": st.column_config.NumberColumn("Orphan Rows"),
            "_key": None,  # hidden
        },
        hide_index=True,
        use_container_width=True,
        key="orphan_selection_editor",
    )

    selected_keys = edited[edited["delete"] == True]["_key"].tolist()
    selected_rows = edited[edited["delete"] == True]["rows"].sum() if selected_keys else 0

    st.metric("Selected for Deletion", f"{int(selected_rows):,} rows across {len(selected_keys)} check(s)")

    if not selected_keys:
        st.info("Check the boxes above to select orphan types to delete.")
        return

    # ── SQL preview ───────────────────────────────────────────────────────────
    checks_ordered = sorted(
        [CHECKS_BY_KEY[k] for k in selected_keys],
        key=lambda c: c["delete_order"],
    )

    with st.expander(f"🔎 Preview SQL ({len(checks_ordered)} statement(s))", expanded=False):
        st.caption(
            "Statements are executed in this exact order (children before parents). "
            "For cross-DB checks the NOT IN list shows up to 5 user UUIDs; "
            "the rest are passed as parameters at runtime."
        )
        for i, check in enumerate(checks_ordered, start=1):
            st.markdown(f"**{i}. `{check['db']}` — {check['label']}**")
            sql_display = _preview_sql(check, valid_user_ids)
            st.code(sql_display, language="sql")

    # ── Confirmation + execute ────────────────────────────────────────────────
    st.markdown("---")
    st.error(
        f"⚠️ You are about to permanently delete **{int(selected_rows):,} rows** "
        f"across **{len(selected_keys)}** orphan type(s). This cannot be undone."
    )

    confirm = st.text_input(
        "Type **DELETE ORPHANS** to confirm:",
        placeholder="DELETE ORPHANS",
        key="orphan_confirm_input",
    )

    if st.button("🗑️ EXECUTE ORPHAN DELETION", type="primary",
                 use_container_width=True,
                 disabled=(confirm.strip() != "DELETE ORPHANS")):

        base: ConnectionConfig = st.session_state["orphan_base_config"]

        # Sort selected checks by delete_order (children before parents)
        checks_to_run = sorted(
            [CHECKS_BY_KEY[k] for k in selected_keys],
            key=lambda c: c["delete_order"],
        )

        progress = st.progress(0)
        log_area = st.empty()
        log_lines: List[str] = []
        all_ok = True

        for i, check in enumerate(checks_to_run):
            progress.progress((i + 1) / len(checks_to_run))
            log_lines.append(
                f"▶ Deleting **{check['db']}.{check['label']}** …"
            )
            log_area.markdown("\n\n".join(log_lines))

            ok, rows, err = _run_delete(base, check, valid_user_ids)

            if ok:
                log_lines[-1] += f" ✅ {rows:,} rows deleted"
            else:
                log_lines[-1] += f" ❌ FAILED: {err}"
                all_ok = False

            log_area.markdown("\n\n".join(log_lines))

        progress.progress(1.0)

        if all_ok:
            st.success("✅ All selected orphan deletions completed.")
        else:
            st.error("⚠️ Some deletions failed — see log above.")

        # Clear scan cache so user re-scans to see updated state
        st.session_state.pop("orphan_scan_results", None)
        st.session_state["orphan_deletion_done"] = True


def render_post_delete_hint():
    if st.session_state.get("orphan_deletion_done"):
        st.info(
            "💡 Click **▶ Run Orphan Scan** again to verify all orphans have been removed."
        )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    render_connection()
    render_scan()

    total = 0
    if "orphan_scan_results" in st.session_state:
        total = render_results() or 0

    render_delete_section(total)
    render_post_delete_hint()


main()
