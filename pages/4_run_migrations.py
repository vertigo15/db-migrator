"""
Page: Run Migration SQL Scripts

Features:
- List all generated SQL migration files
- Run them one by one in order
- Show execution status and results
- Support for separate databases (user_db, document_db, completion_db)
"""
import os
import glob
import streamlit as st
import psycopg2
from datetime import datetime

from utils.db import ConnectionConfig, get_connection
from utils.config import SessionKeys

# Page config
st.set_page_config(page_title="Run Migrations", page_icon="🚀", layout="wide")
st.title("🚀 Run Migration SQL Scripts")

# Directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MIGRATIONS_DIR = os.path.join(BASE_DIR, "output", "migrations")


def _ensure_target_config():
    """Auto-populate target_config from .env if not already in session state."""
    if "target_config" not in st.session_state:
        from utils.config import get_env_target_defaults
        env_defaults = get_env_target_defaults()
        if env_defaults.get("host") and env_defaults.get("database") and env_defaults.get("username") and env_defaults.get("password"):
            config = ConnectionConfig(
                host=env_defaults["host"],
                port=int(env_defaults["port"]),
                database=env_defaults["database"],
                username=env_defaults["username"],
                password=env_defaults["password"],
            )
            st.session_state["target_config"] = config
            st.session_state["target_schema_mode"] = env_defaults.get("schema_mode", "schemas")


_ensure_target_config()


def _get_tracking_connection():
    """Get a connection to the database that holds migration tracking tables."""
    from utils.config import get_env_target_defaults
    defaults = get_env_target_defaults()
    config = ConnectionConfig(
        host=defaults["host"],
        port=int(defaults["port"]),
        database=defaults["database"],
        username=defaults["username"],
        password=defaults["password"],
    )
    conn = get_connection(config)
    conn.autocommit = True
    return conn


def _update_batch_step_results(batch_id: str, step_name: str, success: bool, error_msg: str = None):
    """Update migration_user_results after a SQL step executes.

    On success: appends step to steps_completed JSONB for all pending users.
    On failure: marks all pending users as failed with the step name and error.
    """
    if not batch_id:
        return
    try:
        conn = _get_tracking_connection()
        cursor = conn.cursor()
        # Normalize filename to a canonical step key (e.g. "01_users_20260720.sql" → "01_users")
        step_key = step_name
        for prefix in ["01_users", "02_folders", "03_documents", "04_chunks_embeddings",
                       "05_conversations", "06_agents", "07_conversions"]:
            if step_name.startswith(prefix):
                step_key = prefix
                break

        if success:
            cursor.execute("""
                UPDATE migration.migration_user_results
                SET steps_completed = COALESCE(steps_completed, '{}'::jsonb) || %s::jsonb
                WHERE batch_id = %s::uuid AND result IN ('pending', 'reused_existing_user')
            """, (f'{{"{step_key}": "success"}}', batch_id))
            cursor.close()
            conn.close()
            if step_key == "01_users":
                _verify_users_step(batch_id)
            return
        else:
            cursor.execute("""
                UPDATE migration.migration_user_results
                SET result = 'failed',
                    failed_step = %s,
                    error_message = %s,
                    completed_at = now(),
                    steps_completed = COALESCE(steps_completed, '{}'::jsonb) || %s::jsonb
                WHERE batch_id = %s::uuid AND result IN ('pending', 'reused_existing_user')
            """, (step_key, error_msg[:500] if error_msg else None,
                  f'{{"{step_key}": "failed"}}', batch_id))
        cursor.close()
        conn.close()
    except Exception:
        pass


def _verify_users_step(batch_id: str):
    """After step 01_users, distinguish reused_existing_user from newly created.

    A user is 'reused' if their V5 record existed before the batch started
    (detected by comparing users.created_at with the batch started_at).
    """
    if not batch_id:
        return
    try:
        conn = _get_tracking_connection()
        cursor = conn.cursor()
        # Get batch emails and batch start time
        cursor.execute("""
            SELECT r.email, b.started_at
            FROM migration.migration_user_results r
            JOIN migration.migration_batches b ON b.id = r.batch_id
            WHERE r.batch_id = %s::uuid AND r.result = 'pending'
        """, (batch_id,))
        rows = cursor.fetchall()
        if not rows:
            cursor.close()
            conn.close()
            return

        batch_started = rows[0][1]
        batch_emails = [r[0] for r in rows]

        # Check user_db for users created before the batch (= pre-existing / reused)
        base_config = st.session_state.get("target_config")
        if not base_config:
            cursor.close()
            conn.close()
            return

        user_conn = get_connection(ConnectionConfig(
            host=base_config.host, port=base_config.port,
            database="user_db", username=base_config.username,
            password=base_config.password
        ))
        user_cursor = user_conn.cursor()
        user_cursor.execute("""
            SELECT email FROM public.users
            WHERE email = ANY(%s) AND created_at < %s
        """, (batch_emails, batch_started))
        reused_emails = [r[0] for r in user_cursor.fetchall()]
        user_cursor.close()
        user_conn.close()

        if reused_emails:
            cursor.execute("""
                UPDATE migration.migration_user_results
                SET result = 'reused_existing_user',
                    steps_completed = COALESCE(steps_completed, '{}'::jsonb) || '{"01_users": "reused"}'::jsonb,
                    completed_at = now()
                WHERE batch_id = %s::uuid AND email = ANY(%s) AND result = 'pending'
            """, (batch_id, reused_emails))

        cursor.close()
        conn.close()
    except Exception:
        pass


def _finalize_batch(batch_id: str):
    """Mark all remaining pending users as success and close the batch."""
    if not batch_id:
        return
    try:
        conn = _get_tracking_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE migration.migration_user_results
            SET result = 'success', completed_at = now()
            WHERE batch_id = %s::uuid AND result = 'pending'
        """, (batch_id,))
        cursor.execute("""
            UPDATE migration.migration_batches
            SET status = 'completed', completed_at = now()
            WHERE id = %s::uuid
        """, (batch_id,))
        cursor.close()
        conn.close()
    except Exception:
        pass


# Database mapping for each migration file prefix
DB_MAPPING = {
    "01_users_": "user_db",
    "02_folders_": "document_db",  # folders is in document_db, not user_db
    "03_documents_": "document_db",
    "04_chunks_embeddings_": "document_db",
    "05_conversations_": "completion_db",
    "06_agents_": "completion_db",
    "07_conversions_": "completion_db",
}

# Table mapping for rollback — each entry defines the mapping_table used in
# migration.id_mappings and a list of delete operations (executed in order).
# Each delete op specifies the table and the SQL WHERE clause referencing mappings.
TABLE_MAPPING = {
    "01_users_": {
        "mapping_table": "users",
        "tables": ["users"],
        "deletes": [
            ("users", "id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'users')"),
        ],
    },
    "02_folders_": {
        "mapping_table": "folders",
        "tables": ["folders"],
        "deletes": [
            ("folders", "id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'folders')"),
        ],
    },
    "03_documents_": {
        "mapping_table": "documents",
        "tables": ["documents"],
        "deletes": [
            ("documents", "id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'documents')"),
        ],
    },
    "04_chunks_embeddings_": {
        "mapping_table": "documents",
        "tables": ["chunks", "embeddings"],
        "deletes": [
            ("embeddings", "document_id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'documents')"),
            ("chunks", "document_id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'documents')"),
        ],
    },
    "05_conversations_": {
        "mapping_table": "conversations",
        "tables": ["conversations", "messages", "message_content_blocks"],
        "deletes": [
            ("message_content_blocks", "message_id IN (SELECT id FROM messages WHERE conversation_id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'conversations'))"),
            ("messages", "conversation_id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'conversations')"),
            ("conversations", "id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'conversations')"),
        ],
    },
    "06_agents_": {
        "mapping_table": "agents",
        "tables": ["agents", "agent_settings", "knowledge_bases", "knowledge_base_assignments", "knowledge_base_items", "legacy_bot_to_agent_mapping"],
        "deletes": [
            ("knowledge_base_items", "knowledge_base_id IN (SELECT id FROM knowledge_bases WHERE agent_id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'agents'))"),
            ("knowledge_base_assignments", "knowledge_base_id IN (SELECT id FROM knowledge_bases WHERE agent_id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'agents'))"),
            ("knowledge_bases", "agent_id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'agents')"),
            ("agent_settings", "agent_id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'agents')"),
            ("legacy_bot_to_agent_mapping", "agent_id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'agents')"),
            ("agents", "id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'agents')"),
        ],
    },
    "07_conversions_": {
        "mapping_table": "conversions",
        "tables": ["agent_conversions", "conversions"],
        "deletes": [
            ("agent_conversions", "conversion_id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'conversions')"),
            ("conversions", "id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'conversions')"),
        ],
    },
}


def verify_migration_result(base_config: ConnectionConfig, filename: str) -> dict:
    """
    After a successful migration, connect to the destination DB and return
    live row counts for every affected table plus the latest batch_log entry.
    """
    # Resolve target DB and table list from the filename prefix
    table_info = None
    target_db = "user_db"
    for prefix, info in TABLE_MAPPING.items():
        if filename.startswith(prefix):
            table_info = info
            break
    for prefix, db in DB_MAPPING.items():
        if filename.startswith(prefix):
            target_db = db
            break

    if not table_info:
        return {"error": "Unknown migration type"}

    db_config = ConnectionConfig(
        host=base_config.host,
        port=base_config.port,
        database=target_db,
        username=base_config.username,
        password=base_config.password,
    )

    result = {
        "target_db": target_db,
        "tables": {},        # table -> total row count in destination
        "batch_log": None,   # latest migration.batch_log entry
        "migrated_ids": None,# count from migration.id_mappings
        "error": None,
    }

    try:
        conn = get_connection(db_config)
        cursor = conn.cursor()

        # ── Per-table row counts ──────────────────────────────────────────
        for table in table_info["tables"]:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM public.{table};")
                result["tables"][table] = cursor.fetchone()[0]
            except Exception:
                result["tables"][table] = None
                try:
                    conn.rollback()
                except Exception:
                    pass

        # ── Latest batch_log entry ────────────────────────────────────────
        try:
            cursor.execute("""
                SELECT batch_id, record_count, status, started_at, completed_at
                FROM migration.batch_log
                WHERE table_name = %s
                ORDER BY started_at DESC
                LIMIT 1
            """, (table_info["mapping_table"],))
            row = cursor.fetchone()
            if row:
                result["batch_log"] = {
                    "batch_id":    row[0],
                    "record_count": row[1],
                    "status":      row[2],
                    "started_at":  row[3],
                    "completed_at": row[4],
                }
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass

        # ── Total tracked IDs ─────────────────────────────────────────────
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM migration.id_mappings
                WHERE table_name = %s
            """, (table_info["mapping_table"],))
            result["migrated_ids"] = cursor.fetchone()[0]
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass

        cursor.close()
        conn.close()

    except Exception as e:
        result["error"] = str(e)

    return result


def render_verification(verif: dict) -> None:
    """Render the destination verification block inside an expander."""
    with st.expander("🔍 Destination Verification", expanded=True):
        if verif.get("error"):
            st.error(f"Verification error: {verif['error']}")
            return

        # Batch summary
        bl = verif.get("batch_log")
        if bl:
            ts = bl["completed_at"].strftime("%Y-%m-%d %H:%M:%S") if bl.get("completed_at") else "—"
            status_icon = "✅" if bl["status"] == "completed" else "⚠️"
            st.markdown(
                f"{status_icon} **Batch:** `{bl['batch_id']}`  "
                f"| **Status:** `{bl['status']}`  "
                f"| **Records in batch:** {bl['record_count']:,}  "
                f"| **Completed:** {ts}"
            )

        # Migrated count (from id_mappings) — shown prominently
        mid = verif.get("migrated_ids")
        if mid is not None:
            st.metric(
                label="✅ Added by migration",
                value=f"{mid:,}",
                help="Rows tracked in migration.id_mappings for this table type"
            )

        # Per-table total row counts
        tables = verif.get("tables", {})
        if tables:
            cols = st.columns(max(len(tables), 1))
            for i, (tbl, cnt) in enumerate(tables.items()):
                with cols[i]:
                    if cnt is not None:
                        st.metric(label=f"📊 {tbl} (total)", value=f"{cnt:,}")
                    else:
                        st.metric(label=f"📊 {tbl} (total)", value="N/A")

        st.caption(f"Database: `{verif.get('target_db', '—')}`")


def get_migration_files():
    """Get all migration SQL files sorted by prefix."""
    if not os.path.exists(MIGRATIONS_DIR):
        return []
    
    files = glob.glob(os.path.join(MIGRATIONS_DIR, "*.sql"))
    # Sort by filename (which starts with numbers)
    files.sort()
    
    migration_files = []
    for file_path in files:
        filename = os.path.basename(file_path)
        
        # Determine target database
        target_db = "user_db"  # default
        for prefix, db in DB_MAPPING.items():
            if filename.startswith(prefix):
                target_db = db
                break
        
        migration_files.append({
            "path": file_path,
            "filename": filename,
            "target_db": target_db,
            "size": os.path.getsize(file_path),
            "modified": datetime.fromtimestamp(os.path.getmtime(file_path))
        })
    
    return migration_files


def execute_sql_file(config: ConnectionConfig, file_path: str) -> tuple:
    """
    Execute a SQL file.
    
    Returns:
        (success: bool, message: str, rows_affected: int)
    """
    try:
        # Read SQL file as UTF-8 (required for Hebrew/multilingual content)
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Connect and execute
        # Note: get_connection() already sets client_encoding=UTF8 globally.
        conn = get_connection(config)
        # Use autocommit so DDL statements (CREATE EXTENSION, CREATE SCHEMA, etc.)
        # in the migration file are not wrapped in a single implicit transaction.
        conn.autocommit = True
        cursor = conn.cursor()
        
        rows_affected = 0
        
        try:
            # Execute the SQL
            cursor.execute(sql_content)
            rows_affected = cursor.rowcount
            
            cursor.close()
            conn.close()
            
            return (True, f"✅ Successfully executed! Rows affected: {rows_affected}", rows_affected)
            
        except Exception as e:
            cursor.close()
            conn.close()
            return (False, f"❌ Execution failed: {str(e)}", 0)
            
    except Exception as e:
        return (False, f"❌ Failed to read file: {str(e)}", 0)


def rollback_migration(config: ConnectionConfig, filename: str, target_db: str) -> tuple:
    """
    Rollback a migration by deleting migrated data and clearing mapping table.
    
    Returns:
        (success: bool, message: str, rows_deleted: int)
    """
    # Determine table info from filename
    table_info = None
    for prefix, info in TABLE_MAPPING.items():
        if filename.startswith(prefix):
            table_info = info
            break
    
    if not table_info:
        return (False, "❌ Unknown migration type", 0)
    
    try:
        conn = get_connection(config)
        conn.autocommit = False
        cursor = conn.cursor()
        
        total_deleted = 0
        
        try:
            # Get count of migrated records from mapping table
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM migration.id_mappings 
                WHERE table_name = %s
            """, (table_info["mapping_table"],))
            
            mapped_count = cursor.fetchone()[0]
            
            if mapped_count == 0:
                cursor.close()
                conn.close()
                return (True, "ℹ️ No migrated records found to rollback", 0)
            
            # Delete using FK-aware queries (children before parents)
            for table, where_clause in table_info["deletes"]:
                try:
                    cursor.execute(f"DELETE FROM {table} WHERE {where_clause}")
                    deleted = cursor.rowcount
                    total_deleted += deleted
                except Exception:
                    pass
            
            # Clear migration mappings
            cursor.execute("""
                DELETE FROM migration.id_mappings 
                WHERE table_name = %s
            """, (table_info["mapping_table"],))
            
            # Clear batch log
            cursor.execute("""
                DELETE FROM migration.batch_log 
                WHERE table_name = %s
            """, (table_info["mapping_table"],))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return (True, f"✅ Rollback successful! Deleted {total_deleted} records and {mapped_count} mappings", total_deleted)
            
        except Exception as e:
            conn.rollback()
            cursor.close()
            conn.close()
            return (False, f"❌ Rollback failed: {str(e)}", 0)
            
    except Exception as e:
        return (False, f"❌ Failed to connect: {str(e)}", 0)


def render_migration_files():
    """Render the list of migration files with run buttons."""
    
    # Check if target connection is configured
    if "target_config" not in st.session_state:
        st.warning("⚠️ Please configure target database connection first on the **Target** page.")
        return
    
    schema_mode = st.session_state.get("target_schema_mode", "databases")
    
    if schema_mode != "databases":
        st.warning("⚠️ This feature requires 'databases' mode. Please set TARGET_SCHEMA_MODE=databases in Target page.")
        return
    
    # Get migration files
    migration_files = get_migration_files()
    
    if not migration_files:
        st.info("📭 No migration SQL files found in `output/migrations/`")
        st.markdown("Run the **Select Data** extraction first to generate migration files.")
        return
    
    st.markdown(f"**Found {len(migration_files)} migration file(s)**")
    st.markdown("---")
    
    # Initialize execution status in session state
    if "migration_status" not in st.session_state:
        st.session_state.migration_status = {}
    
    # Show all expected steps in order, greying out missing ones
    ALL_STEPS = [
        ("01_users_", "user_db", "Users"),
        ("02_folders_", "document_db", "Document folders"),
        ("03_documents_", "document_db", "Documents"),
        ("04_chunks_embeddings_", "document_db", "Chunks & embeddings"),
        ("05_conversations_", "completion_db", "Conversations"),
        ("06_agents_", "completion_db", "Agents"),
        ("07_conversions_", "completion_db", "Agent-conversation links"),
    ]
    files_by_prefix = {}
    for file_info in migration_files:
        for prefix, _, _ in ALL_STEPS:
            if file_info["filename"].startswith(prefix):
                files_by_prefix[prefix] = file_info
                break

    for step_prefix, step_db, step_label in ALL_STEPS:
        file_info = files_by_prefix.get(step_prefix)
        if not file_info:
            st.markdown(
                f"<div style='padding:12px 16px;border-radius:4px;background:#f0f0f0;"
                f"color:#999;margin-bottom:8px'>"
                f"🗃️ {step_prefix[:-1]} — <em>{step_label}: 0 rows, skipped</em>"
                f" &nbsp;(target: {step_db})</div>",
                unsafe_allow_html=True,
            )
            continue

        filename = file_info["filename"]
        target_db = file_info["target_db"]
        
        # Create a container for each file
        with st.container():
            col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
            
            with col1:
                st.markdown(f"### {step_prefix[:2]}. {filename}")
                st.caption(f"📊 Target DB: **{target_db}** | Size: {file_info['size']:,} bytes | Modified: {file_info['modified'].strftime('%Y-%m-%d %H:%M:%S')}")
            
            with col2:
                # Show status
                status = st.session_state.migration_status.get(filename, {})
                if status.get("success") is True:
                    st.success("✅ Executed")
                elif status.get("success") is False:
                    st.error("❌ Failed")
                else:
                    st.info("⏸️ Pending")
            
            with col3:
                # Run button
                if st.button(f"▶️ Run", key=f"run_{filename}", type="primary"):
                    base_config = st.session_state["target_config"]
                    db_config = ConnectionConfig(
                        host=base_config.host,
                        port=base_config.port,
                        database=target_db,
                        username=base_config.username,
                        password=base_config.password
                    )
                    
                    with st.spinner(f"Executing {filename} on {target_db}..."):
                        success, message, rows = execute_sql_file(db_config, file_info["path"])
                        
                        status_entry = {
                            "success": success,
                            "message": message,
                            "rows_affected": rows,
                            "timestamp": datetime.now()
                        }

                        # Track per-step result
                        batch_id = st.session_state.get("_current_batch_id")
                        _update_batch_step_results(batch_id, filename, success, message if not success else None)

                        if success:
                            with st.spinner("Verifying destination tables..."):
                                base_config = st.session_state["target_config"]
                                status_entry["verification"] = verify_migration_result(
                                    base_config, filename
                                )

                        st.session_state.migration_status[filename] = status_entry

                        # Auto-finalize batch if all files executed successfully
                        if success and batch_id:
                            all_done = all(
                                st.session_state.migration_status.get(f["filename"], {}).get("success")
                                for f in migration_files
                            )
                            if all_done:
                                _finalize_batch(batch_id)

                        st.rerun()
            
            with col4:
                # Rollback button with popover confirmation
                with st.popover("🔙 Rollback", use_container_width=True):
                    st.warning("⚠️ **Warning: This will delete all migrated data!**")
                    st.markdown(f"""This will:
- Delete all records from the target tables
- Clear migration mappings
- Clear batch logs

**Target DB:** {target_db}
**File:** {filename}""")
                    
                    if st.button("Confirm Rollback", key=f"confirm_rollback_{filename}", type="primary"):
                        # Create config for target database
                        base_config = st.session_state["target_config"]
                        db_config = ConnectionConfig(
                            host=base_config.host,
                            port=base_config.port,
                            database=target_db,
                            username=base_config.username,
                            password=base_config.password
                        )
                        
                        # Execute rollback
                        with st.spinner(f"Rolling back {filename} on {target_db}..."):
                            success, message, rows = rollback_migration(db_config, filename, target_db)
                            
                            # Update status to show rollback
                            st.session_state.migration_status[filename] = {
                                "success": None,  # Reset to pending
                                "message": message,
                                "rows_affected": rows,
                                "timestamp": datetime.now(),
                                "rollback": True
                            }
                            
                            st.rerun()
            
            # Show execution result if available
            status = st.session_state.migration_status.get(filename, {})
            if status:
                if status.get("success"):
                    st.success(status.get("message", "Success"))
                else:
                    st.error(status.get("message", "Failed"))
                
                if status.get("timestamp"):
                    st.caption(f"Executed at: {status['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")

                # Destination verification (only shown after a successful run)
                if status.get("success") and "verification" in status:
                    render_verification(status["verification"])
            
            # Show SQL preview
            with st.expander(f"📄 View SQL ({filename})"):
                try:
                    with open(file_info["path"], 'r', encoding='utf-8') as f:
                        sql_preview = f.read()
                    
                    # Show full content with scrollable text area
                    lines = sql_preview.split('\n')
                    total_lines = len(lines)
                    
                    st.caption(f"📏 Total lines: {total_lines:,} | Size: {file_info['size']:,} bytes")
                    
                    # Use text_area for scrollable view with full content
                    st.text_area(
                        "SQL Content (scrollable)",
                        value=sql_preview,
                        height=400,
                        label_visibility="collapsed"
                    )
                    
                    # Download button
                    st.download_button(
                        label="💾 Download SQL",
                        data=sql_preview,
                        file_name=filename,
                        mime="text/plain",
                        key=f"download_{filename}"
                    )
                    
                except Exception as e:
                    st.error(f"Failed to read file: {str(e)}")
            
            st.markdown("---")
    
    # Bulk actions
    st.markdown("### 🎛️ Bulk Actions")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("▶️ Run All (In Order)", type="primary"):
            base_config = st.session_state["target_config"]
            batch_id = st.session_state.get("_current_batch_id")
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            all_success = True
            
            for idx, file_info in enumerate(migration_files):
                filename = file_info["filename"]
                target_db = file_info["target_db"]
                
                status_text.text(f"Executing {idx + 1}/{len(migration_files)}: {filename}")
                
                db_config = ConnectionConfig(
                    host=base_config.host,
                    port=base_config.port,
                    database=target_db,
                    username=base_config.username,
                    password=base_config.password
                )
                
                success, message, rows = execute_sql_file(db_config, file_info["path"])

                status_entry = {
                    "success": success,
                    "message": message,
                    "rows_affected": rows,
                    "timestamp": datetime.now()
                }

                _update_batch_step_results(batch_id, filename, success, message if not success else None)

                if success:
                    status_text.text(f"Verifying {idx + 1}/{len(migration_files)}: {filename}")
                    status_entry["verification"] = verify_migration_result(base_config, filename)

                st.session_state.migration_status[filename] = status_entry
                progress_bar.progress((idx + 1) / len(migration_files))

                if not success:
                    all_success = False
                    st.error(f"Stopped at {filename}: {message}")
                    break

            if all_success:
                _finalize_batch(batch_id)
            
            status_text.text("✅ Bulk execution complete!" if all_success else "⚠️ Execution stopped due to error.")
            st.rerun()
    
    with col2:
        if st.button("🗑️ Clear Status"):
            st.session_state.migration_status = {}
            st.rerun()


def main():
    """Main page function."""
    
    st.markdown("""
    This page allows you to run generated migration SQL files one by one.
    
    **✅ How it works:**
    1. SQL files are loaded from `output/migrations/`
    2. Each file is automatically mapped to the correct database (user_db, document_db, completion_db)
    3. Click **▶️ Run** to execute a single file
    4. Click **▶️ Run All** to execute all files in order
    
    **⚠️ Important:**
    - Files should be run in order (01 → 02 → 03 → ...)
    - Each file runs in a transaction (rollback on error)
    - Target database connection must be configured first
    """)
    
    st.markdown("---")
    
    render_migration_files()


if __name__ == "__main__":
    main()
