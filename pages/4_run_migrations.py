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

# Database mapping for each migration file prefix
DB_MAPPING = {
    "01_users_": "user_db",
    "02_folders_": "document_db",  # folders is in document_db, not user_db
    "03_documents_": "document_db",
    "04_chunks_embeddings_": "document_db",
    "05_conversations_": "completion_db",
    "06_agents_": "completion_db",
}

# Table mapping for rollback
TABLE_MAPPING = {
    "01_users_": {"tables": ["users"], "mapping_table": "users"},
    "02_folders_": {"tables": ["folders"], "mapping_table": "folders"},
    "03_documents_": {"tables": ["documents"], "mapping_table": "documents"},
    "04_chunks_embeddings_": {"tables": ["chunks", "embeddings"], "mapping_table": "documents"},
    "05_conversations_": {"tables": ["conversations", "messages", "message_content_blocks"], "mapping_table": "conversations"},
    "06_agents_": {"tables": ["agents", "agent_settings", "agent_documents"], "mapping_table": "agents"},
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

        # Per-table row counts as metrics
        tables = verif.get("tables", {})
        if tables:
            cols = st.columns(max(len(tables), 1))
            for i, (tbl, cnt) in enumerate(tables.items()):
                with cols[i]:
                    if cnt is not None:
                        st.metric(label=f"📊 {tbl}", value=f"{cnt:,}")
                    else:
                        st.metric(label=f"📊 {tbl}", value="N/A")

        # Tracked IDs footnote
        mid = verif.get("migrated_ids")
        if mid is not None:
            st.caption(
                f"🗂️ Total IDs tracked in `migration.id_mappings` "
                f"for this table type: **{mid:,}**"
            )

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
        # Read SQL file
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Connect and execute
        conn = get_connection(config)
        conn.autocommit = False  # Use transactions
        cursor = conn.cursor()
        
        rows_affected = 0
        
        try:
            # Execute the SQL
            cursor.execute(sql_content)
            rows_affected = cursor.rowcount
            conn.commit()
            
            cursor.close()
            conn.close()
            
            return (True, f"✅ Successfully executed! Rows affected: {rows_affected}", rows_affected)
            
        except Exception as e:
            conn.rollback()
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
            
            # Delete from tables in reverse order (children first)
            for table in reversed(table_info["tables"]):
                # Special handling for chunks/embeddings which use document mapping
                if table in ["chunks", "embeddings"]:
                    # Delete chunks/embeddings based on document_id from migration mappings
                    if table == "chunks":
                        cursor.execute(f"""
                            DELETE FROM {table}
                            WHERE document_id IN (
                                SELECT new_id FROM migration.id_mappings 
                                WHERE table_name = 'documents'
                            )
                        """)
                    elif table == "embeddings":
                        cursor.execute(f"""
                            DELETE FROM {table}
                            WHERE document_id IN (
                                SELECT new_id FROM migration.id_mappings 
                                WHERE table_name = 'documents'
                            )
                        """)
                else:
                    # Delete based on id from migration mappings
                    cursor.execute(f"""
                        DELETE FROM {table}
                        WHERE id IN (
                            SELECT new_id FROM migration.id_mappings 
                            WHERE table_name = %s
                        )
                    """, (table_info["mapping_table"],))
                
                deleted = cursor.rowcount
                total_deleted += deleted
            
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
    
    # Display each migration file
    for idx, file_info in enumerate(migration_files):
        filename = file_info["filename"]
        target_db = file_info["target_db"]
        
        # Create a container for each file
        with st.container():
            col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
            
            with col1:
                st.markdown(f"### {idx + 1}. {filename}")
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
                    # Create config for target database
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

                        # Verify destination tables on success
                        if success:
                            with st.spinner("Verifying destination tables..."):
                                base_config = st.session_state["target_config"]
                                status_entry["verification"] = verify_migration_result(
                                    base_config, filename
                                )

                        st.session_state.migration_status[filename] = status_entry
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
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for idx, file_info in enumerate(migration_files):
                filename = file_info["filename"]
                target_db = file_info["target_db"]
                
                status_text.text(f"Executing {idx + 1}/{len(migration_files)}: {filename}")
                
                # Create config for target database
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

                # Verify destination tables on success
                if success:
                    status_text.text(f"Verifying {idx + 1}/{len(migration_files)}: {filename}")
                    status_entry["verification"] = verify_migration_result(base_config, filename)

                st.session_state.migration_status[filename] = status_entry

                # Update progress
                progress_bar.progress((idx + 1) / len(migration_files))

                # Stop on first error
                if not success:
                    st.error(f"Stopped at {filename}: {message}")
                    break
            
            status_text.text("✅ Bulk execution complete!")
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
