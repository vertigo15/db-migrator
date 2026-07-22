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
import json
import streamlit as st
import psycopg2
from datetime import datetime

from utils.db import ConnectionConfig, get_connection
from utils.config import SessionKeys, get_env_connection_defaults
from utils.sql_generator import NAMESPACE_UUID
from utils.migration_tracking import (
    finalize_distributed_run,
    reconcile_rollback_status,
    record_step_result,
)

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


def _ensure_source_config():
    """Auto-populate source_config from .env for V4 audit mirroring."""
    if "source_config" in st.session_state:
        return
    env_defaults = get_env_connection_defaults()
    if (
        env_defaults.get("host")
        and env_defaults.get("database")
        and env_defaults.get("username")
        and env_defaults.get("password")
    ):
        st.session_state["source_config"] = ConnectionConfig(
            host=env_defaults["host"],
            port=int(env_defaults["port"]),
            database=env_defaults["database"],
            username=env_defaults["username"],
            password=env_defaults["password"],
        )


def _source_tracking_config():
    return st.session_state.get("source_config")


_ensure_target_config()
_ensure_source_config()


def _update_batch_step_results(
    batch_id: str,
    step_name: str,
    success: bool,
    error_msg: str = None,
    affected_count: int = None,
):
    """Update migration_user_results after a SQL step executes.

    On success: appends step to steps_completed JSONB for all pending users.
    On failure: marks all pending users as failed with the step name and error.
    """
    if not batch_id:
        return
    step_key = step_name
    target_database = "user_db"
    for prefix in ["01_users", "02_folders", "03_documents", "04_chunks_embeddings",
                   "05_conversations", "06_agents", "07_conversions"]:
        if step_name.startswith(prefix):
            step_key = prefix
            target_database = DB_MAPPING[f"{prefix}_"]
            break
    base_config = st.session_state.get("target_config")
    if base_config is None:
        raise RuntimeError("Target connection is unavailable for migration tracking.")
    record_step_result(
        base_config,
        batch_id,
        step_key,
        target_database,
        success,
        affected_count=affected_count,
        error_message=error_msg[:500] if error_msg else None,
        source_config=_source_tracking_config(),
    )


def _finalize_batch(batch_id: str):
    """Mark all remaining pending users as success and close the batch."""
    if not batch_id:
        return
    base_config = st.session_state.get("target_config")
    if base_config is None:
        raise RuntimeError("Target connection is unavailable for migration tracking.")
    finalize_distributed_run(
        base_config,
        batch_id,
        source_config=_source_tracking_config(),
    )


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

ALL_STEPS = [
    ("01_users_", "user_db", "Users"),
    ("02_folders_", "document_db", "Document folders"),
    ("03_documents_", "document_db", "Documents"),
    ("04_chunks_embeddings_", "document_db", "Chunks & embeddings"),
    ("05_conversations_", "completion_db", "Conversations"),
    ("06_agents_", "completion_db", "Agents"),
    ("07_conversions_", "completion_db", "Agent-conversation links"),
]
ROLLBACK_STEP_ORDER = list(reversed(ALL_STEPS))

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
        "clear_mappings": False,
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
            ("knowledge_base_items", "knowledge_base_id IN (SELECT knowledge_base_id FROM knowledge_base_assignments WHERE assigned_to_id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'agents'))"),
            ("knowledge_base_assignments", "assigned_to_id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'agents')"),
            ("knowledge_bases", f"id IN (SELECT migration.deterministic_uuid_v4('{NAMESPACE_UUID}'::uuid, old_id || '-kb') FROM migration.id_mappings WHERE table_name = 'agents')"),
            ("agent_settings", "agent_id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'agents')"),
            ("legacy_bot_to_agent_mapping", "new_agent_id IN (SELECT new_id FROM migration.id_mappings WHERE table_name = 'agents')"),
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

ROLLBACK_DEPENDENCIES = {
    "01_users": {
        "02_folders", "03_documents", "04_chunks_embeddings",
        "05_conversations", "06_agents", "07_conversions",
    },
    "02_folders": {"03_documents", "04_chunks_embeddings", "06_agents"},
    "03_documents": {"04_chunks_embeddings", "06_agents"},
    "04_chunks_embeddings": set(),
    "05_conversations": set(),
    "06_agents": {"07_conversions"},
    "07_conversions": set(),
}


def _rollback_order_blockers(
    config: ConnectionConfig,
    step_key: str,
    migration_run_id: str,
) -> list:
    blockers = []
    for dependency in ROLLBACK_DEPENDENCIES.get(step_key, set()):
        database = DB_MAPPING[f"{dependency}_"]
        dependency_config = ConnectionConfig(
            host=config.host,
            port=config.port,
            database=database,
            username=config.username,
            password=config.password,
        )
        conn = None
        try:
            conn = get_connection(dependency_config)
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT status
                    FROM migration.migration_steps
                    WHERE migration_run_id = %s::uuid AND step_key = %s
                    """,
                    (migration_run_id, dependency),
                )
                row = cursor.fetchone()
                if row is None:
                    blockers.append(f"{dependency}=tracking_missing")
                elif row[0] not in (
                    "pending", "skipped", "failed", "rolled_back"
                ):
                    blockers.append(f"{dependency}={row[0]}")
        except Exception as exc:
            blockers.append(f"{dependency}=tracking_unavailable({exc})")
        finally:
            if conn is not None and not conn.closed:
                conn.close()
    return sorted(blockers)


def verify_migration_result(
    base_config: ConnectionConfig,
    filename: str,
    migration_run_id: str = None,
) -> dict:
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
            if migration_run_id:
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM migration.id_mappings
                    WHERE table_name = %s AND migration_run_id = %s::uuid
                    """,
                    (table_info["mapping_table"], migration_run_id),
                )
            else:
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM migration.id_mappings
                    WHERE table_name = %s
                    """,
                    (table_info["mapping_table"],),
                )
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


def _verify_step_before_commit(
    cursor,
    filename: str,
    migration_run_id: str,
) -> tuple:
    """Return a truthful affected count or raise before the step commits."""
    step_key = next(
        (
            prefix.rstrip("_")
            for prefix in TABLE_MAPPING
            if filename.startswith(prefix)
        ),
        None,
    )
    if step_key is None:
        raise RuntimeError(f"Unknown migration step for {filename}")
    cursor.execute(
        """
        SELECT expected_count, verification_details
        FROM migration.migration_steps
        WHERE migration_run_id = %s::uuid AND step_key = %s
        """,
        (migration_run_id, step_key),
    )
    tracking = cursor.fetchone()
    if tracking is None or tracking[0] is None:
        raise RuntimeError(
            f"Missing extraction expectation for {step_key}; refusing to commit"
        )
    expected_count = int(tracking[0])
    expected_details = tracking[1] or {}

    if step_key == "04_chunks_embeddings":
        cursor.execute(
            """
            WITH run_docs AS (
                SELECT m.new_id AS document_id, dp.id AS processing_id
                FROM migration.id_mappings m
                LEFT JOIN public.document_processing dp
                  ON dp.document_id = m.new_id
                 AND dp.deleted_at IS NULL
                WHERE m.table_name = 'documents'
                  AND m.migration_run_id = %s::uuid
                  AND m.record_action = 'created'
            )
            SELECT
                (
                    SELECT COUNT(DISTINCT c.id)
                    FROM run_docs d
                    JOIN public.chunks c
                      ON c.document_id = d.document_id
                     AND c.document_processing_id = d.processing_id
                ),
                (
                    SELECT COUNT(DISTINCT e.id)
                    FROM run_docs d
                    JOIN public.chunks c
                      ON c.document_id = d.document_id
                     AND c.document_processing_id = d.processing_id
                    JOIN public.embeddings e
                      ON e.document_id = d.document_id
                     AND e.chunk_id = c.id
                ),
                (
                    SELECT COUNT(DISTINCT c.id)
                    FROM run_docs d
                    JOIN public.chunks c ON c.document_id = d.document_id
                    WHERE c.document_processing_id IS DISTINCT FROM d.processing_id
                ),
                (
                    SELECT COUNT(*)
                    FROM run_docs
                    WHERE processing_id IS NULL
                )
            """,
            (migration_run_id,),
        )
        (
            chunk_count,
            embedding_count,
            invalid_processing_links,
            missing_processing_rows,
        ) = (int(value) for value in cursor.fetchone())
        expected_embeddings = int(
            expected_details.get("expected_embeddings", expected_count)
        )
        if (
            chunk_count != expected_count
            or embedding_count != expected_embeddings
            or invalid_processing_links
            or missing_processing_rows
        ):
            raise RuntimeError(
                "Step verification failed: "
                f"chunks {chunk_count}/{expected_count}, "
                f"embeddings {embedding_count}/{expected_embeddings}, "
                f"invalid processing links {invalid_processing_links}, "
                f"documents missing processing rows {missing_processing_rows}"
            )
        actual_details = {
            "actual_chunks": chunk_count,
            "actual_embeddings": embedding_count,
            "invalid_processing_links": invalid_processing_links,
            "missing_processing_rows": missing_processing_rows,
        }
        affected_count = chunk_count
    else:
        mapping_table = TABLE_MAPPING[
            next(prefix for prefix in TABLE_MAPPING if filename.startswith(prefix))
        ]["mapping_table"]
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM migration.id_mappings
            WHERE table_name = %s AND migration_run_id = %s::uuid
            """,
            (mapping_table, migration_run_id),
        )
        affected_count = int(cursor.fetchone()[0])
        if affected_count != expected_count:
            raise RuntimeError(
                f"Step verification failed for {step_key}: "
                f"{affected_count}/{expected_count} run-scoped mappings"
            )
        actual_details = {"actual_mappings": affected_count}

    cursor.execute(
        """
        UPDATE migration.migration_steps
        SET verification_details =
            COALESCE(verification_details, '{}'::jsonb) || %s::jsonb
        WHERE migration_run_id = %s::uuid AND step_key = %s
        """,
        (json.dumps(actual_details), migration_run_id, step_key),
    )
    return affected_count, actual_details


def execute_sql_file(
    config: ConnectionConfig,
    file_path: str,
    migration_run_id: str = None,
) -> tuple:
    """
    Execute a SQL file.
    
    Returns:
        (success: bool, message: str, rows_affected: int)
    """
    try:
        # Read SQL file as UTF-8 (required for Hebrew/multilingual content)
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        if migration_run_id and migration_run_id not in sql_content:
            return (
                False,
                "❌ SQL file does not belong to the selected migration run",
                0,
            )
        
        # Connect and execute
        # Note: get_connection() already sets client_encoding=UTF8 globally.
        conn = get_connection(config)
        # Execute each generated file atomically. DDL used by these files is
        # transactional in PostgreSQL, so a failed statement must not leave a
        # partially-applied migration step.
        conn.autocommit = False
        cursor = conn.cursor()
        
        rows_affected = 0
        
        try:
            # Execute the SQL
            cursor.execute(sql_content)
            if migration_run_id:
                rows_affected, _ = _verify_step_before_commit(
                    cursor,
                    os.path.basename(file_path),
                    migration_run_id,
                )
            else:
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


def _cross_database_rollback_blockers(
    config: ConnectionConfig,
    mapping_table: str,
    new_ids: list,
) -> list:
    """Return dependent rows that require reverse-order rollback first."""
    if not new_ids:
        return []

    checks = []
    if mapping_table == "users":
        checks.extend([
            ("document_db", "folders", "user_id"),
            ("document_db", "documents", "user_id"),
            ("completion_db", "agents", "user_id"),
            ("completion_db", "conversations", "user_id"),
            ("completion_db", "conversions", "user_id"),
        ])
    elif mapping_table == "documents":
        checks.append(("completion_db", "knowledge_base_items", "item_id"))
    elif mapping_table == "folders":
        checks.extend([
            ("completion_db", "knowledge_base_items", "item_id"),
            ("completion_db", "agents", "folder_id"),
        ])

    blockers = []
    for database, table, column in checks:
        dep_config = ConnectionConfig(
            host=config.host,
            port=config.port,
            database=database,
            username=config.username,
            password=config.password,
        )
        conn = get_connection(dep_config)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT COUNT(*) FROM public.{table} WHERE {column} = ANY(%s::uuid[])",
                    (new_ids,),
                )
                count = cursor.fetchone()[0]
                if count:
                    blockers.append(f"{database}.{table}: {count}")
        finally:
            conn.close()
    return blockers


def _local_rollback_blockers(
    cursor,
    step_key: str,
    mapped_ids: list,
    migration_run_id: str,
) -> list:
    """Detect rows that rollback would unexpectedly cascade-delete or mutate."""
    if not mapped_ids:
        return []
    checks = []
    if step_key == "02_folders":
        checks = [
            (
                "folders.child",
                """
                SELECT COUNT(*) FROM folders
                WHERE parent_id = ANY(%s::uuid[])
                  AND NOT (id = ANY(%s::uuid[]))
                """,
                (mapped_ids, mapped_ids),
            ),
            (
                "documents.folder",
                """
                SELECT COUNT(*) FROM documents d
                WHERE d.folder_id = ANY(%s::uuid[])
                  AND NOT EXISTS (
                      SELECT 1 FROM migration.id_mappings m
                      WHERE m.table_name = 'documents'
                        AND m.new_id = d.id
                        AND m.migration_run_id = %s::uuid
                        AND m.record_action = 'created'
                  )
                """,
                (mapped_ids, migration_run_id),
            ),
            (
                "upload_batch.target_folder",
                "SELECT COUNT(*) FROM upload_batch WHERE target_folder_id = ANY(%s::uuid[])",
                (mapped_ids,),
            ),
        ]
    elif step_key == "03_documents":
        checks = [
            (
                "upload_attempts.document",
                "SELECT COUNT(*) FROM upload_attempts WHERE document_id = ANY(%s::uuid[])",
                (mapped_ids,),
            ),
            (
                "external_source_reference.document",
                "SELECT COUNT(*) FROM external_source_reference WHERE document_id = ANY(%s::uuid[])",
                (mapped_ids,),
            ),
            (
                "chunks.document",
                "SELECT COUNT(*) FROM chunks WHERE document_id = ANY(%s::uuid[])",
                (mapped_ids,),
            ),
            (
                "embeddings.document",
                "SELECT COUNT(*) FROM embeddings WHERE document_id = ANY(%s::uuid[])",
                (mapped_ids,),
            ),
        ]
    elif step_key == "05_conversations":
        checks = [
            (
                "canvases.conversation",
                "SELECT COUNT(*) FROM canvases WHERE conversation_id = ANY(%s::uuid[])",
                (mapped_ids,),
            ),
            (
                "document_attachments.conversation",
                "SELECT COUNT(*) FROM document_attachments WHERE conversation_id = ANY(%s::uuid[])",
                (mapped_ids,),
            ),
            (
                "message_reactions.message",
                """
                SELECT COUNT(*) FROM message_reactions
                WHERE message_id IN (
                    SELECT id FROM messages
                    WHERE conversation_id = ANY(%s::uuid[])
                )
                """,
                (mapped_ids,),
            ),
        ]
    elif step_key == "06_agents":
        checks = [
            (
                "agent_drafts.agent",
                """
                SELECT COUNT(*) FROM agent_drafts
                WHERE original_agent_id = ANY(%s::uuid[])
                   OR draft_agent_id = ANY(%s::uuid[])
                """,
                (mapped_ids, mapped_ids),
            ),
            (
                "agent_skills.agent",
                "SELECT COUNT(*) FROM agent_skills WHERE agent_id = ANY(%s::uuid[])",
                (mapped_ids,),
            ),
            (
                "agent_sub_agents.agent",
                """
                SELECT COUNT(*) FROM agent_sub_agents
                WHERE brain_agent_id = ANY(%s::uuid[])
                   OR sub_agent_id = ANY(%s::uuid[])
                """,
                (mapped_ids, mapped_ids),
            ),
            (
                "agent_conversions.agent",
                "SELECT COUNT(*) FROM agent_conversions WHERE agent_id = ANY(%s::uuid[])",
                (mapped_ids,),
            ),
        ]

    blockers = []
    for label, query, params in checks:
        relation = label.split(".", 1)[0]
        cursor.execute("SELECT to_regclass(%s)", (f"public.{relation}",))
        if cursor.fetchone()[0] is None:
            continue
        cursor.execute(query, params)
        count = int(cursor.fetchone()[0])
        if count:
            blockers.append(f"{label}: {count}")
    return blockers


def _execute_scoped_rollback(
    cursor,
    step_key: str,
    mapped_ids: list,
    migration_run_id: str,
) -> int:
    """Execute explicit child-before-parent deletes for one tracked step."""
    deleted = 0

    def delete(query, params):
        nonlocal deleted
        cursor.execute(query, params)
        deleted += max(cursor.rowcount, 0)

    if step_key == "07_conversions":
        delete(
            "DELETE FROM agent_conversions WHERE conversion_id = ANY(%s::uuid[])",
            (mapped_ids,),
        )
        delete("DELETE FROM conversions WHERE id = ANY(%s::uuid[])", (mapped_ids,))
    elif step_key == "06_agents":
        cursor.execute(
            """
            SELECT DISTINCT new_id
            FROM migration.id_mappings
            WHERE table_name = 'knowledge_bases'
              AND migration_run_id = %s::uuid
              AND record_action = 'created'
            UNION
            SELECT migration.deterministic_uuid_v4(
                %s::uuid, old_id || '-kb'
            )
            FROM migration.id_mappings
            WHERE table_name = 'agents'
              AND migration_run_id = %s::uuid
              AND record_action = 'created'
            """,
            (migration_run_id, NAMESPACE_UUID, migration_run_id),
        )
        kb_ids = [str(row[0]) for row in cursor.fetchall()]
        if kb_ids:
            cursor.execute(
                """
                SELECT COUNT(*) FROM knowledge_base_assignments
                WHERE knowledge_base_id = ANY(%s::uuid[])
                  AND NOT (
                      assigned_to_type = 'agent'
                      AND assigned_to_id = ANY(%s::uuid[])
                  )
                """,
                (kb_ids, mapped_ids),
            )
            shared = int(cursor.fetchone()[0])
            if shared:
                raise RuntimeError(
                    f"Rollback blocked: {shared} shared KB assignment(s)"
                )
            delete(
                "DELETE FROM knowledge_base_items WHERE knowledge_base_id = ANY(%s::uuid[])",
                (kb_ids,),
            )
            delete(
                "DELETE FROM knowledge_base_assignments WHERE knowledge_base_id = ANY(%s::uuid[])",
                (kb_ids,),
            )
            delete(
                "DELETE FROM knowledge_bases WHERE id = ANY(%s::uuid[])",
                (kb_ids,),
            )
        delete("DELETE FROM agent_settings WHERE agent_id = ANY(%s::uuid[])", (mapped_ids,))
        delete(
            "DELETE FROM legacy_bot_to_agent_mapping WHERE new_agent_id = ANY(%s::uuid[])",
            (mapped_ids,),
        )
        delete("DELETE FROM agents WHERE id = ANY(%s::uuid[])", (mapped_ids,))
        cursor.execute(
            """
            DELETE FROM migration.id_mappings
            WHERE migration_run_id = %s::uuid
              AND table_name IN (
                  'knowledge_base_items',
                  'knowledge_base_assignments',
                  'knowledge_bases'
              )
            """,
            (migration_run_id,),
        )
    elif step_key == "05_conversations":
        delete(
            """
            DELETE FROM message_content_blocks
            WHERE message_id IN (
                SELECT id FROM messages
                WHERE conversation_id = ANY(%s::uuid[])
            )
            """,
            (mapped_ids,),
        )
        delete(
            "DELETE FROM messages WHERE conversation_id = ANY(%s::uuid[])",
            (mapped_ids,),
        )
        delete("DELETE FROM conversations WHERE id = ANY(%s::uuid[])", (mapped_ids,))
    elif step_key == "04_chunks_embeddings":
        delete(
            "DELETE FROM embeddings WHERE document_id = ANY(%s::uuid[])",
            (mapped_ids,),
        )
        delete(
            "DELETE FROM chunks WHERE document_id = ANY(%s::uuid[])",
            (mapped_ids,),
        )
    elif step_key == "03_documents":
        delete("DELETE FROM documents WHERE id = ANY(%s::uuid[])", (mapped_ids,))
    elif step_key == "02_folders":
        delete("DELETE FROM folders WHERE id = ANY(%s::uuid[])", (mapped_ids,))
    elif step_key == "01_users":
        delete("DELETE FROM users WHERE id = ANY(%s::uuid[])", (mapped_ids,))
    else:
        raise RuntimeError(f"Unsupported rollback step: {step_key}")
    return deleted


def rollback_migration(
    config: ConnectionConfig,
    filename: str,
    target_db: str,
    migration_run_id: str,
) -> tuple:
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
    if not migration_run_id:
        return (False, "❌ Select a tracked migration run before rollback", 0)

    step_key = next(
        prefix.rstrip("_")
        for prefix in TABLE_MAPPING
        if filename.startswith(prefix)
    )
    order_blockers = _rollback_order_blockers(
        config, step_key, migration_run_id
    )
    if order_blockers:
        return (
            False,
            "❌ Rollback order violation; rollback these later steps first: "
            + ", ".join(order_blockers),
            0,
        )
    
    try:
        conn = get_connection(config)
        conn.autocommit = False
        cursor = conn.cursor()
        
        total_deleted = 0
        
        try:
            cursor.execute(
                """
                SELECT status
                FROM migration.migration_steps
                WHERE migration_run_id = %s::uuid AND step_key = %s
                """,
                (migration_run_id, step_key),
            )
            step_status = cursor.fetchone()
            if step_status is None:
                raise RuntimeError(
                    f"Missing tracking row for rollback step {step_key}"
                )
            if step_status[0] == "rolled_back":
                conn.rollback()
                cursor.close()
                conn.close()
                return (True, "ℹ️ Step was already rolled back.", 0)
            if step_status[0] in ("pending", "skipped"):
                raise RuntimeError(
                    f"Step {step_key} has status {step_status[0]} and cannot be rolled back"
                )

            # Snapshot only records created by this run. Reused entities are
            # intentionally excluded from all target-table DELETE statements.
            cursor.execute("""
                SELECT new_id
                FROM migration.id_mappings
                WHERE table_name = %s
                  AND migration_run_id = %s::uuid
                  AND record_action = 'created'
            """, (table_info["mapping_table"], migration_run_id))
            mapped_ids = [str(row[0]) for row in cursor.fetchall()]
            mapped_count = len(mapped_ids)
            cursor.execute("""
                SELECT COUNT(*)
                FROM migration.id_mappings
                WHERE table_name = %s
                  AND migration_run_id = %s::uuid
            """, (table_info["mapping_table"], migration_run_id))
            all_mapping_count = cursor.fetchone()[0]
            
            blockers = (
                _cross_database_rollback_blockers(
                    config, table_info["mapping_table"], mapped_ids
                )
                if table_info.get("clear_mappings", True)
                else []
            )
            if blockers:
                conn.rollback()
                cursor.close()
                conn.close()
                return (
                    False,
                    "❌ Rollback blocked by dependent rows; rollback later steps first: "
                    + ", ".join(blockers),
                    0,
                )

            local_blockers = _local_rollback_blockers(
                cursor,
                step_key,
                mapped_ids,
                migration_run_id,
            )
            if local_blockers:
                raise RuntimeError(
                    "Rollback would affect unexpected dependent rows: "
                    + ", ".join(local_blockers)
                )

            total_deleted = _execute_scoped_rollback(
                cursor,
                step_key,
                mapped_ids,
                migration_run_id,
            )

            parent_table = {
                "users": "users",
                "folders": "folders",
                "documents": "documents",
                "conversations": "conversations",
                "agents": "agents",
                "conversions": "conversions",
            }.get(table_info["mapping_table"])
            if parent_table and table_info.get("clear_mappings", True):
                cursor.execute(
                    f"SELECT COUNT(*) FROM public.{parent_table} WHERE id = ANY(%s::uuid[])",
                    (mapped_ids,),
                )
                if cursor.fetchone()[0]:
                    raise RuntimeError(
                        f"Rollback verification failed: {parent_table} rows survived"
                    )

            # Step 04 owns no document mappings, so it must never clear them.
            if table_info.get("clear_mappings", True):
                cursor.execute("""
                    DELETE FROM migration.id_mappings
                    WHERE table_name = %s
                      AND migration_run_id = %s::uuid
                """, (table_info["mapping_table"], migration_run_id))

            cursor.execute("""
                UPDATE migration.migration_steps
                SET status = 'rolled_back', completed_at = now()
                WHERE migration_run_id = %s::uuid AND step_key = %s
            """, (migration_run_id, step_key))
            cursor.execute("""
                UPDATE migration.migration_runs
                SET status = 'rollback_pending'
                WHERE id = %s::uuid
            """, (migration_run_id,))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return (
                True,
                (
                    f"✅ Batch-scoped rollback successful! Deleted {total_deleted} "
                    f"records created by run {migration_run_id}."
                    if all_mapping_count
                    else "ℹ️ No run-owned mappings existed; the step was marked rolled back."
                ),
                total_deleted,
            )
            
        except Exception as e:
            conn.rollback()
            cursor.close()
            conn.close()
            return (False, f"❌ Rollback failed: {str(e)}", 0)
            
    except Exception as e:
        return (False, f"❌ Failed to connect: {str(e)}", 0)


def rollback_all_migrations(
    base_config: ConnectionConfig,
    migration_files: list,
    migration_run_id: str,
    source_config: ConnectionConfig = None,
    progress_callback=None,
) -> tuple:
    """Rollback every produced step in strict reverse dependency order."""
    if not migration_run_id:
        return False, [], "No tracked migration run selected"

    files_by_prefix = {}
    for file_info in migration_files:
        for prefix, _, _ in ALL_STEPS:
            if file_info["filename"].startswith(prefix):
                files_by_prefix[prefix] = file_info
                break

    results = []
    total_steps = len(ROLLBACK_STEP_ORDER)
    for index, (prefix, database, label) in enumerate(ROLLBACK_STEP_ORDER):
        file_info = files_by_prefix.get(prefix)
        if file_info is None:
            continue
        config = ConnectionConfig(
            host=base_config.host,
            port=base_config.port,
            database=database,
            username=base_config.username,
            password=base_config.password,
        )
        conn = get_connection(config)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT status
                    FROM migration.migration_steps
                    WHERE migration_run_id = %s::uuid AND step_key = %s
                    """,
                    (migration_run_id, prefix.rstrip("_")),
                )
                row = cursor.fetchone()
                if row is None:
                    return (
                        False,
                        results,
                        f"Missing tracking row for {prefix.rstrip('_')}",
                    )
                status = row[0]
        finally:
            conn.close()

        if status in ("rolled_back", "skipped"):
            continue
        if status == "pending":
            return (
                False,
                results,
                f"Step {prefix.rstrip('_')} is pending; rollback refuses to guess "
                "whether untracked SQL was executed.",
            )

        if progress_callback:
            progress_callback(index, total_steps, label)
        success, message, rows = rollback_migration(
            config,
            file_info["filename"],
            database,
            migration_run_id,
        )
        result = {
            "filename": file_info["filename"],
            "database": database,
            "success": success,
            "message": message,
            "rows": rows,
        }
        results.append(result)
        overall = reconcile_rollback_status(
            base_config,
            migration_run_id,
            source_config=source_config,
        )
        if not success:
            return False, results, message

    overall = reconcile_rollback_status(
        base_config,
        migration_run_id,
        source_config=source_config,
    )
    if overall != "rolled_back":
        return (
            False,
            results,
            f"Rollback stopped in state {overall}; inspect pending/failed steps.",
        )
    return True, results, "All produced steps rolled back successfully"


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
    if not st.session_state.get("_current_batch_id"):
        st.error(
            "No migration run is selected. Return to Select Data and generate "
            "a tracked extraction before executing SQL files."
        )
        return
    
    st.markdown(f"**Found {len(migration_files)} migration file(s)**")
    st.markdown("---")
    
    # Initialize execution status in session state
    if "migration_status" not in st.session_state:
        st.session_state.migration_status = {}
    
    # Show all expected steps in order, greying out missing ones
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
                        success, message, rows = execute_sql_file(
                            db_config,
                            file_info["path"],
                            st.session_state.get("_current_batch_id"),
                        )
                        
                        status_entry = {
                            "success": success,
                            "message": message,
                            "rows_affected": rows,
                            "timestamp": datetime.now()
                        }

                        # Track per-step result
                        batch_id = st.session_state.get("_current_batch_id")
                        _update_batch_step_results(
                            batch_id,
                            filename,
                            success,
                            message if not success else None,
                            rows,
                        )

                        if success:
                            with st.spinner("Verifying destination tables..."):
                                base_config = st.session_state["target_config"]
                                status_entry["verification"] = verify_migration_result(
                                    base_config,
                                    filename,
                                    batch_id,
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
                    st.warning("⚠️ This deletes only rows created by the selected run.")
                    st.markdown(f"""This will:
- Delete records marked `created` for this run
- Preserve reused users and all pre-existing V5 data
- Preserve document mappings during Step 04 rollback

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
                            success, message, rows = rollback_migration(
                                db_config,
                                filename,
                                target_db,
                                st.session_state.get("_current_batch_id"),
                            )
                            if success:
                                reconcile_rollback_status(
                                    base_config,
                                    st.session_state.get("_current_batch_id"),
                                    source_config=_source_tracking_config(),
                                )
                            
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
    col1, col2, col3 = st.columns(3)
    
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
                
                success, message, rows = execute_sql_file(
                    db_config,
                    file_info["path"],
                    batch_id,
                )

                status_entry = {
                    "success": success,
                    "message": message,
                    "rows_affected": rows,
                    "timestamp": datetime.now()
                }

                _update_batch_step_results(
                    batch_id,
                    filename,
                    success,
                    message if not success else None,
                    rows,
                )

                if success:
                    status_text.text(f"Verifying {idx + 1}/{len(migration_files)}: {filename}")
                    status_entry["verification"] = verify_migration_result(
                        base_config,
                        filename,
                        batch_id,
                    )

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
        with st.popover("🔙 Rollback All (Reverse Order)", use_container_width=True):
            st.warning(
                "This removes only rows created by the selected run. "
                "Reused users and pre-existing V5 data are preserved."
            )
            st.code(
                "07 → 06 → 05 → 04 → 03 → 02 → 01",
                language=None,
            )
            confirm_run = st.text_input(
                "Type the migration run ID to confirm",
                key="rollback_all_confirmation",
            )
            batch_id = st.session_state.get("_current_batch_id")
            if st.button(
                "Confirm Rollback All",
                type="primary",
                disabled=confirm_run != batch_id,
            ):
                progress_bar = st.progress(0)
                status_text = st.empty()

                def update_rollback_progress(index, total, label):
                    status_text.text(
                        f"Rolling back {index + 1}/{total}: {label}"
                    )
                    progress_bar.progress((index + 1) / total)

                success, rollback_results, message = rollback_all_migrations(
                    st.session_state["target_config"],
                    migration_files,
                    batch_id,
                    source_config=_source_tracking_config(),
                    progress_callback=update_rollback_progress,
                )
                for result in rollback_results:
                    st.session_state.migration_status[result["filename"]] = {
                        "success": None,
                        "message": result["message"],
                        "rows_affected": result["rows"],
                        "timestamp": datetime.now(),
                        "rollback": True,
                    }
                if success:
                    st.success(message)
                else:
                    st.error(message)
                st.rerun()

    with col3:
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
