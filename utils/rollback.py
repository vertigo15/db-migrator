"""
Rollback engine for tracked migrations.

Holds the constants, dependency graph, and rollback routines shared by the
Run Migrations and Migration History pages. Every delete is strictly scoped to
rows created by a specific migration run and executed child-before-parent in
reverse dependency order.
"""
from utils.db import ConnectionConfig, get_connection
from utils.sql_generator import NAMESPACE_UUID
from utils.migration_tracking import (
    config_for_database,
    reconcile_rollback_status,
    record_user_rollback_result,
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

# step_key (prefix without trailing underscore) -> human label
STEP_LABELS = {prefix.rstrip("_"): label for prefix, _, label in ALL_STEPS}

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
                SELECT COUNT(*) FROM documents
                WHERE folder_id = ANY(%s::uuid[])
                """,
                (mapped_ids,),
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
            SELECT DISTINCT knowledge_base_id
            FROM knowledge_base_assignments
            WHERE assigned_to_type = 'agent'
              AND assigned_to_id = ANY(%s::uuid[])
            UNION
            SELECT kb.new_id
            FROM migration.id_mappings kb
            JOIN migration.id_mappings agent
              ON agent.table_name = 'agents'
             AND agent.old_id = kb.old_id
             AND agent.migration_run_id = kb.migration_run_id
            WHERE kb.table_name = 'knowledge_bases'
              AND kb.migration_run_id = %s::uuid
              AND kb.record_action = 'created'
              AND agent.new_id = ANY(%s::uuid[])
            """,
            (mapped_ids, migration_run_id, mapped_ids),
        )
        kb_ids = [str(row[0]) for row in cursor.fetchall()]
        if kb_ids:
            cursor.execute(
                """
                SELECT id FROM knowledge_base_items
                WHERE knowledge_base_id = ANY(%s::uuid[])
                UNION
                SELECT id FROM knowledge_base_assignments
                WHERE knowledge_base_id = ANY(%s::uuid[])
                """,
                (kb_ids, kb_ids),
            )
            helper_ids = [str(row[0]) for row in cursor.fetchall()] + kb_ids
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
            cursor.execute(
                """
                DELETE FROM migration.id_mappings
                WHERE migration_run_id = %s::uuid
                  AND table_name IN (
                      'knowledge_base_items',
                      'knowledge_base_assignments',
                      'knowledge_bases'
                  )
                  AND new_id = ANY(%s::uuid[])
                """,
                (migration_run_id, helper_ids),
            )
        delete("DELETE FROM agent_settings WHERE agent_id = ANY(%s::uuid[])", (mapped_ids,))
        delete(
            "DELETE FROM legacy_bot_to_agent_mapping WHERE new_agent_id = ANY(%s::uuid[])",
            (mapped_ids,),
        )
        delete("DELETE FROM agents WHERE id = ANY(%s::uuid[])", (mapped_ids,))
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


def _user_scoped_mapping_rows(
    cursor,
    mapping_table: str,
    migration_run_id: str,
    user_scope: dict,
) -> tuple[list, list]:
    """Return all and run-created mapping IDs owned by one migrated user."""
    target_user_id = str(user_scope["v5_user_id"])
    legacy_user_id = user_scope.get("legacy_user_id")

    if mapping_table == "users":
        cursor.execute(
            """
            SELECT new_id, record_action
            FROM migration.id_mappings
            WHERE table_name = 'users'
              AND migration_run_id = %s::uuid
              AND new_id = %s::uuid
              AND (%s::text IS NULL OR old_id = %s)
            """,
            (
                migration_run_id,
                target_user_id,
                legacy_user_id,
                legacy_user_id,
            ),
        )
    else:
        owner_tables = {
            "folders": "folders",
            "documents": "documents",
            "conversations": "conversations",
            "agents": "agents",
            "conversions": "conversions",
        }
        target_table = owner_tables.get(mapping_table)
        if target_table is None:
            raise RuntimeError(
                f"Per-user rollback does not support mapping table {mapping_table}"
            )
        cursor.execute(
            f"""
            SELECT m.new_id, m.record_action
            FROM migration.id_mappings m
            JOIN public.{target_table} owned ON owned.id = m.new_id
            WHERE m.table_name = %s
              AND m.migration_run_id = %s::uuid
              AND owned.user_id = %s::uuid
            """,
            (mapping_table, migration_run_id, target_user_id),
        )

    rows = [(str(new_id), action) for new_id, action in cursor.fetchall()]
    all_ids = [new_id for new_id, _ in rows]
    created_ids = [
        new_id for new_id, action in rows if action == "created"
    ]
    return all_ids, created_ids


def rollback_migration(
    config: ConnectionConfig,
    filename: str,
    target_db: str,
    migration_run_id: str,
    user_scope: dict = None,
    force: bool = False,
) -> tuple:
    """
    Rollback a migration by deleting migrated data and clearing mapping table.

    When ``force`` is True the same-database "unexpected dependent rows" guard
    is skipped. This is meant for dev / re-migration scenarios where the run
    recorded pre-existing entities as ``created`` and application rows (e.g.
    ``agent_drafts``) now reference them. Deletion still relies on the database's
    own foreign keys: ``ON DELETE CASCADE`` children are removed automatically,
    while a ``RESTRICT`` child aborts the whole transaction so nothing is
    partially deleted. The cross-database reverse-order guard always applies.

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
    order_blockers = (
        []
        if user_scope
        else _rollback_order_blockers(config, step_key, migration_run_id)
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
            if step_status[0] == "skipped":
                conn.rollback()
                cursor.close()
                conn.close()
                return (True, "ℹ️ Step was skipped for this run.", 0)
            if step_status[0] == "pending" and not user_scope:
                raise RuntimeError(
                    f"Step {step_key} has status {step_status[0]} and cannot be rolled back"
                )

            # Snapshot only records created by this run. Reused entities are
            # intentionally excluded from all target-table DELETE statements.
            if user_scope:
                scope_mapping_ids, mapped_ids = _user_scoped_mapping_rows(
                    cursor,
                    table_info["mapping_table"],
                    migration_run_id,
                    user_scope,
                )
                all_mapping_count = len(scope_mapping_ids)
            else:
                cursor.execute("""
                    SELECT new_id, record_action
                    FROM migration.id_mappings
                    WHERE table_name = %s
                      AND migration_run_id = %s::uuid
                """, (table_info["mapping_table"], migration_run_id))
                mapping_rows = [
                    (str(new_id), action)
                    for new_id, action in cursor.fetchall()
                ]
                scope_mapping_ids = [
                    new_id for new_id, _ in mapping_rows
                ]
                mapped_ids = [
                    new_id
                    for new_id, action in mapping_rows
                    if action == "created"
                ]
                all_mapping_count = len(scope_mapping_ids)

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
                    "❌ Rollback blocked: rows in other databases still reference "
                    "this step's entities: "
                    + ", ".join(blockers)
                    + ". If these belong to a later migration step, roll that "
                    "step back first. If they were created in the app (they are "
                    "not part of this run), they must be removed there first — "
                    "Force does not bypass this cross-database guard because "
                    "PostgreSQL cannot protect references that span databases.",
                    0,
                )

            local_blockers = (
                []
                if force
                else _local_rollback_blockers(
                    cursor,
                    step_key,
                    mapped_ids,
                    migration_run_id,
                )
            )
            if local_blockers:
                raise RuntimeError(
                    "Rollback blocked to protect application data not created "
                    "by this migration. The following rows reference entities "
                    "this run recorded and would be cascade-deleted: "
                    + ", ".join(local_blockers)
                    + ". These are typically created in the app after migration "
                    "(or belong to pre-existing entities the run mislabeled as "
                    "'created'). Re-run with the Force option to delete them "
                    "anyway (database foreign keys remain the final safeguard)."
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
                cursor.execute(
                    """
                    DELETE FROM migration.id_mappings
                    WHERE table_name = %s
                      AND migration_run_id = %s::uuid
                      AND new_id = ANY(%s::uuid[])
                    """,
                    (
                        table_info["mapping_table"],
                        migration_run_id,
                        scope_mapping_ids,
                    ),
                )

            if not user_scope:
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
                    f"✅ User-scoped rollback successful for "
                    f"{user_scope['email']}! Deleted {total_deleted} records."
                    if user_scope
                    else (
                        (
                            "✅ Batch-scoped rollback successful! Deleted "
                            f"{total_deleted} records created by run "
                            f"{migration_run_id}."
                        )
                        if all_mapping_count
                        else (
                            "ℹ️ No run-owned mappings existed; the step was "
                            "marked rolled back."
                        )
                    )
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
    force: bool = False,
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
            force=force,
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


def rollback_tracked_batch(
    base_config: ConnectionConfig,
    migration_run_id: str,
    source_config: ConnectionConfig = None,
    progress_callback=None,
    force: bool = False,
) -> tuple:
    """Rollback a historical run using tracking rows, without SQL files."""
    tracked_steps = [
        {
            "filename": f"{prefix}tracked_{migration_run_id}.sql",
            "database": database,
        }
        for prefix, database, _ in ALL_STEPS
    ]
    return rollback_all_migrations(
        base_config,
        tracked_steps,
        migration_run_id,
        source_config=source_config,
        progress_callback=progress_callback,
        force=force,
    )


def rollback_tracked_step(
    base_config: ConnectionConfig,
    migration_run_id: str,
    step_key: str,
    source_config: ConnectionConfig = None,
    force: bool = False,
) -> tuple:
    """Rollback a single tracked step for the whole batch.

    Reverse dependency order is still enforced: rolling back an earlier step
    while a later dependent step is still produced returns an order-violation
    error instead of deleting rows.

    Returns:
        (success: bool, message: str, rows_deleted: int)
    """
    if not migration_run_id:
        return (False, "❌ Select a tracked migration run before rollback", 0)
    prefix = f"{step_key}_"
    database = DB_MAPPING.get(prefix)
    if database is None:
        return (False, f"❌ Unknown rollback step: {step_key}", 0)

    config = config_for_database(base_config, database)
    success, message, rows = rollback_migration(
        config,
        f"{prefix}tracked_{migration_run_id}.sql",
        database,
        migration_run_id,
        force=force,
    )
    # Keep the canonical batch status in sync after a single-step rollback.
    reconcile_rollback_status(
        base_config,
        migration_run_id,
        source_config=source_config,
    )
    return success, message, rows


def _load_tracked_user(
    base_config: ConnectionConfig,
    migration_run_id: str,
    email: str,
) -> dict:
    config = config_for_database(base_config, "user_db")
    conn = get_connection(config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT email, legacy_user_id, v5_user_id, user_action, result
                FROM migration.migration_user_results
                WHERE batch_id = %s::uuid AND email = %s
                """,
                (migration_run_id, email),
            )
            row = cursor.fetchone()
    finally:
        conn.close()
    if row is None:
        raise RuntimeError(
            f"User {email} is not tracked in migration run {migration_run_id}"
        )
    if row[2] is None:
        raise RuntimeError(
            f"User {email} has no resolved V5 UUID; per-user rollback is unsafe"
        )
    return {
        "email": row[0],
        "legacy_user_id": row[1],
        "v5_user_id": str(row[2]),
        "user_action": row[3],
        "result": row[4],
    }


def rollback_tracked_user(
    base_config: ConnectionConfig,
    migration_run_id: str,
    email: str,
    source_config: ConnectionConfig = None,
    progress_callback=None,
    force: bool = False,
) -> tuple:
    """Rollback only one user's run-created entities in reverse step order."""
    try:
        user_scope = _load_tracked_user(
            base_config, migration_run_id, email
        )
    except Exception as exc:
        return False, [], str(exc)
    if user_scope["result"] == "rolled_back":
        return True, [], f"{user_scope['email']} is already rolled back"

    results = []
    total_steps = len(ROLLBACK_STEP_ORDER)
    for index, (prefix, database, label) in enumerate(ROLLBACK_STEP_ORDER):
        config = config_for_database(base_config, database)
        if progress_callback:
            progress_callback(index, total_steps, label)
        success, message, rows = rollback_migration(
            config,
            f"{prefix}tracked_{migration_run_id}.sql",
            database,
            migration_run_id,
            user_scope=user_scope,
            force=force,
        )
        results.append(
            {
                "filename": prefix.rstrip("_"),
                "database": database,
                "success": success,
                "message": message,
                "rows": rows,
            }
        )
        if not success:
            return False, results, message

    try:
        remaining = record_user_rollback_result(
            base_config,
            migration_run_id,
            user_scope["email"],
            source_config=source_config,
        )
    except Exception as exc:
        return (
            False,
            results,
            "Data was rolled back, but tracking reconciliation failed: "
            + str(exc),
        )

    if remaining == 0:
        success, batch_results, message = rollback_tracked_batch(
            base_config,
            migration_run_id,
            source_config=source_config,
            force=force,
        )
        results.extend(batch_results)
        if not success:
            return False, results, message

    return (
        True,
        results,
        (
            f"Rolled back {user_scope['email']}. "
            f"{remaining} user(s) remain active in this batch."
        ),
    )


def _load_rollback_history(base_config: ConnectionConfig) -> tuple[list, dict]:
    """Load selectable historical runs and their per-user results."""
    config = config_for_database(base_config, "user_db")
    conn = get_connection(config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id::text, started_at, status, total_users
                FROM migration.migration_batches
                ORDER BY started_at DESC
                LIMIT 100
                """
            )
            runs = [
                {
                    "id": row[0],
                    "started_at": row[1],
                    "status": row[2],
                    "total_users": row[3],
                }
                for row in cursor.fetchall()
            ]
            users_by_run = {}
            for run in runs:
                cursor.execute(
                    """
                    SELECT email, result, user_action
                    FROM migration.migration_user_results
                    WHERE batch_id = %s::uuid
                    ORDER BY email
                    """,
                    (run["id"],),
                )
                users_by_run[run["id"]] = [
                    {
                        "email": row[0],
                        "result": row[1],
                        "user_action": row[2],
                    }
                    for row in cursor.fetchall()
                ]
    finally:
        conn.close()
    return runs, users_by_run


def load_batch_step_statuses(
    base_config: ConnectionConfig,
    migration_run_id: str,
) -> dict:
    """Return {step_key: status} for one run across all target databases."""
    statuses = {}
    for prefix, database, _ in ALL_STEPS:
        step_key = prefix.rstrip("_")
        config = config_for_database(base_config, database)
        conn = None
        try:
            conn = get_connection(config)
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT status
                    FROM migration.migration_steps
                    WHERE migration_run_id = %s::uuid AND step_key = %s
                    """,
                    (migration_run_id, step_key),
                )
                row = cursor.fetchone()
                statuses[step_key] = row[0] if row else "tracking_missing"
        except Exception as exc:
            statuses[step_key] = f"unavailable({exc})"
        finally:
            if conn is not None and not conn.closed:
                conn.close()
    return statuses
