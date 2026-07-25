"""
Shared post-execution step verification.

Both the synchronous "Run Migrations" UI path and the background shard
worker must run the exact same truthful verification before a step is
allowed to be marked ``completed``. This module is the single
implementation so the two callers can never drift apart.
"""
from __future__ import annotations

import json
from typing import Tuple

from utils.migration_steps import normalize_step_key, STEP_TARGET_DB
from utils.rollback import TABLE_MAPPING


def verify_step(cursor, step_key: str, migration_run_id: str) -> Tuple[int, dict]:
    """Return a truthful affected count or raise before the step commits.

    ``cursor`` must belong to a connection to the step's target database and
    have already executed (but not committed) the step's SQL. Raises
    ``RuntimeError`` when the destination data does not match what the
    extraction recorded as expected.
    """
    step_key = normalize_step_key(step_key)
    if step_key not in STEP_TARGET_DB:
        raise RuntimeError(f"Unknown migration step: {step_key}")

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
    elif step_key == "05_conversations":
        cursor.execute(
            """
            WITH verified AS (
                SELECT e.old_id, e.new_id, e.record_action
                FROM migration.migration_step_entities e
                WHERE e.migration_run_id = %s::uuid
                  AND e.step_key = '05_conversations'
                  AND e.table_name = 'conversations'

                UNION ALL

                SELECT m.old_id, m.new_id, m.record_action
                FROM migration.id_mappings m
                WHERE m.table_name = 'conversations'
                  AND m.migration_run_id = %s::uuid
                  AND NOT EXISTS (
                      SELECT 1
                      FROM migration.migration_step_entities e
                      WHERE e.migration_run_id = %s::uuid
                        AND e.step_key = '05_conversations'
                        AND e.table_name = 'conversations'
                        AND e.old_id = m.old_id
                  )
            )
            SELECT
                COUNT(*),
                COUNT(*) FILTER (WHERE v.record_action = 'created'),
                COUNT(*) FILTER (WHERE v.record_action = 'reused'),
                COUNT(*) FILTER (
                    WHERE c.id IS NULL OR m.new_id IS NULL
                )
            FROM verified v
            LEFT JOIN public.conversations c ON c.id = v.new_id
            LEFT JOIN migration.id_mappings m
              ON m.table_name = 'conversations'
             AND m.old_id = v.old_id
             AND m.new_id = v.new_id
            """,
            (migration_run_id, migration_run_id, migration_run_id),
        )
        (
            affected_count,
            created_count,
            reused_count,
            invalid_count,
        ) = (int(value) for value in cursor.fetchone())
        if affected_count != expected_count or invalid_count:
            raise RuntimeError(
                "Step verification failed for 05_conversations: "
                f"{affected_count}/{expected_count} verified entities, "
                f"{invalid_count} missing conversations or canonical mappings"
            )
        actual_details = {
            "actual_mappings": affected_count,
            "created_entities": created_count,
            "reused_entities": reused_count,
            "invalid_entities": invalid_count,
        }
    else:
        mapping_table = _mapping_table_for_step(step_key)
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM migration.migration_step_entities
            WHERE migration_run_id = %s::uuid
              AND step_key = %s
              AND table_name = %s
            """,
            (migration_run_id, step_key, mapping_table),
        )
        step_entity_count = int(cursor.fetchone()[0])
        if not step_entity_count:
            cursor.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER (WHERE record_action = 'created'),
                    COUNT(*) FILTER (WHERE record_action = 'reused')
                FROM migration.id_mappings
                WHERE table_name = %s
                  AND migration_run_id = %s::uuid
                """,
                (mapping_table, migration_run_id),
            )
            affected_count, created_count, reused_count = (
                int(value) for value in cursor.fetchone()
            )
            invalid_count = 0
        else:
            cursor.execute(
                f"""
                WITH verified AS (
                    SELECT e.old_id, e.new_id, e.record_action,
                           TRUE AS requires_target_validation
                    FROM migration.migration_step_entities e
                    WHERE e.migration_run_id = %s::uuid
                      AND e.step_key = %s
                      AND e.table_name = %s

                    UNION ALL

                    SELECT m.old_id, m.new_id, m.record_action,
                           FALSE AS requires_target_validation
                    FROM migration.id_mappings m
                    WHERE m.table_name = %s
                      AND m.migration_run_id = %s::uuid
                      AND NOT EXISTS (
                          SELECT 1
                          FROM migration.migration_step_entities e
                          WHERE e.migration_run_id = %s::uuid
                            AND e.step_key = %s
                            AND e.table_name = %s
                            AND e.old_id = m.old_id
                      )
                )
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER (WHERE v.record_action = 'created'),
                    COUNT(*) FILTER (WHERE v.record_action = 'reused'),
                    COUNT(*) FILTER (
                        WHERE v.requires_target_validation
                          AND (target.id IS NULL OR canonical.new_id IS NULL)
                    )
                FROM verified v
                LEFT JOIN public.{mapping_table} target ON target.id = v.new_id
                LEFT JOIN migration.id_mappings canonical
                  ON canonical.table_name = %s
                 AND canonical.old_id = v.old_id
                 AND canonical.new_id = v.new_id
                """,
                (
                    migration_run_id,
                    step_key,
                    mapping_table,
                    mapping_table,
                    migration_run_id,
                    migration_run_id,
                    step_key,
                    mapping_table,
                    mapping_table,
                ),
            )
            (
                affected_count,
                created_count,
                reused_count,
                invalid_count,
            ) = (int(value) for value in cursor.fetchone())
        if affected_count != expected_count or invalid_count:
            raise RuntimeError(
                f"Step verification failed for {step_key}: "
                f"{affected_count}/{expected_count} verified entities, "
                f"{invalid_count} missing target rows or canonical mappings"
            )
        actual_details = {
            "actual_mappings": affected_count,
            "created_entities": created_count,
            "reused_entities": reused_count,
            "invalid_entities": invalid_count,
        }

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


def _mapping_table_for_step(step_key: str) -> str:
    return TABLE_MAPPING[f"{step_key}_"]["mapping_table"]
