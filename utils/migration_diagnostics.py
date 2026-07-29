"""Turn migration tracking facts into operator-friendly explanations."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import re
from typing import Iterable, Mapping, Optional


@dataclass(frozen=True)
class DiagnosticIssue:
    key: str
    code: str
    title: str
    summary: str
    cause: str
    recommendation: str
    step_key: Optional[str] = None
    affected_users: tuple[str, ...] = ()
    technical_details: Mapping[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


def exception_context(exc: Exception) -> dict:
    """Extract safe PostgreSQL diagnostics without depending on psycopg2."""
    context = {"exception_type": type(exc).__name__}
    pgcode = getattr(exc, "pgcode", None)
    if pgcode:
        context["sqlstate"] = pgcode
    diag = getattr(exc, "diag", None)
    if diag is not None:
        for source, target in (
            ("message_detail", "detail"),
            ("message_hint", "hint"),
            ("constraint_name", "constraint"),
            ("table_name", "table"),
            ("column_name", "column"),
            ("statement_position", "statement_position"),
        ):
            value = getattr(diag, source, None)
            if value:
                context[target] = value
    return context


def _count_facts(message: str) -> dict:
    facts = {}
    patterns = (
        r"expected\s+(?P<expected>\d+).*?(?:found|actual(?:ly)?|got)\s+(?P<actual>\d+)",
        r"(?P<actual>\d+)\s*/\s*(?P<expected>\d+)",
    )
    for pattern in patterns:
        match = re.search(pattern, message, flags=re.IGNORECASE)
        if match:
            facts["expected"] = int(match.group("expected"))
            facts["actual"] = int(match.group("actual"))
            break
    return facts


def classify_error(
    message: str,
    *,
    phase: str = "migration",
    step_key: Optional[str] = None,
    mapping_evidence: Iterable[Mapping[str, object]] = (),
) -> dict:
    """Classify a stored error and return plain-language guidance."""
    text = str(message or "").strip()
    lower = text.lower()
    count_facts = _count_facts(text)
    mappings = [
        dict(row)
        for row in mapping_evidence
        if row.get("mapping_owner_run")
    ]
    older_mappings = [
        row
        for row in mappings
        if str(row.get("mapping_owner_run")) != str(row.get("batch_id"))
        and bool(row.get("mapped_target_exists"))
    ]

    if (
        step_key == "01_users"
        and count_facts
        and older_mappings
    ):
        owners = sorted(
            {str(row["mapping_owner_run"]) for row in older_mappings}
        )
        return {
            "code": "USER_ALREADY_MAPPED_OLDER_RUN",
            "title": "Users were already mapped by an older migration",
            "cause": (
                "The selected user records already have live V5 mappings owned "
                "by an earlier migration. This run verified only its own tracked "
                "users, so the reused users were not counted."
            ),
            "recommendation": (
                "Use an image containing cross-run reuse tracking, or select users "
                "that have no existing user mapping. Do not delete mappings by hand."
            ),
            "facts": {**count_facts, "mapping_owner_runs": owners},
        }

    if "references parent" in lower and "not migrated first" in lower:
        return {
            "code": "FOLDER_PARENT_NOT_MIGRATED",
            "title": "A required parent folder was not migrated",
            "cause": (
                "A selected folder belongs below a parent folder that is outside "
                "the current migration scope. Creating the child alone would leave "
                "an invalid hierarchy."
            ),
            "recommendation": (
                "Include the parent folder's owner in the batch, or enable the "
                "cross-owner dependency reassignment option and create a fresh batch."
            ),
            "facts": count_facts,
        }

    if "canonical" in lower and "mapping mismatch" in lower:
        return {
            "code": "CANONICAL_MAPPING_MISMATCH",
            "title": "Existing mapping conflicts with the selected record",
            "cause": (
                "The legacy record points to a different V5 entity than the "
                "canonical migration mapping. Continuing could attach data to the "
                "wrong entity."
            ),
            "recommendation": (
                "Inspect the referenced older migration and roll it back cleanly "
                "or reset the test target. Do not overwrite the mapping manually."
            ),
            "facts": count_facts,
        }

    if "rollback blocked" in lower or "rollback order violation" in lower:
        return {
            "code": "ROLLBACK_BLOCKED",
            "title": "Rollback was blocked to protect dependent data",
            "cause": (
                "Rows still depend on entities this rollback would remove, or a "
                "later migration step must be rolled back first."
            ),
            "recommendation": (
                "Open the technical details to see the blocking rows. Roll back "
                "later steps first; use Force only for explicitly forceable app rows."
            ),
            "facts": count_facts,
        }

    if "permission denied" in lower:
        return {
            "code": "DATABASE_PERMISSION_DENIED",
            "title": "Database permissions are insufficient",
            "cause": "The migration database user cannot perform the required operation.",
            "recommendation": (
                "Grant the shown table/schema permission to the migration user, "
                "then resume the failed shards."
            ),
            "facts": count_facts,
        }

    if (
        "does not exist" in lower
        or "undefinedtable" in lower
        or "undefinedcolumn" in lower
    ):
        return {
            "code": "DATABASE_SCHEMA_MISMATCH",
            "title": "Database schema does not match the migration image",
            "cause": (
                "A table or column expected by the migrator is missing from the "
                "connected database."
            ),
            "recommendation": (
                "Verify the V4 table prefix and the deployed V4/V5 schema versions. "
                "Deploy the matching migration image before retrying."
            ),
            "facts": count_facts,
        }

    if "duplicate key" in lower or "unique constraint" in lower:
        return {
            "code": "UNIQUE_CONSTRAINT_VIOLATION",
            "title": "A target record conflicts with existing data",
            "cause": (
                "The migration attempted to create a value that must be unique but "
                "already exists in V5."
            ),
            "recommendation": (
                "Inspect the constraint and existing entity in technical details. "
                "Resolve the duplicate or reuse the existing entity before retrying."
            ),
            "facts": count_facts,
        }

    if "foreign key" in lower:
        return {
            "code": "FOREIGN_KEY_VIOLATION",
            "title": "A required related record is missing",
            "cause": (
                "The target database rejected a row because its parent or referenced "
                "entity does not exist."
            ),
            "recommendation": (
                "Check which constraint failed and ensure the earlier migration step "
                "completed successfully before resuming."
            ),
            "facts": count_facts,
        }

    if "checksum" in lower:
        return {
            "code": "SHARD_CHECKSUM_MISMATCH",
            "title": "Generated migration file changed after extraction",
            "cause": (
                "The shard file no longer matches the checksum recorded when it was "
                "generated, so the worker refused to execute it."
            ),
            "recommendation": "Regenerate the migration batch instead of editing the SQL file.",
            "facts": count_facts,
        }

    if any(word in lower for word in ("timeout", "connection refused", "connection reset")):
        return {
            "code": "DATABASE_CONNECTION_FAILED",
            "title": "Database connection was interrupted",
            "cause": "The worker could not maintain a connection to the target database.",
            "recommendation": (
                "Verify database availability and network access, then resume the "
                "failed shards. Repeated timeouts may require lower worker concurrency."
            ),
            "facts": count_facts,
        }

    if "failed to generate" in lower or "sql generation" in lower:
        return {
            "code": "SQL_GENERATION_FAILED",
            "title": "Migration SQL could not be generated",
            "cause": (
                "Source data failed a safety or consistency check while the "
                "migration files were being prepared."
            ),
            "recommendation": (
                "Read the named entity and dependency in technical details, correct "
                "the selection or source inconsistency, then create a fresh batch."
            ),
            "facts": count_facts,
        }

    if count_facts or "verification failed" in lower or "mismatch" in lower:
        return {
            "code": "STEP_VERIFICATION_MISMATCH",
            "title": "Migrated data did not match the expected count",
            "cause": (
                "The SQL finished, but post-migration verification found fewer, more, "
                "or invalid records than extraction expected."
            ),
            "recommendation": (
                "Review expected versus actual counts and affected users below. "
                "Do not mark the batch successful or rerun blindly."
            ),
            "facts": count_facts,
        }

    if phase == "extraction":
        return {
            "code": "EXTRACTION_FAILED",
            "title": "Source extraction failed",
            "cause": (
                "The migrator could not finish reading or transforming the selected "
                "V4 data, so no complete executable batch was produced."
            ),
            "recommendation": (
                "Review the original extraction error below, correct the source "
                "selection or configuration, and create a fresh batch."
            ),
            "facts": count_facts,
        }

    if "shard" in lower and ("failed" in lower or "retry" in lower):
        return {
            "code": "SHARD_EXECUTION_FAILED",
            "title": "A background worker shard failed",
            "cause": (
                "One part of this migration could not complete within its retry limit."
            ),
            "recommendation": (
                "Read the underlying shard error in technical details. Resume only "
                "after correcting a data/schema issue; transient connection errors "
                "can be retried directly."
            ),
            "facts": count_facts,
        }

    return {
        "code": f"{phase.upper()}_FAILED",
        "title": "Migration step failed",
        "cause": (
            "The migrator recorded an error but could not identify a more specific "
            "known failure category."
        ),
        "recommendation": (
            "Use the technical details and downloadable report. It contains the run, "
            "step, shard, affected users, counts, and original database error."
        ),
        "facts": count_facts,
    }


def build_history_issues(
    migration_run_id: str,
    *,
    step_rows: Iterable[Mapping[str, object]],
    shard_rows: Iterable[Mapping[str, object]],
    user_rows: Iterable[Mapping[str, object]],
    event_rows: Iterable[Mapping[str, object]] = (),
    run_status: Optional[str] = None,
) -> list[DiagnosticIssue]:
    """Build one actionable issue per failed step plus run-level failures."""
    steps = [dict(row) for row in step_rows]
    shards = [dict(row) for row in shard_rows]
    users = [dict(row) for row in user_rows]
    events = [dict(row) for row in event_rows]
    issues = []

    failed_steps = [
        row
        for row in steps
        if str(row.get("status", "")).lower() in {"failed", "partial"}
        or str(row.get("verification", "")).lower() == "mismatch"
        or bool(row.get("error_message"))
    ]
    shard_step_keys = {
        str(row.get("step_key"))
        for row in shards
        if row.get("step_key")
    }
    for missing_step in sorted(shard_step_keys - {str(r.get("step_key")) for r in failed_steps}):
        failed_steps.append({"step_key": missing_step, "status": "failed"})

    for step in failed_steps:
        step_key = str(step.get("step_key") or "unknown")
        step_shards = [row for row in shards if row.get("step_key") == step_key]
        step_events = [row for row in events if row.get("step_key") == step_key]
        failed_users = [
            row
            for row in users
            if row.get("failed_step") == step_key
            or (
                str(row.get("result", "")).lower() == "failed"
                and not row.get("failed_step")
            )
        ]
        mapping_evidence = [
            {**row, "batch_id": migration_run_id}
            for row in users
            if row.get("mapping_owner_run")
        ]
        candidate_messages = [
            str(row.get("error_message"))
            for row in step_shards
            if row.get("error_message")
        ]
        candidate_messages.extend(
            str(row.get("message"))
            for row in reversed(step_events)
            if row.get("message")
        )
        if step.get("error_message"):
            candidate_messages.append(str(step["error_message"]))
        candidate_messages.extend(
            str(row.get("error_message"))
            for row in failed_users
            if row.get("error_message")
        )
        message = next(
            (
                item
                for item in candidate_messages
                if "shard(s) failed for step" not in item.lower()
            ),
            candidate_messages[0] if candidate_messages else "Step status is failed.",
        )
        classification = classify_error(
            message,
            phase="step",
            step_key=step_key,
            mapping_evidence=mapping_evidence,
        )
        affected = {
            str(row["email"])
            for row in failed_users
            if row.get("email")
        }
        for shard in step_shards:
            affected.update(str(email) for email in shard.get("owner_emails") or [])
        details = {
            "migration_run_id": migration_run_id,
            "step_key": step_key,
            "target_database": step.get("target_database"),
            "status": step.get("status"),
            "expected_count": step.get("expected_count"),
            "affected_count": step.get("affected_count"),
            "verification": step.get("verification"),
            "verification_details": step.get("verification_details") or {},
            "original_error": message,
            "failed_shards": step_shards,
            "diagnostic_events": step_events,
            "mapping_evidence": mapping_evidence,
            **classification.get("facts", {}),
        }
        issues.append(
            DiagnosticIssue(
                key=f"step-{step_key}",
                code=classification["code"],
                title=classification["title"],
                summary=message,
                cause=classification["cause"],
                recommendation=classification["recommendation"],
                step_key=step_key,
                affected_users=tuple(sorted(affected)),
                technical_details=details,
            )
        )

    for index, event in enumerate(events):
        if event.get("step_key") or str(event.get("severity", "")).lower() != "error":
            continue
        if (
            str(run_status or "").lower() == "rolled_back"
            and event.get("phase") == "rollback"
        ):
            continue
        message = str(event.get("message") or "Migration run failed.")
        classification = classify_error(
            message,
            phase=str(event.get("phase") or "run"),
        )
        issues.append(
            DiagnosticIssue(
                key=f"run-{event.get('id', index)}",
                code=str(event.get("code") or classification["code"]),
                title=classification["title"],
                summary=message,
                cause=classification["cause"],
                recommendation=classification["recommendation"],
                affected_users=tuple(
                    sorted(str(email) for email in event.get("owner_emails") or [])
                ),
                technical_details={
                    "migration_run_id": migration_run_id,
                    "diagnostic_event": event,
                    **classification.get("facts", {}),
                },
            )
        )

    if not issues:
        orphan_failures = [
            row for row in users if str(row.get("result", "")).lower() == "failed"
        ]
        if orphan_failures:
            message = str(
                orphan_failures[0].get("error_message")
                or "One or more users are marked failed without a failed step."
            )
            classification = classify_error(message, phase="user")
            issues.append(
                DiagnosticIssue(
                    key="failed-users",
                    code=classification["code"],
                    title=classification["title"],
                    summary=message,
                    cause=classification["cause"],
                    recommendation=classification["recommendation"],
                    affected_users=tuple(
                        sorted(
                            str(row["email"])
                            for row in orphan_failures
                            if row.get("email")
                        )
                    ),
                    technical_details={
                        "migration_run_id": migration_run_id,
                        "failed_users": orphan_failures,
                    },
                )
            )
    return issues


def build_support_report(
    migration_run_id: str,
    issues: Iterable[DiagnosticIssue],
    *,
    events: Iterable[Mapping[str, object]] = (),
) -> str:
    """Return a complete, copyable JSON report with no database credentials."""
    payload = {
        "schema_version": 1,
        "migration_run_id": migration_run_id,
        "issues": [issue.to_dict() for issue in issues],
        "diagnostic_events": [dict(event) for event in events],
    }
    return json.dumps(payload, indent=2, default=str, ensure_ascii=False)
