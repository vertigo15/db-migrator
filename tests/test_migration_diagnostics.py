import json

from utils.migration_diagnostics import (
    build_history_issues,
    build_support_report,
    classify_error,
    exception_context,
    is_non_retryable_failure,
)


def test_user_verification_explains_cross_run_reuse():
    result = classify_error(
        "Expected 1 users, but found 0 users tracked for this migration run.",
        phase="step",
        step_key="01_users",
        mapping_evidence=[
            {
                "batch_id": "new-run",
                "mapping_owner_run": "old-run",
                "mapped_target_exists": True,
            }
        ],
    )

    assert result["code"] == "USER_ALREADY_MAPPED_OLDER_RUN"
    assert result["facts"] == {
        "expected": 1,
        "actual": 0,
        "mapping_owner_runs": ["old-run"],
    }
    assert "Do not delete mappings by hand" in result["recommendation"]


def test_folder_parent_error_has_selection_guidance():
    result = classify_error(
        "folder '202' references parent 'abc' that was not migrated first",
        phase="shard",
        step_key="02_folders",
    )

    assert result["code"] == "FOLDER_PARENT_NOT_MIGRATED"
    assert "owned-data-only batch" in result["recommendation"]


def test_canonical_folder_owner_mismatch_identifies_old_reassign():
    result = classify_error(
        "Canonical folder owner mismatch for legacy folder 202: mapped folder "
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa is not owned by user "
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        phase="shard",
        step_key="02_folders",
    )

    assert result["code"] == "CANONICAL_FOLDER_OWNER_MISMATCH"
    assert "older migration" in result["title"].lower()
    assert "Do not resume" in result["recommendation"]


def test_canonical_document_owner_mismatch_identifies_old_reassign():
    result = classify_error(
        "Canonical document owner mismatch for legacy document doc-1: mapped "
        "document aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa is not owned by user "
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        phase="shard",
        step_key="03_documents",
    )

    assert result["code"] == "CANONICAL_DOCUMENT_OWNER_MISMATCH"
    assert "older migration" in result["title"].lower()


def test_conversation_adoption_mismatch_has_authoritative_option():
    result = classify_error(
        "Conversation exact adoption content mismatch for legacy conversation "
        "11111111-1111-4111-8111-111111111111 (messages 2/4, blocks 2/4)",
        phase="shard",
        step_key="05_conversations",
    )

    assert result["code"] == "CONVERSATION_ADOPTION_MISMATCH"
    assert "V4-authoritative" in result["recommendation"]


def test_deterministic_failures_are_terminal_but_transient_errors_retry():
    assert is_non_retryable_failure(
        "Conversation UUID collision for legacy conversation abc"
    )
    assert is_non_retryable_failure("duplicate", {"sqlstate": "23505"})
    assert not is_non_retryable_failure(
        "deadlock detected", {"sqlstate": "40P01"}
    )
    assert not is_non_retryable_failure(
        "server closed the connection", {"sqlstate": "08006"}
    )
    assert is_non_retryable_failure(
        "Canonical agent owner mismatch for legacy bot bot-1"
    )


def test_agent_collision_has_preflight_guidance():
    result = classify_error(
        "Agent exact adoption helper mismatch for legacy bot bot-1",
        phase="shard",
        step_key="06_agents",
    )

    assert result["code"] == "AGENT_COLLISION"
    assert "preflight" in result["recommendation"].lower()


def test_history_issue_combines_step_shards_users_and_mapping_evidence():
    issues = build_history_issues(
        "new-run",
        step_rows=[
            {
                "step_key": "01_users",
                "target_database": "user_db",
                "status": "failed",
                "expected_count": 1,
                "affected_count": 0,
                "verification": "mismatch",
                "verification_details": {"expected_mappings": 1},
                "error_message": "Expected 1 users, but found 0 users tracked.",
            }
        ],
        shard_rows=[
            {
                "step_key": "01_users",
                "error_message": "Expected 1 users, but found 0 users tracked.",
                "owner_emails": ["user@example.com"],
                "attempts": 1,
            }
        ],
        user_rows=[
            {
                "email": "user@example.com",
                "result": "failed",
                "failed_step": "01_users",
                "mapping_owner_run": "old-run",
                "mapped_target_exists": True,
            }
        ],
    )

    assert len(issues) == 1
    assert issues[0].code == "USER_ALREADY_MAPPED_OLDER_RUN"
    assert issues[0].affected_users == ("user@example.com",)
    assert issues[0].technical_details["expected_count"] == 1
    assert issues[0].technical_details["mapping_evidence"][0][
        "mapping_owner_run"
    ] == "old-run"


def test_support_report_is_complete_json():
    issues = build_history_issues(
        "run-1",
        step_rows=[],
        shard_rows=[],
        user_rows=[
            {
                "email": "failed@example.com",
                "result": "failed",
                "error_message": "permission denied for table users",
            }
        ],
    )

    report = json.loads(build_support_report("run-1", issues))
    assert report["schema_version"] == 1
    assert report["migration_run_id"] == "run-1"
    assert report["issues"][0]["code"] == "DATABASE_PERMISSION_DENIED"


def test_exception_context_extracts_postgres_diagnostics():
    class _Diag:
        message_detail = "Key already exists"
        message_hint = "Use another value"
        constraint_name = "users_email_key"
        table_name = "users"
        column_name = None
        statement_position = "42"

    class _PgError(Exception):
        pgcode = "23505"
        diag = _Diag()

    context = exception_context(_PgError("duplicate"))

    assert context["exception_type"] == "_PgError"
    assert context["sqlstate"] == "23505"
    assert context["constraint"] == "users_email_key"
    assert context["detail"] == "Key already exists"
