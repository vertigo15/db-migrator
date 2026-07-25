"""
Page 7: Migration History

Displays migration batch history and per-user results with per-step details,
CSV export, and the ability to re-run failed users.
"""
import streamlit as st
import pandas as pd

from utils.db import ConnectionConfig, execute_query
from utils.config import get_env_target_defaults
from utils.migration_tracking import (
    TARGET_DATABASES,
    config_for_database,
    ensure_tracking_schema,
)
from utils.rollback import (
    ALL_STEPS,
    STEP_LABELS,
    rollback_tracked_batch,
    rollback_tracked_step,
    rollback_tracked_user,
)
from utils.queue_ui import (
    cancel_run,
    failed_shard_details,
    has_actionable_failures,
    has_in_flight_shards,
    overall_counts,
    resume_run,
    run_progress_by_step,
    steps_with_shards,
)

SHARD_STATUS_ICONS = {
    "queued": "⏸️",
    "retrying": "🔁",
    "running": "⚙️",
    "completed": "✅",
    "failed": "❌",
    "cancelled": "🚫",
}

st.set_page_config(page_title="Migration History", page_icon="📋", layout="wide")

STEP_KEYS = ["01_users", "02_folders", "03_documents", "04_chunks_embeddings",
             "05_conversations", "06_agents", "07_conversions"]


def _get_target_config() -> ConnectionConfig:
    """Build target DB config from session state or env defaults."""
    defaults = get_env_target_defaults()
    return ConnectionConfig(
        host=st.session_state.get("target_host") or defaults["host"],
        port=int(st.session_state.get("target_port") or defaults["port"]),
        database=st.session_state.get("target_database") or defaults["database"],
        username=st.session_state.get("target_username") or defaults["username"],
        password=st.session_state.get("target_password") or defaults["password"],
    )


@st.cache_resource(show_spinner=False)
def _ensure_tracking_once(host, port, database, username, password):
    """Create tracking DDL once per target configuration, not per rerun."""
    config = ConnectionConfig(host, int(port), database, username, password)
    ensure_tracking_schema(config_for_database(config, "user_db"), coordinator=True)
    return True


def _ensure_tracking_tables(config: ConnectionConfig):
    try:
        return _ensure_tracking_once(
            config.host,
            config.port,
            config.database,
            config.username,
            config.password,
        )
    except Exception:
        return False


def _config_from_cache_args(host, port, database, username, password):
    return ConnectionConfig(host, int(port), database, username, password)


@st.cache_data(ttl=30, show_spinner=False)
def _cached_history_snapshot(
    host, port, database, username, password, refresh_token
) -> pd.DataFrame:
    config = _config_from_cache_args(host, port, database, username, password)
    return execute_query(
        config,
        """
        WITH summary AS (
            SELECT COUNT(DISTINCT batch_id)::bigint AS total_batches,
                   COUNT(*)::bigint AS total_users,
                   COUNT(*) FILTER (WHERE result = 'success')::bigint AS success,
                   COUNT(*) FILTER (WHERE result = 'reused_existing_user')::bigint AS reused,
                   COUNT(*) FILTER (WHERE result = 'failed')::bigint AS failed,
                   COUNT(*) FILTER (WHERE result = 'skipped')::bigint AS skipped,
                   COUNT(*) FILTER (WHERE result = 'pending')::bigint AS pending
            FROM migration.migration_user_results
        )
        SELECT b.id, b.started_at, b.completed_at, b.status, b.total_users,
               b.source_info->>'host' AS source_host,
               b.source_info->>'database' AS source_db, b.notes,
               s.total_batches, s.total_users AS summary_total_users,
               s.success, s.reused, s.failed, s.skipped, s.pending
        FROM migration.migration_batches b
        CROSS JOIN summary s
        ORDER BY b.started_at DESC
        """,
    )


@st.cache_data(ttl=30, show_spinner=False)
def _cached_user_results(
    host, port, database, username, password, batch_id, refresh_token
) -> pd.DataFrame:
    config = _config_from_cache_args(host, port, database, username, password)
    return _load_user_results(config, batch_id or None)


@st.cache_data(ttl=30, show_spinner=False)
def _cached_distributed_steps(
    host, port, database, username, password, migration_run_id, refresh_token
) -> pd.DataFrame:
    config = _config_from_cache_args(host, port, database, username, password)
    return _load_distributed_steps(config, migration_run_id)


def _cache_args(config: ConnectionConfig):
    return (
        config.host,
        config.port,
        config.database,
        config.username,
        config.password,
        st.session_state.get("_history_refresh_token", 0),
    )


def _load_batches(config: ConnectionConfig) -> pd.DataFrame:
    """Load all migration batches."""
    query = """
        SELECT id, started_at, completed_at, status, total_users,
               source_info->>'host' AS source_host,
               source_info->>'database' AS source_db,
               notes
        FROM migration.migration_batches
        ORDER BY started_at DESC
    """
    try:
        return execute_query(config, query)
    except Exception:
        return pd.DataFrame()


def _load_user_results(config: ConnectionConfig, batch_id: str = None) -> pd.DataFrame:
    """Load user results, optionally filtered by batch."""
    if batch_id:
        query = """
            SELECT r.email, r.result, r.failed_step, r.error_message,
                   r.legacy_user_id, r.v5_user_id, r.user_action,
                   r.steps_completed, r.started_at, r.completed_at
            FROM migration.migration_user_results r
            WHERE r.batch_id = %s
            ORDER BY r.email
        """
        try:
            return execute_query(config, query, (batch_id,))
        except Exception:
            return pd.DataFrame()
    else:
        query = """
            SELECT r.email, r.result, r.failed_step, r.error_message,
                   r.legacy_user_id, r.v5_user_id, r.user_action,
                   r.steps_completed, r.started_at, r.completed_at,
                   b.started_at AS batch_started, b.id AS batch_id
            FROM migration.migration_user_results r
            JOIN migration.migration_batches b ON r.batch_id = b.id
            ORDER BY r.started_at DESC
            LIMIT 500
        """
        try:
            return execute_query(config, query)
        except Exception:
            return pd.DataFrame()


def _load_summary(config: ConnectionConfig) -> dict:
    """Load overall migration statistics."""
    query = """
        SELECT
            COUNT(DISTINCT batch_id) AS total_batches,
            COUNT(*) AS total_users,
            COUNT(*) FILTER (WHERE result = 'success') AS success,
            COUNT(*) FILTER (WHERE result = 'reused_existing_user') AS reused,
            COUNT(*) FILTER (WHERE result = 'failed') AS failed,
            COUNT(*) FILTER (WHERE result = 'skipped') AS skipped,
            COUNT(*) FILTER (WHERE result = 'pending') AS pending
        FROM migration.migration_user_results
    """
    try:
        df = execute_query(config, query)
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception:
        return {}


def _load_distributed_steps(
    config: ConnectionConfig,
    migration_run_id: str,
) -> pd.DataFrame:
    frames = []
    for database in TARGET_DATABASES:
        database_config = config_for_database(config, database)
        try:
            frame = execute_query(
                database_config,
                """
                SELECT step_key, target_database, status, expected_count,
                       affected_count,
                       CASE
                           WHEN expected_count IS NULL THEN 'missing expectation'
                           WHEN affected_count IS NULL THEN 'not executed'
                           WHEN expected_count = affected_count THEN 'verified'
                           ELSE 'mismatch'
                       END AS verification,
                       verification_details, error_message,
                       started_at, completed_at
                FROM migration.migration_steps
                WHERE migration_run_id = %s::uuid
                ORDER BY step_key
                """,
                (migration_run_id,),
            )
            if not frame.empty:
                frames.append(frame)
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _expand_steps_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Expand steps_completed JSONB into individual columns."""
    if df.empty or "steps_completed" not in df.columns:
        return df

    for step in STEP_KEYS:
        df[step] = df["steps_completed"].apply(
            lambda x: x.get(step, "") if isinstance(x, dict) else ""
        )
    df = df.drop(columns=["steps_completed"])
    return df


def _get_source_config():
    """V4 audit-mirroring config, if the user connected a source DB."""
    return st.session_state.get("source_config")


# Result / status → colour for badges.
_STATUS_COLORS = {
    "success": "#1a7f37",
    "completed": "#1a7f37",
    "produced": "#1a7f37",
    "executed": "#1a7f37",
    "verified": "#1a7f37",
    "reused_existing_user": "#0969da",
    "reused": "#0969da",
    "rolled_back": "#8250df",
    "rollback_pending": "#8250df",
    "partial": "#9a6700",
    "pending": "#9a6700",
    "running": "#9a6700",
    "skipped": "#57606a",
    "tracking_missing": "#57606a",
    "failed": "#cf222e",
    "mismatch": "#cf222e",
    "error": "#cf222e",
}


def _status_color(value: str) -> str:
    value = (value or "").lower()
    for key, color in _STATUS_COLORS.items():
        if key in value:
            return color
    return "#57606a"


def _badge(label: str, value: str) -> str:
    color = _status_color(value)
    return (
        f"<span style='display:inline-block;margin:2px 6px 2px 0;padding:2px 10px;"
        f"border-radius:12px;background:{color};color:#fff;font-size:0.78rem;"
        f"font-weight:600;white-space:nowrap'>{label}: {value}</span>"
    )


def _batch_label(row) -> str:
    """One-line, human-readable label for a batch selectbox option."""
    icon = {
        "rolled_back": "↩️",
        "partial": "🟡",
        "running": "🟡",
        "failed": "🔴",
    }.get(str(row.get("status", "")).lower(), "✅")
    started = row.get("started_at")
    started_txt = started.strftime("%Y-%m-%d %H:%M") if pd.notna(started) else "—"
    return (
        f"{icon} {str(row['id'])[:8]}…  ·  {started_txt}  ·  "
        f"{row.get('status', '?')}  ·  {int(row.get('total_users') or 0)} user(s)"
    )


def render_batch_overview(base_config, batch_row, step_statuses):
    """Compact card summarising one batch: key facts + per-step chips."""
    started = batch_row.get("started_at")
    completed = batch_row.get("completed_at")
    cols = st.columns([1, 1, 1.4, 1.4, 1.6])
    cols[0].metric("Status", str(batch_row.get("status", "—")))
    cols[1].metric("Users", int(batch_row.get("total_users") or 0))
    cols[2].metric(
        "Started",
        started.strftime("%m-%d %H:%M") if pd.notna(started) else "—",
    )
    cols[3].metric(
        "Completed",
        completed.strftime("%m-%d %H:%M") if pd.notna(completed) else "—",
    )
    cols[4].metric(
        "Source",
        str(batch_row.get("source_db") or batch_row.get("source_host") or "—"),
    )

    chips = "".join(
        _badge(prefix.rstrip("_")[3:], step_statuses.get(prefix.rstrip("_"), "unknown"))
        for prefix, _, _ in ALL_STEPS
    )
    st.markdown(
        "<div style='margin-top:4px'><span style='color:#57606a;font-size:0.8rem;"
        "font-weight:600;margin-right:6px'>Steps:</span>" + chips + "</div>",
        unsafe_allow_html=True,
    )
    st.caption(f"Batch ID: `{batch_row['id']}`")


def render_shard_queue_status(base_config, batch_id: str):
    """Queued/running/failed/cancelled shard state for the background workers.

    Read-only progress plus cancel/resume controls; actually executing
    shards happens out-of-process in ``worker.py``.
    """
    progress = run_progress_by_step(base_config, batch_id)
    active = steps_with_shards(progress)
    if not active:
        return

    st.markdown("#### 🚀 Background Worker Queue")
    totals = overall_counts(progress)
    total_shards = sum(totals.values())
    completed = totals.get("completed", 0)
    st.progress(completed / total_shards if total_shards else 0.0)
    st.caption(
        "Overall: "
        + " · ".join(
            f"{SHARD_STATUS_ICONS.get(status, '')} {status}: {count}"
            for status, count in totals.items()
            if count
        )
    )

    for prefix, _, _ in ALL_STEPS:
        step_key = prefix.rstrip("_")
        statuses = active.get(step_key)
        if not statuses:
            continue
        cols = st.columns([2, 4])
        with cols[0]:
            st.markdown(f"**{STEP_LABELS.get(step_key, step_key)}**")
        with cols[1]:
            st.caption(
                " · ".join(
                    f"{SHARD_STATUS_ICONS.get(status, '')} {status}: {count}"
                    for status, count in statuses.items()
                    if count
                )
            )

    failures = failed_shard_details(base_config, batch_id)
    if failures:
        with st.expander(
            f"Failed shard diagnostics ({len(failures)})",
            expanded=True,
        ):
            for failure in failures:
                st.error(
                    f"{failure['step_key']} shard "
                    f"{failure['shard_index']}/{failure['total_shards']} "
                    f"on {failure['target_database']} "
                    f"(attempt {failure['attempts']}/{failure['max_attempts']}): "
                    f"{failure['error_message']}"
                )
                if failure["owner_emails"]:
                    st.caption(
                        "Affected users: "
                        + ", ".join(failure["owner_emails"])
                    )
                st.caption(f"SQL shard: {failure['file_path']}")

    col_cancel, col_resume = st.columns(2)
    with col_cancel:
        if st.button(
            "⏸️ Cancel remaining shards",
            disabled=not has_in_flight_shards(progress),
            key=f"hist_cancel_shards_{batch_id}",
        ):
            cancelled = cancel_run(base_config, batch_id)
            st.warning(
                f"Cancelled {cancelled} queued/retrying shard(s). "
                "Already-running shards will finish safely."
            )
            st.session_state["_history_refresh_token"] = (
                st.session_state.get("_history_refresh_token", 0) + 1
            )
            st.rerun()
    with col_resume:
        if st.button(
            "▶️ Resume failed/cancelled shards",
            disabled=not (has_actionable_failures(progress) or totals.get("cancelled", 0)),
            key=f"hist_resume_shards_{batch_id}",
        ):
            resumed = resume_run(base_config, batch_id)
            st.success(f"Re-queued {resumed} shard(s) for workers to retry.")
            st.session_state["_history_refresh_token"] = (
                st.session_state.get("_history_refresh_token", 0) + 1
            )
            st.rerun()


def render_rollback_panel(base_config, batch_row, step_statuses):
    """A single, streamlined rollback area for the selected batch.

    Scope is chosen with one control (batch / step / user); only rows created
    by this run are deleted, reused users and pre-existing V5 data are kept, and
    reverse dependency order is always enforced.
    """
    batch_id = str(batch_row["id"])
    source_config = _get_source_config()
    queue_progress = run_progress_by_step(base_config, batch_id)
    rollback_blocked = has_in_flight_shards(queue_progress)

    st.markdown("#### ↩️ Rollback")
    scope = st.radio(
        "Scope",
        ["Entire batch", "Single step", "Single user"],
        horizontal=True,
        key=f"hist_scope_{batch_id}",
        label_visibility="collapsed",
    )

    # Resolve the concrete target + a human summary for the chosen scope.
    target_kind = None       # "batch" | "step" | "user"
    target_value = None
    summary = ""
    can_run = True
    if rollback_blocked:
        st.warning(
            "Rollback is disabled while migration shards are queued or running. "
            "Cancel queued work and wait for active workers to finish."
        )
        can_run = False

    if scope == "Entire batch":
        target_kind = "batch"
        summary = (
            "Deletes **all run-created rows for every user** in this batch, "
            "in reverse order (07 → 01)."
        )
        if str(batch_row.get("status", "")).lower() == "rolled_back":
            st.info("This batch is already rolled back.")
            can_run = False

    elif scope == "Single step":
        step_keys = [p.rstrip("_") for p, _, _ in ALL_STEPS]
        target_value = st.selectbox(
            "Step",
            step_keys,
            format_func=lambda k: (
                f"{k[3:].replace('_', ' ').title()}  ·  "
                f"{step_statuses.get(k, 'unknown')}"
            ),
            key=f"hist_step_{batch_id}",
        )
        target_kind = "step"
        current = step_statuses.get(target_value, "unknown")
        summary = (
            f"Deletes only the **{STEP_LABELS.get(target_value, target_value)}** "
            f"rows for every user in this batch (current status: `{current}`)."
        )
        if current in ("rolled_back", "skipped", "tracking_missing"):
            st.info(f"Step `{target_value}` is `{current}` — nothing to roll back.")
            can_run = False

    else:  # Single user
        user_config = config_for_database(base_config, "user_db")
        users_df = _cached_user_results(
            *_cache_args(user_config)[:-1],
            batch_id,
            _cache_args(user_config)[-1],
        )
        eligible = (
            users_df[users_df["result"] != "rolled_back"]
            if not users_df.empty
            else users_df
        )
        if eligible.empty:
            st.info("No users available to roll back in this batch.")
            can_run = False
        else:
            target_value = st.selectbox(
                "User",
                eligible["email"].tolist(),
                format_func=lambda e: (
                    f"{e}  ·  "
                    f"{eligible[eligible['email'] == e]['result'].iloc[0]}"
                ),
                key=f"hist_user_{batch_id}",
            )
            target_kind = "user"
            summary = (
                f"Deletes only **{target_value}**'s run-created rows. "
                "Other users in this batch are untouched."
            )

    if summary:
        st.markdown(summary)

    opt_col, confirm_col, run_col = st.columns([1.4, 1.2, 1])
    with opt_col:
        force = st.checkbox(
            "Force (also remove dependent app rows)",
            key=f"hist_force_{batch_id}",
            help=(
                "Skips the safety guard that blocks deletion when application "
                "rows created after migration (e.g. agent_drafts) reference "
                "this run's entities. It can also remove completion-db "
                "knowledge-base item links to documents/folders being rolled "
                "back. Other cross-database dependencies remain blocked."
            ),
        )
    with confirm_col:
        confirmed = st.checkbox(
            "I understand this deletes data",
            key=f"hist_confirm_{batch_id}_{scope}",
        )
    with run_col:
        run = st.button(
            "🔙 Run rollback",
            type="primary",
            disabled=not (can_run and confirmed),
            use_container_width=True,
            key=f"hist_run_{batch_id}_{scope}",
        )

    if force:
        st.caption(
            "⚠️ Force is enabled — dependent application rows referencing this "
            "run's entities may be cascade-deleted."
        )

    if run:
        progress = st.progress(0)
        status = st.empty()

        def _progress(index, total, label):
            status.text(f"Rolling back {index + 1}/{total}: {label}")
            progress.progress((index + 1) / total)

        if target_kind == "batch":
            success, details, message = rollback_tracked_batch(
                base_config, batch_id, source_config=source_config,
                progress_callback=_progress, force=force,
            )
        elif target_kind == "step":
            with st.spinner(f"Rolling back {target_value}..."):
                success, message, rows = rollback_tracked_step(
                    base_config, batch_id, target_value,
                    source_config=source_config, force=force,
                )
            details = [{"step": target_value, "rows": rows, "message": message}]
        else:  # user
            success, details, message = rollback_tracked_user(
                base_config, batch_id, target_value,
                source_config=source_config, progress_callback=_progress,
                force=force,
            )

        st.session_state["hist_rollback_result"] = {
            "success": success,
            "message": message,
            "details": details,
        }
        st.session_state["_history_refresh_token"] = (
            st.session_state.get("_history_refresh_token", 0) + 1
        )
        st.rerun()

    result = st.session_state.get("hist_rollback_result")
    if result:
        (st.success if result["success"] else st.error)(result["message"])
        if result.get("details"):
            with st.expander("Rollback details", expanded=not result["success"]):
                st.dataframe(result["details"], hide_index=True, use_container_width=True)
        if st.button("Dismiss", key="hist_rollback_dismiss"):
            del st.session_state["hist_rollback_result"]
            st.rerun()


def render_user_results(config, batch_id, step_results):
    """Per-user results table for the selected batch (or all batches)."""
    show_all = st.checkbox(
        "Show users from all batches (read-only)",
        key="hist_show_all_users",
    )
    cache_args = _cache_args(config)
    results_df = _cached_user_results(
        *cache_args[:-1],
        None if show_all else batch_id,
        cache_args[-1],
    )

    if not show_all:
        if not step_results.empty:
            with st.expander("Per-database step facts", expanded=False):
                st.dataframe(step_results, hide_index=True, use_container_width=True)

    if results_df.empty:
        st.info("No user results for this selection.")
        return

    filter_col1, filter_col2 = st.columns([2, 3])
    with filter_col1:
        options = results_df["result"].unique().tolist()
        result_filter = st.multiselect(
            "Filter by result", options=options, default=options,
        )
    with filter_col2:
        email_search = st.text_input("Search by email", key="history_email_search")

    display_df = results_df[results_df["result"].isin(result_filter)]
    if email_search:
        display_df = display_df[
            display_df["email"].str.contains(email_search, case=False, na=False)
        ]

    display_df = _expand_steps_columns(display_df.copy())

    step_col_config = {}
    for step in STEP_KEYS:
        if step in display_df.columns:
            step_col_config[step] = st.column_config.TextColumn(
                step.replace("_", " ").title()[:12],
                help=f"Step {step}",
                width="small",
            )

    st.dataframe(
        display_df,
        hide_index=True,
        use_container_width=True,
        height=400,
        column_config={
            "email": st.column_config.TextColumn("Email"),
            "result": st.column_config.TextColumn("Result"),
            "failed_step": st.column_config.TextColumn("Failed Step"),
            "error_message": st.column_config.TextColumn("Error"),
            "started_at": st.column_config.DatetimeColumn("Started", format="YYYY-MM-DD HH:mm"),
            "completed_at": st.column_config.DatetimeColumn("Completed", format="YYYY-MM-DD HH:mm"),
            **step_col_config,
        },
    )

    status_counts = display_df["result"].value_counts()
    st.caption("Result breakdown: " + ", ".join(f"{k}: {v}" for k, v in status_counts.items()))

    action_col1, action_col2 = st.columns(2)
    with action_col1:
        st.download_button(
            label="📥 Export as CSV",
            data=display_df.to_csv(index=False),
            file_name="migration_results.csv",
            mime="text/csv",
        )
    with action_col2:
        failed_emails = display_df[display_df["result"] == "failed"]["email"].tolist()
        if failed_emails:
            if st.button(f"🔄 Re-run {len(failed_emails)} Failed Users", type="primary"):
                st.session_state["_p2_batch_saved_emails"] = failed_emails
                st.switch_page("pages/2_select_data.py")
        else:
            st.info("No failed users to re-run.")


def main():
    st.title("📋 Migration History")
    st.caption(
        "Pick a batch to see its status, per-step details, users, and rollback "
        "controls — everything below is scoped to the batch you select."
    )

    config = config_for_database(_get_target_config(), "user_db")
    if not config.host:
        st.warning("Target database not configured. Go to the Connect page first.")
        return

    _ensure_tracking_tables(config)
    refresh_col, _ = st.columns([1, 5])
    with refresh_col:
        if st.button("↻ Refresh history", use_container_width=True):
            st.session_state["_history_refresh_token"] = (
                st.session_state.get("_history_refresh_token", 0) + 1
            )
            st.rerun()

    cache_args = _cache_args(config)
    try:
        snapshot = _cached_history_snapshot(*cache_args)
    except Exception as exc:
        st.error(f"Could not load migration history: {exc}")
        return
    if snapshot.empty:
        st.info("No migration history found. Run a migration to see results here.")
        return
    first = snapshot.iloc[0]
    summary = {
        "total_batches": first.get("total_batches", 0),
        "total_users": first.get("summary_total_users", 0),
        "success": first.get("success", 0),
        "reused": first.get("reused", 0),
        "failed": first.get("failed", 0),
        "skipped": first.get("skipped", 0),
        "pending": first.get("pending", 0),
    }

    cols = st.columns(6)
    cols[0].metric("Total Batches", summary.get("total_batches", 0))
    cols[1].metric("Total Users", summary.get("total_users", 0))
    cols[2].metric("Success", summary.get("success", 0))
    cols[3].metric("Reused Existing", summary.get("reused", 0))
    cols[4].metric("Failed", summary.get("failed", 0))
    cols[5].metric("Pending", summary.get("pending", 0))

    batches_df = snapshot[
        [
            "id",
            "started_at",
            "completed_at",
            "status",
            "total_users",
            "source_host",
            "source_db",
            "notes",
        ]
    ].copy()
    if batches_df.empty:
        st.info("No batches recorded yet.")
        return

    st.markdown("---")

    # ── Batch selector (defaults to the most recent batch) ────────────────
    batch_ids = batches_df["id"].tolist()
    row_by_id = {str(r["id"]): r for _, r in batches_df.iterrows()}
    selected_batch = st.selectbox(
        "Select a migration batch",
        options=batch_ids,
        format_func=lambda x: _batch_label(row_by_id[str(x)]),
        key="hist_selected_batch",
    )
    batch_row = row_by_id[str(selected_batch)]
    base_config = _get_target_config()

    step_cache_args = _cache_args(base_config)
    step_results = _cached_distributed_steps(
        *step_cache_args[:-1],
        str(selected_batch),
        step_cache_args[-1],
    )
    step_statuses = (
        {
            str(row["step_key"]): str(row["status"])
            for _, row in step_results.iterrows()
        }
        if not step_results.empty
        else {}
    )

    # ── Selected batch: overview + rollback, side by side ─────────────────
    overview_col, rollback_col = st.columns([1.15, 1], gap="large")
    with overview_col:
        st.markdown("#### 📦 Selected batch")
        render_batch_overview(base_config, batch_row, step_statuses)
    with rollback_col:
        render_rollback_panel(base_config, batch_row, step_statuses)

    st.markdown("---")
    render_shard_queue_status(base_config, str(selected_batch))
    st.markdown("---")

    # ── Per-user results for the selected batch ───────────────────────────
    st.subheader("👥 User Results")
    render_user_results(config, str(selected_batch), step_results)

    # ── All batches (reference) ───────────────────────────────────────────
    with st.expander("📚 All batches (reference table)", expanded=False):
        st.dataframe(
            batches_df,
            hide_index=True,
            use_container_width=True,
            column_config={
                "id": st.column_config.TextColumn("Batch ID"),
                "started_at": st.column_config.DatetimeColumn("Started", format="YYYY-MM-DD HH:mm"),
                "completed_at": st.column_config.DatetimeColumn("Completed", format="YYYY-MM-DD HH:mm"),
                "status": st.column_config.TextColumn("Status"),
                "total_users": st.column_config.NumberColumn("Users"),
                "source_host": st.column_config.TextColumn("Source Host"),
                "source_db": st.column_config.TextColumn("Source DB"),
                "notes": st.column_config.TextColumn("Notes"),
            },
        )


main()
