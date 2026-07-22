"""
Page 7: Migration History

Displays migration batch history and per-user results with per-step details,
CSV export, and the ability to re-run failed users.
"""
import streamlit as st
import pandas as pd

from utils.db import ConnectionConfig, execute_query, get_connection
from utils.config import get_env_target_defaults
from utils.migration_tracking import (
    TARGET_DATABASES,
    config_for_database,
    ensure_tracking_schema,
)
from utils.rollback import (
    ALL_STEPS,
    STEP_LABELS,
    load_batch_step_statuses,
    rollback_tracked_batch,
    rollback_tracked_step,
    rollback_tracked_user,
)

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


def _ensure_tracking_tables(config: ConnectionConfig):
    """Create migration tracking tables if they don't exist."""
    try:
        ensure_tracking_schema(
            config_for_database(config, "user_db"), coordinator=True
        )
    except Exception:
        pass


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


def render_rollback_controls(base_config, batch_id, batches_df):
    """Batch / per-step / per-user rollback for one selected migration batch.

    Deletes are always scoped to rows created by this run; reused users and
    pre-existing V5 data are preserved, and reverse dependency order is enforced.
    """
    batch_id = str(batch_id)
    st.markdown("---")
    st.subheader("↩️ Rollback Controls")
    st.caption(
        "Roll back an entire batch, a single step (e.g. only Agents), or a "
        "single user. Only rows created by this run are deleted — reused users "
        "and pre-existing V5 data are always preserved."
    )

    try:
        batch_status = (
            batches_df[batches_df["id"].astype(str) == batch_id]["status"].iloc[0]
        )
    except Exception:
        batch_status = None

    source_config = _get_source_config()

    try:
        step_statuses = load_batch_step_statuses(base_config, batch_id)
    except Exception as exc:
        step_statuses = {}
        st.warning(f"Could not load per-step status: {exc}")

    tab_batch, tab_step, tab_user = st.tabs(
        ["Entire batch", "Single step", "Single user"]
    )

    # ── Entire batch ──────────────────────────────────────────────────────
    with tab_batch:
        st.warning(
            "Deletes all run-created entities for every user in this batch, "
            "in strict reverse dependency order (07 → 01)."
        )
        st.code("07 → 06 → 05 → 04 → 03 → 02 → 01", language=None)
        with st.popover(
            "🔙 Rollback Entire Batch",
            use_container_width=True,
            disabled=batch_status == "rolled_back",
        ):
            confirm_batch = st.text_input(
                "Type the full batch ID to confirm",
                key=f"hist_batch_confirm_{batch_id}",
            )
            if st.button(
                "Confirm Entire Batch Rollback",
                type="primary",
                disabled=confirm_batch != batch_id,
                key=f"hist_batch_rollback_{batch_id}",
            ):
                progress = st.progress(0)
                status = st.empty()

                def _batch_progress(index, total, label):
                    status.text(f"Rolling back {index + 1}/{total}: {label}")
                    progress.progress((index + 1) / total)

                success, details, message = rollback_tracked_batch(
                    base_config,
                    batch_id,
                    source_config=source_config,
                    progress_callback=_batch_progress,
                )
                st.session_state["hist_rollback_result"] = {
                    "success": success,
                    "message": message,
                    "details": details,
                }
                st.rerun()

    # ── Single step ───────────────────────────────────────────────────────
    with tab_step:
        step_keys = [prefix.rstrip("_") for prefix, _, _ in ALL_STEPS]
        selected_step = st.selectbox(
            "Step to roll back",
            step_keys,
            format_func=lambda k: (
                f"{k} · {STEP_LABELS.get(k, k)} · {step_statuses.get(k, 'unknown')}"
            ),
            key=f"hist_step_select_{batch_id}",
        )
        current = step_statuses.get(selected_step, "unknown")
        st.caption(f"Current status: **{current}**")
        st.info(
            "Reverse-order safety still applies: rolling back an earlier step "
            "while a later dependent step is still live will be blocked with a "
            "clear message telling you which steps to roll back first."
        )
        blocked = current in ("rolled_back", "skipped", "tracking_missing")
        with st.popover(
            "🔙 Rollback Selected Step",
            use_container_width=True,
            disabled=blocked,
        ):
            st.warning(
                f"Deletes only the **{STEP_LABELS.get(selected_step, selected_step)}** "
                "step's run-created rows for every user in this batch."
            )
            confirm_step = st.text_input(
                f"Type {selected_step} to confirm",
                key=f"hist_step_confirm_{batch_id}_{selected_step}",
            )
            if st.button(
                "Confirm Step Rollback",
                type="primary",
                disabled=confirm_step != selected_step,
                key=f"hist_step_rollback_{batch_id}_{selected_step}",
            ):
                with st.spinner(f"Rolling back {selected_step}..."):
                    success, message, rows = rollback_tracked_step(
                        base_config,
                        batch_id,
                        selected_step,
                        source_config=source_config,
                    )
                st.session_state["hist_rollback_result"] = {
                    "success": success,
                    "message": message,
                    "details": [
                        {"step": selected_step, "rows": rows, "message": message}
                    ],
                }
                st.rerun()

    # ── Single user ───────────────────────────────────────────────────────
    with tab_user:
        user_config = config_for_database(base_config, "user_db")
        users_df = _load_user_results(user_config, batch_id)
        if users_df.empty:
            st.info("No tracked users for this batch.")
        else:
            eligible = users_df[users_df["result"] != "rolled_back"]
            if eligible.empty:
                st.info("Every user in this batch is already rolled back.")
            else:
                emails = eligible["email"].tolist()
                selected_email = st.selectbox(
                    "User to roll back",
                    emails,
                    format_func=lambda e: (
                        f"{e} · "
                        f"{eligible[eligible['email'] == e]['result'].iloc[0]}"
                    ),
                    key=f"hist_user_select_{batch_id}",
                )
                with st.popover(
                    "🔙 Rollback Selected User",
                    use_container_width=True,
                ):
                    st.warning(
                        "Deletes only this user's run-created entities. Other "
                        "users in this batch are not changed."
                    )
                    confirm_user = st.text_input(
                        f"Type {selected_email} to confirm",
                        key=f"hist_user_confirm_{batch_id}_{selected_email}",
                    )
                    if st.button(
                        "Confirm User Rollback",
                        type="primary",
                        disabled=confirm_user != selected_email,
                        key=f"hist_user_rollback_{batch_id}_{selected_email}",
                    ):
                        progress = st.progress(0)
                        status = st.empty()

                        def _user_progress(index, total, label):
                            status.text(
                                f"Rolling back {index + 1}/{total}: {label}"
                            )
                            progress.progress((index + 1) / total)

                        success, details, message = rollback_tracked_user(
                            base_config,
                            batch_id,
                            selected_email,
                            source_config=source_config,
                            progress_callback=_user_progress,
                        )
                        st.session_state["hist_rollback_result"] = {
                            "success": success,
                            "message": message,
                            "details": details,
                        }
                        st.rerun()

    result = st.session_state.get("hist_rollback_result")
    if result:
        if result["success"]:
            st.success(result["message"])
        else:
            st.error(result["message"])
        if result.get("details"):
            with st.expander("Rollback details", expanded=not result["success"]):
                st.dataframe(result["details"], hide_index=True)
        if st.button("Dismiss result", key="hist_rollback_dismiss"):
            del st.session_state["hist_rollback_result"]
            st.rerun()


def main():
    st.title("📋 Migration History")
    st.caption("View migration batches and per-user results with per-step tracking.")
    st.caption(
        "Select a specific batch below to reveal batch, per-step, and per-user "
        "rollback controls."
    )

    config = config_for_database(_get_target_config(), "user_db")

    if not config.host:
        st.warning("Target database not configured. Go to the Connect page first.")
        return

    _ensure_tracking_tables(config)

    # Summary metrics
    summary = _load_summary(config)
    if summary:
        cols = st.columns(6)
        cols[0].metric("Total Batches", summary.get("total_batches", 0))
        cols[1].metric("Total Users", summary.get("total_users", 0))
        cols[2].metric("Success", summary.get("success", 0))
        cols[3].metric("Reused Existing", summary.get("reused", 0))
        cols[4].metric("Failed", summary.get("failed", 0))
        cols[5].metric("Pending", summary.get("pending", 0))
    else:
        st.info("No migration history found. Run a migration to see results here.")
        return

    st.markdown("---")

    # Batches table
    st.subheader("Migration Batches")
    batches_df = _load_batches(config)
    if batches_df.empty:
        st.info("No batches recorded yet.")
        return

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

    # Batch selector
    batch_ids = batches_df["id"].tolist()
    batch_options = ["All"] + batch_ids
    selected_batch = st.selectbox(
        "Select batch to view details",
        options=batch_options,
        format_func=lambda x: x if x == "All" else f"{str(x)[:8]}... ({batches_df[batches_df['id'] == x]['started_at'].iloc[0]})",
    )

    if selected_batch != "All":
        render_rollback_controls(_get_target_config(), selected_batch, batches_df)

    st.markdown("---")
    st.subheader("User Results")

    if selected_batch == "All":
        results_df = _load_user_results(config)
    else:
        results_df = _load_user_results(config, selected_batch)
        step_results = _load_distributed_steps(config, str(selected_batch))
        if not step_results.empty:
            st.subheader("Per-database Step Facts")
            st.dataframe(step_results, hide_index=True, use_container_width=True)

    if results_df.empty:
        st.info("No user results for this selection.")
        return

    # Filters row
    filter_col1, filter_col2 = st.columns([2, 3])
    with filter_col1:
        result_filter = st.multiselect(
            "Filter by result",
            options=results_df["result"].unique().tolist(),
            default=results_df["result"].unique().tolist(),
        )
    with filter_col2:
        email_search = st.text_input("Search by email", key="history_email_search")

    display_df = results_df[results_df["result"].isin(result_filter)]
    if email_search:
        display_df = display_df[display_df["email"].str.contains(email_search, case=False, na=False)]

    # Expand steps_completed into individual columns
    display_df = _expand_steps_columns(display_df.copy())

    # Column config for step columns
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

    # Status breakdown
    status_counts = display_df["result"].value_counts()
    st.caption("Result breakdown: " + ", ".join(f"{k}: {v}" for k, v in status_counts.items()))

    # Action buttons
    st.markdown("---")
    action_col1, action_col2 = st.columns(2)

    with action_col1:
        # CSV Export
        csv_data = display_df.to_csv(index=False)
        st.download_button(
            label="📥 Export as CSV",
            data=csv_data,
            file_name="migration_results.csv",
            mime="text/csv",
        )

    with action_col2:
        # Re-run failed users
        failed_emails = display_df[display_df["result"] == "failed"]["email"].tolist()
        if failed_emails:
            if st.button(f"🔄 Re-run {len(failed_emails)} Failed Users", type="primary"):
                st.session_state["_p2_batch_saved_emails"] = failed_emails
                st.switch_page("pages/2_select_data.py")
        else:
            st.info("No failed users to re-run.")


main()
