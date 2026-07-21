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


def main():
    st.title("📋 Migration History")
    st.caption("View migration batches and per-user results with per-step tracking.")

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
