"""
Page: Run Migration SQL Scripts

Features:
- List all generated SQL migration files and their shard manifests
- Enqueue a run's shards onto the durable PostgreSQL queue for background
  ``worker.py`` processes to execute (this page never runs migration SQL
  itself)
- Monitor per-step shard progress, retry/cancel/resume the queue, and
  finalize a batch once every step is verified complete
- Support for separate databases (user_db, document_db, completion_db)
"""
import streamlit as st
from datetime import datetime

from utils.db import ConnectionConfig
from utils.file_preview import (
    INLINE_DOWNLOAD_BYTES,
    human_file_size,
    migration_file_metadata,
    read_inline_download,
    read_text_preview,
)
from utils.config import SessionKeys, get_env_connection_defaults
from utils.migration_tracking import (
    finalize_distributed_run,
    reconcile_rollback_status,
)
from utils.rollback import (
    DB_MAPPING,
    ALL_STEPS,
    rollback_migration,
    rollback_all_migrations,
)
from utils.migration_steps import MIGRATION_STEP_ORDER
from utils.queue_ui import (
    cancel_run,
    enqueue_step,
    enqueue_run,
    failed_shard_details,
    has_actionable_failures,
    has_in_flight_shards,
    is_run_fully_enqueued_and_done,
    overall_counts,
    resume_run,
    run_progress_by_step,
    steps_with_shards,
)

STATUS_ICONS = {
    "queued": "⏸️",
    "retrying": "🔁",
    "running": "⚙️",
    "completed": "✅",
    "failed": "❌",
    "cancelled": "🚫",
}

# Page config
st.set_page_config(page_title="Run Migrations", page_icon="🚀", layout="wide")
st.title("🚀 Run Migration SQL Scripts")

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


def get_migration_files(file_paths=None):
    """Return only SQL files belonging to the active extraction."""
    return migration_file_metadata(file_paths or [], DB_MAPPING)


def render_shard_queue_section(migration_files, files_by_prefix, progress):
    """Enqueue/monitor/retry/cancel/resume view for the background workers.

    Streamlit's job here is limited to populating and inspecting the durable
    ``migration.migration_shards`` queue -- actually executing shards happens
    out-of-process in ``worker.py``, so this view never blocks on long SQL.
    """
    st.markdown("### 🚀 Background Worker Queue")
    base_config = st.session_state["target_config"]
    batch_id = st.session_state.get("_current_batch_id")
    if not batch_id:
        st.info("No tracked migration run is selected.")
        return

    active = steps_with_shards(progress)

    col_enqueue, col_refresh = st.columns([2, 1])
    with col_enqueue:
        if st.button(
            "📥 Enqueue All Steps for Background Workers",
            type="primary",
            disabled=bool(active),
            help=(
                "Disabled once shards exist for this run -- re-run extraction "
                "to generate a new run instead of enqueuing duplicates."
            ),
        ):
            primary_file_by_prefix = {
                prefix: info["path"] for prefix, info in files_by_prefix.items()
            }
            selected_users = st.session_state.get(SessionKeys.SELECTED_USERS)
            owner_emails = selected_users if isinstance(selected_users, list) else None
            enqueued = enqueue_run(
                base_config, batch_id, primary_file_by_prefix, owner_emails=owner_emails
            )
            if enqueued:
                st.success(
                    f"Enqueued {sum(enqueued.values())} shard(s) across "
                    f"{len(enqueued)} step(s). Start `worker.py` processes to execute them."
                )
            else:
                st.warning(
                    "No shard manifests were found to enqueue. Re-run extraction "
                    "on the Select Data page if this is unexpected."
                )
            st.rerun()
    with col_refresh:
        if st.button("🔄 Refresh Status"):
            st.rerun()

    if not active:
        st.caption(
            "Nothing is enqueued yet for this run. Enqueueing hands the generated "
            "SQL shards to `migration.migration_shards`; run `python worker.py` "
            "(or `docker compose up -d --scale migration-worker=2`) to execute them. "
            "Shards are idempotent, so it is always safe to enqueue then scale "
            "workers up or down."
        )
        return

    totals = overall_counts(progress)
    total_shards = sum(totals.values())
    completed = totals.get("completed", 0)
    st.progress(completed / total_shards if total_shards else 0.0)
    st.caption(
        "Overall: "
        + " · ".join(
            f"{STATUS_ICONS.get(status, '')} {status}: {count}"
            for status, count in totals.items()
            if count
        )
    )

    for step_key, _, label in MIGRATION_STEP_ORDER:
        statuses = active.get(step_key)
        if not statuses:
            continue
        step_total = sum(statuses.values())
        step_done = statuses.get("completed", 0)
        cols = st.columns([2, 4, 1])
        with cols[0]:
            st.markdown(f"**{step_key[:2]}. {label}**")
        with cols[1]:
            st.caption(
                " · ".join(
                    f"{STATUS_ICONS.get(status, '')} {status}: {count}"
                    for status, count in statuses.items()
                    if count
                )
            )
        with cols[2]:
            st.caption(f"{step_done}/{step_total}")

    col_cancel, col_resume = st.columns(2)
    with col_cancel:
        if st.button(
            "⏸️ Cancel Remaining Shards",
            disabled=not has_in_flight_shards(progress),
        ):
            cancelled = cancel_run(base_config, batch_id)
            st.warning(
                f"Cancelled {cancelled} queued/retrying shard(s). "
                "Already-running shards will finish safely."
            )
            st.rerun()
    with col_resume:
        if st.button(
            "▶️ Resume Failed/Cancelled Shards",
            disabled=not (has_actionable_failures(progress) or totals.get("cancelled", 0)),
        ):
            resumed = resume_run(base_config, batch_id)
            st.success(f"Re-queued {resumed} shard(s) for workers to retry.")
            st.rerun()

    if is_run_fully_enqueued_and_done(progress):
        st.success("✅ All enqueued steps are completed and verified.")
        if st.button("🏁 Finalize Batch"):
            try:
                _finalize_batch(batch_id)
                st.success("Batch finalized.")
            except Exception as exc:
                st.error(f"Could not finalize batch: {exc}")
            st.rerun()

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

    st.markdown("---")


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
    
    extracted = st.session_state.get(SessionKeys.EXTRACTED_DATA, {})
    active_sql_files = extracted.get("sql_files", {}) if isinstance(extracted, dict) else {}
    migration_files = get_migration_files(active_sql_files.values())
    
    if not migration_files:
        st.info("📭 No SQL files are attached to the active extraction.")
        st.markdown(
            "Run **Select Data → Start Extraction** in this session. "
            "Stale files from older runs are intentionally ignored."
        )
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

    queue_progress = run_progress_by_step(
        st.session_state["target_config"], st.session_state.get("_current_batch_id")
    )
    rollback_blocked = has_in_flight_shards(queue_progress)

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
            col1, col2, col3 = st.columns([3, 2, 1])
            
            with col1:
                st.markdown(f"### {step_prefix[:2]}. {filename}")
                st.caption(
                    f"📊 Target DB: **{target_db}** | "
                    f"Size: {human_file_size(file_info['size'])} | "
                    f"Modified: {file_info['modified'].strftime('%Y-%m-%d %H:%M:%S')}"
                )
            
            with col2:
                # Queue-derived status (execution now happens via background
                # workers, not a synchronous button in this UI).
                step_statuses = queue_progress.get(step_prefix.rstrip("_"), {})
                if not step_statuses:
                    st.info("⏸️ Not enqueued")
                    if st.button(
                        "Enqueue this step",
                        key=f"enqueue_step_{step_prefix}",
                    ):
                        selected_users = st.session_state.get(
                            SessionKeys.SELECTED_USERS
                        )
                        enqueued = enqueue_step(
                            st.session_state["target_config"],
                            st.session_state.get("_current_batch_id"),
                            step_prefix.rstrip("_"),
                            file_info["path"],
                            owner_emails=(
                                selected_users
                                if isinstance(selected_users, list)
                                else None
                            ),
                        )
                        if enqueued:
                            st.success(f"Enqueued {enqueued} shard(s).")
                        else:
                            st.error("No shard manifest was found for this step.")
                        st.rerun()
                elif step_statuses.get("failed") or step_statuses.get("cancelled"):
                    st.error(
                        f"❌ {step_statuses.get('failed', 0)} failed · "
                        f"{step_statuses.get('cancelled', 0)} cancelled"
                    )
                    if st.button(
                        "Retry this step",
                        key=f"retry_step_{step_prefix}",
                    ):
                        resumed = resume_run(
                            st.session_state["target_config"],
                            st.session_state.get("_current_batch_id"),
                            step_key=step_prefix.rstrip("_"),
                        )
                        st.success(f"Re-queued {resumed} shard(s).")
                        st.rerun()
                elif set(step_statuses.keys()) == {"completed"}:
                    st.success(f"✅ Completed ({step_statuses['completed']} shard(s))")
                else:
                    in_flight = sum(
                        step_statuses.get(s, 0) for s in ("queued", "retrying", "running")
                    )
                    st.warning(f"⚙️ {in_flight} shard(s) in progress")

            with col3:
                # Rollback button with popover confirmation
                with st.popover("🔙 Rollback", use_container_width=True):
                    st.warning("⚠️ This deletes only rows created by the selected run.")
                    st.markdown(f"""This will:
- Delete records marked `created` for this run
- Preserve reused users and all pre-existing V5 data
- Preserve document mappings during Step 04 rollback

**Target DB:** {target_db}
**File:** {filename}""")
                    
                    if rollback_blocked:
                        st.info(
                            "Rollback is unavailable while shards are queued or running."
                        )
                    if st.button(
                        "Confirm Rollback",
                        key=f"confirm_rollback_{filename}",
                        type="primary",
                        disabled=rollback_blocked,
                    ):
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
            
            # Show the most recent rollback result for this step, if any.
            status = st.session_state.migration_status.get(filename, {})
            if status.get("rollback"):
                st.info(status.get("message", "Rolled back"))
                if status.get("timestamp"):
                    st.caption(f"Rolled back at: {status['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Collapsed Streamlit expanders still execute their body, so the
            # actual file read is additionally gated by an explicit toggle.
            with st.expander(f"📄 View SQL ({filename})"):
                st.caption(
                    f"Server path: `{file_info['path']}` · "
                    f"{human_file_size(file_info['size'])}"
                )
                if st.toggle(
                    "Load bounded preview",
                    value=False,
                    key=f"load_preview_{filename}",
                ):
                    try:
                        sql_preview, truncated = read_text_preview(file_info["path"])
                        if truncated:
                            sql_preview += "\n\n-- [PREVIEW TRUNCATED AT 50 KB] --"
                        st.code(sql_preview, language="sql")
                    except Exception as exc:
                        st.error(f"Failed to read preview: {exc}")

                if file_info["size"] <= INLINE_DOWNLOAD_BYTES:
                    try:
                        download_data = read_inline_download(file_info["path"])
                        st.download_button(
                            label="💾 Download SQL",
                            data=download_data,
                            file_name=filename,
                            mime="text/plain",
                            key=f"download_{filename}",
                            on_click="ignore",
                        )
                    except Exception as exc:
                        st.error(f"Failed to prepare download: {exc}")
                else:
                    st.info(
                        "Large-file browser download is disabled to keep the UI responsive. "
                        "Use the server path shown above."
                    )
            
            st.markdown("---")

    render_shard_queue_section(migration_files, files_by_prefix, queue_progress)
    if rollback_blocked:
        st.warning(
            "Rollback is disabled while migration shards are queued or running. "
            "Cancel queued work and wait for active workers to finish."
        )

    # Bulk actions
    st.markdown("### 🎛️ Bulk Actions")
    col2, col3 = st.columns(2)

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
                disabled=rollback_blocked or confirm_run != batch_id,
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
    This page enqueues generated migration SQL shards for background workers
    to execute -- it does not run the SQL itself.

    **✅ How it works:**
    1. SQL shards (and their manifests) are loaded from `output/migrations/`
    2. Click **📥 Enqueue All Steps for Background Workers** to hand this
       run's shards to the durable PostgreSQL queue
    3. Start one or more `python worker.py` processes (or
       `docker compose up -d --scale migration-worker=2`) to execute them
    4. Use **🔄 Refresh Status** to watch per-step progress, and
       **⏸️ Cancel** / **▶️ Resume** to control the queue

    **⚠️ Important:**
    - Steps execute in dependency order automatically; you do not need to
      wait for one step before enqueuing the next
    - Every shard commits in its own transaction and its INSERTs are
      idempotent, so retries and re-enqueues are always safe
    - Target database connection must be configured first

    ℹ️ Batch, per-step, and per-user rollback controls now live on the
    **📋 Migration History** page.
    """)
    
    st.markdown("---")

    render_migration_files()


if __name__ == "__main__":
    main()
