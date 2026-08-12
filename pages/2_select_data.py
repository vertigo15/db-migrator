"""
Page 2: Select Data to Migrate

Features:
- User selection with searchable dataframe
- Document filters (date range, max size)
- Related data counts (folders, embeddings, agents)
- Extraction with progress
- CSV preview and download
"""
import hashlib
import os
import json
import uuid
from datetime import datetime, date, timedelta
from typing import Optional
import streamlit as st
import pandas as pd

from utils.db import ConnectionConfig, execute_query
from utils.file_preview import (
    INLINE_DOWNLOAD_BYTES,
    human_file_size,
    read_inline_download,
    read_text_preview,
)
from utils.storage import (
    save_selected_users, load_selected_users,
    save_document_filters, load_document_filters
)
from utils.config import (
    EMBEDDING_MODEL_OPTIONS,
    SHAREPOINT_DOCUMENT_BLOB_SOURCE,
    SessionKeys,
    get_env_batch_size,
    get_env_embedding_model,
    get_env_org_id,
    get_env_target_defaults,
    get_table_name,
)
from utils.db import test_connection
from utils.extraction import (
    ExtractionEngine,
    build_conversation_scope_cte,
    get_document_count_preview,
    get_selection_summary,
    resolve_existing_user_overrides,
    validate_target_organization,
)
from utils.migration_tracking import (
    create_distributed_run,
    mark_unproduced_steps_skipped,
    record_step_expectations,
    update_local_run,
    update_source_run,
)
from utils.ownership_preflight import (
    find_canonical_ownership_conflicts,
    ownership_conflict_message,
    repair_orphaned_document_owners,
    repair_orphaned_folder_owners,
)
from utils.conversation_preflight import (
    conversation_conflict_message,
    inspect_conversation_conflicts,
)
from utils.agent_preflight import (
    agent_conflict_message,
    inspect_agent_conflicts,
)
from utils.user_batching import (
    select_user_letter_batch,
    user_bucket_counts,
)

# Page config
st.set_page_config(page_title="Select Data", page_icon="📋", layout="wide")
st.title("📋 Select Data to Migrate")

# Output directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(BASE_DIR, "output", "extract")
DEPENDENCY_PREVIEW_LIMIT = 5000
DEPENDENCY_DETAIL_KEYS = (
    "_p2_show_document_details",
    "_p2_show_embedding_details",
    "_p2_show_agent_details",
    "_p2_show_conversation_details",
)


def _contains_literal(series: pd.Series, value: str) -> pd.Series:
    """Return case-insensitive literal matches for a UI search value."""
    return series.astype("string").str.contains(
        value,
        case=False,
        na=False,
        regex=False,
    )


def _activate_dependency_detail(active_key: str) -> None:
    """Keep at most one expensive dependency preview active."""
    if not st.session_state.get(active_key):
        return
    for key in DEPENDENCY_DETAIL_KEYS:
        if key != active_key:
            st.session_state[key] = False


def _get_already_migrated_emails() -> set:
    """Return normalized terminal-success emails or fail closed."""
    from utils.db import get_connection
    target_defaults = get_env_target_defaults()
    conn = None
    try:
        target_config = ConnectionConfig(
            host=target_defaults["host"],
            port=int(target_defaults["port"]),
            database="user_db",
            username=target_defaults["username"],
            password=target_defaults["password"],
        )
        conn = get_connection(target_config)
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT lower(btrim(email))
                FROM migration.migration_user_results
                WHERE result IN ('success', 'reused_existing_user')
                  AND email IS NOT NULL
                  AND btrim(email) <> ''
            """)
            return {
                str(row[0]).strip().lower()
                for row in cursor.fetchall()
                if row[0]
            }
    except Exception as exc:
        raise RuntimeError(
            "Could not read completed-user tracking from user_db. "
            "Next Batch was stopped to prevent duplicate migration."
        ) from exc
    finally:
        if conn is not None and not conn.closed:
            conn.close()


def _source_scope_key(config: ConnectionConfig, prefix: str) -> str:
    """Stable key for caching reads from the source DB (unchanged when only SQL/org UI changes)."""
    d = config.to_dict()
    return f"{d['host']}:{d['port']}:{d['database']}:{d['username']}:{prefix}"


def _filters_fingerprint(filters: dict) -> str:
    """Stable fingerprint for document filter dict (may contain datetimes)."""
    if not filters:
        return "none"

    def _conv(v):
        if isinstance(v, datetime):
            return v.isoformat()
        if isinstance(v, date):
            return v.isoformat()
        return v

    try:
        return json.dumps(
            {str(k): _conv(v) for k, v in sorted(filters.items(), key=lambda kv: str(kv[0]))},
            sort_keys=True,
        )
    except TypeError:
        return repr(filters)


# ─── Destination DB lookup helpers ──────────────────────────────────────────

def _make_target_config(database: str):
    """Return a ConnectionConfig for a specific target database, or None if
    no target connection is available in session state."""
    target: ConnectionConfig = st.session_state.get("target_config")
    if target is None:
        td = st.session_state.get(SessionKeys.TARGET_CONNECTION, {})
        if not td.get("host") or not td.get("password"):
            defaults = get_env_target_defaults()
            td = {
                "host": defaults.get("host"),
                "port": defaults.get("port"),
                "database": defaults.get("database"),
                "username": defaults.get("username"),
                "password": defaults.get("password"),
            }
        if not td.get("host") or not td.get("password"):
            return None
        try:
            target = ConnectionConfig.from_dict(td)
        except Exception:
            return None
    return ConnectionConfig(
        host=target.host,
        port=target.port,
        database=database,
        username=target.username,
        password=target.password,
    )


def fetch_dest_org_ids() -> list:
    """Query target admin_db for organizations (id + name).
    Falls back to user_db.users if admin_db is unavailable.
    Returns list of dicts: [{"id": "uuid", "name": "Org Name"}, ...]
    """
    cfg = _make_target_config("admin_db")
    if cfg is not None:
        try:
            df = execute_query(
                cfg,
                "SELECT id::text AS org_id, name AS org_name "
                "FROM public.organizations "
                "WHERE is_active = true "
                "ORDER BY name"
            )
            if not df.empty:
                return [{"id": r["org_id"], "name": r["org_name"]} for _, r in df.iterrows()]
        except Exception:
            pass

    cfg = _make_target_config("user_db")
    if cfg is None:
        return []
    try:
        df = execute_query(
            cfg,
            "SELECT DISTINCT organization_id::text AS org_id "
            "FROM public.users "
            "WHERE organization_id IS NOT NULL "
            "ORDER BY org_id"
        )
        return [{"id": r["org_id"], "name": None} for _, r in df.iterrows()] if not df.empty else []
    except Exception:
        return []


def _org_name_label(org: dict) -> str:
    """Label for org name selectbox (name from DB; fallback if missing)."""
    name = (org.get("name") or "").strip()
    if name:
        return name
    oid = str(org.get("id") or "")
    return oid if len(oid) <= 48 else f"{oid[:8]}…{oid[-4:]}"


def fetch_dest_embedding_models() -> list:
    """Query target document_db for distinct embedding model_names."""
    cfg = _make_target_config("document_db")
    if cfg is None:
        return []
    try:
        df = execute_query(
            cfg,
            "SELECT DISTINCT model_name "
            "FROM public.embeddings "
            "WHERE model_name IS NOT NULL "
            "ORDER BY model_name"
        )
        return df["model_name"].tolist() if not df.empty else []
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────


def convert_timestamp_to_datetime(ts):
    """Convert Unix timestamp (float/int) or string to datetime."""
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return None
    if isinstance(ts, str):
        try:
            return pd.to_datetime(ts)
        except:
            return None
    if isinstance(ts, (int, float)):
        try:
            return pd.to_datetime(ts, unit='s')
        except:
            return None
    return ts


def check_connection():
    """Check if source connection is available. Auto-loads from .env if needed."""
    if "source_config" not in st.session_state:
        from utils.config import get_env_connection_defaults, get_env_table_prefix
        defaults = get_env_connection_defaults()
        if defaults.get("host") and defaults.get("database") and defaults.get("username"):
            config = ConnectionConfig(
                host=defaults["host"],
                port=int(defaults["port"]),
                database=defaults["database"],
                username=defaults["username"],
                password=defaults.get("password", ""),
            )
            st.session_state["source_config"] = config
            st.session_state[SessionKeys.SOURCE_CONNECTION] = config.to_dict()
            if SessionKeys.TABLE_PREFIX not in st.session_state:
                st.session_state[SessionKeys.TABLE_PREFIX] = get_env_table_prefix()
        else:
            st.warning("⚠️ Please connect to the source database first.")
            st.page_link("pages/1_connect.py", label="Go to Connect Page", icon="🔌")
            return False
    return True


def load_users_data(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Load users from the source database with document and agent counts."""
    table_name = get_table_name("users", prefix)
    docs_table = get_table_name("custom_documents", prefix)
    query = f"""
        SELECT u.id, u.name, u.email, u.company_name, u.created_at, u.last_connected,
               COALESCE(d.doc_count, 0) AS doc_count,
               COALESCE(a.agent_count, 0) AS agent_count
        FROM public.{table_name} u
        LEFT JOIN (
            SELECT owner_id, COUNT(*) AS doc_count
            FROM public.{docs_table}
            GROUP BY owner_id
        ) d ON d.owner_id = u.id
        LEFT JOIN (
            SELECT user_id, COUNT(*) AS agent_count
            FROM public.playground_bot_generator_config
            WHERE deleted_at IS NULL
            GROUP BY user_id
        ) a ON a.user_id = u.id
        ORDER BY doc_count DESC, agent_count DESC, u.email
    """
    df = execute_query(config, query)

    if not df.empty:
        if "email" in df.columns:
            # V4 may store emails in fixed-width CHAR columns padded with spaces.
            df["email"] = df["email"].astype("string").str.strip()
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], unit='s', errors='coerce')
        if 'last_connected' in df.columns:
            df['last_connected'] = pd.to_datetime(df['last_connected'], unit='s', errors='coerce')

    return df


def _load_saved_emails() -> list:
    """Load saved emails from session_state (primary) or localStorage (fallback)."""
    ss_key = "_p2_saved_user_emails"
    if ss_key in st.session_state:
        return st.session_state[ss_key]
    loaded = load_selected_users()
    if isinstance(loaded, list):
        emails = [
            e.strip()
            for e in loaded
            if isinstance(e, str) and e.strip()
        ]
        st.session_state[ss_key] = emails
        return emails
    st.session_state[ss_key] = []
    return []


def _persist_saved_emails(emails: list):
    """Persist only changed selections; avoid an iframe on unrelated reruns."""
    normalized = list(
        dict.fromkeys(
            str(email).strip()
            for email in emails
            if str(email).strip()
        )
    )
    if normalized == st.session_state.get("_p2_saved_user_emails", []):
        return False
    st.session_state["_p2_saved_user_emails"] = normalized
    save_selected_users(normalized)
    return True


def _queue_bulk_user_selection(emails: list) -> None:
    """Queue a bulk selection and invalidate stale data-editor checkbox state."""
    st.session_state["_p2_batch_saved_emails"] = list(
        dict.fromkeys(
            str(email).strip()
            for email in emails
            if str(email).strip()
        )
    )
    st.session_state["_p2_users_editor_revision"] = (
        int(st.session_state.get("_p2_users_editor_revision", 0)) + 1
    )


def _match_user_emails(
    source_emails: list,
    requested_emails: list,
) -> tuple[list, list]:
    """Match emails case-insensitively after trimming V4 CHAR padding."""
    source_lookup = {}
    for source_email in source_emails:
        canonical = str(source_email).strip()
        if canonical:
            source_lookup.setdefault(canonical.lower(), canonical)

    requested = list(
        dict.fromkeys(
            str(email).strip().lower()
            for email in requested_emails
            if str(email).strip()
        )
    )
    matched = [source_lookup[email] for email in requested if email in source_lookup]
    unmatched = [email for email in requested if email not in source_lookup]
    return matched, unmatched


def _merge_visible_selection(
    previous: list,
    visible_ids: list,
    selected_visible_ids: list,
    valid_ids: list,
) -> list:
    """Apply live editor choices without dropping rows hidden by filters."""
    valid = {str(value) for value in valid_ids}
    visible = {str(value) for value in visible_ids}
    selected_visible = {
        str(value) for value in selected_visible_ids if str(value) in visible
    }
    merged = [
        str(value)
        for value in previous
        if str(value) in valid and str(value) not in visible
    ]
    merged.extend(
        str(value)
        for value in visible_ids
        if str(value) in selected_visible
    )
    return list(dict.fromkeys(merged))


def _prioritize_selected_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Move selected rows first while preserving the active sort within groups."""
    if frame.empty or "selected" not in frame.columns:
        return frame
    return frame.sort_values(
        by="selected",
        ascending=False,
        kind="stable",
    ).reset_index(drop=True)


def render_user_selection(config: ConnectionConfig, prefix: str):
    """Render the user selection section with sorting, batch select, and file import."""
    st.subheader("👥 Select Users")

    _usk = _source_scope_key(config, prefix)
    if st.session_state.get("_p2_users_scope") != _usk:
        with st.spinner("Loading users..."):
            users_df = load_users_data(config, prefix)
        st.session_state["_p2_users_scope"] = _usk
        st.session_state["_p2_users_df"] = users_df
    else:
        users_df = st.session_state["_p2_users_df"]

    if users_df.empty:
        st.warning("No users found in the database.")
        return

    st.caption(f"Found {len(users_df)} users in `{get_table_name('users', prefix)}`")

    # Load previously selected users
    saved_emails = _load_saved_emails()

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION 1: Filters
    # ─────────────────────────────────────────────────────────────────────────
    sort_options = {
        "Documents (desc)": ("doc_count", False),
        "Agents (desc)": ("agent_count", False),
        "Email (A-Z)": ("email", True),
        "Created (newest)": ("created_at", False),
    }
    with st.expander("Filters", expanded=True):
        with st.form("user_filters_form"):
            f_col1, f_col2 = st.columns(2)
            with f_col1:
                email_filter = st.text_input(
                    "Email filter (whitelist)",
                    placeholder="e.g. @company.co.il",
                    help="Show only users whose email contains this text.",
                    key="_p2_email_whitelist",
                )
            with f_col2:
                search = st.text_input(
                    "Search",
                    placeholder="Search by name, email, or company...",
                    key="_p2_user_search",
                )
            sort_label = st.selectbox(
                "Sort by",
                list(sort_options.keys()),
                index=0,
                key="_p2_user_sort",
            )
            st.form_submit_button("Apply filters", use_container_width=True)

    filtered_df = users_df

    # Apply email whitelist filter
    if email_filter:
        filtered_df = filtered_df[
            _contains_literal(filtered_df["email"], email_filter)
        ].copy()

    # Apply search filter
    if search:
        mask = (
            _contains_literal(filtered_df["name"], search) |
            _contains_literal(filtered_df["email"], search) |
            _contains_literal(filtered_df["company_name"], search)
        )
        filtered_df = filtered_df[mask].copy()

    # Apply sort
    sort_col, sort_asc = sort_options[sort_label]
    filtered_df = filtered_df.sort_values(
        by=sort_col,
        ascending=sort_asc,
        na_position="last",
    ).reset_index(drop=True)

    if email_filter or search:
        st.caption(f"Showing {len(filtered_df)} users after filters")

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION 2: Bulk Selection Tools
    # ─────────────────────────────────────────────────────────────────────────
    with st.expander("Bulk Selection Tools", expanded=False):
        tab_batch, tab_letters, tab_paste, tab_file = st.tabs(
            ["Next Batch", "By Letter", "Paste Emails", "Import File"]
        )

        with tab_batch:
            b_col1, b_col2 = st.columns([2, 3])
            with b_col1:
                batch_size = st.number_input(
                    "Batch size", min_value=1, value=get_env_batch_size(), step=10, key="_p2_batch_size"
                )
            with b_col2:
                st.write("")
                if st.button("Select Next Batch", type="primary"):
                    try:
                        already_migrated = _get_already_migrated_emails()
                    except RuntimeError as exc:
                        st.error(str(exc))
                        st.stop()
                    new_selection = []
                    count = 0
                    for email in filtered_df["email"].tolist():
                        if count >= int(batch_size):
                            break
                        normalized_email = str(email).strip().lower()
                        if normalized_email not in already_migrated:
                            new_selection.append(email)
                            count += 1
                    _queue_bulk_user_selection(new_selection)
                    st.rerun()

        with tab_letters:
            letter_col1, letter_col2 = st.columns([2, 3])
            with letter_col1:
                letter_field_label = st.radio(
                    "Group and sort by",
                    ["Email", "Name"],
                    horizontal=True,
                    key="_p2_letter_batch_field",
                )
            letter_field = letter_field_label.lower()
            letter_records = filtered_df[["name", "email"]].to_dict("records")
            bucket_counts = user_bucket_counts(letter_records, letter_field)
            bucket_options = list(bucket_counts)
            with letter_col2:
                selected_buckets = st.multiselect(
                    "Starting letters or digits",
                    options=bucket_options,
                    format_func=lambda bucket: (
                        f"{bucket} ({bucket_counts[bucket]:,})"
                    ),
                    key=f"_p2_letter_batch_buckets_{letter_field}",
                    help=(
                        "Choose one or more first-character buckets. Email "
                        "buckets use the local-part before @."
                    ),
                )

            size_col, action_col = st.columns([2, 3])
            with size_col:
                letter_batch_size = st.number_input(
                    "Maximum users in this letter batch",
                    min_value=1,
                    value=get_env_batch_size(),
                    step=10,
                    key="_p2_letter_batch_size",
                    help=(
                        "Large letter or digit groups are split into manageable "
                        "batches. Already completed users are skipped."
                    ),
                )
            matching_count = sum(
                bucket_counts.get(bucket, 0) for bucket in selected_buckets
            )
            with action_col:
                st.write("")
                st.caption(
                    f"{matching_count:,} user(s) match the selected buckets "
                    "within the active filters."
                )
                if st.button(
                    "Select Letter Batch",
                    type="primary",
                    disabled=not selected_buckets,
                    key="_p2_select_letter_batch",
                ):
                    try:
                        already_migrated = _get_already_migrated_emails()
                    except RuntimeError as exc:
                        st.error(str(exc))
                        st.stop()
                    new_selection = select_user_letter_batch(
                        letter_records,
                        field=letter_field,
                        buckets=selected_buckets,
                        limit=int(letter_batch_size),
                        excluded_emails=already_migrated,
                    )
                    _queue_bulk_user_selection(new_selection)
                    if new_selection:
                        st.session_state["_p2_bulk_selection_notice"] = (
                            f"Selected {len(new_selection)} user"
                            f"{'s' if len(new_selection) != 1 else ''} by "
                            f"{letter_field} bucket."
                        )
                    else:
                        st.session_state["_p2_bulk_selection_warning"] = (
                            "No unmigrated users matched the selected buckets."
                        )
                    st.rerun()

            st.caption(
                "Letter matching is case-insensitive. Numeric email local-parts "
                "are grouped by first digit and sorted numerically within each "
                "digit bucket. Switching Email/Name does not change the current "
                "selection until you click Select Letter Batch."
            )

        with tab_paste:
            pasted = st.text_area(
                "Paste emails (one per line or comma-separated)",
                placeholder="user1@example.com\nuser2@example.com",
                height=120,
                key="_p2_paste_emails",
            )
            if st.button("Apply Pasted Emails"):
                if pasted.strip():
                    raw_emails = [e.strip().lower() for e in pasted.replace(",", "\n").split("\n") if e.strip()]
                    matched, unmatched = _match_user_emails(
                        users_df["email"].tolist(),
                        raw_emails,
                    )
                    saved_emails = list(dict.fromkeys(saved_emails + matched))
                    _queue_bulk_user_selection(saved_emails)
                    st.session_state["_p2_bulk_selection_notice"] = (
                        f"Selected {len(matched)} pasted email"
                        f"{'s' if len(matched) != 1 else ''}."
                    )
                    if unmatched:
                        st.session_state["_p2_bulk_selection_warning"] = (
                            f"{len(unmatched)} emails not found in source DB: "
                            f"{', '.join(unmatched[:5])}"
                            f"{'...' if len(unmatched) > 5 else ''}"
                        )
                    st.rerun()

        with tab_file:
            uploaded_file = st.file_uploader(
                "Upload CSV/Excel with an 'email' column",
                type=["csv", "xlsx"],
                key="_p2_user_upload",
            )
            if uploaded_file is not None:
                upload_fingerprint = hashlib.sha256(
                    uploaded_file.getvalue()
                ).hexdigest()
                upload_result = st.session_state.get("_p2_user_upload_result")
                if (
                    not isinstance(upload_result, dict)
                    or upload_result.get("fingerprint") != upload_fingerprint
                ):
                    try:
                        if uploaded_file.name.endswith(".xlsx"):
                            import openpyxl  # noqa: F401
                            upload_df = pd.read_excel(uploaded_file)
                        else:
                            upload_df = pd.read_csv(uploaded_file)
                        col_map = {c.lower().strip(): c for c in upload_df.columns}
                        if "email" not in col_map:
                            upload_result = {
                                "fingerprint": upload_fingerprint,
                                "error": "File must contain an 'email' column.",
                            }
                        else:
                            uploaded_emails = (
                                upload_df[col_map["email"]]
                                .dropna()
                                .astype(str)
                                .str.strip()
                                .str.lower()
                                .tolist()
                            )
                            matched, unmatched = _match_user_emails(
                                users_df["email"].tolist(),
                                uploaded_emails,
                            )
                            saved_emails = list(
                                dict.fromkeys(saved_emails + matched)
                            )
                            _queue_bulk_user_selection(saved_emails)
                            upload_result = {
                                "fingerprint": upload_fingerprint,
                                "matched": matched,
                                "unmatched": unmatched,
                                "total": len(uploaded_emails),
                            }
                    except Exception as e:
                        upload_result = {
                            "fingerprint": upload_fingerprint,
                            "error": f"Error reading file: {e}",
                        }
                    st.session_state["_p2_user_upload_result"] = upload_result

                if upload_result.get("error"):
                    st.error(upload_result["error"])
                else:
                    matched = upload_result["matched"]
                    unmatched = upload_result["unmatched"]
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Matched", len(matched))
                    c2.metric("Unmatched", len(unmatched))
                    c3.metric("Total in file", upload_result["total"])
                    if unmatched:
                        with st.expander("Unmatched emails"):
                            st.write(unmatched)
            else:
                st.session_state.pop("_p2_user_upload_result", None)

    # ─────────────────────────────────────────────────────────────────────────
    # Apply pending batch selection
    # ─────────────────────────────────────────────────────────────────────────
    if "_p2_batch_saved_emails" in st.session_state:
        saved_emails = st.session_state.pop("_p2_batch_saved_emails")
        _persist_saved_emails(saved_emails)
    if "_p2_bulk_selection_notice" in st.session_state:
        st.success(st.session_state.pop("_p2_bulk_selection_notice"))
    if "_p2_bulk_selection_warning" in st.session_state:
        st.warning(st.session_state.pop("_p2_bulk_selection_warning"))

    # ─────────────────────────────────────────────────────────────────────────
    # SECTION 3: User Table
    # ─────────────────────────────────────────────────────────────────────────
    display_cols = ["selected", "name", "email", "company_name", "doc_count", "agent_count", "created_at", "last_connected"]
    filtered_df["selected"] = filtered_df["email"].isin(saved_emails)
    filtered_df = _prioritize_selected_rows(filtered_df[display_cols])
    select_all = st.checkbox(
        "Select all filtered users",
        value=False,
        key="_p2_select_all_users",
    )
    edited_df = st.data_editor(
        filtered_df,
        column_config={
            "selected": st.column_config.CheckboxColumn("Select", help="Select users to migrate", default=False),
            "name": st.column_config.TextColumn("Name"),
            "email": st.column_config.TextColumn("Email"),
            "company_name": st.column_config.TextColumn("Company"),
            "doc_count": st.column_config.NumberColumn("Docs", help="Number of documents owned"),
            "agent_count": st.column_config.NumberColumn("Agents", help="Number of active agents"),
            "created_at": st.column_config.DatetimeColumn("Created", format="YYYY-MM-DD"),
            "last_connected": st.column_config.DatetimeColumn("Last Connected", format="YYYY-MM-DD"),
        },
        hide_index=True,
        use_container_width=True,
        height=400,
        key=(
            f"_p2_users_editor_"
            f"{int(st.session_state.get('_p2_users_editor_revision', 0))}"
        ),
        disabled=[column for column in display_cols if column != "selected"],
    )

    # Selection editors are deliberately outside forms: every visible checkbox
    # change becomes canonical before an extraction button can be handled.
    visible_emails = filtered_df["email"].astype(str).tolist()
    selected_visible = (
        visible_emails
        if select_all
        else edited_df[edited_df["selected"] == True]["email"].astype(str).tolist()
    )
    selected_emails = _merge_visible_selection(
        saved_emails,
        visible_emails,
        selected_visible,
        users_df["email"].astype(str).tolist(),
    )
    if selected_emails != saved_emails:
        # Recreate the editor before reordering rows so its positional widget
        # state cannot apply a checkbox value to a different user.
        _queue_bulk_user_selection(selected_emails)
        st.rerun()
    _persist_saved_emails(selected_emails)

    st.session_state[SessionKeys.SELECTED_USERS] = selected_emails
    selected_user_ids = users_df[users_df["email"].isin(selected_emails)]["id"].tolist()
    st.session_state[SessionKeys.SELECTED_USER_IDS] = selected_user_ids
    st.session_state["_p2_selected_user_email_by_id"] = {
        str(row["id"]): str(row["email"]).strip()
        for _, row in users_df[
            users_df["email"].isin(selected_emails)
        ][["id", "email"]].iterrows()
    }

    st.metric("Selected Users", len(selected_emails))

    return selected_emails, selected_user_ids

def render_document_filters(config: ConnectionConfig, prefix: str, user_ids: list):
    """Render document filter section."""
    if not user_ids:
        st.info("Select users above to filter documents.")
        return None, None, None
    
    # Load saved filters
    saved_filters = load_document_filters()
    if not isinstance(saved_filters, dict):
        saved_filters = {}
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        date_from = st.date_input(
            "Created After",
            value=saved_filters.get("date_from"),
            key="doc_date_from"
        )
    
    with col2:
        date_to = st.date_input(
            "Created Before",
            value=saved_filters.get("date_to"),
            key="doc_date_to"
        )
    
    with col3:
        max_size = st.number_input(
            "Max Document Size (bytes)",
            value=saved_filters.get("max_size", 0),
            min_value=0,
            step=1000000,
            help="0 = no limit"
        )
    
    # Convert date to datetime if set
    date_from_dt = datetime.combine(date_from, datetime.min.time()) if date_from else None
    date_to_dt = datetime.combine(date_to, datetime.max.time()) if date_to else None
    max_size_val = max_size if max_size > 0 else None

    _filters_snap = {"date_from": date_from_dt, "date_to": date_to_dt, "max_size": max_size_val}
    # Get preview count (cached — avoids re-query on unrelated widget changes e.g. org select)
    _fp = _source_scope_key(config, prefix)
    _uids = tuple(sorted(str(u) for u in user_ids))
    _dcc_key = f"{_fp}|{_uids}|{_filters_fingerprint(_filters_snap)}"
    if st.session_state.get("_p2_doccount_key") == _dcc_key:
        doc_count = st.session_state["_p2_doccount_val"]
    else:
        with st.spinner("Counting matching documents..."):
            doc_count = get_document_count_preview(
                config, prefix, user_ids,
                date_from_dt, date_to_dt, max_size_val
            )
        st.session_state["_p2_doccount_key"] = _dcc_key
        st.session_state["_p2_doccount_val"] = doc_count
    
    st.metric("📝 Matching Documents", f"{doc_count:,}")
    
    # Save filters
    if st.button("💾 Save Filters", type="secondary", key="save_filters"):
        filters = {
            "date_from": str(date_from) if date_from else None,
            "date_to": str(date_to) if date_to else None,
            "max_size": max_size
        }
        save_document_filters(filters)
        st.success("Filters saved!")
    
    # Store in session state
    st.session_state[SessionKeys.DOCUMENT_FILTERS] = {
        "date_from": date_from_dt,
        "date_to": date_to_dt,
        "max_size": max_size_val
    }
    
    return date_from_dt, date_to_dt, max_size_val


def _load_documents_df(config: ConnectionConfig, prefix: str, user_ids: list, filters: dict) -> pd.DataFrame:
    """Load documents for selected users + current filters."""
    if not user_ids:
        return pd.DataFrame()
    doc_table = get_table_name("custom_documents", prefix)
    embeddings_table = get_table_name("embeddings", prefix)
    placeholders = ", ".join(["%s"] * len(user_ids))
    query = f"""
        SELECT d.doc_id, d.owner_id, d.doc_title, d.doc_name_origin, d.doc_size,
               d.created_at, d.folder_id, d.doc_type,
               EXISTS (
                   SELECT 1
                   FROM public.{embeddings_table} e
                   WHERE e.metadata->>'doc_id' = d.doc_id
                     AND e.metadata->>'type' = 'chunk-data'
               ) AS has_chunks
        FROM public.{doc_table} d
        WHERE d.owner_id IN ({placeholders})
          AND COALESCE(d.blob_source, '') <> %s
    """
    params = list(user_ids) + [SHAREPOINT_DOCUMENT_BLOB_SOURCE]
    if filters.get("date_from"):
        query += " AND d.created_at >= %s"
        params.append(filters["date_from"])
    if filters.get("date_to"):
        query += " AND d.created_at <= %s"
        params.append(filters["date_to"])
    if filters.get("max_size"):
        query += " AND d.doc_size <= %s"
        params.append(filters["max_size"])
    query += " ORDER BY d.created_at DESC"
    df = execute_query(config, query, tuple(params))
    if not df.empty and "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], unit="s", errors="coerce")
    return df

def render_document_selection(config: ConnectionConfig, prefix: str, user_ids: list):
    """Render document filters + selectable documents list (default all selected)."""
    st.markdown("---")
    st.subheader("📚 Documents (Filters + Selection)")
    if not user_ids:
        st.info("Select users first.")
        st.session_state["selected_doc_ids"] = []
        return []
    # Keep all filters above the table
    render_document_filters(config, prefix, user_ids)
    filters = st.session_state.get(SessionKeys.DOCUMENT_FILTERS, {})
    _dk = f"{_source_scope_key(config, prefix)}|{tuple(sorted(str(u) for u in user_ids))}|{_filters_fingerprint(filters)}"
    if st.session_state.get("_p2_docs_df_key") == _dk:
        docs_df = st.session_state["_p2_docs_df"]
    else:
        with st.spinner("Loading documents..."):
            docs_df = _load_documents_df(config, prefix, user_ids, filters)
        st.session_state["_p2_docs_df_key"] = _dk
        st.session_state["_p2_docs_df"] = docs_df.copy()
    if docs_df.empty:
        st.info("No matching documents found.")
        st.session_state["selected_doc_ids"] = []
        return []
    chunkless_scope = (
        f"{_source_scope_key(config, prefix)}|"
        f"{tuple(sorted(str(u) for u in user_ids))}"
    )
    if st.session_state.get("_chunkless_policy_scope") != chunkless_scope:
        st.session_state["_chunkless_policy_scope"] = chunkless_scope
        st.session_state["include_chunkless_documents"] = False
    include_chunkless_documents = st.checkbox(
        "Include documents without chunks",
        value=False,
        key="include_chunkless_documents",
        help=(
            "Off by default. Chunkless documents contain no usable content in "
            "the migration and agent references to them will be removed."
        ),
    )
    st.caption("SharePoint-backed documents are always excluded.")
    chunkless_count = (
        int((~docs_df["has_chunks"].fillna(False).astype(bool)).sum())
        if "has_chunks" in docs_df.columns
        else 0
    )
    if chunkless_count and not include_chunkless_documents:
        st.warning(
            f"{chunkless_count} chunkless document(s) are excluded by default."
        )
        docs_df = docs_df[docs_df["has_chunks"].fillna(False).astype(bool)].copy()
    st.caption(f"Found {len(docs_df)} migratable documents")
    if docs_df.empty:
        st.info("No documents with chunks match the current selection.")
        st.session_state["selected_doc_ids"] = []
        return []
    if not st.toggle(
        "Customize document selection",
        value=False,
        key="_p2_show_document_details",
        help="All matching documents are selected by default.",
        on_change=_activate_dependency_detail,
        args=("_p2_show_document_details",),
    ):
        selected_doc_ids = docs_df["doc_id"].astype(str).tolist()
        st.session_state["selected_doc_ids"] = selected_doc_ids
        st.caption(
            f"All {len(selected_doc_ids):,} matching documents will migrate. "
            "Load details only when individual exclusions are needed."
        )
        return selected_doc_ids
    all_doc_ids = docs_df["doc_id"].astype(str).tolist()
    previous = st.session_state.get("selected_doc_ids")
    if not isinstance(previous, list):
        previous = all_doc_ids
    previous = [str(doc_id) for doc_id in previous if str(doc_id) in set(all_doc_ids)]

    owner_options = sorted(docs_df["owner_id"].dropna().astype(str).unique().tolist())
    selected_owners = st.multiselect(
        "Filter by owner",
        options=owner_options,
        default=owner_options,
        key="doc_owner_filter",
    )
    search = st.text_input(
        "🔍 Search documents",
        placeholder="Search by doc id/title/name...",
        key="doc_search",
    )
    filtered_df = docs_df.copy()
    if selected_owners:
        filtered_df = filtered_df[
            filtered_df["owner_id"].astype(str).isin(selected_owners)
        ]
    if search:
        mask = (
            _contains_literal(filtered_df["doc_id"], search)
            | _contains_literal(filtered_df["doc_title"], search)
            | _contains_literal(filtered_df["doc_name_origin"], search)
        )
        filtered_df = filtered_df[mask]
    select_all_docs = st.checkbox(
        "Select all documents in current list",
        value=True,
        key="select_all_docs",
    )
    if select_all_docs:
        filtered_df["selected"] = True
    else:
        filtered_df["selected"] = filtered_df["doc_id"].astype(str).isin(previous)
    filtered_df["doc_name"] = filtered_df["doc_title"].fillna("").where(
        filtered_df["doc_title"].str.strip().ne(""),
        filtered_df["doc_name_origin"],
    )
    display_columns = [
        "selected", "doc_name", "doc_id", "owner_id", "doc_size",
        "created_at", "folder_id", "doc_type",
    ]
    if include_chunkless_documents and "has_chunks" in filtered_df.columns:
        display_columns.append("has_chunks")
    filtered_df = filtered_df[display_columns]
    edited_df = st.data_editor(
        filtered_df,
        hide_index=True,
        use_container_width=True,
        height=350,
        column_config={
            "selected": st.column_config.CheckboxColumn("Select", default=True),
            "doc_name": st.column_config.TextColumn("Name"),
            "created_at": st.column_config.DatetimeColumn("Created", format="YYYY-MM-DD"),
        },
        key="documents_editor",
        disabled=[column for column in display_columns if column != "selected"],
    )
    visible_doc_ids = filtered_df["doc_id"].astype(str).tolist()
    selected_visible = (
        visible_doc_ids
        if select_all_docs
        else edited_df[edited_df["selected"] == True]["doc_id"].astype(str).tolist()
    )
    selected_doc_ids = _merge_visible_selection(
        previous,
        visible_doc_ids,
        selected_visible,
        all_doc_ids,
    )
    st.session_state["selected_doc_ids"] = selected_doc_ids
    st.metric("Selected Documents", len(selected_doc_ids))
    return selected_doc_ids

def _extract_doc_id_from_metadata(value):
    if isinstance(value, dict):
        return value.get("doc_id")
    if isinstance(value, str):
        try:
            obj = json.loads(value)
            if isinstance(obj, dict):
                return obj.get("doc_id")
        except Exception:
            return None
    return None

@st.fragment
def render_embeddings_selection(config: ConnectionConfig, prefix: str, doc_ids: list):
    """Preview chunk dependencies without turning the preview cap into a data cap."""
    st.markdown("---")
    st.subheader("🧮 Select Embeddings")
    if not doc_ids:
        st.info("No selected documents, so no embeddings to select.")
        st.session_state["selected_embedding_ids"] = None
        return None
    embeddings_table = get_table_name("embeddings", prefix)
    placeholders = ", ".join(["%s"] * len(doc_ids))
    count_query = f"""
        SELECT
            COUNT(*) FILTER (WHERE metadata->>'type' = 'chunk-data') AS valid_count,
            COUNT(*) FILTER (
                WHERE metadata->>'type' = 'chunk-data' AND embeddings IS NULL
            ) AS empty_count,
            COUNT(DISTINCT metadata->>'doc_id') FILTER (
                WHERE metadata->>'type' = 'chunk-data'
            ) AS document_count
        FROM public.{embeddings_table}
        WHERE metadata->>'doc_id' IN ({placeholders})
    """
    _ek = f"{_source_scope_key(config, prefix)}|{tuple(sorted(str(d) for d in doc_ids))}"
    if st.session_state.get("_p2_emb_count_key") == _ek:
        valid_count, empty_count, covered_documents = st.session_state["_p2_emb_counts"]
    else:
        counts = execute_query(config, count_query, tuple(doc_ids))
        valid_count = int(counts.iloc[0]["valid_count"] or 0) if not counts.empty else 0
        empty_count = int(counts.iloc[0]["empty_count"] or 0) if not counts.empty else 0
        covered_documents = int(counts.iloc[0]["document_count"] or 0) if not counts.empty else 0
        st.session_state["_p2_emb_count_key"] = _ek
        st.session_state["_p2_emb_counts"] = (
            valid_count,
            empty_count,
            covered_documents,
        )
    st.caption(
        f"{valid_count:,} valid chunk row(s) across {covered_documents:,} document(s); "
        f"{empty_count:,} row(s) have no vector."
    )
    st.session_state["selected_embedding_ids"] = None
    if valid_count == 0:
        st.info("No embeddings found for selected documents.")
        return None
    if not st.toggle(
        "Load chunk preview",
        value=False,
        key="_p2_show_embedding_details",
        help="Migration always includes every chunk for selected documents.",
        on_change=_activate_dependency_detail,
        args=("_p2_show_embedding_details",),
    ):
        st.caption("The bounded row preview is loaded only on request.")
        return None

    query = f"""
        SELECT id, external_id, collection, metadata
        FROM public.{embeddings_table}
        WHERE metadata->>'doc_id' IN ({placeholders})
          AND metadata->>'type' = 'chunk-data'
        ORDER BY id
        LIMIT {DEPENDENCY_PREVIEW_LIMIT}
    """
    if st.session_state.get("_p2_emb_df_key") == _ek:
        emb_df = st.session_state["_p2_emb_df"]
    else:
        with st.spinner("Loading embeddings..."):
            emb_df = execute_query(config, query, tuple(doc_ids))
        st.session_state["_p2_emb_df_key"] = _ek
        st.session_state["_p2_emb_df"] = emb_df.copy()
    if emb_df.empty:
        st.info("No embeddings found for selected documents.")
        st.session_state["selected_embedding_ids"] = None
        return None
    if valid_count > DEPENDENCY_PREVIEW_LIMIT:
        st.info(
            f"Showing the first {DEPENDENCY_PREVIEW_LIMIT:,} rows only. "
            f"Complete extraction still migrates all {valid_count:,} rows."
        )
    emb_df["doc_id"] = emb_df["metadata"].apply(_extract_doc_id_from_metadata)
    search = st.text_input("🔍 Search embeddings", placeholder="Search by id/external_id/collection/doc_id...", key="emb_search")
    filtered_df = emb_df.copy()
    if search:
        mask = (
            _contains_literal(filtered_df["id"], search)
            | _contains_literal(filtered_df["external_id"], search)
            | _contains_literal(filtered_df["collection"], search)
            | _contains_literal(filtered_df["doc_id"], search)
        )
        filtered_df = filtered_df[mask]
    st.caption(
        "This table is preview-only. Migration always includes every chunk row "
        "for the selected documents."
    )
    st.dataframe(
        filtered_df[["id", "external_id", "collection", "doc_id"]],
        hide_index=True,
        use_container_width=True,
        height=320,
    )
    # None explicitly means complete extraction by document IDs. Never retain a
    # stale list from a prior Streamlit session, which could cap migration at
    # the bounded preview.
    st.session_state["selected_embedding_ids"] = None
    st.metric("Chunks to migrate", valid_count)
    return None

@st.fragment
def render_conversations_selection(config: ConnectionConfig, prefix: str, user_ids: list):
    """Render conversation-level filters and an optional unique-chat preview."""
    st.markdown("---")
    st.subheader("💬 Select Conversations")
    if not user_ids:
        st.info("No selected users, so no conversations to select.")
        st.session_state["selected_conversation_chat_ids"] = []
        st.session_state["_p2_conv_scope"] = {
            "conversation_count": 0,
            "log_row_count": 0,
            "per_user": {},
        }
        return []

    logs_table = get_table_name("logs", prefix)

    # ── Filters ──────────────────────────────────────────────────────────────
    if "conv_date_from" not in st.session_state:
        st.session_state["conv_date_from"] = (date.today() - timedelta(days=90)).strftime("%Y-%m-%d")
    if "conv_date_to" not in st.session_state:
        st.session_state["conv_date_to"] = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")

    with st.form("conversation_filters_form"):
        col1, col2, col3 = st.columns(3)
        with col1:
            conv_date_from_raw = st.text_input(
                "Created After",
                key="conv_date_from",
                placeholder="YYYY-MM-DD",
                help="Only migrate conversations created on or after this date.",
            )
        with col2:
            conv_date_to_raw = st.text_input(
                "Created Before",
                key="conv_date_to",
                placeholder="YYYY-MM-DD",
                help="Only migrate conversations created on or before this date.",
            )
        with col3:
            conv_max_per_user = st.number_input(
                "Max per user (0 = no limit)",
                min_value=0,
                value=0,
                step=100,
                key="conv_max_per_user",
                help="Keep only the N most recently created unique conversations per user.",
            )
        st.form_submit_button("Apply conversation filters", use_container_width=True)

    def _parse_conv_date(raw: str, end_of_day: bool = False):
        raw = (raw or "").strip()
        if not raw:
            return None
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
            try:
                d = datetime.strptime(raw, fmt)
                return d.replace(hour=23, minute=59, second=59) if end_of_day else d
            except ValueError:
                continue
        st.warning(f"⚠️ Could not parse date '{raw}' — use YYYY-MM-DD format.")
        return None

    conv_date_from_dt = _parse_conv_date(conv_date_from_raw, end_of_day=False)
    conv_date_to_dt = _parse_conv_date(conv_date_to_raw, end_of_day=True)
    conv_max_per_user_val = int(conv_max_per_user) if conv_max_per_user and conv_max_per_user > 0 else None

    # Store parsed filter values under separate keys (widget keys are reserved by Streamlit)
    st.session_state["conv_date_from_parsed"] = conv_date_from_dt
    st.session_state["conv_date_to_parsed"] = conv_date_to_dt
    st.session_state["conv_max_per_user_parsed"] = conv_max_per_user_val

    # ── Count query (fast — no data transfer) ────────────────────────────────
    _ck = (
        f"{_source_scope_key(config, prefix)}"
        f"|{tuple(sorted(str(u) for u in user_ids))}"
        f"|{conv_date_from_raw}|{conv_date_to_raw}|{conv_max_per_user}"
    )
    if st.session_state.get("_p2_conv_count_key") == _ck:
        scope_rows = st.session_state.get("_p2_conv_scope_rows", [])
    else:
        scope_cte, scope_params = build_conversation_scope_cte(
            logs_table,
            user_ids,
            date_from=conv_date_from_dt,
            date_to=conv_date_to_dt,
            max_per_user=conv_max_per_user_val,
        )
        count_df = execute_query(
            config,
            f"""
            {scope_cte}
            SELECT
                user_id,
                COUNT(*)::bigint AS conversation_count,
                COALESCE(SUM(log_row_count), 0)::bigint AS log_row_count
            FROM selected_conversations
            GROUP BY user_id
            ORDER BY user_id
            """,
            scope_params,
        )
        scope_rows = [
            {
                "user_id": str(row["user_id"]),
                "conversation_count": int(row["conversation_count"]),
                "log_row_count": int(row["log_row_count"]),
            }
            for _, row in count_df.iterrows()
        ]
        st.session_state["_p2_conv_count_key"] = _ck
        st.session_state["_p2_conv_scope_rows"] = scope_rows

    total_count = sum(row["conversation_count"] for row in scope_rows)
    total_log_rows = sum(row["log_row_count"] for row in scope_rows)
    scope = {
        "conversation_count": total_count,
        "log_row_count": total_log_rows,
        "date_from": conv_date_from_raw,
        "date_to": conv_date_to_raw,
        "max_per_user": conv_max_per_user_val,
        "per_user": {
            row["user_id"]: {
                "conversation_count": row["conversation_count"],
                "log_row_count": row["log_row_count"],
            }
            for row in scope_rows
        },
    }
    st.session_state["_p2_conv_scope"] = scope

    mc1, mc2, mc3 = st.columns(3)
    mc1.metric(
        "📊 Matching Conversations",
        f"{total_count:,}",
        help="Unique valid chat IDs matching the current conversation-level filters.",
    )
    mc2.metric(
        "💬 Source Turns",
        f"{total_log_rows:,}",
        help="V4 log rows inside the matching conversations.",
    )
    if total_count > 5000:
        mc3.info("Preview is capped at 5,000 conversations; extraction is not capped.")
    if total_count == 0:
        st.info("No conversations found for selected users with current filters.")
        st.session_state["selected_conversation_chat_ids"] = []
        return []
    if not st.toggle(
        "Load conversation preview",
        value=False,
        key="_p2_show_conversation_details",
        help="The migration count above does not depend on loading preview rows.",
        on_change=_activate_dependency_detail,
        args=("_p2_show_conversation_details",),
    ):
        st.session_state["selected_conversation_chat_ids"] = None
        return None

    if st.session_state.get("_p2_conv_df_key") == _ck:
        convs_df = st.session_state["_p2_conv_df"]
    else:
        scope_cte, scope_params = build_conversation_scope_cte(
            logs_table,
            user_ids,
            date_from=conv_date_from_dt,
            date_to=conv_date_to_dt,
            max_per_user=conv_max_per_user_val,
        )
        query = f"""
            {scope_cte}
            SELECT
                user_id,
                chat_id,
                conversation_created_at AS created_at,
                conversation_updated_at AS last_interacted_at,
                log_row_count
            FROM selected_conversations
            ORDER BY conversation_created_at DESC, chat_id
            LIMIT 5000
        """

        with st.spinner("Loading conversations..."):
            convs_df = execute_query(config, query, scope_params)
        st.session_state["_p2_conv_df_key"] = _ck
        st.session_state["_p2_conv_df"] = convs_df.copy()

    if not convs_df.empty and "created_at" in convs_df.columns:
        convs_df["created_at"] = pd.to_datetime(
            convs_df["created_at"], errors="coerce"
        )
        convs_df["last_interacted_at"] = pd.to_datetime(
            convs_df["last_interacted_at"], errors="coerce"
        )

    if convs_df.empty:
        st.info("No conversations found for selected users with current filters.")
        st.session_state["selected_conversation_chat_ids"] = []
        return []

    search = st.text_input(
        "🔍 Search conversations",
        placeholder="Search by chat ID or user ID...",
        key="conv_search",
    )
    filtered_df = convs_df.copy()
    if search:
        mask = (
            _contains_literal(filtered_df["user_id"], search)
            | _contains_literal(filtered_df["chat_id"], search)
        )
        filtered_df = filtered_df[mask]
    select_all_convs = st.checkbox(
        "Migrate all conversations matching these filters",
        value=True,
        key="select_all_convs",
    )
    previous = st.session_state.get("selected_conversation_chat_ids")
    if select_all_convs:
        filtered_df["selected"] = True
    else:
        if isinstance(previous, list):
            filtered_df["selected"] = filtered_df["chat_id"].isin(previous)
        else:
            filtered_df["selected"] = True
    filtered_df = filtered_df[[
        "selected",
        "user_id",
        "chat_id",
        "log_row_count",
        "created_at",
        "last_interacted_at",
    ]]
    edited_df = st.data_editor(
        filtered_df,
        hide_index=True,
        use_container_width=True,
        height=320,
        column_config={
            "created_at": st.column_config.DatetimeColumn("Created", format="YYYY-MM-DD"),
            "last_interacted_at": st.column_config.DatetimeColumn(
                "Last activity", format="YYYY-MM-DD"
            ),
            "chat_id": st.column_config.TextColumn("Chat ID"),
            "log_row_count": st.column_config.NumberColumn("Source turns"),
        },
        key=f"conversations_editor_{uuid.uuid5(uuid.NAMESPACE_URL, _ck)}",
    )
    if select_all_convs:
        st.session_state["selected_conversation_chat_ids"] = None
        st.metric("Selected Conversations", total_count)
        return None

    selected_rows = edited_df[edited_df["selected"] == True]
    selected_chat_ids = selected_rows["chat_id"].astype(str).tolist()
    st.session_state["selected_conversation_chat_ids"] = selected_chat_ids
    selected_per_user = {}
    for user_id, rows in selected_rows.groupby("user_id"):
        selected_per_user[str(user_id)] = {
            "conversation_count": len(rows),
            "log_row_count": int(rows["log_row_count"].sum()),
        }
    st.session_state["_p2_conv_scope"] = {
        **scope,
        "conversation_count": len(selected_chat_ids),
        "log_row_count": int(selected_rows["log_row_count"].sum()),
        "per_user": selected_per_user,
        "explicit_selection": True,
    }
    st.metric("Selected Conversations", len(selected_chat_ids))
    return selected_chat_ids


@st.fragment
def render_agents_selection(config: ConnectionConfig, prefix: str, user_ids: list):
    """Preview agents without allowing the preview limit to cap extraction."""
    st.markdown("---")
    st.subheader("🤖 Select Agents")
    if not user_ids:
        st.info("No selected users, so no agents to select.")
        st.session_state["selected_agent_ids"] = []
        return []
    agents_table = get_table_name("agents", prefix)
    placeholders = ", ".join(["%s"] * len(user_ids))
    count_query = f"""
        SELECT
            COUNT(*) AS total_count,
            COUNT(*) FILTER (
                WHERE COALESCE(toolkit_settings->>'is_active', 'Yes') = 'Yes'
            ) AS active_count
        FROM public.{agents_table}
        WHERE user_id IN ({placeholders})
          AND deleted_at IS NULL
    """
    _ak = f"{_source_scope_key(config, prefix)}|{tuple(sorted(str(u) for u in user_ids))}"
    if st.session_state.get("_p2_agents_count_key") == _ak:
        total_count, active_count = st.session_state["_p2_agents_counts"]
    else:
        counts = execute_query(config, count_query, tuple(user_ids))
        total_count = int(counts.iloc[0]["total_count"] or 0) if not counts.empty else 0
        active_count = int(counts.iloc[0]["active_count"] or 0) if not counts.empty else 0
        st.session_state["_p2_agents_count_key"] = _ak
        st.session_state["_p2_agents_counts"] = (total_count, active_count)
    st.caption(
        f"{total_count:,} agent(s): {active_count:,} active and "
        f"{total_count - active_count:,} inactive."
    )
    if total_count == 0:
        st.info("No agents found for selected users.")
        st.session_state["selected_agent_ids"] = []
        return []
    if not st.toggle(
        "Customize agent selection",
        value=False,
        key="_p2_show_agent_details",
        help="All agents are migrated by default; the table is only a bounded preview.",
        on_change=_activate_dependency_detail,
        args=("_p2_show_agent_details",),
    ):
        st.session_state["selected_agent_ids"] = None
        return None
    query = f"""
        SELECT bot_id, user_id, folder_id, created_at,
               COALESCE(NULLIF(bot_data->>'bot_name', ''), NULLIF(bot_data->>'botName', ''), '') AS agent_name,
               COALESCE(toolkit_settings->>'is_active', 'Yes') = 'Yes' AS is_active,
               COALESCE(array_length(docs_chosen, 1), 0) AS docs,
               COALESCE(array_length(chosen_docs_folders, 1), 0) AS folders,
               array_to_string(docs_chosen, ', ') AS doc_ids,
               (lower(trim(character_prompts->>'model')) = 'workflow'
                AND COALESCE(jsonb_array_length(character_prompts->'agentFlow'->'flows'), 0) > 0)
                   AS uses_workflow,
               character_prompts->'agentFlow'->'flows'->0->>'id' AS flow_id
        FROM public.{agents_table}
        WHERE user_id IN ({placeholders})
          AND deleted_at IS NULL
        ORDER BY created_at DESC
        LIMIT {DEPENDENCY_PREVIEW_LIMIT}
    """
    if st.session_state.get("_p2_agents_df_key") == _ak:
        agents_df = st.session_state["_p2_agents_df"]
    else:
        with st.spinner("Loading agents..."):
            agents_df = execute_query(config, query, tuple(user_ids))
        st.session_state["_p2_agents_df_key"] = _ak
        st.session_state["_p2_agents_df"] = agents_df.copy()
    if not agents_df.empty and "created_at" in agents_df.columns:
        agents_df["created_at"] = pd.to_datetime(agents_df["created_at"], unit="s", errors="coerce")
    if agents_df.empty:
        st.info("No agents found for selected users.")
        st.session_state["selected_agent_ids"] = []
        return []
    _wf_total = int(agents_df["uses_workflow"].sum()) if "uses_workflow" in agents_df.columns else 0
    st.caption(f"{_wf_total} in the preview reference a Langflow workflow.")
    if total_count > DEPENDENCY_PREVIEW_LIMIT:
        st.info(
            f"Showing the first {DEPENDENCY_PREVIEW_LIMIT:,} agents only. "
            f"Complete extraction still migrates all {total_count:,} agents."
        )
    search = st.text_input("🔍 Search agents", placeholder="Search by name/bot_id/user_id/folder_id...", key="agent_search")
    only_workflow = st.checkbox("🔀 Show only Langflow-workflow agents", value=False, key="agent_workflow_only")
    filtered_df = agents_df.copy()
    if search:
        mask = (
            _contains_literal(filtered_df["agent_name"], search)
            | _contains_literal(filtered_df["bot_id"], search)
            | _contains_literal(filtered_df["user_id"], search)
            | _contains_literal(filtered_df["agent_name"], search)
            | _contains_literal(filtered_df["folder_id"], search)
        )
        filtered_df = filtered_df[mask]
    if only_workflow:
        filtered_df = filtered_df[filtered_df["uses_workflow"] == True]
    select_all_agents = st.checkbox(
        "Migrate all agents for the selected users",
        value=True,
        key="select_all_agents",
        help="Recommended. The table below is only a bounded preview.",
    )
    previous = st.session_state.get("selected_agent_ids")
    if select_all_agents:
        filtered_df["selected"] = True
    else:
        if isinstance(previous, list):
            filtered_df["selected"] = filtered_df["bot_id"].isin(previous)
        else:
            filtered_df["selected"] = True
    filtered_df = filtered_df[["selected", "agent_name", "is_active", "uses_workflow", "flow_id", "bot_id", "user_id", "folder_id", "docs", "doc_ids", "folders", "created_at"]]
    edited_df = st.data_editor(
        filtered_df,
        hide_index=True,
        use_container_width=True,
        height=320,
        column_config={
            "created_at": st.column_config.DatetimeColumn("Created", format="YYYY-MM-DD"),
            "agent_name": st.column_config.TextColumn("Agent Name", help="Agent display name (bot_data.bot_name)"),
            "is_active": st.column_config.CheckboxColumn(
                "Active in V4",
                disabled=True,
                help="Inactive agents are migrated faithfully but may be hidden by the V5 UI.",
            ),
            "uses_workflow": st.column_config.CheckboxColumn(
                "🔀 Workflow",
                disabled=True,
                help="Agent references a Langflow workflow (model='Workflow' with a flow assigned)",
            ),
            "flow_id": st.column_config.TextColumn("Flow ID", help="Referenced Langflow flow id (first, if multiple)"),
        },
        disabled=["agent_name", "is_active", "uses_workflow", "flow_id"],
        key="agents_editor",
    )
    selected_agent_ids = (
        None
        if select_all_agents
        else edited_df[edited_df["selected"] == True]["bot_id"].astype(str).tolist()
    )
    st.session_state["selected_agent_ids"] = selected_agent_ids
    st.metric(
        "Selected Agents",
        total_count if selected_agent_ids is None else len(selected_agent_ids),
    )
    return selected_agent_ids








def render_related_counts(config: ConnectionConfig, prefix: str, user_ids: list, doc_count: int):
    """Render related data counts."""
    st.markdown("---")
    st.subheader("📊 Related Data Summary")
    
    if not user_ids:
        st.info("Select users to see related data counts.")
        return False
    
    filters = st.session_state.get(SessionKeys.DOCUMENT_FILTERS, {})
    _rk = (
        f"{_source_scope_key(config, prefix)}|"
        f"{tuple(sorted(str(u) for u in user_ids))}|"
        f"{_filters_fingerprint(filters)}|"
        f"{st.session_state.get('include_chunkless_documents', False)}"
    )
    if st.session_state.get("_p2_related_key") == _rk:
        counts = st.session_state["_p2_related_counts"]
        est_size = st.session_state.get("_p2_related_est_size", 0.0)
    else:
        with st.spinner("Calculating related data..."):
            counts = get_selection_summary(
                config,
                prefix,
                user_ids,
                filters=filters,
                include_chunkless_documents=st.session_state.get(
                    "include_chunkless_documents", False
                ),
            )
            est_size = counts.get("embedding_bytes", 0) / (1024 * 1024)

        st.session_state["_p2_related_key"] = _rk
        st.session_state["_p2_related_counts"] = counts
        st.session_state["_p2_related_est_size"] = est_size
    
    # Display as metric cards
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.metric("👥 Users", len(user_ids))
    with col2:
        st.metric("📄 Documents", f"{counts.get('documents', doc_count):,}")
    with col3:
        st.metric("📁 Folders", f"{counts.get('folders', 0):,}")
    with col4:
        st.metric("🧮 Embeddings", f"{counts.get('embeddings', 0):,}")
    with col5:
        st.metric("🤖 Agents", f"{counts.get('agents', 0):,}")
    with col6:
        st.metric("💬 Conversations", f"{counts.get('logs', 0):,}")
    
    # Embedding size warning
    if counts.get("embeddings", 0) and est_size > 500:
        st.warning(
            f"⚠️ Estimated embeddings size: {est_size:.1f} MB. "
            "Consider batched extraction for large datasets."
        )

    embedding_bytes = int(counts.get("embedding_bytes", 0))
    override = st.checkbox(
        "Override workload safety limit",
        value=False,
        key="_p2_workload_override",
        help=(
            "Use only after reviewing target database capacity and intentionally "
            "accepting a larger or mixed-heavy batch."
        ),
    )
    blocked, guardrail_messages = _evaluate_workload_guardrails(
        len(user_ids),
        embedding_bytes,
        override=override,
    )
    for level, message in guardrail_messages:
        if level == "error":
            st.error(message)
        else:
            st.warning(message)
    
    # Summary bar
    summary_doc_count = counts.get("documents", doc_count)
    total_items = len(user_ids) + summary_doc_count + counts.get("folders", 0) + counts.get("embeddings", 0) + counts.get("agents", 0) + counts.get("logs", 0)
    st.success(f"**Ready to migrate:** {len(user_ids)} users, {summary_doc_count:,} documents, {counts.get('embeddings', 0):,} embeddings, {counts.get('folders', 0):,} folders, {counts.get('agents', 0):,} agents, {counts.get('logs', 0):,} conversations")
    return blocked


def _evaluate_workload_guardrails(
    user_count: int,
    embedding_bytes: int,
    *,
    override: bool,
):
    """Return (blocked, messages) for workload-aware batch safety."""
    def _positive_env_int(name: str, default: int) -> int:
        try:
            value = int(os.getenv(name, str(default)))
        except (TypeError, ValueError):
            return default
        return value if value > 0 else default

    max_safe_bytes = _positive_env_int(
        "MIGRATION_MAX_SAFE_EMBEDDING_BYTES",
        2 * 1024 * 1024 * 1024,
    )
    heavy_average_bytes = _positive_env_int(
        "MIGRATION_HEAVY_USER_AVG_EMBEDDING_BYTES",
        250 * 1024 * 1024,
    )
    messages = []
    if user_count > 50:
        messages.append(
            (
                "warning",
                f"Batch contains {user_count} users. The recommended default is "
                "25–50; split larger batches unless they are known to be light.",
            )
        )

    blocking_reasons = []
    if embedding_bytes > max_safe_bytes:
        blocking_reasons.append(
            f"estimated embedding payload is "
            f"{embedding_bytes / (1024 ** 3):.2f} GiB, above the configured "
            f"{max_safe_bytes / (1024 ** 3):.2f} GiB safety limit"
        )
    average_bytes = embedding_bytes / user_count if user_count else 0
    if user_count > 1 and average_bytes > heavy_average_bytes:
        blocking_reasons.append(
            "the average embedding payload indicates heavy users are mixed in "
            "this batch; isolate them into smaller runs"
        )

    if blocking_reasons:
        message = "Workload safety check: " + "; ".join(blocking_reasons) + "."
        if override:
            messages.append(("warning", message + " Explicit override accepted."))
        else:
            messages.append(
                (
                    "error",
                    message
                    + " Extraction is blocked unless the explicit override is enabled.",
                )
            )
    return bool(blocking_reasons and not override), messages

def render_copy_preview(config: ConnectionConfig, prefix: str, user_ids: list):
    """Optional preview of folders and embeddings that will be copied."""
    st.markdown("---")
    st.subheader("🔎 Preview Data to Copy")

    if not user_ids:
        st.info("Select users to preview folders and embeddings.")
        return

    show_preview = st.toggle("Show folders and embeddings preview", value=False)
    if not show_preview:
        return

    filters = st.session_state.get(SessionKeys.DOCUMENT_FILTERS, {})
    doc_table = get_table_name("custom_documents", prefix)
    folders_table = get_table_name("folders", prefix)
    embeddings_table = get_table_name("embeddings", prefix)

    placeholders = ", ".join(["%s"] * len(user_ids))
    doc_query = f"""
        SELECT doc_id
        FROM public.{doc_table}
        WHERE owner_id IN ({placeholders})
          AND COALESCE(blob_source, '') <> %s
    """
    doc_params = list(user_ids) + [SHAREPOINT_DOCUMENT_BLOB_SOURCE]

    if not st.session_state.get("include_chunkless_documents", False):
        doc_query += f"""
            AND EXISTS (
                SELECT 1
                FROM public.{embeddings_table} e
                WHERE e.metadata->>'doc_id' = public.{doc_table}.doc_id
                  AND e.metadata->>'type' = 'chunk-data'
            )
        """

    if filters.get("date_from"):
        doc_query += " AND created_at >= %s"
        doc_params.append(filters["date_from"])
    if filters.get("date_to"):
        doc_query += " AND created_at <= %s"
        doc_params.append(filters["date_to"])
    if filters.get("max_size"):
        doc_query += " AND doc_size <= %s"
        doc_params.append(filters["max_size"])

    doc_ids_df = execute_query(config, doc_query, tuple(doc_params))
    doc_ids = doc_ids_df["doc_id"].tolist() if not doc_ids_df.empty else []

    with st.expander("📁 Folders that will be copied", expanded=True):
        folders_query = f"""
            SELECT id, folder_name, owner_id, parent_id, created_at, folder_type
            FROM public.{folders_table}
            WHERE owner_id IN ({placeholders})
            ORDER BY created_at DESC
            LIMIT 200
        """
        folders_df = execute_query(config, folders_query, tuple(user_ids))
        st.caption(f"Rows shown: {len(folders_df)} (max 200)")
        if folders_df.empty:
            st.info("No folders found for selected users.")
        else:
            st.dataframe(folders_df, use_container_width=True, hide_index=True)

    with st.expander("🧮 Embeddings that will be copied", expanded=True):
        if not doc_ids:
            st.info("No matching documents found for current filters, so no embeddings to preview.")
        else:
            emb_placeholders = ", ".join(["%s"] * len(doc_ids))
            embeddings_query = f"""
                SELECT id, external_id, collection, metadata
                FROM public.{embeddings_table}
                WHERE metadata->>'doc_id' IN ({emb_placeholders})
                LIMIT 200
            """
            embeddings_df = execute_query(config, embeddings_query, tuple(doc_ids))
            st.caption(f"Rows shown: {len(embeddings_df)} (max 200)")
            if embeddings_df.empty:
                st.info("No embeddings found for selected documents.")
            else:
                st.dataframe(embeddings_df, use_container_width=True, hide_index=True)


def render_extraction_section(
    config: ConnectionConfig,
    prefix: str,
    user_emails: list,
    workload_blocked: bool = False,
):
    """Render the extraction section."""
    st.markdown("---")
    st.subheader("📥 Extract Data")
    
    if not user_emails:
        st.info("Select users above to enable extraction.")
        return
    
    filters = st.session_state.get(SessionKeys.DOCUMENT_FILTERS, {})
    
    # Export options
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        generate_sql = st.checkbox(
            "📝 Generate SQL migration files",
            value=True,
            help="Generate SQL INSERT statements for direct database execution"
        )
    with col2:
        export_csv = st.checkbox(
            "📄 Export CSV files",
            value=False,
            help="Export data as CSV files (can be disabled if you only need SQL)"
        )
    with col3:
        extract_conversions = st.checkbox(
            "🔄 Extract conversions",
            value=True,
            help="Extract jeen_dev_translate table and generate conversions migration SQL"
        )
    
    # SQL-specific options (shown only if SQL generation is enabled)
    _has_target = "target_config" in st.session_state
    if generate_sql:

        # ── Inline destination connection (to load Org IDs & Embedding Models) ──
        _target_label = (
            "✅ Destination DB connected — use buttons below to load Org IDs & Models"
            if _has_target
            else "🔌 Connect to destination DB — load Org IDs & Embedding Models"
        )
        with st.expander(_target_label, expanded=not _has_target):
            if _has_target:
                _tc = st.session_state["target_config"]
                st.caption(f"Connected: `{_tc.host}:{_tc.port}` (set via Target page or quick-connect below)")
                if st.button("❌ Disconnect", key="_dest_disconnect"):
                    for _k in ("target_config", "_dest_org_ids", "_dest_embedding_models"):
                        st.session_state.pop(_k, None)
                    st.rerun()
            else:
                st.caption(
                    "Enter destination credentials to fetch Org IDs from `user_db` and "
                    "Embedding Models from `document_db`. Credentials are used only for "
                    "these lookups and are stored in session state only."
                )
                _env_t = get_env_target_defaults()
                _dc1, _dc2, _dc3 = st.columns([3, 1, 2])
                with _dc1:
                    _t_host = st.text_input(
                        "Host", value=_env_t.get("host", ""),
                        key="_dest_host", placeholder="db.example.com"
                    )
                with _dc2:
                    _t_port = st.number_input(
                        "Port", value=int(_env_t.get("port", 5432)),
                        min_value=1, max_value=65535, key="_dest_port"
                    )
                with _dc3:
                    _t_user = st.text_input(
                        "Username", value=_env_t.get("username", ""),
                        key="_dest_user", placeholder="postgres"
                    )
                _t_pass = st.text_input(
                    "Password", type="password",
                    value=_env_t.get("password", ""),
                    key="_dest_pass", placeholder="••••••••"
                )
                if st.button(
                    "📡 Connect & load Org IDs and Embedding Models",
                    key="_dest_connect", type="primary", use_container_width=True
                ):
                    if not all([_t_host, _t_user, _t_pass]):
                        st.error("Please fill in host, username, and password.")
                    else:
                        _probe = ConnectionConfig(
                            host=_t_host, port=int(_t_port),
                            database="user_db",
                            username=_t_user, password=_t_pass
                        )
                        with st.spinner("Connecting…"):
                            _ok, _msg = test_connection(_probe)
                        if not _ok:
                            st.error(f"❌ {_msg}")
                        else:
                            st.session_state["target_config"] = _probe
                            _has_target = True
                            # Fetch both values immediately
                            with st.spinner("Loading Organizations…"):
                                _org_ids = fetch_dest_org_ids()
                            with st.spinner("Loading Embedding Models from document_db…"):
                                _emb_models = fetch_dest_embedding_models()
                            if _org_ids:
                                st.session_state["_dest_org_ids"] = _org_ids
                            if _emb_models:
                                st.session_state["_dest_embedding_models"] = _emb_models
                            st.success(
                                f"✅ Connected! Loaded {len(_org_ids)} Org ID(s) and "
                                f"{len(_emb_models)} Embedding Model(s)."
                            )
                            st.rerun()

        col3, col4, col5 = st.columns([2, 2, 1], vertical_alignment="bottom")

        # ── Organization (name + UUID for SQL) ─────────────────────────────
        with col3:
            _dest_orgs = st.session_state.get("_dest_org_ids", [])

            if _dest_orgs:
                _env_org = get_env_org_id()
                _default_idx = 0
                for _i, _o in enumerate(_dest_orgs):
                    if str(_o.get("id")) == str(_env_org):
                        _default_idx = _i
                        break
                _org_row = st.selectbox(
                    "Org Name",
                    options=list(range(len(_dest_orgs))),
                    index=_default_idx,
                    format_func=lambda i: _org_name_label(_dest_orgs[i]),
                    key="_extract_sql_org_row",
                    help="From destination `admin_db.organizations` (or user_db fallback). "
                         "Only the name is shown here; UUID is used in generated SQL.",
                )
                org_id = str(_dest_orgs[int(_org_row)]["id"])
                # Key includes org_id so this field refreshes when selection changes
                # (a fixed-key disabled text_input can show a stale UUID in Streamlit).
                st.text_input(
                    "Org UUID",
                    value=org_id,
                    disabled=True,
                    key=f"_extract_org_uuid_{org_id}",
                    help="Organization id written into migration SQL.",
                )

                if st.button("✕ Clear / enter manually", key="_clear_org_ids",
                             use_container_width=True):
                    del st.session_state["_dest_org_ids"]
                    st.rerun()
            else:
                org_id = st.text_input(
                    "Org UUID",
                    value=get_env_org_id(),
                    help="Organization UUID for SQL generation "
                         "(set DEFAULT_ORG_ID in .env to change default)"
                )
                if _has_target:
                    if st.button("📡 Load from destination", key="_load_org_ids",
                                 use_container_width=True):
                        with st.spinner("Querying admin_db.organizations…"):
                            _fetched = fetch_dest_org_ids()
                        if _fetched:
                            st.session_state["_dest_org_ids"] = _fetched
                            st.rerun()
                        else:
                            st.warning("No organizations found in destination.")

        # ── Embedding Model ─────────────────────────────────────────────────
        with col4:
            _env_model    = get_env_embedding_model()
            _custom_label = "Custom..."
            _dest_models  = st.session_state.get("_dest_embedding_models", [])

            # Merge presets + destination models (deduplicated, presets first)
            _combined = list(dict.fromkeys(EMBEDDING_MODEL_OPTIONS + _dest_models + [_custom_label]))

            _default_index = (
                _combined.index(_env_model)
                if _env_model in _combined
                else _combined.index(_custom_label)
            )
            _selected_model = st.selectbox(
                "Embedding Model",
                options=_combined,
                index=_default_index,
                help="Model name written into the embeddings table. "
                     "Destination models (if loaded) are merged with the preset list."
            )
            if _selected_model == _custom_label:
                embedding_model = st.text_input(
                    "Custom model name",
                    value=_env_model if _env_model not in EMBEDDING_MODEL_OPTIONS else "",
                    placeholder="e.g. my-custom-model",
                    label_visibility="collapsed"
                )
            else:
                embedding_model = _selected_model

            if _has_target:
                _btn_label = (
                    f"📡 Reload from destination ({len(_dest_models)} loaded)"
                    if _dest_models else "📡 Load from destination"
                )
                if st.button(_btn_label, key="_load_embedding_models",
                             use_container_width=True):
                    with st.spinner("Querying document_db.embeddings…"):
                        _fetched = fetch_dest_embedding_models()
                    if _fetched:
                        st.session_state["_dest_embedding_models"] = _fetched
                        st.rerun()
                    else:
                        st.warning("No model_names found in destination document_db.")
        with col5:
            target_embedding_dim = st.number_input(
                "Target dim",
                min_value=0,
                max_value=4096,
                value=0,
                step=1,
                help=(
                    "0 preserves each source vector's dimension (recommended). "
                    "A smaller value is allowed only when every removed component is zero."
                ),
            )
            if target_embedding_dim == 0:
                target_embedding_dim = None
            skip_empty_embeddings = st.checkbox(
                "Skip empty",
                value=False,
                help="Skip rows without embeddings"
            )
        
        # User ID overrides for users who already exist in V5 with a different UUID
        with st.expander("👤 User ID Overrides *(optional)*"):
            st.caption(
                "Use this when a user exists in both V4 and V5 but with different UUIDs. "
                "Their content will be migrated and linked to their existing V5 UUID instead of generating a new one. "
                "Enter one mapping per line in the format: `v4_uuid=v5_uuid`"
            )
            overrides_text = st.text_area(
                "V4 UUID → V5 UUID mappings",
                value="",
                height=100,
                placeholder="3fa85f64-5717-4562-b3fc-2c963f66afa6=7c9e6679-7425-40de-944b-e07fc1f90ae7",
                label_visibility="collapsed"
            )
        user_id_overrides = {}
        if overrides_text.strip():
            for _line in overrides_text.strip().splitlines():
                _line = _line.strip()
                if '=' in _line:
                    _parts = _line.split('=', 1)
                    if len(_parts) == 2:
                        _v4, _v5 = _parts[0].strip(), _parts[1].strip()
                        if _v4 and _v5:
                            user_id_overrides[_v4] = _v5
            if user_id_overrides:
                st.info(f"🔀 {len(user_id_overrides)} user ID override(s) configured.")
        cross_owner_policy = "owned_only"
        st.info(
            "**Owned data only:** documents and folders owned by other users are "
            "not migrated. Agent links to them are removed, and owned folders "
            "below an unowned parent are migrated as root folders."
        )
        conversation_policy_labels = {
            "adopt_exact": (
                "Automatically reuse exact legacy conversation copies (recommended)"
            ),
            "replace_unmapped": (
                "V4 authoritative: replace unmapped V5 conversation collisions"
            ),
            "block": "Stop on every unmapped conversation collision",
        }
        conversation_collision_policy = st.selectbox(
            "Existing conversation UUIDs",
            options=list(conversation_policy_labels),
            format_func=conversation_policy_labels.get,
            help=(
                "Exact reuse verifies the owner and every deterministic message "
                "and content-block UUID. V4-authoritative replacement is atomic "
                "inside the shard and never deletes canonically mapped conversations."
            ),
        )
        conversation_policy_confirmed = True
        if conversation_collision_policy == "replace_unmapped":
            st.warning(
                "This mode deletes an unmapped V5 conversation and its messages "
                "inside the migration transaction, then recreates it from V4."
            )
            conversation_policy_confirmed = st.checkbox(
                "I confirm that V4 conversation data is authoritative.",
                value=False,
            )
    else:
        org_id = get_env_org_id()
        embedding_model = get_env_embedding_model()
        skip_empty_embeddings = False
        target_embedding_dim = None
        user_id_overrides = {}
        cross_owner_policy = "owned_only"
        conversation_collision_policy = "adopt_exact"
        conversation_policy_confirmed = True

    conversation_scope = st.session_state.get("_p2_conv_scope", {})
    email_by_user_id = st.session_state.get(
        "_p2_selected_user_email_by_id", {}
    )
    per_user_scope = conversation_scope.get("per_user", {})
    confirmation_rows = []
    scoped_emails = set()
    for user_id, counts in per_user_scope.items():
        email = email_by_user_id.get(str(user_id), str(user_id))
        scoped_emails.add(email)
        confirmation_rows.append({
            "Email": email,
            "Conversations": int(counts.get("conversation_count", 0)),
            "Source turns": int(counts.get("log_row_count", 0)),
        })
    for email in user_emails:
        if email not in scoped_emails:
            confirmation_rows.append({
                "Email": email,
                "Conversations": 0,
                "Source turns": 0,
            })
    confirmation_rows.sort(key=lambda row: row["Email"].lower())

    st.markdown("#### Confirm migration scope")
    st.caption(
        f"Conversation creation range: "
        f"{conversation_scope.get('date_from') or 'no lower bound'} → "
        f"{conversation_scope.get('date_to') or 'no upper bound'} · "
        f"{len(user_emails)} user(s) · "
        f"{int(conversation_scope.get('conversation_count', 0)):,} unique conversations · "
        f"{int(conversation_scope.get('log_row_count', 0)):,} source turns"
    )
    st.dataframe(
        pd.DataFrame(confirmation_rows),
        hide_index=True,
        use_container_width=True,
    )
    scope_fingerprint = json.dumps(
        {
            "emails": sorted(str(email).strip().lower() for email in user_emails),
            "conversation_scope": conversation_scope,
        },
        sort_keys=True,
        default=str,
    )
    scope_confirmation = st.checkbox(
        "I confirm these users, conversation dates, and per-user counts.",
        value=False,
        key=f"_p2_scope_confirm_{uuid.uuid5(uuid.NAMESPACE_URL, scope_fingerprint)}",
    )

    if st.button(
        "🚀 Start Extraction",
        type="primary",
        use_container_width=True,
        disabled=(
            workload_blocked
            or not scope_confirmation
            or not conversation_policy_confirmed
        ),
    ):
        if generate_sql and not org_id:
            st.error("Please select an organization before starting extraction.")
            st.stop()

        migration_run_id = None
        if generate_sql:
            target_user_config = _make_target_config("user_db")
            target_admin_config = _make_target_config("admin_db")
            if target_user_config is None or target_admin_config is None:
                st.error(
                    "A target connection is required to resolve existing users "
                    "and validate the selected organization."
                )
                st.stop()
            try:
                validate_target_organization(target_admin_config, org_id)
                resolution = resolve_existing_user_overrides(
                    source_config=config,
                    target_user_config=target_user_config,
                    prefix=prefix,
                    user_emails=user_emails,
                    manual_overrides=user_id_overrides,
                )
                user_id_overrides = resolution["overrides"]
                migration_run_id = str(uuid.uuid4())
                create_distributed_run(
                    target_user_config,
                    migration_run_id,
                    resolution["users"],
                    {
                        "host": config.host,
                        "port": config.port,
                        "database": config.database,
                        "prefix": prefix,
                        "conversation_scope": {
                            "date_from": conversation_scope.get("date_from"),
                            "date_to": conversation_scope.get("date_to"),
                            "max_per_user": conversation_scope.get("max_per_user"),
                            "conversation_count": conversation_scope.get(
                                "conversation_count", 0
                            ),
                            "log_row_count": conversation_scope.get(
                                "log_row_count", 0
                            ),
                            "collision_policy": conversation_collision_policy,
                        },
                    },
                    source_config=config,
                )
                st.session_state["_current_batch_id"] = migration_run_id
                st.session_state["_current_batch_emails"] = user_emails
                for warning in resolution["warnings"]:
                    st.warning(warning)
            except Exception as exc:
                st.error(f"Migration preflight failed: {exc}")
                st.stop()
        # Create progress containers
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def progress_callback(table_name: str, current: int, total: int):
            progress_bar.progress(current / total)
            status_text.text(f"Extracting {table_name}... ({current}/{total})")
        
        # Create extraction engine
        engine = ExtractionEngine(
            config=config,
            prefix=prefix,
            output_dir=OUT_DIR,
            progress_callback=progress_callback,
            generate_sql=generate_sql,
            export_csv=export_csv,
            organization_id=org_id if generate_sql else None,
            embedding_model=embedding_model if generate_sql else 'text-embedding-ada-002',
            skip_empty_embeddings=skip_empty_embeddings if generate_sql else False,
            target_embedding_dim=target_embedding_dim if generate_sql else None,
            user_id_overrides=user_id_overrides if generate_sql else {},
            migration_run_id=migration_run_id,
            cross_owner_policy=cross_owner_policy,
            conversation_collision_policy=conversation_collision_policy,
            agent_collision_policy="adopt_exact",
            include_chunkless_documents=st.session_state.get(
                "include_chunkless_documents", False
            ),
        )
        
        # Run extraction
        with st.spinner("Extracting data..."):
            results = engine.run_full_extraction(
                user_emails=user_emails,
                date_from=filters.get("date_from"),
                date_to=filters.get("date_to"),
                max_doc_size=filters.get("max_size"),
                selected_doc_ids=st.session_state.get("selected_doc_ids"),
                selected_embedding_ids=st.session_state.get("selected_embedding_ids"),
                selected_agent_ids=st.session_state.get("selected_agent_ids"),
                selected_conversation_chat_ids=st.session_state.get(
                    "selected_conversation_chat_ids"
                ),
                extract_conversions=extract_conversions,
                conv_date_from=st.session_state.get("conv_date_from_parsed"),
                conv_date_to=st.session_state.get("conv_date_to_parsed"),
                conv_max_per_user=st.session_state.get("conv_max_per_user_parsed"),
            )

        if generate_sql and not results.get("errors"):
            preflight_stopped = False
            try:
                ownership_conflicts = find_canonical_ownership_conflicts(
                    target_user_config,
                    results.get("ownership_manifest", {}),
                    user_id_overrides,
                )
            except Exception as exc:
                preflight_stopped = True
                results.setdefault("errors", []).append(
                    "Could not validate existing V5 entity ownership: "
                    + str(exc)
                )
            else:
                try:
                    repaired_documents = repair_orphaned_document_owners(
                        target_user_config,
                        ownership_conflicts,
                        migration_run_id,
                    )
                    repaired_folders = repair_orphaned_folder_owners(
                        target_user_config,
                        ownership_conflicts,
                        migration_run_id,
                    )
                    if repaired_documents:
                        results["repaired_document_owners"] = (
                            repaired_documents
                        )
                    if repaired_folders:
                        results["repaired_folder_owners"] = repaired_folders
                    if repaired_documents or repaired_folders:
                        ownership_conflicts = (
                            find_canonical_ownership_conflicts(
                                target_user_config,
                                results.get("ownership_manifest", {}),
                                user_id_overrides,
                            )
                        )
                    folder_conflicts_to_exclude = [
                        row
                        for row in ownership_conflicts
                        if row.get("entity_type") == "folders"
                    ]
                    if folder_conflicts_to_exclude:
                        engine.exclude_canonical_folder_conflicts(
                            [
                                str(row["old_id"])
                                for row in folder_conflicts_to_exclude
                            ],
                            results,
                        )
                        ownership_conflicts = (
                            find_canonical_ownership_conflicts(
                                target_user_config,
                                results.get("ownership_manifest", {}),
                                user_id_overrides,
                            )
                        )
                except Exception as exc:
                    preflight_stopped = True
                    results.setdefault("errors", []).append(
                        "Could not safely resolve V5 ownership conflicts: "
                        + str(exc)
                    )
                else:
                    if ownership_conflicts:
                        preflight_stopped = True
                        results["ownership_conflicts"] = ownership_conflicts
                        results.setdefault("errors", []).append(
                            ownership_conflict_message(ownership_conflicts)
                        )
            try:
                conversation_preflight = inspect_conversation_conflicts(
                    target_user_config,
                    results.get("conversation_manifest", []),
                    conversation_collision_policy,
                )
                results["conversation_preflight"] = conversation_preflight
                if conversation_preflight["conflicts"]:
                    preflight_stopped = True
                    results.setdefault("errors", []).append(
                        conversation_conflict_message(
                            conversation_preflight["conflicts"]
                        )
                    )
            except Exception as exc:
                preflight_stopped = True
                results.setdefault("errors", []).append(
                    "Could not validate existing V5 conversations: " + str(exc)
                )
            try:
                agent_preflight = inspect_agent_conflicts(
                    target_user_config,
                    results.get("agent_manifest", []),
                    "adopt_exact",
                )
                results["agent_preflight"] = agent_preflight
                if agent_preflight["conflicts"]:
                    preflight_stopped = True
                    results.setdefault("errors", []).append(
                        agent_conflict_message(
                            agent_preflight["conflicts"]
                        )
                    )
            except Exception as exc:
                preflight_stopped = True
                results.setdefault("errors", []).append(
                    "Could not validate existing V5 agents: " + str(exc)
                )

            if preflight_stopped:
                undeleted_files = []
                for filename in os.listdir(engine.sql_output_dir):
                    if engine.timestamp not in filename:
                        continue
                    try:
                        os.remove(
                            os.path.join(engine.sql_output_dir, filename)
                        )
                    except OSError:
                        undeleted_files.append(filename)
                results["sql_files"] = {}
                if undeleted_files:
                    results.setdefault("errors", []).append(
                        "The blocked SQL files were detached from this batch but "
                        "could not be deleted from disk: "
                        + ", ".join(sorted(undeleted_files))
                    )
        
        progress_bar.progress(1.0)
        status_text.text("Extraction complete!")
        
        # Store results
        st.session_state[SessionKeys.EXTRACTED_DATA] = results

        if generate_sql and migration_run_id:
            generated_step_keys = {
                prefix
                for prefix in (
                    "01_users",
                    "02_folders",
                    "03_documents",
                    "04_chunks_embeddings",
                    "05_conversations",
                    "06_agents",
                    "07_conversions",
                )
                if any(
                    os.path.basename(path).startswith(prefix)
                    for path in results.get("sql_files", {}).values()
                )
            }
            try:
                summary = results.get("summary", {})
                expectation_counts = {
                    "01_users": summary.get("users", 0),
                    "02_folders": summary.get("folders", 0),
                    "03_documents": summary.get("documents", 0),
                    "04_chunks_embeddings": summary.get("embeddings", 0),
                    "05_conversations": summary.get("conversations", 0),
                    "06_agents": summary.get("agents", 0),
                    "07_conversions": summary.get("translate", 0),
                }
                expectations = {
                    step_key: {
                        "expected_count": expectation_counts[step_key],
                        "details": (
                            {
                                "expected_chunks": summary.get("embeddings", 0),
                                "expected_embeddings": summary.get(
                                    "embedding_vectors", 0
                                ),
                            }
                            if step_key == "04_chunks_embeddings"
                            else {}
                        ),
                    }
                    for step_key in generated_step_keys
                }
                record_step_expectations(
                    target_user_config,
                    migration_run_id,
                    expectations,
                    source_config=config,
                )
                mark_unproduced_steps_skipped(
                    target_user_config,
                    migration_run_id,
                    generated_step_keys,
                    source_config=config,
                )
            except Exception as exc:
                results.setdefault("errors", []).append(
                    f"Could not mark skipped migration steps: {exc}"
                )
            run_status = "failed" if results.get("errors") else "running"
            run_error = "; ".join(results.get("errors", [])) or None
            for target_database in ("user_db", "document_db", "completion_db"):
                try:
                    update_local_run(
                        target_user_config,
                        target_database,
                        migration_run_id,
                        run_status,
                        run_error,
                    )
                except Exception as exc:
                    results.setdefault("errors", []).append(
                        f"Could not update {target_database} run tracking: {exc}"
                    )
            try:
                update_source_run(
                    config,
                    migration_run_id,
                    run_status,
                    run_error,
                )
            except Exception as exc:
                results.setdefault("errors", []).append(
                    f"Could not update V4 source run tracking: {exc}"
                )

        # Show results
        if results.get("errors"):
            for error in results["errors"]:
                st.error(error)
            ownership_conflicts = results.get("ownership_conflicts", [])
            if ownership_conflicts:
                with st.expander(
                    "Existing V5 ownership conflicts",
                    expanded=True,
                ):
                    st.warning(
                        "These rows were created or reassigned by an older "
                        "migration. This extraction produced no runnable SQL. "
                        "Clean or roll back the older mappings before retrying."
                    )
                    st.dataframe(
                        pd.DataFrame(ownership_conflicts),
                        hide_index=True,
                        use_container_width=True,
                    )
            conversation_conflicts = results.get(
                "conversation_preflight", {}
            ).get("conflicts", [])
            if conversation_conflicts:
                with st.expander(
                    "Existing V5 conversation conflicts",
                    expanded=True,
                ):
                    st.dataframe(
                        pd.DataFrame(conversation_conflicts),
                        hide_index=True,
                        use_container_width=True,
                    )
            agent_conflicts = results.get("agent_preflight", {}).get(
                "conflicts", []
            )
            if agent_conflicts:
                with st.expander(
                    "Existing V5 agent conflicts",
                    expanded=True,
                ):
                    st.dataframe(
                        pd.DataFrame(agent_conflicts),
                        hide_index=True,
                        use_container_width=True,
                    )
        else:
            st.success(f"✅ Extraction complete! Timestamp: {results['timestamp']}")

        repaired_document_owners = results.get(
            "repaired_document_owners", []
        )
        if repaired_document_owners:
            st.success(
                f"Repaired {len(repaired_document_owners)} canonical document "
                "owner(s) that pointed to deleted V5 users."
            )
        repaired_folder_owners = results.get("repaired_folder_owners", [])
        if repaired_folder_owners:
            st.success(
                f"Repaired {len(repaired_folder_owners)} canonical folder "
                "owner(s) that pointed to deleted V5 users."
            )
        folder_exclusions = results.get("canonical_folder_exclusions", {})
        if folder_exclusions.get("excluded_folder_ids"):
            with st.expander("Canonical folder conflicts excluded"):
                st.info(
                    "Conflicting existing V5 folders were preserved. Their V4 "
                    "copies were excluded; selected users' documents and child "
                    "folders were detached and will migrate as roots."
                )
                st.json(folder_exclusions)

        conversation_preflight = results.get("conversation_preflight", {})
        will_adopt = conversation_preflight.get("will_adopt", [])
        will_replace = conversation_preflight.get("will_replace", [])
        if will_adopt or will_replace:
            with st.expander("Conversation collision plan"):
                if will_adopt:
                    st.info(
                        f"{len(will_adopt)} exact legacy conversation copy/copies "
                        "will be adopted without rewriting their data."
                    )
                if will_replace:
                    st.warning(
                        f"{len(will_replace)} unmapped V5 conversation(s) will be "
                        "replaced atomically from V4."
                    )
                st.dataframe(
                    pd.DataFrame(will_adopt + will_replace),
                    hide_index=True,
                    use_container_width=True,
                )
        agent_adoptions = results.get("agent_preflight", {}).get(
            "will_adopt", []
        )
        if agent_adoptions:
            with st.expander("Agent collision plan"):
                st.info(
                    f"{len(agent_adoptions)} exact previously migrated agent(s) "
                    "will be adopted without rewriting their data."
                )
                st.dataframe(
                    pd.DataFrame(agent_adoptions),
                    hide_index=True,
                    use_container_width=True,
                )

        invalid_chat_rows = results.get("summary", {}).get(
            "invalid_chat_rows", 0
        )
        if invalid_chat_rows:
            st.warning(
                f"{invalid_chat_rows} conversation log row(s) had a null, "
                "blank, or invalid chat UUID and were skipped."
            )

        document_filter_report = results.get("document_filter_report", {})
        excluded_chunkless = document_filter_report.get("chunkless_doc_ids", [])
        if excluded_chunkless:
            st.warning(
                f"{len(excluded_chunkless)} document(s) had no V4 chunks and "
                "were not migrated. Any agent links to them were removed."
            )
            with st.expander("Excluded chunkless documents"):
                st.code("\n".join(excluded_chunkless))

        detached_document_folders = document_filter_report.get(
            "detached_document_folder_ids", []
        )
        if detached_document_folders:
            st.info(
                f"{len(detached_document_folders)} owned document(s) belonged "
                "to folders outside the owned migration scope and will be "
                "migrated without a folder."
            )
            with st.expander("Documents detached from shared folders"):
                st.code("\n".join(detached_document_folders))

        readiness = results.get("document_readiness", {})
        needs_reprocessing = readiness.get(
            "documents_requiring_reprocessing", []
        )
        if needs_reprocessing:
            st.warning(
                f"{len(needs_reprocessing)} document(s) have no complete "
                "chunk/embedding set. They will remain not ready for optional "
                "V5 reprocessing."
            )
            with st.expander("Documents requiring V5 reprocessing"):
                st.code("\n".join(needs_reprocessing))

        folder_report = results.get("folder_hierarchy_report", {})
        detached_folders = folder_report.get("detached_folder_ids", [])
        stale_parents = folder_report.get("stale_parent_ids", [])
        cross_owner_parents = folder_report.get(
            "dropped_cross_owner_parent_ids", []
        )
        if detached_folders:
            st.warning(
                f"{len(detached_folders)} owned folder(s) referenced missing or "
                "other-user parents and were safely migrated as root folders."
            )
            with st.expander("Detached folder hierarchy details"):
                st.write({
                    "folders": detached_folders,
                    "missing_parents": stale_parents,
                    "other_user_parents": cross_owner_parents,
                })

        # ── Agent-document topup report ──────────────────────────────────────
        topup = results.get("topup_report")
        if topup:
            added_docs    = topup.get("added_doc_ids", [])
            stale_docs    = topup.get("stale_doc_ids", [])
            added_folders = topup.get("added_folder_ids", [])
            stale_folders = topup.get("stale_folder_ids", [])
            chunkless_docs = topup.get("chunkless_doc_ids", [])
            dropped_cross_owner_docs = topup.get(
                "dropped_cross_owner_doc_ids", []
            )
            dropped_cross_owner_folders = topup.get(
                "dropped_cross_owner_folder_ids", []
            )

            st.subheader("🤖 Agent Document Coverage")

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("📄 Docs auto-added",    len(added_docs),
                      help="Documents not in original selection but required by agents — fetched automatically.")
            c2.metric("⚠️ Stale doc refs",     len(stale_docs),
                      help="Agent-referenced documents no longer found in V4. These links will be dropped.")
            c3.metric("📁 Folders auto-added", len(added_folders),
                      help="Folders (including ancestors) fetched because agents reference them.")
            c4.metric("⚠️ Stale folder refs",  len(stale_folders),
                      help="Agent-referenced folders no longer found in V4. These links will be dropped.")

            if added_docs:
                st.success(
                    f"✅ **{len(added_docs)} document(s)** were automatically added to the migration because "
                    f"selected agents depend on them. They appear in `03_documents_*.sql` annotated with "
                    f"`[agent-topup]`."
                )

            if stale_docs:
                with st.expander(f"⚠️ {len(stale_docs)} stale document reference(s) — links will be dropped"):
                    st.warning(
                        "These documents are referenced by agents in V4 but no longer exist in the source "
                        "database. The agent-document links cannot be migrated."
                    )
                    st.dataframe(
                        pd.DataFrame({"Stale doc_id": stale_docs}),
                        hide_index=True, use_container_width=True
                    )

            if chunkless_docs:
                with st.expander(
                    f"⚠️ {len(chunkless_docs)} chunkless agent document "
                    "reference(s) — links will be dropped"
                ):
                    st.warning(
                        "These documents exist in V4 but have no chunk data, "
                        "so neither the documents nor their agent links will migrate."
                    )
                    st.dataframe(
                        pd.DataFrame({"Chunkless doc_id": chunkless_docs}),
                        hide_index=True,
                        use_container_width=True,
                    )

            if stale_folders:
                with st.expander(f"⚠️ {len(stale_folders)} stale folder reference(s) — links will be dropped"):
                    st.warning("These folders are referenced by agents but no longer exist in V4.")
                    st.dataframe(
                        pd.DataFrame({"Stale folder_id": stale_folders}),
                        hide_index=True, use_container_width=True
                    )

            if dropped_cross_owner_docs or dropped_cross_owner_folders:
                with st.expander(
                    "Shared dependencies excluded (owned data only)"
                ):
                    st.info(
                        "These documents and folders belong to other V4 users. "
                        "They were not migrated and all selected-agent links to "
                        "them were removed."
                    )
                    if dropped_cross_owner_docs:
                        st.dataframe(
                            pd.DataFrame({
                                "Excluded document": dropped_cross_owner_docs
                            }),
                            hide_index=True, use_container_width=True
                        )
                    if dropped_cross_owner_folders:
                        st.dataframe(
                            pd.DataFrame({
                                "Excluded folder": dropped_cross_owner_folders
                            }),
                            hide_index=True, use_container_width=True
                        )

        # ── Extraction summary table ─────────────────────────────────────────
        # Show summary
        st.subheader("📊 Extraction Summary")
        summary_data = [
            {"Table": table, "Rows Extracted": count}
            for table, count in results.get("summary", {}).items()
        ]
        st.dataframe(pd.DataFrame(summary_data), hide_index=True)
        
        # Download buttons for CSV files
        st.subheader("📥 Download CSV Files")
        cols = st.columns(3)
        for i, (table, filepath) in enumerate(results.get("files", {}).items()):
            if os.path.exists(filepath):
                with cols[i % 3]:
                    file_size = os.path.getsize(filepath)
                    download_data = read_inline_download(filepath)
                    if download_data is not None:
                        st.download_button(
                            label=f"📄 {table}.csv",
                            data=download_data,
                            file_name=os.path.basename(filepath),
                            mime="text/csv",
                            key=f"dl_csv_{table}",
                            on_click="ignore",
                        )
                    else:
                        st.caption(
                            f"`{os.path.basename(filepath)}` · "
                            f"{human_file_size(file_size)} · server-side only"
                        )
        
        # Download buttons for SQL files (if generated)
        if results.get("sql_files"):
            st.subheader("📥 Download SQL Migration Files")
            st.info("💡 These SQL files can be executed directly with: `psql -h <host> -U <user> -d <database> -f <file>.sql`")
            
            # Show the host folder path (volume mounted)
            # Container path: /app/output/migrations -> Host path: ./output/migrations
            host_folder = "output/migrations"  # Relative to project root on host
            st.info(f"📂 **SQL files location (copy to File Explorer):**")
            st.code(host_folder, language=None)
            cols_sql = st.columns(3)
            for i, (table, filepath) in enumerate(results.get("sql_files", {}).items()):
                if os.path.exists(filepath):
                    with cols_sql[i % 3]:
                        display_name = os.path.basename(filepath)
                        file_size = os.path.getsize(filepath)
                        download_data = read_inline_download(filepath)
                        if download_data is not None:
                            st.download_button(
                                label=f"🗃️ {display_name}",
                                data=download_data,
                                file_name=display_name,
                                mime="text/plain",
                                key=f"dl_sql_{table}",
                                on_click="ignore",
                            )
                        else:
                            st.caption(
                                f"`{display_name}` · {human_file_size(file_size)}"
                            )
                            st.caption(f"Server path: `{filepath}`")
            
            # SQL file preview expanders — show all expected steps in order
            st.subheader("👁️ SQL Files Preview")
            ALL_STEPS = [
                ("01", "users", "Users"),
                ("02", "folders", "Document folders"),
                ("03", "documents", "Documents"),
                ("04", "chunks_embeddings", "Chunks & embeddings"),
                ("05", "conversations", "Conversations"),
                ("06", "agents", "Agents"),
                ("07", "conversions", "Agent-conversation links"),
            ]
            sql_files = results.get("sql_files", {})
            for step_num, table_key, step_label in ALL_STEPS:
                filepath = sql_files.get(table_key)
                if filepath and os.path.exists(filepath):
                    file_size = os.path.getsize(filepath)
                    size_str = human_file_size(file_size)
                    display_name = os.path.basename(filepath)

                    expander_label = f"🗃️ {display_name} ({size_str})"
                    if file_size <= INLINE_DOWNLOAD_BYTES:
                        col_exp, col_btn = st.columns([10, 1])
                        with col_exp:
                            st.caption(expander_label)
                        with col_btn:
                            download_data = read_inline_download(filepath)
                            st.download_button(
                                label="💾",
                                data=download_data,
                                file_name=display_name,
                                mime="text/plain",
                                key=f"save_sql_{table_key}",
                                help="Save SQL file",
                                on_click="ignore",
                            )
                    else:
                        st.caption(
                            f"{expander_label} · browser download disabled · "
                            f"`{filepath}`"
                        )

                    with st.expander(expander_label):
                        content, truncated = read_text_preview(filepath)
                        if truncated:
                            content += "\n\n-- [TRUNCATED - File too large for full preview] --"
                        st.code(content, language="sql")
                else:
                    st.markdown(
                        f"<div style='padding:12px 16px;border-radius:4px;background:#f0f0f0;"
                        f"color:#999;margin-bottom:8px'>"
                        f"🗃️ {step_num}_{table_key} — <em>{step_label}: 0 rows, skipped</em></div>",
                        unsafe_allow_html=True,
                    )


def main():
    """Main page function."""
    if not check_connection():
        return
    
    config = st.session_state["source_config"]
    prefix = st.session_state.get(SessionKeys.TABLE_PREFIX, "jeen_dev")
    
    # User selection
    result = render_user_selection(config, prefix)
    if result is None:
        return
    
    selected_emails, selected_user_ids = result
    
    # Documents filters + selection (default all selected)
    selected_doc_ids = render_document_selection(config, prefix, selected_user_ids)
    
    # Embeddings selection (default all selected)
    selected_embedding_ids = render_embeddings_selection(config, prefix, selected_doc_ids)
    
    # Agents selection (default all selected)
    selected_agent_ids = render_agents_selection(config, prefix, selected_user_ids)


    # Conversations selection (default all selected)
    selected_conversation_ids = render_conversations_selection(config, prefix, selected_user_ids)
    
    # Doc count for summary (reuse cache from document filters when possible)
    filters = st.session_state.get(SessionKeys.DOCUMENT_FILTERS, {})
    if selected_user_ids:
        _dc_main_k = (
            f"{_source_scope_key(config, prefix)}|"
            f"{tuple(sorted(str(u) for u in selected_user_ids))}|"
            f"{_filters_fingerprint(filters)}"
        )
        if st.session_state.get("_p2_doccount_key") == _dc_main_k:
            doc_count = st.session_state["_p2_doccount_val"]
        else:
            doc_count = get_document_count_preview(
                config, prefix, selected_user_ids,
                filters.get("date_from"), filters.get("date_to"), filters.get("max_size"),
            )
    else:
        doc_count = 0
    if isinstance(selected_doc_ids, list):
        doc_count = len(selected_doc_ids)
    
    # Related counts
    workload_blocked = render_related_counts(
        config, prefix, selected_user_ids, doc_count
    )
    
    # Optional preview of folders/embeddings that will be copied
    render_copy_preview(config, prefix, selected_user_ids)
    
    # Extraction
    render_extraction_section(
        config,
        prefix,
        selected_emails,
        workload_blocked=workload_blocked,
    )
    
    # Next step hint
    if SessionKeys.EXTRACTED_DATA in st.session_state:
        st.markdown("---")
        st.info("👉 **Next Step:** Go to **Transform** page to configure column mappings.")


if __name__ == "__main__":
    main()
