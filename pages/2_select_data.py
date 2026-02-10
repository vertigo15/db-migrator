"""
Page 2: Select Data to Migrate

Features:
- User selection with searchable dataframe
- Document filters (date range, max size)
- Related data counts (folders, embeddings, agents)
- Extraction with progress
- CSV preview and download
"""
import os
from datetime import datetime, date
import streamlit as st
import pandas as pd

from utils.db import ConnectionConfig, execute_query
from utils.storage import (
    save_selected_users, load_selected_users,
    save_document_filters, load_document_filters
)
from utils.config import SessionKeys, get_table_name
from utils.extraction import (
    ExtractionEngine,
    get_document_count_preview,
    get_related_counts,
    estimate_embeddings_size
)

# Page config
st.set_page_config(page_title="Select Data", page_icon="📋", layout="wide")
st.title("📋 Select Data to Migrate")

# Output directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "extract")


def check_connection():
    """Check if source connection is available."""
    if "source_config" not in st.session_state:
        st.warning("⚠️ Please connect to the source database first.")
        st.page_link("pages/1_connect.py", label="Go to Connect Page", icon="🔌")
        return False
    return True


def load_users_data(config: ConnectionConfig, prefix: str) -> pd.DataFrame:
    """Load users from the source database."""
    table_name = get_table_name("users", prefix)
    query = f"""
        SELECT id, name, last_name, email, company_name, created_at, last_connected
        FROM public.{table_name}
        ORDER BY email
    """
    return execute_query(config, query)


def render_user_selection(config: ConnectionConfig, prefix: str):
    """Render the user selection section."""
    st.subheader("👥 Select Users")
    
    # Load users
    with st.spinner("Loading users..."):
        users_df = load_users_data(config, prefix)
    
    if users_df.empty:
        st.warning("No users found in the database.")
        return
    
    st.caption(f"Found {len(users_df)} users in `{get_table_name('users', prefix)}`")
    
    # Load previously selected users from localStorage
    saved_emails = load_selected_users()
    
    # Select all checkbox
    col1, col2 = st.columns([1, 4])
    with col1:
        select_all = st.checkbox("Select All", value=False)
    
    # Search filter
    with col2:
        search = st.text_input("🔍 Search users", placeholder="Search by name or email...")
    
    # Filter dataframe
    if search:
        mask = (
            users_df["name"].str.contains(search, case=False, na=False) |
            users_df["last_name"].str.contains(search, case=False, na=False) |
            users_df["email"].str.contains(search, case=False, na=False) |
            users_df["company_name"].str.contains(search, case=False, na=False)
        )
        filtered_df = users_df[mask].copy()
    else:
        filtered_df = users_df.copy()
    
    # Add selection column
    if select_all:
        filtered_df["selected"] = True
    else:
        filtered_df["selected"] = filtered_df["email"].isin(saved_emails)
    
    # Reorder columns
    display_cols = ["selected", "name", "last_name", "email", "company_name", "created_at", "last_connected"]
    filtered_df = filtered_df[display_cols]
    
    # Display editable dataframe
    edited_df = st.data_editor(
        filtered_df,
        column_config={
            "selected": st.column_config.CheckboxColumn(
                "Select",
                help="Select users to migrate",
                default=False
            ),
            "name": st.column_config.TextColumn("First Name"),
            "last_name": st.column_config.TextColumn("Last Name"),
            "email": st.column_config.TextColumn("Email"),
            "company_name": st.column_config.TextColumn("Company"),
            "created_at": st.column_config.DatetimeColumn("Created", format="YYYY-MM-DD"),
            "last_connected": st.column_config.DatetimeColumn("Last Connected", format="YYYY-MM-DD"),
        },
        hide_index=True,
        use_container_width=True,
        height=400
    )
    
    # Get selected emails
    selected_emails = edited_df[edited_df["selected"] == True]["email"].tolist()
    
    # Store selection
    st.session_state[SessionKeys.SELECTED_USERS] = selected_emails
    
    # Get selected user IDs
    selected_user_ids = users_df[users_df["email"].isin(selected_emails)]["id"].tolist()
    st.session_state[SessionKeys.SELECTED_USER_IDS] = selected_user_ids
    
    # Save selection button
    if st.button("💾 Save Selection", type="secondary"):
        save_selected_users(selected_emails)
        st.success(f"Saved {len(selected_emails)} selected users!")
    
    # Selection summary
    st.metric("Selected Users", len(selected_emails))
    
    return selected_emails, selected_user_ids


def render_document_filters(config: ConnectionConfig, prefix: str, user_ids: list):
    """Render document filter section."""
    st.markdown("---")
    st.subheader("📄 Document Filters")
    
    if not user_ids:
        st.info("Select users above to filter documents.")
        return None, None, None
    
    # Load saved filters
    saved_filters = load_document_filters()
    
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
    
    # Get preview count
    with st.spinner("Counting matching documents..."):
        doc_count = get_document_count_preview(
            config, prefix, user_ids,
            date_from_dt, date_to_dt, max_size_val
        )
    
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


def render_related_counts(config: ConnectionConfig, prefix: str, user_ids: list, doc_count: int):
    """Render related data counts."""
    st.markdown("---")
    st.subheader("📊 Related Data Summary")
    
    if not user_ids:
        st.info("Select users to see related data counts.")
        return
    
    # Get document IDs for embedding count (if we have the preview)
    # For now, we'll show estimated counts
    
    with st.spinner("Calculating related data..."):
        # First get document IDs for the current filters
        filters = st.session_state.get(SessionKeys.DOCUMENT_FILTERS, {})
        doc_table = get_table_name("custom_documents", prefix)
        placeholders = ", ".join(["%s"] * len(user_ids))
        
        query = f"SELECT doc_id FROM public.{doc_table} WHERE owner_id IN ({placeholders})"
        params = list(user_ids)
        
        if filters.get("date_from"):
            query += " AND created_at >= %s"
            params.append(filters["date_from"])
        if filters.get("date_to"):
            query += " AND created_at <= %s"
            params.append(filters["date_to"])
        if filters.get("max_size"):
            query += " AND doc_size <= %s"
            params.append(filters["max_size"])
        
        doc_ids_df = execute_query(config, query, tuple(params))
        doc_ids = doc_ids_df["doc_id"].tolist() if not doc_ids_df.empty else []
        
        # Get counts
        counts = get_related_counts(config, prefix, user_ids, doc_ids)
    
    # Display as metric cards
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("👥 Users", len(user_ids))
    with col2:
        st.metric("📄 Documents", f"{doc_count:,}")
    with col3:
        st.metric("📁 Folders", f"{counts.get('folders', 0):,}")
    with col4:
        st.metric("🧮 Embeddings", f"{counts.get('embeddings', 0):,}")
    with col5:
        st.metric("🤖 Agents", f"{counts.get('agents', 0):,}")
    
    # Embedding size warning
    if doc_ids:
        est_size = estimate_embeddings_size(config, prefix, doc_ids)
        if est_size > 500:
            st.warning(f"⚠️ Estimated embeddings size: {est_size:.1f} MB. Consider batched extraction for large datasets.")
    
    # Summary bar
    total_items = len(user_ids) + doc_count + counts.get("folders", 0) + counts.get("embeddings", 0) + counts.get("agents", 0)
    st.success(f"**Ready to migrate:** {len(user_ids)} users, {doc_count:,} documents, {counts.get('embeddings', 0):,} embeddings, {counts.get('folders', 0):,} folders, {counts.get('agents', 0):,} agents")


def render_extraction_section(config: ConnectionConfig, prefix: str, user_emails: list):
    """Render the extraction section."""
    st.markdown("---")
    st.subheader("📥 Extract Data")
    
    if not user_emails:
        st.info("Select users above to enable extraction.")
        return
    
    filters = st.session_state.get(SessionKeys.DOCUMENT_FILTERS, {})
    
    if st.button("🚀 Start Extraction", type="primary", use_container_width=True):
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
            output_dir=OUTPUT_DIR,
            progress_callback=progress_callback
        )
        
        # Run extraction
        with st.spinner("Extracting data..."):
            results = engine.run_full_extraction(
                user_emails=user_emails,
                date_from=filters.get("date_from"),
                date_to=filters.get("date_to"),
                max_doc_size=filters.get("max_size")
            )
        
        progress_bar.progress(1.0)
        status_text.text("Extraction complete!")
        
        # Store results
        st.session_state[SessionKeys.EXTRACTED_DATA] = results
        
        # Show results
        if results.get("errors"):
            for error in results["errors"]:
                st.error(error)
        else:
            st.success(f"✅ Extraction complete! Timestamp: {results['timestamp']}")
        
        # Show summary
        st.subheader("📊 Extraction Summary")
        summary_data = [
            {"Table": table, "Rows Extracted": count}
            for table, count in results.get("summary", {}).items()
        ]
        st.dataframe(pd.DataFrame(summary_data), hide_index=True)
        
        # Download buttons
        st.subheader("📥 Download Extracted Files")
        cols = st.columns(3)
        for i, (table, filepath) in enumerate(results.get("files", {}).items()):
            if os.path.exists(filepath):
                with cols[i % 3]:
                    with open(filepath, "rb") as f:
                        st.download_button(
                            label=f"📄 {table}.csv",
                            data=f,
                            file_name=os.path.basename(filepath),
                            mime="text/csv",
                            key=f"dl_{table}"
                        )
        
        # Preview expanders
        st.subheader("👁️ Data Preview")
        for table, filepath in results.get("files", {}).items():
            if os.path.exists(filepath):
                with st.expander(f"📄 {table} ({results['summary'].get(table, 0)} rows)"):
                    preview_df = pd.read_csv(filepath, nrows=100)
                    st.dataframe(preview_df, use_container_width=True)


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
    
    # Document filters
    date_from, date_to, max_size = render_document_filters(config, prefix, selected_user_ids)
    
    # Get current doc count for summary
    filters = st.session_state.get(SessionKeys.DOCUMENT_FILTERS, {})
    if selected_user_ids:
        doc_count = get_document_count_preview(
            config, prefix, selected_user_ids,
            filters.get("date_from"), filters.get("date_to"), filters.get("max_size")
        )
    else:
        doc_count = 0
    
    # Related counts
    render_related_counts(config, prefix, selected_user_ids, doc_count)
    
    # Extraction
    render_extraction_section(config, prefix, selected_emails)
    
    # Next step hint
    if SessionKeys.EXTRACTED_DATA in st.session_state:
        st.markdown("---")
        st.info("👉 **Next Step:** Go to **Transform** page to configure column mappings.")


if __name__ == "__main__":
    main()
