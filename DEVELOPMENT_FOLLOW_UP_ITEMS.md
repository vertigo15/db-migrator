# Development Follow-Up Items

# Completed

- **Smarter User Selection** — Users sorted by document/agent count, sort dropdown, "Select Next Batch" button with configurable batch size (`DEFAULT_BATCH_SIZE`). Batch button skips already-migrated users (queries `migration_user_results` for `result = 'success'`).
- **Email Whitelist Filter** — Text input filters the user list to only show emails containing the entered text (e.g. `@company.co.il`).
- **Paste Emails** — Text area to paste a list of emails (one per line or comma-separated) to select matching users.
- **Import User Selection from Excel/CSV** — File uploader that matches emails and auto-selects users, with match report.
- **Remove LLM Prompt-Merger Flow** — Prompt parts (tone, guardrail, response) are now concatenated locally during extraction. No LLM calls, no separate UI step.
- **Support Existing V5 Users** — User INSERT now checks if email already exists in V5 before inserting. Existing users are linked (mapping only) without modifying their data. Added UNIQUE constraint on `email` in schema. Validation block at end of `01_users_*.sql` confirms all expected users are mapped.
- **Rollback Completeness** — Fixed all rollback DELETE predicates to use proper FK relationships (children via parent FK, not `id_mappings.new_id`). Added `id_mappings` writes for conversations and conversions. Fixed `agent_conversions` rollback (composite PK). Added `legacy_bot_to_agent_mapping` cleanup.
- **Persistent Connection** — Source and target DB configs are auto-loaded from `.env` on every page, eliminating the need to re-click "Test Connection" after navigation.
- **localStorage Fix** — Removed `streamlit-javascript` dependency entirely. localStorage writes now use hidden `st.components.v1.html` iframes (fire-and-forget). Reads use `st.session_state` cache. Eliminates "duplicate component_Instance" errors.
- **UI Reorganization** — Select Data page reorganized into collapsible sections: Filters (email whitelist + search + sort), Bulk Selection Tools (tabs: Next Batch / Paste Emails / Import File), and User Table.

- **Per-Step Migration Result Tracking** — Each SQL step execution updates `migration_user_results` with per-step status via `steps_completed` JSONB column. After step `01_users`, verifies against `user_db` to distinguish `reused_existing_user` from newly created. Batch is finalized (all pending → success) when all steps pass.
- **Migration History Page** — Enhanced with per-step columns, result filters, email search, CSV export button, and "Re-run Failed Users" button that sends failed emails back to Select Data page.

---

# Remaining Work

- **Rollback end-to-end verification** — Run a full migration + rollback on local target and verify zero orphan rows remain. Manual testing task.
- **Docker hardening** — Auto-create `migration` schema on container start (not just on page visit).
