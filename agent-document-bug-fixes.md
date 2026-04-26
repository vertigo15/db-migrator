# Agent-Document Connection Bug Fixes

Two bugs prevent agent-document links from being created during migration.

## Bug 1: Cross-Database Lookup Failure

**File:** `utils/sql_generator.py` — function `generate_agent_insert`

**Root cause:** When `TARGET_SCHEMA_MODE=databases`, the agent migration SQL (`06_agents_*.sql`) runs against `completion_db`. However, the code uses `migration.get_new_id('documents', ...)` and `migration.get_new_id('folders', ...)` which query `migration.id_mappings` — a table that only exists in the database where documents/folders were migrated (`document_db`). Since the lookup runs in `completion_db`, it always returns NULL, and the `IF ... IS NOT NULL` guard skips every INSERT.

**Fix:** Replace `migration.get_new_id(...)` with `migration.deterministic_uuid_v4(...)` to compute the target UUID directly (same deterministic function used when the document/folder was originally inserted). This avoids any cross-database lookup.

### 1a. `agents.folder_id` (line ~2446)

**Before:**
```python
folder_id_sql = f"migration.get_new_id('folders', {escape_sql_string(folder_id)})"
```

**After:**
```python
folder_id_sql = f"migration.deterministic_uuid_v4('{namespace_uuid}'::uuid, {escape_sql_string(folder_id)})"
```

### 1b. `agent_documents` document links (line ~2530)

**Before:**
```python
doc_inserts += f"""
    -- Link document: {doc_id} (skip if document wasn't migrated)
    IF migration.get_new_id('documents', {escape_sql_string(doc_id)}) IS NOT NULL THEN
        INSERT INTO agent_documents (id, agent_id, document_id, is_active, type)
        SELECT
            migration.deterministic_uuid_v4('{namespace_uuid}'::uuid, '{bot_id}-doc-{doc_id}'),
            v_agent_id,
            migration.get_new_id('documents', {escape_sql_string(doc_id)}),
            true,
            'document'::agent_documents_type_enum
        WHERE NOT EXISTS (
            SELECT 1 FROM agent_documents
            WHERE id = migration.deterministic_uuid_v4('{namespace_uuid}'::uuid, '{bot_id}-doc-{doc_id}')
        );
        v_docs_linked := v_docs_linked + 1;
    ELSE
        RAISE NOTICE 'Agent {bot_id}: skipping document link {doc_id} — document not migrated';
    END IF;
"""
```

**After:**
```python
doc_inserts += f"""
    -- Link document: {doc_id}
    INSERT INTO agent_documents (id, agent_id, document_id)
    SELECT
        migration.deterministic_uuid_v4('{namespace_uuid}'::uuid, '{bot_id}-doc-{doc_id}'),
        v_agent_id,
        migration.deterministic_uuid_v4('{DOC_NAMESPACE_UUID}'::uuid, {escape_sql_string(doc_id)})
    WHERE NOT EXISTS (
        SELECT 1 FROM agent_documents
        WHERE id = migration.deterministic_uuid_v4('{namespace_uuid}'::uuid, '{bot_id}-doc-{doc_id}')
    );
    v_docs_linked := v_docs_linked + 1;
"""
```

Note: `DOC_NAMESPACE_UUID = 'b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e'` (defined at top of `sql_generator.py`).

### 1c. `agent_documents` folder links (line ~2553)

**Before:**
```python
doc_inserts += f"""
    -- Link folder: {fid} (skip if folder wasn't migrated)
    IF migration.get_new_id('folders', {escape_sql_string(fid)}) IS NOT NULL THEN
        INSERT INTO agent_documents (id, agent_id, document_id, is_active, type)
        SELECT
            migration.deterministic_uuid_v4('{namespace_uuid}'::uuid, '{bot_id}-folder-{fid}'),
            v_agent_id,
            migration.get_new_id('folders', {escape_sql_string(fid)}),
            true,
            'folder'::agent_documents_type_enum
        WHERE NOT EXISTS (
            SELECT 1 FROM agent_documents
            WHERE id = migration.deterministic_uuid_v4('{namespace_uuid}'::uuid, '{bot_id}-folder-{fid}')
        );
        v_docs_linked := v_docs_linked + 1;
    ELSE
        RAISE NOTICE 'Agent {bot_id}: skipping folder link {fid} — folder not migrated';
    END IF;
"""
```

**After:**
```python
doc_inserts += f"""
    -- Link folder: {fid}
    INSERT INTO agent_documents (id, agent_id, document_id)
    SELECT
        migration.deterministic_uuid_v4('{namespace_uuid}'::uuid, '{bot_id}-folder-{fid}'),
        v_agent_id,
        migration.deterministic_uuid_v4('{NAMESPACE_UUID}'::uuid, {escape_sql_string(fid)})
    WHERE NOT EXISTS (
        SELECT 1 FROM agent_documents
        WHERE id = migration.deterministic_uuid_v4('{namespace_uuid}'::uuid, '{bot_id}-folder-{fid}')
    );
    v_docs_linked := v_docs_linked + 1;
"""
```

Note: `NAMESPACE_UUID = '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'` (used for folders, agents, chunks, embeddings).

---

## Bug 2: Column Mismatch in `agent_documents` INSERT

**File:** `utils/sql_generator.py` — function `generate_agent_insert`

**Root cause:** The INSERT statement specifies columns `(id, agent_id, document_id, is_active, type)` and uses `'document'::agent_documents_type_enum` / `'folder'::agent_documents_type_enum`. But the actual target schema (`schemas/target_completion_db_schema.sql`) defines `agent_documents` with only:

```sql
CREATE TABLE IF NOT EXISTS public.agent_documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL,
    document_id UUID NOT NULL,
    added_at TIMESTAMP DEFAULT now(),
    CONSTRAINT uq_agent_document UNIQUE (agent_id, document_id),
    CONSTRAINT fk_agent_documents_agent
        FOREIGN KEY (agent_id) REFERENCES public.agents(id)
        ON DELETE CASCADE ON UPDATE CASCADE
);
```

The columns `is_active` and `type` don't exist, and the enum `agent_documents_type_enum` doesn't exist. Before Bug 1's fix, this error was masked because the `IF get_new_id(...) IS NOT NULL` guard always evaluated to false (due to Bug 1), so the INSERT was never actually attempted.

**Fix:** Remove `is_active` and `type` from the INSERT — only use `(id, agent_id, document_id)`. The `added_at` column defaults to `now()`.

This fix is already shown in the "After" code blocks above (Bug 1b and 1c).
