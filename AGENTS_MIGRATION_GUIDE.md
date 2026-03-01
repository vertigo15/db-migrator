# Agent Migration Guide

> **One sentence**: We take the single legacy `playground_bot_generator_config` table and split it into 3 new tables: `agents`, `agent_settings`, and `agent_documents`.

---

## Dependency Diagram

```
┌────────────────────┐
│  1. users          │  ← No dependencies (runs first)
└────────┬───────────┘
         │
    ┌────▼────────────┐
    │  2. folders     │  ← Depends on users (owner_id)
    └────┬────────────┘
         │
    ┌────▼─────────────┐
    │  3. documents    │  ← Depends on users + folders
    └────┬─────────────┘
         │
    ┌────▼──────────────────┐
    │  6. AGENTS            │  ← Depends on users + folders + documents
    │  (this migration)     │
    └───────────────────────┘

     (Also runs: 4. chunks_embeddings, 5. conversations)
```

**Critical**: Agents migration MUST run **after** users, folders, and documents migrations complete.

---

## Overview

The agents migration SQL generator (`generate_agents_migration_sql`) creates a comprehensive migration script that transforms legacy bot configurations into the new V5 agent architecture.

## Prerequisites

Run these migrations **before** `06_agents_{timestamp}.sql`:

```
1. 01_users_{timestamp}.sql        ← agent user_id lookup via migration.id_mappings
2. 02_folders_{timestamp}.sql      ← agent folder_id + chosen_docs_folders conversion
3. 03_documents_{timestamp}.sql    ← docs_chosen document ID lookup
```

## Pre-Flight Checklist

Before running the agents migration, verify:

- [ ] **Users migration completed** - Check `migration.id_mappings` has entries for table_name='users'
- [ ] **Folders migration completed** - Verify deterministic UUIDs created with same namespace
- [ ] **Documents migration completed** - Check migration.id_mappings has entries for table_name='documents'
- [ ] **Source table exists** - Confirm `playground_bot_generator_config` table exists and has data
- [ ] **Deleted filter appropriate** - Review that `WHERE deleted_at IS NULL` matches your requirements
- [ ] **UUID extension available** - Verify `uuid-ossp` extension installed: `CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`
- [ ] **Agent type logic validated** - Confirm workflow/cortex/spark detection matches business requirements
- [ ] **Target database accessible** - Verify connection to `completion_db` with write permissions
- [ ] **Namespace UUID consistent** - Confirm using `0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b` across all migrations

## Source Table

**`playground_bot_generator_config`** — one row per bot with JSONB configuration columns:

| Column | Type | Purpose |
|---|---|---|
| `bot_id` | varchar | Legacy bot identifier (not UUID) |
| `user_id` | varchar(32) | Legacy user hash — looked up via migration.id_mappings |
| `bot_data` | jsonb | `{bot_name, bot_description, ...}` |
| `toolkit_settings` | jsonb | Model config, toggles, workflow mode, RAG settings |
| `character_prompts` | jsonb | System prompt (`content`) and `model` |
| `hack_prompt` | jsonb | Fallback prompt (deprecated) |
| `analysis_prompt` | jsonb | RAG classifier prompt — `is_selected` flag |
| `grade_prompt` | jsonb | RAG ranking prompt — vector settings |
| `relevant_answer_prompt` | jsonb | RAG response prompt |
| `first_message` | text | Conversation starter text |
| `additional_links_title` | jsonb | `{is_selected: true/false}` |
| `docs_chosen` | varchar[] | Array of legacy document IDs |
| `chosen_docs_folders` | int4[] | Array of legacy folder IDs (integers) |
| `folder_id` | int4 | Bot's folder (integer) |
| `created_at` / `updated_at` / `last_activity` | timestamp | Lifecycle dates |
| `deleted_at` | timestamp | Soft delete flag — **filtered out in query** |

## Agent Type Detection

The legacy table has **no explicit type column**. Type is derived from JSONB configuration:

```sql
CASE
    WHEN toolkit_settings->>'workflow_mode' IS NOT NULL 
        THEN 'workflow'
    WHEN (analysis_prompt->>'is_selected')::boolean = true
      OR (grade_prompt->>'is_selected')::boolean = true
      OR (relevant_answer_prompt->>'is_selected')::boolean = true
        THEN 'cortex'  -- RAG/interactive bot
    ELSE 'spark'        -- Simple chat bot
END
```

## Destination Tables

### → `completion_db.public.agents`

| Destination Column | Source | Logic |
|---|---|---|
| `id` | `bot_id` | **Deterministic UUID**: `uuid_generate_v5(namespace, bot_id + '-agent')` |
| `name` | `bot_data->>'bot_name'` | Truncated to 128 chars. Fallback: `'Unnamed Agent'` |
| `description` | `bot_data->>'bot_description'` | Truncated to 2048 chars |
| `type` | _(derived)_ | `spark` / `cortex` / `workflow` |
| `user_id` | `user_id` | **Looked up** via `migration.get_new_id('users', old_user_id)` |
| `avatar_url` | `toolkit_settings` | `assistantIcon.url`, fallback to `logo_url` |
| `is_active` | `toolkit_settings->>'is_active'` | `'Yes'` → `true`, else `false` |
| `is_public` | _(hardcoded)_ | `false` |
| `is_prebuilt` | _(hardcoded)_ | `false` |
| `is_draft` | _(hardcoded)_ | `false` |
| `folder_id` | `folder_id` (int) | **Deterministic UUID**: `uuid_generate_v5(namespace, folder_id::text)` |
| `created_at` | `created_at` | Direct copy |
| `updated_at` | `updated_at` | Direct copy |
| `last_interacted_at` | `last_activity` | Direct copy |
| `deleted_at` | _(hardcoded)_ | `NULL` (deleted bots filtered out) |

### → `completion_db.public.agent_settings`

One settings row per agent (1:1 relationship, enforced by unique constraint on `agent_id`).

| Destination Column | Source | Logic |
|---|---|---|
| `id` | `bot_id` | `uuid_generate_v5(namespace, bot_id + '-settings')` |
| `agent_id` | _(above)_ | References the agent |
| `model` | `character_prompts->>'model'` | Fallback chain: character → hack → analysis → relevant_answer prompts |
| `instructions` | `character_prompts->>'content'` | Main system prompt |
| `enabled_tools` | `toolkit_settings->'data'` | JSON object keys where value = `true` → collected into JSONB array |
| `conversation_starters` | `first_message` | Single string wrapped in `["..."]` JSONB array |
| `workflow_flow_id` | _(hardcoded)_ | `NULL` |
| `base_answers_on_files_only` | `toolkit_settings->>'isAnswerBasedOnBestGrade'` | Boolean. Fallback: `relevant_answer_prompt` |
| `combines_multiple_answers` | _(hardcoded)_ | `true` |
| `retrieved_context_size` | `toolkit_settings->>'vectorsNumber'` | Integer. Fallback: `grade_prompt->'vectors'->>'vectorsNumber'` |
| `re_rank_score` | `toolkit_settings->>'passingGrade'` | **Divided by 100** (legacy 1–100 → new 0.0–1.0) |
| `query_instructions` | _(hardcoded)_ | `NULL` |
| `search_in_english` | `toolkit_settings->>'inputVectorsLanguage'` | `'To English'` → `true` |
| `show_source_links` | `toolkit_settings->'questions_selected'` | Array contains `"Display the source link"` |
| `show_source_text` | `toolkit_settings->'questions_selected'` | Array contains `"Display the source text"` |
| `follow_up_questions` | `toolkit_settings->'questions_selected'` | Array contains `"Follow-up questions"` |
| `additional_links` | `additional_links_title->>'is_selected'` | Boolean |

### → `completion_db.public.agent_documents`

Junction table linking agents to their knowledge base. Two sources:

**From `docs_chosen[]` (individual documents):**

| Destination Column | Source | Logic |
|---|---|---|
| `id` | _(derived)_ | `uuid_generate_v5(namespace, bot_id + '-doc-' + doc_id)` |
| `agent_id` | _(above)_ | References the agent |
| `document_id` | `docs_chosen` element | **Looked up** via `migration.get_new_id('documents', old_doc_id)` |
| `is_active` | _(hardcoded)_ | `true` |
| `type` | _(hardcoded)_ | `'document'` |

**From `chosen_docs_folders[]` (folders):**

| Destination Column | Source | Logic |
|---|---|---|
| `id` | _(derived)_ | `uuid_generate_v5(namespace, bot_id + '-folder-' + folder_id)` |
| `agent_id` | _(above)_ | References the agent |
| `document_id` | `chosen_docs_folders` element | **Deterministic UUID**: `uuid_generate_v5(namespace, folder_id::text)` |
| `is_active` | _(hardcoded)_ | `true` |
| `type` | _(hardcoded)_ | `'folder'` |

### → `legacy_bot_to_agent_mapping` (tracking table)

| Column | Content |
|---|---|
| `old_bot_id` | Original `bot_id` from legacy table |
| `new_agent_id` | New UUID in `agents` table |
| `agent_type` | `spark` / `cortex` / `workflow` |
| `bot_name` | Agent display name |
| `migrated_at` | Migration timestamp |

---

## Migration Process (7 Steps)

### Step 0: Create Mapping Table
Creates `legacy_bot_to_agent_mapping` for tracking and rollback.

**SQL**: `CREATE TABLE IF NOT EXISTS legacy_bot_to_agent_mapping (...)`

---

### Step 1: Build Working Table (_migration_bots)
Creates temp table that **pre-computes ALL derived fields** to avoid repeating complex logic:

**Pre-computed UUIDs**:
- `new_agent_id` - Deterministic from `bot_id + '-agent'`
- `new_settings_id` - Deterministic from `bot_id + '-settings'`
- `migrated_user_id` - Looked up via `migration.id_mappings`
- `migrated_folder_id` - Deterministic from `folder_id::text`

**Extracted JSONB fields**:
- `bot_name`, `bot_description` (from `bot_data`)
- `agent_type` (derived from `toolkit_settings`, prompts)
- `model` (fallback chain: character → hack → analysis → relevant_answer)
- `instructions` (from `character_prompts->>'content'`)
- `enabled_tools_json` (keys where value = `true`)
- `conversation_starters_json` (wrapped `first_message`)
- RAG settings: `base_answers_on_files_only`, `retrieved_context_size`, `re_rank_score`
- UI toggles: `show_source_links`, `show_source_text`, `follow_up_questions`

**Why temp table?** 
- JSONB extraction happens **once**, not 3+ times
- UUIDs computed **once**, guaranteed consistent
- Easy to debug: `SELECT * FROM _migration_bots LIMIT 10;`
- Better performance (no repeated complex operations)

**SQL**: `CREATE TEMP TABLE _migration_bots AS SELECT ... FROM playground_bot_generator_config WHERE deleted_at IS NULL;`

---

### Step 2: Insert Agents
Simple `SELECT ... FROM _migration_bots` into `completion_db.public.agents`.

**Key logic**:
- Uses pre-computed `new_agent_id`, `migrated_user_id`, `migrated_folder_id`
- `LEFT()` truncation: `name` (128 chars), `avatar_url` (512 chars)
- Timestamp fallbacks: `COALESCE(updated_at::timestamptz, created_at, now())`
- **Filter**: `WHERE migrated_user_id IS NOT NULL` (skips agents with missing users)
- **Idempotency**: `WHERE NOT EXISTS (... WHERE id = new_agent_id)`

**SQL**: `INSERT INTO completion_db.public.agents (...) SELECT ... FROM _migration_bots ...`

---

### Step 3: Insert Agent Settings
One row per agent into `completion_db.public.agent_settings` (1:1 relationship).

**Key logic**:
- Uses pre-computed `new_settings_id`, `new_agent_id`
- All JSONB extraction already done in Step 1 (from temp table)
- Explicit defaults: `enabled_tools` → `'[]'::jsonb`, `combines_multiple_answers` → `true`
- **Filter**: `WHERE migrated_user_id IS NOT NULL` (skip if agent wasn't created)
- **Idempotency**: `WHERE NOT EXISTS (... WHERE agent_id = new_agent_id)`

**SQL**: `INSERT INTO completion_db.public.agent_settings (...) SELECT ... FROM _migration_bots ...`

---

### Step 4: Insert Agent Documents (from docs_chosen)
Links agents to individual documents via many-to-many junction table.

**Key logic**:
- `CROSS JOIN LATERAL unnest(COALESCE(docs_chosen, ARRAY[]::varchar[]))` expands array
- Document UUID looked up via `migration.id_mappings` (where table_name='documents')
- Uses pre-computed `new_agent_id` from temp table
- **Empty string check**: `AND TRIM(doc_id_elem) != ''`
- **Verification**: `AND EXISTS (... FROM migration.id_mappings ...)` confirms document migrated
- **Idempotency**: Deterministic UUID prevents duplicates
- Type: `'document'::agent_documents_type_enum`

**SQL**: `INSERT INTO completion_db.public.agent_documents (...) SELECT DISTINCT ... CROSS JOIN LATERAL unnest(...)`

---

### Step 5: Insert Agent Documents (from chosen_docs_folders)
Links agents to folders (all documents in folder).

**Key logic**:
- `CROSS JOIN LATERAL unnest(COALESCE(chosen_docs_folders, ARRAY[]::int4[]))` expands array
- Folder UUID computed deterministically: `uuid_generate_v5(namespace, folder_id_elem::text)`
- **Same deterministic logic as folders migration** (consistency)
- **Verification**: `AND EXISTS (... FROM document_db.public.folders ...)` confirms folder exists
- Type: `'folder'::agent_documents_type_enum`

**SQL**: `INSERT INTO completion_db.public.agent_documents (...) SELECT DISTINCT ... CROSS JOIN LATERAL unnest(...)`

---

### Step 6: Insert into migration.id_mappings
**NEW**: Integrates with unified migration tracking system.

**Key logic**:
- Stores `agents` table mappings: `old_id` (bot_id) → `new_id` (new_agent_id)
- Migration batch: `'agents_migration'`
- Notes include agent type for traceability
- **Only tracks successfully created agents** (`WHERE migrated_user_id IS NOT NULL`)
- Enables `migration.get_new_id('agents', old_bot_id)` lookups in future migrations

**SQL**: `INSERT INTO migration.id_mappings (table_name, old_id, new_id, ...) SELECT 'agents', bot_id, new_agent_id, ... FROM _migration_bots ...`

---

### Step 7: Populate Legacy Mapping Table
Maintains backward-compatible `legacy_bot_to_agent_mapping` table for reference.

**Key logic**:
- Simple tracking table with `old_bot_id` → `new_agent_id`
- Includes `agent_type` and `bot_name` for easy queries
- `ON CONFLICT DO NOTHING` for idempotency

**SQL**: `INSERT INTO legacy_bot_to_agent_mapping (...) SELECT ... FROM _migration_bots ... ON CONFLICT (old_bot_id) DO NOTHING;`

---

## Foreign Key Enforcement

All FKs use the **migration.id_mappings** table for lookups:

| Table | FK Column | Enforcement Method |
|---|---|---|
| **agents** | user_id | `migration.get_new_id('users', old_user_id)` via subquery |
| **agents** | folder_id | Deterministic UUID (same as folders migration) |
| **agent_documents** | document_id (docs) | `migration.get_new_id('documents', old_doc_id)` via subquery |
| **agent_documents** | document_id (folders) | Deterministic UUID (verified against folders table) |

**Result**: ✅ All FKs point to correct new UUIDs, not old hashes.

---

## Idempotency

Script is fully idempotent and safe to re-run:

1. **Deterministic UUIDs** — same bot_id always generates same agent UUID
2. **NOT EXISTS checks** — skips records that already exist
3. **ON CONFLICT DO NOTHING** — mapping table uses UPSERT pattern
4. **WHERE deleted_at IS NULL** — only processes active bots

---

## What Is NOT Migrated

The following legacy fields are **intentionally excluded** from migration:

| Legacy Column | Reason | New Equivalent (if any) |
|---|---|---|
| `hack_prompt` | Old pipeline logic, deprecated | N/A |
| `analysis_prompt` (content) | Only `is_selected` flag used for type detection | RAG settings in `agent_settings` |
| `grade_prompt` (content) | Prompt text deprecated | `re_rank_score` + `retrieved_context_size` |
| `relevant_answer_prompt` (content) | Prompt text deprecated | RAG settings in `agent_settings` |
| `special_prompts` | Image prompt feature removed | N/A |
| `conversation_review_prompt` | Feature deprecated | N/A |
| `conversation_summary_prompt` | Feature deprecated | N/A |
| `end_conversation_message` | Feature deprecated | N/A |
| `error_message` | Feature deprecated | N/A |
| `password` | Security model changed | N/A |
| `tags` | Not in new schema | N/A |
| `bot_usages` | Tracked differently in V5 | Analytics system |
| `is_favorite` | Not in new schema | N/A |

---

## Verification Queries

After migration, run these in the target database:

```sql
-- Count comparison
SELECT 'legacy bots' AS source, COUNT(*) 
FROM playground_bot_generator_config 
WHERE deleted_at IS NULL
UNION ALL
SELECT 'migrated agents', COUNT(*) 
FROM legacy_bot_to_agent_mapping;

-- Type distribution
SELECT agent_type, COUNT(*) 
FROM legacy_bot_to_agent_mapping 
GROUP BY agent_type;

-- Agents without settings (should be 0)
SELECT COUNT(*) AS agents_missing_settings
FROM completion_db.public.agents a
JOIN legacy_bot_to_agent_mapping m ON m.new_agent_id = a.id
WHERE NOT EXISTS (
    SELECT 1 FROM completion_db.public.agent_settings s 
    WHERE s.agent_id = a.id
);

-- Document/folder link counts
SELECT
    m.old_bot_id,
    m.bot_name,
    COUNT(ad.id) AS linked_docs
FROM legacy_bot_to_agent_mapping m
LEFT JOIN completion_db.public.agent_documents ad ON ad.agent_id = m.new_agent_id
GROUP BY m.old_bot_id, m.bot_name
ORDER BY linked_docs DESC
LIMIT 10;
```

---

## Rollback

```sql
-- Delete in reverse FK order
DELETE FROM completion_db.public.agent_documents
WHERE agent_id IN (SELECT new_agent_id FROM legacy_bot_to_agent_mapping);

DELETE FROM completion_db.public.agent_settings
WHERE agent_id IN (SELECT new_agent_id FROM legacy_bot_to_agent_mapping);

DELETE FROM completion_db.public.agents
WHERE id IN (SELECT new_agent_id FROM legacy_bot_to_agent_mapping);

DROP TABLE legacy_bot_to_agent_mapping;
```

---

## Usage in UI

The SQL file is automatically generated when you:

1. Navigate to **"Select & Extract Data"** page
2. Select users whose agents you want to migrate
3. Click **"Extract Data"**

Generated file: `output/migrations/06_agents_{timestamp}.sql`

### Manual Extraction Column Selection

When extracting agents data, the query includes all required columns:

```sql
SELECT bot_id, user_id, bot_data, toolkit_settings, character_prompts,
       hack_prompt, analysis_prompt, grade_prompt, relevant_answer_prompt,
       first_message, additional_links_title, docs_chosen, chosen_docs_folders,
       folder_id, created_at, updated_at, last_activity, deleted_at, tags
FROM playground_bot_generator_config
WHERE deleted_at IS NULL
```

**Note**: `WHERE deleted_at IS NULL` automatically filters out deleted bots.

---

## Migration Order (Complete Pipeline)

```
00_migration_schema_setup.sql       (if needed - creates migration.id_mappings)
01_users_{timestamp}.sql            ← Required for agent user_id
02_folders_{timestamp}.sql          ← Required for folder_id + chosen_docs_folders
03_documents_{timestamp}.sql        ← Required for docs_chosen
04_chunks_embeddings_{timestamp}.sql
05_conversations_{timestamp}.sql
06_agents_{timestamp}.sql           ← This file (depends on 1, 2, 3)
```

---

## Technical Details

- **Namespace UUID**: `0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b` (same as other migrations)
- **Target Database**: `completion_db`
- **Target Schema**: `public`
- **Agent Type Enum**: `agents_type_enum` (values: `spark`, `cortex`, `workflow`)
- **Document Type Enum**: `agent_documents_type_enum` (values: `document`, `folder`)

---

## Performance Notes

- **Temp table approach** avoids repeating complex JSONB extraction
- **Indexed lookups** via `migration.id_mappings` table (fast O(1) performance)
- **Batch processing** — entire extraction done in single SQL script
- **No row-by-row loops** — uses set-based operations (CROSS JOIN LATERAL)

---

## Known Limitations

1. **Missing user IDs** — Agents with user_id not in migration.id_mappings will have NULL user_id (skipped by NOT EXISTS check)
2. **Missing folders** — Agents with folder_id not in folders table will have NULL folder_id
3. **Orphaned documents** — Documents in `docs_chosen` that weren't migrated are silently skipped
4. **Array conversion** — PostgreSQL arrays (`docs_chosen`, `chosen_docs_folders`) must be properly formatted in source

---

## Success Criteria

✅ Agent count matches legacy bot count (excluding deleted_at IS NOT NULL)
✅ Every agent has exactly 1 settings record
✅ Agent type distribution makes sense (spark > cortex > workflow typically)
✅ No NULL user_id values (indicates lookup failure)
✅ Document/folder links exist for agents that had docs_chosen or chosen_docs_folders

---

## Example Output

Migration summary shows:

```
============================================================
AGENTS MIGRATION COMPLETE
============================================================
Agents migrated: 127
Settings created: 127
Document/folder links: 845
============================================================

 agent_type | count 
------------+-------
 spark      |    89
 cortex     |    35
 workflow   |     3
```
