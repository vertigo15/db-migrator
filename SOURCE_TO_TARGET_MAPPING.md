# V4 → V5 Source-to-Target Column Mapping

This document describes every column-level mapping from v4 source tables to v5 target tables,
as implemented in `utils/sql_generator.py` and `utils/extraction.py`.

## Overview

| Item | Value |
|---|---|
| Source | `{prefix}_*` tables (e.g. `jeen_dev_users`) |
| Targets | `user_db`, `document_db`, `completion_db` |
| ID Strategy | Deterministic UUIDs via `migration.deterministic_uuid_v4(namespace, old_id)` |

**UUID Strategy:** `migration.deterministic_uuid_v4(ns, input)` wraps `uuid_generate_v5` and flips
the version nibble from `5` → `4` so all generated UUIDs pass UUID v4 validation in the target
application. The same input always produces the same output — idempotent across databases and
migration runs.

**Namespaces:**
- Users: `a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d`
- Documents: `b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e`
- Everything else (folders, agents, chunks, conversations/messages): `0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b`

**Agent-Document Topup:** Before `06_agents_*.sql` is generated, every document and folder
referenced by selected agents is checked against the migration plan. Missing records are
automatically fetched from V4 and appended to `03_documents_*.sql` and `04_chunks_embeddings_*.sql`
(annotated `[agent-topup]`). Stale references (records no longer in V4) are flagged as warnings —
those specific `agent_documents` links will be dropped at runtime.

---

## 1. users → user_db.public.users

**Source table:** `{prefix}_users`  
**SQL file:** `01_users_*.sql`  
**Prerequisite:** None

| Target Column | Source Column | Transformation |
|---|---|---|
| `id` | `id` | `migration.deterministic_uuid_v4(USER_NAMESPACE, id)` |
| `email` | `email` | Direct |
| `first_name` | `name` | Direct |
| `last_name` | `last_name` | Direct |
| `username` | `email` | `email.split('@')[0].lower().replace('.', '')` |
| `avatar_url` | — | `NULL` |
| `metadata` | Multiple | JSONB `legacyData`: `id, job, model, __group_id__, azure_oid, department, token_used, words_used, subfeatures, token_limit, company_name, phone_number, last_connected, letter_checkbox, times_connected, enabled_features, history_categories, company_name_in_hebrew` |
| `created_at` | `created_at` | Direct |
| `updated_at` | — | `now()` |
| `deleted_at` | — | `NULL` |
| `zitadel_user_id` | — | `NULL` |
| `organization_id` | — | Configurable UUID from `DEFAULT_ORG_ID` env var or Org ID field in UI (default: `356b50f7-bcbd-42aa-9392-e1605f42f7a1`) |
| `is_owner` | — | `false` (DB default — not explicitly inserted) |
| `preferred_language` | — | `NULL` (DB default — not explicitly inserted) |

**Skipped if:** `email` is NULL

**Note:** If a user already exists in V5 with a known UUID (supplied via User ID Overrides in
the UI), the INSERT is skipped and only the `migration.id_mappings` entry is registered.

---

## 2. folders → document_db.public.folders

**Source table:** `{prefix}_folders`  
**SQL file:** `02_folders_*.sql`  
**Prerequisite:** Users migrated first

| Target Column | Source Column | Transformation |
|---|---|---|
| `id` | `id` | `migration.deterministic_uuid_v4(NAMESPACE, id)` |
| `folder_name` | `folder_name` | Direct |
| `parent_id` | `parent_id` | `migration.deterministic_uuid_v4(NAMESPACE, parent_id)` or `NULL` |
| `folder_type` | `folder_type` | V4→V5 type map: `'bot'`→`'agent'`, `'document'`→`'document'`, `'agent'`→`'agent'`, else `'default'` |
| `user_id` | `owner_id` | `migration.deterministic_uuid_v4(USER_NAMESPACE, owner_id)` |
| `created_at` | `created_at` | Direct |
| `updated_at` | — | `now()` |
| `deleted_at` | — | `NULL` |

**Note:** Folders inserted in parent-first order (topological sort) to satisfy FK constraints.
Agent-topup may add extra folders (including their ancestor chain) when agents reference folders
not in the original user selection.

---

## 3. custom_documents → document_db.public.documents

**Source table:** `{prefix}_custom_documents`  
**SQL file:** `03_documents_*.sql`  
**Prerequisite:** Users and folders migrated first

| Target Column | Source Column | Transformation |
|---|---|---|
| `id` | `doc_id` | `migration.deterministic_uuid_v4(DOC_NAMESPACE, doc_id)` |
| `status` | — | Always `'UPLOADED'` |
| `file_name` | `doc_name_origin` / `doc_title` | Priority: `doc_name_origin` → `doc_title` → `'unnamed'` |
| `file_size` | `doc_size` | Integer cast |
| `storage_type` | `blob_source` | `'azure_blob'` → `'azure'`; else direct |
| `storage_path` | `doc_id` | Old `doc_id` stored as path reference |
| `storage_id` | — | `NULL` |
| `metadata` | Multiple | JSONB: `{name, source: 'legacy-migration', legacyData: {doc_id, doc_title, doc_description, doc_summery, doc_summery_modified_by, doc_summery_modified_at, tags, embedding_model, vector_methods, version, doc_checksum, data_integration_doc_metadata}}` |
| `created_at` | `created_at` | Direct |
| `updated_at` | — | `now()` |
| `deleted_at` | — | `NULL` |
| `folder_id` | `folder_id` | `migration.get_new_id('folders', folder_id)` or `NULL` |
| `user_id` | `owner_id` | `migration.deterministic_uuid_v4(USER_NAMESPACE, owner_id)` |
| `content_type` | `doc_type` | Mapped to MIME type (e.g. `pdf` → `application/pdf`) |
| `parsing_technique_id` | — | `NULL` |
| `source_type` | — | Always `'upload'` |
| `organization_id` | — | `NULL` |

**Skipped if:** `doc_id` is NULL

**Note:** Agent-topup records are appended to this file with an SQL comment
`-- [agent-topup] Auto-added: required by agent:<bot_id>`.

---

## 3b. custom_documents → document_db.public.document_processing

**Same source table and SQL file as documents** (`{prefix}_custom_documents`, `03_documents_*.sql`)  
One `document_processing` row is inserted per document, inside the same DO block, immediately after the `documents` INSERT.

**Translation detection:** `translate_to_english` is computed at SQL generation time by scanning the source `{prefix}_embeddings` rows for the document. If any chunk's `document` field contains a `translated_content:` section, the flag is set to `true`. This scan runs in Python before the SQL loop, so detection is accurate regardless of the order documents and chunks are executed on the target.

| Target Column | Source / Derivation | Value |
|---|---|---|
| `id` | `doc_id` | `migration.deterministic_uuid_v4(DOC_NAMESPACE, doc_id \|\| '-processing')` |
| `document_id` | `doc_id` | References the newly inserted `documents.id` (`v_new_doc_id`) |
| `parsing_technique_id` | — | `(SELECT id FROM public.parsing_techniques ORDER BY created_at LIMIT 1)` |
| `chunk_size` | — | `512` (hardcoded default) |
| `chunk_overlap` | — | `50` (hardcoded default) |
| `status` | — | Always `'COMPLETED'` |
| `is_active` | — | `true` |
| `translate_to_english` | `{prefix}_embeddings.document` | `true` if any chunk for this doc has `translated_content:` prefix; else `false` |
| `embedding_model_id` | — | `'00000000-0000-0000-0000-000000000001'` (schema default) |
| `is_ready` | — | `true` |
| `created_at` | — | `now()` (DB default) |

**Idempotency:** `ON CONFLICT (id) DO NOTHING` — safe to re-run.

---

## 4. embeddings → document_db.public.chunks

**Source table:** `{prefix}_embeddings`  
**SQL file:** `04_chunks_embeddings_*.sql`  
**Prerequisite:** Documents migrated first  
**Filter:** Only rows where `metadata.type = 'chunk-data'` and `metadata.doc_id` is present

| Target Column | Source Column | Transformation |
|---|---|---|
| `id` | `id` | `migration.deterministic_uuid_v4(NAMESPACE, id)` |
| `document_id` | `metadata.doc_id` | Looked up via `migration.get_new_id('documents', doc_id)` |
| `chunk_index` | — | Computed: `cumcount()` per `doc_id` (0-based) |
| `content` | `document` | Text after `'original_content:'` prefix; fallback to full `document` |
| `content_hash` | `document` | `md5(content)` computed |
| `content_type` | — | Always `'text'` |
| `page_number` | — | `NULL` |
| `char_count` | `document` | `len(content)` computed |
| `word_count` | `document` | `len(content.split())` computed |
| `metadata` | Multiple | JSONB: `{parser: 'legacy-migration', file_name: metadata.file_title, file_type, legacyData: {legacy_id, external_id, collection, type, tags, user_id, create_date, link_to_file, excerptKeywords}}` |
| `created_at` | `metadata.create_date` | Parsed as timestamptz |
| `translated_content` | `document` | Text between `'translated_content:'` and `'original_content:'`; `NULL` if absent |

---

## 5. embeddings → document_db.public.embeddings

**Same source table and SQL file as chunks** (`{prefix}_embeddings`, `04_chunks_embeddings_*.sql`)  
One embedding row generated per chunk row (only if `embeddings` column is not NULL)

| Target Column | Source Column | Transformation |
|---|---|---|
| `id` | — | `gen_random_uuid()` (random UUID v4 — embeddings have no cross-db references) |
| `chunk_id` | `id` | `migration.deterministic_uuid_v4(NAMESPACE, id)` → references `chunks.id` |
| `document_id` | `metadata.doc_id` | Looked up via `migration.get_new_id('documents', doc_id)` |
| `embedding` | `embeddings` | Cast to `vector`; optionally truncated to `target_embedding_dim` |
| `model_name` | — | Config value (default: `'text-embedding-ada-002'`) |
| `created_at` | `metadata.create_date` | Parsed as timestamptz |

---

## 6. logs → conversations + messages + message_content_blocks

**Source table:** `{prefix}_logs`  
**SQL file:** `05_conversations_*.sql`  
**Prerequisite:** Users migrated first  
**Output per row:** 1 conversation (aggregated), 2 messages (user + assistant), 2 content blocks

### 6a. → conversations

| Target Column | Source Column | Transformation |
|---|---|---|
| `id` | `chat_id` | Direct UUID |
| `title` | `title` | From last row in chat; fallback `'Conversation {chat_id[:8]}'` |
| `message_count` | — | `count(rows) × 2` |
| `total_tokens` | `token_amount` | `sum(token_amount)` across chat |
| `is_active` | — | `true` |
| `deleted_at` | — | `NULL` |
| `created_at` | `created_at` | `min(created_at)` in chat |
| `updated_at` | `created_at` | `max(created_at)` in chat |
| `last_interacted_at` | `created_at` | `max(created_at)` in chat |
| `user_id` | `user_id` | `migration.deterministic_uuid_v4(USER_NAMESPACE, user_id)` |

### 6b. → messages (user message)

| Target Column | Source Column | Transformation |
|---|---|---|
| `id` | `id` | `migration.deterministic_uuid_v4(NAMESPACE, '{id}-user')` |
| `conversation_id` | `chat_id` | Direct |
| `parent_message_id` | — | Previous turn's assistant msg ID; `NULL` for first turn |
| `role` | — | `'user'` |
| `has_tool_calls` | — | `false` |
| `iteration_count` | — | `1` |
| `content_block_count` | — | `1` |
| `finish_reason` | — | `NULL` |
| `created_at` | `created_at` | `created_at - interval '1 second'` |
| `updated_at` | `created_at` | `created_at - interval '1 second'` |
| `deleted_at` | — | `NULL` |
| `user_id` | `user_id` | `migration.deterministic_uuid_v4(USER_NAMESPACE, user_id)` |
| `metadata` | — | JSONB: `{message_order: turn_idx×2, turn_index: turn_idx}` |

### 6c. → messages (assistant message)

| Target Column | Source Column | Transformation |
|---|---|---|
| `id` | `id` | `migration.deterministic_uuid_v4(NAMESPACE, '{id}-assistant')` |
| `conversation_id` | `chat_id` | Direct |
| `parent_message_id` | — | Current turn's user message ID |
| `role` | — | `'assistant'` |
| `has_tool_calls` | — | `false` |
| `iteration_count` | — | `1` |
| `content_block_count` | — | `1` |
| `finish_reason` | — | `'stop'` |
| `created_at` | `created_at` | Direct |
| `updated_at` | `created_at` | Direct |
| `deleted_at` | — | `NULL` |
| `user_id` | `user_id` | `migration.deterministic_uuid_v4(USER_NAMESPACE, user_id)` |
| `metadata` | Multiple | JSONB: `{model: toolkit_settings.model, type, bot_id, is_like, token_amount, words_amount, calculated_time, category, sentiment, message_order: turn_idx×2+1, turn_index, legacyData: {legacy_log_id, title, toolkit_settings, sourcetext, sourcelink, webpagelink, documents_selected}}` |

### 6d. → message_content_blocks (user block)

| Target Column | Source Column | Transformation |
|---|---|---|
| `id` | `id` | `migration.deterministic_uuid_v4(NAMESPACE, '{id}-user-block-0')` |
| `message_id` | `id` | `migration.deterministic_uuid_v4(NAMESPACE, '{id}-user')` |
| `sequence` | — | `0` |
| `type` | — | `'message'` |
| `content` | `question` / `question_in_english` | JSONB: `{role:'user', type:'message', content:[{type:'text', text:<question>}]}`; `question[1].value` → fallback `question_in_english` → fallback `'[no question text]'` |
| `execution_time_ms` | — | `NULL` |
| `created_at` | `created_at` | `created_at - interval '1 second'` |

### 6e. → message_content_blocks (assistant block)

| Target Column | Source Column | Transformation |
|---|---|---|
| `id` | `id` | `migration.deterministic_uuid_v4(NAMESPACE, '{id}-assistant-block-0')` |
| `message_id` | `id` | `migration.deterministic_uuid_v4(NAMESPACE, '{id}-assistant')` |
| `sequence` | — | `0` |
| `type` | — | `'message'` |
| `content` | `answer` | JSONB: `{role:'assistant', type:'message', content:[{type:'text', text:<answer>}]}` |
| `execution_time_ms` | `calculated_time` | Direct integer or `NULL` |
| `created_at` | `created_at` | Direct |

---

## 7. playground_bot_generator_config → agents + agent_settings + agent_documents

**Source table:** `{prefix}_playground_bot_generator_config`  
**SQL file:** `06_agents_*.sql`  
**Prerequisite:** Users, folders, and documents migrated first (agent-topup guarantees all agent-referenced documents and folders are present)  
**Output per row:** 1 agent, 1 agent_settings, 0–N agent_documents

### 7a. → agents

| Target Column | Source Column | Transformation |
|---|---|---|
| `id` | `bot_id` | `migration.deterministic_uuid_v4(NAMESPACE, '{bot_id}-agent')` |
| `name` | `bot_data.bot_name` | Max 128 chars; fallback `'Unnamed Agent'` |
| `description` | `bot_data.bot_description` | Max 2048 chars |
| `type` | — | Always `'cortex'` (all legacy bots) |
| `user_id` | `user_id` | `migration.deterministic_uuid_v4(USER_NAMESPACE, user_id)` |
| `avatar_url` | `toolkit_settings` | `assistantIcon.url` or `logo_url`; max 512 chars |
| `is_active` | `toolkit_settings.is_active` | `== 'Yes'`; default `true` |
| `is_public` | — | `false` |
| `is_prebuilt` | — | `false` |
| `is_draft` | — | `false` |
| `folder_id` | `folder_id` | `migration.get_new_id('folders', folder_id)` or `NULL` |
| `created_at` | `created_at` | Direct |
| `updated_at` | `updated_at` | Direct |
| `last_interacted_at` | `last_activity` | Direct |
| `deleted_at` | — | `NULL` |

**Skipped if:** `bot_id` is NULL

### 7b. → agent_settings

| Target Column | Source Column | Transformation |
|---|---|---|
| `id` | `bot_id` | `migration.deterministic_uuid_v4(NAMESPACE, '{bot_id}-settings')` |
| `agent_id` | `bot_id` | `migration.deterministic_uuid_v4(NAMESPACE, '{bot_id}-agent')` |
| `model` | Prompts | First non-null `model` from: `character_prompts`, `hack_prompt`, `analysis_prompt`, `relevant_answer_prompt` |
| `instructions` | `character_prompts.content` | Direct text |
| `enabled_tools` | `toolkit_settings.data` | JSON array of keys where value is `'true'` |
| `conversation_starters` | `first_message` | `[first_message]` if present, else `[]` |
| `workflow_flow_id` | — | `NULL` |
| `base_answers_on_files_only` | `toolkit_settings` / `relevant_answer_prompt` | First non-null `isAnswerBasedOnBestGrade`; Boolean |
| `combines_multiple_answers` | — | `true` |
| `retrieved_context_size` | `toolkit_settings` / `grade_prompt.vectors` | First non-null `vectorsNumber`; Integer |
| `re_rank_score` | `toolkit_settings` / `grade_prompt.vectors` | First non-null `passingGrade / 100` |
| `query_instructions` | — | `NULL` |
| `search_in_english` | `toolkit_settings.inputVectorsLanguage` | `== 'To English'` |
| `show_source_links` | `toolkit_settings.questions_selected` | Contains `'Display the source link'` |
| `show_source_text` | `toolkit_settings.questions_selected` | Contains `'Display the source text'` |
| `follow_up_questions` | `toolkit_settings.questions_selected` | Contains `'Follow-up questions'` |
| `additional_links` | `additional_links_title.is_selected` | `== 'true'` |

### 7c. → agent_documents

One row per entry in `docs_chosen`, one row per entry in `chosen_docs_folders`

| Target Column | Source Column | Transformation |
|---|---|---|
| `id` | `bot_id` + doc/folder id | `migration.deterministic_uuid_v4(NAMESPACE, '{bot_id}-doc-{doc_id}')` or `migration.deterministic_uuid_v4(NAMESPACE, '{bot_id}-folder-{fid}')` |
| `agent_id` | `bot_id` | `migration.deterministic_uuid_v4(NAMESPACE, '{bot_id}-agent')` |
| `document_id` | `docs_chosen` | `migration.get_new_id('documents', doc_id)` — guaranteed present by agent-topup |
| `document_id` | `chosen_docs_folders` | `migration.get_new_id('folders', fid)` — guaranteed present by agent-topup |
| `is_active` | — | `true` |
| `type` | — | `'document'` or `'folder'` |

**Note:** Agent-topup ensures all referenced documents and folders are in the migration plan
before this SQL is generated. If a referenced record no longer exists in V4 (stale reference),
that specific `agent_documents` row is silently skipped at runtime and flagged as a warning in
the extraction UI.

---

## Migration Run Order

| Order | SQL File | Source Table | Requires |
|---|---|---|---|
| 1 | `01_users_*.sql` | `{prefix}_users` | — |
| 2 | `02_folders_*.sql` | `{prefix}_folders` | Users |
| 3 | `03_documents_*.sql` | `{prefix}_custom_documents` | Users + Folders | Also inserts `document_processing` (one row per document) |
| 4 | `04_chunks_embeddings_*.sql` | `{prefix}_embeddings` | Documents |
| 5 | `05_conversations_*.sql` | `{prefix}_logs` | Users |
| 6 | `06_agents_*.sql` | `{prefix}_playground_bot_generator_config` | Users + Folders + Documents |

**Note:** Steps 02, 03, and 04 may be regenerated automatically during step 06 extraction if
agent-topup adds missing documents or folders. Always use the latest generated files.
