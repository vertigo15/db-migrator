# DB Migrator — Project Context Reference

## What It Is
A **Streamlit-based database migration tool** that migrates data from a legacy V4 single-database PostgreSQL schema to a V5 multi-database architecture. It consists of two services:

1. **db-migrator** — Streamlit UI (port 8501) for the full migration workflow
2. **prompt-merger** — FastAPI microservice (port 8100) that uses an LLM to merge/consolidate agent prompt instructions

---

## Architecture

### V4 → V5 Migration Map
| Step | V4 Source Table | V5 Target | Notes |
|------|----------------|-----------|-------|
| 01 | `{prefix}_users` | `user_db.users` | Deterministic UUIDs; legacy fields → `metadata` JSONB |
| 02 | `{prefix}_folders` | `document_db.folders` | Topological sort for parent→child |
| 03 | `{prefix}_custom_documents` | `document_db.documents` + `document_processing` | MIME type mapping; agent-topup |
| 04 | `{prefix}` (embeddings collection) | `document_db.chunks` + `embeddings` | Split into two tables |
| 05 | `{prefix}_logs` | `completion_db.conversations` + `messages` + `message_content_blocks` | Aggregated by `chat_id` |
| 06 | `playground_bot_generator_config` | `completion_db.agents` + `agent_settings` + `knowledge_bases` + `knowledge_base_assignments` + `knowledge_base_items` | JSONB decomposed; agent document/folder choices become KB items |

### V5 Database Structure
- **`user_db`** — users
- **`document_db`** — folders, documents, document_processing, chunks, embeddings
- **`completion_db`** — agents, agent_settings, knowledge_bases, knowledge_base_assignments, knowledge_base_items, conversations, messages, message_content_blocks
- **`migration` schema** — `id_mappings`, `batch_log`, helper functions (`deterministic_uuid_v4`, `get_new_id`, `progress_summary`)

### UUID Strategy
All IDs use `migration.deterministic_uuid_v4(namespace, legacy_id)` — same input always produces same UUID v4, enabling idempotent re-runs. Separate namespaces per entity type (users / documents / everything else).

---

## Project Structure

```
db-migrator/
├── app.py                        # Main entry; loads .env, renders sidebar status + DB lineage diagram
├── pages/
│   ├── 1_connect.py              # Source DB connection, table verification, pre-migration audit, pg_dump backup
│   ├── 2_select_data.py          # Select users/groups/docs/embeddings/agents/conversations; generate SQL
│   ├── 3_target.py               # Target DB connection & verification
│   ├── 4_run_migrations.py       # Execute generated SQL files (Steps 01–06)
│   ├── 5_erase_user_data_v5.py   # Delete specific user data from V5
│   └── 6_erase_orphan_data_v5.py # Detect & delete orphaned FK records in V5
├── utils/
│   ├── config.py                 # TABLE_DEFINITIONS, SessionKeys, env-var helpers, DEFAULT_MAPPINGS
│   ├── db.py                     # DB connection & query utilities (psycopg2)
│   ├── extraction.py             # Queries V4, drives SQL generation, agent-topup logic
│   ├── sql_generator.py          # Generates numbered SQL INSERT files for all 6 steps
│   ├── transformation.py         # Data transformation helpers
│   ├── loader.py                 # Direct DB insert engine (alternative to SQL files)
│   ├── validation.py             # Data integrity checks
│   ├── audit.py                  # Pre-migration audit query engine
│   ├── pdf_export.py             # PDF report generation (reportlab)
│   └── storage.py                # Browser localStorage helpers (streamlit-javascript)
├── schemas/
│   ├── target_user_db_schema.sql
│   ├── target_document_db_schema.sql
│   ├── target_completion_db_schema.sql
│   └── migration_id_mappings.sql
├── prompt-merger/
│   ├── main.py                   # FastAPI app: /merge-prompts, /merge-prompts/batch, /health, /ready
│   ├── llm_client.py             # LLM client wrapper (OpenAI-compatible API)
│   ├── prompt_builder.py         # Builds system/user messages for prompt merging
│   ├── schemas.py                # Pydantic models: MergeRequest, BatchMergeRequest, etc.
│   └── prompts.py                # Prompt templates
├── output/
│   ├── migrations/               # Generated SQL files: 01_users_*.sql … 06_agents_*.sql
│   └── extract/                  # Optional CSV exports
├── configs/                      # Saved YAML mapping configs (persisted via Docker volume)
├── backups/                      # pg_dump backups (persisted via Docker volume)
├── Dockerfile
├── docker-compose.yml            # Two services: db-migrator + prompt-merger
├── docker-compose.airgapped.yml  # Run from pre-built image (no internet)
└── .env.example
```

---

## Key Concepts

### Table Prefix
V4 tables follow the pattern `{prefix}_users`, `{prefix}_folders`, etc. The prefix (e.g. `jeen_dev`) is configurable. The embeddings table IS the prefix itself (no suffix). Agents table `playground_bot_generator_config` has no prefix.

### Agent-Document Topup
When generating Step 06 SQL, the extraction engine scans each agent's `docs_chosen` and `chosen_docs_folders`. Any documents/folders not already in the migration plan are auto-fetched from V4 and appended to Steps 03 & 04 with `-- [agent-topup]` annotation. Stale references (records deleted from V4) are flagged as warnings and dropped.

### Session State Keys (`utils/config.py → SessionKeys`)
- `source_connection`, `target_connection`, `table_prefix`
- `resolved_tables`, `selected_users`, `selected_user_ids`
- `document_filters`, `extracted_data`, `transformed_data`
- `mapping_config`, `migration_log`, `v5_erase_config`

### Target Schema Mode
- **`databases`** — separate PostgreSQL databases (`user_db`, `document_db`, `completion_db`)
- **`schemas`** — all tables in one database under separate schemas

---

## Services & Ports

| Service | Port | Tech |
|---------|------|------|
| db-migrator (Streamlit UI) | 8501 | Python 3.11, Streamlit |
| prompt-merger (FastAPI) | 8100 | Python, FastAPI, OpenAI-compatible LLM |

---

## Environment Variables (`.env`)

| Variable | Description | Default |
|----------|-------------|---------|
| `SOURCE_DB_HOST/PORT/DATABASE/USERNAME/PASSWORD` | V4 source DB | localhost:5432 |
| `TABLE_PREFIX` | V4 table prefix | `jeen_dev` |
| `TARGET_DB_HOST/PORT/DATABASE/USERNAME/PASSWORD` | V5 target DB | localhost:5432 |
| `TARGET_SCHEMA_MODE` | `databases` or `schemas` | `schemas` |
| `DEFAULT_ORG_ID` | UUID written to migrated users | `356b50f7-...` |
| `DEFAULT_EMBEDDING_MODEL` | Model name for embeddings | `text-embedding-ada-002` |
| `LLM_PROVIDER_NAME` | prompt-merger LLM provider | `runpod_vllm` |
| `LLM_BASE_URL` | OpenAI-compatible base URL | RunPod endpoint |
| `LLM_API_KEY` | LLM API key | — |
| `LLM_MODEL` | Model name | `openai/gpt-oss-120b` |
| `LLM_TEMPERATURE` | LLM temperature | `0.1` |
| `LLM_MAX_TOKENS` | Max tokens per LLM call | `4096` |
| `PROMPT_MERGER_MAX_WORKERS` | Thread pool size | `4` |

---

## Docker

```bash
# Build and run both services
docker-compose up --build

# Airgapped (pre-built image)
docker-compose -f docker-compose.airgapped.yml up
```

Volumes persisted: `./output`, `./configs`, `./backups`

---

## Tech Stack
- **Python 3.11+**
- **Streamlit ≥ 1.30** — UI framework
- **psycopg2-binary** — PostgreSQL driver
- **pandas** — data manipulation
- **reportlab** — PDF export
- **PyYAML** — config files
- **python-dotenv** — env management
- **streamlit-javascript** — localStorage persistence
- **FastAPI** — prompt-merger microservice
- **PostgreSQL** — source (V4) and target (V5) databases

---

## Workflow Summary
1. **Connect** (`1_connect.py`) → test source DB, run pre-migration audit, optional pg_dump backup
2. **Select Data** (`2_select_data.py`) → pick users/docs/agents/conversations, configure SQL options, run extraction → generates `output/migrations/01_*.sql … 06_*.sql`
3. **Target** (`3_target.py`) → verify V5 DB structure exists
4. **Run** (`4_run_migrations.py`) → execute SQL files in order
5. **Erase User Data** (`5_erase_user_data_v5.py`) → targeted deletion from V5
6. **Erase Orphans** (`6_erase_orphan_data_v5.py`) → clean up broken FK references
