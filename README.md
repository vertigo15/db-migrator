# DB Migrator

A Streamlit-based database migration tool for migrating data from a V4 PostgreSQL schema to the V5 multi-database architecture.

## Features

- **Source Connection**: Connect to PostgreSQL V4 source database with table prefix configuration, table verification, and pg_dump backup
- **Pre-Migration Audit**: Comprehensive data quality analysis across all tables with PDF export — identifies data loss risks before migration begins
- **Granular Data Selection**: Select users, user groups, documents, embeddings, agents, and conversations independently with searchable/filterable tables
- **SQL Generation**: Generate numbered, execution-ready SQL INSERT files for all 6 migration steps
- **Agent-Document Topup**: Automatically detects and fetches documents/folders referenced by selected agents but missing from the selection
- **User ID Overrides**: Map specific V4 UUIDs to existing V5 UUIDs for users that already exist in the target
- **Target Verification**: Connect to V5 target and verify all required databases and tables exist
- **Erase User Data**: Targeted deletion of specific users and all their associated data from a V5 instance
- **Erase Orphan Data**: Detect and remove broken FK references (intra-DB and cross-DB) from V5 databases

## Project Structure

```
db-migrator/
├── app.py                       # Main Streamlit app (home + DB lineage diagram)
├── pages/
│   ├── 1_connect.py             # Source DB connection, table verification, pre-migration audit, backup
│   ├── 2_select_data.py         # User/group/document/embedding/agent/conversation selection & SQL extraction
│   ├── 3_target.py              # Target DB connection & verification
│   ├── 4_run_migrations.py      # Execute generated SQL migration files
│   ├── 5_erase_user_data_v5.py  # Delete specific user data from a V5 instance
│   └── 6_erase_orphan_data_v5.py# Detect & delete orphaned records across V5 databases
├── utils/
│   ├── config.py                # Table definitions, session keys & env-var helpers
│   ├── storage.py               # Browser localStorage helpers
│   ├── db.py                    # Database connection & query utilities
│   ├── extraction.py            # Extraction engine (queries V4, drives SQL generation)
│   ├── sql_generator.py         # SQL INSERT statement generator for all 6 steps
│   ├── transformation.py        # Transformation helpers
│   ├── loader.py                # Data loading engine for direct DB inserts
│   ├── validation.py            # Data integrity validation utilities
│   ├── audit.py                 # Pre-migration audit query engine
│   └── pdf_export.py            # PDF report generation for audit results
├── schemas/
│   ├── target_user_db_schema.sql       # V5 user_db DDL
│   ├── target_document_db_schema.sql   # V5 document_db DDL
│   ├── target_completion_db_schema.sql # V5 completion_db DDL
│   └── migration_id_mappings.sql       # migration schema (id_mappings, batch_log, helpers)
├── output/
│   ├── migrations/              # Generated SQL files (01_users_*.sql … 06_agents_*.sql)
│   └── extract/                 # Optional CSV exports
├── configs/                     # Saved YAML mapping configs
├── backups/                     # pg_dump backups
├── Dockerfile
├── docker-compose.yml
├── docker-compose.airgapped.yml # Run from a pre-built image (no internet)
└── requirements.txt
```

## Quick Start with Docker

```bash
# Copy and configure environment
cp .env.example .env
# Edit .env with your database credentials

# Build and run
docker-compose up --build

# Access the application
open http://localhost:8501
```

## Quick Start without Docker

```bash
# Install dependencies (Python 3.11+ recommended)
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your database credentials

# Run the application
streamlit run app.py
```

## Usage Workflow

### 1. Connect to Source Database (`1_connect.py`)
- Enter V4 source database connection details and table prefix (e.g. `jeen_dev` → `jeen_dev_users`)
- Click **Test Connection** to verify and resolve all expected table names with row counts
- Run the **Pre-Migration Audit** to identify data quality issues, missing references, and estimated data-loss risk across all tables; export as PDF
- Optionally create a `pg_dump` backup (full database or selected tables)

### 2. Select Data & Generate SQL (`2_select_data.py`)
- Select **users** (searchable table with checkbox selection)
- Select **user groups** associated with the chosen users
- Apply **document filters** (date range, max file size) and select individual documents
- Select **embeddings** filtered by selected documents
- Select **agents** (bots) owned by the selected users
- Select **conversations** (chat logs) for the selected users
- Configure SQL generation options:
  - **Org ID**: used as `organization_id` on all migrated users (fetched live from target `user_db` or entered manually; set `DEFAULT_ORG_ID` in `.env` for a persistent default)
  - **Embedding Model**: model name written into `embeddings.model_name` (fetched live from target `document_db` or chosen from preset list; set `DEFAULT_EMBEDDING_MODEL` in `.env`)
  - **Target embedding dimension**: truncate 1536-dim vectors to a smaller dimension (e.g. 1024)
  - **User ID Overrides**: map specific V4 UUIDs to existing V5 UUIDs for users that already have a V5 account
- Click **Start Extraction** to generate numbered SQL files under `output/migrations/`
- After extraction, an **Agent-Document Coverage** report shows auto-added (`[agent-topup]`) documents/folders and any stale references that will be dropped

### 3. Target Database (`3_target.py`)
- Enter V5 target connection details
- Choose **Target Structure**: `databases` (separate `user_db`, `document_db`, `completion_db`) or `schemas` (all in one database)
- Click **Test Connection** to verify all V5 databases and tables exist

### 4. Run Migrations (`4_run_migrations.py`)
- Review and execute the generated SQL files (Steps 01–06) in order
- Each step can be run individually or all at once
- Execution progress and row counts are displayed in real time

### 5. Erase User Data V5 (`5_erase_user_data_v5.py`)
- Connect to a V5 instance and search for users by email
- Generate a **deletion plan** showing affected row counts per table across all three databases
- Execute deletion in dependency order (children before parents) with transaction safety
- Post-delete verification confirms all rows have been removed

### 6. Erase Orphan Data V5 (`6_erase_orphan_data_v5.py`)
- Scans V5 databases for broken FK references:
  - **Intra-DB orphans**: e.g. `message_content_blocks` with no parent `message`
  - **Cross-DB orphans**: records whose `user_id` no longer exists in `user_db`
- Select which orphan types to delete and execute in safe dependency order

## V4 → V5 Migration Overview

The migration transforms a single V4 database into three separate V5 databases:

| Step | Source (V4) | Target (V5) | Key Logic |
|------|-------------|-------------|-----------|
| 01 | `{prefix}_users` | `user_db.users` | New deterministic UUIDs; legacy fields preserved in `metadata` JSONB |
| 02 | `{prefix}_folders` | `document_db.folders` | Deterministic UUIDs; topological sort preserves parent→child hierarchy |
| 03 | `{prefix}_custom_documents` | `document_db.documents` | New UUID per doc; MIME type mapping; agent-topup appends missing docs |
| 04 | `{prefix}` (embeddings) | `document_db.chunks` + `embeddings` | Single source split into two tables; chunk_index assigned per document |
| 05 | `{prefix}_logs` | `completion_db.conversations` + `messages` + `message_content_blocks` | Logs aggregated by `chat_id`; each row → user + assistant message pair |
| 06 | `playground_bot_generator_config` | `completion_db.agents` + `agent_settings` + `agent_documents` | JSONB fields decomposed; `docs_chosen` expanded via `migration.get_new_id` |

All IDs are generated using `migration.deterministic_uuid_v4(namespace, legacy_id)` so the same legacy ID always produces the same new UUID regardless of run. Each mapping is recorded in `migration.id_mappings` enabling idempotent re-runs.

See `SOURCE_TO_TARGET_MAPPING.md` for full column-level details and `AGENTS_MIGRATION_GUIDE.md` for agent-specific notes.

## Environment Variables

Copy `.env.example` to `.env` and set the following:

| Variable | Description | Default |
|----------|-------------|--------|
| `SOURCE_DB_HOST` | V4 source database host | `localhost` |
| `SOURCE_DB_PORT` | V4 source database port | `5432` |
| `SOURCE_DB_DATABASE` | V4 source database name | — |
| `SOURCE_DB_USERNAME` | V4 source database user | — |
| `SOURCE_DB_PASSWORD` | V4 source database password | — |
| `TABLE_PREFIX` | V4 table prefix (e.g. `jeen_dev`) | `jeen_dev` |
| `TARGET_DB_HOST` | V5 target database host | `localhost` |
| `TARGET_DB_PORT` | V5 target database port | `5432` |
| `TARGET_DB_DATABASE` | V5 connection database name | — |
| `TARGET_DB_USERNAME` | V5 database user | — |
| `TARGET_DB_PASSWORD` | V5 database password | — |
| `TARGET_SCHEMA_MODE` | `databases` or `schemas` | `schemas` |
| `DEFAULT_ORG_ID` | Default `organization_id` UUID written to migrated users | `356b50f7-...` |
| `DEFAULT_EMBEDDING_MODEL` | Default model name written to `embeddings.model_name` | `text-embedding-ada-002` |

## Requirements

- Python 3.11+ (Docker image uses `python:3.11-slim`)
- PostgreSQL client tools (`postgresql-client`) for `pg_dump`
- Dependencies in `requirements.txt`:
  - `streamlit>=1.30.0`
  - `streamlit-javascript>=0.1.5`
  - `psycopg2-binary>=2.9.9`
  - `pandas>=2.0.0`
  - `pyyaml>=6.0.1`
  - `python-dotenv>=1.0.0`
  - `reportlab>=4.0.0` (PDF audit export)

## License

Internal use only.
