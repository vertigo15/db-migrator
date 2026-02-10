# DB Migrator

A Streamlit-based database migration tool for migrating data from V4 to V5 schema.

## Features

- **Source Connection**: Connect to PostgreSQL source database with table prefix configuration
- **Data Selection**: Select users, filter documents by date and size, view related data counts
- **Extraction**: Extract data from source with progress tracking and CSV export
- **Transformation**: Visual column mapping editor with V4→V5 schema transformation
- **Target Loading**: Load to target database with truncate/upsert modes
- **Validation**: Data integrity checks (row counts, nulls, referential integrity)
- **Full Pipeline**: Run complete migration with real-time logging and reports

## Project Structure

```
db-migrator/
├── app.py                  # Main Streamlit app
├── pages/
│   ├── 1_connect.py        # Source DB connection
│   ├── 2_select_data.py    # Data selection & extraction
│   ├── 3_transform.py      # Column mapping configuration
│   ├── 4_target.py         # Target DB & loading
│   └── 5_run.py            # Full migration pipeline
├── utils/
│   ├── config.py           # Table definitions & constants
│   ├── storage.py          # localStorage helpers
│   ├── db.py               # Database connection utilities
│   ├── extraction.py       # Extraction engine
│   ├── transformation.py   # Transformation engine
│   ├── loader.py           # Data loading engine
│   └── validation.py       # Validation utilities
├── output/                 # Extracted/transformed CSV files
├── configs/                # Saved YAML mapping configs
├── backups/                # pg_dump backups
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## Quick Start with Docker

```bash
# Build and run
docker-compose up --build

# Access the application
open http://localhost:8501
```

## Quick Start without Docker

```bash
# Install dependencies
pip install -r requirements.txt

# Run the application
streamlit run app.py
```

## Usage Workflow

### 1. Connect to Source Database
- Navigate to the **Connect** page
- Enter source database connection details (host, port, database, username, password)
- Set the table prefix (e.g., `jeen_dev` for `jeen_dev_users`)
- Click "Test Connection" to verify and see available tables
- Optionally create a pg_dump backup

### 2. Select Data to Migrate
- Navigate to the **Select Data** page
- Use the searchable user table to select users by email
- Apply document filters (date range, max size)
- View summary counts for users, documents, folders, embeddings, agents
- Click "Start Extraction" to extract selected data to CSV files

### 3. Configure Transformations
- Navigate to the **Transform** page
- Review and edit column mappings for each table (V4 → V5)
- Fields with warnings are highlighted in yellow
- Add constant columns if needed (e.g., `source_system` = `v4`)
- Save/load mapping configurations as YAML
- Click "Run Transform" to apply mappings

### 4. Connect to Target Database
- Navigate to the **Target** page
- Enter target database connection details
- Select schema mode (schemas vs separate databases)
- View target table status
- Configure load mode per table (truncate or upsert)
- Use dry-run to preview SQL before executing

### 5. Run Migration
- Navigate to the **Run** page
- View migration status overview
- Configure run options (dry run, backup before, stop on validation fail)
- Run individual stages or full pipeline
- View validation results and migration report
- Download report as JSON or HTML

## Table Mapping (V4 → V5)

### Users (`{prefix}_users` → `users`)
| V4 Column | V5 Column | Type |
|-----------|-----------|------|
| id | id | varchar(255) |
| name | firstname | varchar(255) |
| last_name | lastname | varchar(255) |
| email | email | varchar(255) |
| phone_number | mobile_user_id | varchar(255) |
| azure_oid | organization_id | uuid |
| created_at | created_at | timestamp |

### Documents (`{prefix}_custom_documents` → `documents`)
| V4 Column | V5 Column | Type |
|-----------|-----------|------|
| doc_id | id | uuid |
| doc_name_origin | blob_name | varchar(255) |
| owner_id | user_id | uuid |
| created_at | created_at | timestamp |

*See the Transform page for complete mappings*

## Validation Checks

- **Row Count Consistency**: Extracted vs transformed counts match
- **Required Columns**: No nulls in id, email (users), doc_id (documents)
- **Referential Integrity**: Document owner_ids exist in users
- **Embedding Integrity**: Embedding doc_ids exist in documents
- **UUID Format**: Valid UUIDs where expected
- **Timestamp Format**: Parseable timestamps

## Configuration Persistence

- Connection details (except password) saved to browser localStorage
- User selections persist across browser sessions
- Mapping configurations can be saved/loaded as YAML files
- Use "Reset All Settings" in sidebar to clear localStorage

## Environment Variables (Docker)

| Variable | Description |
|----------|-------------|
| `STREAMLIT_SERVER_HEADLESS` | Run without browser opening |
| `STREAMLIT_SERVER_PORT` | Server port (default: 8501) |

## Requirements

- Python 3.9+
- PostgreSQL client tools (for pg_dump)
- Dependencies in `requirements.txt`:
  - streamlit>=1.30.0
  - streamlit-javascript>=0.1.5
  - psycopg2-binary>=2.9.9
  - pandas>=2.0.0
  - pyyaml>=6.0.1

## License

Internal use only.
