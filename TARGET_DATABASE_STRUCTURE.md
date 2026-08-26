# Target Database Structure - 3 Separate Databases

## Overview
The V5 target environment uses **3 separate PostgreSQL databases**, each containing tables in the **public schema**.

## Database Architecture

```
PostgreSQL Server: jeen-dev-db-migration-test:5432
├── user_db
│   └── public
│       ├── users
│       └── users_groups
│
├── document_db
│   └── public
│       ├── folders
│       ├── documents
│       └── chunks (embeddings)
│
└── completion_db
    └── public
        ├── agents
        ├── agent_settings
        ├── knowledge_bases
        ├── knowledge_base_assignments
        ├── knowledge_base_items
        ├── conversations
        ├── messages
        └── message_content_blocks
```

## Database Details

### 1. **user_db** - User Management
**Purpose**: User accounts and group management

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `users` | User accounts | id (UUID), email, firstname, lastname, organization_id |
| `users_groups` | User groups/organizations | id (UUID), group_name, default_model |

**Connection String**:
```
Host: jeen-dev-db-migration-test
Port: 5432
Database: user_db
Schema: public
```

### 2. **document_db** - Document Storage
**Purpose**: Document management and embeddings

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `folders` | Folder hierarchy | id (UUID), folder_name, user_id, parent_id |
| `documents` | Document metadata | id (UUID), blob_name, user_id, folder_id |
| `chunks` | Document chunks with embeddings | id (UUID), document_id, embedding (vector) |

**Connection String**:
```
Host: jeen-dev-db-migration-test
Port: 5432
Database: document_db
Schema: public
```

### 3. **completion_db** - AI Agents & Conversations
**Purpose**: AI agent configurations and conversation logs

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `agents` | AI agent configurations | id (UUID), name, bot_data (JSONB) |
| `agent_settings` | Agent-specific settings | agent_id, user_id, settings (JSONB) |
| `knowledge_bases` | RAG settings and document-set metadata | id (UUID), name, similarity_top_k |
| `knowledge_base_assignments` | Links knowledge bases to agents | knowledge_base_id, assigned_to_id |
| `knowledge_base_items` | Links knowledge bases to documents/folders | knowledge_base_id, item_id, item_type |
| `conversations` | Conversation headers | id (UUID), user_id, title |
| `messages` | Conversation messages | id (UUID), conversation_id, role |
| `message_content_blocks` | Message content payloads | id (UUID), message_id, content |

**Connection String**:
```
Host: jeen-dev-db-migration-test
Port: 5432
Database: completion_db
Schema: public
```

## Configuration

### .env Settings
```bash
TARGET_DB_HOST=jeen-dev-db-migration-test
TARGET_DB_PORT=5432
TARGET_DB_DATABASE=user_db              # Base connection database
TARGET_DB_USERNAME=jeen_dev_db_admin
TARGET_DB_PASSWORD=Jddb171125
TARGET_SCHEMA_MODE=databases            # IMPORTANT: Use 'databases' not 'schemas'
```

### Schema Mode Comparison

| Mode | Description | Table Reference Format | Use Case |
|------|-------------|----------------------|----------|
| **databases** | Separate databases | `public.users` | V5 architecture (3 DBs) |
| **schemas** | Schemas in one DB | `user_db.users` | Single database with multiple schemas |

## How the Loader Works

When `TARGET_SCHEMA_MODE=databases`:

1. **Base connection** is made to `TARGET_DB_DATABASE` (user_db)
2. **For each table**, the loader:
   - Reads `target_schema` from table config (e.g., "document_db")
   - Creates a new connection to that specific database
   - Loads data into `public.<table_name>` in that database

### Example: Loading folders
```python
# Table config
{
    "target_schema": "document_db",  # Database name
    "target_table": "folders"        # Table in public schema
}

# Loader connects to:
# Host: jeen-dev-db-migration-test
# Database: document_db
# Executes: INSERT INTO public.folders (...)
```

## Table-to-Database Mapping

### Migration Mapping Configuration
Located in `utils/config.py` → `DEFAULT_MAPPINGS`

```python
"users": {
    "target_table": "users",
    "target_schema": "user_db",      # Database name
}

"folders": {
    "target_table": "folders",
    "target_schema": "document_db",  # Database name
}

"agents": {
    "target_table": "agents",
    "target_schema": "completion_db", # Database name
}
```

### Load Order (Respects Foreign Keys)
```
1. users_groups   → user_db
2. users          → user_db
3. folders        → document_db
4. documents      → document_db
5. embeddings     → document_db (as chunks)
6. agents         → completion_db
```

## Cross-Database Foreign Keys

⚠️ **Important**: PostgreSQL does not support foreign key constraints across databases.

### Referential Integrity Strategy
Since tables are in separate databases, foreign key constraints cannot be enforced at the database level. Instead:

1. **Application-level validation**: Validate relationships before insert
2. **Migration mapping table**: Track old_id → new_id mappings for lookups
3. **Pre-migration validation**: Check that referenced records exist in target DBs

### Example: documents → folders relationship
```sql
-- documents table in document_db references folders.id
-- But folders.user_id references users.id in user_db
-- These FK constraints must be validated by the application
```

## Connection Credentials

All 3 databases use the **same credentials**:
- Username: `jeen_dev_db_admin`
- Password: `Jddb171125`
- Host: `jeen-dev-db-migration-test:5432`

Only the database name changes.

## Testing Connections

### Manual psql Test
```bash
# Test user_db
psql -h jeen-dev-db-migration-test -p 5432 -U jeen_dev_db_admin -d user_db
\dt public.*

# Test document_db
psql -h jeen-dev-db-migration-test -p 5432 -U jeen_dev_db_admin -d document_db
\dt public.*

# Test completion_db
psql -h jeen-dev-db-migration-test -p 5432 -U jeen_dev_db_admin -d completion_db
\dt public.*
```

### Using the UI
1. Go to http://localhost:8501 → **Target Configuration & Load**
2. Form will be pre-filled with .env values
3. Click **Test Connection** to verify connection to `user_db`
4. The loader will automatically connect to other databases as needed

## Verification Queries

After migration, verify data in each database:

```sql
-- user_db
SELECT COUNT(*) FROM public.users;
SELECT COUNT(*) FROM public.users_groups;

-- document_db
SELECT COUNT(*) FROM public.folders;
SELECT COUNT(*) FROM public.documents;
SELECT COUNT(*) FROM public.chunks;

-- completion_db
SELECT COUNT(*) FROM public.agents;
SELECT COUNT(*) FROM public.agent_settings;
SELECT COUNT(*) FROM public.knowledge_bases;
SELECT COUNT(*) FROM public.knowledge_base_assignments;
SELECT COUNT(*) FROM public.knowledge_base_items;
SELECT COUNT(*) FROM public.conversations;
SELECT COUNT(*) FROM public.messages;
SELECT COUNT(*) FROM public.message_content_blocks;
```

## Summary

✅ **3 separate databases**: user_db, document_db, completion_db  
✅ **All tables in public schema**: No schema prefixes needed  
✅ **Same credentials for all databases**: Simplified access control  
✅ **Automatic database switching**: Loader handles connections per table  
✅ **Configuration-driven**: Change `.env` to switch between modes  

---

**Last Updated**: 2026-02-28  
**Configuration File**: `.env`  
**Schema Mode**: `databases`
