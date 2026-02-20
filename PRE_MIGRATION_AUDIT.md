# Pre-Migration Audit Documentation

This document explains the Pre-Migration Audit feature, which analyzes your V4 source database before migration to identify potential data issues, estimate migration risks, and provide baseline statistics.

## Overview

The Pre-Migration Audit runs a comprehensive set of queries against your source database to:
- Identify data quality issues that may cause migration failures
- Estimate how many rows will be skipped due to missing references
- Provide baseline statistics for post-migration validation
- Help you understand your data distribution and usage patterns

---

## Section 1: Overall Counts

### What it Calculates
Displays row counts for each source table from the Table Verification step.

### Why it Matters
- Provides a quick overview of your database size
- Baseline numbers to compare against post-migration counts
- Helps estimate migration duration and resource requirements

---

## Section 2: User Analytics

### What it Calculates

| Metric | Description |
|--------|-------------|
| **Top 10 Users by Logs** | Users with the most chat activity (conversation turns) |
| **Top 10 Users by Documents** | Users who uploaded the most documents |
| **Top 10 Users by Chunks** | Users with the most embeddings/chunks |
| **Users Without Email** | Users missing email addresses |
| **Username Collisions** | Email prefixes shared across multiple domains |

### Why it Matters

- **Top Users**: Identifies power users whose data is most critical to migrate correctly
- **Users Without Email**: These users will be **SKIPPED** during migration because email is required for the V5 schema. You need to either:
  - Add email addresses before migration
  - Accept that these users won't be migrated
- **Username Collisions**: Multiple users with the same email prefix (e.g., `john@company1.com` and `john@company2.com`) may cause confusion or conflicts

---

## Section 3: Folder Analytics

### What it Calculates

| Metric | Description |
|--------|-------------|
| **Folder Hierarchy Depth** | Count of folders at each nesting level (1=root, 2=child, etc.) |
| **Folder Type Distribution** | Breakdown of folder types (default, shared, etc.) |
| **Orphaned Folders** | Folders whose `parent_id` references a non-existent folder |

### Why it Matters

- **Hierarchy Depth**: The migration preserves parent-child relationships using deterministic UUID generation. Deep hierarchies (3+ levels) are more complex to migrate and validate
- **Orphaned Folders**: These indicate data corruption. Folders with missing parents will have their `parent_id` point to a non-existent UUID after migration, potentially causing issues
- **Folder Types**: Helps understand your folder structure and verify type mapping is correct

---

## Section 4: Document Analytics

### What it Calculates

| Metric | Description |
|--------|-------------|
| **Document Type Distribution** | Breakdown of file types (pdf, docx, txt, etc.) |
| **Problematic Doc Types** | Types that will become `application/octet-stream` |
| **Blob Source Distribution** | Where documents are stored (azure, local, etc.) |
| **Orphaned Documents** | Documents whose `owner_id` doesn't exist in users table |
| **Documents Missing Folders** | Documents referencing non-existent folders |
| **Duplicate doc_ids** | Multiple documents with the same ID |

### Why it Matters

- **Problematic Doc Types**: Unknown file types get mapped to `application/octet-stream`, which may affect how they're handled in V5. Consider adding custom mappings
- **Orphaned Documents**: These will be **SKIPPED** because we can't link them to a V5 user
- **Missing Folders**: Documents will be migrated but their `folder_id` will reference a non-existent folder
- **Duplicate doc_ids**: Could cause primary key conflicts. Investigate and deduplicate before migration

---

## Section 5: Chunks & Embeddings Analytics

### What it Calculates

| Metric | Description |
|--------|-------------|
| **Chunks per Document** | Distribution showing documents with most chunks |
| **Chunk Type Distribution** | Types in the embeddings table (should mostly be `chunk-data`) |
| **Embedding Dimensions** | Vector dimension counts (sanity check) |
| **Orphaned Chunks** | Chunks referencing documents that don't exist |
| **Chunks Without Embeddings** | Chunks with NULL embedding vectors |

### Why it Matters

- **Chunks per Document**: Large documents may have thousands of chunks. Helps estimate embedding migration size
- **Orphaned Chunks**: Will be **SKIPPED** because we can't link them to a V5 document
- **Chunks Without Embeddings**: These will create chunk records but no embedding records. May need re-embedding in V5
- **Embedding Dimensions**: All vectors should have the same dimension (typically 1024 for bge-m3). Mixed dimensions indicate a problem

---

## Section 6: Conversation Analytics

### What it Calculates

| Metric | Description |
|--------|-------------|
| **Top 10 Users by Conversations** | Most active conversation users |
| **Conversation Size Distribution** | Breakdown by turn count (1, 2-5, 6-20, etc.) |
| **Model Usage Distribution** | Which AI models were used |
| **Bot/Agent Usage** | Usage by bot_id and conversation type |
| **Token Statistics** | Total, average, median, p95 token usage |
| **Logs Without User** | Logs with NULL user_id |
| **Logs Without Chat ID** | Logs with NULL/empty chat_id |
| **Invalid Chat ID Format** | Chat IDs that aren't valid UUIDs |
| **Question Extraction Issues** | Logs where question JSON parsing fails |
| **Orphaned Logs** | Logs referencing users that don't exist |

### Why it Matters

- **Model/Bot Usage**: Helps understand which models and agents are used, useful for V5 configuration
- **Token Statistics**: Baseline for usage tracking and billing comparison
- **Logs Without User/Chat ID**: Will be **SKIPPED** - these are incomplete records
- **Invalid Chat IDs**: May cause UUID parsing errors during migration
- **Question Extraction Issues**: The migration extracts user questions from `question::jsonb->1->>'value'`. Rows where this fails will have missing question text
- **Orphaned Logs**: Will be **SKIPPED** because we can't link them to a V5 user

---

## Section 7: Cross-Table Integrity (DATA LOSS RISK)

### What it Calculates

| Metric | Description |
|--------|-------------|
| **Data Loss Risk Summary** | Count of rows that will be SKIPPED per table |
| **Missing User References** | Which tables reference non-existent users |

### Why it Matters

**This is the most critical section.** It shows exactly how much data you will lose during migration due to referential integrity issues.

| Risk | Impact |
|------|--------|
| Documents without valid user | Document won't be migrated |
| Folders without valid user | Folder won't be migrated |
| Chunks without valid document | Chunk/embedding won't be migrated |
| Logs without valid user | Conversation won't be migrated |
| Users without email | User and all their data won't be migrated |

**Action Required**: If you see significant numbers here, investigate and fix the data before migration:
- Add missing email addresses to users
- Delete orphaned records
- Reassign data to valid users

---

## SQL Queries Reference

Below are all the SQL queries used by the Pre-Migration Audit. Replace `{prefix}` with your actual table prefix (e.g., `jeen_dev`).

### Section 2: User Analytics

#### Top 10 Users by Log Count
```sql
SELECT
    u.id AS legacy_user_id,
    u.name AS user_name,
    TRIM(u.email) AS email,
    COUNT(l.id) AS total_log_entries,
    COUNT(DISTINCT l.chat_id) AS total_conversations
FROM public.{prefix}_logs l
JOIN public.{prefix}_users u ON u.id = l.user_id
WHERE l.user_id IS NOT NULL
GROUP BY u.id, u.name, u.email
ORDER BY total_log_entries DESC
LIMIT 10;
```

#### Top 10 Users by Document Count
```sql
SELECT
    u.id AS legacy_user_id,
    u.name AS user_name,
    TRIM(u.email) AS email,
    COUNT(d.doc_id) AS total_documents,
    SUM(COALESCE(d.doc_size, 0)) AS total_doc_size_bytes
FROM public.{prefix}_custom_documents d
JOIN public.{prefix}_users u ON u.id = d.owner_id
GROUP BY u.id, u.name, u.email
ORDER BY total_documents DESC
LIMIT 10;
```

#### Top 10 Users by Chunk Count
```sql
SELECT
    u.id AS legacy_user_id,
    u.name AS user_name,
    TRIM(u.email) AS email,
    COUNT(c.id) AS total_chunks
FROM public.{prefix} c
JOIN public.{prefix}_users u ON u.id = c.metadata->>'user_id'
WHERE c.metadata->>'type' = 'chunk-data'
GROUP BY u.id, u.name, u.email
ORDER BY total_chunks DESC
LIMIT 10;
```

#### Users Without Email
```sql
SELECT
    id AS legacy_user_id,
    name AS user_name,
    email
FROM public.{prefix}_users
WHERE TRIM(COALESCE(email, '')) = '';
```

#### Username Collisions
```sql
SELECT
    SPLIT_PART(TRIM(email), '@', 1) AS username_prefix,
    COUNT(*) AS user_count,
    ARRAY_AGG(TRIM(email)) AS emails
FROM public.{prefix}_users
WHERE TRIM(COALESCE(email, '')) != ''
GROUP BY SPLIT_PART(TRIM(email), '@', 1)
HAVING COUNT(*) > 1
ORDER BY user_count DESC;
```

### Section 3: Folder Analytics

#### Folder Hierarchy Depth
```sql
WITH RECURSIVE folder_tree AS (
    SELECT id, folder_name, parent_id, 1 AS depth
    FROM public.{prefix}_folders
    WHERE parent_id IS NULL
    
    UNION ALL
    
    SELECT f.id, f.folder_name, f.parent_id, ft.depth + 1
    FROM public.{prefix}_folders f
    JOIN folder_tree ft ON f.parent_id = ft.id
)
SELECT depth, COUNT(*) AS folder_count
FROM folder_tree
GROUP BY depth
ORDER BY depth;
```

#### Folder Type Distribution
```sql
SELECT
    COALESCE(folder_type, '(null)') AS folder_type,
    COUNT(*) AS folder_count
FROM public.{prefix}_folders
GROUP BY folder_type
ORDER BY folder_count DESC;
```

#### Orphaned Folders
```sql
SELECT
    f.id,
    f.folder_name,
    f.parent_id AS missing_parent_id
FROM public.{prefix}_folders f
WHERE f.parent_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM public.{prefix}_folders p WHERE p.id = f.parent_id
  );
```

### Section 4: Document Analytics

#### Document Type Distribution
```sql
SELECT
    COALESCE(TRIM(doc_type), '(null)') AS doc_type,
    COUNT(*) AS doc_count
FROM public.{prefix}_custom_documents
GROUP BY TRIM(doc_type)
ORDER BY doc_count DESC;
```

#### Problematic Doc Types
```sql
SELECT
    TRIM(doc_type) AS doc_type,
    COUNT(*) AS doc_count,
    (SELECT ARRAY_AGG(sub.doc_id) FROM (
        SELECT doc_id FROM public.{prefix}_custom_documents d2 
        WHERE TRIM(d2.doc_type) = TRIM(d.doc_type) 
        ORDER BY doc_id LIMIT 3
    ) sub) AS sample_doc_ids
FROM public.{prefix}_custom_documents d
WHERE TRIM(LOWER(doc_type)) NOT IN (
    'pdf','docx','pptx','xlsx','doc','ppt','xls','txt','csv','html','json',
    'png','jpg','jpeg','gif','svg','webp','md','mp3','mp4',
    'application/pdf','image/png','image/jpeg'
)
GROUP BY TRIM(doc_type)
ORDER BY doc_count DESC;
```

#### Blob Source Distribution
```sql
SELECT
    COALESCE(blob_source, '(null)') AS blob_source,
    COUNT(*) AS doc_count
FROM public.{prefix}_custom_documents
GROUP BY blob_source
ORDER BY doc_count DESC;
```

#### Orphaned Documents
```sql
SELECT COUNT(*) AS orphaned_docs
FROM public.{prefix}_custom_documents d
WHERE NOT EXISTS (
    SELECT 1 FROM public.{prefix}_users u WHERE u.id = d.owner_id
);
```

#### Documents Missing Folders
```sql
SELECT COUNT(*) AS docs_with_missing_folder
FROM public.{prefix}_custom_documents d
WHERE d.folder_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM public.{prefix}_folders f WHERE f.id = d.folder_id
  );
```

#### Duplicate doc_ids
```sql
SELECT doc_id, COUNT(*) AS occurrences
FROM public.{prefix}_custom_documents
GROUP BY doc_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;
```

### Section 5: Chunks & Embeddings Analytics

#### Chunks per Document (Top 20)
```sql
SELECT
    metadata->>'doc_id' AS doc_id,
    COUNT(*) AS chunk_count
FROM public.{prefix}
WHERE metadata->>'type' = 'chunk-data'
GROUP BY metadata->>'doc_id'
ORDER BY chunk_count DESC
LIMIT 20;
```

#### Orphaned Chunks
```sql
SELECT
    COUNT(*) AS orphaned_chunks,
    COUNT(DISTINCT metadata->>'doc_id') AS orphaned_doc_ids
FROM public.{prefix} c
WHERE c.metadata->>'type' = 'chunk-data'
  AND NOT EXISTS (
    SELECT 1 FROM public.{prefix}_custom_documents d
    WHERE d.doc_id = c.metadata->>'doc_id'
  );
```

#### Chunks Without Embeddings
```sql
SELECT COUNT(*) AS chunks_without_embeddings
FROM public.{prefix}
WHERE metadata->>'type' = 'chunk-data'
  AND embeddings IS NULL;
```

#### Embedding Vector Dimensions
```sql
SELECT
    array_length(embeddings::text::float[], 1) AS vector_dimension,
    COUNT(*) AS chunk_count
FROM public.{prefix}
WHERE metadata->>'type' = 'chunk-data'
  AND embeddings IS NOT NULL
GROUP BY array_length(embeddings::text::float[], 1)
ORDER BY chunk_count DESC
LIMIT 5;
```

#### Chunk Type Distribution
```sql
SELECT
    COALESCE(metadata->>'type', '(null)') AS chunk_type,
    COUNT(*) AS row_count
FROM public.{prefix}
GROUP BY metadata->>'type'
ORDER BY row_count DESC;
```

### Section 6: Conversation Analytics

#### Top 10 Users by Conversations
```sql
SELECT
    l.user_id AS legacy_user_id,
    u.name AS user_name,
    COUNT(DISTINCT l.chat_id) AS conversation_count,
    COUNT(*) AS total_messages
FROM public.{prefix}_logs l
LEFT JOIN public.{prefix}_users u ON u.id = l.user_id
WHERE l.user_id IS NOT NULL
GROUP BY l.user_id, u.name
ORDER BY total_messages DESC
LIMIT 10;
```

#### Conversation Size Distribution
```sql
SELECT
    CASE
        WHEN cnt = 1      THEN '1 turn'
        WHEN cnt <= 5     THEN '2-5 turns'
        WHEN cnt <= 20    THEN '6-20 turns'
        WHEN cnt <= 50    THEN '21-50 turns'
        WHEN cnt <= 100   THEN '51-100 turns'
        ELSE '100+ turns'
    END AS turn_range,
    COUNT(*) AS conversation_count
FROM (
    SELECT chat_id, COUNT(*) AS cnt
    FROM public.{prefix}_logs
    WHERE user_id IS NOT NULL AND chat_id IS NOT NULL
    GROUP BY chat_id
) sub
GROUP BY
    CASE
        WHEN cnt = 1      THEN '1 turn'
        WHEN cnt <= 5     THEN '2-5 turns'
        WHEN cnt <= 20    THEN '6-20 turns'
        WHEN cnt <= 50    THEN '21-50 turns'
        WHEN cnt <= 100   THEN '51-100 turns'
        ELSE '100+ turns'
    END
ORDER BY MIN(cnt);
```

#### Logs Without User
```sql
SELECT
    COUNT(*) AS logs_without_user,
    COUNT(DISTINCT chat_id) AS conversations_affected
FROM public.{prefix}_logs
WHERE user_id IS NULL;
```

#### Logs Without Chat ID
```sql
SELECT COUNT(*) AS logs_without_chat_id
FROM public.{prefix}_logs
WHERE chat_id IS NULL OR TRIM(chat_id::text) = '';
```

#### Invalid Chat ID Format
```sql
SELECT
    chat_id::text,
    'INVALID UUID FORMAT' AS issue
FROM public.{prefix}_logs
WHERE user_id IS NOT NULL
  AND chat_id IS NOT NULL
  AND chat_id::text !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
GROUP BY chat_id
LIMIT 20;
```

#### Question JSON Extraction Validation
```sql
SELECT
    id AS legacy_log_id,
    question::jsonb->1->>'value' AS extracted_user_question,
    LEFT(answer, 80) AS answer_preview,
    jsonb_array_length(question::jsonb) AS history_length
FROM public.{prefix}_logs
WHERE user_id IS NOT NULL
  AND chat_id IS NOT NULL
  AND (
    question::jsonb->1->>'value' IS NULL
    OR TRIM(question::jsonb->1->>'value') = ''
  )
LIMIT 20;
```

#### Orphaned Logs
```sql
SELECT
    COUNT(*) AS orphaned_logs,
    COUNT(DISTINCT user_id) AS orphaned_user_ids
FROM public.{prefix}_logs l
WHERE l.user_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM public.{prefix}_users u WHERE u.id = l.user_id
  );
```

#### Model Usage Distribution
```sql
SELECT
    COALESCE(
        (toolkit_settings::jsonb->>'model'),
        '(unknown)'
    ) AS model_name,
    COUNT(*) AS usage_count
FROM public.{prefix}_logs
WHERE user_id IS NOT NULL
GROUP BY (toolkit_settings::jsonb->>'model')
ORDER BY usage_count DESC;
```

#### Bot/Agent Usage Distribution
```sql
SELECT
    COALESCE(bot_id::text, '(none)') AS bot_id,
    COALESCE(type, '(none)') AS conversation_type,
    COUNT(*) AS log_count,
    COUNT(DISTINCT chat_id) AS conversation_count
FROM public.{prefix}_logs
WHERE user_id IS NOT NULL
GROUP BY bot_id, type
ORDER BY log_count DESC
LIMIT 20;
```

#### Token Statistics
```sql
SELECT
    COUNT(*) AS total_turns,
    SUM(token_amount)::bigint AS total_tokens,
    ROUND(AVG(token_amount), 0) AS avg_tokens_per_turn,
    MIN(token_amount) AS min_tokens,
    MAX(token_amount) AS max_tokens,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY token_amount) AS median_tokens,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY token_amount) AS p95_tokens
FROM public.{prefix}_logs
WHERE user_id IS NOT NULL
  AND token_amount IS NOT NULL;
```

### Section 7: Cross-Table Integrity

#### Missing User References by Table
```sql
SELECT DISTINCT d.owner_id AS missing_user_id, 'documents' AS referenced_from
FROM public.{prefix}_custom_documents d
WHERE NOT EXISTS (SELECT 1 FROM public.{prefix}_users u WHERE u.id = d.owner_id)

UNION ALL

SELECT DISTINCT f.owner_id, 'folders'
FROM public.{prefix}_folders f
WHERE NOT EXISTS (SELECT 1 FROM public.{prefix}_users u WHERE u.id = f.owner_id)

UNION ALL

SELECT DISTINCT l.user_id, 'logs'
FROM public.{prefix}_logs l
WHERE l.user_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM public.{prefix}_users u WHERE u.id = l.user_id)

UNION ALL

SELECT DISTINCT c.metadata->>'user_id', 'chunks'
FROM public.{prefix} c
WHERE c.metadata->>'type' = 'chunk-data'
  AND NOT EXISTS (SELECT 1 FROM public.{prefix}_users u WHERE u.id = c.metadata->>'user_id')

ORDER BY referenced_from, missing_user_id;
```

#### Data Loss Risk Summary
```sql
SELECT
    'documents without valid user' AS risk,
    COUNT(*) AS rows_at_risk
FROM public.{prefix}_custom_documents d
WHERE NOT EXISTS (SELECT 1 FROM public.{prefix}_users u WHERE u.id = d.owner_id)

UNION ALL

SELECT
    'folders without valid user',
    COUNT(*)
FROM public.{prefix}_folders f
WHERE NOT EXISTS (SELECT 1 FROM public.{prefix}_users u WHERE u.id = f.owner_id)

UNION ALL

SELECT
    'chunks without valid document',
    COUNT(*)
FROM public.{prefix} c
WHERE c.metadata->>'type' = 'chunk-data'
  AND NOT EXISTS (
    SELECT 1 FROM public.{prefix}_custom_documents d
    WHERE d.doc_id = c.metadata->>'doc_id'
  )

UNION ALL

SELECT
    'logs without valid user',
    COUNT(*)
FROM public.{prefix}_logs l
WHERE l.user_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM public.{prefix}_users u WHERE u.id = l.user_id)

UNION ALL

SELECT
    'users without email (skipped)',
    COUNT(*)
FROM public.{prefix}_users
WHERE TRIM(COALESCE(email, '')) = ''

ORDER BY rows_at_risk DESC;
```

---

## Running the Audit

1. Navigate to the **Connect** page
2. Connect to your source database
3. Verify tables are found
4. Click **"📊 Calculate Audit Statistics"**
5. Review each section, paying special attention to **Section 7: Cross-Table Integrity**
6. Address any critical issues before proceeding with migration

## Recommended Actions

| Finding | Action |
|---------|--------|
| Users without email | Add email addresses or accept data loss |
| Orphaned documents/folders/logs | Delete orphans or reassign to valid users |
| Duplicate doc_ids | Investigate and deduplicate |
| Orphaned chunks | Delete or reassign to valid documents |
| Invalid chat_id format | May need manual data cleanup |
