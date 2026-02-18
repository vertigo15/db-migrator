-- Migration script: jeen_dev_folders -> user_db.public.folders
--
-- Key challenges:
--   1. id: integer -> uuid (need a deterministic mapping for document references)
--   2. owner_id: varchar(64) legacy hash -> uuid (must lookup from migrated users)
--   3. parent_id: always NULL (flat folder structure in destination)
--   4. folder_type: varchar -> enum with value mapping:
--        conversation -> default
--        bot          -> agent
--        default      -> default
--        document     -> document
--
-- Strategy: Use uuid_generate_v5 (deterministic UUID) with a namespace to convert
-- integer folder IDs to UUIDs so document references remain valid.
-- Parent-child hierarchy is flattened (parent_id always NULL).

-- Step 0: Ensure uuid-ossp extension is available
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- We use a fixed namespace UUID for deterministic id generation from legacy integer IDs
-- This ensures parent_id references remain consistent
-- Namespace: '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b' (arbitrary but fixed)

INSERT INTO user_db.public.folders (
    id,
    folder_name,
    parent_id,
    folder_type,
    user_id,
    created_at,
    updated_at,
    deleted_at
)
SELECT
    -- Deterministic UUID from legacy integer id
    uuid_generate_v5(
        '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid,
        src.id::text
    )                                                       AS id,

    src.folder_name                                         AS folder_name,

    -- Parent_id is always NULL (flat structure in destination)
    NULL                                                    AS parent_id,

    -- Map legacy folder_type to destination enum values
    (CASE TRIM(LOWER(COALESCE(src.folder_type, 'default')))
        WHEN 'conversation' THEN 'default'
        WHEN 'bot'          THEN 'agent'
        WHEN 'default'      THEN 'default'
        WHEN 'document'     THEN 'document'
        ELSE 'default'
    END)::public.folders_folder_type_enum                   AS folder_type,

    -- Lookup the new UUID user_id from the migrated users table via legacy id in metadata
    u.id                                                    AS user_id,

    src.created_at AT TIME ZONE 'UTC'                       AS created_at,
    src.updated_at AT TIME ZONE 'UTC'                       AS updated_at,
    NULL                                                    AS deleted_at

FROM public.jeen_dev_folders src
-- Join to migrated users: owner_id in source matches legacyData.id in destination metadata
JOIN user_db.public.users u
    ON u.metadata->'legacyData'->>'id' = src.owner_id
WHERE NOT EXISTS (
    -- Skip already-migrated folders
    SELECT 1 FROM user_db.public.folders f
    WHERE f.id = uuid_generate_v5(
        '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid,
        src.id::text
    )
)
ORDER BY src.id;
