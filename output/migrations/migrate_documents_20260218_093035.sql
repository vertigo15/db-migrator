-- Migration script: jeen_dev_custom_documents -> user_db.public.documents
--
-- Mapping:
--   id              <- gen_random_uuid() (new UUID)
--   status          <- 'PROCESSED' (assuming all migrated docs are already processed)
--   file_name       <- doc_name_origin
--   file_size       <- doc_size
--   storage_type    <- blob_source mapped ('azure_blob' -> 'azure')
--   storage_path    <- doc_id (the legacy doc_id IS the blob path)
--   storage_id      <- NULL
--   metadata        <- JSON with legacy fields (title, description, summary, tags, checksum, etc.)
--   created_at      <- created_at
--   updated_at      <- now()
--   deleted_at      <- NULL
--   folder_id       <- deterministic UUID from legacy folder_id (same namespace as folders migration)
--   user_id         <- looked up from migrated users via legacyData.id = owner_id
--   content_type    <- derived from doc_type (pdf -> application/pdf, docx -> application/vnd..., etc.)
--   parsing_technique_id <- NULL
--   source_type     <- 'upload'
--   organization_id <- NULL
--
-- PREREQUISITE: Run users migration and folders migration first.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

INSERT INTO user_db.public.documents (
    id,
    status,
    file_name,
    file_size,
    storage_type,
    storage_path,
    storage_id,
    metadata,
    created_at,
    updated_at,
    deleted_at,
    folder_id,
    user_id,
    content_type,
    parsing_technique_id,
    source_type,
    organization_id
)
SELECT
    gen_random_uuid()                                       AS id,

    'PROCESSED'::public.documents_status_enum               AS status,

    COALESCE(src.doc_name_origin, src.doc_title, 'unnamed') AS file_name,

    COALESCE(src.doc_size, 0)::int8                         AS file_size,

    -- Map blob_source to storage_type
    CASE
        WHEN src.blob_source = 'azure_blob' THEN 'azure'
        WHEN src.blob_source IS NOT NULL     THEN src.blob_source
        ELSE NULL
    END                                                     AS storage_type,

    -- Legacy doc_id serves as the storage path
    src.doc_id                                              AS storage_path,

    NULL                                                    AS storage_id,

    -- Pack all legacy-specific fields into metadata
    jsonb_build_object(
        'name',   COALESCE(src.doc_name_origin, src.doc_title),
        'source', 'legacy-migration',
        'legacyData', jsonb_build_object(
            'doc_id',                        src.doc_id,
            'doc_title',                     src.doc_title,
            'doc_description',               src.doc_description,
            'doc_summery',                   src.doc_summery,
            'doc_summery_modified_by',       src.doc_summery_modified_by,
            'doc_summery_modified_at',       src.doc_summery_modified_at,
            'tags',                          COALESCE(src.tags, '[]'::jsonb),
            'embedding_model',               NULLIF(TRIM(COALESCE(src.embedding_model, '')), ''),
            'vector_methods',                src.vector_methods,
            'version',                       src.version,
            'doc_checksum',                  src.doc_checksum,
            'data_integration_doc_metadata', src.data_integration_doc_metadata
        )
    )                                                       AS metadata,

    COALESCE(src.created_at, now()) AT TIME ZONE 'UTC'      AS created_at,
    now()                                                   AS updated_at,
    NULL                                                    AS deleted_at,

    -- Convert legacy integer folder_id to UUID (same namespace as folders migration)
    CASE
        WHEN src.folder_id IS NOT NULL THEN
            uuid_generate_v5(
                '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid,
                src.folder_id::text
            )
        ELSE NULL
    END                                                     AS folder_id,

    -- Lookup new user UUID from migrated users via legacy id
    u.id::varchar(255)                                      AS user_id,

    -- Map doc_type to MIME content_type
    CASE TRIM(LOWER(src.doc_type))
        WHEN 'pdf'   THEN 'application/pdf'
        WHEN 'docx'  THEN 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        WHEN 'pptx'  THEN 'application/vnd.openxmlformats-officedocument.presentationml.presentation'
        WHEN 'xlsx'  THEN 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        WHEN 'doc'   THEN 'application/msword'
        WHEN 'ppt'   THEN 'application/vnd.ms-powerpoint'
        WHEN 'xls'   THEN 'application/vnd.ms-excel'
        WHEN 'txt'   THEN 'text/plain'
        WHEN 'csv'   THEN 'text/csv'
        WHEN 'html'  THEN 'text/html'
        WHEN 'json'  THEN 'application/json'
        WHEN 'png'   THEN 'image/png'
        WHEN 'jpg'   THEN 'image/jpeg'
        WHEN 'jpeg'  THEN 'image/jpeg'
        WHEN 'gif'   THEN 'image/gif'
        WHEN 'svg'   THEN 'image/svg+xml'
        WHEN 'webp'  THEN 'image/webp'
        WHEN 'md'    THEN 'text/markdown'
        WHEN 'mp3'   THEN 'audio/mpeg'
        WHEN 'mp4'   THEN 'video/mp4'
        -- Source values that are already valid MIME types — pass through
        WHEN 'application/pdf' THEN 'application/pdf'
        WHEN 'image/png'       THEN 'image/png'
        WHEN 'image/jpeg'      THEN 'image/jpeg'
        ELSE 'application/octet-stream'
    END                                                     AS content_type,

    NULL                                                    AS parsing_technique_id,

    'upload'::public.documents_source_type_enum             AS source_type,

    NULL                                                    AS organization_id

FROM public.jeen_dev_custom_documents src
-- Join to migrated users
JOIN user_db.public.users u
    ON u.metadata->'legacyData'->>'id' = src.owner_id
WHERE NOT EXISTS (
    -- Skip docs whose legacy doc_id is already present in destination metadata
    SELECT 1 FROM user_db.public.documents d
    WHERE d.metadata->'legacyData'->>'doc_id' = src.doc_id
);

