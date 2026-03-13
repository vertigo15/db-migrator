-- ============================================================
-- CONVERSATIONS, MESSAGES & MESSAGE_CONTENT_BLOCKS MIGRATION SQL
-- ============================================================
-- Generated: 2026-03-13T14:54:51.193632
-- Source: jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)
-- Destination: conversations + messages + message_content_blocks
-- Source rows: 345
-- 
-- IMPORTANT: This script will INSERT data into 3 tables!
-- IMPORTANT: Run users migration first.
--
-- Each source row creates entries in 3 tables:
--   1. conversations (aggregated per chat_id)
--   2. messages (user + assistant per row)
--   3. message_content_blocks (one per message)
--
-- Uses deterministic UUID generation (uuid_generate_v5).
-- Namespace UUID: 0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b
-- Multi-INSERT format: grouped by user, max 50 conversations per INSERT
-- ============================================================

-- Ensure PostgreSQL interprets this file as UTF-8 (required for Hebrew/multilingual content)
SET client_encoding = 'UTF8';

-- Ensure uuid-ossp extension is available
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- CONFIRMATION PROMPT
DO $$
DECLARE
    user_confirmation TEXT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'CONVERSATIONS/MESSAGES MIGRATION - CONFIRMATION REQUIRED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'This script will migrate conversations and messages';
    RAISE NOTICE 'Source rows: 345';
    RAISE NOTICE 'Namespace UUID: 0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b';
    RAISE NOTICE 'Generated: 2026-03-13T14:54:51.193729';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'PREREQUISITE: Users must be migrated first!';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';
    
    user_confirmation := NULL;
    
    IF current_setting('is_superuser') = 'off' THEN
        RAISE NOTICE 'Ready to proceed. Press Ctrl+C to cancel or Enter to continue...';
    END IF;
    
    RAISE NOTICE 'Starting migration...';
    RAISE NOTICE '';
END $$;

-- Uncomment for manual confirmation
-- \\prompt 'Type YES to confirm: ' user_confirmation
-- \\if :'user_confirmation' != 'YES'
--   \\echo 'Migration cancelled.'
--   \\quit
-- \\endif

-- ============================================================
-- MIGRATION MAPPING TABLE SETUP (idempotent)
-- ============================================================
-- Creates schema and tables for tracking ID mappings
-- Safe to run multiple times - uses IF NOT EXISTS
-- ============================================================

-- Create migration schema
CREATE SCHEMA IF NOT EXISTS migration;

-- Create ID mappings table
CREATE TABLE IF NOT EXISTS migration.id_mappings (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    old_id VARCHAR(255) NOT NULL,
    new_id UUID NOT NULL,
    migration_batch VARCHAR(50),
    migrated_at TIMESTAMP DEFAULT now(),
    notes TEXT,
    CONSTRAINT uq_table_old_id UNIQUE (table_name, old_id),
    CONSTRAINT uq_table_new_id UNIQUE (table_name, new_id)
);

-- Create indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_mappings_table_old_id 
    ON migration.id_mappings(table_name, old_id);
CREATE INDEX IF NOT EXISTS idx_mappings_table_new_id 
    ON migration.id_mappings(table_name, new_id);
CREATE INDEX IF NOT EXISTS idx_mappings_batch 
    ON migration.id_mappings(migration_batch);

-- Create batch tracking table
CREATE TABLE IF NOT EXISTS migration.batch_log (
    batch_id VARCHAR(50) PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    started_at TIMESTAMP DEFAULT now(),
    completed_at TIMESTAMP,
    record_count INTEGER,
    status VARCHAR(20) DEFAULT 'in_progress',
    error_message TEXT,
    source_info JSONB
);

-- Helper function: Get new ID from old ID
CREATE OR REPLACE FUNCTION migration.get_new_id(
    p_table_name VARCHAR,
    p_old_id VARCHAR
) RETURNS UUID AS $$
DECLARE
    v_new_id UUID;
BEGIN
    SELECT new_id INTO v_new_id
    FROM migration.id_mappings
    WHERE table_name = p_table_name AND old_id = p_old_id;
    RETURN v_new_id;
END;
$$ LANGUAGE plpgsql;

-- Helper function: Check if record already migrated
CREATE OR REPLACE FUNCTION migration.is_migrated(
    p_table_name VARCHAR,
    p_old_id VARCHAR
) RETURNS BOOLEAN AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1 FROM migration.id_mappings
        WHERE table_name = p_table_name AND old_id = p_old_id
    );
END;
$$ LANGUAGE plpgsql;

-- Progress summary view
CREATE OR REPLACE VIEW migration.progress_summary AS
SELECT 
    table_name,
    COUNT(*) as migrated_count,
    MIN(migrated_at) as first_migrated,
    MAX(migrated_at) as last_migrated,
    COUNT(DISTINCT migration_batch) as batch_count
FROM migration.id_mappings
GROUP BY table_name
ORDER BY table_name;

-- ============================================================
-- MIGRATION MAPPING TABLE SETUP COMPLETE
-- ============================================================



-- User: 306916b02924f0142a236cc978b0d013 (Batch 1, 50 conversations)

-- Conversations INSERT
INSERT INTO conversations (id, title, message_count, total_tokens, is_active, deleted_at, created_at, updated_at, last_interacted_at, user_id)
SELECT * FROM (
  VALUES
    ('001d30e1-8a7e-4deb-9bd4-8c02a3c9b499'::uuid, 'Token Usage of User Tehila', 12, 1342, true, NULL::timestamp, '2026-01-04T12:05:24.934815'::timestamptz, '2026-01-04T12:26:43.637916'::timestamptz, '2026-01-04T12:26:43.637916'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('03af3077-c23e-4760-aa27-71688476f2e5'::uuid, 'דוח עמידה בתקנון הבורסה של רני צים', 2, 1950, true, NULL::timestamp, '2026-01-08T12:17:56.138843'::timestamptz, '2026-01-08T12:17:56.138843'::timestamptz, '2026-01-08T12:17:56.138843'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('041cb78a-6df6-4f98-a32f-d09810a54c52'::uuid, 'no chat title', 2, 23, true, NULL::timestamp, '2025-12-28T10:23:50.094302'::timestamptz, '2025-12-28T10:23:50.094302'::timestamptz, '2025-12-28T10:23:50.094302'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('05012290-41c0-4f7b-9fc8-1ec14ba13cee'::uuid, 'בדיקת עמידה בתנאי הבורסה - רני צים', 2, 1117, true, NULL::timestamp, '2026-01-05T10:25:30.951325'::timestamptz, '2026-01-05T10:25:30.951325'::timestamptz, '2026-01-05T10:25:30.951325'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('053f2d4f-c08b-4758-8b26-04094c285f1f'::uuid, 'no chat title', 2, 23, true, NULL::timestamp, '2025-12-28T12:39:17.298307'::timestamptz, '2025-12-28T12:39:17.298307'::timestamptz, '2025-12-28T12:39:17.298307'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('059573b9-cd30-4f90-ab65-649a7cca4e67'::uuid, 'Contractual Terms Comparison and Analysis', 2, 5328, true, NULL::timestamp, '2026-01-05T07:39:54.138729'::timestamptz, '2026-01-05T07:39:54.138729'::timestamptz, '2026-01-05T07:39:54.138729'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('071e4062-87e6-4f55-802e-07f2cdf9ca9f'::uuid, 'Medical Summary and Recommendations for 89-Year-Old Woman', 2, 774, true, NULL::timestamp, '2026-01-18T09:06:26.658525'::timestamptz, '2026-01-18T09:06:26.658525'::timestamptz, '2026-01-18T09:06:26.658525'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('08786046-f1f8-4b21-b1d7-806034eb1626'::uuid, 'Conversation 08786046', 2, 5569, true, NULL::timestamp, '2026-02-24T14:43:25.485929'::timestamptz, '2026-02-24T14:43:25.485929'::timestamptz, '2026-02-24T14:43:25.485929'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('08c7358f-c210-4360-bc47-11e91394069f'::uuid, 'Title Analyzing Yearly Sales Internet vs Reseller', 6, 1050, true, NULL::timestamp, '2026-01-04T07:40:54.806510'::timestamptz, '2026-01-04T07:43:17.378577'::timestamptz, '2026-01-04T07:43:17.378577'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('091aacc2-fcfe-4e9c-b207-c05d1249f16e'::uuid, 'Title Retrieve Base Tables and Descriptions from Public Schema', 2, 937, true, NULL::timestamp, '2026-01-04T10:39:23.020944'::timestamptz, '2026-01-04T10:39:23.020944'::timestamptz, '2026-01-04T10:39:23.020944'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('0931e99a-ce4b-4699-a1e8-26e14574ff4d'::uuid, 'Contract Comparison Between Master and Candidate Contracts', 2, 4432, true, NULL::timestamp, '2025-12-29T12:11:36.695104'::timestamptz, '2025-12-29T12:11:36.695104'::timestamptz, '2025-12-29T12:11:36.695104'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('0a5a1077-07d3-4e45-8875-0766104446ce'::uuid, 'הקצאה לבורסה עמידה בתנאים וליקויים', 2, 1158, true, NULL::timestamp, '2026-01-05T09:10:30.876754'::timestamptz, '2026-01-05T09:10:30.876754'::timestamptz, '2026-01-05T09:10:30.876754'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('0b5ce59c-bf1e-4e5a-bca7-9cf01256333c'::uuid, 'Medical Summary Asthma and Aortic Stenosis Episode', 2, 542, true, NULL::timestamp, '2026-01-18T10:02:46.048306'::timestamptz, '2026-01-18T10:02:46.048306'::timestamptz, '2026-01-18T10:02:46.048306'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('0c93e10d-893c-4d75-a588-f01d3550107b'::uuid, 'Stores Average Profit Margin Overview', 14, 4094, true, NULL::timestamp, '2025-12-25T09:39:25.941980'::timestamptz, '2025-12-25T09:51:12.230066'::timestamptz, '2025-12-25T09:51:12.230066'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('0d409b26-6061-4007-bfff-ab73905572eb'::uuid, 'no chat title', 8, 4327, true, NULL::timestamp, '2026-01-01T15:16:08.152675'::timestamptz, '2026-01-01T15:21:23.700185'::timestamptz, '2026-01-01T15:21:23.700185'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('0ef1f2a9-70fa-423d-b3c4-6fa15ae7f503'::uuid, 'Building PostgreSQL connection URL for specific database', 6, 993, true, NULL::timestamp, '2026-01-18T07:36:59.208577'::timestamptz, '2026-01-18T07:38:44.728839'::timestamptz, '2026-01-18T07:38:44.728839'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('119e20aa-bd60-47b2-ad4c-6582102dcb20'::uuid, 'Medical Summary and Recommendations for Elderly Patient', 2, 779, true, NULL::timestamp, '2026-01-18T12:50:19.238708'::timestamptz, '2026-01-18T12:50:19.238708'::timestamptz, '2026-01-18T12:50:19.238708'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('148cd1b4-3c59-47cd-913a-cc311d326918'::uuid, 'Querying Vendor 112 Row Count in Dataset', 2, 47, true, NULL::timestamp, '2025-12-29T08:45:49.826493'::timestamptz, '2025-12-29T08:45:49.826493'::timestamptz, '2025-12-29T08:45:49.826493'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('1877cb85-340a-4077-b189-0aa2a598793d'::uuid, 'Medical Summary and Recommendations Evaluation', 2, 457, true, NULL::timestamp, '2026-01-18T15:49:55.538721'::timestamptz, '2026-01-18T15:49:55.538721'::timestamptz, '2026-01-18T15:49:55.538721'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('19fc2403-0757-45f5-8d92-19de212ce10f'::uuid, 'no chat title', 2, 23, true, NULL::timestamp, '2025-12-28T09:32:19.043638'::timestamptz, '2025-12-28T09:32:19.043638'::timestamptz, '2025-12-28T09:32:19.043638'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('1a5f4d83-1f6a-4e12-91dd-da795ecc1c7b'::uuid, 'no chat title', 2, 23, true, NULL::timestamp, '2025-12-28T14:18:36.508498'::timestamptz, '2025-12-28T14:18:36.508498'::timestamptz, '2025-12-28T14:18:36.508498'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('1df40006-9537-4bd6-9f55-50136d1256ed'::uuid, 'no chat title', 2, 23, true, NULL::timestamp, '2025-12-25T14:59:52.834797'::timestamptz, '2025-12-25T14:59:52.834797'::timestamptz, '2025-12-25T14:59:52.834797'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('1e0ade29-8a6c-49fb-8911-3d5f2c87c498'::uuid, 'Title Retrieve All Data from Products Table', 4, 1589, true, NULL::timestamp, '2026-01-04T10:16:41.860880'::timestamptz, '2026-01-04T10:17:23.328467'::timestamptz, '2026-01-04T10:17:23.328467'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('1f60f911-fdb0-4aa7-958d-b26c523cb64e'::uuid, 'Medical Summary and Follow-Up Recommendations', 2, 540, true, NULL::timestamp, '2026-01-18T09:29:34.285062'::timestamptz, '2026-01-18T09:29:34.285062'::timestamptz, '2026-01-18T09:29:34.285062'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('1fc5ef4d-07c3-42bf-bbd1-df46782826d5'::uuid, 'תחליף לסודה לשתייה באפייה', 4, 287, true, NULL::timestamp, '2026-03-11T09:19:02.363002'::timestamptz, '2026-03-11T09:19:27.949183'::timestamptz, '2026-03-11T09:19:27.949183'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('20e45a70-89a9-4c18-90a0-e2d0f88a19b3'::uuid, 'דוח עמידה בתקנון הבורסה עבור companyname', 2, 1717, true, NULL::timestamp, '2026-01-06T10:13:50.352299'::timestamptz, '2026-01-06T10:13:50.352299'::timestamptz, '2026-01-06T10:13:50.352299'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('2450e324-5b44-4d4e-afc6-1732e9d9a062'::uuid, 'תזכיר בדיקה להקצאה פרטית לעובדים', 2, 1153, true, NULL::timestamp, '2025-12-30T09:11:18.563601'::timestamptz, '2025-12-30T09:11:18.563601'::timestamptz, '2025-12-30T09:11:18.563601'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('258632d0-16d5-4df2-b470-8425241a21aa'::uuid, 'Medical Summary for Shortness of Breath Evaluation', 2, 418, true, NULL::timestamp, '2026-01-18T14:16:01.993831'::timestamptz, '2026-01-18T14:16:01.993831'::timestamptz, '2026-01-18T14:16:01.993831'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('26b7bffa-d51b-4a29-b338-0b82c486182f'::uuid, 'Querying Row Count for Vendor 112', 2, 64, true, NULL::timestamp, '2025-12-30T09:46:16.748428'::timestamptz, '2025-12-30T09:46:16.748428'::timestamptz, '2025-12-30T09:46:16.748428'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('273a901b-6fd6-4887-9191-79efc95dcb0f'::uuid, 'Private Allocation Instructions for Employees', 2, 909, true, NULL::timestamp, '2025-12-30T10:17:10.742108'::timestamptz, '2025-12-30T10:17:10.742108'::timestamptz, '2025-12-30T10:17:10.742108'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('278f4a8c-0f92-4313-8114-2e8a3e3a171b'::uuid, 'Greeting and Offer for Assistance', 2, 9, true, NULL::timestamp, '2026-01-14T16:27:18.951959'::timestamptz, '2026-01-14T16:27:18.951959'::timestamptz, '2026-01-14T16:27:18.951959'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('2b19aa86-6d88-478b-944f-fa7d040a6fa7'::uuid, 'Medical Summary PEG Removal and Follow-up Plan', 2, 431, true, NULL::timestamp, '2026-01-19T13:58:46.155441'::timestamptz, '2026-01-19T13:58:46.155441'::timestamptz, '2026-01-19T13:58:46.155441'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('2b83e4a1-0694-4b90-a004-4ac9cd402e55'::uuid, 'no chat title', 2, 23, true, NULL::timestamp, '2025-12-28T12:10:11.437236'::timestamptz, '2025-12-28T12:10:11.437236'::timestamptz, '2025-12-28T12:10:11.437236'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('2c682191-7b37-4a07-a6fe-9aa6e8e3f808'::uuid, 'no chat title', 2, 23, true, NULL::timestamp, '2026-01-14T12:38:50.036725'::timestamptz, '2026-01-14T12:38:50.036725'::timestamptz, '2026-01-14T12:38:50.036725'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('2e71e326-4821-453b-a441-204111696588'::uuid, 'no chat title', 2, 23, true, NULL::timestamp, '2025-12-28T09:33:56.917918'::timestamptz, '2025-12-28T09:33:56.917918'::timestamptz, '2025-12-28T09:33:56.917918'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('2f16a2d4-92ef-49f7-8e35-50d8c2934b60'::uuid, 'Title Master vs. Candidate Contract Clause Analysis', 2, 7259, true, NULL::timestamp, '2025-12-28T13:01:12.820882'::timestamptz, '2025-12-28T13:01:12.820882'::timestamptz, '2025-12-28T13:01:12.820882'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('314b8c2f-0286-4f06-b6d4-ee67a865ab83'::uuid, 'Recommendation for Dyspnea Hospitalization Summary', 2, 421, true, NULL::timestamp, '2026-01-18T14:12:47.583945'::timestamptz, '2026-01-18T14:12:47.583945'::timestamptz, '2026-01-18T14:12:47.583945'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('33c94b3e-f934-4fb2-ad46-299c6799f0d9'::uuid, 'Medical Summary for Breathing Difficulties Consultation', 2, 414, true, NULL::timestamp, '2026-01-18T17:21:41.097346'::timestamptz, '2026-01-18T17:21:41.097346'::timestamptz, '2026-01-18T17:21:41.097346'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('358d447b-80e5-4336-9e91-8e08d404245f'::uuid, 'Inducing Labor Due to Reduced Fetal Movement', 2, 256, true, NULL::timestamp, '2026-01-19T13:56:32.774954'::timestamptz, '2026-01-19T13:56:32.774954'::timestamptz, '2026-01-19T13:56:32.774954'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('383b1f6c-0109-4755-9431-bd000ba043dc'::uuid, 'Audit Checklist for Stock Exchange Compliance', 2, 1169, true, NULL::timestamp, '2026-01-04T16:23:23.891250'::timestamptz, '2026-01-04T16:23:23.891250'::timestamptz, '2026-01-04T16:23:23.891250'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('3a4b3b09-bae0-44bd-aa4d-795b2e227993'::uuid, 'Audit Report on Stock Exchange Compliance', 2, 1450, true, NULL::timestamp, '2026-01-06T09:35:56.545882'::timestamptz, '2026-01-06T09:35:56.545882'::timestamptz, '2026-01-06T09:35:56.545882'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('3b96b675-5872-42ad-8883-dfdc58beb9b9'::uuid, 'Avocado Sales Dataset Analysis Overview', 2, 259, true, NULL::timestamp, '2025-12-29T07:50:28.259748'::timestamptz, '2025-12-29T07:50:28.259748'::timestamptz, '2025-12-29T07:50:28.259748'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('3bc0c3cc-92bc-4496-9f36-9888468031e3'::uuid, 'no chat title', 2, 23, true, NULL::timestamp, '2025-12-31T08:51:12.780632'::timestamptz, '2025-12-31T08:51:12.780632'::timestamptz, '2025-12-31T08:51:12.780632'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('3e22a68e-b553-4e2f-878f-f412ce3446f5'::uuid, 'Title בדיקת עמידה בתנאי הבורסה לרני צים', 2, 1034, true, NULL::timestamp, '2026-01-06T07:47:13.491890'::timestamptz, '2026-01-06T07:47:13.491890'::timestamptz, '2026-01-06T07:47:13.491890'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('4309d033-78e1-41b9-a880-2f9cc4365ba9'::uuid, 'Drafting Employee Stock Option Agreement Terms', 2, 586, true, NULL::timestamp, '2025-12-30T10:40:34.230542'::timestamptz, '2025-12-30T10:40:34.230542'::timestamptz, '2025-12-30T10:40:34.230542'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('44b87ab7-f0a8-48e6-a171-17f8de9e2a16'::uuid, 'no chat title', 12, 3609, true, NULL::timestamp, '2026-01-04T09:40:10.869356'::timestamptz, '2026-01-04T09:52:16.976680'::timestamptz, '2026-01-04T09:52:16.976680'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('45e49b70-3428-4e9c-a527-85d1bb61cb27'::uuid, 'Checking Supplier Transaction Data for Errors', 4, 154, true, NULL::timestamp, '2025-12-29T08:44:43.723294'::timestamptz, '2025-12-29T08:45:12.264738'::timestamptz, '2025-12-29T08:45:12.264738'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('476692b1-3c6a-4401-af79-6650c61ec622'::uuid, 'Contract Comparison and Analysis Report', 2, 7245, true, NULL::timestamp, '2025-12-25T13:23:47.000126'::timestamptz, '2025-12-25T13:23:47.000126'::timestamptz, '2025-12-25T13:23:47.000126'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('4778bd18-9a5d-42e6-920f-35f60e6e62a9'::uuid, 'Total Q1 2024 Revenue Sum', 4, 160, true, NULL::timestamp, '2025-12-29T08:46:41.265312'::timestamptz, '2025-12-29T08:47:56.353200'::timestamptz, '2025-12-29T08:47:56.353200'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013')),
    ('48685470-d314-4b97-bf7b-dfd6aac45d81'::uuid, 'Which workbook sheets contain balancesheet data?', 20, 9607, true, NULL::timestamp, '2025-12-22T10:02:25.350996'::timestamptz, '2025-12-22T14:31:32.284549'::timestamptz, '2025-12-22T14:31:32.284549'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, '306916b02924f0142a236cc978b0d013'))
) AS v(id, title, message_count, total_tokens, is_active, deleted_at, created_at, updated_at, last_interacted_at, user_id)
WHERE NOT EXISTS (SELECT 1 FROM conversations WHERE id = v.id);

