-- ============================================================
-- CONVERSATIONS, MESSAGES & MESSAGE_CONTENT_BLOCKS MIGRATION SQL
-- ============================================================
-- Generated: 2026-04-10T05:21:37.296511
-- Source: jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)
-- Destination: conversations + messages + message_content_blocks
-- Source rows: 12
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
    RAISE NOTICE 'Source rows: 12';
    RAISE NOTICE 'Namespace UUID: 0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b';
    RAISE NOTICE 'Generated: 2026-04-10T05:21:37.296559';
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



-- User: de0ff05457533c93fdf3e0d1cdd0f808 (Batch 1, 2 conversations)

-- Conversations INSERT
INSERT INTO conversations (id, title, message_count, total_tokens, is_active, deleted_at, created_at, updated_at, last_interacted_at, user_id)
SELECT * FROM (
  VALUES
    ('7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, 'Request for Email Sending Assistance', 16, 440, true, NULL::timestamp, '2025-08-25T11:50:33.993556'::timestamptz, '2025-08-25T11:51:45.649515'::timestamptz, '2025-08-25T11:51:45.649515'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808')),
    ('a6030c5e-8ab0-42db-8dda-ddb30d53088a'::uuid, 'Title חוויות מחופשת חברות ביוון', 8, 198, true, NULL::timestamp, '2025-08-25T11:53:51.440362'::timestamptz, '2025-08-25T11:58:19.268601'::timestamptz, '2025-08-25T11:58:19.268601'::timestamptz, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'))
) AS v(id, title, message_count, total_tokens, is_active, deleted_at, created_at, updated_at, last_interacted_at, user_id)
WHERE NOT EXISTS (SELECT 1 FROM conversations WHERE id = v.id);

-- Messages INSERT
INSERT INTO messages (id, conversation_id, parent_message_id, role, has_tool_calls, iteration_count, content_block_count, finish_reason, created_at, updated_at, deleted_at, user_id, metadata)
SELECT * FROM (
  VALUES
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '758d999c-07b9-42c9-bffd-d891a2643b45-user'), '7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, NULL::uuid, 'user'::messages_role_enum, false, 1, 1, NULL::text, '2025-08-25T11:50:33.993556'::timestamptz - interval '1 second', '2025-08-25T11:50:33.993556'::timestamptz - interval '1 second', NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"message_order": 0, "turn_index": 0}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '758d999c-07b9-42c9-bffd-d891a2643b45-assistant'), '7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '758d999c-07b9-42c9-bffd-d891a2643b45-user'), 'assistant'::messages_role_enum, false, 1, 1, 'stop', '2025-08-25T11:50:33.993556'::timestamptz, '2025-08-25T11:50:33.993556'::timestamptz, NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"model": null, "type": "agent", "bot_id": "GADLZ1vd7hvv5lx9", "is_like": {"like": false, "dislike": false, "comment": "", "logId": ""}, "token_amount": null, "words_amount": null, "calculated_time": null, "category": "character prompt - no documents", "sentiment": null, "message_order": 1, "turn_index": 0, "legacyData": {"legacy_log_id": "758d999c-07b9-42c9-bffd-d891a2643b45", "title": null, "toolkit_settings": {"isStream": false}, "sourcetext": null, "sourcelink": null, "webpagelink": null, "documents_selected": "[]"}}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'puGtoLuWvJUI-user'), '7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '758d999c-07b9-42c9-bffd-d891a2643b45-assistant'), 'user'::messages_role_enum, false, 1, 1, NULL::text, '2025-08-25T11:50:35.184284'::timestamptz - interval '1 second', '2025-08-25T11:50:35.184284'::timestamptz - interval '1 second', NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"message_order": 2, "turn_index": 1}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'puGtoLuWvJUI-assistant'), '7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'puGtoLuWvJUI-user'), 'assistant'::messages_role_enum, false, 1, 1, 'stop', '2025-08-25T11:50:35.184284'::timestamptz, '2025-08-25T11:50:35.184284'::timestamptz, NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"model": "gpt-4o", "type": "playground", "bot_id": "GADLZ1vd7hvv5lx9", "is_like": {"like": false, "dislike": false, "comment": "", "logId": ""}, "token_amount": 94, "words_amount": 64, "calculated_time": 1, "category": null, "sentiment": null, "message_order": 3, "turn_index": 1, "legacyData": {"legacy_log_id": "puGtoLuWvJUI", "title": "Title יצירת מייל חמוד לפגישה במסעדה תאילנדית", "toolkit_settings": {"chatLang": "none", "template": "none", "model": "gpt-4o", "isMultiple": false, "isLongAnswer": false, "isTypingEffect": false, "temperature": 0, "question_choice": ["Vectors", "Q&A"], "embeddingModel": "", "stream": true}, "sourcetext": null, "sourcelink": null, "webpagelink": null, "documents_selected": "[]"}}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'e8c9173e-4bed-4a42-9c8b-7f0cb52b7024-user'), '7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'puGtoLuWvJUI-assistant'), 'user'::messages_role_enum, false, 1, 1, NULL::text, '2025-08-25T11:50:47.638263'::timestamptz - interval '1 second', '2025-08-25T11:50:47.638263'::timestamptz - interval '1 second', NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"message_order": 4, "turn_index": 2}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'e8c9173e-4bed-4a42-9c8b-7f0cb52b7024-assistant'), '7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'e8c9173e-4bed-4a42-9c8b-7f0cb52b7024-user'), 'assistant'::messages_role_enum, false, 1, 1, 'stop', '2025-08-25T11:50:47.638263'::timestamptz, '2025-08-25T11:50:47.638263'::timestamptz, NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"model": null, "type": "agent", "bot_id": "GADLZ1vd7hvv5lx9", "is_like": {"like": false, "dislike": false, "comment": "", "logId": ""}, "token_amount": null, "words_amount": null, "calculated_time": null, "category": "character prompt - no documents", "sentiment": null, "message_order": 5, "turn_index": 2, "legacyData": {"legacy_log_id": "e8c9173e-4bed-4a42-9c8b-7f0cb52b7024", "title": null, "toolkit_settings": {"isStream": false}, "sourcetext": null, "sourcelink": null, "webpagelink": null, "documents_selected": "[]"}}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'eudDLJqm60n9-user'), '7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'e8c9173e-4bed-4a42-9c8b-7f0cb52b7024-assistant'), 'user'::messages_role_enum, false, 1, 1, NULL::text, '2025-08-25T11:50:48.223914'::timestamptz - interval '1 second', '2025-08-25T11:50:48.223914'::timestamptz - interval '1 second', NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"message_order": 6, "turn_index": 3}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'eudDLJqm60n9-assistant'), '7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'eudDLJqm60n9-user'), 'assistant'::messages_role_enum, false, 1, 1, 'stop', '2025-08-25T11:50:48.223914'::timestamptz, '2025-08-25T11:50:48.223914'::timestamptz, NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"model": "gpt-4o", "type": "playground", "bot_id": "GADLZ1vd7hvv5lx9", "is_like": {"like": false, "dislike": false, "comment": "", "logId": ""}, "token_amount": 197, "words_amount": 137, "calculated_time": 0, "category": null, "sentiment": null, "message_order": 7, "turn_index": 3, "legacyData": {"legacy_log_id": "eudDLJqm60n9", "title": "כיצד לשלוח דואל בצורה עצמאית", "toolkit_settings": {"chatLang": "none", "template": "none", "model": "gpt-4o", "isMultiple": false, "isLongAnswer": false, "isTypingEffect": false, "temperature": 0, "question_choice": ["Vectors", "Q&A"], "embeddingModel": "", "stream": true}, "sourcetext": null, "sourcelink": null, "webpagelink": null, "documents_selected": "[]"}}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '367d6f52-b7c5-41c2-a51d-c0b0776fb5eb-user'), '7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'eudDLJqm60n9-assistant'), 'user'::messages_role_enum, false, 1, 1, NULL::text, '2025-08-25T11:51:13.333441'::timestamptz - interval '1 second', '2025-08-25T11:51:13.333441'::timestamptz - interval '1 second', NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"message_order": 8, "turn_index": 4}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '367d6f52-b7c5-41c2-a51d-c0b0776fb5eb-assistant'), '7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '367d6f52-b7c5-41c2-a51d-c0b0776fb5eb-user'), 'assistant'::messages_role_enum, false, 1, 1, 'stop', '2025-08-25T11:51:13.333441'::timestamptz, '2025-08-25T11:51:13.333441'::timestamptz, NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"model": null, "type": "agent", "bot_id": "GADLZ1vd7hvv5lx9", "is_like": {"like": false, "dislike": false, "comment": "", "logId": ""}, "token_amount": null, "words_amount": null, "calculated_time": null, "category": "character prompt - no documents", "sentiment": null, "message_order": 9, "turn_index": 4, "legacyData": {"legacy_log_id": "367d6f52-b7c5-41c2-a51d-c0b0776fb5eb", "title": null, "toolkit_settings": {"isStream": false}, "sourcetext": null, "sourcelink": null, "webpagelink": null, "documents_selected": "[]"}}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'MdlIucKhLAah-user'), '7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '367d6f52-b7c5-41c2-a51d-c0b0776fb5eb-assistant'), 'user'::messages_role_enum, false, 1, 1, NULL::text, '2025-08-25T11:51:13.825539'::timestamptz - interval '1 second', '2025-08-25T11:51:13.825539'::timestamptz - interval '1 second', NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"message_order": 10, "turn_index": 5}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'MdlIucKhLAah-assistant'), '7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'MdlIucKhLAah-user'), 'assistant'::messages_role_enum, false, 1, 1, 'stop', '2025-08-25T11:51:13.825539'::timestamptz, '2025-08-25T11:51:13.825539'::timestamptz, NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"model": "gpt-4o", "type": "playground", "bot_id": "GADLZ1vd7hvv5lx9", "is_like": {"like": false, "dislike": false, "comment": "", "logId": ""}, "token_amount": 99, "words_amount": 71, "calculated_time": 0, "category": null, "sentiment": null, "message_order": 11, "turn_index": 5, "legacyData": {"legacy_log_id": "MdlIucKhLAah", "title": "מייל לפגישת חברים במסעדה תאילנדית", "toolkit_settings": {"chatLang": "none", "template": "none", "model": "gpt-4o", "isMultiple": false, "isLongAnswer": false, "isTypingEffect": false, "temperature": 0, "question_choice": ["Vectors", "Q&A"], "embeddingModel": "", "stream": true}, "sourcetext": null, "sourcelink": null, "webpagelink": null, "documents_selected": "[]"}}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '80744c21-b7e4-4506-a52d-3d569961372e-user'), '7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'MdlIucKhLAah-assistant'), 'user'::messages_role_enum, false, 1, 1, NULL::text, '2025-08-25T11:51:44.717275'::timestamptz - interval '1 second', '2025-08-25T11:51:44.717275'::timestamptz - interval '1 second', NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"message_order": 12, "turn_index": 6}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '80744c21-b7e4-4506-a52d-3d569961372e-assistant'), '7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '80744c21-b7e4-4506-a52d-3d569961372e-user'), 'assistant'::messages_role_enum, false, 1, 1, 'stop', '2025-08-25T11:51:44.717275'::timestamptz, '2025-08-25T11:51:44.717275'::timestamptz, NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"model": null, "type": "agent", "bot_id": "GADLZ1vd7hvv5lx9", "is_like": {"like": false, "dislike": false, "comment": "", "logId": ""}, "token_amount": null, "words_amount": null, "calculated_time": null, "category": "character prompt - no documents", "sentiment": null, "message_order": 13, "turn_index": 6, "legacyData": {"legacy_log_id": "80744c21-b7e4-4506-a52d-3d569961372e", "title": null, "toolkit_settings": {"isStream": false}, "sourcetext": null, "sourcelink": null, "webpagelink": null, "documents_selected": "[]"}}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'DYv1Mq2qjV9V-user'), '7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '80744c21-b7e4-4506-a52d-3d569961372e-assistant'), 'user'::messages_role_enum, false, 1, 1, NULL::text, '2025-08-25T11:51:45.649515'::timestamptz - interval '1 second', '2025-08-25T11:51:45.649515'::timestamptz - interval '1 second', NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"message_order": 14, "turn_index": 7}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'DYv1Mq2qjV9V-assistant'), '7929aa9a-d56a-416d-8647-ba509a0fe785'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'DYv1Mq2qjV9V-user'), 'assistant'::messages_role_enum, false, 1, 1, 'stop', '2025-08-25T11:51:45.649515'::timestamptz, '2025-08-25T11:51:45.649515'::timestamptz, NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"model": "gpt-4o", "type": "playground", "bot_id": "GADLZ1vd7hvv5lx9", "is_like": {"like": false, "dislike": false, "comment": "", "logId": ""}, "token_amount": 50, "words_amount": 38, "calculated_time": 0, "category": null, "sentiment": null, "message_order": 15, "turn_index": 7, "legacyData": {"legacy_log_id": "DYv1Mq2qjV9V", "title": "Request for Email Sending Assistance", "toolkit_settings": {"chatLang": "none", "template": "none", "model": "gpt-4o", "isMultiple": false, "isLongAnswer": false, "isTypingEffect": false, "temperature": 0, "question_choice": ["Vectors", "Q&A"], "embeddingModel": "", "stream": true}, "sourcetext": null, "sourcelink": null, "webpagelink": null, "documents_selected": "[]"}}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'fa29fc58-5688-4439-9d72-6be94e2f10f5-user'), 'a6030c5e-8ab0-42db-8dda-ddb30d53088a'::uuid, NULL::uuid, 'user'::messages_role_enum, false, 1, 1, NULL::text, '2025-08-25T11:53:51.440362'::timestamptz - interval '1 second', '2025-08-25T11:53:51.440362'::timestamptz - interval '1 second', NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"message_order": 0, "turn_index": 0}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'fa29fc58-5688-4439-9d72-6be94e2f10f5-assistant'), 'a6030c5e-8ab0-42db-8dda-ddb30d53088a'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'fa29fc58-5688-4439-9d72-6be94e2f10f5-user'), 'assistant'::messages_role_enum, false, 1, 1, 'stop', '2025-08-25T11:53:51.440362'::timestamptz, '2025-08-25T11:53:51.440362'::timestamptz, NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"model": null, "type": "agent", "bot_id": "Gbo4b23IBNP2GswG", "is_like": {"like": false, "dislike": false, "comment": "", "logId": ""}, "token_amount": null, "words_amount": null, "calculated_time": null, "category": "character prompt - no documents", "sentiment": null, "message_order": 1, "turn_index": 0, "legacyData": {"legacy_log_id": "fa29fc58-5688-4439-9d72-6be94e2f10f5", "title": null, "toolkit_settings": {"isStream": false}, "sourcetext": null, "sourcelink": null, "webpagelink": null, "documents_selected": "[]"}}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'Vgg0DIhgB7CK-user'), 'a6030c5e-8ab0-42db-8dda-ddb30d53088a'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'fa29fc58-5688-4439-9d72-6be94e2f10f5-assistant'), 'user'::messages_role_enum, false, 1, 1, NULL::text, '2025-08-25T11:53:52.482367'::timestamptz - interval '1 second', '2025-08-25T11:53:52.482367'::timestamptz - interval '1 second', NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"message_order": 2, "turn_index": 1}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'Vgg0DIhgB7CK-assistant'), 'a6030c5e-8ab0-42db-8dda-ddb30d53088a'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'Vgg0DIhgB7CK-user'), 'assistant'::messages_role_enum, false, 1, 1, 'stop', '2025-08-25T11:53:52.482367'::timestamptz, '2025-08-25T11:53:52.482367'::timestamptz, NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"model": "gpt-4o", "type": "playground", "bot_id": "Gbo4b23IBNP2GswG", "is_like": {"like": false, "dislike": false, "comment": "", "logId": ""}, "token_amount": 178, "words_amount": 120, "calculated_time": 1, "category": null, "sentiment": null, "message_order": 3, "turn_index": 1, "legacyData": {"legacy_log_id": "Vgg0DIhgB7CK", "title": "Creating a Vacation Post for a Trip to Greece", "toolkit_settings": {"chatLang": "none", "template": "none", "model": "gpt-4o", "isMultiple": false, "isLongAnswer": false, "isTypingEffect": false, "temperature": 0, "question_choice": ["Vectors", "Q&A"], "embeddingModel": "", "stream": true}, "sourcetext": null, "sourcelink": null, "webpagelink": null, "documents_selected": "[]"}}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '0ea28da9-03b0-46dd-bad6-19f1fc3c7ebe-user'), 'a6030c5e-8ab0-42db-8dda-ddb30d53088a'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'Vgg0DIhgB7CK-assistant'), 'user'::messages_role_enum, false, 1, 1, NULL::text, '2025-08-25T11:58:14.749113'::timestamptz - interval '1 second', '2025-08-25T11:58:14.749113'::timestamptz - interval '1 second', NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"message_order": 4, "turn_index": 2}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '0ea28da9-03b0-46dd-bad6-19f1fc3c7ebe-assistant'), 'a6030c5e-8ab0-42db-8dda-ddb30d53088a'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '0ea28da9-03b0-46dd-bad6-19f1fc3c7ebe-user'), 'assistant'::messages_role_enum, false, 1, 1, 'stop', '2025-08-25T11:58:14.749113'::timestamptz, '2025-08-25T11:58:14.749113'::timestamptz, NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"model": null, "type": "agent", "bot_id": "Gbo4b23IBNP2GswG", "is_like": {"like": false, "dislike": false, "comment": "", "logId": ""}, "token_amount": null, "words_amount": null, "calculated_time": null, "category": "character prompt - no documents", "sentiment": null, "message_order": 5, "turn_index": 2, "legacyData": {"legacy_log_id": "0ea28da9-03b0-46dd-bad6-19f1fc3c7ebe", "title": null, "toolkit_settings": {"isStream": false}, "sourcetext": null, "sourcelink": null, "webpagelink": null, "documents_selected": "[]"}}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '7qvMTJaYUduO-user'), 'a6030c5e-8ab0-42db-8dda-ddb30d53088a'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '0ea28da9-03b0-46dd-bad6-19f1fc3c7ebe-assistant'), 'user'::messages_role_enum, false, 1, 1, NULL::text, '2025-08-25T11:58:19.268601'::timestamptz - interval '1 second', '2025-08-25T11:58:19.268601'::timestamptz - interval '1 second', NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"message_order": 6, "turn_index": 3}'::jsonb),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '7qvMTJaYUduO-assistant'), 'a6030c5e-8ab0-42db-8dda-ddb30d53088a'::uuid, uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '7qvMTJaYUduO-user'), 'assistant'::messages_role_enum, false, 1, 1, 'stop', '2025-08-25T11:58:19.268601'::timestamptz, '2025-08-25T11:58:19.268601'::timestamptz, NULL::timestamp, uuid_generate_v5('a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::uuid, 'de0ff05457533c93fdf3e0d1cdd0f808'), '{"model": "gpt-4o", "type": "playground", "bot_id": "Gbo4b23IBNP2GswG", "is_like": {"like": false, "dislike": false, "comment": "", "logId": ""}, "token_amount": 20, "words_amount": 14, "calculated_time": 1, "category": null, "sentiment": null, "message_order": 7, "turn_index": 3, "legacyData": {"legacy_log_id": "7qvMTJaYUduO", "title": "Title חוויות מחופשת חברות ביוון", "toolkit_settings": {"chatLang": "none", "template": "none", "model": "gpt-4o", "isMultiple": false, "isLongAnswer": false, "isTypingEffect": false, "temperature": 0, "question_choice": ["Vectors", "Q&A"], "embeddingModel": "", "stream": true}, "sourcetext": null, "sourcelink": null, "webpagelink": null, "documents_selected": "[]"}}'::jsonb)
) AS v(id, conversation_id, parent_message_id, role, has_tool_calls, iteration_count, content_block_count, finish_reason, created_at, updated_at, deleted_at, user_id, metadata)
WHERE NOT EXISTS (SELECT 1 FROM messages WHERE id = v.id);

-- Message Content Blocks INSERT
INSERT INTO message_content_blocks (id, message_id, sequence, type, content, execution_time_ms, created_at)
SELECT * FROM (
  VALUES
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '758d999c-07b9-42c9-bffd-d891a2643b45-user-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '758d999c-07b9-42c9-bffd-d891a2643b45-user'), 0, 'message'::message_content_blocks_type_enum, '{"role": "user", "type": "message", "content": [{"text": "[no question text]", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:50:33.993556'::timestamptz - interval '1 second'),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '758d999c-07b9-42c9-bffd-d891a2643b45-assistant-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '758d999c-07b9-42c9-bffd-d891a2643b45-assistant'), 0, 'message'::message_content_blocks_type_enum, '{"role": "assistant", "type": "message", "content": [{"text": "נושא: פגישה במסעדה התאילנדית 🥢\n\nהיי עדי!\n\nאיזה כיף שניפגש היום! ממש מתרגשת לבלות איתך במסעדה התאילנדית. אני בטוחה שיהיה אוכל טעים וגם אווירה מדליקה! 🎉\n\nמחכה לראות אותך,\n[השמך שלך]\n\nאם יש לך שאלות או צריך משהו נוסף, תרגיש חופשי לכתוב לי! 🌟", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:50:33.993556'::timestamptz),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'puGtoLuWvJUI-user-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'puGtoLuWvJUI-user'), 0, 'message'::message_content_blocks_type_enum, '{"role": "user", "type": "message", "content": [{"text": "היי אשמח לשלוח מייל לכתובת \nadiokun1108@gmail.com \nאני רוצה בשפה חמודה וכייפית \nהיי, ניפגש היום בבית התיאלנדי היהי אוכל טעים ", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:50:35.184284'::timestamptz - interval '1 second'),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'puGtoLuWvJUI-assistant-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'puGtoLuWvJUI-assistant'), 0, 'message'::message_content_blocks_type_enum, '{"role": "assistant", "type": "message", "content": [{"text": "נושא: פגישה במסעדה התאילנדית 🥢\n\n\n\nהיי עדי!\n\n\n\nאיזה כיף שניפגש היום! ממש מתרגשת לבלות איתך במסעדה התאילנדית. אני בטוחה שיהיה אוכל טעים וגם אווירה מדליקה! 🎉\n\n\n\nמחכה לראות אותך,\n\n[השמך שלך]\n\n\n\nאם יש לך שאלות או צריך משהו נוסף, תרגיש חופשי לכתוב לי! 🌟", "type": "text"}]}'::jsonb, 1, '2025-08-25T11:50:35.184284'::timestamptz),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'e8c9173e-4bed-4a42-9c8b-7f0cb52b7024-user-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'e8c9173e-4bed-4a42-9c8b-7f0cb52b7024-user'), 0, 'message'::message_content_blocks_type_enum, '{"role": "user", "type": "message", "content": [{"text": "[no question text]", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:50:47.638263'::timestamptz - interval '1 second'),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'e8c9173e-4bed-4a42-9c8b-7f0cb52b7024-assistant-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'e8c9173e-4bed-4a42-9c8b-7f0cb52b7024-assistant'), 0, 'message'::message_content_blocks_type_enum, '{"role": "assistant", "type": "message", "content": [{"text": "לצערי, אני לא יכול לשלוח את המייל בשבילך, אבל הנה איך שאתה יכול לעשות את זה בעצמך דרך יישום הדוא\"ל שלך:\n\n1. פתח את היישום או האתר שבו אתה משתמש בשביל לשלוח דוא\"ל (כגון גימייל, אאוטלוק וכו).\n   \n2. לחץ על כפתור \"כתיבת הודעה חדשה\" או \"New Email\".\n\n3. הזן את כתובת הדוא\"ל של הנמען בשדה \"אל\": adiokun1108@gmail.com.\n\n4. בשדה \"נושא\", כתוב: פגישה במסעדה התאילנדית 🥢.\n\n5. הדבק את תוכן המייל שכתבנו עבורך בתיבת התוכן של ההודעה:\n\n   ```\n   היי עדי!\n\n   איזה כיף שניפגש היום! ממש מתרגשת לבלות איתך במסעדה התאילנדית. אני בטוחה שיהיה אוכל טעים וגם אווירה מדליקה! 🎉\n\n   מחכה לראות אותך,\n   [השמך שלך]\n\n   אם יש לך שאלות או צריך משהו נוסף, תרגיש חופשי לכתוב לי! 🌟\n   ```\n\n6. החלף את \"[השמך שלך]\" בשם שלך.\n\n7. לחץ על כפתור \"שלח\" או \"Send\".\n\nבהצלחה! 🌟", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:50:47.638263'::timestamptz),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'eudDLJqm60n9-user-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'eudDLJqm60n9-user'), 0, 'message'::message_content_blocks_type_enum, '{"role": "user", "type": "message", "content": [{"text": "מגניב אשמח לשלוח ", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:50:48.223914'::timestamptz - interval '1 second'),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'eudDLJqm60n9-assistant-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'eudDLJqm60n9-assistant'), 0, 'message'::message_content_blocks_type_enum, '{"role": "assistant", "type": "message", "content": [{"text": "לצערי, אני לא יכול לשלוח את המייל בשבילך, אבל הנה איך שאתה יכול לעשות את זה בעצמך דרך יישום הדוא\"ל שלך:\n\n\n\n1. פתח את היישום או האתר שבו אתה משתמש בשביל לשלוח דוא\"ל (כגון גימייל, אאוטלוק וכו).\n\n   \n\n2. לחץ על כפתור \"כתיבת הודעה חדשה\" או \"New Email\".\n\n\n\n3. הזן את כתובת הדוא\"ל של הנמען בשדה \"אל\": adiokun1108@gmail.com.\n\n\n\n4. בשדה \"נושא\", כתוב: פגישה במסעדה התאילנדית 🥢.\n\n\n\n5. הדבק את תוכן המייל שכתבנו עבורך בתיבת התוכן של ההודעה:\n\n\n\n   ```\n\n   היי עדי!\n\n\n\n   איזה כיף שניפגש היום! ממש מתרגשת לבלות איתך במסעדה התאילנדית. אני בטוחה שיהיה אוכל טעים וגם אווירה מדליקה! 🎉\n\n\n\n   מחכה לראות אותך,\n\n   [השמך שלך]\n\n\n\n   אם יש לך שאלות או צריך משהו נוסף, תרגיש חופשי לכתוב לי! 🌟\n\n   ```\n\n\n\n6. החלף את \"[השמך שלך]\" בשם שלך.\n\n\n\n7. לחץ על כפתור \"שלח\" או \"Send\".\n\n\n\nבהצלחה! 🌟", "type": "text"}]}'::jsonb, 0, '2025-08-25T11:50:48.223914'::timestamptz),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '367d6f52-b7c5-41c2-a51d-c0b0776fb5eb-user-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '367d6f52-b7c5-41c2-a51d-c0b0776fb5eb-user'), 0, 'message'::message_content_blocks_type_enum, '{"role": "user", "type": "message", "content": [{"text": "[no question text]", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:51:13.333441'::timestamptz - interval '1 second'),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '367d6f52-b7c5-41c2-a51d-c0b0776fb5eb-assistant-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '367d6f52-b7c5-41c2-a51d-c0b0776fb5eb-assistant'), 0, 'message'::message_content_blocks_type_enum, '{"role": "assistant", "type": "message", "content": [{"text": "נפלא! הנה הגרסה המעודכנת של המייל:\n\n---\n\nנושא: פגישה במסעדה התאילנדית 🥢\n\nהיי עדי!\n\nאיזה כיף שניפגש היום! ממש מתרגשת לבלות איתך במסעדה התאילנדית. אני בטוחה שיהיה אוכל טעים וגם אווירה מדליקה! 🎉\n\nמחכה לראות אותך,  \nעדי אוקון\n\nאם יש לך שאלות או צריך משהו נוסף, תרגיש חופשי לכתוב לי! 🌟\n\n---\n\nכל שנותר זה להעתיק את התוכן ולשלוח אותו מכתובת הדוא\"ל שלך. בהצלחה! 😊", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:51:13.333441'::timestamptz),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'MdlIucKhLAah-user-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'MdlIucKhLAah-user'), 0, 'message'::message_content_blocks_type_enum, '{"role": "user", "type": "message", "content": [{"text": "השם הוא - עדי אוקון ", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:51:13.825539'::timestamptz - interval '1 second'),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'MdlIucKhLAah-assistant-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'MdlIucKhLAah-assistant'), 0, 'message'::message_content_blocks_type_enum, '{"role": "assistant", "type": "message", "content": [{"text": "נפלא! הנה הגרסה המעודכנת של המייל:\n\n\n\n---\n\n\n\nנושא: פגישה במסעדה התאילנדית 🥢\n\n\n\nהיי עדי!\n\n\n\nאיזה כיף שניפגש היום! ממש מתרגשת לבלות איתך במסעדה התאילנדית. אני בטוחה שיהיה אוכל טעים וגם אווירה מדליקה! 🎉\n\n\n\nמחכה לראות אותך,  \n\nעדי אוקון\n\n\n\nאם יש לך שאלות או צריך משהו נוסף, תרגיש חופשי לכתוב לי! 🌟\n\n\n\n---\n\n\n\nכל שנותר זה להעתיק את התוכן ולשלוח אותו מכתובת הדוא\"ל שלך. בהצלחה! 😊", "type": "text"}]}'::jsonb, 0, '2025-08-25T11:51:13.825539'::timestamptz),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '80744c21-b7e4-4506-a52d-3d569961372e-user-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '80744c21-b7e4-4506-a52d-3d569961372e-user'), 0, 'message'::message_content_blocks_type_enum, '{"role": "user", "type": "message", "content": [{"text": "[no question text]", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:51:44.717275'::timestamptz - interval '1 second'),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '80744c21-b7e4-4506-a52d-3d569961372e-assistant-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '80744c21-b7e4-4506-a52d-3d569961372e-assistant'), 0, 'message'::message_content_blocks_type_enum, '{"role": "assistant", "type": "message", "content": [{"text": "לצערי, איני יכול לשלוח אימיילים ישירות. אני יכול לעזור לך לנסח את תוכן האימייל המושלם ולתת הדרכה איך לשלוח אותו דרך חשבון הדוא\"ל שלך. אם יש לך שאלות נוספות, אני כאן לעזור! 🌟", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:51:44.717275'::timestamptz),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'DYv1Mq2qjV9V-user-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'DYv1Mq2qjV9V-user'), 0, 'message'::message_content_blocks_type_enum, '{"role": "user", "type": "message", "content": [{"text": "אתה לא יכול לשלוח בשבילי?", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:51:45.649515'::timestamptz - interval '1 second'),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'DYv1Mq2qjV9V-assistant-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'DYv1Mq2qjV9V-assistant'), 0, 'message'::message_content_blocks_type_enum, '{"role": "assistant", "type": "message", "content": [{"text": "לצערי, איני יכול לשלוח אימיילים ישירות. אני יכול לעזור לך לנסח את תוכן האימייל המושלם ולתת הדרכה איך לשלוח אותו דרך חשבון הדוא\"ל שלך. אם יש לך שאלות נוספות, אני כאן לעזור! 🌟", "type": "text"}]}'::jsonb, 0, '2025-08-25T11:51:45.649515'::timestamptz),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'fa29fc58-5688-4439-9d72-6be94e2f10f5-user-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'fa29fc58-5688-4439-9d72-6be94e2f10f5-user'), 0, 'message'::message_content_blocks_type_enum, '{"role": "user", "type": "message", "content": [{"text": "[no question text]", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:53:51.440362'::timestamptz - interval '1 second'),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'fa29fc58-5688-4439-9d72-6be94e2f10f5-assistant-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'fa29fc58-5688-4439-9d72-6be94e2f10f5-assistant'), 0, 'message'::message_content_blocks_type_enum, '{"role": "assistant", "type": "message", "content": [{"text": "🏖️🇬🇷 חופשה חלומית ביוון עם החברות הכי טובות! 🇬🇷🏖️\n\nאין כמו לברוח קצת מהשגרה וליהנות מהשמש, הים והנופים המדהימים של יוון. 🏝️ יחד עם החברות האהובות שלי, אנחנו חוות הרפתקאות בלתי נשכחות, צחוק אינסופי ורגעים שנזכור לנצח. 💙\n\n📍 מה בתכנון? \n- חקר איים קסומים 🏝️\n- אוכל מקומי טעים שאין כמוהו 🍴\n- שעות של התרגעות בחופים זהובים 🌅\n- תמונות זכורות מהחיים עם השקיעות היפות בעולם 📸\n\nזה הזמן לחלום, לצחוק וליהנות מכל הטוב שיש לחיים להציע. החברות הטובות ביותר, בארץ היפיפיה ביותר 🥂💫\n\nמוכנות לכבוש את יוון? כי אנחנו כבר בעננים! ☁️✈️\n\n#חופשהביוון #עםהחברות #רגעיםבלתינשכחים #חייםטובים\n\nאל תשכחו לשתף אותנו במה שעושה את החופשה שלכן למושלמת בתגובות! 👇💬", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:53:51.440362'::timestamptz),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'Vgg0DIhgB7CK-user-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'Vgg0DIhgB7CK-user'), 0, 'message'::message_content_blocks_type_enum, '{"role": "user", "type": "message", "content": [{"text": "אשמח שתייצר בשבילי פוסט לחופשה ביוון עם חברות ", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:53:52.482367'::timestamptz - interval '1 second'),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'Vgg0DIhgB7CK-assistant-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, 'Vgg0DIhgB7CK-assistant'), 0, 'message'::message_content_blocks_type_enum, '{"role": "assistant", "type": "message", "content": [{"text": "🏖️🇬🇷 חופשה חלומית ביוון עם החברות הכי טובות! 🇬🇷🏖️\n\n\n\nאין כמו לברוח קצת מהשגרה וליהנות מהשמש, הים והנופים המדהימים של יוון. 🏝️ יחד עם החברות האהובות שלי, אנחנו חוות הרפתקאות בלתי נשכחות, צחוק אינסופי ורגעים שנזכור לנצח. 💙\n\n\n\n📍 מה בתכנון? \n\n- חקר איים קסומים 🏝️\n\n- אוכל מקומי טעים שאין כמוהו 🍴\n\n- שעות של התרגעות בחופים זהובים 🌅\n\n- תמונות זכורות מהחיים עם השקיעות היפות בעולם 📸\n\n\n\nזה הזמן לחלום, לצחוק וליהנות מכל הטוב שיש לחיים להציע. החברות הטובות ביותר, בארץ היפיפיה ביותר 🥂💫\n\n\n\nמוכנות לכבוש את יוון? כי אנחנו כבר בעננים! ☁️✈️\n\n\n\n#חופשהביוון #עםהחברות #רגעיםבלתינשכחים #חייםטובים\n\n\n\nאל תשכחו לשתף אותנו במה שעושה את החופשה שלכן למושלמת בתגובות! 👇💬", "type": "text"}]}'::jsonb, 1, '2025-08-25T11:53:52.482367'::timestamptz),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '0ea28da9-03b0-46dd-bad6-19f1fc3c7ebe-user-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '0ea28da9-03b0-46dd-bad6-19f1fc3c7ebe-user'), 0, 'message'::message_content_blocks_type_enum, '{"role": "user", "type": "message", "content": [{"text": "[no question text]", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:58:14.749113'::timestamptz - interval '1 second'),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '0ea28da9-03b0-46dd-bad6-19f1fc3c7ebe-assistant-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '0ea28da9-03b0-46dd-bad6-19f1fc3c7ebe-assistant'), 0, 'message'::message_content_blocks_type_enum, '{"role": "assistant", "type": "message", "content": [{"text": "היי! אני כאן ובשבילך. איך אפשר לעזור לך היום? 😊", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:58:14.749113'::timestamptz),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '7qvMTJaYUduO-user-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '7qvMTJaYUduO-user'), 0, 'message'::message_content_blocks_type_enum, '{"role": "user", "type": "message", "content": [{"text": "היי מה שלומך?", "type": "text"}]}'::jsonb, NULL::integer, '2025-08-25T11:58:19.268601'::timestamptz - interval '1 second'),
    (uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '7qvMTJaYUduO-assistant-block-0'), uuid_generate_v5('0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'::uuid, '7qvMTJaYUduO-assistant'), 0, 'message'::message_content_blocks_type_enum, '{"role": "assistant", "type": "message", "content": [{"text": "הוספתי פוסט על חופשת חברות ביוון עם תיאור חוויות, פעילויות ותמונות.", "type": "text"}]}'::jsonb, 1, '2025-08-25T11:58:19.268601'::timestamptz)
) AS v(id, message_id, sequence, type, content, execution_time_ms, created_at)
WHERE NOT EXISTS (SELECT 1 FROM message_content_blocks WHERE id = v.id);


-- ============================================================
-- MIGRATION SUMMARY
-- ============================================================
-- Users processed: 1
-- Conversations processed: 2
-- Messages processed: 24
-- Content blocks processed: 24
-- ============================================================
