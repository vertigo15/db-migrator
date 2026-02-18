-- ============================================================
-- CONVERSATIONS, MESSAGES & MESSAGE_CONTENT_BLOCKS MIGRATION SQL
-- ============================================================
-- Generated: 2026-02-17T12:58:47.597541
-- Source: jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)
-- Destination: conversations + messages + message_content_blocks
-- Source rows: 31
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
    RAISE NOTICE 'Source rows: 31';
    RAISE NOTICE 'Namespace UUID: 0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b';
    RAISE NOTICE 'Generated: 2026-02-17T12:58:47.597591';
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


-- User: f2442a210e4377ab4ecf6a636cda59ed (Batch 1, 13 conversations)

-- Conversations INSERT
INSERT INTO conversations (id, title, message_count, total_tokens, is_active, deleted_at, created_at, updated_at, last_interacted_at, user_id)
SELECT * FROM (
  VALUES
    ('2a6960c8-ef5b-48dd-b55a-a075ab1ed6d0'::uuid, 'Adding a Passover Greeting Email Template to Canvas', 6, 240, true, NULL, '2025-04-15T08:06:52.635083'::timestamptz, '2025-04-15T08:07:56.444542'::timestamptz, '2025-04-15T08:07:56.444542'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'f2442a210e4377ab4ecf6a636cda59ed')),
    ('4d39eafb-3e3f-4d9e-a75d-de619a04403a'::uuid, 'שיר פיוטי ומיוחד', 18, 1078, true, NULL, '2025-07-08T11:34:25.782286'::timestamptz, '2025-07-08T11:36:19.196529'::timestamptz, '2025-07-08T11:36:19.196529'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'f2442a210e4377ab4ecf6a636cda59ed')),
    ('51aea67f-b6f2-4e8b-98f7-cd3f91896b27'::uuid, 'תיאור ארכיטקטורה של מערכת מבוזרת בשירותי ענן', 2, 488, true, NULL, '2025-04-15T06:18:23.431117'::timestamptz, '2025-04-15T06:18:23.431117'::timestamptz, '2025-04-15T06:18:23.431117'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'f2442a210e4377ab4ecf6a636cda59ed')),
    ('65724ef9-c625-4916-81c3-6261c2e5875b'::uuid, 'תקציר דוח על מערכת עיבוד תשלומים של Stripe', 2, 159, true, NULL, '2025-04-15T06:10:21.062887'::timestamptz, '2025-04-15T06:10:21.062887'::timestamptz, '2025-04-15T06:10:21.062887'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'f2442a210e4377ab4ecf6a636cda59ed')),
    ('707ffb1c-dc32-4164-9fbf-6f24d717a048'::uuid, 'Growth of Container Exports in USA Over 5 Years', 6, 143, true, NULL, '2025-05-07T07:20:26.380347'::timestamptz, '2025-05-07T07:20:56.592244'::timestamptz, '2025-05-07T07:20:56.592244'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'f2442a210e4377ab4ecf6a636cda59ed')),
    ('7d5b5ad3-c382-4fce-9c46-d29efbdef5e7'::uuid, 'Greeting and Offer to Assist', 2, 10, true, NULL, '2025-03-19T09:29:12.113038'::timestamptz, '2025-03-19T09:29:12.113038'::timestamptz, '2025-03-19T09:29:12.113038'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'f2442a210e4377ab4ecf6a636cda59ed')),
    ('90f7b641-9114-417c-b879-78111d8da1e0'::uuid, 'סיכום דוח SOC 3 של חברת Stripe לשנת 2022-2023', 4, 254, true, NULL, '2025-04-15T08:03:43.492549'::timestamptz, '2025-04-15T08:04:34.240262'::timestamptz, '2025-04-15T08:04:34.240262'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'f2442a210e4377ab4ecf6a636cda59ed')),
    ('943c6c42-61f9-436d-8f49-994beadba2ea'::uuid, 'מידע על חברת אלביט מערכות', 4, 200, true, NULL, '2025-04-15T06:06:58.705499'::timestamptz, '2025-04-15T06:07:14.182359'::timestamptz, '2025-04-15T06:07:14.182359'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'f2442a210e4377ab4ecf6a636cda59ed')),
    ('b12b8b4b-a95d-4e0e-ae47-1aef80c3d4ee'::uuid, 'מהשלומך', 2, 27, true, NULL, '2025-05-13T14:47:17.928114'::timestamptz, '2025-05-13T14:47:17.928114'::timestamptz, '2025-05-13T14:47:17.928114'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'f2442a210e4377ab4ecf6a636cda59ed')),
    ('b5413572-7416-4eb6-9a25-3dbc8451e19b'::uuid, 'הסבר על אי-יכולת שליחת אימייל ישירות', 4, 179, true, NULL, '2025-04-15T06:14:05.202260'::timestamptz, '2025-04-15T06:14:27.235631'::timestamptz, '2025-04-15T06:14:27.235631'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'f2442a210e4377ab4ecf6a636cda59ed')),
    ('ba7cbe2a-c2c6-4bb2-aa16-80a02c62d02f'::uuid, 'התקנת יחידת Be Mesh בנתב Be', 4, 129, true, NULL, '2025-04-15T06:19:17.228027'::timestamptz, '2025-04-15T06:19:30.750078'::timestamptz, '2025-04-15T06:19:30.750078'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'f2442a210e4377ab4ecf6a636cda59ed')),
    ('c652c503-e3fd-4d66-833c-e8947427b3d3'::uuid, NULL, 2, 9, true, NULL, '2025-03-24T18:22:00.943973'::timestamptz, '2025-03-24T18:22:00.943973'::timestamptz, '2025-03-24T18:22:00.943973'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'f2442a210e4377ab4ecf6a636cda59ed')),
    ('e226568f-d4fa-479c-9f96-d03eeae6d3c8'::uuid, 'התחברות לאינטרנט דרך שירות לקוחות בזק', 6, 170, true, NULL, '2025-04-15T06:08:02.361904'::timestamptz, '2025-04-15T06:08:43.257298'::timestamptz, '2025-04-15T06:08:43.257298'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'f2442a210e4377ab4ecf6a636cda59ed'))
) AS v(id, title, message_count, total_tokens, is_active, deleted_at, created_at, updated_at, last_interacted_at, user_id)
WHERE v.user_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM conversations WHERE id = v.id);

