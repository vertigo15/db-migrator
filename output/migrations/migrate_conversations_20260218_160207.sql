-- ============================================================
-- CONVERSATIONS, MESSAGES & MESSAGE_CONTENT_BLOCKS MIGRATION SQL
-- ============================================================
-- Generated: 2026-02-18T16:02:17.150441
-- Source: jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)
-- Destination: conversations + messages + message_content_blocks
-- Source rows: 396
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
    RAISE NOTICE 'Source rows: 396';
    RAISE NOTICE 'Namespace UUID: 0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b';
    RAISE NOTICE 'Generated: 2026-02-18T16:02:17.150476';
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


-- User: ab7d0370aa10773f1c1db8e3b500374c (Batch 1, 42 conversations)

-- Conversations INSERT
INSERT INTO conversations (id, title, message_count, total_tokens, is_active, deleted_at, created_at, updated_at, last_interacted_at, user_id)
SELECT * FROM (
  VALUES
    ('009ab74c-6047-4292-b8c0-478c2102558a'::uuid, 'Title החשיבות והחזון בכנס ה-NLP הישראלי', 12, 5246, true, NULL, '2025-09-07T06:21:02.471784'::timestamptz, '2025-09-07T07:12:48.254090'::timestamptz, '2025-09-07T07:12:48.254090'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('01dc3995-754f-4cf4-b307-5c9c37cf1a80'::uuid, 'תמצית לוז לכנס עם מושבים מרכזיים ומנהלי בתי ספר', 6, 1174, true, NULL, '2025-09-14T07:50:05.662638'::timestamptz, '2025-09-14T08:00:32.981627'::timestamptz, '2025-09-14T08:00:32.981627'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('03db9003-545d-47ea-8b21-97723a04e221'::uuid, 'הזדמנות להציג מוצרים בכנס NLP', 6, 2344, true, NULL, '2025-10-16T10:53:46.641529'::timestamptz, '2025-10-16T10:55:05.743696'::timestamptz, '2025-10-16T10:55:05.743696'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('072ee505-3217-45e7-9856-da97c079f1a7'::uuid, 'שיפור הודעת הזמנה לכנס NLP', 12, 1818, true, NULL, '2025-10-05T06:16:51.526698'::timestamptz, '2025-10-05T06:28:58.524850'::timestamptz, '2025-10-05T06:28:58.524850'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('0b0aa570-e5c2-4816-91c5-0bcd079258b8'::uuid, 'השגת שלווה והצבת גבולות אישיים', 2, 154, true, NULL, '2025-12-11T20:06:26.039709'::timestamptz, '2025-12-11T20:06:26.039709'::timestamptz, '2025-12-11T20:06:26.039709'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('155781ec-d9b8-4df4-93cb-072cea233115'::uuid, 'Title ניסוח הודעה לבתי ספר בכנס NLP ישראל', 4, 2684, true, NULL, '2025-09-25T11:49:37.318030'::timestamptz, '2025-09-25T11:50:40.103462'::timestamptz, '2025-09-25T11:50:40.103462'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('27c015b6-1f75-4768-a5cd-f9b63a3479d0'::uuid, 'מכתב לתיקון מעמד וקשר בהסמכת NLP', 40, 11147, true, NULL, '2025-09-10T11:51:11.055894'::timestamptz, '2025-09-10T17:04:32.031854'::timestamptz, '2025-09-10T17:04:32.031854'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('2a8fb3e0-11e7-43a3-9c25-5f8b4195d545'::uuid, 'Drafting Legal Warning for Unrecognized Schools', 4, 2019, true, NULL, '2025-09-11T06:22:58.439661'::timestamptz, '2025-09-11T06:25:10.409033'::timestamptz, '2025-09-11T06:25:10.409033'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('2aa90bef-dc7c-426b-aa4e-63f94e202bb9'::uuid, 'Profile Overview of Alin Tohari', 4, 439, true, NULL, '2025-10-28T08:28:46.092848'::timestamptz, '2025-10-28T08:29:06.860397'::timestamptz, '2025-10-28T08:29:06.860397'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('2c4f25f2-210e-4c09-86db-0a116235bddd'::uuid, 'Title ניסוח מסמך בלשון זכר לכל המגדרים', 6, 3018, true, NULL, '2025-09-29T16:31:06.747213'::timestamptz, '2025-09-29T16:32:17.572367'::timestamptz, '2025-09-29T16:32:17.572367'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('2e89b22a-9d4f-4967-b0e7-384e96b93fef'::uuid, 'מהי סדנה פסיכו-חינוכית ואילו מטרות יש לה?', 6, 704, true, NULL, '2025-12-14T10:29:17.301860'::timestamptz, '2025-12-14T10:36:17.993863'::timestamptz, '2025-12-14T10:36:17.993863'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('446dd960-27c4-47f8-9c11-f9dacd494422'::uuid, 'Creating an Effective Presentation for a Professional Community', 8, 1751, true, NULL, '2025-12-18T07:28:27.600284'::timestamptz, '2025-12-18T08:07:02.230680'::timestamptz, '2025-12-18T08:07:02.230680'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('4fe7a6ba-1d2d-4618-96ad-4bffeab78af4'::uuid, 'Translation Request Hebrew to English', 28, 8154, true, NULL, '2025-12-30T15:59:49.536127'::timestamptz, '2025-12-30T17:45:48.419334'::timestamptz, '2025-12-30T17:45:48.419334'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('55663b04-9873-4fdc-99b6-526197cc0ed6'::uuid, 'Rephrasing Legal Standards for Presentation Content', 26, 5472, true, NULL, '2025-09-14T14:46:12.213249'::timestamptz, '2025-09-14T15:52:34.098262'::timestamptz, '2025-09-14T15:52:34.098262'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('62219311-b985-4013-9fb2-6e3c6724a4a8'::uuid, 'Request for Combined Letter Version', 34, 6972, true, NULL, '2025-09-16T08:10:36.849205'::timestamptz, '2025-09-16T11:33:46.102277'::timestamptz, '2025-09-16T11:33:46.102277'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('67741aaa-96b2-45ed-8a59-924f100ac65a'::uuid, 'המלצות למיקוד ושיפור מכתב בירור בעמותה', 18, 7156, true, NULL, '2025-09-10T21:55:13.495632'::timestamptz, '2025-09-10T22:48:12.608375'::timestamptz, '2025-09-10T22:48:12.608375'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('6791ecc6-9400-4b7c-bd55-51ba4088ec85'::uuid, 'Translation and Tense Adjustment Request', 110, 22069, true, NULL, '2026-01-02T10:50:21.912763'::timestamptz, '2026-01-04T16:21:35.446199'::timestamptz, '2026-01-04T16:21:35.446199'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('67d5e190-912f-4a5f-9da3-997e0b36e734'::uuid, 'סיכום החלטות אתיקה ופרוק ועדת תוכן בעמותה', 6, 557, true, NULL, '2026-01-13T14:25:57.591611'::timestamptz, '2026-01-13T15:27:55.878295'::timestamptz, '2026-01-13T15:27:55.878295'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('68c93738-0a4d-4363-af04-13f2ebab6f5a'::uuid, 'לא', 20, 3763, true, NULL, '2025-08-10T06:29:02.245717'::timestamptz, '2025-08-10T06:47:16.880056'::timestamptz, '2025-08-10T06:47:16.880056'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('788da91a-8971-4a8d-b6fc-c39f2abb85b4'::uuid, 'Promoting NLP as a Professional Discipline in Israel', 10, 1703, true, NULL, '2025-11-14T00:17:04.926507'::timestamptz, '2025-11-14T00:55:23.716296'::timestamptz, '2025-11-14T00:55:23.716296'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('794d6eed-47f6-4574-864c-2cb5814d7f00'::uuid, 'אתגרים בתקציב והכנסות עמותה חינוכית', 2, 272, true, NULL, '2025-11-26T13:33:40.799007'::timestamptz, '2025-11-26T13:33:40.799007'::timestamptz, '2025-11-26T13:33:40.799007'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('7cc9de35-4e89-4d4d-ac60-969d908f1703'::uuid, 'Understanding Legal Terms in Administrative Agreements', 4, 416, true, NULL, '2025-09-03T12:50:23.245632'::timestamptz, '2025-09-03T13:23:40.206623'::timestamptz, '2025-09-03T13:23:40.206623'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('8115dd8b-b40e-4ae3-a3cb-d70a514bbafe'::uuid, 'Request to Address Unauthorized Logo Use on Website', 6, 632, true, NULL, '2025-09-17T09:03:53.143865'::timestamptz, '2025-09-17T09:21:50.302397'::timestamptz, '2025-09-17T09:21:50.302397'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('8b5d7dec-b15a-4208-80b0-6854495ca610'::uuid, 'נושא הארכת מדיניות ביטול השתתפות בכנס', 70, 16078, true, NULL, '2025-09-28T06:18:05.113132'::timestamptz, '2025-09-28T09:12:38.074326'::timestamptz, '2025-09-28T09:12:38.074326'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('8ce91fe1-0c83-43b6-89a3-4625c5c4e181'::uuid, 'Request for Volunteer Recruitment for Exceptions Committee', 8, 2792, true, NULL, '2025-12-03T06:46:38.915856'::timestamptz, '2025-12-03T07:10:14.994371'::timestamptz, '2025-12-03T07:10:14.994371'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('a13048f9-67a5-4b03-985b-9dbea1620361'::uuid, 'Request to Draft a Polite Message for Landlords', 12, 1274, true, NULL, '2025-12-30T10:13:36.609604'::timestamptz, '2025-12-30T10:54:02.079038'::timestamptz, '2025-12-30T10:54:02.079038'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('a9030230-60b8-44a8-ba6b-e7a5284c2fc1'::uuid, 'תיקון לתקנון והנחיות חברי העמותה ב-NLP', 34, 5641, true, NULL, '2026-01-11T17:11:26.428766'::timestamptz, '2026-01-11T19:29:00.965370'::timestamptz, '2026-01-11T19:29:00.965370'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('af051709-861c-4d11-a965-1d845226c200'::uuid, 'Conversational Greeting Inquiry', 2, 9, true, NULL, '2026-01-04T11:14:04.300584'::timestamptz, '2026-01-04T11:14:04.300584'::timestamptz, '2026-01-04T11:14:04.300584'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('b78f3157-9cdf-4a06-97ab-d2aa00141dc2'::uuid, 'Title הצעות שמות מגניבות להרצאה בתחום NLP', 14, 4877, true, NULL, '2025-10-23T08:52:23.350676'::timestamptz, '2025-10-23T09:02:27.746241'::timestamptz, '2025-10-23T09:02:27.746241'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('c74cba03-814a-40f8-94e9-ecc410266f4f'::uuid, 'Creating Social Media Post Content for Conference', 10, 1862, true, NULL, '2025-08-11T16:58:42.019132'::timestamptz, '2025-08-11T17:06:28.077103'::timestamptz, '2025-08-11T17:06:28.077103'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('c7c88674-7c72-4f21-859d-9c443f99de74'::uuid, 'ב-26 בינואר 2025, באסיפה כללית של העמותה, נבחרנו אנחנו, אלין טהורי ונחמה פיינסטון, לכהן כיו"ר וכסגנית יו"ר במודל רוטציה. הבחירה שלנו במודל זה נולדה מתוך רצון להבטיח יציבות ניהולית, המשכיות בעשייה ושיתוף פעולה מלא – תוך הצבת טובת העמותה והקהילה במרכז.
גיליון זה נכתב על ידינו – יחד, כצוות מוביל וכשותפות לדרך.
חשוב לנו לשתף אתכם בעדכונים על התקדמותם של נושאים מרכזיים – תהליכים שנעשו בעמותה מתוך מחויבות עמוקה לחזון שלה, לערכי השקיפות, ולרוח השותפות שמובילה אותנו.', 10, 1809, true, NULL, '2025-08-10T09:35:27.983645'::timestamptz, '2025-08-10T09:38:27.923542'::timestamptz, '2025-08-10T09:38:27.923542'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('ca57ac46-7fa5-4e1a-9f0e-f8146b4d3332'::uuid, 'Requesting Removal of NLP Association Logo from Website', 20, 3253, true, NULL, '2025-09-18T10:12:05.335382'::timestamptz, '2025-09-18T12:23:16.399197'::timestamptz, '2025-09-18T12:23:16.399197'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('cf79b60b-c462-42aa-a2e4-df0abcfd6295'::uuid, 'תפקיד המלווה בוועדות בעמותה', 6, 424, true, NULL, '2025-09-06T18:21:26.884178'::timestamptz, '2025-09-06T18:23:08.499796'::timestamptz, '2025-09-06T18:23:08.499796'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('d1ab55e4-9fca-4821-b546-fdbf59762f26'::uuid, 'Translation of קנאה to English', 10, 3667, true, NULL, '2025-12-28T11:14:35.199217'::timestamptz, '2025-12-28T12:18:36.253458'::timestamptz, '2025-12-28T12:18:36.253458'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('d5645cb4-d02b-4a45-9e80-c56ee6cd4cb0'::uuid, 'הצעת מכתב ברכה לבוגר פרקטישינר ב-NLP', 52, 17523, true, NULL, '2025-12-10T09:36:04.267059'::timestamptz, '2025-12-10T13:36:34.682732'::timestamptz, '2025-12-10T13:36:34.682732'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('db1cf394-9d84-46ff-a019-6f94a0396a6f'::uuid, 'Title הצעה לניסוח טקסט ללא חזרתיות', 14, 2902, true, NULL, '2025-12-01T10:52:11.400531'::timestamptz, '2025-12-01T11:09:39.308897'::timestamptz, '2025-12-01T11:09:39.308897'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('e65213bd-94e0-4d0d-a00b-f61191d5f21f'::uuid, 'בנושא אחר: אני צריכה שתעזור לי למצוא חברת שילוח לאיסוף חבילה בארץ לאדם פרטי. מה הטלפון לביצוע ההזמנה?', 24, 10667, true, NULL, '2025-08-04T07:48:20.476164'::timestamptz, '2025-08-04T12:01:26.149507'::timestamptz, '2025-08-04T12:01:26.149507'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('e926d182-0270-47fc-b27e-e29de3657124'::uuid, 'כנס לשכת ה-NLP הישראלית 2025 פרטי האירוע וההצטרפות', 8, 2069, true, NULL, '2025-09-30T01:57:59.813212'::timestamptz, '2025-09-30T03:41:09.389310'::timestamptz, '2025-09-30T03:41:09.389310'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('e9488dd9-8a72-4a0a-a398-36a2077f71be'::uuid, 'סוגי העברה ואתגרים בטיפול פסיכולוגי', 32, 9863, true, NULL, '2025-12-13T19:33:00.816561'::timestamptz, '2025-12-13T21:32:50.926296'::timestamptz, '2025-12-13T21:32:50.926296'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('f37fea85-80bc-436c-a199-bfea546b773f'::uuid, 'Title הצעת שורת הנדון למכתב בירור מעמד בלשכה', 2, 1868, true, NULL, '2025-09-11T09:34:20.273087'::timestamptz, '2025-09-11T09:34:20.273087'::timestamptz, '2025-09-11T09:34:20.273087'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('f8adb0b9-76f8-4212-8e19-9a4df795c4d0'::uuid, 'תיאור מפורט למפגשי עמותה עתידיים', 14, 1451, true, NULL, '2025-08-12T08:37:47.673332'::timestamptz, '2025-08-12T08:49:34.389419'::timestamptz, '2025-08-12T08:49:34.389419'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c')),
    ('fa5df530-3416-45f1-ad58-f31c94f6c657'::uuid, 'Enhancing Self-Integration for Healthier Relationships', 52, 8680, true, NULL, '2025-12-14T14:59:24.175100'::timestamptz, '2025-12-14T16:16:33.281580'::timestamptz, '2025-12-14T16:16:33.281580'::timestamptz, (SELECT id FROM users WHERE metadata->'legacyData'->>'id' = 'ab7d0370aa10773f1c1db8e3b500374c'))
) AS v(id, title, message_count, total_tokens, is_active, deleted_at, created_at, updated_at, last_interacted_at, user_id)
WHERE v.user_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM conversations WHERE id = v.id);

