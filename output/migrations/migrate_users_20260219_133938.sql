-- ============================================================
-- USERS MIGRATION SQL
-- ============================================================
-- Generated: 2026-02-19T13:39:40.072482
-- Source: jeen-pg-dev-weu.postgres.database.azure.com:5432/postgres (prefix: jeen_dev)
-- Destination: user_db.public.users
-- Records to migrate: 3
-- 
-- IMPORTANT: This script will INSERT records into the target database!
-- IMPORTANT: Review organization_id and other constants before execution!
--
-- Each INSERT checks if record already exists before inserting.
-- ============================================================

-- CONFIRMATION PROMPT: User must confirm before execution
DO $$
DECLARE
    user_confirmation TEXT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'USERS MIGRATION - CONFIRMATION REQUIRED';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'This script will migrate 3 records to: user_db.public.users';
    RAISE NOTICE 'Organization ID: 356b50f7-bcbd-42aa-9392-e1605f42f7a1';
    RAISE NOTICE 'Generated: 2026-02-19T13:39:40.072482';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';
    
    user_confirmation := NULL;
    
    IF current_setting('is_superuser') = 'off' THEN
        RAISE NOTICE 'Ready to proceed. Press Ctrl+C to cancel or Enter to continue...';
    END IF;
    
    RAISE NOTICE 'Starting migration...';
    RAISE NOTICE '';
END $$;

-- Uncomment the lines below to require manual confirmation (recommended for first run)
-- Note: These are psql meta-commands that work in interactive psql sessions
-- \\prompt 'Type YES to confirm and continue with migration: ' user_confirmation
-- \\if :'user_confirmation' != 'YES'
--   \\echo 'Migration cancelled by user.'
--   \\quit
-- \\endif


-- User: adi@jeen.ai
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM user_db.public.users 
        WHERE email = 'adi@jeen.ai' OR metadata->'legacyData'->>'id' = 'de0ff05457533c93fdf3e0d1cdd0f808'
    ) THEN
        INSERT INTO user_db.public.users (
            email,
            first_name,
            last_name,
            username,
            avatar_url,
            metadata,
            created_at,
            updated_at,
            deleted_at,
            id,
            zitadel_user_id,
            organization_id,
            is_owner,
            preferred_language
        ) VALUES (
            'adi@jeen.ai',
            'adi',
            NULL,
            'adi',
            NULL,
            '{"legacyData": {"id": "de0ff05457533c93fdf3e0d1cdd0f808", "job": null, "model": ["gemini-2.5-pro-preview-06-05", "gpt-oss-120b", "gpt-5", "gpt-5-mini", "gpt-5-nano", "gpt-5.1", "gpt-4o"], "group_id": "1", "azure_oid": null, "department": null, "token_used": "287", "words_used": "141", "subfeatures": {"reasoning": false, "web_search": true, "control_panel": true, "reasoning_web": true, "see_all_agents": false, "create_new_agent": true, "read_aloud_message": false, "organizational_files": false}, "token_limit": "1000000", "company_name": null, "phone_number": null, "last_connected": "1770025989837", "letter_checkbox": null, "times_connected": "11", "enabled_features": ["admin", "voice", "sources", "automation", "chat", "workflow", "interactive"], "history_categories": ["tech", "tools", "ai"], "company_name_in_hebrew": null}}'::jsonb,
            '2025-08-25T07:15:18.828417',
            now(),
            NULL,
            gen_random_uuid(),
            NULL,
            '356b50f7-bcbd-42aa-9392-e1605f42f7a1'::uuid,
            false,
            NULL
        );
    END IF;
END $$;


-- User: amitt@xactech.co.il
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM user_db.public.users 
        WHERE email = 'amitt@xactech.co.il' OR metadata->'legacyData'->>'id' = '9472542407fdce30dd0f37f93818f745'
    ) THEN
        INSERT INTO user_db.public.users (
            email,
            first_name,
            last_name,
            username,
            avatar_url,
            metadata,
            created_at,
            updated_at,
            deleted_at,
            id,
            zitadel_user_id,
            organization_id,
            is_owner,
            preferred_language
        ) VALUES (
            'amitt@xactech.co.il',
            'amitt',
            NULL,
            'amitt',
            NULL,
            '{"legacyData": {"id": "9472542407fdce30dd0f37f93818f745", "job": null, "model": ["gemini-2.5-pro-preview-06-05", "gpt-oss-120b", "gpt-5", "gpt-5-mini", "gpt-5-nano", "gpt-5.1", "gpt-4o"], "group_id": "1", "azure_oid": null, "department": null, "token_used": "0", "words_used": "0", "subfeatures": {"reasoning": false, "web_search": true, "control_panel": true, "reasoning_web": true, "see_all_agents": false, "create_new_agent": true, "read_aloud_message": false, "organizational_files": false}, "token_limit": "1000000", "company_name": null, "phone_number": null, "last_connected": "1717492006958", "letter_checkbox": null, "times_connected": "18", "enabled_features": ["admin", "voice", "sources", "automation", "chat", "workflow", "interactive"], "history_categories": null, "company_name_in_hebrew": null}}'::jsonb,
            '2023-12-27T11:05:57.156245',
            now(),
            NULL,
            gen_random_uuid(),
            NULL,
            '356b50f7-bcbd-42aa-9392-e1605f42f7a1'::uuid,
            false,
            NULL
        );
    END IF;
END $$;


-- User: alonv@mta.ac.il
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM user_db.public.users 
        WHERE email = 'alonv@mta.ac.il' OR metadata->'legacyData'->>'id' = 'e132f7d5e5b1f7fa7a365f872cec7356'
    ) THEN
        INSERT INTO user_db.public.users (
            email,
            first_name,
            last_name,
            username,
            avatar_url,
            metadata,
            created_at,
            updated_at,
            deleted_at,
            id,
            zitadel_user_id,
            organization_id,
            is_owner,
            preferred_language
        ) VALUES (
            'alonv@mta.ac.il',
            'alon',
            NULL,
            'alonv',
            NULL,
            '{"legacyData": {"id": "e132f7d5e5b1f7fa7a365f872cec7356", "job": null, "model": ["gemini-2.5-pro-preview-06-05", "gpt-oss-120b", "gpt-5", "gpt-5-mini", "gpt-5-nano", "gpt-5.1", "gpt-4o"], "group_id": "1", "azure_oid": null, "department": null, "token_used": "0", "words_used": "0", "subfeatures": {"reasoning": false, "web_search": true, "control_panel": true, "reasoning_web": true, "see_all_agents": false, "create_new_agent": true, "read_aloud_message": false, "organizational_files": false}, "token_limit": "1000000", "company_name": null, "phone_number": null, "last_connected": "1718791364363", "letter_checkbox": null, "times_connected": "35", "enabled_features": ["admin", "voice", "sources", "automation", "chat", "workflow", "interactive"], "history_categories": null, "company_name_in_hebrew": null}}'::jsonb,
            '2024-03-12T07:29:39.532941',
            now(),
            NULL,
            gen_random_uuid(),
            NULL,
            '356b50f7-bcbd-42aa-9392-e1605f42f7a1'::uuid,
            false,
            NULL
        );
    END IF;
END $$;


-- Total records processed: 3
-- Skipped (no email): 0
