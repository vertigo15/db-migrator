import psycopg2

conn = psycopg2.connect(
    host='jeen-dev-db-migration-test.postgres.database.azure.com',
    port=5432,
    database='document_db',
    user='jeen_dev_db_admin',
    password='Jddb171125',
    options='-c client_encoding=UTF8'
)
conn.autocommit = True
cur = conn.cursor()

print("client_encoding:", conn.encoding)

# Test 1: direct cast
cur.execute("SELECT 'PROCESSED'::public.documents_status_enum")
print("Test 1 - Direct cast:", cur.fetchone())

# Test 2: DO block with cast
cur.execute("""
DO $$
DECLARE
    v_val public.documents_status_enum := 'PROCESSED'::public.documents_status_enum;
BEGIN
    RAISE NOTICE 'enum value: %', v_val;
END $$;
""")
print("Test 2 - DO block cast: OK")

# Test 3: DO block INSERT with three-part name
cur.execute("""
DO $$
DECLARE
    v_id UUID := gen_random_uuid();
BEGIN
    INSERT INTO document_db.public.documents (
        id, status, file_name, file_size, storage_type, storage_path,
        metadata, created_at, updated_at, source_type
    ) VALUES (
        v_id,
        'PROCESSED'::public.documents_status_enum,
        'debug-test.csv', 1, 'azure', 'debug/path',
        '{}'::jsonb, now(), now(),
        'upload'::public.documents_source_type_enum
    ) ON CONFLICT (id) DO NOTHING;
    RAISE NOTICE 'Inserted: %', v_id;
END $$;
""")
print("Test 3 - DO block INSERT: OK")

cur.close()
conn.close()
print("All tests passed!")
