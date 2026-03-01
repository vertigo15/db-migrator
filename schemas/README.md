# Migration Schema Files

This directory contains database schema files for supporting the V4 → V5 migration process.

## Files

### `migration_id_mappings.sql`
Creates the migration support infrastructure including:
- `migration.id_mappings` - Stores old ID → new UUID mappings
- `migration.batch_log` - Tracks migration batches
- Helper functions for ID lookups
- Indexes for performance
- Progress monitoring views

## Quick Start

### 1. Setup

```powershell
# Run on target V5 database
psql -h <host> -U <username> -d <database> -f migration_id_mappings.sql
```

### 2. Verify

```sql
-- Check tables created
\dt migration.*

-- Check functions created
\df migration.*

-- View empty progress
SELECT * FROM migration.progress_summary;
```

### 3. Use in Migration

See `../MAPPING_TABLE_INTEGRATION.md` for detailed integration guide.

## Benefits

✅ **Performance**: 10-100x faster ID lookups vs JSONB searches  
✅ **Tracking**: Real-time migration progress monitoring  
✅ **Reliability**: Idempotent operations, no duplicates  
✅ **Debugging**: Easy to find ID mappings and orphaned records  
✅ **Auditability**: Complete history of what was migrated when  

## Example Usage

```sql
-- Check if record already migrated
SELECT migration.is_migrated('users', 'old_hash_123');

-- Get new UUID from old ID
SELECT migration.get_new_id('users', 'old_hash_123');

-- Store mapping after migration
INSERT INTO migration.id_mappings (table_name, old_id, new_id, migration_batch)
VALUES ('users', 'old_hash_123', gen_random_uuid(), 'batch_20260228');

-- Monitor progress
SELECT * FROM migration.progress_summary;
```

## When to Use

Use this for **all table migrations** that:
- Generate new UUIDs from old IDs
- Have foreign key relationships to other migrated tables
- Need progress tracking
- Require idempotent operations

## Documentation

- Full integration guide: `../MAPPING_TABLE_INTEGRATION.md`
- Example SQL: `../examples/user_migration_with_mappings.sql`
- Main README: `../README.md`

## Support

For questions or issues:
1. Check `MAPPING_TABLE_INTEGRATION.md`
2. Review example SQL files
3. Test on staging database first
