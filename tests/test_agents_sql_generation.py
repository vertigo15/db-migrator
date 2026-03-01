"""
Test agents SQL generation to verify namespace_uuid replacement and temp table structure.
"""
import os
import sys
import pandas as pd

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.sql_generator import generate_agents_migration_sql


def test_agents_sql_generation():
    """Test that agents SQL generation works and replaces namespace_uuid correctly."""
    
    # Create a minimal test DataFrame with one agent
    test_data = {
        'bot_id': ['test_bot_123'],
        'user_id': ['test_user_456'],
        'folder_id': [1],
        'bot_data': ['{"bot_name": "Test Agent", "bot_description": "Test Description"}'],
        'toolkit_settings': ['{"is_active": "Yes", "data": {}}'],
        'character_prompts': ['{"model": "gpt-4", "content": "You are a helpful assistant"}'],
        'hack_prompt': ['{}'],
        'analysis_prompt': ['{"is_selected": false}'],
        'grade_prompt': ['{"is_selected": false}'],
        'relevant_answer_prompt': ['{"is_selected": false}'],
        'first_message': ['Hello! How can I help?'],
        'additional_links_title': ['{"is_selected": false}'],
        'docs_chosen': [['doc1', 'doc2']],
        'chosen_docs_folders': [[1, 2]],
        'created_at': [pd.Timestamp('2024-01-01')],
        'updated_at': [pd.Timestamp('2024-01-02')],
        'last_activity': [pd.Timestamp('2024-01-03')],
        'deleted_at': [None]
    }
    
    agents_df = pd.DataFrame(test_data)
    
    # Generate SQL
    output_file = 'output/test_agents_generation.sql'
    namespace_uuid = '0b1e4c6a-1f4a-4b6e-8c3d-2a5f7e9d0c1b'
    
    result = generate_agents_migration_sql(
        agents_df=agents_df,
        output_file=output_file,
        source_info='test_source',
        namespace_uuid=namespace_uuid
    )
    
    print(f"\n✓ Generated SQL file: {result['file']}")
    print(f"✓ Agents processed: {result['agents_processed']}")
    
    # Read the generated file
    with open(output_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # Verify namespace_uuid was replaced
    print("\n=== Verification Checks ===")
    
    # Check 1: No template placeholders remaining
    if '{{namespace_uuid}}' in sql_content or '{namespace_uuid}' in sql_content:
        print(f"❌ FAIL: Template placeholder {{namespace_uuid}} still present in SQL")
        return False
    else:
        print(f"✓ PASS: No template placeholders found")
    
    # Check 2: Actual namespace UUID is present
    if namespace_uuid in sql_content:
        print(f"✓ PASS: Namespace UUID '{namespace_uuid}' found in SQL")
    else:
        print(f"❌ FAIL: Namespace UUID '{namespace_uuid}' not found in SQL")
        return False
    
    # Check 3: Temp table created
    if 'CREATE TEMP TABLE _migration_bots' in sql_content:
        print(f"✓ PASS: Temp table _migration_bots created")
    else:
        print(f"❌ FAIL: Temp table _migration_bots not found")
        return False
    
    # Check 4: All expected steps present
    expected_steps = [
        'STEP 0: CREATE MAPPING TABLE',
        'STEP 1: BUILD WORKING TABLE',
        'STEP 2: INSERT AGENTS',
        'STEP 3: INSERT AGENT SETTINGS',
        'STEP 4: INSERT AGENT_DOCUMENTS (from docs_chosen array)',
        'STEP 5: INSERT AGENT_DOCUMENTS (from chosen_docs_folders array)',
        'STEP 6: INSERT INTO MIGRATION.ID_MAPPINGS',
        'STEP 7: POPULATE LEGACY MAPPING TABLE'
    ]
    
    for step in expected_steps:
        if step in sql_content:
            print(f"✓ PASS: {step} found")
        else:
            print(f"❌ FAIL: {step} not found")
            return False
    
    # Check 5: Explicit defaults present
    if 'false  -- explicit default' in sql_content or 'false) AS base_answers_on_files_only' in sql_content:
        print(f"✓ PASS: Explicit defaults found")
    else:
        print(f"⚠ WARNING: Explicit default comments may not be present")
    
    # Check 6: Empty string check present
    if "TRIM(doc_id_elem) != ''" in sql_content:
        print(f"✓ PASS: Empty string check found for docs_chosen")
    else:
        print(f"❌ FAIL: Empty string check not found")
        return False
    
    # Show sample of generated SQL
    print(f"\n=== Sample SQL (first 50 lines) ===")
    lines = sql_content.split('\n')[:50]
    for i, line in enumerate(lines, 1):
        print(f"{i:3}| {line}")
    
    print(f"\n✓ All checks passed!")
    print(f"✓ Generated file: {os.path.abspath(output_file)} ({len(sql_content)} bytes)")
    
    return True


if __name__ == '__main__':
    try:
        success = test_agents_sql_generation()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
