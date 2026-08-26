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
        'character_prompts': ['{"model": "gpt-4", "content": "You are a helpful assistant", "is_selected": true}'],
        'hack_prompt': ['{"is_selected": false}'],
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
    
    # Check 3: Agent DO block present
    if 'DO $agent_fn$' in sql_content:
        print(f"✓ PASS: Agent DO block found")
    else:
        print(f"❌ FAIL: Agent DO block not found")
        return False

    # Check 4: All expected structural elements present
    expected_elements = [
        'CREATE TABLE IF NOT EXISTS legacy_bot_to_agent_mapping',
        'INSERT INTO agents',
        'INSERT INTO agent_settings',
        'INSERT INTO knowledge_bases',
        'INSERT INTO knowledge_base_assignments',
        'INSERT INTO knowledge_base_items',
        'INSERT INTO migration.id_mappings',
        'INSERT INTO legacy_bot_to_agent_mapping',
        'migration.deterministic_uuid_v4',
    ]

    for element in expected_elements:
        if element in sql_content:
            print(f"✓ PASS: '{element}' found")
        else:
            print(f"❌ FAIL: '{element}' not found")
            return False

    # Check 5: deterministic_uuid_v4 helper function is included
    if 'CREATE OR REPLACE FUNCTION migration.deterministic_uuid_v4' in sql_content:
        print(f"✓ PASS: deterministic_uuid_v4 helper function present")
    else:
        print(f"❌ FAIL: deterministic_uuid_v4 helper function missing")
        return False

    # Check 6: docs_chosen links are generated for the test agent
    if "'doc1'" in sql_content or "'doc2'" in sql_content:
        print(f"✓ PASS: docs_chosen document links found")
    else:
        print(f"❌ FAIL: docs_chosen document links not found")
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
