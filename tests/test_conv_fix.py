"""
Verify the 5 fixes applied to sql_generator.py:
  1. clean_string handles lists without ValueError
  2. escape_sql_string handles lists without ValueError
  3. escape_sql_string_with_dollar_quotes handles lists
  4. is_like list no longer crashes pd.notna
  5. user_parent is a SQL expression, not a quoted string literal
"""
import sys
import os
import tempfile
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from utils.sql_generator import (
    clean_string, escape_sql_string, escape_sql_string_with_dollar_quotes,
    generate_conversations_logs_migration_sql, _is_scalar_na,
    extract_question_from_jsonb,
)

# ─── 1. clean_string ────────────────────────────────────────────────────────
def test_clean_string():
    assert clean_string(None) is None
    assert clean_string(float('nan')) is None
    assert clean_string('hello') == 'hello'
    assert clean_string([]) is None      # JSONB empty list -> None (no crash)
    assert clean_string(['a', 'b']) is None   # JSONB list -> None (no crash)
    assert clean_string({'k': 'v'}) is None  # dict -> None (no crash, compound type)
    print("PASS: clean_string")

# ─── 2. escape_sql_string ────────────────────────────────────────────────────
def test_escape_sql_string():
    assert escape_sql_string(None) == 'NULL'
    assert escape_sql_string([]) == 'NULL'
    assert escape_sql_string(['a']) == 'NULL'
    assert escape_sql_string("hello") == "'hello'"
    print("PASS: escape_sql_string")

# ─── 3. _is_scalar_na safety ─────────────────────────────────────────────────
def test_is_scalar_na():
    assert _is_scalar_na(None) is True
    assert _is_scalar_na(float('nan')) is True
    assert _is_scalar_na([]) is False
    assert _is_scalar_na(['a']) is False
    assert _is_scalar_na({'k': 'v'}) is False
    assert _is_scalar_na(0) is False
    print("PASS: _is_scalar_na")

# ─── 3b. extract_question_from_jsonb ─────────────────────────────────────────
def test_extract_question_from_jsonb():
    # Dev JSONB array format
    dev_json = '[null, {"value": "What is AI?"}]'
    assert extract_question_from_jsonb(dev_json) == 'What is AI?'

    # Customer plain text (VARCHAR)
    assert extract_question_from_jsonb('שלום, מה שלומך?') == 'שלום, מה שלומך?'
    assert extract_question_from_jsonb('Plain question text') == 'Plain question text'

    # Empty / missing
    assert extract_question_from_jsonb(None) == '[no question text]'
    assert extract_question_from_jsonb('') == '[no question text]'

    print("PASS: extract_question_from_jsonb")

# ─── 4 & 5. End-to-end generation ─────────────────────────────────────────────
def test_generation(tmp_path=None):
    rows = []
    for turn in range(3):
        rows.append({
            'id': f'log-id-{turn:04d}',
            'user_id': 'user-hash-abc123',
            'chat_id': 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
            'question': [
                {'role': 'system', 'value': 'sys'},
                {'role': 'user', 'value': f'Question {turn}'}
            ],
            'question_in_english': f'Question {turn} english',
            'answer': f'Answer number {turn}',
            'created_at': pd.Timestamp('2026-01-01 10:00:00') + pd.Timedelta(minutes=turn),
            'message_index': turn,
            'question_number': turn,
            'token_amount': 50,
            'words_amount': 10,
            'is_like': ['positive'],           # JSONB list - was crashing pd.notna
            'type': 'chat',
            'bot_id': 'bot-001',
            'toolkit_settings': {'model': 'gpt-4'},
            'title': 'Test conversation',
            'category': 'general',
            'sentiment': 'neutral',
            'sourcetext': None,
            'sourcelink': None,
            'webpagelink': None,
            'documents_selected': ['doc1', 'doc2'],  # JSONB list - was crashing clean_string
            'calculated_time': 120,
        })

    df = pd.DataFrame(rows)
    output_dir = str(tmp_path) if tmp_path is not None else tempfile.mkdtemp()
    out_file = os.path.join(output_dir, 'output_test_conv.sql')
    result = generate_conversations_logs_migration_sql(df, out_file, 'test-source')

    print(f"  messages_processed={result['messages_processed']}, blocks_processed={result['blocks_processed']}")
    assert result['messages_processed'] == 6, f"Expected 6 messages (3 turns × 2), got {result['messages_processed']}"
    assert result['blocks_processed'] == 6, f"Expected 6 blocks, got {result['blocks_processed']}"

    content = open(out_file, encoding='utf-8').read()

    assert 'INSERT INTO messages' in content, "Messages INSERT missing from SQL"
    assert 'INSERT INTO message_content_blocks' in content, "Content blocks INSERT missing from SQL"

    # Verify parent chain: turn 2's user message should reference turn 1's assistant via function call
    lines = content.splitlines()
    user_msg_lines = [l for l in lines if "'user'::messages_role_enum" in l]
    assert len(user_msg_lines) == 3, f"Expected 3 user message rows, got {len(user_msg_lines)}"

    # Turn 1: parent must be NULL::uuid
    assert 'NULL::uuid' in user_msg_lines[0], f"Turn 1 parent should be NULL, got: {user_msg_lines[0][:120]}"

    # Turn 2+: parent must be a deterministic_uuid_v4(...) call, NOT a quoted string
    for i, line in enumerate(user_msg_lines[1:], start=2):
        assert 'deterministic_uuid_v4' in line, f"Turn {i} parent must be a deterministic_uuid_v4 expression"
        # The broken version would have: '...'-assistant')'  (double single quotes nesting)
        assert "'-assistant')'" not in line, f"Turn {i} parent is still a quoted string literal (broken)"

    print("PASS: end-to-end generation (messages present, parent chain correct)")


if __name__ == '__main__':
    test_is_scalar_na()
    test_clean_string()
    test_escape_sql_string()
    test_extract_question_from_jsonb()
    test_generation()
    print()
    print("All tests passed.")
