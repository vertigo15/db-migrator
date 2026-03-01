"""
Tests for dollar-quoting fixes in sql_generator.

Covers:
  1. Correct PostgreSQL syntax: $TAG$content$TAG$  (not $$TAG$$)
  2. Outer DO block uses tagged dollar quote ($chunk_fn$) so $$ in content
     doesn't prematurely close the block.
  3. Tag collision avoidance when content contains the tag literal.
"""
import sys
import os
import re

# Add project root to path so we can import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from utils.sql_generator import (
    escape_sql_string_with_dollar_quotes,
    generate_chunk_and_embedding_inserts,
)


# ── 1. escape_sql_string_with_dollar_quotes ─────────────────────────────────

def test_basic_dollar_quoting():
    """Output must use $TAG$...$TAG$, NOT $$TAG$$."""
    result = escape_sql_string_with_dollar_quotes("hello world", tag="T")
    assert result == "$T$hello world$T$", f"Got: {result}"


def test_dollar_quoting_with_hebrew():
    """Hebrew content should be wrapped correctly."""
    hebrew = "שלום עולם"
    result = escape_sql_string_with_dollar_quotes(hebrew, tag="ORIG")
    assert result.startswith("$ORIG$"), f"Bad opening: {result[:20]}"
    assert result.endswith("$ORIG$"), f"Bad closing: {result[-20:]}"
    assert hebrew in result


def test_dollar_quoting_none_returns_null():
    assert escape_sql_string_with_dollar_quotes(None) == "NULL"


def test_dollar_quoting_empty_returns_null():
    assert escape_sql_string_with_dollar_quotes("") == "NULL"


def test_tag_collision_avoidance():
    """If content contains $TAG$, the tag must be adjusted."""
    content = "before $ORIG$ after"
    result = escape_sql_string_with_dollar_quotes(content, tag="ORIG")
    # The tag should have been changed (appended _) since content has $ORIG$
    assert not result.startswith("$ORIG$"), (
        "Tag should have been changed to avoid collision"
    )
    assert result.startswith("$ORIG_$"), f"Expected ORIG_ tag, got: {result[:20]}"
    assert content in result


def test_content_with_double_dollar():
    """Content containing $$ should not break the dollar-quoted string."""
    content = "the price is $$100 per item"
    result = escape_sql_string_with_dollar_quotes(content, tag="ORIG")
    assert result == f"$ORIG${content}$ORIG$", f"Got: {result}"


def test_no_double_dollar_wrapper():
    """Ensure we never produce $$TAG$$ pattern (the old bug)."""
    result = escape_sql_string_with_dollar_quotes("test", tag="X")
    assert "$$X$$" not in result, "Old $$TAG$$ bug detected"


# ── 2. generate_chunk_and_embedding_inserts ──────────────────────────────────

def _make_chunk_row(document_text, embeddings=None):
    """Helper: build a minimal pandas Series that looks like a jeen_dev row."""
    import json
    metadata = json.dumps({
        "doc_id": "doc_abc",
        "user_id": "user_123",
        "type": "chunk-data",
        "tags": [],
        "file_title": "test.pdf",
        "create_date": "2026-01-01",
    })
    return pd.Series({
        "id": "legacy_001",
        "external_id": "ext_001",
        "collection": "col_1",
        "document": document_text,
        "metadata": metadata,
        "embeddings": embeddings,
    })


def test_chunk_outer_block_uses_tagged_dollar_quote():
    """The DO block must NOT use bare $$ — it should use $chunk_fn$."""
    row = _make_chunk_row("Simple text without special chars")
    sql = generate_chunk_and_embedding_inserts(row, chunk_index=0)
    assert sql is not None

    # Must contain the tagged DO block, not bare DO $$
    assert "DO $chunk_fn$" in sql, "Outer DO block should use $chunk_fn$ tag"
    assert "END $chunk_fn$;" in sql, "Outer END should use $chunk_fn$ tag"


def test_chunk_with_double_dollar_in_content():
    """$$ in Hebrew/multilingual content must not break the outer DO block."""
    row = _make_chunk_row("For example, the word hello: $$hello division$$")
    sql = generate_chunk_and_embedding_inserts(row, chunk_index=0)
    assert sql is not None

    # The outer block tag must differ from $$
    assert "DO $$\n" not in sql, "Bare DO $$ would break on $$ in content"
    # Content must appear inside the SQL
    assert "$$hello division$$" in sql


def test_chunk_with_dollar_tag_in_content():
    """If content contains $chunk_fn$, the outer tag should adapt."""
    row = _make_chunk_row("text with $chunk_fn$ literal inside")
    sql = generate_chunk_and_embedding_inserts(row, chunk_index=0)
    assert sql is not None

    # Tag must have been adjusted (appended _)
    assert "DO $chunk_fn_$" in sql, (
        f"Outer tag should have been adjusted. SQL start: {sql[:200]}"
    )
    assert "END $chunk_fn_$;" in sql


def test_chunk_content_tags_are_correct_syntax():
    """Inner $ORIG$/$TRANS$ tags must use correct PostgreSQL syntax."""
    row = _make_chunk_row(
        "excerptKeywords: kw\n\ntranslated_content:\nתרגום\n\noriginal_content:\nמקור"
    )
    sql = generate_chunk_and_embedding_inserts(row, chunk_index=0)
    assert sql is not None

    # Should have $ORIG$...$ORIG$ for original content
    assert "$ORIG$" in sql, "Missing $ORIG$ tag"
    # Should have $TRANS$...$TRANS$ for translated content
    assert "$TRANS$" in sql, "Missing $TRANS$ tag"
    # Must NOT have the old broken pattern $$ORIG$$
    assert "$$ORIG$$" not in sql, "Old $$TAG$$ bug in ORIG"
    assert "$$TRANS$$" not in sql, "Old $$TAG$$ bug in TRANS"


def test_embedding_block_still_works():
    """Embedding DO block (no text content) should generate valid SQL."""
    row = _make_chunk_row("simple content", embeddings="[0.1,0.2,0.3]")
    sql = generate_chunk_and_embedding_inserts(row, chunk_index=0)
    assert sql is not None
    assert "INSERT INTO embeddings" in sql


def test_embedding_block_uses_named_tag():
    """Embedding DO block must use $emb_fn$ tag, not bare $$."""
    row = _make_chunk_row("simple content", embeddings="[0.1,0.2,0.3]")
    sql = generate_chunk_and_embedding_inserts(row, chunk_index=0)
    assert sql is not None

    # Find the embedding portion of the SQL (after "-- Embedding for chunk")
    emb_idx = sql.index("-- Embedding for chunk")
    emb_sql = sql[emb_idx:]

    assert "DO $emb_fn$" in emb_sql, "Embedding DO block should use $emb_fn$ tag"
    assert "END $emb_fn$;" in emb_sql, "Embedding END should use $emb_fn$ tag"


def test_no_bare_double_dollar_in_full_output():
    """Neither chunk nor embedding blocks should use bare DO $$."""
    row = _make_chunk_row(
        "excerptKeywords: kw\n\ntranslated_content:\ntranslated\n\noriginal_content:\noriginal",
        embeddings="[0.1,0.2,0.3]"
    )
    sql = generate_chunk_and_embedding_inserts(row, chunk_index=0)
    assert sql is not None

    # Split on newlines and check no line has bare "DO $$" or "END $$;"
    for line in sql.split('\n'):
        stripped = line.strip()
        assert stripped != "DO $$", f"Found bare 'DO $$' in line: {line}"
        assert stripped != "END $$;", f"Found bare 'END $$;' in line: {line}"


# ── Run ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  FAIL  {t.__name__}: {e}")
            failed += 1
    print(f"\n{'='*60}")
    print(f"Results: {passed} passed, {failed} failed, {passed+failed} total")
    if failed:
        sys.exit(1)
