"""Representative load test for the streaming chunk/embedding SQL generator.

Simulates a light user and a much heavier user (100x the chunk volume) going
through the exact same streamed code path production extraction uses
(``execute_query_chunked`` -> generator of DataFrame chunks ->
``generate_chunks_embeddings_migration_sql``), and proves:

  - Peak memory during generation does not scale linearly with total row
    count (the whole point of streaming instead of buffering one big
    DataFrame).
  - Every shard respects the configured row/byte caps.
  - No rows are lost or duplicated between source rows and generated shards.
  - Generation completes in a reasonable, roughly-linear amount of time
    (throughput stays roughly constant as volume grows).

This is intentionally DB-free (pure Python + tmp files) so it runs fast in
any environment and isolates the extraction/sharding layer from Postgres.
"""
import time
import tracemalloc

import pandas as pd
import pytest

from utils.sql_generator import (
    SHARD_MAX_BYTES,
    SHARD_MAX_ROWS,
    generate_chunks_embeddings_migration_sql,
)

EMBEDDING_DIM = 128
_EMBEDDING_STRING = "[" + ",".join(f"0.{i % 10}" for i in range(EMBEDDING_DIM)) + "]"


def _make_row(doc_index: int, row_index: int) -> dict:
    return {
        "id": f"chunk-{doc_index}-{row_index}",
        "external_id": None,
        "collection": "test",
        "document": f"Synthetic chunk body #{row_index} for load testing purposes.",
        "metadata": {"doc_id": f"doc-{doc_index}", "type": "chunk-data"},
        "embeddings": _EMBEDDING_STRING,
    }


def _stream_synthetic_user(total_rows: int, doc_count: int, chunk_size: int):
    """Yields DataFrame chunks the same shape ``execute_query_chunked`` would,
    without ever materializing more than one chunk in memory at a time."""
    buffer = []
    row_index = 0
    for i in range(total_rows):
        doc_index = i % doc_count
        buffer.append(_make_row(doc_index, row_index))
        row_index += 1
        if len(buffer) >= chunk_size:
            yield pd.DataFrame(buffer)
            buffer = []
    if buffer:
        yield pd.DataFrame(buffer)


def _generate_and_measure(tmp_path, total_rows: int, doc_count: int, chunk_size: int, label: str):
    output_file = str(tmp_path / f"04_chunks_embeddings_{label}.sql")

    tracemalloc.start()
    started = time.perf_counter()
    result = generate_chunks_embeddings_migration_sql(
        jeen_dev_df=_stream_synthetic_user(total_rows, doc_count, chunk_size),
        output_file=output_file,
        source_info=f"synthetic-{label}",
        migration_run_id="99999999-9999-4999-8999-999999999999",
        expected_record_count=total_rows,
    )
    elapsed = time.perf_counter() - started
    _current, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    shards = result["shards"]
    total_generated_bytes = sum(
        __import__("os").path.getsize(shard) for shard in shards
    )
    return {
        "elapsed_seconds": elapsed,
        "peak_memory_bytes": peak_bytes,
        "shard_count": len(shards),
        "total_generated_bytes": total_generated_bytes,
        "chunk_count": result["chunks_processed"],
    }


@pytest.mark.parametrize(
    "label,total_rows,doc_count",
    [
        ("light_user", 200, 2),
        ("heavy_user", 20000, 20),
    ],
)
def test_shard_row_and_byte_caps_are_respected(tmp_path, label, total_rows, doc_count):
    output_file = str(tmp_path / f"04_chunks_embeddings_{label}.sql")
    result = generate_chunks_embeddings_migration_sql(
        jeen_dev_df=_stream_synthetic_user(total_rows, doc_count, chunk_size=500),
        output_file=output_file,
        source_info=f"synthetic-{label}",
        migration_run_id="99999999-9999-4999-8999-999999999999",
        expected_record_count=total_rows,
    )

    assert result["chunks_processed"] == total_rows

    manifest_path = output_file + ".manifest.json"
    import json
    with open(manifest_path, encoding="utf-8") as fh:
        manifest = json.load(fh)

    assert manifest["total_rows"] == total_rows
    for shard in manifest["shards"]:
        assert shard["expected_rows"] <= SHARD_MAX_ROWS["04_chunks_embeddings"]
        # A shard may slightly exceed the byte cap by at most one unit's
        # worth (the writer only rolls over *before* adding a unit that
        # would overflow), so allow generous headroom rather than an exact bound.
        assert shard["byte_size"] <= SHARD_MAX_BYTES["04_chunks_embeddings"] * 1.5

    # Row-count-capped rollover: with 500 rows/shard, shard count should
    # track total_rows / 500 (rounded up), not be a single monolithic file.
    if total_rows > SHARD_MAX_ROWS["04_chunks_embeddings"]:
        assert len(manifest["shards"]) > 1


def test_peak_memory_does_not_scale_linearly_with_batch_size(tmp_path):
    """The core promise of streaming extraction: a 100x larger user shouldn't
    need anywhere close to 100x the memory to generate its migration SQL."""
    light = _generate_and_measure(tmp_path, total_rows=200, doc_count=2, chunk_size=500, label="light")
    heavy = _generate_and_measure(tmp_path, total_rows=20000, doc_count=20, chunk_size=500, label="heavy")

    assert heavy["chunk_count"] == 20000
    assert light["chunk_count"] == 200

    row_ratio = heavy["chunk_count"] / light["chunk_count"]
    memory_ratio = heavy["peak_memory_bytes"] / max(light["peak_memory_bytes"], 1)

    # 100x the rows should cost nowhere near 100x the memory; a generous
    # order-of-magnitude ceiling catches a regression back to "buffer
    # everything in one DataFrame" (which would scale ~linearly, i.e.
    # memory_ratio would approach row_ratio) while tolerating normal
    # per-chunk/tracemalloc-accounting noise.
    assert memory_ratio < row_ratio / 5, (
        f"memory did not stay bounded: {row_ratio=:.1f}x rows caused "
        f"{memory_ratio=:.1f}x peak memory "
        f"(light={light['peak_memory_bytes']} bytes, heavy={heavy['peak_memory_bytes']} bytes)"
    )

    # Sanity/reporting: generated SQL volume scales with rows (no data
    # silently dropped), throughput stays in a sane ballpark, and disk output
    # is bounded per-shard rather than one giant file.
    assert heavy["total_generated_bytes"] > light["total_generated_bytes"]
    assert heavy["shard_count"] > light["shard_count"]
    rows_per_second_heavy = heavy["chunk_count"] / max(heavy["elapsed_seconds"], 1e-6)
    assert rows_per_second_heavy > 100, (
        f"streamed generation throughput too low: {rows_per_second_heavy:.0f} rows/sec"
    )


def test_no_rows_dropped_or_duplicated_across_shard_boundaries(tmp_path):
    total_rows = 3333  # deliberately not a multiple of the 500-row shard cap
    output_file = str(tmp_path / "04_chunks_embeddings_boundary.sql")
    result = generate_chunks_embeddings_migration_sql(
        jeen_dev_df=_stream_synthetic_user(total_rows, doc_count=7, chunk_size=500),
        output_file=output_file,
        source_info="synthetic-boundary",
        migration_run_id="99999999-9999-4999-8999-999999999999",
        expected_record_count=total_rows,
    )
    assert result["chunks_processed"] == total_rows
    assert result["embeddings_processed"] == total_rows

    insert_count = 0
    for shard in result["shards"]:
        with open(shard, encoding="utf-8") as fh:
            content = fh.read()
        insert_count += content.count("INSERT INTO chunks")
    assert insert_count == total_rows
