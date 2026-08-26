"""Unit tests for ``utils.sharding``: bounded shard rollover, checksums,
epilogue placement, and manifest round-tripping. Pure Python, no database
required -- these prove the shard *writer* is correct in isolation, leaving
the queue/worker tests to prove the *runtime* behaves correctly."""
import json
import os

import pytest

from utils.sharding import (
    ShardWriter,
    ShardWriterFileAdapter,
    checksum_file,
    load_manifest,
    manifest_path_for,
)


PREAMBLE = "-- preamble\n"


def _writer(tmp_path, max_rows=1000, max_bytes=8 * 1024 * 1024, output_name="01_users_test.sql"):
    return ShardWriter(
        output_file=str(tmp_path / output_name),
        step_key="01_users",
        target_database="user_db",
        migration_run_id="11111111-1111-4111-8111-111111111111",
        preamble=PREAMBLE,
        max_rows_per_shard=max_rows,
        max_bytes_per_shard=max_bytes,
    )


def test_single_shard_matches_legacy_single_file_layout(tmp_path):
    """When everything fits in one shard, the first shard reuses the
    original output filename (no `.shardNNNN.sql` suffix) so old single-file
    consumers and file-naming conventions keep working unchanged."""
    writer = _writer(tmp_path)
    writer.write_unit("INSERT INTO users VALUES (1);")
    writer.write_unit("INSERT INTO users VALUES (2);")
    manifest = writer.finalize(epilogue="-- epilogue\n")

    assert len(manifest.shards) == 1
    shard = manifest.shards[0]
    assert shard.file_path == str(tmp_path / "01_users_test.sql")
    assert shard.expected_rows == 2
    content = open(shard.file_path, encoding="utf-8").read()
    assert content.startswith(PREAMBLE)
    assert content.endswith("-- epilogue\n")
    assert manifest.total_rows == 2


def test_row_count_boundary_triggers_rollover(tmp_path):
    writer = _writer(tmp_path, max_rows=2)
    for i in range(5):
        writer.write_unit(f"INSERT INTO users VALUES ({i});")
    manifest = writer.finalize()

    # 5 rows capped at 2/shard -> 3 shards (2, 2, 1).
    assert [s.expected_rows for s in manifest.shards] == [2, 2, 1]
    assert manifest.total_rows == 5
    names = [os.path.basename(s.file_path) for s in manifest.shards]
    assert names == [
        "01_users_test.sql",
        "01_users_test.shard0002.sql",
        "01_users_test.shard0003.sql",
    ]


def test_byte_size_boundary_triggers_rollover_even_under_row_cap(tmp_path):
    big_unit = "INSERT INTO users VALUES ('" + ("x" * 500) + "');"
    writer = _writer(tmp_path, max_rows=1000, max_bytes=1200)
    for _ in range(4):
        writer.write_unit(big_unit)
    manifest = writer.finalize()

    # ~500 bytes/unit with an ~1200 byte cap allows 2 units/shard.
    assert [s.expected_rows for s in manifest.shards] == [2, 2]


def test_weighted_unit_counts_toward_row_cap(tmp_path):
    """A single ``write_unit`` call can represent multiple logical rows via
    ``weight`` (used for batched multi-row INSERTs)."""
    writer = _writer(tmp_path, max_rows=5)
    writer.write_unit("INSERT INTO chunks VALUES (1), (2), (3);", weight=3)
    writer.write_unit("INSERT INTO chunks VALUES (4), (5), (6);", weight=3)
    manifest = writer.finalize()

    assert [s.expected_rows for s in manifest.shards] == [3, 3]
    assert manifest.total_rows == 6


def test_manifest_records_exact_owners_per_shard(tmp_path):
    writer = _writer(tmp_path, max_rows=2)
    writer.write_unit(
        "INSERT INTO users VALUES (1);",
        owner_legacy_ids=["legacy-a"],
    )
    writer.write_unit(
        "INSERT INTO users VALUES (2);",
        owner_legacy_ids=["legacy-b"],
    )
    writer.write_unit(
        "INSERT INTO users VALUES (3);",
        owner_legacy_ids=["legacy-c"],
    )
    manifest = writer.finalize()

    assert [shard.owner_legacy_ids for shard in manifest.shards] == [
        ["legacy-a", "legacy-b"],
        ["legacy-c"],
    ]


def test_epilogue_merged_into_last_shard_only(tmp_path):
    writer = _writer(tmp_path, max_rows=1)
    for i in range(3):
        writer.write_unit(f"INSERT INTO users VALUES ({i});")
    manifest = writer.finalize(epilogue="-- epilogue only here\n")

    assert len(manifest.shards) == 3
    for shard in manifest.shards[:-1]:
        assert "epilogue" not in open(shard.file_path, encoding="utf-8").read()
    last_content = open(manifest.shards[-1].file_path, encoding="utf-8").read()
    assert last_content.endswith("-- epilogue only here\n")


def test_epilogue_appended_to_already_flushed_last_shard_when_no_pending_rows(tmp_path):
    """If the buffer happens to be empty right when finalize() is called
    (exact multiple of max_rows), the epilogue must still land on the last
    *written* shard rather than creating a new empty one."""
    writer = _writer(tmp_path, max_rows=2)
    writer.write_unit("INSERT INTO users VALUES (1);")
    writer.write_unit("INSERT INTO users VALUES (2);")  # exactly fills shard 1, no rollover yet
    manifest = writer.finalize(epilogue="-- epilogue\n")

    assert len(manifest.shards) == 1
    content = open(manifest.shards[0].file_path, encoding="utf-8").read()
    assert content.endswith("-- epilogue\n")


def test_checksum_matches_actual_file_contents(tmp_path):
    writer = _writer(tmp_path)
    writer.write_unit("INSERT INTO users VALUES (1);")
    manifest = writer.finalize(epilogue="-- done\n")

    for shard in manifest.shards:
        assert shard.checksum == checksum_file(shard.file_path)
        assert shard.byte_size == os.path.getsize(shard.file_path)


def test_checksum_changes_if_file_is_tampered_with(tmp_path):
    writer = _writer(tmp_path)
    writer.write_unit("INSERT INTO users VALUES (1);")
    manifest = writer.finalize()
    shard = manifest.shards[0]

    original_checksum = shard.checksum
    with open(shard.file_path, "a", encoding="utf-8") as fh:
        fh.write("-- tampered\n")

    assert checksum_file(shard.file_path) != original_checksum


def test_manifest_round_trips_through_json(tmp_path):
    writer = _writer(tmp_path, max_rows=2)
    for i in range(3):
        writer.write_unit(f"INSERT INTO users VALUES ({i});")
    manifest = writer.finalize(epilogue="-- epilogue\n")

    manifest_path = manifest_path_for(str(tmp_path / "01_users_test.sql"))
    manifest.save(manifest_path)
    assert os.path.exists(manifest_path)

    with open(manifest_path, encoding="utf-8") as fh:
        raw = json.load(fh)
    assert raw["step_key"] == "01_users"
    assert raw["target_database"] == "user_db"
    assert raw["total_shards"] == 2

    reloaded = load_manifest(manifest_path)
    assert reloaded.step_key == manifest.step_key
    assert reloaded.total_rows == manifest.total_rows
    assert [s.checksum for s in reloaded.shards] == [s.checksum for s in manifest.shards]
    assert reloaded.primary_file == manifest.shards[0].file_path


def test_empty_writer_still_produces_one_shard_with_only_preamble_and_epilogue(tmp_path):
    """Callers that always call finalize() even for a step with zero rows
    (e.g. an empty epilogue-only bookkeeping step) still get a valid, single
    shard rather than no output at all."""
    writer = _writer(tmp_path)
    manifest = writer.finalize(epilogue="-- bookkeeping\n")

    assert len(manifest.shards) == 1
    assert manifest.shards[0].expected_rows == 0
    assert manifest.total_rows == 0


def test_write_unit_after_finalize_raises(tmp_path):
    writer = _writer(tmp_path)
    writer.write_unit("INSERT INTO users VALUES (1);")
    writer.finalize()

    with pytest.raises(RuntimeError):
        writer.write_unit("INSERT INTO users VALUES (2);")


def test_finalize_twice_raises(tmp_path):
    writer = _writer(tmp_path)
    writer.write_unit("INSERT INTO users VALUES (1);")
    writer.finalize()

    with pytest.raises(RuntimeError):
        writer.finalize()


def test_shards_are_written_atomically_via_rename(tmp_path, monkeypatch):
    """Shard files must never be visible half-written: content is written to
    a `.tmp` sibling and atomically renamed into place."""
    writer = _writer(tmp_path, max_rows=1)
    seen_tmp_files = []

    real_replace = os.replace

    def spy_replace(src, dst):
        seen_tmp_files.append(src)
        assert src.endswith(".tmp")
        assert not dst.endswith(".tmp")
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", spy_replace)
    writer.write_unit("INSERT INTO users VALUES (1);")
    writer.write_unit("INSERT INTO users VALUES (2);")
    writer.finalize()

    assert len(seen_tmp_files) == 2
    for tmp_file in seen_tmp_files:
        assert not os.path.exists(tmp_file)


class _FakeShardWriter:
    """Records every ``write_unit`` call made through the adapter."""

    def __init__(self):
        self.calls = []

    def write_unit(self, sql_text, weight=1, owner_legacy_ids=None):
        self.calls.append((sql_text, weight, list(owner_legacy_ids or [])))


def test_file_adapter_groups_writes_within_a_unit_into_one_call():
    fake = _FakeShardWriter()
    adapter = ShardWriterFileAdapter(fake)

    adapter.begin_unit(weight=1)
    adapter.write("INSERT INTO conversations VALUES (1);\n")
    adapter.write("INSERT INTO messages VALUES (1, 1);\n")
    adapter.end_unit()

    assert len(fake.calls) == 1
    text, weight, owner_legacy_ids = fake.calls[0]
    assert weight == 1
    assert owner_legacy_ids == []
    assert "conversations" in text and "messages" in text


def test_file_adapter_writes_outside_a_unit_flush_immediately_with_zero_weight():
    fake = _FakeShardWriter()
    adapter = ShardWriterFileAdapter(fake)

    adapter.write("-- preamble comment\n")

    assert len(fake.calls) == 1
    assert fake.calls[0] == ("-- preamble comment\n", 0, [])


def test_file_adapter_rejects_mismatched_begin_end_unit_calls():
    adapter = ShardWriterFileAdapter(_FakeShardWriter())
    with pytest.raises(RuntimeError):
        adapter.end_unit()

    adapter.begin_unit()
    with pytest.raises(RuntimeError):
        adapter.begin_unit()
