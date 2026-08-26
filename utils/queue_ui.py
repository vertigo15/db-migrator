"""Pure helpers backing the async enqueue/monitor/retry/cancel/resume UI.

Kept free of Streamlit imports so the enqueue and progress-aggregation logic
stays directly unit-testable; ``pages/4_run_migrations.py`` and
``pages/7_migration_history.py`` render these results.
"""
from __future__ import annotations

import json
import os
from typing import Dict, List, Optional, Sequence

from utils.db import ConnectionConfig
from utils.migration_steps import MIGRATION_STEP_ORDER
from utils.sharding import manifest_path_for
from utils.shard_queue import (
    cancel_run_shards,
    enqueue_shards,
    get_failed_shard_details,
    get_shard_progress,
    resume_run_shards,
)

SHARD_STATUSES = (
    "queued",
    "retrying",
    "running",
    "completed",
    "failed",
    "cancelled",
)


def load_manifest_for_file(primary_file_path: str) -> Optional[dict]:
    """Load the shard manifest generated alongside a step's primary SQL file."""
    manifest_file = manifest_path_for(primary_file_path)
    if not os.path.isfile(manifest_file):
        return None
    with open(manifest_file, "r", encoding="utf-8") as fh:
        return json.load(fh)


def enqueue_run(
    base_config: ConnectionConfig,
    run_id: str,
    primary_file_by_step_prefix: Dict[str, str],
    owner_emails: Optional[Sequence[str]] = None,
) -> Dict[str, int]:
    """Enqueue every step's shards for a run.

    ``primary_file_by_step_prefix`` maps a step prefix (e.g. "01_users_", as
    used by ``ALL_STEPS``/``TABLE_MAPPING``) to that step's primary generated
    SQL file path. Steps with no file (e.g. zero-row steps already marked
    'skipped' by extraction) or no manifest are silently skipped. Returns
    step_key -> shard count for every step actually enqueued.
    """
    enqueued: Dict[str, int] = {}
    # Insert downstream work first and the first runnable step last. Workers
    # cannot claim downstream shards until their prior step completes, so this
    # makes the full queue visible before execution can begin. If a terminal
    # failure then cancels the run, no later enqueue loop can add fresh queued
    # shards behind the cancellation.
    for step_key, _, _ in reversed(MIGRATION_STEP_ORDER):
        primary_file = primary_file_by_step_prefix.get(f"{step_key}_")
        if not primary_file:
            continue
        manifest = load_manifest_for_file(primary_file)
        if not manifest or not manifest.get("shards"):
            continue
        count = enqueue_shards(base_config, run_id, step_key, manifest, owner_emails=owner_emails)
        enqueued[step_key] = count
    return enqueued


def enqueue_step(
    base_config: ConnectionConfig,
    run_id: str,
    step_key: str,
    primary_file_path: str,
    owner_emails: Optional[Sequence[str]] = None,
) -> int:
    """Enqueue one selected step while preserving queue dependency checks."""
    manifest = load_manifest_for_file(primary_file_path)
    if not manifest or not manifest.get("shards"):
        return 0
    return enqueue_shards(
        base_config,
        run_id,
        step_key,
        manifest,
        owner_emails=owner_emails,
    )


def run_progress_by_step(base_config: ConnectionConfig, run_id: str) -> Dict[str, Dict[str, int]]:
    """Aggregate ``get_shard_progress`` rows into step_key -> {status: count}."""
    progress: Dict[str, Dict[str, int]] = {
        step_key: {} for step_key, _, _ in MIGRATION_STEP_ORDER
    }
    for row in get_shard_progress(base_config, run_id):
        step_key = row["step_key"]
        progress.setdefault(step_key, {})
        progress[step_key][row["status"]] = (
            progress[step_key].get(row["status"], 0) + row["shard_count"]
        )
    return progress


def steps_with_shards(progress: Dict[str, Dict[str, int]]) -> Dict[str, Dict[str, int]]:
    return {step_key: statuses for step_key, statuses in progress.items() if statuses}


def is_run_fully_enqueued_and_done(progress: Dict[str, Dict[str, int]]) -> bool:
    """True once every step that has any shards shows only 'completed'."""
    active = steps_with_shards(progress)
    if not active:
        return False
    return all(set(statuses.keys()) == {"completed"} for statuses in active.values())


def has_actionable_failures(progress: Dict[str, Dict[str, int]]) -> bool:
    return any(statuses.get("failed", 0) > 0 for statuses in progress.values())


def has_in_flight_shards(progress: Dict[str, Dict[str, int]]) -> bool:
    return any(
        statuses.get(status, 0) > 0
        for statuses in progress.values()
        for status in ("queued", "retrying", "running")
    )


def overall_counts(progress: Dict[str, Dict[str, int]]) -> Dict[str, int]:
    totals = {status: 0 for status in SHARD_STATUSES}
    for statuses in progress.values():
        for status, count in statuses.items():
            totals[status] = totals.get(status, 0) + count
    return totals


def cancel_run(base_config: ConnectionConfig, run_id: str) -> int:
    return cancel_run_shards(base_config, run_id)


def resume_run(base_config: ConnectionConfig, run_id: str, step_key: Optional[str] = None) -> int:
    return resume_run_shards(base_config, run_id, step_key=step_key)


def failed_shard_details(base_config: ConnectionConfig, run_id: str) -> List[dict]:
    return get_failed_shard_details(base_config, run_id)
