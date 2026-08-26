"""Canonical migration step ordering shared by tracking, rollback, and the
sharded job queue.

This is the single source of truth for step order, target database, and
human-readable labels so that the worker's cross-database dependency checks
stay in sync with the rollback engine and the UI.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

# (step_key, target_database, label) in required execution order.
# step_key never has a trailing underscore; file prefixes (e.g. "01_users_")
# are step_key + "_".
MIGRATION_STEP_ORDER: List[Tuple[str, str, str]] = [
    ("01_users", "user_db", "Users"),
    ("02_folders", "document_db", "Document folders"),
    ("03_documents", "document_db", "Documents"),
    ("04_chunks_embeddings", "document_db", "Chunks & embeddings"),
    ("05_conversations", "completion_db", "Conversations"),
    ("06_agents", "completion_db", "Agents"),
    ("07_conversions", "completion_db", "Agent-conversation links"),
]

STEP_KEYS: List[str] = [key for key, _, _ in MIGRATION_STEP_ORDER]
STEP_TARGET_DB: Dict[str, str] = {key: db for key, db, _ in MIGRATION_STEP_ORDER}
STEP_LABELS: Dict[str, str] = {key: label for key, _, label in MIGRATION_STEP_ORDER}
STEP_INDEX: Dict[str, int] = {key: index for index, key in enumerate(STEP_KEYS)}

# Steps whose shards must execute in strict shard_index order because a later
# shard's rows can depend on an earlier shard's rows having already committed
# (folders are topologically sorted parent-before-child, and a shard boundary
# can fall between a parent and its child). Every other step's shards insert
# fully independent rows and may be claimed and executed by workers in any
# order, including concurrently.
SEQUENTIAL_SHARD_STEPS = {"02_folders"}


def normalize_step_key(step_key: str) -> str:
    """Accept either '01_users' or '01_users_' and return the canonical key."""
    return step_key.rstrip("_")


def prior_step(step_key: str) -> Optional[str]:
    """Return the step_key that must fully complete before ``step_key`` may run."""
    index = STEP_INDEX[normalize_step_key(step_key)]
    if index == 0:
        return None
    return STEP_KEYS[index - 1]


def target_database_for(step_key: str) -> str:
    return STEP_TARGET_DB[normalize_step_key(step_key)]
