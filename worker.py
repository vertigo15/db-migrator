#!/usr/bin/env python3
"""Standalone migration shard worker process.

Each instance claims one shard at a time from the durable PostgreSQL queue
(``migration.migration_shards``), executes it against its target database,
and loops. Safe to run several instances concurrently -- shards are claimed
with ``FOR UPDATE SKIP LOCKED`` so two workers can never execute the same
shard, and every generated INSERT is idempotent so a shard re-run after a
crash is harmless.

Scale with Docker Compose:
    docker compose up -d --scale migration-worker=2

Configuration (environment variables, all optional):
    TARGET_DB_HOST / TARGET_DB_PORT / TARGET_DB_DATABASE /
    TARGET_DB_USERNAME / TARGET_DB_PASSWORD   Target V5 connection (required)
    SOURCE_DB_HOST / ... / SOURCE_DB_PASSWORD Source V4 connection, for
                                               mirroring run status (optional)
    WORKER_ID                       Defaults to "<hostname>-<pid>-<random>"
    WORKER_POLL_INTERVAL_SECONDS    Idle poll delay. Default 2
    WORKER_LEASE_SECONDS            Claim lease window. Default 300
    WORKER_RECOVERY_INTERVAL_SECONDS  How often to sweep stale leases. Default 30
    WORKER_LOG_LEVEL                Default INFO
"""
from __future__ import annotations

import logging
import os
import signal
import socket
import sys
import time
import uuid

from utils.config import get_env_connection_defaults, get_env_target_defaults
from utils.db import ConnectionConfig
from utils.migration_tracking import config_for_database
from utils.shard_queue import claim_shard, recover_stale_leases
from utils.worker_runtime import (
    ensure_worker_runtime_schema,
    execute_claimed_shard,
    reconcile_terminal_failures,
)

TARGET_DATABASES = ("user_db", "document_db", "completion_db")

logging.basicConfig(
    level=os.getenv("WORKER_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("migration_worker")


class GracefulShutdown:
    """Lets the current shard finish before the process exits on SIGTERM/SIGINT."""

    def __init__(self):
        self.should_stop = False
        signal.signal(signal.SIGTERM, self._handle)
        signal.signal(signal.SIGINT, self._handle)

    def _handle(self, signum, _frame):
        logger.info("Received signal %s; finishing current shard then exiting", signum)
        self.should_stop = True


def _build_base_config() -> ConnectionConfig:
    env = get_env_target_defaults()
    missing = [k for k in ("host", "database", "username", "password") if not env.get(k)]
    if missing:
        raise RuntimeError(
            f"Missing target DB configuration ({', '.join(missing)}). "
            "Set TARGET_DB_HOST/TARGET_DB_DATABASE/TARGET_DB_USERNAME/TARGET_DB_PASSWORD."
        )
    return ConnectionConfig(
        host=env["host"],
        port=int(env["port"]),
        database=env["database"],
        username=env["username"],
        password=env["password"],
    )


def _build_source_config():
    env = get_env_connection_defaults()
    if env.get("host") and env.get("database") and env.get("username") and env.get("password"):
        return ConnectionConfig(
            host=env["host"],
            port=int(env["port"]),
            database=env["database"],
            username=env["username"],
            password=env["password"],
        )
    return None


def run(shutdown: GracefulShutdown) -> None:
    worker_id = os.getenv("WORKER_ID") or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    poll_interval = float(os.getenv("WORKER_POLL_INTERVAL_SECONDS", "2"))
    lease_seconds = int(os.getenv("WORKER_LEASE_SECONDS", "300"))
    recovery_interval = float(os.getenv("WORKER_RECOVERY_INTERVAL_SECONDS", "30"))

    base_config = _build_base_config()
    source_config = _build_source_config()

    for database in TARGET_DATABASES:
        ensure_worker_runtime_schema(
            config_for_database(base_config, database),
            coordinator=database == "user_db",
        )

    logger.info(
        "Migration worker %s starting (lease=%ss, poll=%ss, recovery_sweep=%ss)",
        worker_id, lease_seconds, poll_interval, recovery_interval,
    )

    last_recovery = 0.0
    while not shutdown.should_stop:
        now = time.monotonic()
        if now - last_recovery >= recovery_interval:
            for database in TARGET_DATABASES:
                try:
                    recovered = recover_stale_leases(base_config, database)
                except Exception:
                    logger.exception("Stale-lease recovery sweep failed for %s", database)
                else:
                    if recovered:
                        logger.warning("Recovered %s stale shard(s) in %s", recovered, database)
                    finalized_failures = reconcile_terminal_failures(
                        base_config, database, source_config
                    )
                    if finalized_failures:
                        logger.error(
                            "Finalized %s terminally failed step(s) in %s",
                            finalized_failures,
                            database,
                        )
            last_recovery = now

        claimed_any = False
        for database in TARGET_DATABASES:
            if shutdown.should_stop:
                break
            try:
                claimed = claim_shard(base_config, database, worker_id, lease_seconds=lease_seconds)
            except Exception:
                logger.exception("Claim attempt failed for %s", database)
                continue
            if claimed is None:
                continue

            claimed_any = True
            logger.info(
                "Claimed shard %s/%s for run=%s step=%s db=%s (%s)",
                claimed.shard_index, claimed.total_shards, claimed.migration_run_id,
                claimed.step_key, database, claimed.file_path,
            )
            try:
                outcome = execute_claimed_shard(
                    base_config, claimed, worker_id, source_config, lease_seconds=lease_seconds
                )
            except Exception:
                logger.exception("Unexpected error executing shard %s", claimed.file_path)
                continue

            if outcome.success:
                finalize_note = ""
                if outcome.step_finalized is True:
                    finalize_note = " -- step COMPLETED and verified"
                elif outcome.step_finalized is False:
                    finalize_note = " -- step FAILED verification"
                logger.info(
                    "Shard %s done (driver_rowcount=%s, diagnostic only)%s",
                    claimed.file_path,
                    outcome.driver_rowcount,
                    finalize_note,
                )
            else:
                logger.error("Shard %s failed: %s", claimed.file_path, outcome.error)

        if not claimed_any and not shutdown.should_stop:
            time.sleep(poll_interval)

    logger.info("Migration worker shut down cleanly")


def main() -> int:
    shutdown = GracefulShutdown()
    try:
        run(shutdown)
    except Exception:
        logger.exception("Migration worker crashed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
