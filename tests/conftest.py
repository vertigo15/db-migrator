"""Shared pytest fixtures for real-PostgreSQL-backed integration tests."""
import os
import shutil
import socket
import subprocess

import pytest

from utils.db import ConnectionConfig


@pytest.fixture(scope="session")
def postgres_cluster(tmp_path_factory):
    """Spin up an ephemeral local PostgreSQL with the three migration target
    databases pre-created. Shared (session-scoped) across all test modules
    that request it, so tests must use unique migration_run_id values to
    avoid clashing with each other's rows."""
    required = ["initdb", "pg_ctl", "psql"]
    if any(shutil.which(binary) is None for binary in required):
        pytest.skip("Local PostgreSQL binaries are required for integration proof")

    root = tmp_path_factory.mktemp("migration-postgres")
    data = root / "data"
    log = root / "postgres.log"
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    postgres_env = {**os.environ, "LC_ALL": "C", "LANG": "C"}

    subprocess.run(
        ["initdb", "-D", str(data), "-A", "trust", "-U", "postgres", "--no-locale"],
        check=True,
        capture_output=True,
        text=True,
        env=postgres_env,
    )
    subprocess.run(
        [
            "pg_ctl",
            "-D",
            str(data),
            "-l",
            str(log),
            "-o",
            f"-F -p {port} -h 127.0.0.1",
            "start",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=postgres_env,
    )
    try:
        for database in ("user_db", "document_db", "completion_db"):
            subprocess.run(
                [
                    "psql",
                    "-h",
                    "127.0.0.1",
                    "-p",
                    str(port),
                    "-U",
                    "postgres",
                    "-d",
                    "postgres",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-c",
                    f"CREATE DATABASE {database}",
                ],
                check=True,
                capture_output=True,
                text=True,
                env=postgres_env,
            )
        yield ConnectionConfig("127.0.0.1", port, "user_db", "postgres", "")
    finally:
        subprocess.run(
            ["pg_ctl", "-D", str(data), "stop", "-m", "fast"],
            check=False,
            capture_output=True,
            text=True,
            env=postgres_env,
        )
