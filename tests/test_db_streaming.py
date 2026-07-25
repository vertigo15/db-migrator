import pandas as pd
import psycopg2
import pytest

from utils import db
from utils.db import ConnectionConfig, execute_query_chunked
from utils.extraction import _get_extraction_stream_chunk_size


CONFIG = ConnectionConfig("host", 5432, "database", "user", "password")


class FakeServerCursor:
    def __init__(self, connection, rows):
        self.connection = connection
        self.rows = list(rows)
        self.description = None
        self.itersize = None
        self.executed = None
        self.fetch_sizes = []
        self.closed = False

    def execute(self, query, params):
        self.executed = (query, params)
        self.connection.transaction_status = (
            psycopg2.extensions.TRANSACTION_STATUS_INTRANS
        )

    def fetchmany(self, size):
        self.fetch_sizes.append(size)
        chunk, self.rows = self.rows[:size], self.rows[size:]
        self.description = [("record_id",), ("payload",)]
        return chunk

    def close(self):
        self.closed = True
        self.connection.events.append("cursor.close")


class FakeConnection:
    def __init__(self, rows):
        self.closed = 0
        self.transaction_status = psycopg2.extensions.TRANSACTION_STATUS_IDLE
        self.events = []
        self.autocommit_values = []
        self.cursor_names = []
        self.rollback_count = 0
        self.server_cursor = FakeServerCursor(self, rows)

    @property
    def autocommit(self):
        return self.autocommit_values[-1] if self.autocommit_values else True

    @autocommit.setter
    def autocommit(self, value):
        self.autocommit_values.append(value)
        self.events.append(f"autocommit={value}")

    def get_transaction_status(self):
        return self.transaction_status

    def rollback(self):
        self.rollback_count += 1
        self.transaction_status = psycopg2.extensions.TRANSACTION_STATUS_IDLE
        self.events.append("rollback")

    def cursor(self, *, name=None):
        self.cursor_names.append(name)
        return self.server_cursor


class FakePool:
    def __init__(self, connection):
        self.connection = connection
        self.returned = []

    def getconn(self):
        return self.connection

    def putconn(self, connection, close=False):
        self.returned.append((connection, close))
        connection.events.append("pool.putconn")


def _install_fake_pool(monkeypatch, rows):
    connection = FakeConnection(rows)
    pool = FakePool(connection)
    monkeypatch.setattr(db, "get_read_pool", lambda _config: pool)
    return connection, pool


def test_execute_query_chunked_uses_named_cursor_and_preserves_rows(monkeypatch):
    connection, pool = _install_fake_pool(
        monkeypatch,
        [(1, "first"), (2, "second"), (3, "third"), (4, "fourth"), (5, "fifth")],
    )

    chunks = list(
        execute_query_chunked(
            CONFIG,
            "SELECT record_id, payload FROM records ORDER BY record_id",
            ("parameter",),
            chunk_size=2,
        )
    )

    assert len(chunks) == 3
    assert all(list(chunk.columns) == ["record_id", "payload"] for chunk in chunks)
    combined = pd.concat(chunks, ignore_index=True)
    assert combined["record_id"].tolist() == [1, 2, 3, 4, 5]
    assert combined["payload"].tolist() == [
        "first",
        "second",
        "third",
        "fourth",
        "fifth",
    ]
    assert connection.cursor_names[0].startswith("db_migrator_stream_")
    assert connection.server_cursor.itersize == 2
    assert connection.server_cursor.executed == (
        "SELECT record_id, payload FROM records ORDER BY record_id",
        ("parameter",),
    )
    assert connection.server_cursor.fetch_sizes == [2, 2, 2, 2]
    assert connection.autocommit_values == [False, True]
    assert connection.rollback_count == 1
    assert pool.returned == [(connection, False)]


def test_closing_chunk_generator_cleans_cursor_and_transaction(monkeypatch):
    connection, pool = _install_fake_pool(
        monkeypatch,
        [(1, "first"), (2, "second"), (3, "third")],
    )
    chunks = execute_query_chunked(CONFIG, "SELECT record_id, payload", chunk_size=2)

    assert next(chunks)["record_id"].tolist() == [1, 2]
    chunks.close()

    assert connection.server_cursor.closed is True
    assert connection.rollback_count == 1
    assert connection.autocommit is True
    assert pool.returned == [(connection, False)]
    assert connection.events.index("cursor.close") < connection.events.index("rollback")
    assert connection.events.index("rollback") < connection.events.index("pool.putconn")


@pytest.mark.parametrize("chunk_size", [0, -1, True, 1.5, "2", None])
def test_execute_query_chunked_rejects_invalid_chunk_sizes(monkeypatch, chunk_size):
    monkeypatch.setattr(
        db,
        "get_read_pool",
        lambda _config: pytest.fail("invalid size must fail before borrowing a connection"),
    )

    with pytest.raises(ValueError, match="chunk_size must be a positive integer"):
        next(execute_query_chunked(CONFIG, "SELECT 1", chunk_size=chunk_size))


@pytest.mark.parametrize("value", ["0", "-10", "not-an-integer"])
def test_extraction_stream_chunk_size_must_be_positive_integer(monkeypatch, value):
    monkeypatch.setenv("EXTRACTION_STREAM_CHUNK_SIZE", value)

    with pytest.raises(
        ValueError,
        match="EXTRACTION_STREAM_CHUNK_SIZE must be a positive integer",
    ):
        _get_extraction_stream_chunk_size()


def test_extraction_stream_chunk_size_accepts_positive_integer(monkeypatch):
    monkeypatch.setenv("EXTRACTION_STREAM_CHUNK_SIZE", "37")

    assert _get_extraction_stream_chunk_size() == 37


def test_execute_query_chunked_streams_real_postgres_rows(postgres_cluster):
    chunks = list(
        execute_query_chunked(
            postgres_cluster,
            "SELECT value FROM generate_series(1, 23) AS value ORDER BY value",
            chunk_size=5,
        )
    )

    assert [len(chunk) for chunk in chunks] == [5, 5, 5, 5, 3]
    assert pd.concat(chunks, ignore_index=True)["value"].tolist() == list(
        range(1, 24)
    )
