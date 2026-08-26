import importlib.util
from pathlib import Path

import pytest


def _load_select_page():
    path = Path(__file__).parents[1] / "pages" / "2_select_data.py"
    spec = importlib.util.spec_from_file_location("batch_select_page", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


SELECT_PAGE = _load_select_page()


class _Cursor:
    def __init__(self, rows):
        self.rows = rows
        self.query = ""

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query):
        self.query = query

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, rows):
        self.cursor_instance = _Cursor(rows)
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


def _defaults():
    return {
        "host": "target",
        "port": 5432,
        "database": "ignored",
        "username": "user",
        "password": "password",
    }


def test_completed_batch_emails_include_created_and_reused(monkeypatch):
    connection = _Connection([
        ("Created@Example.com",),
        (" reused@example.com ",),
    ])
    captured = {}

    def fake_connection(config):
        captured["database"] = config.database
        return connection

    monkeypatch.setattr(SELECT_PAGE, "get_env_target_defaults", _defaults)
    monkeypatch.setattr("utils.db.get_connection", fake_connection)

    assert SELECT_PAGE._get_already_migrated_emails() == {
        "created@example.com",
        "reused@example.com",
    }
    assert captured["database"] == "user_db"
    assert "reused_existing_user" in connection.cursor_instance.query
    assert connection.closed


def test_completed_batch_tracking_failure_stops_selection(monkeypatch):
    monkeypatch.setattr(SELECT_PAGE, "get_env_target_defaults", _defaults)
    monkeypatch.setattr(
        "utils.db.get_connection",
        lambda _config: (_ for _ in ()).throw(RuntimeError("offline")),
    )

    with pytest.raises(RuntimeError, match="Next Batch was stopped"):
        SELECT_PAGE._get_already_migrated_emails()
