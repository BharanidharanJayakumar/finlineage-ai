"""
Wk17 — real unit tests for sync_gold_to_databricks.py's sync_to_databricks()
SUCCESS path (table creation + batched INSERT + the _pipeline_sync_log
freshness marker), with the databricks-sql-connector mocked out — there's no
real Databricks workspace reachable from this sandbox, but the SQL-building
logic (_databricks_type_for_column / _sql_literal are already unit-tested
directly in test_gold_export_and_sync_scripts.py) is exercised end-to-end
here via a fake cursor that records every statement it's asked to execute.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import sync_gold_to_databricks as sync_script


class _FakeCursor:
    def __init__(self):
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@pytest.fixture()
def fake_databricks_module(monkeypatch):
    """Injects a fake `databricks.sql` module into sys.modules so
    `from databricks import sql` inside sync_to_databricks() resolves to it,
    without needing the real databricks-sql-connector package configured
    against a live workspace."""
    cursor = _FakeCursor()
    fake_conn = _FakeConnection(cursor)
    fake_sql_module = MagicMock()
    fake_sql_module.connect = MagicMock(return_value=fake_conn)

    fake_databricks_pkg = MagicMock()
    fake_databricks_pkg.sql = fake_sql_module
    monkeypatch.setitem(sys.modules, "databricks", fake_databricks_pkg)
    monkeypatch.setitem(sys.modules, "databricks.sql", fake_sql_module)
    return cursor, fake_sql_module


def test_sync_to_databricks_creates_a_table_and_inserts_rows_per_mart(fake_databricks_module, monkeypatch):
    cursor, fake_sql_module = fake_databricks_module
    monkeypatch.setenv("DATABRICKS_SERVER_HOSTNAME", "dbc-fake.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/fake")
    monkeypatch.setenv("DATABRICKS_ACCESS_TOKEN", "dapi-fake-token")

    frames = {
        "mart_pnl_summary": pd.DataFrame({
            "period_month": ["2026-01-01"], "revenue": [1000.0], "is_ok": [True],
        }),
        "ai_narrative_snippets": pd.DataFrame({"snippet_id": ["s1"], "word_count": [10]}),
    }

    ok = sync_script.sync_to_databricks(frames)

    assert ok is True
    fake_sql_module.connect.assert_called_once_with(
        server_hostname="dbc-fake.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/fake",
        access_token="dapi-fake-token",
    )
    joined = "\n".join(cursor.executed)
    assert "CREATE SCHEMA IF NOT EXISTS" in joined
    assert "CREATE OR REPLACE TABLE" in joined
    assert "mart_pnl_summary" in joined
    assert "ai_narrative_snippets" in joined
    assert "INSERT INTO" in joined
    assert "_pipeline_sync_log" in joined


def test_sync_to_databricks_handles_empty_dataframe_without_inserting(fake_databricks_module, monkeypatch):
    cursor, _ = fake_databricks_module
    monkeypatch.setenv("DATABRICKS_SERVER_HOSTNAME", "host")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "path")
    monkeypatch.setenv("DATABRICKS_ACCESS_TOKEN", "token")

    frames = {"mart_empty": pd.DataFrame({"a": pd.Series([], dtype="float64")})}
    ok = sync_script.sync_to_databricks(frames)

    assert ok is True
    joined = "\n".join(cursor.executed)
    assert "CREATE OR REPLACE TABLE" in joined
    # No INSERT for the empty mart itself, only the final sync-log insert.
    insert_statements = [s for s in cursor.executed if s.startswith("INSERT INTO") and "mart_empty" in s]
    assert insert_statements == []


def test_sync_to_databricks_uses_custom_catalog_and_schema_env_vars(fake_databricks_module, monkeypatch):
    cursor, _ = fake_databricks_module
    monkeypatch.setenv("DATABRICKS_SERVER_HOSTNAME", "host")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "path")
    monkeypatch.setenv("DATABRICKS_ACCESS_TOKEN", "token")
    monkeypatch.setenv("DATABRICKS_CATALOG", "my_catalog")
    monkeypatch.setenv("DATABRICKS_SCHEMA", "my_schema")

    frames = {"mart_x": pd.DataFrame({"a": [1]})}
    sync_script.sync_to_databricks(frames)

    joined = "\n".join(cursor.executed)
    assert "my_catalog.my_schema" in joined


def test_sync_to_databricks_returns_false_when_connector_import_fails(monkeypatch):
    # Simulate the connector genuinely not being installed.
    monkeypatch.setitem(sys.modules, "databricks", None)
    monkeypatch.setitem(sys.modules, "databricks.sql", None)
    monkeypatch.setenv("DATABRICKS_SERVER_HOSTNAME", "host")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "path")
    monkeypatch.setenv("DATABRICKS_ACCESS_TOKEN", "token")

    ok = sync_script.sync_to_databricks({"mart_x": pd.DataFrame({"a": [1]})})
    assert ok is False
