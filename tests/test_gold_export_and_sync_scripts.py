"""
Wk17 — real unit tests for scripts/export_marts_to_csv.py and
scripts/sync_gold_to_databricks.py.

Includes a REGRESSION test for the exact real bug found and fixed this
session (2026-09-03): ai_narrative_snippets missing from sync_gold_to_databricks.py's
MARTS list, which meant the Databricks-sourced Power BI cards never saw a
fresh headline no matter how many times the sync script ran. This test fails
immediately if that table is ever dropped from either MARTS list again.
"""
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

import export_marts_to_csv as export_script
import sync_gold_to_databricks as sync_script


EXPECTED_MARTS = {
    "mart_pnl_summary", "mart_revenue_trends", "mart_cost_analysis",
    "mart_variance_analysis", "mart_metric_dictionary", "ai_narrative_snippets",
}


def test_export_marts_list_includes_ai_narrative_snippets():
    """Regression test for the Wk17 bug: this table was missing from the
    export list, so the .pbix's CSV-sourced copy went stale after every new
    AI-1 headline write."""
    assert set(export_script.MARTS) == EXPECTED_MARTS


def test_sync_marts_list_includes_ai_narrative_snippets():
    """Regression test for the matching Wk17 bug in the Databricks sync path
    — same missing-table issue, same fix, same reason it matters (the
    Databricks-sourced Power BI cards silently never refreshed)."""
    assert set(sync_script.MARTS) == EXPECTED_MARTS


def test_export_writes_one_csv_per_mart_with_matching_row_counts(tmp_path, monkeypatch):
    monkeypatch.setattr(export_script, "DB_PATH", PROJECT_ROOT / "data" / "finlineage.duckdb")
    monkeypatch.setattr(export_script, "GOLD", tmp_path)
    export_script.export()

    import duckdb
    conn = duckdb.connect(str(PROJECT_ROOT / "data" / "finlineage.duckdb"), read_only=True)
    for mart in export_script.MARTS:
        csv_path = tmp_path / f"{mart}.csv"
        assert csv_path.exists()
        expected_rows = conn.execute(f"SELECT COUNT(*) FROM main.{mart}").fetchone()[0]
        written_rows = len(pd.read_csv(csv_path))
        assert written_rows == expected_rows, f"{mart}: expected {expected_rows}, wrote {written_rows}"
    conn.close()


def test_get_gold_dataframes_loads_every_mart(writable_db, monkeypatch):
    monkeypatch.setattr(sync_script, "DB_PATH", writable_db)
    frames = sync_script.get_gold_dataframes()
    assert set(frames.keys()) == EXPECTED_MARTS
    for mart, df in frames.items():
        assert isinstance(df, pd.DataFrame)


# --------------------------------------------------------- _databricks_type_for_column
@pytest.mark.parametrize("values,expected_type", [
    ([True, False, None], "BOOLEAN"),
    ([np.bool_(True), np.bool_(False)], "BOOLEAN"),
    ([1, 2, 3], "BIGINT"),
    ([np.int64(1), np.int64(2)], "BIGINT"),
    ([1.5, 2.5], "DOUBLE"),
    ([np.float64(1.5)], "DOUBLE"),
    ([date(2026, 1, 1), date(2026, 1, 2)], "DATE"),
    ([datetime(2026, 1, 1, 12, 0, 0)], "TIMESTAMP"),
    (["a", "b", "c"], "STRING"),
    ([None, None], "STRING"),  # all-null column
])
def test_databricks_type_for_column_infers_correctly(values, expected_type):
    series = pd.Series(values)
    assert sync_script._databricks_type_for_column(series) == expected_type


def test_databricks_type_for_column_uses_first_non_null_value():
    # Nullable-int dtype keeps the non-null values as real ints instead of
    # upcasting to float64 the way a plain list [None, ..., 42] would —
    # this is the case that actually matters for a nullable BIGINT mart column.
    series = pd.Series([None, None, 42, 43], dtype="Int64")
    assert sync_script._databricks_type_for_column(series) == "BIGINT"


# --------------------------------------------------------------- _sql_literal
def test_sql_literal_none_and_nan_become_null():
    assert sync_script._sql_literal(None, "STRING") == "NULL"
    assert sync_script._sql_literal(float("nan"), "DOUBLE") == "NULL"


def test_sql_literal_boolean():
    assert sync_script._sql_literal(True, "BOOLEAN") == "TRUE"
    assert sync_script._sql_literal(False, "BOOLEAN") == "FALSE"


def test_sql_literal_numeric_passthrough():
    assert sync_script._sql_literal(42, "BIGINT") == "42"
    assert sync_script._sql_literal(3.14, "DOUBLE") == "3.14"


def test_sql_literal_date_and_timestamp_are_cast():
    assert sync_script._sql_literal(date(2026, 1, 1), "DATE") == "CAST('2026-01-01' AS DATE)"
    assert "CAST(" in sync_script._sql_literal(datetime(2026, 1, 1, 12), "TIMESTAMP")


def test_sql_literal_string_escapes_single_quotes():
    result = sync_script._sql_literal("O'Brien", "STRING")
    assert result == "'O''Brien'"


# --------------------------------------------------------------- sync_to_databricks guardrails
def test_sync_to_databricks_fails_cleanly_without_credentials(monkeypatch):
    for var in ("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_ACCESS_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    ok = sync_script.sync_to_databricks({"mart_pnl_summary": pd.DataFrame({"a": [1]})})
    assert ok is False
