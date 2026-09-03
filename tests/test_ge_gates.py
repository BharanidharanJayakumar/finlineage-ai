"""
Wk17 — real unit tests for the 6 GE quality gates (ge/g1..g6), run directly
against the project's actual bronze CSVs / finlineage.duckdb — no Docker,
no Airflow, no LLM calls needed, since none of these gates touch an AI chain.

Each gate is exercised on BOTH its pass path (real, known-good data) and at
least one fail path (a deliberately corrupted copy of the input), so the
"Gate: FAIL" branch of each module — never previously executed by any
automated test — is covered too, not just the happy path.
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "ge"))

import g1_raw_gl_completeness as g1
import g2_fx_rate_coverage as g2
import g3_payroll_completeness as g3
import g4_pnl_aggregation_consistency as g4
import g5_cross_mart_consistency as g5
import g6_metric_drift as g6

REAL_BRONZE = PROJECT_ROOT / "data" / "bronze"


# --------------------------------------------------------------------------- G1
def test_g1_passes_on_real_bronze_data():
    assert g1.run_g1(bronze_dir=REAL_BRONZE) is True


def test_g1_fails_on_duplicate_entry_id(tmp_path):
    case_dir = tmp_path / "bronze"
    case_dir.mkdir()
    for f in REAL_BRONZE.iterdir():
        (case_dir / f.name).write_bytes(f.read_bytes())
    df = pd.read_csv(case_dir / "erp_gl_entries.csv")
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)  # duplicate entry_id
    df.to_csv(case_dir / "erp_gl_entries.csv", index=False)
    assert g1.run_g1(bronze_dir=case_dir) is False


def test_g1_fails_on_invalid_account_type(tmp_path):
    case_dir = tmp_path / "bronze"
    case_dir.mkdir()
    for f in REAL_BRONZE.iterdir():
        (case_dir / f.name).write_bytes(f.read_bytes())
    df = pd.read_csv(case_dir / "erp_gl_entries.csv")
    df.loc[0, "account_type"] = "Miscellaneous"
    df.to_csv(case_dir / "erp_gl_entries.csv", index=False)
    assert g1.run_g1(bronze_dir=case_dir) is False


# --------------------------------------------------------------------------- G2
def test_g2_passes_on_real_bronze_data():
    assert g2.run_g2(bronze_dir=REAL_BRONZE) is True


def test_g2_fails_on_zero_fx_rate(tmp_path):
    case_dir = tmp_path / "bronze"
    case_dir.mkdir()
    for f in REAL_BRONZE.iterdir():
        (case_dir / f.name).write_bytes(f.read_bytes())
    df = pd.read_csv(case_dir / "fx_rates.csv")
    df.loc[0, "rate"] = 0
    df.to_csv(case_dir / "fx_rates.csv", index=False)
    assert g2.run_g2(bronze_dir=case_dir) is False


def test_g2_fails_on_missing_fx_rate_for_used_date(tmp_path):
    case_dir = tmp_path / "bronze"
    case_dir.mkdir()
    for f in REAL_BRONZE.iterdir():
        (case_dir / f.name).write_bytes(f.read_bytes())
    fx = pd.read_csv(case_dir / "fx_rates.csv")
    gl = pd.read_csv(case_dir / "erp_gl_entries.csv")
    foreign = gl[gl["currency"] != "INR"].iloc[0]
    fx = fx[~((fx["rate_date"] == foreign["entry_date"]) & (fx["from_currency"] == foreign["currency"]))]
    fx.to_csv(case_dir / "fx_rates.csv", index=False)
    assert g2.run_g2(bronze_dir=case_dir) is False


# --------------------------------------------------------------------------- G3
def test_g3_passes_on_real_bronze_data():
    assert g3.run_g3(bronze_dir=REAL_BRONZE) is True


def test_g3_fails_on_negative_hours(tmp_path):
    case_dir = tmp_path / "bronze"
    case_dir.mkdir()
    for f in REAL_BRONZE.iterdir():
        (case_dir / f.name).write_bytes(f.read_bytes())
    df = pd.read_csv(case_dir / "payroll_entries.csv")
    df.loc[0, "hours_logged"] = -10.0
    df.to_csv(case_dir / "payroll_entries.csv", index=False)
    assert g3.run_g3(bronze_dir=case_dir) is False


def test_g3_fails_on_duplicate_allocation_id(tmp_path):
    case_dir = tmp_path / "bronze"
    case_dir.mkdir()
    for f in REAL_BRONZE.iterdir():
        (case_dir / f.name).write_bytes(f.read_bytes())
    df = pd.read_csv(case_dir / "payroll_entries.csv")
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    df.to_csv(case_dir / "payroll_entries.csv", index=False)
    assert g3.run_g3(bronze_dir=case_dir) is False


# --------------------------------------------------------------------------- G4
def test_g4_passes_on_real_marts(full_view_db):
    g4.DB_PATH = full_view_db
    assert g4.run_g4() is True


def test_g4_fails_when_mart_pnl_summary_is_corrupted(full_view_db):
    import duckdb
    conn = duckdb.connect(str(full_view_db), read_only=False)
    conn.execute("UPDATE mart_pnl_summary SET operating_income = operating_income + 1 WHERE period_month = (SELECT MIN(period_month) FROM mart_pnl_summary)")
    conn.close()
    g4.DB_PATH = full_view_db
    assert g4.run_g4() is False


# --------------------------------------------------------------------------- G5
def test_g5_passes_on_real_marts(writable_db):
    g5.DB_PATH = writable_db
    assert g5.run_g5() is True


def test_g5_fails_when_revenue_totals_diverge(writable_db):
    import duckdb
    conn = duckdb.connect(str(writable_db), read_only=False)
    conn.execute("UPDATE mart_revenue_trends SET revenue_inr = revenue_inr + 1000000 WHERE rowid = 0")
    conn.close()
    g5.DB_PATH = writable_db
    assert g5.run_g5() is False


# --------------------------------------------------------------------------- G6
def test_g6_passes_on_real_marts(writable_db):
    g6.DB_PATH = writable_db
    assert g6.run_g6() is True


def test_g6_fails_on_out_of_range_percentage(writable_db):
    import duckdb
    conn = duckdb.connect(str(writable_db), read_only=False)
    conn.execute("UPDATE mart_pnl_summary SET operating_margin_pct = 250 WHERE rowid = 0")
    conn.close()
    g6.DB_PATH = writable_db
    assert g6.run_g6() is False


def test_g6_fails_on_negative_revenue(writable_db):
    import duckdb
    conn = duckdb.connect(str(writable_db), read_only=False)
    conn.execute("UPDATE mart_pnl_summary SET revenue = -100 WHERE rowid = 0")
    conn.close()
    g6.DB_PATH = writable_db
    assert g6.run_g6() is False


def test_g6_fails_when_a_governed_metric_disappears(writable_db):
    import duckdb
    conn = duckdb.connect(str(writable_db), read_only=False)
    conn.execute("DELETE FROM mart_metric_dictionary WHERE metric_key = 'revenue'")
    conn.close()
    g6.DB_PATH = writable_db
    assert g6.run_g6() is False


# --------------------------------------------------------------------------- run_all_gates.py
def test_run_all_gates_module_maps_every_gate_to_its_run_function():
    import run_all_gates
    assert len(run_all_gates.GATES) == 6
    for module_name, label in run_all_gates.GATES:
        mod = __import__(module_name)
        fn_name = f"run_{module_name.split('_', 1)[0]}"
        assert hasattr(mod, fn_name), f"{module_name} has no {fn_name}()"
