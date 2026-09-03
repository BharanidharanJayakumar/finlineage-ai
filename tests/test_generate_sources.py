"""
Wk17 — real unit tests for scripts/generate_sources.py: determinism under a
fixed seed (every dbt test / P&L reconciliation / evidence screenshot in this
project depends on this staying true), row-count control, and basic schema
sanity of each generated table.
"""
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import generate_sources as gs


def test_gen_erp_entries_is_deterministic_under_fixed_seed():
    random.seed(42)
    first = gs.gen_erp_entries(n=50)
    random.seed(42)
    second = gs.gen_erp_entries(n=50)
    assert first == second


def test_gen_erp_entries_differs_across_seeds():
    random.seed(1)
    a = gs.gen_erp_entries(n=50)
    random.seed(2)
    b = gs.gen_erp_entries(n=50)
    assert a != b


def test_gen_erp_entries_respects_row_count():
    random.seed(42)
    rows = gs.gen_erp_entries(n=123)
    assert len(rows) == 123


def test_gen_erp_entries_revenue_positive_cogs_opex_negative():
    random.seed(42)
    rows = gs.gen_erp_entries(n=200)
    for r in rows:
        if r["account_type"] == "Revenue":
            assert r["amount"] > 0
        else:  # COGS or OpEx
            assert r["amount"] < 0


def test_gen_erp_entries_uses_only_governed_accounts_and_projects():
    random.seed(42)
    rows = gs.gen_erp_entries(n=200)
    valid_codes = {p[0] for p in gs.PROJECTS}
    valid_accounts = {a[0] for a in gs.ACCOUNTS}
    assert all(r["project_code"] in valid_codes for r in rows)
    assert all(r["account_code"] in valid_accounts for r in rows)


def test_gen_payroll_entries_is_deterministic_under_fixed_seed():
    random.seed(42)
    first = gs.gen_payroll_entries(n=30)
    random.seed(42)
    second = gs.gen_payroll_entries(n=30)
    assert first == second


def test_gen_payroll_entries_cost_equals_hours_times_rate():
    random.seed(42)
    rows = gs.gen_payroll_entries(n=50)
    for r in rows:
        assert round(r["hours_logged"] * r["rate_per_hour"], 2) == r["cost_inr"]


def test_gen_fx_rates_covers_every_date_in_range_for_every_currency():
    rows = gs.gen_fx_rates()
    n_days = (gs.END_DATE - gs.START_DATE).days
    expected = n_days * 4  # 4 foreign currencies (USD/EUR/GBP/AED)
    assert len(rows) == expected


def test_gen_fx_rates_rates_are_always_positive():
    rows = gs.gen_fx_rates()
    assert all(r["rate"] > 0 for r in rows)


def test_write_csv_writes_header_and_all_rows(tmp_path, monkeypatch):
    monkeypatch.setattr(gs, "BRONZE", tmp_path)
    rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    gs.write_csv(rows, "sample.csv")
    content = (tmp_path / "sample.csv").read_text().strip().splitlines()
    assert content[0] == "a,b"
    assert len(content) == 3  # header + 2 rows


def test_daterange_is_exclusive_of_end_date():
    from datetime import date
    days = list(gs.daterange(date(2026, 1, 1), date(2026, 1, 4)))
    assert days == [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)]
