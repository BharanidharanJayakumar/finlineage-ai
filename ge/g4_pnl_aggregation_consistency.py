"""
GE Gate G4 — P&L Aggregation Consistency
Validates that mart_pnl_summary totals reconcile with the intermediate layer.
Runs AFTER dbt build — catches aggregation logic bugs.
"""

import sys
from pathlib import Path
import duckdb

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "finlineage.duckdb"


def run_g4():
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    results = []

    # Check 1: total amount reconciliation
    source_total = conn.execute(
        "SELECT ROUND(SUM(amount_inr), 2) FROM main.int_entries_cost_centre_mapped"
    ).fetchone()[0]
    mart_total = conn.execute(
        "SELECT ROUND(SUM(revenue + cogs + opex), 2) FROM main.mart_pnl_summary"
    ).fetchone()[0]
    diff = abs(source_total - mart_total)
    ok = diff < 0.01
    results.append(("total INR reconciliation", ok, f"source={source_total:,.2f} mart={mart_total:,.2f} diff={diff:.2f}"))

    # Check 2: entry count reconciliation
    source_count = conn.execute(
        "SELECT COUNT(*) FROM main.int_entries_cost_centre_mapped"
    ).fetchone()[0]
    mart_count = conn.execute(
        "SELECT SUM(entry_count) FROM main.mart_pnl_summary"
    ).fetchone()[0]
    ok = source_count == mart_count
    results.append(("entry count reconciliation", ok, f"source={source_count} mart={mart_count}"))

    # Check 3: no null operating_income
    nulls = conn.execute(
        "SELECT COUNT(*) FROM main.mart_pnl_summary WHERE operating_income IS NULL"
    ).fetchone()[0]
    ok = nulls == 0
    results.append(("no null operating_income", ok, f"nulls={nulls}"))

    # Check 4: gross_margin = revenue + cogs (mathematical identity)
    mismatches = conn.execute("""
        SELECT COUNT(*) FROM main.mart_pnl_summary
        WHERE ABS(gross_margin - (revenue + cogs)) > 0.01
    """).fetchone()[0]
    ok = mismatches == 0
    results.append(("gross_margin = revenue + cogs", ok, f"mismatches={mismatches}"))

    # Check 5: operating_income = revenue + cogs + opex
    mismatches2 = conn.execute("""
        SELECT COUNT(*) FROM main.mart_pnl_summary
        WHERE ABS(operating_income - (revenue + cogs + opex)) > 0.01
    """).fetchone()[0]
    ok = mismatches2 == 0
    results.append(("operating_income = revenue + cogs + opex", ok, f"mismatches={mismatches2}"))

    conn.close()

    # -- Report --
    total = len(results)
    failures = [r for r in results if not r[1]]
    passed = len(failures) == 0

    print(f"\n{'='*60}")
    print(f"  GE Gate G4 — P&L Aggregation Consistency")
    print(f"  Checks: {total}  |  Passed: {total - len(failures)}  |  Failed: {len(failures)}")
    print(f"  Gate: {'PASS ✓' if passed else 'FAIL ✗ — pipeline blocked'}")
    print(f"{'='*60}\n")

    if failures:
        for name, _, detail in failures:
            print(f"  FAILED: {name} ({detail})")
        print()

    return passed


if __name__ == "__main__":
    ok = run_g4()
    sys.exit(0 if ok else 1)