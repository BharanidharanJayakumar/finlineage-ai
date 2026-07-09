"""
GE Gate G5 — Cross-Mart Consistency
Validates that revenue in mart_revenue_trends matches revenue in mart_pnl_summary,
and that costs in mart_cost_analysis match COGS+OpEx in mart_pnl_summary.
"""

import sys
from pathlib import Path
import duckdb

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "finlineage.duckdb"


def run_g5():
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    results = []

    # Check 1: revenue totals match across marts
    pnl_rev = conn.execute(
        "SELECT ROUND(SUM(revenue), 2) FROM main.mart_pnl_summary"
    ).fetchone()[0]
    trend_rev = conn.execute(
        "SELECT ROUND(SUM(revenue_inr), 2) FROM main.mart_revenue_trends"
    ).fetchone()[0]
    diff = abs(pnl_rev - trend_rev)
    ok = diff < 0.01
    results.append(("revenue: pnl vs trends", ok, f"pnl={pnl_rev:,.2f} trends={trend_rev:,.2f} diff={diff:.2f}"))

    # Check 2: cost totals match (mart_cost_analysis uses abs(), so compare absolute values)
    pnl_cost = conn.execute(
        "SELECT ROUND(SUM(ABS(cogs) + ABS(opex)), 2) FROM main.mart_pnl_summary"
    ).fetchone()[0]
    cost_mart = conn.execute(
        "SELECT ROUND(SUM(cost_inr), 2) FROM main.mart_cost_analysis"
    ).fetchone()[0]
    diff2 = abs(pnl_cost - cost_mart)
    ok = diff2 < 0.01
    results.append(("costs: pnl vs cost_analysis", ok, f"pnl={pnl_cost:,.2f} cost_mart={cost_mart:,.2f} diff={diff2:.2f}"))

    # Check 3: all segments in variance match segments in pnl
    pnl_segs = set(r[0] for r in conn.execute(
        "SELECT DISTINCT segment FROM main.mart_pnl_summary"
    ).fetchall())
    var_segs = set(r[0] for r in conn.execute(
        "SELECT DISTINCT segment FROM main.mart_variance_analysis"
    ).fetchall())
    ok = var_segs == pnl_segs
    results.append(("segments match across marts", ok, f"pnl={pnl_segs} var={var_segs}"))

    # Check 4: metric dictionary covers all mart models
    expected_models = {"mart_pnl_summary", "mart_revenue_trends",
                       "mart_cost_analysis", "mart_variance_analysis"}
    dict_models = set(r[0] for r in conn.execute(
        "SELECT DISTINCT source_model FROM main.mart_metric_dictionary"
    ).fetchall())
    missing = expected_models - dict_models
    ok = len(missing) == 0
    results.append(("metric dict covers all marts", ok, f"missing={missing}"))

    conn.close()

    total = len(results)
    failures = [r for r in results if not r[1]]
    passed = len(failures) == 0

    print(f"\n{'='*60}")
    print(f"  GE Gate G5 — Cross-Mart Consistency")
    print(f"  Checks: {total}  |  Passed: {total - len(failures)}  |  Failed: {len(failures)}")
    print(f"  Gate: {'PASS ✓' if passed else 'FAIL ✗ — pipeline blocked'}")
    print(f"{'='*60}\n")

    if failures:
        for name, _, detail in failures:
            print(f"  FAILED: {name} ({detail})")
        print()

    return passed


if __name__ == "__main__":
    ok = run_g5()
    sys.exit(0 if ok else 1)