"""
GE Gate G6 — Metric Drift Detection
Phase 2, Wk 12.

Two different things live in one gate on purpose, because they need different
severities:
  1. HARD checks — impossible values (a percentage, by definition, can't be
     outside +/-100%; revenue can't be negative) or a governed metric silently
     disappearing from mart_metric_dictionary. These block the pipeline like
     G4/G5 do, because they mean the pipeline itself is broken.
  2. DRIFT checks — a segment's operating margin moving further from its own
     trailing history than a z-score threshold. This does NOT block the
     pipeline: a real business swing looks identical to a bug from inside a
     single gate check, so it's logged as a WARNING for a human to look at,
     not a hard failure.

Run standalone: python ge/g6_metric_drift.py
"""

import sys
import statistics
from pathlib import Path
import duckdb

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "finlineage.duckdb"

# A segment's latest operating_margin_pct beyond this many std devs from its
# own trailing average triggers a drift warning (not a failure).
DRIFT_ZSCORE_THRESHOLD = 3.0
# Need at least this many prior periods before a z-score means anything.
MIN_PRIOR_PERIODS_FOR_BASELINE = 3

REQUIRED_METRIC_KEYS = {
    "revenue", "cogs", "gross_margin", "gross_margin_pct", "opex",
    "operating_income", "operating_margin_pct", "revenue_mom_change",
    "cost_mom_change", "oi_variance_pct", "is_material_variance",
}


def run_g6():
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    hard_results = []
    warnings = []

    # -- HARD 1: percentage metrics must be within [-100, 100] --
    bad_pct = conn.execute("""
        SELECT period_month, segment, gross_margin_pct, operating_margin_pct
        FROM main.mart_pnl_summary
        WHERE gross_margin_pct NOT BETWEEN -100 AND 100
           OR operating_margin_pct NOT BETWEEN -100 AND 100
    """).fetchdf()
    ok = bad_pct.empty
    hard_results.append((
        "percentage metrics within [-100, 100]", ok,
        "all in range" if ok else f"{len(bad_pct)} rows out of range"
    ))

    # -- HARD 2: revenue must never be negative --
    bad_rev = conn.execute("""
        SELECT period_month, segment, revenue
        FROM main.mart_pnl_summary
        WHERE revenue < 0
    """).fetchdf()
    ok = bad_rev.empty
    hard_results.append((
        "revenue never negative", ok,
        "all non-negative" if ok else f"{len(bad_rev)} rows with negative revenue"
    ))

    # -- HARD 3: no governed metric has silently disappeared --
    present = set(r[0] for r in conn.execute(
        "SELECT DISTINCT metric_key FROM main.mart_metric_dictionary"
    ).fetchall())
    missing = REQUIRED_METRIC_KEYS - present
    ok = len(missing) == 0
    hard_results.append((
        "all governed metrics still defined", ok,
        "all present" if ok else f"missing: {sorted(missing)}"
    ))

    # -- DRIFT (warning only): per-segment operating_margin_pct z-score --
    df = conn.execute("""
        SELECT period_month, segment, operating_margin_pct
        FROM main.mart_pnl_summary
        ORDER BY segment, period_month
    """).fetchdf()
    conn.close()

    for segment, group in df.groupby("segment"):
        values = group["operating_margin_pct"].tolist()
        if len(values) < MIN_PRIOR_PERIODS_FOR_BASELINE + 1:
            continue  # not enough history to call anything "drift" yet
        *prior, latest = values
        mean = statistics.mean(prior)
        stdev = statistics.pstdev(prior)
        if stdev == 0:
            continue  # no variance in history — a z-score is meaningless here
        z = (latest - mean) / stdev
        if abs(z) > DRIFT_ZSCORE_THRESHOLD:
            latest_period = group["period_month"].iloc[-1]
            warnings.append(
                f"{segment} operating_margin_pct at {latest_period}: "
                f"{latest:.2f}% is {z:+.1f} sigma from its own {len(prior)}-period "
                f"trailing average ({mean:.2f}%)"
            )

    total = len(hard_results)
    failures = [r for r in hard_results if not r[1]]
    passed = len(failures) == 0

    print(f"\n{'='*60}")
    print(f"  GE Gate G6 — Metric Drift Detection")
    print(f"  Hard checks: {total}  |  Passed: {total - len(failures)}  |  Failed: {len(failures)}")
    print(f"  Drift warnings: {len(warnings)}")
    print(f"  Gate: {'PASS ✓' if passed else 'FAIL ✗ — pipeline blocked'}")
    print(f"{'='*60}\n")

    if failures:
        for name, _, detail in failures:
            print(f"  FAILED: {name} ({detail})")
        print()

    if warnings:
        print("  Drift warnings (non-blocking — worth a human look):")
        for w in warnings:
            print(f"    - {w}")
        print()

    return passed


if __name__ == "__main__":
    ok = run_g6()
    sys.exit(0 if ok else 1)
