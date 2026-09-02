"""
GE Gate G3 — Payroll Completeness
Validates payroll_entries.csv structure and data quality.
"""

import sys
from pathlib import Path
import pandas as pd

BRONZE = Path(__file__).resolve().parent.parent / "data" / "bronze"


def run_g3(bronze_dir: Path = None):
    """bronze_dir override added for ai5_anomaly_generator.py — see the same
    note in g1_raw_gl_completeness.py. Defaults to the real BRONZE dir."""
    bronze_dir = bronze_dir or BRONZE
    df = pd.read_csv(bronze_dir / "payroll_entries.csv")

    results = []

    # Row count
    ok = 10 <= len(df) <= 100000
    results.append(("row count in range", ok, f"rows={len(df)}"))

    # Required columns
    required = ["allocation_id", "allocation_date", "employee_id",
                 "project_code", "cost_centre", "hours_logged",
                 "rate_per_hour", "cost_inr"]
    missing_cols = [c for c in required if c not in df.columns]
    ok = len(missing_cols) == 0
    results.append(("all columns present", ok, f"missing={missing_cols}"))

    # allocation_id unique
    dupes = df["allocation_id"].duplicated().sum()
    ok = dupes == 0
    results.append(("allocation_id unique", ok, f"dupes={dupes}"))

    # No null employee_id
    nulls = df["employee_id"].isna().sum()
    ok = nulls == 0
    results.append(("no null employee_id", ok, f"nulls={nulls}"))

    # hours_logged positive
    bad_hours = (df["hours_logged"] <= 0).sum()
    ok = bad_hours == 0
    results.append(("hours_logged positive", ok, f"non_positive={bad_hours}"))

    # cost_inr positive
    bad_cost = (df["cost_inr"] <= 0).sum()
    ok = bad_cost == 0
    results.append(("cost_inr positive", ok, f"non_positive={bad_cost}"))

    # -- Report --
    total = len(results)
    failures = [r for r in results if not r[1]]
    passed = len(failures) == 0

    print(f"\n{'='*60}")
    print(f"  GE Gate G3 — Payroll Completeness")
    print(f"  Checks: {total}  |  Passed: {total - len(failures)}  |  Failed: {len(failures)}")
    print(f"  Gate: {'PASS ✓' if passed else 'FAIL ✗ — pipeline blocked'}")
    print(f"{'='*60}\n")

    if failures:
        for name, _, detail in failures:
            print(f"  FAILED: {name} ({detail})")
        print()

    return passed


if __name__ == "__main__":
    ok = run_g3()
    sys.exit(0 if ok else 1)