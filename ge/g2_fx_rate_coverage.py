"""
GE Gate G2 — FX Rate Coverage
Validates that fx_rates.csv has a rate for every currency on every date
in the GL entries. Runs after G1, before dbt intermediate layer.

Checks:
  - FX file not empty
  - rate is always positive
  - No null rates
  - Every (date, currency) pair in GL has a matching FX rate
"""

import sys
from pathlib import Path
import pandas as pd

BRONZE = Path(__file__).resolve().parent.parent / "data" / "bronze"


def run_g2(bronze_dir: Path = None):
    """bronze_dir override added for ai5_anomaly_generator.py — see the same
    note in g1_raw_gl_completeness.py. Defaults to the real BRONZE dir."""
    bronze_dir = bronze_dir or BRONZE
    fx = pd.read_csv(bronze_dir / "fx_rates.csv")
    gl = pd.read_csv(bronze_dir / "erp_gl_entries.csv")

    results = []

    # Check 1: FX file not empty
    ok = len(fx) > 0
    results.append(("fx_rates row count > 0", ok, f"rows={len(fx)}"))

    # Check 2: rate always positive
    bad_rates = fx[fx["rate"] <= 0]
    ok = len(bad_rates) == 0
    results.append(("all rates positive", ok, f"non_positive={len(bad_rates)}"))

    # Check 3: no null rates
    null_rates = fx["rate"].isna().sum()
    ok = null_rates == 0
    results.append(("no null rates", ok, f"nulls={null_rates}"))

    # Check 4: every foreign-currency GL entry date has an FX rate
    foreign = gl[gl["currency"] != "INR"][["entry_date", "currency"]].drop_duplicates()
    fx_keys = set(zip(fx["rate_date"], fx["from_currency"]))
    missing = foreign[
        ~foreign.apply(lambda r: (r["entry_date"], r["currency"]) in fx_keys, axis=1)
    ]
    ok = len(missing) == 0
    results.append(("all GL dates have FX rates", ok, f"missing_pairs={len(missing)}"))

    # -- Report --
    total = len(results)
    failures = [r for r in results if not r[1]]
    passed = len(failures) == 0

    print(f"\n{'='*60}")
    print(f"  GE Gate G2 — FX Rate Coverage")
    print(f"  Checks: {total}  |  Passed: {total - len(failures)}  |  Failed: {len(failures)}")
    print(f"  Gate: {'PASS ✓' if passed else 'FAIL ✗ — pipeline blocked'}")
    print(f"{'='*60}\n")

    if failures:
        for name, _, detail in failures:
            print(f"  FAILED: {name} ({detail})")
        print()

    return passed


if __name__ == "__main__":
    ok = run_g2()
    sys.exit(0 if ok else 1)