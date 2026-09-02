"""
GE Gate G1 — Raw GL Completeness
Validates that erp_gl_entries.csv is structurally sound BEFORE dbt touches it.
Runs as the first step in the Airflow DAG — if it fails, dbt never runs.
"""

import sys
from pathlib import Path
import pandas as pd
import great_expectations as gx
from great_expectations.expectations import (
    ExpectTableRowCountToBeBetween,
    ExpectTableColumnsToMatchSet,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeInSet,
)

BRONZE = Path(__file__).resolve().parent.parent / "data" / "bronze"


def run_g1(bronze_dir: Path = None):
    """bronze_dir override added for ai5_anomaly_generator.py, which runs this
    same check against an isolated, mutated copy of the bronze CSVs — never
    against the real data/bronze/ — to verify a synthetic anomaly actually
    gets caught. Defaults to the real BRONZE dir, so every existing caller
    (the DAG, ci.yml, running this file directly) is unaffected."""
    bronze_dir = bronze_dir or BRONZE
    csv_path = bronze_dir / "erp_gl_entries.csv"

    context = gx.get_context()

    # Use in-memory pandas datasource (works reliably across GE versions)
    df = pd.read_csv(csv_path)

    ds = context.data_sources.add_or_update_pandas(name="bronze_pandas")
    asset = ds.add_dataframe_asset(name="erp_gl_entries")
    batch_definition = asset.add_batch_definition_whole_dataframe("full")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    # Build expectation suite
    suite = gx.ExpectationSuite(name="g1_raw_gl_completeness")

    suite.add_expectation(ExpectTableRowCountToBeBetween(min_value=10, max_value=100000))
    suite.add_expectation(ExpectTableColumnsToMatchSet(
        column_set=[
            "entry_id", "entry_date", "project_code", "project_name",
            "segment", "cost_centre", "account_code", "account_name",
            "account_type", "currency", "amount", "description"
        ],
        exact_match=True,
    ))
    suite.add_expectation(ExpectColumnValuesToBeUnique(column="entry_id"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="entry_id"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="entry_date"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="amount"))
    suite.add_expectation(ExpectColumnValuesToBeInSet(
        column="account_type", value_set=["Revenue", "COGS", "OpEx"]
    ))
    suite.add_expectation(ExpectColumnValuesToBeInSet(
        column="currency", value_set=["INR", "USD", "EUR", "GBP", "AED"]
    ))

    suite = context.suites.add_or_update(suite)

    # Run validation
    results = batch.validate(suite)

    # Report
    passed = results.success
    total = len(results.results)
    failures = [r for r in results.results if not r.success]

    print(f"\n{'='*60}")
    print(f"  GE Gate G1 — Raw GL Completeness")
    print(f"  File: {csv_path.name}")
    print(f"  Expectations: {total}  |  Passed: {total - len(failures)}  |  Failed: {len(failures)}")
    print(f"  Gate: {'PASS ✓' if passed else 'FAIL ✗ — pipeline blocked'}")
    print(f"{'='*60}\n")

    if failures:
        for f in failures:
            print(f"  FAILED: {f.expectation_config.type}")
        print()

    return passed


if __name__ == "__main__":
    ok = run_g1()
    sys.exit(0 if ok else 1)