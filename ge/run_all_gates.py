"""
Run all GE gates in pipeline order. Stops on first failure.
Usage: python ge/run_all_gates.py
"""

import sys
import importlib


GATES = [
    ("g1_raw_gl_completeness", "G1 — Raw GL Completeness"),
    ("g2_fx_rate_coverage",    "G2 — FX Rate Coverage"),
    ("g3_payroll_completeness","G3 — Payroll Completeness"),
    ("g4_pnl_aggregation_consistency", "G4 — P&L Aggregation Consistency"),
    ("g5_cross_mart_consistency",      "G5 — Cross-Mart Consistency"),
]


def main():
    # Pre-mart gates (run before dbt)
    pre_mart = GATES[:3]
    post_mart = GATES[3:]

    print("\n" + "="*60)
    print("  FinLineage AI — Quality Gate Runner")
    print("="*60)

    print("\n--- PRE-DBT GATES (run before dbt build) ---")
    for module_name, label in pre_mart:
        mod = importlib.import_module(module_name)
        run_fn = getattr(mod, f"run_{module_name.split('_', 1)[0]}")
        if not run_fn():
            print(f"\n  PIPELINE BLOCKED at {label}. Fix the data and re-run.")
            return False

    print("\n--- POST-DBT GATES (run after dbt build) ---")
    for module_name, label in post_mart:
        mod = importlib.import_module(module_name)
        run_fn = getattr(mod, f"run_{module_name.split('_', 1)[0]}")
        if not run_fn():
            print(f"\n  PIPELINE BLOCKED at {label}. Fix the model and re-run.")
            return False

    print("\n" + "="*60)
    print("  ALL GATES PASSED ✓ — pipeline clear for publication")
    print("="*60 + "\n")
    return True


if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)