"""
scripts/scale_test.py — Wk16 scale test (50K GL entries / 20K payroll
entries, up from the official baseline's 500/200).

WHAT THIS VALIDATES: that dbt build, all 6 GE gates, and the P&L
reconciliation still hold at 100x the row volume — the layer that actually
processes raw row counts. It deliberately does NOT touch the AI chains:
ai1_pnl_narrative sends Gemini the already-aggregated mart_pnl_summary
(~15 period+segment rows regardless of how many GL/payroll rows fed it), so
a 100x increase in bronze row counts doesn't change the LLM prompt size or
exercise LiteLLM's rate-limit/fallback handling any differently than a
normal run already does. That part of Wk16's scope ("LiteLLM rate-limit
handling ... validated at scale") is instead covered by the existing
free-tier-exhaustion evidence from 2026-09-02 (see
FINLINEAGE_PROJECT_KNOWLEDGE_BASE_1.md Section 7.4) and by
tests/test_e2e_pipeline.py's per-run handling of it.

IMPORTANT — THIS TEMPORARILY REPLACES YOUR REAL DATA:
This script runs generate_sources.py with FINLINEAGE_GL_ROWS=50000 and
FINLINEAGE_PAYROLL_ROWS=20000 against the SAME data/bronze/*.csv files and
the SAME data/finlineage.duckdb your normal runs use (seed stays 42, so
it's still deterministic — just bigger). After this script finishes, your
bronze CSVs and marts reflect 50K/20K-row data, NOT the official 500/200-row
baseline your P&L reconciliation evidence and Power BI report are built on.

When this script finishes (pass or fail), run this to restore the official
baseline before doing anything evidence- or demo-related:

    python scripts/generate_sources.py
    cd finlineage && dbt build --exclude source:* && cd ..

This script prints that same reminder at the end regardless of outcome.
Assumes `dbt deps` has already been run at least once (same assumption as
Section 13 of the knowledge base's Environment Setup steps).

Run: python scripts/scale_test.py
"""

import os
import subprocess
import sys
import time
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DBT_DIR = PROJECT_ROOT / "finlineage"
DB_PATH = PROJECT_ROOT / "data" / "finlineage.duckdb"

GL_ROWS = 50_000
PAYROLL_ROWS = 20_000


def _timed_run(label: str, cmd: list, cwd: Path = PROJECT_ROOT, env: dict = None):
    print(f"\n--- {label} ---")
    start = time.time()
    # Force UTF-8 for the child's stdout/stderr. On Windows, a Python
    # process whose stdout is piped (as it is here, via capture_output)
    # rather than attached to a real console falls back to the system's
    # ANSI codepage (cp1252) instead of UTF-8 unless told otherwise — and
    # ge/g1_raw_gl_completeness.py (and the other gate scripts) print a
    # literal "✓"/"✗" in their PASS/FAIL banner, which cp1252 can't encode
    # at all. Without this, every gate subprocess crashes with
    # UnicodeEncodeError before it even gets to run its checks — a tooling
    # bug in how this script invokes them, not a problem with the gates
    # themselves (they run fine when invoked directly in a real terminal).
    full_env = {**os.environ, **(env or {}), "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"}
    # encoding="utf-8" here is the other half of the fix: it tells THIS
    # (parent) process to decode the child's captured stdout/stderr bytes
    # as UTF-8. Without it, subprocess.run falls back to
    # locale.getpreferredencoding() — cp1252 on Windows — which doesn't
    # crash (cp1252 can decode any byte to *something*) but silently
    # mangles the "✓"/"—" characters the child wrote as UTF-8 into
    # garbage like "âœ"" / "â€"".
    result = subprocess.run(cmd, cwd=cwd, env=full_env, capture_output=True, text=True, encoding="utf-8")
    elapsed = time.time() - start
    tail = result.stdout[-3000:] if result.stdout else ""
    print(tail)
    if result.returncode != 0:
        print(result.stderr[-3000:] if result.stderr else "(no stderr captured)")
    print(f"--- {label}: {'PASS' if result.returncode == 0 else 'FAIL'} in {elapsed:.1f}s ---")
    return result.returncode == 0, elapsed


def _print_summary_and_restore_reminder(report):
    print("\n" + "=" * 70)
    print("  SCALE TEST SUMMARY")
    print("=" * 70)
    for label, ok, elapsed in report:
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {label:45s} {elapsed:6.1f}s")
    print("=" * 70)
    print("  RESTORE THE OFFICIAL BASELINE before doing anything evidence/demo-related:")
    print("      python scripts/generate_sources.py")
    print("      cd finlineage && dbt build --exclude source:* && cd ..")
    print("=" * 70)


def main():
    print("=" * 70)
    print(f"  Wk16 SCALE TEST — {GL_ROWS:,} GL entries / {PAYROLL_ROWS:,} payroll entries")
    print("  WARNING: this replaces data/bronze/*.csv and data/finlineage.duckdb")
    print("  with scaled data. See this script's module docstring for how to")
    print("  restore the official 500/200 baseline afterward.")
    print("=" * 70)

    report = []

    ok, t = _timed_run(
        "generate_sources.py (scaled)",
        [sys.executable, "scripts/generate_sources.py"],
        env={"FINLINEAGE_GL_ROWS": str(GL_ROWS), "FINLINEAGE_PAYROLL_ROWS": str(PAYROLL_ROWS)},
    )
    report.append(("generate_sources (scaled)", ok, t))
    if not ok:
        _print_summary_and_restore_reminder(report)
        sys.exit(1)

    for label, script in [
        ("GE gate G1", "ge/g1_raw_gl_completeness.py"),
        ("GE gate G2", "ge/g2_fx_rate_coverage.py"),
        ("GE gate G3", "ge/g3_payroll_completeness.py"),
    ]:
        ok, t = _timed_run(label, [sys.executable, script])
        report.append((label, ok, t))
        if not ok:
            _print_summary_and_restore_reminder(report)
            sys.exit(1)

    ok, t = _timed_run("dbt build", ["dbt", "build", "--exclude", "source:*"], DBT_DIR)
    report.append(("dbt build", ok, t))
    if not ok:
        _print_summary_and_restore_reminder(report)
        sys.exit(1)

    for label, script in [
        ("GE gate G4", "ge/g4_pnl_aggregation_consistency.py"),
        ("GE gate G5", "ge/g5_cross_mart_consistency.py"),
        ("GE gate G6", "ge/g6_metric_drift.py"),
    ]:
        ok, t = _timed_run(label, [sys.executable, script])
        report.append((label, ok, t))
        if not ok:
            _print_summary_and_restore_reminder(report)
            sys.exit(1)

    # Same reconciliation check ci.yml / tests/test_e2e_pipeline.py already run.
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    gl_rows = conn.execute("SELECT COUNT(*) FROM main.stg_erp_gl_entries").fetchone()[0]
    payroll_rows = conn.execute("SELECT COUNT(*) FROM main.stg_payroll_entries").fetchone()[0]
    src = conn.execute("SELECT ROUND(SUM(amount_inr), 2) FROM main.int_entries_cost_centre_mapped").fetchone()[0]
    mrt = conn.execute("SELECT ROUND(SUM(revenue + cogs + opex), 2) FROM main.mart_pnl_summary").fetchone()[0]
    conn.close()
    diff = abs(src - mrt)
    recon_ok = diff < 0.01
    report.append((f"P&L reconciliation (diff={diff:.2f})", recon_ok, 0.0))
    print(f"\nStaged row counts — GL: {gl_rows:,}, Payroll: {payroll_rows:,}")
    print(f"Reconciliation: source={src:,.2f}  mart={mrt:,.2f}  diff={diff:.2f}  "
          f"({'PASS' if recon_ok else 'FAIL'})")

    _print_summary_and_restore_reminder(report)
    sys.exit(0 if recon_ok else 1)


if __name__ == "__main__":
    main()
