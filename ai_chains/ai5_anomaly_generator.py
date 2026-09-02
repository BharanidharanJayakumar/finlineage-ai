"""
AI-5 — Synthetic Anomaly Generator (Phase 2, Wk 15 roadmap item, added 2026-09-02)

Deterministic, no-LLM check (₹0, no gateway/API dependency — same philosophy as
AI-6, and deliberate given today's live experience of both Gemini and Groq free
tiers running out of headroom under normal testing volume). Answers: do the
pre-dbt GE gates (G1/G2/G3) actually catch bad data, or do we just assume they
do because they've never been tested against a known-bad input?

How it works: for each anomaly type below, this script copies the REAL
data/bronze/*.csv files (already known-clean — they pass G1-G3 in every normal
run) into an ISOLATED directory under data/synthetic_anomaly_test/<anomaly_type>/,
applies one deliberate, deterministic mutation to that copy, then calls the
actual g1_raw_gl_completeness.run_g1() / g2_fx_rate_coverage.run_g2() /
g3_payroll_completeness.run_g3() functions directly (imported, not the CI/DAG
subprocess path) against that isolated copy via their new bronze_dir override
parameter. Never touches data/bronze/ itself.

Most anomalies are expected to be caught — that's the primary evidence this
tool produces, the same pattern AI-3 already established at mid-term ("caught
3/30 narrative inconsistencies, proving the judge gate works"), applied here to
the deterministic pre-dbt gates instead. But one anomaly (negative_revenue_amount)
is a deliberate KNOWN GAP: G1-G3 don't check amount sign per account_type at
all — only G6 (post-dbt, on the built marts) catches that. Including a case
we know will escape is what makes this tool honest rather than a rigged
all-green demo — and per the Wk15 roadmap note, any escape gets written to
ai_review_queue (source_chain='ai5_synthetic_anomaly') so it surfaces in the
same HITL review flow as AI-3's low-confidence claims, instead of vanishing.

Always exits 0 — this is a test-and-report tool, not a pipeline gate itself;
an anomaly escaping detection is information (logged + routed to review), not
a reason to fail the DAG run that happens to also be building real data today.
"""

import sys
import uuid
import shutil
from pathlib import Path
from datetime import datetime, timezone

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REAL_BRONZE = PROJECT_ROOT / "data" / "bronze"
TEST_ROOT = PROJECT_ROOT / "data" / "synthetic_anomaly_test"
DB_PATH = PROJECT_ROOT / "data" / "finlineage.duckdb"
ANOMALY_SCHEMA_PATH = PROJECT_ROOT / "synthetic_anomaly_schema.sql"
REVIEW_SCHEMA_PATH = PROJECT_ROOT / "review_queue_schema.sql"

sys.path.insert(0, str(PROJECT_ROOT / "ge"))
from g1_raw_gl_completeness import run_g1
from g2_fx_rate_coverage import run_g2
from g3_payroll_completeness import run_g3

GATE_FUNCS = {"g1": run_g1, "g2": run_g2, "g3": run_g3}


# ---------- mutation functions — each takes the isolated copy's dir, mutates
# one CSV in place, and returns a human-readable description of what changed.

def _mutate_duplicate_entry_id(bronze_dir: Path) -> str:
    path = bronze_dir / "erp_gl_entries.csv"
    df = pd.read_csv(path)
    dup_id = df.iloc[0]["entry_id"]
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    df.to_csv(path, index=False)
    return f"duplicated row with entry_id={dup_id}"


def _mutate_null_entry_date(bronze_dir: Path) -> str:
    path = bronze_dir / "erp_gl_entries.csv"
    df = pd.read_csv(path)
    target_id = df.iloc[0]["entry_id"]
    df.loc[0, "entry_date"] = None
    df.to_csv(path, index=False)
    return f"set entry_date to null on entry_id={target_id}"


def _mutate_invalid_account_type(bronze_dir: Path) -> str:
    path = bronze_dir / "erp_gl_entries.csv"
    df = pd.read_csv(path)
    target_id = df.iloc[0]["entry_id"]
    df.loc[0, "account_type"] = "Miscellaneous"
    df.to_csv(path, index=False)
    return f"set account_type='Miscellaneous' (not in Revenue/COGS/OpEx) on entry_id={target_id}"


def _mutate_invalid_currency(bronze_dir: Path) -> str:
    path = bronze_dir / "erp_gl_entries.csv"
    df = pd.read_csv(path)
    target_id = df.iloc[0]["entry_id"]
    df.loc[0, "currency"] = "JPY"
    df.to_csv(path, index=False)
    return f"set currency='JPY' (not in INR/USD/EUR/GBP/AED) on entry_id={target_id}"


def _mutate_negative_revenue_amount(bronze_dir: Path) -> str:
    path = bronze_dir / "erp_gl_entries.csv"
    df = pd.read_csv(path)
    revenue_rows = df[df["account_type"] == "Revenue"]
    idx = revenue_rows.index[0]
    target_id = df.loc[idx, "entry_id"]
    original = df.loc[idx, "amount"]
    df.loc[idx, "amount"] = -abs(original)
    df.to_csv(path, index=False)
    return (f"flipped Revenue entry_id={target_id} amount {original} -> {-abs(original)} "
            f"(G1-G3 don't check amount sign per account_type — only G6 post-dbt does)")


def _mutate_missing_fx_rate(bronze_dir: Path) -> str:
    fx_path = bronze_dir / "fx_rates.csv"
    gl_path = bronze_dir / "erp_gl_entries.csv"
    fx = pd.read_csv(fx_path)
    gl = pd.read_csv(gl_path)
    foreign = gl[gl["currency"] != "INR"].iloc[0]
    target_date, target_ccy = foreign["entry_date"], foreign["currency"]
    fx = fx[~((fx["rate_date"] == target_date) & (fx["from_currency"] == target_ccy))]
    fx.to_csv(fx_path, index=False)
    return f"removed fx_rates row(s) for ({target_date}, {target_ccy}), still used by entry_id={foreign['entry_id']}"


def _mutate_zero_fx_rate(bronze_dir: Path) -> str:
    path = bronze_dir / "fx_rates.csv"
    df = pd.read_csv(path)
    target = f"{df.iloc[0]['rate_date']}/{df.iloc[0]['from_currency']}"
    df.loc[0, "rate"] = 0
    df.to_csv(path, index=False)
    return f"set fx rate to 0 for {target}"


def _mutate_duplicate_payroll_allocation_id(bronze_dir: Path) -> str:
    path = bronze_dir / "payroll_entries.csv"
    df = pd.read_csv(path)
    dup_id = df.iloc[0]["allocation_id"]
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    df.to_csv(path, index=False)
    return f"duplicated row with allocation_id={dup_id}"


def _mutate_negative_hours_logged(bronze_dir: Path) -> str:
    path = bronze_dir / "payroll_entries.csv"
    df = pd.read_csv(path)
    target_id = df.iloc[0]["allocation_id"]
    df.loc[0, "hours_logged"] = -10.0
    df.to_csv(path, index=False)
    return f"set hours_logged=-10.0 on allocation_id={target_id}"


ANOMALIES = [
    {"anomaly_type": "duplicate_entry_id", "target_file": "erp_gl_entries.csv",
     "gate": "g1", "expected_to_be_caught": True, "mutate": _mutate_duplicate_entry_id},
    {"anomaly_type": "null_entry_date", "target_file": "erp_gl_entries.csv",
     "gate": "g1", "expected_to_be_caught": True, "mutate": _mutate_null_entry_date},
    {"anomaly_type": "invalid_account_type", "target_file": "erp_gl_entries.csv",
     "gate": "g1", "expected_to_be_caught": True, "mutate": _mutate_invalid_account_type},
    {"anomaly_type": "invalid_currency", "target_file": "erp_gl_entries.csv",
     "gate": "g1", "expected_to_be_caught": True, "mutate": _mutate_invalid_currency},
    {"anomaly_type": "negative_revenue_amount", "target_file": "erp_gl_entries.csv",
     "gate": "g1", "expected_to_be_caught": False, "mutate": _mutate_negative_revenue_amount},
    {"anomaly_type": "missing_fx_rate_for_used_date", "target_file": "fx_rates.csv",
     "gate": "g2", "expected_to_be_caught": True, "mutate": _mutate_missing_fx_rate},
    {"anomaly_type": "zero_fx_rate", "target_file": "fx_rates.csv",
     "gate": "g2", "expected_to_be_caught": True, "mutate": _mutate_zero_fx_rate},
    {"anomaly_type": "duplicate_payroll_allocation_id", "target_file": "payroll_entries.csv",
     "gate": "g3", "expected_to_be_caught": True, "mutate": _mutate_duplicate_payroll_allocation_id},
    {"anomaly_type": "negative_hours_logged", "target_file": "payroll_entries.csv",
     "gate": "g3", "expected_to_be_caught": True, "mutate": _mutate_negative_hours_logged},
]


def write_anomaly_log(results: list):
    conn = duckdb.connect(str(DB_PATH), read_only=False)
    conn.execute(ANOMALY_SCHEMA_PATH.read_text(encoding="utf-8"))
    run_at = datetime.now(timezone.utc)
    for r in results:
        conn.execute("""
            INSERT INTO synthetic_anomaly_log (
                test_id, run_at, anomaly_type, target_file, gate_checked,
                mutation_detail, expected_to_be_caught, was_caught
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            str(uuid.uuid4()), run_at, r["anomaly_type"], r["target_file"], r["gate"],
            r["mutation_detail"], r["expected_to_be_caught"], r["was_caught"],
        ])
    conn.close()


def route_escapes_to_review(escapes: list):
    if not escapes:
        return
    conn = duckdb.connect(str(DB_PATH), read_only=False)
    conn.execute(REVIEW_SCHEMA_PATH.read_text(encoding="utf-8"))
    run_at = datetime.now(timezone.utc)
    for r in escapes:
        claim_text = (
            f"[AI-5 SYNTHETIC ANOMALY ESCAPED DETECTION] anomaly='{r['anomaly_type']}' "
            f"file={r['target_file']} gate_checked={r['gate']} "
            f"expected_to_be_caught={r['expected_to_be_caught']} — {r['mutation_detail']}"
        )
        conn.execute("""
            INSERT INTO ai_review_queue (
                review_id, run_at, source_chain, claim_text,
                self_reported_score, second_judge_score, retrieval_grounding_score,
                confidence_score, routing_band
            ) VALUES (?, ?, 'ai5_synthetic_anomaly', ?, NULL, NULL, NULL, 0.0, 'human_review')
        """, [str(uuid.uuid4()), run_at, claim_text])
    conn.close()


def run_ai5():
    print("\n" + "=" * 60)
    print("  AI-5 — Synthetic Anomaly Generator")
    print("=" * 60 + "\n")

    if not REAL_BRONZE.exists():
        print("  ERROR: data/bronze/ not found. Run scripts/generate_sources.py first.")
        return False

    if TEST_ROOT.exists():
        shutil.rmtree(TEST_ROOT)
    TEST_ROOT.mkdir(parents=True)

    results = []
    for spec in ANOMALIES:
        case_dir = TEST_ROOT / spec["anomaly_type"]
        shutil.copytree(REAL_BRONZE, case_dir)
        detail = spec["mutate"](case_dir)

        print(f"  --- {spec['anomaly_type']} ({spec['gate']}) ---")
        gate_passed = GATE_FUNCS[spec["gate"]](bronze_dir=case_dir)
        was_caught = not gate_passed  # the gate FAILING means it caught the anomaly

        results.append({**spec, "mutation_detail": detail, "was_caught": was_caught})

        mark = "caught" if was_caught else "ESCAPED"
        expect_note = "" if was_caught == spec["expected_to_be_caught"] else "  <- unexpected!"
        print(f"  RESULT: {spec['anomaly_type']} -> {mark} "
              f"(expected_to_be_caught={spec['expected_to_be_caught']}){expect_note}\n")

    write_anomaly_log(results)
    escapes = [r for r in results if not r["was_caught"]]
    route_escapes_to_review(escapes)

    caught = [r for r in results if r["was_caught"]]
    unexpected = [r for r in results if r["was_caught"] != r["expected_to_be_caught"]]

    print(f"\n  {len(caught)}/{len(results)} anomalies caught by G1/G2/G3")
    print(f"  {len(escapes)} escaped -> routed to ai_review_queue (source_chain=ai5_synthetic_anomaly)")
    if unexpected:
        print(f"  WARNING: {len(unexpected)} result(s) didn't match their expected outcome — "
              f"a real gate regression, not the known negative_revenue_amount gap:")
        for r in unexpected:
            print(f"    - {r['anomaly_type']}: expected_to_be_caught={r['expected_to_be_caught']}, "
                  f"was_caught={r['was_caught']}")
    else:
        print("  All results matched their expected outcome (including the one deliberate "
              "known gap: negative_revenue_amount, which only G6 post-dbt catches).")

    print(f"\n  {len(results)} rows written to synthetic_anomaly_log in finlineage.duckdb")
    print(f"\n{'='*60}\n")

    # Always non-blocking — see module docstring. Escapes are information,
    # routed to review, not a DAG failure.
    return True


if __name__ == "__main__":
    ok = run_ai5()
    sys.exit(0 if ok else 1)
