"""
Wk17 — real unit tests for AI-5 (ai_chains/ai5_anomaly_generator.py).

AI-5 is entirely deterministic (no LLM/gateway call by design — see its own
module docstring), so its full run_ai5() can be exercised end-to-end here: it
mutates isolated bronze copies, re-runs the real G1/G2/G3 gate functions
against them, and writes results to duckdb. Run against a disposable copy of
finlineage.duckdb (writable_db fixture) — never the real one.
"""
import sys
from pathlib import Path

import duckdb
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "ai_chains"))

import ai5_anomaly_generator as ai5


@pytest.fixture()
def patched_ai5(writable_db, tmp_path, monkeypatch):
    """Points ai5 at the disposable db copy and an isolated
    synthetic_anomaly_test scratch dir, instead of the real ones."""
    monkeypatch.setattr(ai5, "DB_PATH", writable_db)
    monkeypatch.setattr(ai5, "TEST_ROOT", tmp_path / "synthetic_anomaly_test")
    return ai5


def test_run_ai5_catches_8_of_9_anomalies_exactly_as_designed(patched_ai5):
    """This is the project's own headline AI-5 evidence claim (Section 4 of
    the submission doc) — asserting it directly, against real gate logic,
    rather than trusting the printed summary."""
    ok = patched_ai5.run_ai5()
    assert ok is True  # always non-blocking by design

    # synthetic_anomaly_log is append-only by design (same pattern as
    # doc_drift_log/_pipeline_sync_log), and writable_db is a copy of the
    # REAL dev database, which already carries rows from the participant's
    # own earlier local runs — so scope to just the run this test triggered,
    # same pattern tests/test_e2e_pipeline.py already uses for this table.
    conn = duckdb.connect(str(patched_ai5.DB_PATH), read_only=True)
    rows = conn.execute("""
        SELECT anomaly_type, expected_to_be_caught, was_caught FROM synthetic_anomaly_log
        WHERE run_at = (SELECT max(run_at) FROM synthetic_anomaly_log)
    """).fetchdf()
    conn.close()

    assert len(rows) == 9

    # Every outcome should match its expectation — including the one
    # deliberate known gap, where expected_to_be_caught=False and
    # was_caught=False both hold (a correctly-predicted escape, not a
    # regression). A real gate regression would show up here as a mismatch.
    mismatches = rows[rows.expected_to_be_caught != rows.was_caught]
    assert mismatches.empty, f"unexpected gate regression(s): {mismatches.to_dict('records')}"

    escapes = rows[~rows["was_caught"]]
    assert list(escapes["anomaly_type"]) == ["negative_revenue_amount"]
    assert (rows["was_caught"]).sum() == 8


def test_run_ai5_routes_the_escaped_anomaly_to_review_queue(patched_ai5):
    conn = duckdb.connect(str(patched_ai5.DB_PATH), read_only=False)
    conn.execute(patched_ai5.REVIEW_SCHEMA_PATH.read_text(encoding="utf-8"))
    before = conn.execute(
        "SELECT COUNT(*) FROM ai_review_queue WHERE source_chain = 'ai5_synthetic_anomaly'"
    ).fetchone()[0]
    conn.close()

    patched_ai5.run_ai5()

    conn = duckdb.connect(str(patched_ai5.DB_PATH), read_only=True)
    rows = conn.execute(
        "SELECT source_chain, routing_band, confidence_score FROM ai_review_queue "
        "WHERE source_chain = 'ai5_synthetic_anomaly' ORDER BY run_at DESC LIMIT 1"
    ).fetchdf()
    after = conn.execute(
        "SELECT COUNT(*) FROM ai_review_queue WHERE source_chain = 'ai5_synthetic_anomaly'"
    ).fetchone()[0]
    conn.close()

    assert after == before + 1  # exactly one new escape (negative_revenue_amount) this run
    assert rows.iloc[0]["routing_band"] == "human_review"
    assert rows.iloc[0]["confidence_score"] == 0.0


def test_run_ai5_errors_gracefully_when_bronze_is_missing(patched_ai5, monkeypatch, tmp_path):
    monkeypatch.setattr(patched_ai5, "REAL_BRONZE", tmp_path / "does_not_exist")
    assert patched_ai5.run_ai5() is False


@pytest.mark.parametrize("mutate_fn_name,target_file", [
    ("_mutate_duplicate_entry_id", "erp_gl_entries.csv"),
    ("_mutate_null_entry_date", "erp_gl_entries.csv"),
    ("_mutate_invalid_currency", "erp_gl_entries.csv"),
    ("_mutate_zero_fx_rate", "fx_rates.csv"),
])
def test_each_mutation_actually_changes_its_target_file(mutate_fn_name, target_file, tmp_path):
    """Unit-tests the mutation helpers directly (isolated from the gate
    re-run), confirming each one actually perturbs the file it claims to."""
    case_dir = tmp_path / "case"
    import shutil
    shutil.copytree(PROJECT_ROOT / "data" / "bronze", case_dir)
    before = (case_dir / target_file).read_text()
    mutate_fn = getattr(ai5, mutate_fn_name)
    detail = mutate_fn(case_dir)
    after = (case_dir / target_file).read_text()
    assert before != after
    assert isinstance(detail, str) and len(detail) > 0
