"""
Wk16 — pytest E2E suite for the full Docker/Airflow pipeline.

Run with (from the project root, Docker Desktop running):

    pytest tests/ -v

The suite brings the docker-compose stack up (or reuses a running one),
triggers one real finlineage_daily_pipeline DAG run, and asserts against
both the Airflow task states and the resulting data/finlineage.duckdb
content. Deterministic (non-LLM, non-external-service) tasks are hard
assertions; LLM-dependent and Databricks-dependent tasks are checked for
"succeeded, or failed for a documented reason" — see conftest.py's module
docstring for why.
"""

import pytest

from conftest import (
    DETERMINISTIC_TASK_IDS,
    LLM_DEPENDENT_TASK_IDS,
    EXTERNAL_SERVICE_TASK_IDS,
    get_task_state,
    get_task_log,
    failed_for_known_reason,
    run_in_airflow_container,
)


def test_dag_run_reaches_terminal_state(dag_run):
    assert dag_run["state"] in ("success", "failed"), (
        f"DAG run did not finish cleanly, ended in state={dag_run['state']!r}"
    )


@pytest.mark.parametrize("task_id", DETERMINISTIC_TASK_IDS)
def test_deterministic_task_succeeds(dag_run, task_id):
    """These never call Gemini/Groq — a failure here is a real regression,
    never "just the free tier," so it's a hard assertion."""
    state = get_task_state(dag_run["dag_run_id"], task_id)
    assert state == "success", f"{task_id} ended in state={state!r} (expected success)"


@pytest.mark.parametrize("task_id", LLM_DEPENDENT_TASK_IDS)
def test_llm_dependent_task_succeeds_or_fails_for_known_reason(dag_run, task_id):
    state = get_task_state(dag_run["dag_run_id"], task_id)
    if state == "success":
        return
    assert failed_for_known_reason(dag_run["dag_run_id"], task_id), (
        f"{task_id} ended in state={state!r} and its log does NOT show a known "
        "Gemini/Groq rate-limit/quota signature — this looks like a real "
        "regression, not expected free-tier exhaustion."
    )


@pytest.mark.parametrize("task_id", EXTERNAL_SERVICE_TASK_IDS)
def test_external_service_task_succeeds_or_is_unconfigured(dag_run, task_id):
    state = get_task_state(dag_run["dag_run_id"], task_id)
    if state == "success":
        return
    log = get_task_log(dag_run["dag_run_id"], task_id)
    assert "DATABRICKS" in log.upper(), (
        f"{task_id} ended in state={state!r} for a reason other than missing/invalid "
        "Databricks config — investigate."
    )


def test_pnl_reconciliation_is_zero(dag_run):
    """Same check ci.yml already runs — restated here so it's covered
    inside the full Docker/Airflow run too, not just the lighter CI job.

    Deliberately does NOT use the duckdb_conn (host-side) fixture other
    tests use: int_entries_cost_centre_mapped is a dbt VIEW (staging/
    intermediate are configured as views, not tables), so it stores no
    data of its own — only the SQL dbt_build baked in, which reads
    data/bronze/*.csv via a CONTAINER-absolute path. Querying it from the
    host fails with an IOException (the container path doesn't exist on
    the host filesystem) even though the shared .duckdb file opens fine —
    see run_in_airflow_container()'s docstring in conftest.py. Running the
    query through that helper instead executes it inside the same
    container dbt_build ran in, where the baked-in path is real."""
    if get_task_state(dag_run["dag_run_id"], "dbt_build") != "success":
        pytest.skip("dbt_build did not succeed this run — nothing to reconcile")
    result = run_in_airflow_container("""
import duckdb
conn = duckdb.connect('/opt/airflow/project/data/finlineage.duckdb', read_only=True)
src = conn.execute("SELECT ROUND(SUM(amount_inr),2) FROM main.int_entries_cost_centre_mapped").fetchone()[0]
mrt = conn.execute("SELECT ROUND(SUM(revenue+cogs+opex),2) FROM main.mart_pnl_summary").fetchone()[0]
diff = abs(src - mrt)
print(f"RECON_SRC={src}")
print(f"RECON_MRT={mrt}")
print(f"RECON_DIFF={diff}")
assert diff < 0.01, f"RECONCILIATION FAILED: diff={diff}"
""")
    assert result.returncode == 0, (
        "P&L reconciliation check failed inside the airflow-standalone container:\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )


@pytest.mark.parametrize("mart", [
    "mart_pnl_summary", "mart_revenue_trends", "mart_cost_analysis",
    "mart_variance_analysis", "mart_metric_dictionary",
])
def test_mart_has_rows(dag_run, duckdb_conn, mart):
    if get_task_state(dag_run["dag_run_id"], "dbt_build") != "success":
        pytest.skip("dbt_build did not succeed this run")
    count = duckdb_conn.execute(f"SELECT COUNT(*) FROM main.{mart}").fetchone()[0]
    assert count > 0, f"{mart} has zero rows after a successful dbt_build"


def test_ai5_anomalies_match_expected_outcomes(dag_run, duckdb_conn):
    """AI-5's whole point: every anomaly's was_caught must match its
    expected_to_be_caught, INCLUDING the one deliberate known gap
    (negative_revenue_amount) — see ai5_anomaly_generator.py. A mismatch
    here means a real gate regression, not free-tier noise — ai5 makes zero
    LLM calls, so it's a hard assertion."""
    if get_task_state(dag_run["dag_run_id"], "ai5_anomaly_generator") != "success":
        pytest.skip("ai5_anomaly_generator did not succeed this run")
    df = duckdb_conn.execute("""
        SELECT anomaly_type, expected_to_be_caught, was_caught
        FROM synthetic_anomaly_log
        WHERE run_at = (SELECT max(run_at) FROM synthetic_anomaly_log)
    """).fetchdf()
    assert len(df) == 9, f"expected 9 anomaly rows for the latest run, found {len(df)}"
    mismatches = df[df.expected_to_be_caught != df.was_caught]
    assert mismatches.empty, f"AI-5 gate regression(s): {mismatches.to_dict('records')}"


def test_ai6_doc_drift_reports_no_stale_docs(dag_run, duckdb_conn):
    """A dbt model's SQL should never silently drift out of sync with its
    last-documented state within the SAME run that (re)documented it."""
    if get_task_state(dag_run["dag_run_id"], "ai6_doc_drift_detection") != "success":
        pytest.skip("ai6_doc_drift_detection did not succeed this run")
    df = duckdb_conn.execute("""
        SELECT model_name, drift_reason
        FROM doc_drift_log
        WHERE checked_at = (SELECT max(checked_at) FROM doc_drift_log)
          AND drift_reason = 'sql_changed_since_docs'
    """).fetchdf()
    assert df.empty, f"Unexpected doc drift this run: {df.to_dict('records')}"


def test_narrative_headline_snippet_is_bounded(dag_run, duckdb_conn):
    """Covers the Wk15 AI-1-to-BI-card feature: when ai1 succeeds, it must
    have written a fresh, word-bounded headline row that Power BI can bind
    a card to."""
    if get_task_state(dag_run["dag_run_id"], "ai1_pnl_narrative") != "success":
        pytest.skip("ai1_pnl_narrative did not succeed this run (quota) — no fresh headline expected")
    row = duckdb_conn.execute("""
        SELECT snippet_text, word_count FROM ai_narrative_snippets
        WHERE snippet_type = 'pnl_executive_headline'
        ORDER BY generated_at DESC LIMIT 1
    """).fetchone()
    assert row is not None, "ai1 succeeded but no pnl_executive_headline row was written"
    snippet_text, word_count = row
    assert word_count <= 25, f"headline word_count={word_count} exceeds the 25-word bound"
    assert word_count == len(snippet_text.split()), "stored word_count doesn't match snippet_text"
