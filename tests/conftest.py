"""
Shared pytest fixtures for the Wk16 Docker/Airflow E2E test suite.

These fixtures bring up the SAME docker-compose stack used in normal
operation (docker-compose.yml — Airflow standalone + litellm-gateway +
redis), trigger a real DAG run of finlineage_daily_pipeline via Airflow's
REST API, and poll it to completion. This deliberately exercises the real
pipeline (dbt, GE gates, AI-5/AI-6, Databricks sync, and the LLM chains
subject to their real free-tier quotas) rather than mocking any of it —
that's the whole point of an E2E suite for this project.

Design note on the LLM-dependent tasks (ai1_pnl_narrative,
ai2_transformation_docs, ai3_narrative_judge_v2, ai4_variance_agent): these
share Gemini's 20-requests/day free-tier quota with whatever else has
called the same Google Cloud project's API key today (see
FINLINEAGE_PROJECT_KNOWLEDGE_BASE_1.md Section 7.4) — a quota exhaustion
mid-suite is EXPECTED free-tier behaviour, not a suite bug. Tests for those
tasks assert "succeeded, OR failed for a documented rate-limit reason"
rather than a hard pass — see test_e2e_pipeline.py. Every deterministic,
quota-immune task (the dbt/GE/AI-5/AI-6 side of the DAG) IS hard-asserted —
a regression there is a real bug, never "just quota."

Run with (Docker Desktop running, from the project root):

    pytest tests/ -v

Env vars this suite reads:
    AIRFLOW_BASE_URL          default http://localhost:8080
    AIRFLOW_WWW_USER_USERNAME default admin
    AIRFLOW_WWW_USER_PASSWORD default admin
    E2E_DAG_TIMEOUT_S         default 1800 (30 min) — raise this if your
                              machine is slower or the DAG has grown more tasks
    E2E_TEARDOWN              set to "1" to `docker compose down` after the
                              suite finishes (CI sets this — an ephemeral
                              runner should clean up; local runs default to
                              leaving your dev stack up)
"""

import os
import re
import subprocess
import time
import uuid
from pathlib import Path

import duckdb
import pytest
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "finlineage.duckdb"

AIRFLOW_BASE_URL = os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080")
AIRFLOW_AUTH = (
    os.environ.get("AIRFLOW_WWW_USER_USERNAME", "admin"),
    os.environ.get("AIRFLOW_WWW_USER_PASSWORD", "admin"),
)
DAG_ID = "finlineage_daily_pipeline"

# Tasks with NO LLM/gateway dependency — these must succeed every time,
# regardless of Gemini/Groq quota state. A failure here is a real bug.
DETERMINISTIC_TASK_IDS = [
    "generate_source_data",
    "ge_gate_g1_raw_gl_completeness",
    "ge_gate_g2_fx_rate_coverage",
    "ge_gate_g3_payroll_completeness",
    "dbt_build",
    "ge_gate_g4_pnl_aggregation",
    "ge_gate_g5_cross_mart_consistency",
    "ge_gate_g6_metric_drift",
    "ai5_anomaly_generator",
    "ai6_doc_drift_detection",
]

# Tasks that call Gemini/Groq through the LiteLLM gateway — allowed to fail
# ONLY for a documented rate-limit/quota reason (see module docstring).
LLM_DEPENDENT_TASK_IDS = [
    "ai1_pnl_narrative",
    "ai2_transformation_docs",
    "ai4_variance_agent",
    "ai3_narrative_judge_v2",
]

# Needs live Databricks credentials in .env — legitimately fails on a
# machine/CI run that hasn't configured them (see dags/finlineage_daily.py's
# module docstring). Soft-checked, same reasoning as the LLM tasks above.
EXTERNAL_SERVICE_TASK_IDS = ["sync_gold_to_databricks"]

DAG_RUN_TIMEOUT_S = int(os.environ.get("E2E_DAG_TIMEOUT_S", "1800"))
POLL_INTERVAL_S = 10

KNOWN_RATE_LIMIT_PATTERNS = re.compile(
    r"RESOURCE_EXHAUSTED|429|rate.?limit|quota|Rate Limit Reached", re.IGNORECASE
)


def _compose(*args):
    return subprocess.run(
        ["docker", "compose", *args], cwd=PROJECT_ROOT, check=True,
        capture_output=True, text=True,
    )


def _airflow_is_up() -> bool:
    try:
        r = requests.get(f"{AIRFLOW_BASE_URL}/health", timeout=5)
        return r.status_code == 200
    except requests.RequestException:
        return False


def run_in_airflow_container(python_code: str) -> subprocess.CompletedProcess:
    """Runs a short Python snippet using the airflow-standalone container's
    OWN venv, inside that container — not on the host pytest is running on.

    Needed for anything that queries a dbt staging/intermediate VIEW (as
    opposed to a materialized mart TABLE): dbt_project.yml configures
    staging/intermediate as views, so a view like
    int_entries_cost_centre_mapped stores no data of its own — it stores
    the SQL that produces it, and dbt_build baked that SQL with a
    CONTAINER-absolute bronze path (see dags/finlineage_daily.py's
    DBT_VARS). A host-side duckdb connection opens the shared
    finlineage.duckdb file fine and can read any materialized TABLE (the 5
    marts, ai_narrative_snippets, synthetic_anomaly_log, doc_drift_log) —
    those hold real data in the file itself. But re-evaluating a VIEW's
    stored read_csv_auto(...) call from the host fails with an
    IOException: the container path it's looking for doesn't exist on the
    host filesystem. Running the query through this function instead means
    it executes in the same filesystem namespace dbt_build used, so the
    baked-in path resolves correctly."""
    return subprocess.run(
        ["docker", "compose", "exec", "-T", "airflow-standalone",
         "/opt/airflow/project/.venv/bin/python", "-c", python_code],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
    )


@pytest.fixture(scope="session")
def docker_stack():
    """Brings the docker-compose stack up if it isn't already running, and
    waits for Airflow's webserver to answer /health. Leaves the stack
    running afterward on a developer's own machine (it's their persistent
    dev environment) — only tears it down when E2E_TEARDOWN=1, which the CI
    workflow sets explicitly since GitHub Actions runners are ephemeral
    anyway."""
    already_up = _airflow_is_up()
    if not already_up:
        print("\n  docker compose stack not running — starting it (docker compose up -d --build) ...")
        _compose("up", "-d", "--build")

    deadline = time.time() + 180
    while time.time() < deadline:
        if _airflow_is_up():
            break
        time.sleep(5)
    else:
        raise RuntimeError(
            "Airflow webserver never became healthy at "
            f"{AIRFLOW_BASE_URL}/health within 180s — check `docker compose logs airflow-standalone`."
        )

    yield

    if os.environ.get("E2E_TEARDOWN") == "1":
        print("\n  E2E_TEARDOWN=1 — tearing down docker compose stack ...")
        _compose("down")


@pytest.fixture(scope="session")
def dag_run(docker_stack):
    """Unpauses finlineage_daily_pipeline, triggers ONE fresh DAG run, polls
    it to a terminal state, and returns {"dag_run_id", "state"} for the rest
    of the suite to inspect. Session-scoped so every test in one pytest
    invocation checks the SAME run rather than each triggering its own
    (which would multiply Gemini quota usage by the number of test
    functions instead of using it once)."""
    requests.patch(
        f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}",
        json={"is_paused": False}, auth=AIRFLOW_AUTH, timeout=10,
    ).raise_for_status()

    run_id = f"e2e_test_{uuid.uuid4().hex[:12]}"
    trigger_resp = requests.post(
        f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns",
        json={"dag_run_id": run_id}, auth=AIRFLOW_AUTH, timeout=10,
    )
    trigger_resp.raise_for_status()

    print(f"\n  Triggered DAG run {run_id} — polling for completion (timeout {DAG_RUN_TIMEOUT_S}s) ...")
    deadline = time.time() + DAG_RUN_TIMEOUT_S
    state = "running"
    while time.time() < deadline:
        r = requests.get(
            f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns/{run_id}",
            auth=AIRFLOW_AUTH, timeout=10,
        )
        r.raise_for_status()
        state = r.json()["state"]
        if state in ("success", "failed"):
            break
        time.sleep(POLL_INTERVAL_S)
    else:
        pytest.fail(
            f"DAG run {run_id} did not reach a terminal state within "
            f"{DAG_RUN_TIMEOUT_S}s (last state: {state})"
        )

    print(f"  DAG run {run_id} finished with overall state: {state} "
          "(an overall 'failed' state is EXPECTED if an LLM task hit its Gemini/Groq "
          "quota — see the per-task assertions in test_e2e_pipeline.py, not this fixture)")

    return {"dag_run_id": run_id, "state": state}


def get_task_state(dag_run_id: str, task_id: str) -> str:
    r = requests.get(
        f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns/{dag_run_id}/taskInstances/{task_id}",
        auth=AIRFLOW_AUTH, timeout=10,
    )
    r.raise_for_status()
    return r.json()["state"]


def get_task_log(dag_run_id: str, task_id: str) -> str:
    """Best-effort log fetch across the retry attempts default_args allows
    (retries=1 -> up to 2 tries). Used only to classify WHY a LLM-dependent
    or external-service task failed — never to gate the deterministic
    tasks, which are asserted on state alone."""
    log_text = ""
    for try_number in (1, 2):
        r = requests.get(
            f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}",
            auth=AIRFLOW_AUTH, timeout=10, headers={"Accept": "text/plain"},
        )
        if r.status_code == 200:
            log_text += r.text
    return log_text


def failed_for_known_reason(dag_run_id: str, task_id: str) -> bool:
    return bool(KNOWN_RATE_LIMIT_PATTERNS.search(get_task_log(dag_run_id, task_id)))


@pytest.fixture(scope="session")
def duckdb_conn(dag_run):
    """Read-only connection to the SAME data/finlineage.duckdb the DAG run
    just wrote to — valid because docker-compose.yml mounts ./data straight
    through to the container (see Section 10 of the knowledge base), so the
    host and container see the identical file. Opened only after dag_run
    has reached a terminal state, so no task still holds it open."""
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    yield conn
    conn.close()


# =============================================================================
# Wk16/17 — unit-test fixtures (no Docker/Airflow required)
#
# Everything below supports the NEW unit-test files (test_ge_gates.py,
# test_ai1_pnl_narrative.py, test_ai5_anomaly_generator.py, etc.) added to
# actually instrument pytest-cov, rather than estimating coverage. These are
# deliberately independent of the docker_stack/dag_run fixtures above — they
# run against a disposable COPY of the real data/finlineage.duckdb (never the
# file itself, so a test can never corrupt the dev/demo database), and don't
# need Docker, Airflow, or a live Gemini/Groq key at all.
# =============================================================================

import shutil


@pytest.fixture()
def writable_db(tmp_path):
    """A fresh, disposable copy of the real finlineage.duckdb that a test is
    free to INSERT into (ai1's _write_snippet, ai5's run_ai5, ai6's
    write_results all open their DB_PATH read-write). Function-scoped so
    every test gets a clean copy unaffected by any other test's writes.

    Keeps the exact filename "finlineage.duckdb" (just under a fresh tmp_path
    directory) — several of the mart views embed fully-qualified
    `finlineage.main.<table>` references, and duckdb derives that catalog
    name from the file's own basename, so renaming the copy breaks those
    lookups with a "Catalog does not exist" error (confirmed while building
    this fixture)."""
    dst = tmp_path / "finlineage.duckdb"
    shutil.copyfile(DB_PATH, dst)
    return dst


@pytest.fixture()
def full_view_db(tmp_path):
    """A disposable duckdb copy where the three base staging views
    (stg_erp_gl_entries / stg_fx_rates / stg_payroll_entries) are repointed
    at the LOCAL staged bronze CSVs, instead of the Windows-absolute path
    baked into them at dbt-build time on the participant's machine.

    Why this is needed: dbt configures staging/intermediate models as VIEWS
    (see run_in_airflow_container()'s docstring above) whose stored SQL calls
    read_csv_auto('<the exact path bronze lived at when dbt built this file>').
    That is a real, already-documented cross-environment limitation (it's why
    the E2E suite above runs its own reconciliation check INSIDE the Airflow
    container rather than from the host). A host-side pytest run — in CI, in
    this sandbox, or on a teammate's machine with a different checkout path —
    hits the identical problem on any check that reads through
    int_entries_cost_centre_mapped, which is exactly what GE Gate G4's first
    two checks do. Repointing the 3 leaf views at a real, locally-reachable
    CSV directory is a standard test-fixture technique for this class of
    "view remembers an absolute build-time path" problem — it changes ONLY
    the disposable copy, never data/finlineage.duckdb itself, and every
    downstream view/table (int_entries_fx_converted,
    int_entries_cost_centre_mapped, all 5 marts) still runs its own real SQL
    unchanged on top of that swapped-in source. Keeps the filename
    "finlineage.duckdb" for the same catalog-name reason documented in
    writable_db() above — under a dedicated subdirectory so it never
    collides with writable_db()'s own copy in the same test."""
    sub = tmp_path / "full_view"
    sub.mkdir()
    dst = sub / "finlineage.duckdb"
    shutil.copyfile(DB_PATH, dst)
    bronze_dir = PROJECT_ROOT / "data" / "bronze"
    conn = duckdb.connect(str(dst), read_only=False)
    for view, csv_name in [
        ("stg_erp_gl_entries", "erp_gl_entries.csv"),
        ("stg_fx_rates", "fx_rates.csv"),
        ("stg_payroll_entries", "payroll_entries.csv"),
    ]:
        old_sql = conn.execute(
            "SELECT sql FROM duckdb_views() WHERE view_name = ?", [view]
        ).fetchone()[0]
        csv_path = str(bronze_dir / csv_name).replace("\\", "/")
        # Replace the read_csv_auto('<anything>') argument with the local path.
        new_sql = re.sub(
            r"read_csv_auto\('[^']*'",
            f"read_csv_auto('{csv_path}'",
            old_sql,
        )
        new_sql = new_sql.replace(f"CREATE VIEW {view} AS", f"CREATE OR REPLACE VIEW {view} AS", 1)
        conn.execute(new_sql)
    conn.close()
    return dst
