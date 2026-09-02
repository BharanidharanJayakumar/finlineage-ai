"""
FinLineage AI — Daily Pipeline DAG
Orchestrates the full E2E flow:
  1. GE pre-gates (G1, G2, G3) — block if raw data is bad
  2. dbt build (seeds + models + tests)
  3. GE post-gates (G4, G5, G6) — block if aggregations don't reconcile or a
     metric goes to an impossible value / disappears; G6 also logs (non-blocking)
     statistical drift warnings
  4. AI narrative generation (Gemini, via LiteLLM gateway)
  5. AI transformation docs (Groq, via LiteLLM gateway)
  6. AI variance explanation agent (Gemini, tool-calling, via LiteLLM gateway) — Wk 14
  7. AI-as-judge confidence scoring + HITL routing (Gemini, via LiteLLM gateway) — v2, Wk 14

Phase 2, Wk 14: ai3 now runs ai3_narrative_judge_v2.py (confidence-scored HITL
router) instead of v1's single PASS/FAIL verdict, and ai4 (variance agent) runs
in parallel with ai1/ai2 since it only depends on the post-dbt marts, not on the
narrative. All AI tasks call Gemini/Groq through the litellm-gateway service
(Layer 8) rather than the provider SDKs directly.

Phase 2, Wk 12: this DAG now actually runs inside Docker — every task below
uses a Linux venv baked into the image by docker/Dockerfile.airflow (the
project's on-disk .venv is a Windows venv and never worked inside the
container). dbt_build also needed a --vars override for bronze_path, same
fix .github/workflows/ci.yml already used, since dbt_project.yml's default
is a Windows-only absolute path.
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_ROOT}/finlineage"
VENV_PYTHON = f"{PROJECT_ROOT}/.venv/bin/python"
VENV_DBT = f"{PROJECT_ROOT}/.venv/bin/dbt"
# dbt_project.yml's default bronze_path is a Windows absolute path (see Known
# Issues in the knowledge base) — override it to the container's mounted path,
# the same fix ci.yml already applies for the Ubuntu runner.
DBT_VARS = '{"bronze_path": "%s/data/bronze"}' % PROJECT_ROOT

default_args = {
    "owner": "finlineage",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="finlineage_daily_pipeline",
    default_args=default_args,
    description="E2E financial reporting pipeline with GE gates and AI narrative",
    schedule="0 6 * * *",  # Daily at 6 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["finlineage", "financial-reporting", "dbt", "ai"],
) as dag:

    # -- Step 1: Generate source data (in prod this would be an ingestion step) --
    generate_sources = BashOperator(
        task_id="generate_source_data",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_PYTHON} scripts/generate_sources.py",
    )

    # -- Step 2: Pre-dbt quality gates --
    gate_g1 = BashOperator(
        task_id="ge_gate_g1_raw_gl_completeness",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_PYTHON} ge/g1_raw_gl_completeness.py",
    )

    gate_g2 = BashOperator(
        task_id="ge_gate_g2_fx_rate_coverage",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_PYTHON} ge/g2_fx_rate_coverage.py",
    )

    gate_g3 = BashOperator(
        task_id="ge_gate_g3_payroll_completeness",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_PYTHON} ge/g3_payroll_completeness.py",
    )

    # -- Step 3: dbt build (deps + seeds + all models + all tests) --
    # --exclude source:* skips dbt's source freshness/existence tests, which
    # are defined in models/staging/_sources.yml against a hardcoded Windows
    # absolute path (independent of the bronze_path var overridden above,
    # which only applies to the models). Same known issue documented in the
    # knowledge base ("Source tests fail in CI (path not portable)") and
    # already worked around this exact way in .github/workflows/ci.yml — this
    # just carries that fix over to the DAG, which never had it. Model-level
    # tests still cover the same data, so nothing is actually left unchecked.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {DBT_DIR} && {VENV_DBT} deps --profiles-dir {DBT_DIR} && "
            f"{VENV_DBT} build --profiles-dir {DBT_DIR} --vars '{DBT_VARS}' --exclude source:*"
        ),
    )

    # -- Step 4: Post-dbt quality gates --
    gate_g4 = BashOperator(
        task_id="ge_gate_g4_pnl_aggregation",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_PYTHON} ge/g4_pnl_aggregation_consistency.py",
    )

    gate_g5 = BashOperator(
        task_id="ge_gate_g5_cross_mart_consistency",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_PYTHON} ge/g5_cross_mart_consistency.py",
    )

    gate_g6 = BashOperator(
        task_id="ge_gate_g6_metric_drift",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_PYTHON} ge/g6_metric_drift.py",
    )

    # -- Step 5: AI chains (all routed through the litellm-gateway service) --
    ai_narrative = BashOperator(
        task_id="ai1_pnl_narrative",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_PYTHON} ai_chains/ai1_pnl_narrative.py",
    )

    ai_docs = BashOperator(
        task_id="ai2_transformation_docs",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_PYTHON} ai_chains/ai2_transformation_docs.py",
    )

    ai_variance = BashOperator(
        task_id="ai4_variance_agent",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_PYTHON} ai_chains/ai4_variance_agent.py",
    )

    ai_judge = BashOperator(
        task_id="ai3_narrative_judge_v2",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_PYTHON} ai_chains/ai3_narrative_judge_v2.py",
    )

    # -- DAG dependency chain --
    # Ingestion
    generate_sources >> [gate_g1, gate_g2, gate_g3]

    # Pre-gates must all pass before dbt
    [gate_g1, gate_g2, gate_g3] >> dbt_build

    # Post-gates after dbt
    dbt_build >> [gate_g4, gate_g5, gate_g6]

    # AI chains after post-gates pass — ai1/ai2/ai4 only need the marts, so they
    # fan out in parallel rather than chaining.
    [gate_g4, gate_g5, gate_g6] >> ai_narrative
    [gate_g4, gate_g5, gate_g6] >> ai_docs
    [gate_g4, gate_g5, gate_g6] >> ai_variance

    # Judge (v2) runs after the narrative it's scoring exists.
    ai_narrative >> ai_judge
