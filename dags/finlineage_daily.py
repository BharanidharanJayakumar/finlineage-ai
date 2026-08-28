"""
FinLineage AI — Daily Pipeline DAG
Orchestrates the full E2E flow:
  1. GE pre-gates (G1, G2, G3) — block if raw data is bad
  2. dbt build (seeds + models + tests)
  3. GE post-gates (G4, G5) — block if aggregations don't reconcile
  4. AI narrative generation (Gemini, via LiteLLM gateway)
  5. AI transformation docs (Groq, via LiteLLM gateway)
  6. AI variance explanation agent (Gemini, tool-calling, via LiteLLM gateway) — Wk 14
  7. AI-as-judge confidence scoring + HITL routing (Gemini, via LiteLLM gateway) — v2, Wk 14

Phase 2, Wk 14: ai3 now runs ai3_narrative_judge_v2.py (confidence-scored HITL
router) instead of v1's single PASS/FAIL verdict, and ai4 (variance agent) runs
in parallel with ai1/ai2 since it only depends on the post-dbt marts, not on the
narrative. All AI tasks call Gemini/Groq through the litellm-gateway service
(Layer 8) rather than the provider SDKs directly.
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_ROOT}/finlineage"
VENV_PYTHON = f"{PROJECT_ROOT}/.venv/bin/python"

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

    # -- Step 3: dbt build (seeds + all models + all tests) --
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build --profiles-dir {DBT_DIR}",
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
    dbt_build >> [gate_g4, gate_g5]

    # AI chains after post-gates pass — ai1/ai2/ai4 only need the marts, so they
    # fan out in parallel rather than chaining.
    [gate_g4, gate_g5] >> ai_narrative
    [gate_g4, gate_g5] >> ai_docs
    [gate_g4, gate_g5] >> ai_variance

    # Judge (v2) runs after the narrative it's scoring exists.
    ai_narrative >> ai_judge
