"""
AI-1 — P&L Narrative Generation (Gemini 2.5 Flash, via LiteLLM gateway)
Reads mart_pnl_summary from DuckDB, sends it to Gemini, gets back
plain-English management commentary suitable for a board report.

Phase 2, Wk 14: this call now goes through the LiteLLM gateway (Layer 8) instead
of the Gemini SDK directly — see litellm_config.yaml for the "finlineage-narrative"
alias, its Groq fallback, and response caching.
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import duckdb
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "finlineage.duckdb"

LITELLM_BASE_URL = os.getenv("LITELLM_BASE_URL", "http://localhost:4000")
LITELLM_MASTER_KEY = os.getenv("LITELLM_MASTER_KEY", "sk-finlineage-local-dev")


def get_pnl_data():
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    df = conn.execute("""
        SELECT
            period_month,
            segment,
            ROUND(revenue / 10000000, 2)          AS revenue_cr,
            ROUND(cogs / 10000000, 2)              AS cogs_cr,
            ROUND(gross_margin / 10000000, 2)      AS gross_margin_cr,
            gross_margin_pct,
            ROUND(opex / 10000000, 2)              AS opex_cr,
            ROUND(operating_income / 10000000, 2)  AS operating_income_cr,
            operating_margin_pct,
            entry_count
        FROM main.mart_pnl_summary
        ORDER BY period_month, segment
    """).fetchdf()
    conn.close()
    return df


def get_metric_definitions():
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    df = conn.execute("""
        SELECT metric_key, metric_name, calculation_logic, unit
        FROM main.mart_metric_dictionary
    """).fetchdf()
    conn.close()
    return df.to_dict(orient="records")


PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a senior financial analyst at a consulting company called Psiog.
You write concise, professional management commentary for board-level financial reviews.

RULES:
- All amounts are in INR Crores (Cr). Always suffix numbers with "Cr".
- Reference specific numbers from the data — never invent figures.
- Highlight material variances (>10% month-over-month swings).
- Cover all three segments: Enterprise, MidMarket, Internal.
- Structure: Executive Summary (3-4 sentences), then Segment Performance (one paragraph each), then Outlook/Risks.
- Keep it under 500 words.
- Use the governed metric definitions provided — do not redefine metrics.

METRIC DEFINITIONS:
{metric_definitions}
"""),
    ("human", """Here is the P&L summary data for Jan–Jun 2026:

{pnl_data}

Write the management commentary for this period.""")
])


def run_ai1():
    print("\n" + "="*60)
    print("  AI-1 — P&L Narrative Generation (Gemini 2.5 Flash)")
    print("="*60 + "\n")

    pnl_df = get_pnl_data()
    metrics = get_metric_definitions()

    print(f"  Data: {len(pnl_df)} rows from mart_pnl_summary")
    print(f"  Metrics: {len(metrics)} governed definitions loaded")
    print(f"  Calling Gemini via LiteLLM gateway ({LITELLM_BASE_URL})...\n")

    llm = ChatOpenAI(
        model="finlineage-narrative",
        base_url=LITELLM_BASE_URL,
        api_key=LITELLM_MASTER_KEY,
        temperature=0.3,
        max_tokens=4096,
    )

    chain = PROMPT | llm | StrOutputParser()

    narrative = chain.invoke({
        "pnl_data": pnl_df.to_string(index=False),
        "metric_definitions": json.dumps(metrics, indent=2),
    })

    print("-"*60)
    print(narrative)
    print("-"*60)

    # Save to file for downstream use (AI-3 judge, Power BI embedding)
    output_path = Path(__file__).resolve().parent.parent / "data" / "gold" / "pnl_narrative.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(narrative, encoding="utf-8")
    print(f"\n  Saved to: {output_path.name}")

    return narrative


if __name__ == "__main__":
    narrative = run_ai1()
    sys.exit(0 if narrative else 1)
