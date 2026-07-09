"""
AI-3 — AI-as-Judge: Narrative Consistency Validation (Gemini 2.5 Flash)
Reads the AI-generated P&L narrative and the actual mart data, then uses
Gemini to check whether every claim in the narrative is supported by the data.

This is a PIPELINE GATE — if the narrative contradicts the data, it blocks
publication to Power BI.
"""

import sys
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import duckdb
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "finlineage.duckdb"
NARRATIVE_PATH = Path(__file__).resolve().parent.parent / "data" / "gold" / "pnl_narrative.md"


def get_pnl_data():
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    df = conn.execute("""
        SELECT
            period_month, segment,
            ROUND(revenue / 10000000, 2) AS revenue_cr,
            ROUND(gross_margin / 10000000, 2) AS gross_margin_cr,
            gross_margin_pct,
            ROUND(operating_income / 10000000, 2) AS operating_income_cr,
            operating_margin_pct
        FROM main.mart_pnl_summary
        ORDER BY period_month, segment
    """).fetchdf()
    conn.close()
    return df


JUDGE_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a financial data quality auditor. Your job is to verify that
an AI-generated financial narrative is CONSISTENT with the actual data.

For each numerical claim in the narrative, check it against the data table provided.
A claim is CONSISTENT if the number matches the data (within rounding tolerance of ±0.05 Cr or ±1 percentage point).
A claim is INCONSISTENT if the number contradicts the data.
A claim is UNVERIFIABLE if it references a metric not present in the data.

Output a JSON object with this exact structure:
{{
    "verdict": "PASS" or "FAIL",
    "total_claims": <int>,
    "consistent": <int>,
    "inconsistent": <int>,
    "unverifiable": <int>,
    "issues": [
        {{
            "claim": "the exact text from the narrative",
            "expected": "what the data shows",
            "status": "INCONSISTENT or UNVERIFIABLE"
        }}
    ]
}}

If there are zero INCONSISTENT claims, verdict is PASS.
If there is even one INCONSISTENT claim, verdict is FAIL.
UNVERIFIABLE claims do not cause a FAIL but should be flagged.

Output ONLY the JSON — no markdown fences, no preamble."""),
    ("human", """ACTUAL DATA (from mart_pnl_summary):
{pnl_data}

AI-GENERATED NARRATIVE:
{narrative}

Verify every numerical claim. Return the JSON verdict.""")
])


def run_ai3():
    print("\n" + "="*60)
    print("  AI-3 — Narrative Consistency Judge (Gemini 2.5 Flash)")
    print("="*60 + "\n")

    if not NARRATIVE_PATH.exists():
        print("  ERROR: No narrative file found. Run AI-1 first.")
        return False

    narrative = NARRATIVE_PATH.read_text(encoding="utf-8")
    pnl_df = get_pnl_data()

    print(f"  Narrative: {len(narrative)} chars")
    print(f"  Data: {len(pnl_df)} rows from mart_pnl_summary")
    print(f"  Calling Gemini judge...\n")

    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        temperature=0.0,
        max_output_tokens=2000,
    )

    chain = JUDGE_PROMPT | llm | StrOutputParser()

    raw_result = chain.invoke({
        "pnl_data": pnl_df.to_string(index=False),
        "narrative": narrative,
    })

    # Parse JSON
    try:
        clean = raw_result.replace("```json", "").replace("```", "").strip()
        result = json.loads(clean)
    except json.JSONDecodeError:
        print(f"  WARNING: Could not parse judge response as JSON.")
        print(f"  Raw response:\n{raw_result}")
        return False

    # Report
    verdict = result.get("verdict", "UNKNOWN")
    total = result.get("total_claims", 0)
    consistent = result.get("consistent", 0)
    inconsistent = result.get("inconsistent", 0)
    unverifiable = result.get("unverifiable", 0)
    issues = result.get("issues", [])

    print(f"  Claims checked: {total}")
    print(f"  Consistent:     {consistent}")
    print(f"  Inconsistent:   {inconsistent}")
    print(f"  Unverifiable:   {unverifiable}")
    print(f"\n  Verdict: {verdict} {'✓' if verdict == 'PASS' else '✗ — narrative blocked from publication'}")

    if issues:
        print(f"\n  Issues found:")
        for issue in issues:
            print(f"    [{issue.get('status')}] \"{issue.get('claim', '')[:80]}...\"")
            print(f"      Expected: {issue.get('expected', 'N/A')}")

    # Save verdict
    verdict_path = Path(__file__).resolve().parent.parent / "data" / "gold" / "narrative_verdict.json"
    verdict_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"\n  Verdict saved to: {verdict_path.name}")

    print(f"\n{'='*60}\n")
    return verdict == "PASS"


if __name__ == "__main__":
    ok = run_ai3()
    sys.exit(0 if ok else 1)