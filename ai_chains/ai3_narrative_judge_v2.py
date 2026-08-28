"""
AI-3 v2 — Confidence-Scored Judge + HITL Router (Gemini via LiteLLM gateway)
Phase 2, Wk 14 — Layer 9 of the architecture.

Upgrade over v1 (ai3_narrative_judge.py): instead of one PASS/FAIL verdict for the
whole narrative, every individual claim gets a weighted confidence score from three
signals and is routed to auto_accept / human_review / rejected:

    self-reported (0.20)      — the model's own stated confidence
    second judge (0.35)       — an INDEPENDENT Gemini call scoring the claim vs mart data
    retrieval grounding (0.45) — deterministic regex match of the claim's INR/% figures
                                  against the actual values in mart_pnl_summary

Every claim is written as a row to the ai_review_queue DuckDB table (not a single
JSON file), so a human reviewer (review_ui/review_app.py) can act on the ones that
need it — and those decisions become labeled data for tuning the thresholds below
(see review_queue_schema.sql for the accuracy query this feeds).
"""

import os
import re
import sys
import json
import uuid
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import duckdb
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

# Reused helper — see its docstring in ai3_narrative_judge.py. Works because this
# script's own directory (ai_chains/) is on sys.path when run as `python ai_chains/...`.
from ai3_narrative_judge import extract_claims

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "finlineage.duckdb"
NARRATIVE_PATH = Path(__file__).resolve().parent.parent / "data" / "gold" / "pnl_narrative.md"
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "review_queue_schema.sql"

LITELLM_BASE_URL = os.getenv("LITELLM_BASE_URL", "http://localhost:4000")
LITELLM_MASTER_KEY = os.getenv("LITELLM_MASTER_KEY", "sk-finlineage-local-dev")

# Business-owned thresholds (Section 7.5 / 11 of the knowledge base): these start
# strict and are meant to loosen only against real ai_review_queue reviewer
# history, not a code change made on a hunch.
AUTO_ACCEPT_THRESHOLD = 0.85
HUMAN_REVIEW_THRESHOLD = 0.55

# Signal weights (Section 7.4) — must sum to 1.0.
WEIGHT_SELF_REPORTED = 0.20
WEIGHT_SECOND_JUDGE = 0.35
WEIGHT_RETRIEVAL_GROUNDING = 0.45

# Same rounding tolerance as v1's whole-narrative check.
FIGURE_TOLERANCE_ABS = 0.05

# Requires an explicit unit (Cr / % / percent) rather than making it optional —
# an optional suffix pulled in stray numbers (years like "2026", "H1") that have
# nothing to do with a financial figure and would wrongly drag the grounding
# score down. A claim with no INR/% figure at all still falls through to the
# neutral 0.5 case below.
NUMBER_RE = re.compile(r'(\d{1,4}(?:,\d{3})*(?:\.\d+)?)\s*(?:Cr\b|%|percent\b)', re.IGNORECASE)


def _gateway_llm(**kwargs):
    return ChatOpenAI(
        model="finlineage-narrative",
        base_url=LITELLM_BASE_URL,
        api_key=LITELLM_MASTER_KEY,
        **kwargs,
    )


def get_pnl_data():
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    df = conn.execute("""
        SELECT
            period_month, segment,
            ROUND(revenue / 10000000, 2)          AS revenue_cr,
            ROUND(gross_margin / 10000000, 2)      AS gross_margin_cr,
            gross_margin_pct,
            ROUND(operating_income / 10000000, 2)  AS operating_income_cr,
            operating_margin_pct
        FROM main.mart_pnl_summary
        ORDER BY period_month, segment
    """).fetchdf()
    conn.close()
    return df


SELF_REPORT_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are checking a single financial claim against P&L data.
Return ONLY a JSON object: {{"supported": true or false, "confidence": 0.0-1.0}}.
"confidence" is YOUR OWN honest confidence that "supported" is correct — be
calibrated, not just high or low by default."""),
    ("human", """DATA (mart_pnl_summary, INR Cr):
{pnl_data}

CLAIM:
{claim}

Return the JSON only.""")
])

# A separate prompt (not just a second call with the same prompt) so this signal
# is a genuinely independent check, not a repeat of the self-report.
SECOND_JUDGE_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are an INDEPENDENT second reviewer auditing a financial claim
against P&L data — you have not seen any other reviewer's opinion. Be skeptical:
only mark a claim supported if the numbers genuinely match (within ±0.05 Cr or
±1 percentage point). Return ONLY a JSON object:
{{"supported": true or false, "confidence": 0.0-1.0}}."""),
    ("human", """DATA (mart_pnl_summary, INR Cr):
{pnl_data}

CLAIM:
{claim}

Return the JSON only.""")
])


def _parse_score(raw: str):
    try:
        clean = raw.replace("```json", "").replace("```", "").strip()
        obj = json.loads(clean)
        return bool(obj.get("supported", False)), float(obj.get("confidence", 0.0))
    except (json.JSONDecodeError, TypeError, ValueError, AttributeError):
        return False, 0.0


def retrieval_grounding_score(claim: str, pnl_df) -> float:
    """Deterministic check — no LLM call. Extracts numeric figures from the claim
    text and checks each against every value actually present in mart_pnl_summary.
    Weighted highest (0.45) because, unlike the two LLM signals, it can't hallucinate."""
    figures = [float(m.replace(",", "")) for m in NUMBER_RE.findall(claim)]
    if not figures:
        # No numeric figure to ground — neutral score: not a penalty, not a pass.
        return 0.5

    actual_values = []
    for col in ("revenue_cr", "gross_margin_cr", "gross_margin_pct",
                "operating_income_cr", "operating_margin_pct"):
        actual_values.extend(pnl_df[col].dropna().tolist())

    matched = sum(
        1 for fig in figures
        if any(abs(fig - av) <= FIGURE_TOLERANCE_ABS for av in actual_values)
    )
    return matched / len(figures)


def score_claim(claim: str, pnl_df, pnl_data_str: str) -> dict:
    self_llm = _gateway_llm(temperature=0.2, max_tokens=200)
    judge_llm = _gateway_llm(temperature=0.0, max_tokens=200)

    self_chain = SELF_REPORT_PROMPT | self_llm | StrOutputParser()
    judge_chain = SECOND_JUDGE_PROMPT | judge_llm | StrOutputParser()

    self_supported, self_confidence = _parse_score(
        self_chain.invoke({"pnl_data": pnl_data_str, "claim": claim})
    )
    judge_supported, judge_confidence = _parse_score(
        judge_chain.invoke({"pnl_data": pnl_data_str, "claim": claim})
    )

    # A confident "not supported" should score low, not high — fold the
    # supported/not-supported verdict into the directional score.
    self_score = self_confidence if self_supported else (1 - self_confidence)
    judge_score = judge_confidence if judge_supported else (1 - judge_confidence)
    grounding_score = retrieval_grounding_score(claim, pnl_df)

    composite = (
        WEIGHT_SELF_REPORTED * self_score +
        WEIGHT_SECOND_JUDGE * judge_score +
        WEIGHT_RETRIEVAL_GROUNDING * grounding_score
    )

    if composite >= AUTO_ACCEPT_THRESHOLD:
        band = "auto_accept"
    elif composite >= HUMAN_REVIEW_THRESHOLD:
        band = "human_review"
    else:
        band = "rejected"

    return {
        "claim": claim,
        "self_reported_score": round(self_score, 3),
        "second_judge_score": round(judge_score, 3),
        "retrieval_grounding_score": round(grounding_score, 3),
        "confidence_score": round(composite, 3),
        "routing_band": band,
    }


def write_to_review_queue(scored_claims):
    conn = duckdb.connect(str(DB_PATH), read_only=False)
    conn.execute(SCHEMA_PATH.read_text(encoding="utf-8"))
    run_at = datetime.now(timezone.utc)
    for c in scored_claims:
        conn.execute("""
            INSERT INTO ai_review_queue (
                review_id, run_at, source_chain, claim_text,
                self_reported_score, second_judge_score, retrieval_grounding_score,
                confidence_score, routing_band
            ) VALUES (?, ?, 'ai1_pnl_narrative', ?, ?, ?, ?, ?, ?)
        """, [
            str(uuid.uuid4()), run_at, c["claim"],
            c["self_reported_score"], c["second_judge_score"], c["retrieval_grounding_score"],
            c["confidence_score"], c["routing_band"],
        ])
    conn.close()


def run_ai3_v2():
    print("\n" + "="*60)
    print("  AI-3 v2 — Confidence-Scored Judge + HITL Router")
    print("="*60 + "\n")

    if not NARRATIVE_PATH.exists():
        print("  ERROR: No narrative file found. Run AI-1 first.")
        return False

    narrative = NARRATIVE_PATH.read_text(encoding="utf-8")
    pnl_df = get_pnl_data()
    pnl_data_str = pnl_df.to_string(index=False)

    claims = extract_claims(narrative)
    print(f"  Narrative: {len(narrative)} chars, {len(claims)} numeric claims extracted")
    print(f"  Scoring each claim (self-reported + second judge + retrieval grounding)...\n")

    scored = []
    band_counts = {"auto_accept": 0, "human_review": 0, "rejected": 0}
    for i, claim in enumerate(claims, 1):
        result = score_claim(claim, pnl_df, pnl_data_str)
        scored.append(result)
        band_counts[result["routing_band"]] += 1
        print(f"  [{i}/{len(claims)}] {result['routing_band']:>13} "
              f"(score={result['confidence_score']:.2f})  {claim[:70]}")

    write_to_review_queue(scored)

    print(f"\n  auto_accept:   {band_counts['auto_accept']}")
    print(f"  human_review:  {band_counts['human_review']}  <- streamlit run review_ui/review_app.py")
    print(f"  rejected:      {band_counts['rejected']}")
    print(f"\n  {len(scored)} claims written to ai_review_queue in finlineage.duckdb")
    print(f"\n{'='*60}\n")

    # Pipeline gate semantics: block only when something was outright rejected —
    # human_review claims don't fail the DAG, they queue for a reviewer instead.
    return band_counts["rejected"] == 0


if __name__ == "__main__":
    ok = run_ai3_v2()
    sys.exit(0 if ok else 1)
