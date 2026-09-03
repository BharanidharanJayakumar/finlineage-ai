"""
AI-1 — P&L Narrative Generation (Gemini 2.5 Flash, via LiteLLM gateway)
Reads mart_pnl_summary from DuckDB, sends it to Gemini, gets back
plain-English management commentary suitable for a board report.

Phase 2, Wk 14: this call now goes through the LiteLLM gateway (Layer 8) instead
of the Gemini SDK directly — see litellm_config.yaml for the "finlineage-narrative"
alias, its Groq fallback, and response caching.

Phase 2, Wk 15 (added 2026-09-03): the SAME call also produces a short,
card-sized headline for Power BI, via a delimiter in the prompt
(HEADLINE_DELIMITER below) rather than a second LLM call — a second per-run
Gemini call is exactly the kind of thing that exhausted the free-tier daily
quota live on 2026-09-02 (see knowledge base). The headline is bounded to
MAX_HEADLINE_WORDS in Python regardless of what the model returns, then
written to the ai_narrative_snippets DuckDB table (see
ai_narrative_snippets_schema.sql) alongside the existing full-narrative file
write — that table is what scripts/sync_gold_to_databricks.py pushes to
Databricks so Power BI can bind a card to it. This is the first increment of
that feature (snippet_type='pnl_executive_headline', for the P&L Summary
page); more snippet_types get added one BI card at a time.
"""

import os
import sys
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import duckdb
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "finlineage.duckdb"
SNIPPET_SCHEMA_PATH = Path(__file__).resolve().parent.parent / "ai_narrative_snippets_schema.sql"

LITELLM_BASE_URL = os.getenv("LITELLM_BASE_URL", "http://localhost:4000")
LITELLM_MASTER_KEY = os.getenv("LITELLM_MASTER_KEY", "sk-finlineage-local-dev")
LITELLM_MODEL_ALIAS = "finlineage-narrative"

# The model is asked to put this exact line between the full narrative and
# the short card headline (see PROMPT below), so a single response can be
# split into both outputs with no second LLM call.
HEADLINE_DELIMITER = "===CARD_HEADLINE==="
MAX_HEADLINE_WORDS = 25


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

AFTER the full commentary above, on its own new line, write exactly the text
"===CARD_HEADLINE===" (no markdown, nothing else on that line). Then, on the
line after that, write ONE short headline sentence — at most 25 words, no
markdown formatting — summarizing the single most important takeaway of this
period for a dashboard card (not the full report). Reference at least one
specific number from the data, still in INR Cr.

METRIC DEFINITIONS:
{metric_definitions}
"""),
    ("human", """Here is the P&L summary data for Jan–Jun 2026:

{pnl_data}

Write the management commentary for this period.""")
])


def _split_narrative_and_headline(raw: str) -> tuple:
    """Splits one LLM response into (full_narrative, card_headline).

    Deliberately tolerant of the model not following the delimiter
    instruction exactly (LLM output isn't guaranteed) — falls back to the
    narrative's own first sentence rather than failing the whole AI-1 run
    over a missing card headline."""
    if HEADLINE_DELIMITER in raw:
        narrative_part, _, headline_part = raw.partition(HEADLINE_DELIMITER)
        narrative = narrative_part.strip()
        headline = headline_part.strip()
        if headline:
            return narrative, headline
    print("  WARNING: '===CARD_HEADLINE===' not found (or empty) in the LLM "
          "response — falling back to the narrative's first sentence for the card headline.")
    narrative = raw.strip()
    first_sentence = narrative.split(". ")[0].strip().rstrip(".") + "."
    return narrative, first_sentence


def _truncate_words(text: str, max_words: int) -> str:
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]).rstrip(".,;:") + "…"


def write_headline_snippet(headline: str):
    """Bounds the headline to MAX_HEADLINE_WORDS and appends it to the
    ai_narrative_snippets DuckDB table (append-only, same pattern as
    doc_drift_log / _pipeline_sync_log — Power BI reads the LATEST row per
    snippet_type, see docs/power_bi_dax_measures.md)."""
    bounded = _truncate_words(headline, MAX_HEADLINE_WORDS)
    conn = duckdb.connect(str(DB_PATH), read_only=False)
    conn.execute(SNIPPET_SCHEMA_PATH.read_text(encoding="utf-8"))
    conn.execute("""
        INSERT INTO ai_narrative_snippets (
            snippet_id, generated_at, snippet_type, snippet_text, word_count, source_model
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, [
        str(uuid.uuid4()),
        datetime.now(timezone.utc),
        "pnl_executive_headline",
        bounded,
        len(bounded.split()),
        LITELLM_MODEL_ALIAS,
    ])
    conn.close()
    print(f"\n  Card headline ({len(bounded.split())} words): {bounded}")
    print("  Saved to: ai_narrative_snippets (snippet_type='pnl_executive_headline')")


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
        model=LITELLM_MODEL_ALIAS,
        base_url=LITELLM_BASE_URL,
        api_key=LITELLM_MASTER_KEY,
        temperature=0.3,
        max_tokens=4096,
    )

    chain = PROMPT | llm | StrOutputParser()

    raw_output = chain.invoke({
        "pnl_data": pnl_df.to_string(index=False),
        "metric_definitions": json.dumps(metrics, indent=2),
    })

    narrative, headline = _split_narrative_and_headline(raw_output)

    print("-"*60)
    print(narrative)
    print("-"*60)

    # Save to file for downstream use (AI-3 judge, Power BI embedding).
    # narrative here is already split from the card headline above, so this
    # file's content is unchanged from before this feature — AI-3's
    # extract_claims() keeps parsing exactly what it always parsed.
    output_path = Path(__file__).resolve().parent.parent / "data" / "gold" / "pnl_narrative.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(narrative, encoding="utf-8")
    print(f"\n  Saved to: {output_path.name}")

    write_headline_snippet(headline)

    return narrative


if __name__ == "__main__":
    narrative = run_ai1()
    sys.exit(0 if narrative else 1)
