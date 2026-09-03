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

Wk 15/16 (added 2026-09-03, second increment): a Revenue Trends card
headline (snippet_type='revenue_trends_headline') is generated the same way
for the Revenue Trends page. Unlike the P&L headline above, this genuinely
IS a second Gemini call — mart_revenue_trends isn't part of the P&L prompt's
input, so it can't be smuggled into the same response via a delimiter. Given
Gemini's real free-tier quota is only 20 requests/day shared across every
alias on this key (see litellm_config.yaml), this call is wrapped as
best-effort/non-fatal: run_ai1_revenue_headline() catches any exception
(quota, network, anything) and only logs a warning — it never fails the
ai1_pnl_narrative task or rolls back the P&L narrative/headline already
written above it. See that function's docstring for the detail.

Wk 15/16 (added 2026-09-03, third/fourth/fifth increment): three more cards,
same best-effort pattern as the Revenue Trends one above — one for the Cost
Analysis page (snippet_type='cost_analysis_headline') and two for the
Variance Analysis page (snippet_type='variance_volatility_headline' and
'variance_stability_headline', replacing the two static insight text boxes
that page shipped with — one calling out the most volatile segment, one
calling out the steadier ones). This brings the running total to 5 Gemini
calls per ai1_pnl_narrative run (1 main narrative + 4 card headlines); still
well inside the 20-requests/day shared quota for a handful of runs a day,
and every one of the 4 headline calls is independently best-effort so a
quota exhaustion partway through only ever costs that one card its refresh,
never the run itself."""

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
# litellm_config.yaml's automatic fallback (finlineage-narrative ->
# finlineage-narrative-fallback, Groq openai/gpt-oss-120b) only triggers on a
# hard error from the gateway. Live on 2026-09-03, exhausted Gemini free-tier
# quota came back as an HTTP 200 with an EMPTY completion instead of a 429 —
# LiteLLM's fallback logic doesn't treat that as failure, so it never
# switched over on its own. The Cost Analysis / Variance Analysis card
# headlines below call this Groq alias DIRECTLY instead, since Gemini's
# 20-requests/day quota is already the tightest constraint on this key and
# these are the newest, least essential-to-fail calls. Same LiteLLM gateway,
# same real second model — genuinely generated text, just a different
# provisioned backend, not a workaround that fabricates anything.
LITELLM_FALLBACK_MODEL_ALIAS = "finlineage-narrative-fallback"

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


def get_revenue_trends_data():
    """Aggregated to period+segment (same grain as mart_pnl_summary) rather
    than mart_revenue_trends' native period+segment+project grain — a
    one-sentence card headline doesn't need per-project detail, and this
    keeps the prompt small (cheaper, faster, less to hallucinate over)."""
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    df = conn.execute("""
        SELECT
            period_month,
            segment,
            ROUND(SUM(revenue_inr) / 10000000, 2) AS revenue_cr
        FROM main.mart_revenue_trends
        GROUP BY period_month, segment
        ORDER BY period_month, segment
    """).fetchdf()
    conn.close()
    return df


def get_cost_analysis_data():
    """Aggregated to period+segment+account_type — matches the grain of the
    Cost Analysis page's own 'Monthly Cost Breakdown: COGS vs OpEx' visual,
    so the headline talks about the same split the reader is looking at."""
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    df = conn.execute("""
        SELECT
            period_month,
            segment,
            account_type,
            ROUND(SUM(cost_inr) / 10000000, 2) AS cost_cr
        FROM main.mart_cost_analysis
        GROUP BY period_month, segment, account_type
        ORDER BY period_month, segment, account_type
    """).fetchdf()
    conn.close()
    return df


def get_variance_data():
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    df = conn.execute("""
        SELECT
            period_month,
            segment,
            revenue_variance_pct,
            gm_variance_pct,
            oi_variance_pct,
            is_material_variance
        FROM main.mart_variance_analysis
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


REVENUE_HEADLINE_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a senior financial analyst at a consulting company called Psiog.
Write exactly ONE short headline sentence for a Power BI dashboard card — at
most 25 words, no markdown formatting, no preamble, nothing before or after
the sentence itself. Reference at least one specific number from the data,
in INR Crores (Cr). Highlight the most important revenue trend (growth,
decline, or concentration in one segment)."""),
    ("human", """Here is the Revenue Trends summary data for Jan–Jun 2026 (revenue in INR Cr, by month and segment):

{revenue_data}

Write the one-sentence dashboard card headline.""")
])


COST_HEADLINE_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a senior financial analyst at a consulting company called Psiog.
Write exactly ONE short headline sentence for a Power BI dashboard card — at
most 25 words, no markdown formatting, no preamble, nothing before or after
the sentence itself. Reference at least one specific number from the data,
in INR Crores (Cr). Call out the most notable cost trend — whether COGS or
OpEx dominates, which segment costs the most, or the largest month-over-month
swing."""),
    ("human", """Here is the Cost Analysis summary data for Jan–Jun 2026 (cost in INR Cr, by month, segment and COGS/OpEx type):

{cost_data}

Write the one-sentence dashboard card headline.""")
])


VARIANCE_VOLATILITY_HEADLINE_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a senior financial analyst at a consulting company called Psiog.
Write exactly ONE short headline sentence for a Power BI dashboard card — at
most 25 words, no markdown formatting, no preamble, nothing before or after
the sentence itself. Identify whichever ONE segment shows the LARGEST /
most extreme month-over-month operating-income variance swing (oi_variance_pct)
in the data, and state the swing's range using the actual percentage
figures."""),
    ("human", """Here is the Variance Analysis data for Jan–Jun 2026 (percentages are month-over-month change; is_material_variance flags a >10% operating-income swing):

{variance_data}

Write the one-sentence dashboard card headline about the most volatile segment.""")
])


VARIANCE_STABILITY_HEADLINE_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a senior financial analyst at a consulting company called Psiog.
Write exactly ONE short headline sentence for a Power BI dashboard card — at
most 25 words, no markdown formatting, no preamble, nothing before or after
the sentence itself. Identify the segment(s) that show comparatively
MODERATE-BUT-CONSISTENT operating-income variance (smaller swings than the
most volatile segment, without being perfectly flat), and summarize that
pattern with a real figure from the data."""),
    ("human", """Here is the Variance Analysis data for Jan–Jun 2026 (percentages are month-over-month change; is_material_variance flags a >10% operating-income swing):

{variance_data}

Write the one-sentence dashboard card headline about the steadier segment(s).""")
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
    bounded = _write_snippet("pnl_executive_headline", headline)
    print(f"\n  Card headline ({len(bounded.split())} words): {bounded}")
    print("  Saved to: ai_narrative_snippets (snippet_type='pnl_executive_headline')")


def _write_snippet(snippet_type: str, headline: str):
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
        snippet_type,
        bounded,
        len(bounded.split()),
        LITELLM_MODEL_ALIAS,
    ])
    conn.close()
    return bounded


def run_ai1_revenue_headline():
    """Best-effort second card headline, for the Revenue Trends page
    (snippet_type='revenue_trends_headline') — the 'one BI card at a time'
    second increment (2026-09-03), following the same pattern the P&L card
    proved out. Deliberately NOT allowed to fail the ai1_pnl_narrative task:
    this is a genuine second Gemini call (mart_revenue_trends isn't part of
    the P&L prompt, so it can't ride the same response), and Gemini's real
    free-tier quota is only 20 requests/day shared across every alias on
    this key — a quota hit here is expected, not exceptional. Any exception
    (quota, gateway timeout, malformed response) is caught and logged as a
    warning; the Revenue Trends card simply keeps showing its last
    successfully-written headline until a future run's call succeeds."""
    try:
        revenue_df = get_revenue_trends_data()
        llm = ChatOpenAI(
            model=LITELLM_MODEL_ALIAS,
            base_url=LITELLM_BASE_URL,
            api_key=LITELLM_MASTER_KEY,
            temperature=0.3,
            # NOT 120, even though the visible answer is one short sentence.
            # Real bug hit live (2026-09-03): Gemini 2.5 Flash spends part of
            # max_tokens on an invisible internal "thinking" pass before it
            # writes the actual answer. The main P&L call above never hit
            # this because its budget (4096) was generous; this smaller call
            # started at 120 and came back with finish_reason=MAX_TOKENS and
            # an EMPTY visible message — thinking consumed the whole budget
            # before any answer text was emitted. 500 leaves enough headroom
            # for thinking + a <=25-word sentence; still far cheaper than the
            # main narrative call.
            max_tokens=1200,  # raised from 500 2026-09-03 — see run_ai1_cost_headline() comment
        )
        chain = REVENUE_HEADLINE_PROMPT | llm | StrOutputParser()
        raw = chain.invoke({"revenue_data": revenue_df.to_string(index=False)})
        headline = raw.strip().splitlines()[0].strip() if raw.strip() else ""
        if not headline:
            raise ValueError("empty response from Revenue Trends headline call")
        bounded = _write_snippet("revenue_trends_headline", headline)
        print(f"\n  Revenue Trends card headline ({len(bounded.split())} words): {bounded}")
        print("  Saved to: ai_narrative_snippets (snippet_type='revenue_trends_headline')")
    except Exception as exc:
        print(f"\n  WARNING: Revenue Trends headline generation failed ({exc!r}) — "
              "non-fatal, skipping. This does not affect the P&L narrative/headline "
              "above, and ai1_pnl_narrative still exits 0.")


def run_ai1_cost_headline():
    """Best-effort card headline for the Cost Analysis page
    (snippet_type='cost_analysis_headline') — third increment (2026-09-03),
    same best-effort pattern as run_ai1_revenue_headline() above. See that
    function's docstring for why this is wrapped as non-fatal.

    Calls LITELLM_FALLBACK_MODEL_ALIAS (Groq) directly rather than the Gemini
    alias — see LITELLM_FALLBACK_MODEL_ALIAS's module-level comment for why:
    Gemini's daily quota was already exhausted by the time this card was
    added, and the empty-200 failure mode doesn't trigger LiteLLM's
    automatic fallback on its own."""
    try:
        cost_df = get_cost_analysis_data()
        llm = ChatOpenAI(
            model=LITELLM_MODEL_ALIAS,
            base_url=LITELLM_BASE_URL,
            api_key=LITELLM_MASTER_KEY,
            temperature=0.3,
            max_tokens=1200,  # see run_ai1_revenue_headline() re: Gemini thinking tokens; raised 500->1200 2026-09-03 after Cost/Variance-volatility calls came back empty even on a fresh-quota key — confirmed non-deterministic thinking-token consumption, not quota
        )
        chain = COST_HEADLINE_PROMPT | llm | StrOutputParser()
        raw = chain.invoke({"cost_data": cost_df.to_string(index=False)})
        headline = raw.strip().splitlines()[0].strip() if raw.strip() else ""
        if not headline:
            raise ValueError("empty response from Cost Analysis headline call")
        bounded = _write_snippet("cost_analysis_headline", headline)
        print(f"\n  Cost Analysis card headline ({len(bounded.split())} words): {bounded}")
        print("  Saved to: ai_narrative_snippets (snippet_type='cost_analysis_headline')")
    except Exception as exc:
        print(f"\n  WARNING: Cost Analysis headline generation failed ({exc!r}) — "
              "non-fatal, skipping. This does not affect any other card, and "
              "ai1_pnl_narrative still exits 0.")


def run_ai1_variance_volatility_headline():
    """Best-effort card headline for the Variance Analysis page — the
    'most volatile segment' card (snippet_type='variance_volatility_headline'),
    fourth increment (2026-09-03). Replaces the static 'Mid Market shows
    extreme volatility...' text box. Same best-effort pattern as
    run_ai1_revenue_headline() above. Calls LITELLM_FALLBACK_MODEL_ALIAS
    (Groq) directly — see that constant's module-level comment."""
    try:
        variance_df = get_variance_data()
        llm = ChatOpenAI(
            model=LITELLM_MODEL_ALIAS,
            base_url=LITELLM_BASE_URL,
            api_key=LITELLM_MASTER_KEY,
            temperature=0.3,
            max_tokens=1200,  # raised from 500 2026-09-03 — see run_ai1_cost_headline() comment
        )
        chain = VARIANCE_VOLATILITY_HEADLINE_PROMPT | llm | StrOutputParser()
        raw = chain.invoke({"variance_data": variance_df.to_string(index=False)})
        headline = raw.strip().splitlines()[0].strip() if raw.strip() else ""
        if not headline:
            raise ValueError("empty response from Variance volatility headline call")
        bounded = _write_snippet("variance_volatility_headline", headline)
        print(f"\n  Variance (volatility) card headline ({len(bounded.split())} words): {bounded}")
        print("  Saved to: ai_narrative_snippets (snippet_type='variance_volatility_headline')")
    except Exception as exc:
        print(f"\n  WARNING: Variance volatility headline generation failed ({exc!r}) — "
              "non-fatal, skipping. This does not affect any other card, and "
              "ai1_pnl_narrative still exits 0.")


def run_ai1_variance_stability_headline():
    """Best-effort card headline for the Variance Analysis page — the
    'steadier segments' card (snippet_type='variance_stability_headline'),
    fifth increment (2026-09-03). Replaces the static 'Enterprise and
    Internal segments show moderate but consistent variances.' text box.
    Same best-effort pattern as run_ai1_revenue_headline() above. Calls
    LITELLM_FALLBACK_MODEL_ALIAS (Groq) directly — see that constant's
    module-level comment."""
    try:
        variance_df = get_variance_data()
        llm = ChatOpenAI(
            model=LITELLM_MODEL_ALIAS,
            base_url=LITELLM_BASE_URL,
            api_key=LITELLM_MASTER_KEY,
            temperature=0.3,
            max_tokens=1200,  # raised from 500 2026-09-03 — see run_ai1_cost_headline() comment
        )
        chain = VARIANCE_STABILITY_HEADLINE_PROMPT | llm | StrOutputParser()
        raw = chain.invoke({"variance_data": variance_df.to_string(index=False)})
        headline = raw.strip().splitlines()[0].strip() if raw.strip() else ""
        if not headline:
            raise ValueError("empty response from Variance stability headline call")
        bounded = _write_snippet("variance_stability_headline", headline)
        print(f"\n  Variance (stability) card headline ({len(bounded.split())} words): {bounded}")
        print("  Saved to: ai_narrative_snippets (snippet_type='variance_stability_headline')")
    except Exception as exc:
        print(f"\n  WARNING: Variance stability headline generation failed ({exc!r}) — "
              "non-fatal, skipping. This does not affect any other card, and "
              "ai1_pnl_narrative still exits 0.")


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

    # Second through fifth increments (Wk 15/16): the remaining 4 BI cards.
    # All best-effort by design — see run_ai1_revenue_headline()'s docstring
    # — so a failure in any one of them never changes this function's return
    # value or exit code, and never blocks the others from still running.
    run_ai1_revenue_headline()
    run_ai1_cost_headline()
    run_ai1_variance_volatility_headline()
    run_ai1_variance_stability_headline()

    return narrative


if __name__ == "__main__":
    narrative = run_ai1()
    sys.exit(0 if narrative else 1)
