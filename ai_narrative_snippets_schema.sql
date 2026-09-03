-- ai_narrative_snippets: bounded-length text derived from AI-1's P&L
-- narrative, purpose-built to drop straight into Power BI card visuals
-- (added 2026-09-03, first increment of the AI-narrative-to-BI feature).
-- Executed as CREATE TABLE IF NOT EXISTS by ai1_pnl_narrative.py on every
-- run, so it's safe to also run manually against data/finlineage.duckdb.
--
-- Why this exists rather than binding a card directly to
-- data/gold/pnl_narrative.md: that file is the FULL board-report narrative
-- (Executive Summary + one paragraph per segment + Outlook, up to ~500
-- words) — far too long for a dashboard card, and not queryable from
-- Databricks/Power BI at all (it's a markdown file, not a table). AI-1 asks
-- the SAME Gemini call that already produces the full narrative to also
-- emit one short, card-sized headline (see the '===CARD_HEADLINE==='
-- delimiter in ai1_pnl_narrative.py's prompt) — deliberately not a second
-- LLM call, since a second per-run Gemini call is exactly the kind of thing
-- that exhausted the free-tier daily quota live on 2026-09-02 (see
-- knowledge base). A Python-side word-count truncation is applied on top
-- regardless of what the model returns, so a card can never be blown out by
-- the model ignoring the word-limit instruction.
--
-- Append-only, one row per (pipeline run, snippet_type) — same pattern as
-- doc_drift_log and _pipeline_sync_log. Power BI/DAX always reads the LATEST
-- row per snippet_type (see docs/power_bi_dax_measures.md), never averages
-- or sums across runs.

CREATE TABLE IF NOT EXISTS ai_narrative_snippets (
    snippet_id      VARCHAR NOT NULL,   -- uuid4, generated at insert time
    generated_at    TIMESTAMP NOT NULL, -- when ai1_pnl_narrative.py produced this
    -- One value so far: 'pnl_executive_headline' (P&L Summary page card).
    -- More snippet_types get added here one at a time as each new BI card
    -- is wired up — see the Wk 15 note in ai1_pnl_narrative.py's docstring.
    snippet_type    VARCHAR NOT NULL,
    snippet_text    VARCHAR NOT NULL,   -- already word-bounded, safe to bind directly to a card
    word_count      INTEGER NOT NULL,
    source_model    VARCHAR NOT NULL,   -- the LiteLLM alias that generated it, e.g. 'finlineage-narrative'

    PRIMARY KEY (snippet_id)
);

-- Manual query: latest snippet for each snippet_type right now.
--
-- WITH latest AS (
--     SELECT snippet_type, max(generated_at) AS latest_generated_at
--     FROM ai_narrative_snippets GROUP BY snippet_type
-- )
-- SELECT s.snippet_type, s.snippet_text, s.word_count, s.generated_at
-- FROM ai_narrative_snippets s
-- JOIN latest l ON s.snippet_type = l.snippet_type AND s.generated_at = l.latest_generated_at
-- ORDER BY s.snippet_type;
