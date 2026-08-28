-- ai_review_queue: HITL review queue for AI-3 v2's per-claim confidence scoring
-- (Layer 9, Section 7.5). One row per (narrative run, claim). Executed as
-- CREATE TABLE IF NOT EXISTS by ai3_narrative_judge_v2.py on every run, so it's
-- safe to also run manually against data/finlineage.duckdb.

CREATE TABLE IF NOT EXISTS ai_review_queue (
    review_id                  VARCHAR NOT NULL,   -- uuid4, generated at insert time
    run_at                     TIMESTAMP NOT NULL, -- when AI-3 v2 scored this claim
    source_chain                VARCHAR NOT NULL DEFAULT 'ai1_pnl_narrative',
    claim_text                  VARCHAR NOT NULL,

    -- Three confidence signals (0.0-1.0), see Section 7.4 for the weighting.
    self_reported_score         DOUBLE,   -- weight 0.20 — model's own stated confidence
    second_judge_score          DOUBLE,   -- weight 0.35 — independent Gemini call vs mart data
    retrieval_grounding_score   DOUBLE,   -- weight 0.45 — deterministic regex match vs actual values
    confidence_score            DOUBLE NOT NULL,   -- weighted composite of the three above

    -- Routing decision made by AI-3 v2 at score time.
    -- One of: auto_accept | human_review | rejected
    routing_band                VARCHAR NOT NULL,

    -- Reviewer decision — NULL until a human acts via review_ui/review_app.py.
    -- One of: approved | edited | rejected
    reviewer_verdict            VARCHAR,
    -- Fixed reason-code list (not free text) — see Section 7.5:
    -- matches_mart_data | stale_snapshot | rounding_discrepancy |
    -- wrong_segment_attribution | unsupported_by_retrieval | hallucinated_metric | other
    reviewer_reason_code        VARCHAR,
    reviewer_edited_text        VARCHAR,   -- populated only when reviewer_verdict = 'edited'
    reviewed_at                 TIMESTAMP,
    reviewed_by                 VARCHAR,

    PRIMARY KEY (review_id)
);

-- Not yet scheduled (Section 11, "Not yet built"): manual query for threshold
-- auto-tuning. Compares each routing_band's reviewer-confirmed accuracy so
-- AUTO_ACCEPT_THRESHOLD / HUMAN_REVIEW_THRESHOLD in ai3_narrative_judge_v2.py
-- can be re-tuned against real evidence instead of a code change made on a hunch.
--
-- SELECT
--     routing_band,
--     count(*)                                                          AS total_reviewed,
--     sum(case when reviewer_verdict = 'approved' then 1 else 0 end)    AS approved,
--     sum(case when reviewer_verdict = 'edited'   then 1 else 0 end)    AS edited,
--     sum(case when reviewer_verdict = 'rejected' then 1 else 0 end)    AS rejected,
--     round(100.0 * sum(case when reviewer_verdict = 'approved' then 1 else 0 end)
--           / count(*), 1)                                              AS approval_rate_pct
-- FROM ai_review_queue
-- WHERE reviewer_verdict IS NOT NULL
-- GROUP BY routing_band
-- ORDER BY routing_band;
