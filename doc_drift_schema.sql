-- doc_drift_log: AI-6 doc-drift detection (Phase 2, Wk 14 roadmap item, added
-- 2026-09-02). One row per (drift check run, dbt model). Executed as
-- CREATE TABLE IF NOT EXISTS by ai6_doc_drift_detector.py on every run, so
-- it's safe to also run manually against data/finlineage.duckdb.
--
-- Why this exists: AI-2 (ai2_transformation_docs.py) regenerates
-- data/gold/transformation_docs.md from the CURRENT dbt SQL every time it
-- successfully runs — so in principle the docs can never go stale. In
-- practice AI-2 can fail or be skipped on a given DAG run (e.g. an LLM
-- provider outage or, as actually happened on 2026-09-02, a free-tier quota
-- exhausted mid-testing) while dbt models keep changing regardless — nothing
-- else in the pipeline notices when that happens, so the last-committed docs
-- silently drift out of sync with the SQL they're supposed to describe. AI-6
-- is a deterministic (no LLM call, ₹0) fingerprint comparison that catches
-- exactly that gap, independent of whether AI-2 ran successfully this time.

CREATE TABLE IF NOT EXISTS doc_drift_log (
    check_id                VARCHAR NOT NULL,   -- uuid4, generated at insert time
    checked_at              TIMESTAMP NOT NULL, -- when ai6_doc_drift_detector.py ran this check
    model_name              VARCHAR NOT NULL,
    layer                   VARCHAR NOT NULL,   -- staging / intermediate / marts

    -- sha256 of the model's current .sql file content vs. the hash recorded
    -- by AI-2 the last time it successfully generated docs for this model.
    -- current_sql_hash is nullable too, not just documented_sql_hash: a
    -- 'doc_entry_orphaned' row (model removed from disk, stale manifest
    -- entry) legitimately has no current file to hash — caught by testing
    -- this script against a synthetic orphaned-entry case before shipping.
    current_sql_hash        VARCHAR,
    documented_sql_hash     VARCHAR,            -- NULL if this model has never been documented

    is_drifted               BOOLEAN NOT NULL,
    -- One of: in_sync | sql_changed_since_docs | never_documented |
    -- doc_entry_orphaned (model was documented before but no longer exists on disk)
    drift_reason             VARCHAR NOT NULL,

    PRIMARY KEY (check_id)
);

-- Manual query: which models are drifted right now, as of the latest check run.
--
-- WITH latest AS (
--     SELECT model_name, max(checked_at) AS latest_checked_at
--     FROM doc_drift_log GROUP BY model_name
-- )
-- SELECT d.model_name, d.layer, d.drift_reason, d.checked_at
-- FROM doc_drift_log d
-- JOIN latest l ON d.model_name = l.model_name AND d.checked_at = l.latest_checked_at
-- WHERE d.is_drifted
-- ORDER BY d.model_name;
