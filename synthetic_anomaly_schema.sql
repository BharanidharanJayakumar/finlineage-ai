-- synthetic_anomaly_log: AI-5 synthetic anomaly generator (Phase 2, Wk 15
-- roadmap item, added 2026-09-02). One row per (test run, anomaly type).
-- Executed as CREATE TABLE IF NOT EXISTS by ai5_anomaly_generator.py on
-- every run, so it's safe to also run manually against data/finlineage.duckdb.
--
-- What this proves: rather than trusting the G1/G2/G3 pre-dbt gates catch bad
-- data, AI-5 deterministically injects known-bad mutations into an ISOLATED
-- COPY of the bronze CSVs (never data/bronze/ itself) and re-runs the real
-- gate functions against that copy to check whether each mutation actually
-- gets caught. This is the same evidence pattern AI-3 already established at
-- mid-term for the AI narrative judge ("caught 3/30 inconsistencies, proving
-- the gate works") — applied here to the deterministic GE gates instead.

CREATE TABLE IF NOT EXISTS synthetic_anomaly_log (
    test_id                  VARCHAR NOT NULL,   -- uuid4, generated at insert time
    run_at                   TIMESTAMP NOT NULL,

    anomaly_type              VARCHAR NOT NULL,   -- e.g. duplicate_entry_id, negative_revenue_amount
    target_file               VARCHAR NOT NULL,   -- which bronze CSV was mutated
    gate_checked               VARCHAR NOT NULL,   -- g1 | g2 | g3
    mutation_detail            VARCHAR,            -- human-readable description of what was changed

    -- What we expect this mutation SHOULD trigger, set by the anomaly's own
    -- definition in ai5_anomaly_generator.py — not the same as was_caught.
    -- A mismatch (expected true, was_caught false) is a real gate gap;
    -- expected_to_be_caught=false entries are DELIBERATE known-gap cases
    -- (e.g. a sign error only G6 catches post-dbt, not G1-G3), included so
    -- this tool demonstrates real coverage boundaries, not just easy wins.
    expected_to_be_caught      BOOLEAN NOT NULL,
    was_caught                 BOOLEAN NOT NULL,   -- did the gate actually fail (i.e. catch it)?

    PRIMARY KEY (test_id)
);

-- Manual query: any anomaly that escaped detection, latest run only.
--
-- WITH latest_run AS (SELECT max(run_at) AS run_at FROM synthetic_anomaly_log)
-- SELECT anomaly_type, target_file, gate_checked, mutation_detail, expected_to_be_caught
-- FROM synthetic_anomaly_log
-- WHERE run_at = (SELECT run_at FROM latest_run) AND NOT was_caught
-- ORDER BY anomaly_type;
