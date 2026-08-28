"""
review_ui/review_app.py — HITL reviewer for AI-3 v2's ai_review_queue (Layer 9).
Phase 2, Wk 14.

Run with:  streamlit run review_ui/review_app.py

Shows one pending claim at a time (routing_band = 'human_review', not yet
reviewed) with its three confidence signals. The reviewer approves, edits and
saves, or rejects with a fixed reason code. The sidebar shows pending-queue
counts per band and, once decisions accumulate, band-vs-verdict accuracy — the
evidence the thresholds in ai3_narrative_judge_v2.py should eventually be
re-tuned against.
"""

from pathlib import Path
from datetime import datetime, timezone

import duckdb
import streamlit as st

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "finlineage.duckdb"

# Fixed list (not free text) — must match review_queue_schema.sql's comment.
REASON_CODES = [
    "matches_mart_data",
    "stale_snapshot",
    "rounding_discrepancy",
    "wrong_segment_attribution",
    "unsupported_by_retrieval",
    "hallucinated_metric",
    "other",
]

st.set_page_config(page_title="FinLineage AI — Claim Review", layout="centered")


def query_df(sql, params=None):
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return conn.execute(sql, params or []).fetchdf()
    finally:
        conn.close()


def execute(sql, params=None):
    # Opened and closed per action (not held open across reruns) to avoid
    # contending with dbt build / AI-3 v2 writes if they run around the same time.
    conn = duckdb.connect(str(DB_PATH), read_only=False)
    try:
        conn.execute(sql, params or [])
    finally:
        conn.close()


def table_exists() -> bool:
    df = query_df("""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = 'ai_review_queue'
    """)
    return not df.empty


def render_sidebar():
    st.sidebar.header("Queue status")
    counts = query_df("""
        SELECT routing_band,
               sum(case when reviewer_verdict is null then 1 else 0 end) as pending,
               count(*) as total
        FROM ai_review_queue
        GROUP BY routing_band
        ORDER BY routing_band
    """)
    if counts.empty:
        st.sidebar.caption("No claims scored yet. Run ai3_narrative_judge_v2.py first.")
    else:
        for _, row in counts.iterrows():
            st.sidebar.metric(
                f"{row['routing_band']} pending",
                int(row["pending"]),
                help=f"{int(row['total'])} total in this band",
            )

    st.sidebar.divider()
    st.sidebar.subheader("Band vs. reviewer accuracy")
    accuracy = query_df("""
        SELECT routing_band,
               count(*) as reviewed,
               round(100.0 * sum(case when reviewer_verdict = 'approved' then 1 else 0 end)
                     / count(*), 1) as approval_rate_pct
        FROM ai_review_queue
        WHERE reviewer_verdict IS NOT NULL
        GROUP BY routing_band
        ORDER BY routing_band
    """)
    if accuracy.empty:
        st.sidebar.caption("No reviewer decisions yet — this fills in as you review claims.")
    else:
        st.sidebar.dataframe(accuracy, hide_index=True, use_container_width=True)


def main():
    st.title("FinLineage AI — Claim Review Queue")
    st.caption(
        "One flagged claim at a time. Your decision is stored as labeled training "
        "data for tuning AI-3's auto-accept / reject thresholds."
    )

    if not table_exists():
        st.warning("ai_review_queue doesn't exist yet. Run `python ai_chains/ai3_narrative_judge_v2.py` first.")
        return

    render_sidebar()

    pending = query_df("""
        SELECT review_id, claim_text, self_reported_score, second_judge_score,
               retrieval_grounding_score, confidence_score, run_at
        FROM ai_review_queue
        WHERE routing_band = 'human_review' AND reviewer_verdict IS NULL
        ORDER BY run_at
        LIMIT 1
    """)

    if pending.empty:
        st.success("No claims waiting for review.")
        return

    row = pending.iloc[0]
    st.subheader("Claim")
    st.write(row["claim_text"])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Self-reported", f"{row['self_reported_score']:.2f}")
    c2.metric("Second judge", f"{row['second_judge_score']:.2f}")
    c3.metric("Retrieval grounding", f"{row['retrieval_grounding_score']:.2f}")
    c4.metric("Composite", f"{row['confidence_score']:.2f}")

    edited_text = st.text_area(
        "Edited text (only used if you click Save edit)",
        value=row["claim_text"],
        height=100,
    )

    col_approve, col_edit, col_reject = st.columns(3)

    if col_approve.button("Approve", type="primary", use_container_width=True):
        execute("""
            UPDATE ai_review_queue
            SET reviewer_verdict = 'approved',
                reviewer_reason_code = 'matches_mart_data',
                reviewed_at = ?,
                reviewed_by = ?
            WHERE review_id = ?
        """, [datetime.now(timezone.utc), "reviewer", row["review_id"]])
        st.rerun()

    if col_edit.button("Save edit", use_container_width=True):
        execute("""
            UPDATE ai_review_queue
            SET reviewer_verdict = 'edited',
                reviewer_edited_text = ?,
                reviewed_at = ?,
                reviewed_by = ?
            WHERE review_id = ?
        """, [edited_text, datetime.now(timezone.utc), "reviewer", row["review_id"]])
        st.rerun()

    reason = st.selectbox(
        "Reject reason (used only if you click Reject)",
        REASON_CODES,
        index=REASON_CODES.index("unsupported_by_retrieval"),
    )
    if col_reject.button("Reject", use_container_width=True):
        execute("""
            UPDATE ai_review_queue
            SET reviewer_verdict = 'rejected',
                reviewer_reason_code = ?,
                reviewed_at = ?,
                reviewed_by = ?
            WHERE review_id = ?
        """, [reason, datetime.now(timezone.utc), "reviewer", row["review_id"]])
        st.rerun()


if __name__ == "__main__":
    main()
