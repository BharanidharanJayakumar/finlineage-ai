"""
Wk17 — real unit tests for AI-3 v2's score_claim()/run_ai3_v2() orchestration
(ai_chains/ai3_narrative_judge_v2.py), with the two LLM signals (self-report,
second judge) mocked out. retrieval_grounding_score() itself runs for real —
it's deterministic and already the most heavily-weighted signal (0.45).
"""
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ai_chains"))

import ai3_narrative_judge_v2 as ai3v2


class _FakeChain:
    def __init__(self, response):
        self._response = response

    def __or__(self, other):
        return self

    def invoke(self, _inputs):
        return self._response


def test_score_claim_auto_accepts_when_all_signals_agree(monkeypatch):
    monkeypatch.setattr(ai3v2, "SELF_REPORT_PROMPT", _FakeChain('{"supported": true, "confidence": 0.95}'))
    monkeypatch.setattr(ai3v2, "SECOND_JUDGE_PROMPT", _FakeChain('{"supported": true, "confidence": 0.95}'))
    monkeypatch.setattr(ai3v2, "_gateway_llm", lambda **kw: _FakeChain("unused"))

    pnl_df = ai3v2.get_pnl_data()
    row = pnl_df.iloc[0]
    claim = f"Revenue reached {row['revenue_cr']} Cr in the period."

    result = ai3v2.score_claim(claim, pnl_df, pnl_df.to_string(index=False))
    assert result["routing_band"] == "auto_accept"
    assert result["confidence_score"] >= ai3v2.AUTO_ACCEPT_THRESHOLD


def test_score_claim_rejects_when_all_signals_disagree(monkeypatch):
    monkeypatch.setattr(ai3v2, "SELF_REPORT_PROMPT", _FakeChain('{"supported": false, "confidence": 0.9}'))
    monkeypatch.setattr(ai3v2, "SECOND_JUDGE_PROMPT", _FakeChain('{"supported": false, "confidence": 0.9}'))
    monkeypatch.setattr(ai3v2, "_gateway_llm", lambda **kw: _FakeChain("unused"))

    pnl_df = ai3v2.get_pnl_data()
    claim = "Revenue reached 999999 Cr in the period."  # grounds to 0.0 too

    result = ai3v2.score_claim(claim, pnl_df, pnl_df.to_string(index=False))
    assert result["routing_band"] == "rejected"
    assert result["confidence_score"] < ai3v2.HUMAN_REVIEW_THRESHOLD


def test_score_claim_routes_to_human_review_in_the_middle_band(monkeypatch):
    # self: not supported @0.9 -> score 0.1; judge: supported @0.6 -> score 0.6;
    # grounding: neutral (no figure) -> 0.5
    # composite = 0.20*0.1 + 0.35*0.6 + 0.45*0.5 = 0.02+0.21+0.225 = 0.455 -> rejected actually
    # Use values that land squarely in the human_review band instead.
    monkeypatch.setattr(ai3v2, "SELF_REPORT_PROMPT", _FakeChain('{"supported": true, "confidence": 0.7}'))
    monkeypatch.setattr(ai3v2, "SECOND_JUDGE_PROMPT", _FakeChain('{"supported": true, "confidence": 0.7}'))
    monkeypatch.setattr(ai3v2, "_gateway_llm", lambda **kw: _FakeChain("unused"))

    pnl_df = ai3v2.get_pnl_data()
    claim = "Overall performance was reasonable this period."  # no figure -> grounding 0.5

    result = ai3v2.score_claim(claim, pnl_df, pnl_df.to_string(index=False))
    expected = 0.20 * 0.7 + 0.35 * 0.7 + 0.45 * 0.5
    assert abs(result["confidence_score"] - round(expected, 3)) < 0.01
    assert result["routing_band"] == "human_review"


def test_run_ai3_v2_writes_scored_claims_to_review_queue(writable_db, tmp_path, monkeypatch):
    monkeypatch.setattr(ai3v2, "DB_PATH", writable_db)
    # Ground the claim in whatever figure is ACTUALLY in this db copy (baseline
    # or scaled) rather than a hardcoded "48.77 Cr" — retrieval_grounding_score()
    # checks the claimed number against the real mart_pnl_summary rows, so a
    # hardcoded figure silently breaks the moment the real data changes (e.g.
    # after running scripts/scale_test.py without restoring the baseline).
    pnl_df = ai3v2.get_pnl_data()
    row = pnl_df.iloc[0]
    narrative_path = tmp_path / "pnl_narrative.md"
    narrative_path.write_text(
        f"Overall performance was strong. Revenue reached {row['revenue_cr']} Cr in the period.",
        encoding="utf-8",
    )
    monkeypatch.setattr(ai3v2, "NARRATIVE_PATH", narrative_path)
    monkeypatch.setattr(ai3v2, "SELF_REPORT_PROMPT", _FakeChain('{"supported": true, "confidence": 0.9}'))
    monkeypatch.setattr(ai3v2, "SECOND_JUDGE_PROMPT", _FakeChain('{"supported": true, "confidence": 0.9}'))

    ok = ai3v2.run_ai3_v2()
    assert ok is True  # no rejected claims in this scripted scenario

    conn = duckdb.connect(str(writable_db), read_only=True)
    count = conn.execute(
        "SELECT COUNT(*) FROM ai_review_queue WHERE source_chain = 'ai1_pnl_narrative'"
    ).fetchone()[0]
    conn.close()
    assert count >= 1


def test_run_ai3_v2_fails_fast_without_a_narrative_file(tmp_path, monkeypatch):
    monkeypatch.setattr(ai3v2, "NARRATIVE_PATH", tmp_path / "does_not_exist.md")
    assert ai3v2.run_ai3_v2() is False


def test_run_ai3_v2_returns_false_when_any_claim_is_rejected(writable_db, tmp_path, monkeypatch):
    monkeypatch.setattr(ai3v2, "DB_PATH", writable_db)
    narrative_path = tmp_path / "pnl_narrative.md"
    narrative_path.write_text("Revenue reached 999999 Cr in April.", encoding="utf-8")
    monkeypatch.setattr(ai3v2, "NARRATIVE_PATH", narrative_path)
    monkeypatch.setattr(ai3v2, "SELF_REPORT_PROMPT", _FakeChain('{"supported": false, "confidence": 0.9}'))
    monkeypatch.setattr(ai3v2, "SECOND_JUDGE_PROMPT", _FakeChain('{"supported": false, "confidence": 0.9}'))

    assert ai3v2.run_ai3_v2() is False
