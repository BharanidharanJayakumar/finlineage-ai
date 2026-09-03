"""
Wk17 — real unit tests for ai_chains/ai1_pnl_narrative.py's run_ai1() (the
main P&L narrative + first headline card), mocking only the outer LLM call —
_split_narrative_and_headline, write_headline_snippet, and the 4 best-effort
follow-on calls all run for real underneath it (already unit-tested
individually in test_ai1_pnl_narrative.py).
"""
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ai_chains"))

import ai1_pnl_narrative as ai1


class _FakeChain:
    def __init__(self, response):
        self._response = response

    def __or__(self, other):
        return self

    def invoke(self, _inputs):
        return self._response


@pytest.fixture()
def silence_best_effort_cards(monkeypatch):
    """run_ai1() always calls the 4 best-effort card functions too — stub
    them out as no-ops so this test file stays focused on the main
    narrative/headline path (each of those 4 already has its own dedicated
    test coverage in test_ai1_pnl_narrative.py)."""
    for fn in (
        "run_ai1_revenue_headline", "run_ai1_cost_headline",
        "run_ai1_variance_volatility_headline", "run_ai1_variance_stability_headline",
    ):
        monkeypatch.setattr(ai1, fn, lambda: None)


def _clear_snippets(db_path, snippet_type):
    # writable_db carries the participant's own real historical rows (real
    # wall-clock timestamps that can run ahead of "now" in this sandbox) —
    # clear this type first so "the newest row" isn't a pre-existing one.
    conn = duckdb.connect(str(db_path), read_only=False)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS ai_narrative_snippets ("
        "snippet_id VARCHAR, generated_at TIMESTAMP, snippet_type VARCHAR, "
        "snippet_text VARCHAR, word_count INTEGER, source_model VARCHAR)"
    )
    conn.execute("DELETE FROM ai_narrative_snippets WHERE snippet_type = ?", [snippet_type])
    conn.close()


def test_run_ai1_splits_narrative_and_writes_pnl_headline(writable_db, tmp_path, monkeypatch, silence_best_effort_cards):
    monkeypatch.setattr(ai1, "DB_PATH", writable_db)
    _clear_snippets(writable_db, "pnl_executive_headline")
    raw_response = (
        "Enterprise led growth this period.\n"
        "===CARD_HEADLINE===\n"
        "Enterprise revenue reached 48.77 Cr, the strongest segment this quarter."
    )
    monkeypatch.setattr(ai1, "PROMPT", _FakeChain(raw_response))
    monkeypatch.setattr(ai1, "ChatOpenAI", lambda **kw: _FakeChain(raw_response))

    gold_dir = tmp_path / "gold"
    monkeypatch.setattr(
        ai1, "__file__", str(tmp_path / "fake_pkg" / "ai1_pnl_narrative.py")
    )

    narrative = ai1.run_ai1()

    assert narrative == "Enterprise led growth this period."

    saved_path = (tmp_path / "fake_pkg" / ".." / "data" / "gold" / "pnl_narrative.md").resolve()
    assert saved_path.exists()
    assert saved_path.read_text(encoding="utf-8") == narrative

    conn = duckdb.connect(str(writable_db), read_only=True)
    row = conn.execute(
        "SELECT snippet_text FROM ai_narrative_snippets "
        "WHERE snippet_type = 'pnl_executive_headline' ORDER BY generated_at DESC LIMIT 1"
    ).fetchone()
    conn.close()
    assert "48.77" in row[0]


def test_run_ai1_falls_back_to_first_sentence_when_delimiter_missing(writable_db, tmp_path, monkeypatch, silence_best_effort_cards):
    monkeypatch.setattr(ai1, "DB_PATH", writable_db)
    _clear_snippets(writable_db, "pnl_executive_headline")
    raw_response = "Revenue grew steadily. Costs stayed flat."
    monkeypatch.setattr(ai1, "PROMPT", _FakeChain(raw_response))
    monkeypatch.setattr(ai1, "ChatOpenAI", lambda **kw: _FakeChain(raw_response))
    monkeypatch.setattr(ai1, "__file__", str(tmp_path / "fake_pkg" / "ai1_pnl_narrative.py"))

    narrative = ai1.run_ai1()
    assert narrative == raw_response

    conn = duckdb.connect(str(writable_db), read_only=True)
    row = conn.execute(
        "SELECT snippet_text FROM ai_narrative_snippets "
        "WHERE snippet_type = 'pnl_executive_headline' ORDER BY generated_at DESC LIMIT 1"
    ).fetchone()
    conn.close()
    assert row[0] == "Revenue grew steadily."
