"""
Wk17 — real unit tests for ai_chains/ai1_pnl_narrative.py.

Covers the pure text-processing helpers (_truncate_words,
_split_narrative_and_headline), the 5 DuckDB data-getter functions (run
against the real marts), the snippet-writing path (_write_snippet /
write_headline_snippet, against a disposable db copy), and the best-effort
exception-handling contract of the 4 run_ai1_*_headline() functions — that
contract (never raise, never affect other cards) is exercised by mocking the
LangChain chain rather than making a real Gemini/Groq call.
"""
import sys
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "ai_chains"))

import ai1_pnl_narrative as ai1


# --------------------------------------------------------------- _truncate_words
def test_truncate_words_leaves_short_text_untouched():
    text = "Revenue grew steadily this quarter."
    assert ai1._truncate_words(text, 25) == text


def test_truncate_words_cuts_long_text_and_adds_ellipsis():
    text = " ".join(f"word{i}" for i in range(40))
    result = ai1._truncate_words(text, 25)
    assert result.endswith("…")
    assert len(result.rstrip("…").split()) == 25


def test_truncate_words_strips_trailing_punctuation_before_ellipsis():
    text = " ".join(f"word{i}" for i in range(30)) + ","
    result = ai1._truncate_words(text, 5)
    words = result[:-1].split()
    assert not words[-1].endswith(",")


# --------------------------------------------------------- _split_narrative_and_headline
def test_split_narrative_and_headline_normal_case():
    raw = "Full narrative text here.\n===CARD_HEADLINE===\nRevenue grew 10 Cr this quarter."
    narrative, headline = ai1._split_narrative_and_headline(raw)
    assert narrative == "Full narrative text here."
    assert headline == "Revenue grew 10 Cr this quarter."


def test_split_narrative_and_headline_falls_back_when_delimiter_missing():
    raw = "First sentence stands alone. Second sentence follows."
    narrative, headline = ai1._split_narrative_and_headline(raw)
    assert narrative == raw
    assert headline == "First sentence stands alone."


def test_split_narrative_and_headline_falls_back_when_delimiter_present_but_empty():
    raw = "Only narrative here.\n===CARD_HEADLINE===\n   "
    narrative, headline = ai1._split_narrative_and_headline(raw)
    # An empty headline part means the whole raw text is treated as narrative
    # (the delimiter never gets stripped out), and the fallback headline is
    # derived from that same full text since there's no ". " sentence break.
    assert narrative == raw.strip()
    assert headline.startswith("Only narrative here.")


# --------------------------------------------------------------- data getters (real marts)
def test_get_pnl_data_returns_expected_columns():
    df = ai1.get_pnl_data()
    assert len(df) > 0
    for col in ("period_month", "segment", "revenue_cr", "operating_margin_pct"):
        assert col in df.columns


def test_get_revenue_trends_data_is_aggregated_to_period_and_segment():
    df = ai1.get_revenue_trends_data()
    assert len(df) > 0
    assert list(df.columns) == ["period_month", "segment", "revenue_cr"]
    assert not df.duplicated(subset=["period_month", "segment"]).any()


def test_get_cost_analysis_data_has_expected_grain():
    df = ai1.get_cost_analysis_data()
    assert len(df) > 0
    assert set(df.columns) == {"period_month", "segment", "account_type", "cost_cr"}


def test_get_variance_data_has_expected_columns():
    df = ai1.get_variance_data()
    assert len(df) > 0
    for col in ("revenue_variance_pct", "gm_variance_pct", "oi_variance_pct", "is_material_variance"):
        assert col in df.columns


def test_get_metric_definitions_returns_governed_metrics_as_records():
    metrics = ai1.get_metric_definitions()
    assert isinstance(metrics, list)
    assert len(metrics) > 0
    assert "metric_key" in metrics[0]


# --------------------------------------------------------------- _write_snippet
def test_write_snippet_bounds_and_persists(writable_db, monkeypatch):
    monkeypatch.setattr(ai1, "DB_PATH", writable_db)
    long_headline = " ".join(f"w{i}" for i in range(40))
    bounded = ai1._write_snippet("unit_test_snippet", long_headline)
    assert len(bounded.rstrip("…").split()) == 25

    conn = duckdb.connect(str(writable_db), read_only=True)
    row = conn.execute(
        "SELECT snippet_text, word_count FROM ai_narrative_snippets "
        "WHERE snippet_type = 'unit_test_snippet' ORDER BY generated_at DESC LIMIT 1"
    ).fetchone()
    conn.close()
    assert row[0] == bounded
    assert row[1] == len(bounded.split())


def test_write_headline_snippet_uses_pnl_executive_headline_type(writable_db, monkeypatch, capsys):
    monkeypatch.setattr(ai1, "DB_PATH", writable_db)
    # writable_db is a copy of the REAL, already-populated dev database, and
    # its existing rows carry the participant's own real wall-clock
    # timestamps — which can be later than "now" in this sandbox. Clearing
    # the table first (safe: it's a disposable copy) makes "the one row
    # that's there" unambiguous, instead of racing generated_at against
    # pre-existing history.
    conn = duckdb.connect(str(writable_db), read_only=False)
    conn.execute("DELETE FROM ai_narrative_snippets")
    conn.close()

    ai1.write_headline_snippet("Enterprise led growth at 48 Cr this quarter.")

    conn = duckdb.connect(str(writable_db), read_only=True)
    rows = conn.execute("SELECT snippet_type FROM ai_narrative_snippets").fetchall()
    conn.close()
    assert rows == [("pnl_executive_headline",)]


# ------------------------------------------------ best-effort headline calls (mocked LLM)
class _FakeChain:
    """Stands in for `PROMPT | llm | StrOutputParser()` — supports the same
    `.invoke(dict) -> str` interface the real LangChain pipeline exposes."""
    def __init__(self, response=None, raises=None):
        self._response = response
        self._raises = raises

    def __or__(self, other):
        return self  # tolerate `PROMPT | llm | StrOutputParser()` chaining

    def invoke(self, _inputs):
        if self._raises:
            raise self._raises
        return self._response


@pytest.mark.parametrize("fn_name,snippet_type", [
    ("run_ai1_revenue_headline", "revenue_trends_headline"),
    ("run_ai1_cost_headline", "cost_analysis_headline"),
    ("run_ai1_variance_volatility_headline", "variance_volatility_headline"),
    ("run_ai1_variance_stability_headline", "variance_stability_headline"),
])
def test_best_effort_headline_writes_snippet_on_success(fn_name, snippet_type, writable_db, monkeypatch):
    monkeypatch.setattr(ai1, "DB_PATH", writable_db)
    fake_chain = _FakeChain(response="Enterprise revenue rose to 48.77 Cr this quarter.")
    monkeypatch.setattr(ai1, "ChatOpenAI", lambda **kw: fake_chain)
    # Prompt objects define __or__; patch each relevant PROMPT to return our fake chain directly.
    for prompt_name in (
        "REVENUE_HEADLINE_PROMPT", "COST_HEADLINE_PROMPT",
        "VARIANCE_VOLATILITY_HEADLINE_PROMPT", "VARIANCE_STABILITY_HEADLINE_PROMPT",
    ):
        monkeypatch.setattr(ai1, prompt_name, fake_chain)

    # writable_db already carries the participant's own real historical rows
    # (with real wall-clock timestamps that can run ahead of "now" in this
    # sandbox) — clear this snippet_type first so the row this call writes
    # is unambiguously "the" row, rather than racing generated_at.
    conn = duckdb.connect(str(writable_db), read_only=False)
    conn.execute("DELETE FROM ai_narrative_snippets WHERE snippet_type = ?", [snippet_type])
    conn.close()

    fn = getattr(ai1, fn_name)
    fn()  # should not raise

    conn = duckdb.connect(str(writable_db), read_only=True)
    rows = conn.execute(
        "SELECT snippet_text FROM ai_narrative_snippets WHERE snippet_type = ?", [snippet_type]
    ).fetchall()
    conn.close()
    assert len(rows) == 1
    assert "48.77" in rows[0][0]


@pytest.mark.parametrize("fn_name,snippet_type", [
    ("run_ai1_revenue_headline", "revenue_trends_headline"),
    ("run_ai1_cost_headline", "cost_analysis_headline"),
])
def test_best_effort_headline_never_raises_on_empty_response(fn_name, snippet_type, writable_db, monkeypatch):
    """The exact real-world failure mode this session hit live: Gemini's
    thinking tokens consuming the whole budget and returning an empty
    completion. This must be swallowed, not propagated."""
    monkeypatch.setattr(ai1, "DB_PATH", writable_db)
    fake_chain = _FakeChain(response="")
    for prompt_name in (
        "REVENUE_HEADLINE_PROMPT", "COST_HEADLINE_PROMPT",
        "VARIANCE_VOLATILITY_HEADLINE_PROMPT", "VARIANCE_STABILITY_HEADLINE_PROMPT",
    ):
        monkeypatch.setattr(ai1, prompt_name, fake_chain)
    monkeypatch.setattr(ai1, "ChatOpenAI", lambda **kw: fake_chain)

    conn = duckdb.connect(str(writable_db), read_only=False)
    before = conn.execute(
        "SELECT COUNT(*) FROM ai_narrative_snippets WHERE snippet_type = ?", [snippet_type]
    ).fetchone()[0]
    conn.close()

    fn = getattr(ai1, fn_name)
    fn()  # must not raise despite the empty completion

    conn = duckdb.connect(str(writable_db), read_only=True)
    after = conn.execute(
        "SELECT COUNT(*) FROM ai_narrative_snippets WHERE snippet_type = ?", [snippet_type]
    ).fetchone()[0]
    conn.close()
    assert after == before  # nothing new written — the card just keeps its last headline


@pytest.mark.parametrize("fn_name", [
    "run_ai1_revenue_headline", "run_ai1_cost_headline",
    "run_ai1_variance_volatility_headline", "run_ai1_variance_stability_headline",
])
def test_best_effort_headline_never_raises_on_network_exception(fn_name, writable_db, monkeypatch):
    monkeypatch.setattr(ai1, "DB_PATH", writable_db)
    fake_chain = _FakeChain(raises=ConnectionError("gateway unreachable"))
    for prompt_name in (
        "REVENUE_HEADLINE_PROMPT", "COST_HEADLINE_PROMPT",
        "VARIANCE_VOLATILITY_HEADLINE_PROMPT", "VARIANCE_STABILITY_HEADLINE_PROMPT",
    ):
        monkeypatch.setattr(ai1, prompt_name, fake_chain)
    monkeypatch.setattr(ai1, "ChatOpenAI", lambda **kw: fake_chain)

    fn = getattr(ai1, fn_name)
    fn()  # must not raise — caught and logged as a warning per the module's own contract
