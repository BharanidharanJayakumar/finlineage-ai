"""
Wk17 — real unit tests for the superseded-but-still-shipped AI-3 v1
(ai_chains/ai3_narrative_judge.py run_ai3()), mocking the Gemini call. Kept
in the repo (per its own module docstring) mainly so ai3_narrative_judge_v2.py
can reuse extract_claims() — but run_ai3() itself is still real, runnable
code and worth covering.
"""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ai_chains"))

import ai3_narrative_judge as ai3v1


class _FakeChain:
    def __init__(self, response):
        self._response = response

    def __or__(self, other):
        return self

    def invoke(self, _inputs):
        return self._response


def test_run_ai3_fails_fast_without_a_narrative_file(tmp_path, monkeypatch):
    monkeypatch.setattr(ai3v1, "NARRATIVE_PATH", tmp_path / "missing.md")
    assert ai3v1.run_ai3() is False


def test_run_ai3_passes_on_a_clean_pass_verdict(tmp_path, monkeypatch):
    narrative_path = tmp_path / "pnl_narrative.md"
    narrative_path.write_text("Revenue reached 48.77 Cr in April.", encoding="utf-8")
    monkeypatch.setattr(ai3v1, "NARRATIVE_PATH", narrative_path)

    verdict = {
        "verdict": "PASS", "total_claims": 1, "consistent": 1,
        "inconsistent": 0, "unverifiable": 0, "issues": [],
    }
    monkeypatch.setattr(ai3v1, "JUDGE_PROMPT", _FakeChain(json.dumps(verdict)))
    monkeypatch.setattr(ai3v1, "ChatGoogleGenerativeAI", lambda **kw: _FakeChain("unused"))
    monkeypatch.setattr(ai3v1, "__file__", str(tmp_path / "fake_pkg" / "ai3_narrative_judge.py"))
    (tmp_path / "data" / "gold").mkdir(parents=True, exist_ok=True)

    assert ai3v1.run_ai3() is True

    verdict_path = (tmp_path / "fake_pkg" / ".." / "data" / "gold" / "narrative_verdict.json").resolve()
    assert verdict_path.exists()
    assert json.loads(verdict_path.read_text(encoding="utf-8"))["verdict"] == "PASS"


def test_run_ai3_fails_on_an_inconsistent_verdict(tmp_path, monkeypatch):
    narrative_path = tmp_path / "pnl_narrative.md"
    narrative_path.write_text("Revenue reached 999 Cr in April.", encoding="utf-8")
    monkeypatch.setattr(ai3v1, "NARRATIVE_PATH", narrative_path)

    verdict = {
        "verdict": "FAIL", "total_claims": 1, "consistent": 0,
        "inconsistent": 1, "unverifiable": 0,
        "issues": [{"claim": "Revenue reached 999 Cr in April.", "expected": "48.77 Cr", "status": "INCONSISTENT"}],
    }
    monkeypatch.setattr(ai3v1, "JUDGE_PROMPT", _FakeChain(json.dumps(verdict)))
    monkeypatch.setattr(ai3v1, "ChatGoogleGenerativeAI", lambda **kw: _FakeChain("unused"))
    monkeypatch.setattr(ai3v1, "__file__", str(tmp_path / "fake_pkg" / "ai3_narrative_judge.py"))
    (tmp_path / "data" / "gold").mkdir(parents=True, exist_ok=True)

    assert ai3v1.run_ai3() is False


def test_run_ai3_returns_false_on_unparseable_judge_response(tmp_path, monkeypatch, capsys):
    narrative_path = tmp_path / "pnl_narrative.md"
    narrative_path.write_text("Revenue reached 48.77 Cr in April.", encoding="utf-8")
    monkeypatch.setattr(ai3v1, "NARRATIVE_PATH", narrative_path)
    monkeypatch.setattr(ai3v1, "JUDGE_PROMPT", _FakeChain("not valid json at all"))
    monkeypatch.setattr(ai3v1, "ChatGoogleGenerativeAI", lambda **kw: _FakeChain("unused"))

    assert ai3v1.run_ai3() is False
    assert "Could not parse judge response" in capsys.readouterr().out


def test_get_pnl_data_returns_rows():
    df = ai3v1.get_pnl_data()
    assert len(df) > 0
    assert "revenue_cr" in df.columns
