"""
Wk17 — real unit tests for AI-4's agent LOOP (run_ai4()), with the LangChain
LLM mocked so the loop's control flow (tool-call dispatch, iteration cap,
wall-clock timeout, final-answer handling) is exercised without a real
Gemini/Groq call. The 3 tools themselves are tested for real (against the
actual marts) in test_ai4_variance_tools.py.
"""
import sys
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ai_chains"))

import ai4_variance_agent as ai4


class _FakeToolCallResponse:
    def __init__(self, tool_calls=None, content=""):
        self.tool_calls = tool_calls or []
        self.content = content


class _FakeBoundLLM:
    """Returns a scripted sequence of responses, one per .invoke() call —
    mirrors a real bind_tools() model deciding to call a tool, then stopping."""
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    def invoke(self, _messages):
        resp = self._responses[min(self.calls, len(self._responses) - 1)]
        self.calls += 1
        return resp


def _patch_llm(monkeypatch, fake_bound_llm):
    fake_unbound = SimpleNamespace(bind_tools=lambda tools: fake_bound_llm)
    monkeypatch.setattr(ai4, "ChatOpenAI", lambda **kw: fake_unbound)


def test_run_ai4_calls_a_tool_then_returns_final_answer(tmp_path, monkeypatch):
    monkeypatch.setattr(ai4, "OUTPUT_PATH", tmp_path / "variance_explanation.md")
    tool_call_resp = _FakeToolCallResponse(tool_calls=[
        {"name": "get_variance_breakdown", "args": {}, "id": "call_1"}
    ])
    final_resp = _FakeToolCallResponse(content="Enterprise OI swung due to higher COGS in March.")
    _patch_llm(monkeypatch, _FakeBoundLLM([tool_call_resp, final_resp]))

    result = ai4.run_ai4()

    assert result == "Enterprise OI swung due to higher COGS in March."
    assert ai4.OUTPUT_PATH.exists()
    assert "Enterprise OI swung" in ai4.OUTPUT_PATH.read_text(encoding="utf-8")


def test_run_ai4_returns_final_answer_immediately_with_no_tool_calls(tmp_path, monkeypatch):
    monkeypatch.setattr(ai4, "OUTPUT_PATH", tmp_path / "variance_explanation.md")
    final_resp = _FakeToolCallResponse(content="No material variances this period.")
    _patch_llm(monkeypatch, _FakeBoundLLM([final_resp]))

    result = ai4.run_ai4()
    assert result == "No material variances this period."


def test_run_ai4_handles_unknown_tool_name_gracefully(tmp_path, monkeypatch):
    monkeypatch.setattr(ai4, "OUTPUT_PATH", tmp_path / "variance_explanation.md")
    bad_tool_resp = _FakeToolCallResponse(tool_calls=[
        {"name": "not_a_real_tool", "args": {}, "id": "call_1"}
    ])
    final_resp = _FakeToolCallResponse(content="Handled gracefully.")
    _patch_llm(monkeypatch, _FakeBoundLLM([bad_tool_resp, final_resp]))

    result = ai4.run_ai4()
    assert result == "Handled gracefully."


def test_run_ai4_hits_iteration_cap_and_returns_guardrail_message(tmp_path, monkeypatch):
    monkeypatch.setattr(ai4, "OUTPUT_PATH", tmp_path / "variance_explanation.md")
    # Always returns a tool call, never a final answer -> exhausts MAX_TOOL_ITERATIONS.
    looping_resp = _FakeToolCallResponse(tool_calls=[
        {"name": "get_metric_definition", "args": {"metric_key": "revenue"}, "id": "call_x"}
    ])
    _patch_llm(monkeypatch, _FakeBoundLLM([looping_resp]))

    result = ai4.run_ai4()
    assert "stopped before producing a final explanation" in result


def test_run_ai4_respects_wallclock_timeout(tmp_path, monkeypatch):
    monkeypatch.setattr(ai4, "OUTPUT_PATH", tmp_path / "variance_explanation.md")
    monkeypatch.setattr(ai4, "WALLCLOCK_TIMEOUT_SECONDS", 0)  # expire immediately

    looping_resp = _FakeToolCallResponse(tool_calls=[
        {"name": "get_metric_definition", "args": {"metric_key": "revenue"}, "id": "call_x"}
    ])
    fake_llm = _FakeBoundLLM([looping_resp])
    _patch_llm(monkeypatch, fake_llm)

    result = ai4.run_ai4()
    assert "stopped before producing a final explanation" in result
    assert fake_llm.calls == 0  # timeout check happens before the first model call


def test_run_ai4_passes_period_and_segment_into_the_user_ask(tmp_path, monkeypatch):
    monkeypatch.setattr(ai4, "OUTPUT_PATH", tmp_path / "variance_explanation.md")
    final_resp = _FakeToolCallResponse(content="done")
    _patch_llm(monkeypatch, _FakeBoundLLM([final_resp]))

    ai4.run_ai4(period_month="2026-03-01", segment="Enterprise")
    stamp = ai4.OUTPUT_PATH.read_text(encoding="utf-8")
    assert "2026-03-01" in stamp
    assert "Enterprise" in stamp
