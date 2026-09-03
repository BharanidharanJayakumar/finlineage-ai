"""
Wk17 — real unit tests for AI-4's three read-only tools
(ai_chains/ai4_variance_agent.py). These are plain LangChain @tool-decorated
functions, which support .invoke(dict) directly without needing an LLM or
agent loop — so the actual SQL/guardrail logic is fully testable here. The
tool-calling AGENT LOOP itself (run_ai4(), which needs a real Gemini call
through the LiteLLM gateway) is exercised by tests/test_e2e_pipeline.py
against the real Docker stack instead.
"""
import json
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "ai_chains"))

import ai4_variance_agent as ai4


def test_get_variance_breakdown_returns_material_variances_first():
    raw = ai4.get_variance_breakdown.invoke({})
    rows = json.loads(raw)
    assert len(rows) > 0
    # is_material_variance DESC ordering: once we hit the first non-material
    # row, every row after it must also be non-material.
    seen_non_material = False
    for row in rows:
        if seen_non_material:
            assert row["is_material_variance"] is False
        if not row["is_material_variance"]:
            seen_non_material = True


def test_get_variance_breakdown_filters_by_segment():
    raw = ai4.get_variance_breakdown.invoke({"segment": "Enterprise"})
    rows = json.loads(raw)
    assert len(rows) > 0
    assert all(r["segment"] == "Enterprise" for r in rows)


def test_get_variance_breakdown_filters_by_period_and_segment():
    all_rows = json.loads(ai4.get_variance_breakdown.invoke({}))
    sample = all_rows[0]
    raw = ai4.get_variance_breakdown.invoke({
        "period_month": sample["period_month"], "segment": sample["segment"]
    })
    rows = json.loads(raw)
    assert len(rows) >= 1
    assert all(r["period_month"] == sample["period_month"] and r["segment"] == sample["segment"] for r in rows)


def test_get_cost_centre_breakdown_sorted_by_absolute_change():
    raw = ai4.get_cost_centre_breakdown.invoke({})
    rows = json.loads(raw)
    assert len(rows) > 0
    changes = [abs(r["mom_change_pct"]) if r["mom_change_pct"] is not None else 0 for r in rows]
    assert changes == sorted(changes, reverse=True)


def test_get_cost_centre_breakdown_filters_by_segment():
    raw = ai4.get_cost_centre_breakdown.invoke({"segment": "MidMarket"})
    rows = json.loads(raw)
    assert all(r["segment"] == "MidMarket" for r in rows)


def test_get_metric_definition_returns_governed_definition():
    raw = ai4.get_metric_definition.invoke({"metric_key": "revenue"})
    rows = json.loads(raw)
    assert len(rows) == 1
    assert rows[0]["metric_key"] == "revenue"
    assert "calculation_logic" in rows[0]


def test_get_metric_definition_returns_error_for_unknown_key():
    raw = ai4.get_metric_definition.invoke({"metric_key": "not_a_real_metric_xyz"})
    result = json.loads(raw)
    assert "error" in result


def test_tools_list_matches_tools_by_name_registry():
    assert len(ai4.TOOLS) == 3
    assert set(ai4.TOOLS_BY_NAME.keys()) == {
        "get_variance_breakdown", "get_cost_centre_breakdown", "get_metric_definition",
    }


def test_connect_readonly_actually_blocks_writes():
    """The module docstring's own guardrail claim: read_only=True enforced
    at the connection level. A write attempt through this connection must
    fail, not silently succeed."""
    conn = ai4._connect_readonly()
    with pytest.raises(Exception):
        conn.execute("CREATE TABLE should_never_be_created (x INT)")
    conn.close()
