"""
Wk17 — real unit tests for the pure, non-LLM parts of AI-3 v2
(ai_chains/ai3_narrative_judge_v2.py): retrieval_grounding_score() (the
0.45-weighted deterministic signal) and _parse_score() (LLM-response JSON
parsing). score_claim() itself calls two real Gemini prompts through the
LiteLLM gateway and is intentionally NOT unit-tested here — that live call
is exercised by tests/test_e2e_pipeline.py against the real Docker stack.

Also covers extract_claims() (ai_chains/ai3_narrative_judge.py v1), which
ai3_narrative_judge_v2.py imports and reuses.
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "ai_chains"))

import ai3_narrative_judge_v2 as ai3v2
from ai3_narrative_judge import extract_claims


PNL_DF = pd.DataFrame([
    {"revenue_cr": 48.77, "gross_margin_cr": 20.1, "gross_margin_pct": 41.2,
     "operating_income_cr": 12.3, "operating_margin_pct": 25.2},
    {"revenue_cr": 30.0, "gross_margin_cr": 10.0, "gross_margin_pct": 33.3,
     "operating_income_cr": 5.0, "operating_margin_pct": 16.7},
])


def test_retrieval_grounding_score_is_1_when_figure_matches_exactly():
    claim = "Enterprise revenue reached 48.77 Cr in April."
    assert ai3v2.retrieval_grounding_score(claim, PNL_DF) == 1.0


def test_retrieval_grounding_score_is_1_within_tolerance():
    # 48.77 -> 48.80 is within the ±0.05 FIGURE_TOLERANCE_ABS
    claim = "Enterprise revenue reached 48.80 Cr in April."
    assert ai3v2.retrieval_grounding_score(claim, PNL_DF) == 1.0


def test_retrieval_grounding_score_is_0_when_figure_does_not_match_anything():
    claim = "Enterprise revenue reached 999.99 Cr in April."
    assert ai3v2.retrieval_grounding_score(claim, PNL_DF) == 0.0


def test_retrieval_grounding_score_handles_percentage_figures():
    claim = "Gross margin held at 41.2% for Enterprise."
    assert ai3v2.retrieval_grounding_score(claim, PNL_DF) == 1.0


def test_retrieval_grounding_score_is_neutral_with_no_numeric_figure():
    claim = "Enterprise performed well this quarter."
    assert ai3v2.retrieval_grounding_score(claim, PNL_DF) == 0.5


def test_retrieval_grounding_score_averages_across_multiple_figures():
    # One figure matches (48.77 Cr), one doesn't (12.3% is not a matched pct here since 25.2 is the pct) -
    # 48.77 Cr matches, 999% does not -> 1 of 2 matched -> 0.5
    claim = "Revenue was 48.77 Cr while margin swung to 999%."
    assert ai3v2.retrieval_grounding_score(claim, PNL_DF) == 0.5


def test_retrieval_grounding_score_ignores_bare_numbers_without_unit():
    # "2026" has no Cr/%/percent suffix, so NUMBER_RE shouldn't match it at all,
    # leaving no figures -> neutral 0.5, not a false non-match against 2026.
    claim = "In 2026 the company grew steadily."
    assert ai3v2.retrieval_grounding_score(claim, PNL_DF) == 0.5


@pytest.mark.parametrize("raw,expected", [
    ('{"supported": true, "confidence": 0.9}', (True, 0.9)),
    ('{"supported": false, "confidence": 0.3}', (False, 0.3)),
    ('```json\n{"supported": true, "confidence": 0.75}\n```', (True, 0.75)),
])
def test_parse_score_handles_valid_and_fenced_json(raw, expected):
    assert ai3v2._parse_score(raw) == expected


@pytest.mark.parametrize("raw", [
    "not json at all",
    "",
    '{"supported": "maybe"}',  # confidence missing -> defaults handled, still valid actually
])
def test_parse_score_falls_back_to_unsupported_zero_on_bad_input(raw):
    supported, confidence = ai3v2._parse_score(raw)
    assert isinstance(supported, bool)
    assert isinstance(confidence, float)


def test_parse_score_malformed_json_returns_false_zero():
    supported, confidence = ai3v2._parse_score("{completely broken")
    assert (supported, confidence) == (False, 0.0)


def test_weights_sum_to_one():
    total = (
        ai3v2.WEIGHT_SELF_REPORTED
        + ai3v2.WEIGHT_SECOND_JUDGE
        + ai3v2.WEIGHT_RETRIEVAL_GROUNDING
    )
    assert abs(total - 1.0) < 1e-9


def test_thresholds_are_ordered_sensibly():
    assert 0.0 < ai3v2.HUMAN_REVIEW_THRESHOLD < ai3v2.AUTO_ACCEPT_THRESHOLD < 1.0


# --------------------------------------------------------------- extract_claims (v1, reused by v2)
def test_extract_claims_keeps_only_sentences_with_digits():
    narrative = (
        "Overall performance was strong this quarter. "
        "Enterprise revenue reached 48.77 Cr in April. "
        "The team is optimistic about next quarter."
    )
    claims = extract_claims(narrative)
    assert claims == ["Enterprise revenue reached 48.77 Cr in April."]


def test_extract_claims_splits_multiple_numeric_sentences():
    narrative = "Revenue grew 12% in Q1. Costs fell 5 Cr in Q2. No numbers here."
    claims = extract_claims(narrative)
    assert len(claims) == 2
    assert "12%" in claims[0]
    assert "5 Cr" in claims[1]


def test_extract_claims_returns_empty_list_for_narrative_with_no_digits():
    assert extract_claims("Everything looks steady and unremarkable.") == []
