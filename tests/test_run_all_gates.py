"""
Wk17 — real unit tests for ge/run_all_gates.py's orchestration logic (main()):
runs gates in pipeline order, stops at the first failure, and separates
pre-dbt from post-dbt gates correctly. The 6 real gate functions are
monkeypatched here (they're each tested for real in test_ge_gates.py) so
this file tests ONLY the orchestration, not gate logic duplicated elsewhere.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ge"))

import run_all_gates


def _patch_all_gates(monkeypatch, results: dict):
    """results: {module_name: bool} — patches each gate module's run_gN
    function to return the given bool without doing any real work."""
    import importlib
    for module_name, _label in run_all_gates.GATES:
        mod = importlib.import_module(module_name)
        fn_name = f"run_{module_name.split('_', 1)[0]}"
        monkeypatch.setattr(mod, fn_name, lambda *, _r=results[module_name]: _r)


def test_main_returns_true_when_every_gate_passes(monkeypatch, capsys):
    _patch_all_gates(monkeypatch, {name: True for name, _ in run_all_gates.GATES})
    assert run_all_gates.main() is True
    out = capsys.readouterr().out
    assert "ALL GATES PASSED" in out


def test_main_stops_at_first_pre_dbt_failure_and_never_runs_post_dbt(monkeypatch, capsys):
    results = {name: True for name, _ in run_all_gates.GATES}
    results["g2_fx_rate_coverage"] = False
    _patch_all_gates(monkeypatch, results)

    assert run_all_gates.main() is False
    out = capsys.readouterr().out
    assert "PIPELINE BLOCKED at G2" in out
    assert "POST-DBT GATES" not in out  # never reached


def test_main_stops_at_first_post_dbt_failure(monkeypatch, capsys):
    results = {name: True for name, _ in run_all_gates.GATES}
    results["g5_cross_mart_consistency"] = False
    _patch_all_gates(monkeypatch, results)

    assert run_all_gates.main() is False
    out = capsys.readouterr().out
    assert "PRE-DBT GATES" in out  # pre-dbt gates did run
    assert "PIPELINE BLOCKED at G5" in out


def test_gates_list_is_split_3_pre_dbt_3_post_dbt():
    assert len(run_all_gates.GATES) == 6
    names = [n for n, _ in run_all_gates.GATES]
    assert names[:3] == ["g1_raw_gl_completeness", "g2_fx_rate_coverage", "g3_payroll_completeness"]
    assert names[3:] == [
        "g4_pnl_aggregation_consistency", "g5_cross_mart_consistency", "g6_metric_drift",
    ]
