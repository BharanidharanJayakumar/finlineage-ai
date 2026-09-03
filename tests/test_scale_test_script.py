"""
Wk17 — real unit tests for scripts/scale_test.py's _timed_run() helper (the
subprocess-invocation + UTF-8-encoding wrapper around every gate/dbt call in
the scale test). main() itself is a real 50K/20K-row integration run against
dbt/Docker and is intentionally NOT unit-tested here — it's covered by
actually running the scale test (see Section 4 evidence, EV-31).
"""
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import scale_test


def test_timed_run_returns_true_and_elapsed_on_success():
    fake_result = MagicMock(returncode=0, stdout="all good", stderr="")
    with patch("subprocess.run", return_value=fake_result) as mock_run:
        ok, elapsed = scale_test._timed_run("unit test step", ["echo", "hi"])
    assert ok is True
    assert elapsed >= 0
    mock_run.assert_called_once()


def test_timed_run_returns_false_on_nonzero_exit():
    fake_result = MagicMock(returncode=1, stdout="", stderr="boom")
    with patch("subprocess.run", return_value=fake_result):
        ok, elapsed = scale_test._timed_run("failing step", ["false"])
    assert ok is False


def test_timed_run_forces_utf8_env_and_encoding():
    fake_result = MagicMock(returncode=0, stdout="", stderr="")
    with patch("subprocess.run", return_value=fake_result) as mock_run:
        scale_test._timed_run("env check", ["echo"])
    _, kwargs = mock_run.call_args
    assert kwargs["env"]["PYTHONIOENCODING"] == "utf-8"
    assert kwargs["env"]["PYTHONUTF8"] == "1"
    assert kwargs["encoding"] == "utf-8"


def test_timed_run_merges_extra_env_on_top_of_os_environ(monkeypatch):
    monkeypatch.setenv("SOME_EXISTING_VAR", "keep_me")
    fake_result = MagicMock(returncode=0, stdout="", stderr="")
    with patch("subprocess.run", return_value=fake_result) as mock_run:
        scale_test._timed_run("env merge", ["echo"], env={"FINLINEAGE_GL_ROWS": "50000"})
    _, kwargs = mock_run.call_args
    assert kwargs["env"]["SOME_EXISTING_VAR"] == "keep_me"
    assert kwargs["env"]["FINLINEAGE_GL_ROWS"] == "50000"


def test_print_summary_and_restore_reminder_runs_without_error(capsys):
    report = [("step one", True, 1.23), ("step two", False, 4.56)]
    scale_test._print_summary_and_restore_reminder(report)
    out = capsys.readouterr().out
    assert "step one" in out
    assert "RESTORE THE OFFICIAL BASELINE" in out


def test_scale_row_targets_are_100x_the_official_baseline():
    assert scale_test.GL_ROWS == 50_000
    assert scale_test.PAYROLL_ROWS == 20_000
