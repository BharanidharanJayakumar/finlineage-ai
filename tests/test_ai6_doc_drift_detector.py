"""
Wk17 — real unit tests for AI-6 (ai_chains/ai6_doc_drift_detector.py).

AI-6 is fully deterministic (sha256 fingerprint diff, no LLM call), and its
core logic (_sha256, compare(), load_documented_fingerprints()) is pure
enough to test directly with synthetic inputs — no dbt project or duckdb
needed for these. write_results()/run_ai6() ARE exercised against a
disposable db copy for the write path and the "no baseline yet" bootstrap
path.
"""
import json
import sys
from pathlib import Path

import duckdb
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "ai_chains"))

import ai6_doc_drift_detector as ai6


def test_sha256_is_deterministic_and_sensitive_to_content():
    a = ai6._sha256("select 1")
    b = ai6._sha256("select 1")
    c = ai6._sha256("select 2")
    assert a == b
    assert a != c
    assert len(a) == 64  # hex-encoded sha256


def test_compare_flags_never_documented_model():
    current = {"stg_foo": {"layer": "staging", "sha256": "abc"}}
    documented = {}
    results = ai6.compare(current, documented)
    assert len(results) == 1
    assert results[0]["drift_reason"] == "never_documented"
    assert results[0]["is_drifted"] is True


def test_compare_flags_sql_changed_since_docs():
    current = {"stg_foo": {"layer": "staging", "sha256": "new_hash"}}
    documented = {"stg_foo": {"layer": "staging", "sha256": "old_hash"}}
    results = ai6.compare(current, documented)
    assert len(results) == 1
    assert results[0]["drift_reason"] == "sql_changed_since_docs"
    assert results[0]["is_drifted"] is True


def test_compare_reports_in_sync_when_hashes_match():
    current = {"stg_foo": {"layer": "staging", "sha256": "same_hash"}}
    documented = {"stg_foo": {"layer": "staging", "sha256": "same_hash"}}
    results = ai6.compare(current, documented)
    assert len(results) == 1
    assert results[0]["drift_reason"] == "in_sync"
    assert results[0]["is_drifted"] is False


def test_compare_flags_orphaned_doc_entry():
    current = {}
    documented = {"stg_removed": {"layer": "staging", "sha256": "gone"}}
    results = ai6.compare(current, documented)
    assert len(results) == 1
    assert results[0]["drift_reason"] == "doc_entry_orphaned"
    assert results[0]["current_sql_hash"] is None


def test_compare_handles_a_realistic_mixed_batch():
    current = {
        "stg_a": {"layer": "staging", "sha256": "h1"},       # in sync
        "stg_b": {"layer": "staging", "sha256": "h2_new"},   # drifted
        "int_c": {"layer": "intermediate", "sha256": "h3"},  # never documented
    }
    documented = {
        "stg_a": {"layer": "staging", "sha256": "h1"},
        "stg_b": {"layer": "staging", "sha256": "h2_old"},
        "mart_d": {"layer": "marts", "sha256": "h4"},        # orphaned
    }
    results = ai6.compare(current, documented)
    reasons = {r["model_name"]: r["drift_reason"] for r in results}
    assert reasons == {
        "stg_a": "in_sync",
        "stg_b": "sql_changed_since_docs",
        "int_c": "never_documented",
        "mart_d": "doc_entry_orphaned",
    }


def test_load_documented_fingerprints_missing_file_returns_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(ai6, "FINGERPRINTS_PATH", tmp_path / "does_not_exist.json")
    assert ai6.load_documented_fingerprints() == {}


def test_load_documented_fingerprints_corrupt_json_returns_empty(tmp_path, monkeypatch):
    bad = tmp_path / "fingerprints.json"
    bad.write_text("{not valid json", encoding="utf-8")
    monkeypatch.setattr(ai6, "FINGERPRINTS_PATH", bad)
    assert ai6.load_documented_fingerprints() == {}


def test_load_documented_fingerprints_valid_json_roundtrips(tmp_path, monkeypatch):
    good = tmp_path / "fingerprints.json"
    payload = {"stg_a": {"layer": "staging", "sha256": "h1"}}
    good.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(ai6, "FINGERPRINTS_PATH", good)
    assert ai6.load_documented_fingerprints() == payload


def test_get_current_model_fingerprints_skips_underscore_prefixed_files(tmp_path, monkeypatch):
    models_dir = tmp_path / "models"
    (models_dir / "staging").mkdir(parents=True)
    (models_dir / "staging" / "stg_real.sql").write_text("select 1", encoding="utf-8")
    (models_dir / "staging" / "_macro_helper.sql").write_text("select 2", encoding="utf-8")
    monkeypatch.setattr(ai6, "MODELS_DIR", models_dir)
    fps = ai6.get_current_model_fingerprints()
    assert list(fps.keys()) == ["stg_real"]
    assert fps["stg_real"]["layer"] == "staging"


def test_run_ai6_bootstraps_as_never_documented_with_no_baseline(writable_db, tmp_path, monkeypatch):
    models_dir = tmp_path / "models"
    (models_dir / "staging").mkdir(parents=True)
    (models_dir / "staging" / "stg_only.sql").write_text("select 1", encoding="utf-8")
    monkeypatch.setattr(ai6, "MODELS_DIR", models_dir)
    monkeypatch.setattr(ai6, "FINGERPRINTS_PATH", tmp_path / "no_such_manifest.json")
    monkeypatch.setattr(ai6, "DB_PATH", writable_db)

    assert ai6.run_ai6() is True  # always non-blocking by design

    conn = duckdb.connect(str(writable_db), read_only=True)
    row = conn.execute(
        "SELECT model_name, drift_reason FROM doc_drift_log "
        "WHERE checked_at = (SELECT max(checked_at) FROM doc_drift_log)"
    ).fetchdf()
    conn.close()
    assert list(row["model_name"]) == ["stg_only"]
    assert list(row["drift_reason"]) == ["never_documented"]


def test_run_ai6_reports_in_sync_when_fingerprints_match(writable_db, tmp_path, monkeypatch):
    models_dir = tmp_path / "models"
    (models_dir / "staging").mkdir(parents=True)
    sql_file = models_dir / "staging" / "stg_only.sql"
    sql_file.write_text("select 1", encoding="utf-8")
    manifest = tmp_path / "fingerprints.json"
    manifest.write_text(json.dumps({
        "stg_only": {"layer": "staging", "sha256": ai6._sha256("select 1")}
    }), encoding="utf-8")

    monkeypatch.setattr(ai6, "MODELS_DIR", models_dir)
    monkeypatch.setattr(ai6, "FINGERPRINTS_PATH", manifest)
    monkeypatch.setattr(ai6, "DB_PATH", writable_db)

    assert ai6.run_ai6() is True

    conn = duckdb.connect(str(writable_db), read_only=True)
    row = conn.execute(
        "SELECT drift_reason FROM doc_drift_log "
        "WHERE checked_at = (SELECT max(checked_at) FROM doc_drift_log)"
    ).fetchdf()
    conn.close()
    assert list(row["drift_reason"]) == ["in_sync"]
