"""
Wk17 — real unit tests for ai_chains/ai2_transformation_docs.py.
get_model_files() is pure I/O (no LLM) and fully testable against a synthetic
models directory. run_ai2() is exercised end-to-end with the LangChain chain
mocked out (no real Groq/LiteLLM call), verifying the saved doc file AND the
fingerprint manifest AI-6 depends on.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ai_chains"))

import ai2_transformation_docs as ai2


class _FakeChain:
    def __or__(self, other):
        return self

    def invoke(self, inputs):
        return f"DOC for {inputs['model_name']}"


def _make_models_dir(tmp_path):
    models_dir = tmp_path / "models"
    (models_dir / "staging").mkdir(parents=True)
    (models_dir / "marts").mkdir(parents=True)
    (models_dir / "staging" / "stg_a.sql").write_text("select 1 as a", encoding="utf-8")
    (models_dir / "staging" / "stg_a.yml").write_text("version: 2", encoding="utf-8")
    (models_dir / "staging" / "_helper_macro.sql").write_text("select 2", encoding="utf-8")
    (models_dir / "marts" / "mart_b.sql").write_text("select 2 as b", encoding="utf-8")
    return models_dir


def test_get_model_files_skips_underscore_prefixed_and_pairs_yaml(tmp_path, monkeypatch):
    models_dir = _make_models_dir(tmp_path)
    monkeypatch.setattr(ai2, "MODELS_DIR", models_dir)

    models = ai2.get_model_files()
    names = {m["model_name"] for m in models}
    assert names == {"stg_a", "mart_b"}

    stg_a = next(m for m in models if m["model_name"] == "stg_a")
    assert stg_a["layer"] == "staging"
    assert stg_a["yml_content"] == "version: 2"
    assert stg_a["sql_content"] == "select 1 as a"


def test_get_model_files_defaults_yaml_content_when_missing(tmp_path, monkeypatch):
    models_dir = _make_models_dir(tmp_path)
    monkeypatch.setattr(ai2, "MODELS_DIR", models_dir)

    models = ai2.get_model_files()
    mart_b = next(m for m in models if m["model_name"] == "mart_b")
    assert mart_b["yml_content"] == "No YAML file found."


def test_run_ai2_writes_doc_and_fingerprint_manifest(tmp_path, monkeypatch):
    models_dir = _make_models_dir(tmp_path)
    monkeypatch.setattr(ai2, "MODELS_DIR", models_dir)
    monkeypatch.setattr(ai2, "PROMPT", _FakeChain())
    monkeypatch.setattr(ai2, "ChatOpenAI", lambda **kw: _FakeChain())

    fake_gold = tmp_path / "gold_out"

    def _fake_resolve_path():
        return fake_gold / "transformation_docs.md"

    # run_ai2() computes output_path from Path(__file__)... — redirect via
    # monkeypatching Path(__file__).resolve().parent.parent, simplest is to
    # monkeypatch the module's own __file__ so its internal Path(__file__)
    # calls resolve under tmp_path instead of the real project tree.
    monkeypatch.setattr(ai2, "__file__", str(tmp_path / "fake_pkg" / "ai2_transformation_docs.py"))

    full_doc = ai2.run_ai2()

    assert "DOC for stg_a" in full_doc
    assert "DOC for mart_b" in full_doc

    output_path = tmp_path / "fake_pkg" / ".." / "data" / "gold" / "transformation_docs.md"
    output_path = output_path.resolve()
    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8") == full_doc

    fp_path = output_path.parent / "transformation_docs_fingerprints.json"
    fingerprints = json.loads(fp_path.read_text(encoding="utf-8"))
    assert set(fingerprints.keys()) == {"stg_a", "mart_b"}
    assert fingerprints["stg_a"]["layer"] == "staging"
    assert len(fingerprints["stg_a"]["sha256"]) == 64
