"""
AI-6 — Documentation Drift Detector (Phase 2, Wk 14 roadmap item, added 2026-09-02)

Deterministic, no-LLM check (₹0, no gateway/API dependency) that answers one
question: does data/gold/transformation_docs.md (AI-2's output) still
accurately describe the dbt SQL it claims to document?

Why this needs to be a separate chain rather than trusting AI-2 to stay fresh
on its own: AI-2 regenerates the full doc from the CURRENT SQL every time it
runs successfully, so in principle staleness shouldn't be possible. But
AI-2 can fail or be skipped on any given DAG run (a provider outage, or — as
actually happened live on 2026-09-02 — Gemini/Groq free-tier quota exhausted
mid-testing) while dbt models keep changing regardless. Nothing else in the
pipeline notices when that happens; the last-committed docs just quietly
drift out of sync. This script is what notices.

How it works: AI-2 now writes a companion fingerprint manifest
(data/gold/transformation_docs_fingerprints.json — sha256 of each model's
.sql content at the moment it was last successfully documented) alongside
transformation_docs.md. This script recomputes each model's CURRENT sha256
and diffs against that manifest. No LLM call is involved — this is pure
"did the bytes change" detection, same philosophy as G6's deterministic
hard-checks (ge/g6_metric_drift.py).

Non-blocking by design (unlike the GE gates): stale documentation shouldn't
halt a financial pipeline run, it should just be visible. Always exits 0;
drift is logged to the doc_drift_log DuckDB table and printed as a WARNING
banner, not raised as a failure.

Known limitation, documented rather than hidden: data/gold/ is gitignored
(see project knowledge base, Section 4 — "AI outputs + mart CSVs
(gitignored)"), so the fingerprint manifest does NOT persist across a fresh
git clone or an ephemeral CI runner — only across runs on the same machine
against the same data/ directory (which is exactly how the Airflow DAG uses
it, via the docker-compose ./data volume mount). On a machine/CI run that's
never seen this manifest before, every model bootstraps as "never_documented
until first check" rather than a true drift comparison — see
_run_or_bootstrap() below.
"""

import sys
import json
import uuid
import hashlib
from pathlib import Path
from datetime import datetime, timezone

import duckdb

MODELS_DIR = Path(__file__).resolve().parent.parent / "finlineage" / "models"
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "finlineage.duckdb"
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "doc_drift_schema.sql"
FINGERPRINTS_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "gold" / "transformation_docs_fingerprints.json"
)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def get_current_model_fingerprints() -> dict:
    """Same model discovery as ai2_transformation_docs.py's get_model_files()
    (deliberately not imported from there — this script has no LangChain/LLM
    dependency by design, and shouldn't gain one just to reuse a 5-line loop)."""
    fingerprints = {}
    for sql_file in sorted(MODELS_DIR.rglob("*.sql")):
        if sql_file.name.startswith("_"):
            continue
        fingerprints[sql_file.stem] = {
            "layer": sql_file.parent.name,
            "sha256": _sha256(sql_file.read_text(encoding="utf-8")),
        }
    return fingerprints


def load_documented_fingerprints() -> dict:
    if not FINGERPRINTS_PATH.exists():
        return {}
    try:
        return json.loads(FINGERPRINTS_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        # Corrupt manifest shouldn't crash the DAG — treat as "no baseline",
        # same as a missing file. AI-2 will overwrite it clean on its next run.
        return {}


def compare(current: dict, documented: dict) -> list:
    """Returns one result dict per model actually on disk right now, plus one
    per orphaned doc entry (documented before, no longer exists)."""
    results = []

    for model_name, cur in current.items():
        doc = documented.get(model_name)
        if doc is None:
            results.append({
                "model_name": model_name,
                "layer": cur["layer"],
                "current_sql_hash": cur["sha256"],
                "documented_sql_hash": None,
                "is_drifted": True,
                "drift_reason": "never_documented",
            })
        elif doc["sha256"] != cur["sha256"]:
            results.append({
                "model_name": model_name,
                "layer": cur["layer"],
                "current_sql_hash": cur["sha256"],
                "documented_sql_hash": doc["sha256"],
                "is_drifted": True,
                "drift_reason": "sql_changed_since_docs",
            })
        else:
            results.append({
                "model_name": model_name,
                "layer": cur["layer"],
                "current_sql_hash": cur["sha256"],
                "documented_sql_hash": doc["sha256"],
                "is_drifted": False,
                "drift_reason": "in_sync",
            })

    for model_name, doc in documented.items():
        if model_name not in current:
            results.append({
                "model_name": model_name,
                "layer": doc.get("layer", "unknown"),
                "current_sql_hash": None,
                "documented_sql_hash": doc["sha256"],
                "is_drifted": True,
                "drift_reason": "doc_entry_orphaned",
            })

    return results


def write_results(results: list):
    conn = duckdb.connect(str(DB_PATH), read_only=False)
    conn.execute(SCHEMA_PATH.read_text(encoding="utf-8"))
    checked_at = datetime.now(timezone.utc)
    for r in results:
        conn.execute("""
            INSERT INTO doc_drift_log (
                check_id, checked_at, model_name, layer,
                current_sql_hash, documented_sql_hash, is_drifted, drift_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            str(uuid.uuid4()), checked_at, r["model_name"], r["layer"],
            r["current_sql_hash"], r["documented_sql_hash"], r["is_drifted"], r["drift_reason"],
        ])
    conn.close()


def run_ai6():
    print("\n" + "=" * 60)
    print("  AI-6 — Documentation Drift Detector")
    print("=" * 60 + "\n")

    current = get_current_model_fingerprints()
    print(f"  Found {len(current)} dbt models on disk")

    if not FINGERPRINTS_PATH.exists():
        print("  No fingerprint baseline found (data/gold/transformation_docs_fingerprints.json "
              "missing) — this is expected on a fresh checkout/CI runner (data/gold/ is "
              "gitignored) or the very first run since this check was added.")
        print("  Nothing to compare against yet; run ai2_transformation_docs.py to establish "
              "a baseline. Logging all models as 'never_documented' for visibility.\n")

    documented = load_documented_fingerprints()
    results = compare(current, documented)
    write_results(results)

    drifted = [r for r in results if r["is_drifted"]]
    in_sync = [r for r in results if not r["is_drifted"]]

    print(f"\n  {len(in_sync)}/{len(results)} models in sync with transformation_docs.md")
    if drifted:
        print(f"\n  WARNING: {len(drifted)} model(s) drifted:")
        for r in drifted:
            print(f"    - {r['layer']}/{r['model_name']}: {r['drift_reason']}")
        print("\n  Re-run ai2_transformation_docs.py to refresh the docs for these models.")
    else:
        print("  No drift detected.")

    print(f"\n  {len(results)} rows written to doc_drift_log in finlineage.duckdb")
    print(f"\n{'='*60}\n")

    # Non-blocking by design (see module docstring) — always success. Drift is
    # something to see and act on, not something that should fail the pipeline.
    return True


if __name__ == "__main__":
    ok = run_ai6()
    sys.exit(0 if ok else 1)
