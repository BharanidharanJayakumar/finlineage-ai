"""
AI-2 — Transformation Documentation (Groq Llama 3.3 70B, via LiteLLM gateway)
Reads each dbt model's SQL, sends it to Groq, gets back business-readable
documentation that finance teams and auditors can verify without reading SQL.

Phase 2, Wk 14: this call now goes through the LiteLLM gateway (Layer 8) instead
of the Groq SDK directly — see litellm_config.yaml for the "finlineage-docs" alias.

Also writes data/gold/transformation_docs_fingerprints.json — a sha256 of each
model's .sql content at the moment it was documented here. That manifest is
what ai6_doc_drift_detector.py compares against later to catch cases where the
SQL changes but this script doesn't get a chance to re-run (see that script's
docstring for why that's a real, not hypothetical, gap).
"""

import os
import sys
import json
import hashlib
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

MODELS_DIR = Path(__file__).resolve().parent.parent / "finlineage" / "models"

LITELLM_BASE_URL = os.getenv("LITELLM_BASE_URL", "http://localhost:4000")
LITELLM_MASTER_KEY = os.getenv("LITELLM_MASTER_KEY", "sk-finlineage-local-dev")


PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a data engineering documentation specialist.
Given a dbt SQL model, produce business-readable documentation that a finance team
member or auditor can understand WITHOUT reading SQL.

For each model, output:
1. **Purpose**: What this model does in one sentence.
2. **Business Logic**: The transformation rules in plain English (bullet points).
3. **Input Sources**: What upstream models/tables it reads from.
4. **Output Columns**: Key columns and what they represent.
5. **Lineage**: Where does the data come from, and where does it go next?
6. **Quality Controls**: What tests or checks are applied.

Keep each model's documentation under 200 words. Be precise — reference actual
column names and business rules from the SQL."""),
    ("human", """Document this dbt model:

**Model name:** {model_name}
**Layer:** {layer}

**SQL:**
````sql
{sql_content}
````

**Test YAML:**
````yaml
{yml_content}
```""")
])


def get_model_files():
    """Collect all .sql model files with their paired .yml files."""
    models = []
    for sql_file in sorted(MODELS_DIR.rglob("*.sql")):
        if sql_file.name.startswith("_"):
            continue
        yml_file = sql_file.with_suffix(".yml")
        layer = sql_file.parent.name  # staging / intermediate / marts

        yml_content = yml_file.read_text(encoding="utf-8") if yml_file.exists() else "No YAML file found."

        models.append({
            "model_name": sql_file.stem,
            "layer": layer,
            "sql_content": sql_file.read_text(encoding="utf-8"),
            "yml_content": yml_content,
        })
    return models


def run_ai2():
    print("\n" + "="*60)
    print("  AI-2 — Transformation Documentation (Groq Llama 3.3 70B)")
    print("="*60 + "\n")

    models = get_model_files()
    print(f"  Found {len(models)} dbt models to document")
    print(f"  Calling Groq via LiteLLM gateway ({LITELLM_BASE_URL})...\n")

    llm = ChatOpenAI(
        model="finlineage-docs",
        base_url=LITELLM_BASE_URL,
        api_key=LITELLM_MASTER_KEY,
        temperature=0.2,
        max_tokens=1000,
    )

    chain = PROMPT | llm | StrOutputParser()

    all_docs = []
    for m in models:
        print(f"  Documenting: {m['layer']}/{m['model_name']}...", end=" ", flush=True)
        doc = chain.invoke(m)
        all_docs.append(f"# {m['model_name']} ({m['layer']})\n\n{doc}")
        print("done")

    full_doc = "\n\n---\n\n".join(all_docs)

    # Save
    output_path = Path(__file__).resolve().parent.parent / "data" / "gold" / "transformation_docs.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(full_doc, encoding="utf-8")

    print(f"\n  Saved {len(models)} model docs to: {output_path.name}")
    print(f"  Total length: {len(full_doc):,} characters")

    # Fingerprint manifest for AI-6 (ai6_doc_drift_detector.py) — only reachable
    # here because every model above succeeded (an exception mid-loop propagates
    # before this line), so a fingerprint written here always means "this model's
    # doc genuinely reflects this exact SQL", never a partial/failed run.
    fingerprints = {
        m["model_name"]: {"layer": m["layer"], "sha256": hashlib.sha256(m["sql_content"].encode("utf-8")).hexdigest()}
        for m in models
    }
    fingerprints_path = output_path.parent / "transformation_docs_fingerprints.json"
    fingerprints_path.write_text(json.dumps(fingerprints, indent=2), encoding="utf-8")
    print(f"  Saved fingerprint manifest to: {fingerprints_path.name}")

    return full_doc


if __name__ == "__main__":
    docs = run_ai2()
    sys.exit(0 if docs else 1)
