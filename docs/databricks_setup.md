# Databricks setup (Phase 2, Wk 11)

`scripts/sync_gold_to_databricks.py` pushes the 5 Gold mart tables into a
Databricks SQL warehouse as Delta tables. This is one-way (DuckDB stays the
pipeline's real execution engine) and additive — nothing else in the
pipeline depends on Databricks being configured, so skipping this section
doesn't break anything else in Phase 2.

## Getting a $0 workspace

Databricks **Free Edition** (the current replacement for the old Community
Edition) includes one SQL warehouse (2X-Small) at no cost — confirmed against
Databricks' own docs as of this writing:
https://docs.databricks.com/aws/en/getting-started/free-edition-limitations

1. Sign up at https://www.databricks.com/learn/free-edition
2. Once your workspace is up, open **SQL Warehouses** in the left sidebar —
   a warehouse should already exist (or start one; 2X-Small is the free tier).
3. Click the warehouse → **Connection details** tab. Copy:
   - **Server hostname** → `DATABRICKS_SERVER_HOSTNAME`
   - **HTTP path** → `DATABRICKS_HTTP_PATH`
4. Generate a personal access token: your profile icon (top right) →
   **Settings** → **Developer** → **Access tokens** → **Generate new token**.
   Copy it immediately (Databricks only shows it once) → `DATABRICKS_ACCESS_TOKEN`.

## What I couldn't verify from here

I don't have a Databricks account to test against, so two things are
unverified and worth checking once you have real credentials:
- Personal access token creation being enabled on Free Edition specifically
  (some free tiers restrict this for security — if the "Access tokens" page
  is missing or greyed out, that's what's happening).
- Whether your workspace uses Unity Catalog (in which case
  `DATABRICKS_CATALOG` should be your UC catalog name, not `hive_metastore`).
  The script defaults to `hive_metastore` for a legacy-style workspace; a
  new Free Edition workspace is more likely to be Unity Catalog-enabled, so
  you'll probably want to set `DATABRICKS_CATALOG` explicitly.

## Configure and run

Add to `.env` (see `.env.example`):
```
DATABRICKS_SERVER_HOSTNAME=dbc-xxxxxxxx-xxxx.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxxxxxxxxxxxxxx
DATABRICKS_ACCESS_TOKEN=dapi...
DATABRICKS_CATALOG=hive_metastore
DATABRICKS_SCHEMA=finlineage_gold
```

Install the connector and run:
```
pip install databricks-sql-connector
python scripts/sync_gold_to_databricks.py
```

Each run does `CREATE OR REPLACE TABLE` per mart — a full reload, not an
incremental append. That's deliberate for a POC of this size (the largest
mart is a few hundred rows): it keeps the script simple and means a run is
always a clean, correct snapshot rather than needing drift/upsert logic.

## Where this plugs into the pipeline

It's not wired into the Airflow DAG or CI by default — those don't have your
Databricks token, and shouldn't need to for the core pipeline to work. If you
want it to run automatically after a successful build, add a task to
`dags/finlineage_daily.py` after `dbt_build` (same pattern as the GE gates)
and a `DATABRICKS_*` block to `docker-compose.yml`'s airflow environment.
I left this as a manual step for now rather than wiring it in blind, since I
couldn't test it end-to-end without your credentials.
