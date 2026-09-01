"""
scripts/sync_gold_to_databricks.py — Databricks integration (Phase 2, Wk 11)

Pushes the 5 Gold mart tables from local DuckDB into a Databricks SQL
warehouse as Delta tables, so the same governed marts this project already
builds are queryable from Databricks too. This does NOT replace DuckDB —
DuckDB stays the pipeline's own execution engine; this is a one-way sync of
the finished marts outward, run after dbt build (and, in Docker, after the
GE post-gates pass).

Needs a Databricks workspace + SQL warehouse + personal access token — see
docs/databricks_setup.md for how to get one at $0 cost (Databricks Free
Edition, which replaced Community Edition, includes one 2X-Small SQL
warehouse — verified via Databricks' own docs as of this writing; if your
workspace looks different, that page is the source of truth, not this
comment). Set these in .env before running:

    DATABRICKS_SERVER_HOSTNAME   e.g. dbc-xxxxxxxx-xxxx.cloud.databricks.com
    DATABRICKS_HTTP_PATH         e.g. /sql/1.0/warehouses/xxxxxxxxxxxxxxxx
    DATABRICKS_ACCESS_TOKEN      a personal access token (starts dapi...)
    DATABRICKS_CATALOG           default: hive_metastore
    DATABRICKS_SCHEMA            default: finlineage_gold

Run: python scripts/sync_gold_to_databricks.py
"""

import os
import sys
import datetime
import decimal
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import duckdb
import numpy as np

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "finlineage.duckdb"

MARTS = [
    "mart_pnl_summary",
    "mart_revenue_trends",
    "mart_cost_analysis",
    "mart_variance_analysis",
    "mart_metric_dictionary",
]

# Rows per INSERT statement. Marts here are small (tens to low thousands of
# rows), so this stays well under the warehouse's statement-size limits
# without needing a staging/COPY INTO pipeline.
BATCH_SIZE = 500


def _databricks_type_for_column(series) -> str:
    """Infer a Databricks SQL column type from a column's actual Python
    values, not the pandas dtype string — duckdb's DECIMAL/DATE columns can
    come back as different pandas dtypes across versions, but the Python
    object type of an actual value is stable.

    Must check numpy scalar types explicitly (np.int64, np.bool_, ...), not
    just the builtin bool/int/float — pandas/duckdb hand back numpy scalars,
    and numpy.int64 is NOT an instance of Python's int (same for bool_), so
    isinstance(sample, int) silently misses every numeric column and falls
    through to STRING. Caught by testing this against the real marts before
    shipping — entry_count (BIGINT) and is_material_variance (BOOLEAN) both
    came back mistyped as STRING until this was added."""
    non_null = series.dropna()
    if non_null.empty:
        return "STRING"
    sample = non_null.iloc[0]
    if isinstance(sample, (bool, np.bool_)):
        return "BOOLEAN"
    if isinstance(sample, (int, np.integer)):
        return "BIGINT"
    if isinstance(sample, (float, np.floating, decimal.Decimal)):
        return "DOUBLE"
    if isinstance(sample, datetime.datetime):
        return "TIMESTAMP"
    if isinstance(sample, datetime.date):
        return "DATE"
    return "STRING"


def _sql_literal(value, col_type: str) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, float) and value != value:  # NaN
        return "NULL"
    if col_type == "BOOLEAN":
        return "TRUE" if value else "FALSE"
    if col_type in ("BIGINT", "DOUBLE"):
        return str(value)
    if col_type == "DATE":
        return f"CAST('{value}' AS DATE)"
    if col_type == "TIMESTAMP":
        return f"CAST('{value}' AS TIMESTAMP)"
    # STRING — quote and escape. All data here comes from our own governed
    # marts (not external input), so string-building is an acceptable
    # tradeoff for staying dependency-light rather than pulling in a
    # multi-row parameterized-insert helper.
    return "'" + str(value).replace("'", "''") + "'"


def get_gold_dataframes():
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    frames = {mart: conn.execute(f"SELECT * FROM main.{mart}").fetchdf() for mart in MARTS}
    conn.close()
    return frames


def sync_to_databricks(frames: dict) -> bool:
    try:
        from databricks import sql
    except ImportError:
        print("  ERROR: databricks-sql-connector not installed. "
              "Run: pip install databricks-sql-connector")
        return False

    host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    token = os.getenv("DATABRICKS_ACCESS_TOKEN")
    catalog = os.getenv("DATABRICKS_CATALOG", "hive_metastore")
    schema = os.getenv("DATABRICKS_SCHEMA", "finlineage_gold")

    if not all([host, http_path, token]):
        print("  ERROR: DATABRICKS_SERVER_HOSTNAME / DATABRICKS_HTTP_PATH / "
              "DATABRICKS_ACCESS_TOKEN must be set in .env. See docs/databricks_setup.md.")
        return False

    print(f"\n  Connecting to {host} ...")
    with sql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

            for mart, df in frames.items():
                table = f"{catalog}.{schema}.{mart}"
                col_types = [_databricks_type_for_column(df[c]) for c in df.columns]
                cols_sql = ", ".join(
                    f"`{c}` {t}" for c, t in zip(df.columns, col_types)
                )
                cur.execute(f"CREATE OR REPLACE TABLE {table} ({cols_sql}) USING DELTA")

                if df.empty:
                    print(f"  {mart}: 0 rows (table created, nothing to insert)")
                    continue

                cols_list = ", ".join(f"`{c}`" for c in df.columns)
                total = 0
                batch_rows = []
                for row in df.itertuples(index=False, name=None):
                    literals = [
                        _sql_literal(v, t) for v, t in zip(row, col_types)
                    ]
                    batch_rows.append("(" + ", ".join(literals) + ")")
                    if len(batch_rows) >= BATCH_SIZE:
                        cur.execute(f"INSERT INTO {table} ({cols_list}) VALUES {', '.join(batch_rows)}")
                        total += len(batch_rows)
                        batch_rows = []
                if batch_rows:
                    cur.execute(f"INSERT INTO {table} ({cols_list}) VALUES {', '.join(batch_rows)}")
                    total += len(batch_rows)

                print(f"  {mart}: {total} rows -> {table}")

    print("\n  Done. Gold marts are now queryable from Databricks SQL / notebooks.")
    return True


def run():
    print("\n" + "=" * 60)
    print("  Databricks Sync — Gold marts -> Delta tables")
    print("=" * 60)

    frames = get_gold_dataframes()
    print(f"  Loaded {len(frames)} mart tables from local DuckDB")

    return sync_to_databricks(frames)


if __name__ == "__main__":
    ok = run()
    sys.exit(0 if ok else 1)
