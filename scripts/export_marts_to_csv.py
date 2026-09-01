"""
Export all mart tables from DuckDB to CSV files in data/gold/ for Power BI consumption.
Run after dbt build completes.
"""

from pathlib import Path
import duckdb

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "finlineage.duckdb"
GOLD = Path(__file__).resolve().parent.parent / "data" / "gold"
GOLD.mkdir(parents=True, exist_ok=True)

MARTS = [
    "mart_pnl_summary",
    "mart_revenue_trends",
    "mart_cost_analysis",
    "mart_variance_analysis",
    "mart_metric_dictionary",
]

def export():
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    for mart in MARTS:
        path = GOLD / f"{mart}.csv"
        df = conn.execute(f"SELECT * FROM main.{mart}").fetchdf()
        df.to_csv(path, index=False)
        print(f"  {mart}: {len(df)} rows -> {path.name}")
    conn.close()
    print("\nDone. Open these CSVs in Power BI Desktop.")

if __name__ == "__main__":
    export()