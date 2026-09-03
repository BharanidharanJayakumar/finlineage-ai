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
    # Not a dbt mart, but Power BI's AI headline cards (Wk 15/16) read it the
    # same way as everything else in this .pbix — via a CSV in data/gold/,
    # not a live DuckDB connection. Added 2026-09-03 after the Revenue
    # Trends card's new row didn't show up in Power BI: this script never
    # exported the table, so the .pbix's copy was stale from whenever it was
    # first loaded. Every future run of this script now keeps it current.
    "ai_narrative_snippets",
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