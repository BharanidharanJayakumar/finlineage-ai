"""
FinLineage AI — Simulated Source Data Generator
Produces 3 CSV files in data/bronze/ that mimic a consulting company's financial systems:
  - erp_gl_entries.csv  : project-billing general ledger transactions (multi-currency)
  - payroll_entries.csv : employee cost allocations to projects
  - fx_rates.csv        : daily FX rates (INR base)

Run from project root:  python scripts/generate_sources.py
"""

import csv
import os
import random
from datetime import date, timedelta
from pathlib import Path

# Default 42 keeps every normal run reproducible (P&L reconciliation, dbt
# tests, evaluation evidence all depend on this staying deterministic).
# FINLINEAGE_SEED is an escape hatch for demos only — set it in .env to a
# different integer to get a genuinely different (still internally
# consistent) synthetic dataset flow all the way through to Power BI, so a
# refresh actually shows different numbers instead of just a new timestamp.
# Unset it (or delete the line) afterward to go back to the official seed.
SEED = int(os.environ.get("FINLINEAGE_SEED", "42"))
random.seed(SEED)
if SEED != 42:
    print(f"NOTE: using FINLINEAGE_SEED={SEED} (not the default 42) — demo/test data, not the reproducible baseline.")

# Wk 16 scale test escape hatch — same pattern as FINLINEAGE_SEED above.
# Defaults (500/200) are the official, evidence-bearing row counts; set
# these two env vars to generate a bigger dataset (see scripts/scale_test.py,
# which sets them to 50,000/20,000) without touching gen_erp_entries()/
# gen_payroll_entries()'s own code. Unset afterward (or just re-run with no
# overrides) to regenerate the official baseline.
GL_ROWS = int(os.environ.get("FINLINEAGE_GL_ROWS", "500"))
PAYROLL_ROWS = int(os.environ.get("FINLINEAGE_PAYROLL_ROWS", "200"))
if GL_ROWS != 500 or PAYROLL_ROWS != 200:
    print(f"NOTE: using FINLINEAGE_GL_ROWS={GL_ROWS}, FINLINEAGE_PAYROLL_ROWS={PAYROLL_ROWS} "
          "(not the default 500/200) — scale-test data, not the reproducible baseline. "
          "Re-run with no overrides afterward to restore the official dataset.")

BRONZE = Path(__file__).resolve().parent.parent / "data" / "bronze"
BRONZE.mkdir(parents=True, exist_ok=True)

# -- Reference data (small, consulting-company flavour) --
PROJECTS = [
    ("P-1001", "SLB-OFM-Web",       "Enterprise",  "USD"),
    ("P-1002", "HAL-DWP-Phase2",    "Enterprise",  "USD"),
    ("P-1003", "MidMarket-Sales",   "MidMarket",   "INR"),
    ("P-1004", "EU-Banking-Client", "Enterprise",  "EUR"),
    ("P-1005", "Internal-RnD",      "Internal",    "INR"),
    ("P-1006", "APAC-Logistics",    "MidMarket",   "AED"),
]
COST_CENTRES = ["CC-DEL", "CC-CHN", "CC-BLR", "CC-MUM", "CC-LON"]
ACCOUNTS = [
    ("4000", "Revenue - T&M",       "Revenue"),
    ("4010", "Revenue - Fixed Bid", "Revenue"),
    ("4020", "Revenue - Support",   "Revenue"),
    ("5000", "Direct Labour",       "COGS"),
    ("5100", "Subcontractor",       "COGS"),
    ("6000", "Travel",              "OpEx"),
    ("6100", "Software Licences",   "OpEx"),
    ("6200", "Marketing",           "OpEx"),
]
EMPLOYEES = [f"EMP-{i:04d}" for i in range(1, 31)]

START_DATE = date(2026, 1, 1)
END_DATE   = date(2026, 6, 30)


def daterange(start, end):
    """Yield each date from start (inclusive) to end (exclusive)."""
    for n in range((end - start).days):
        yield start + timedelta(days=n)


# ---------- 1) FX RATES ----------
def gen_fx_rates():
    rows = []
    # INR is base. Rates drift slightly day to day.
    base_rates = {"USD": 83.20, "EUR": 90.40, "GBP": 105.10, "AED": 22.65}
    for d in daterange(START_DATE, END_DATE):
        for ccy, rate in base_rates.items():
            drift = random.uniform(-0.5, 0.5)
            rows.append({
                "rate_date": d.isoformat(),
                "from_currency": ccy,
                "to_currency": "INR",
                "rate": round(rate + drift, 4),
            })
    return rows


# ---------- 2) ERP GENERAL LEDGER ----------
def gen_erp_entries(n=500):
    rows = []
    for i in range(1, n + 1):
        proj_code, proj_name, segment, proj_ccy = random.choice(PROJECTS)
        acct_code, acct_name, acct_type = random.choice(ACCOUNTS)
        cc = random.choice(COST_CENTRES)
        d = START_DATE + timedelta(days=random.randint(0, (END_DATE - START_DATE).days - 1))

        # Revenue entries are positive (credit-style), cost entries negative.
        if acct_type == "Revenue":
            amount = round(random.uniform(50_000, 500_000), 2)
        elif acct_type == "COGS":
            amount = -round(random.uniform(20_000, 200_000), 2)
        else:  # OpEx
            amount = -round(random.uniform(5_000, 80_000), 2)

        rows.append({
            "entry_id":      f"GL-{i:06d}",
            "entry_date":    d.isoformat(),
            "project_code":  proj_code,
            "project_name":  proj_name,
            "segment":       segment,
            "cost_centre":   cc,
            "account_code":  acct_code,
            "account_name":  acct_name,
            "account_type":  acct_type,
            "currency":      proj_ccy,
            "amount":        amount,
            "description":   f"{acct_name} for {proj_name}",
        })
    return rows


# ---------- 3) PAYROLL / EXPENSE ALLOCATION ----------
def gen_payroll_entries(n=200):
    rows = []
    for i in range(1, n + 1):
        emp = random.choice(EMPLOYEES)
        proj_code, proj_name, segment, _ = random.choice(PROJECTS)
        cc = random.choice(COST_CENTRES)
        d = START_DATE + timedelta(days=random.randint(0, (END_DATE - START_DATE).days - 1))
        hours = round(random.uniform(20, 160), 1)
        rate_per_hour = round(random.uniform(800, 3500), 2)
        rows.append({
            "allocation_id":    f"PAY-{i:06d}",
            "allocation_date":  d.isoformat(),
            "employee_id":      emp,
            "project_code":     proj_code,
            "cost_centre":      cc,
            "hours_logged":     hours,
            "rate_per_hour":    rate_per_hour,
            "cost_inr":         round(hours * rate_per_hour, 2),
        })
    return rows


def write_csv(rows, filename):
    path = BRONZE / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  wrote {len(rows):>4} rows -> {path.name}")


if __name__ == "__main__":
    print(f"Generating simulated source data into {BRONZE}\n")
    write_csv(gen_fx_rates(),                       "fx_rates.csv")
    write_csv(gen_erp_entries(GL_ROWS),              "erp_gl_entries.csv")
    write_csv(gen_payroll_entries(PAYROLL_ROWS),     "payroll_entries.csv")
    print("\nDone.")