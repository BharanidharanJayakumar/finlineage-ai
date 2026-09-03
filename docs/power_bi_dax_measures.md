# Power BI DAX measures (Phase 2, Wk 13)

Governed measures for `finlineage_report.pbix`, generated from
`mart_metric_dictionary` so every number in Power BI traces back to the same
calculation logic as the dbt marts. I can't edit the `.pbix` file directly
from here — it's a binary format that needs Power BI Desktop itself (no
command-line editing without extra tooling like Tabular Editor, which isn't
installed) — so this is the copy-paste half of the work. To add one: open
`finlineage_report.pbix` in Power BI Desktop → right-click the table in the
**Fields** pane → **New measure** → paste → Enter.

## Why these aren't just `AVERAGE()` of the mart's precomputed % column

`mart_pnl_summary`, `mart_variance_analysis`, etc. already carry precomputed
percentage columns (`gross_margin_pct`, `oi_variance_pct`, ...) — dbt did
that math correctly at the mart's native grain (one row per period+segment).
But averaging a column of percentages across rows is wrong the moment Power
BI's filter context spans more than one row — e.g. select two segments and
`AVERAGE(gross_margin_pct)` gives you the average of two *already-computed
ratios*, not the true blended margin across both segments' actual revenue
and cost. The measures below instead re-derive each ratio from `SUM()` of
the underlying additive amounts, using `DIVIDE()` (safe against divide-by-
zero), so they stay correct at whatever grain the report visual is at —
totals, single segment, single period, anything.

## Base measures — `mart_pnl_summary`

```dax
Total Revenue = SUM(mart_pnl_summary[revenue])

Total COGS = SUM(mart_pnl_summary[cogs])

Total Gross Margin = SUM(mart_pnl_summary[gross_margin])

Total OpEx = SUM(mart_pnl_summary[opex])

Total Operating Income = SUM(mart_pnl_summary[operating_income])
```

## Recomputed ratio measures — correct at any filter context

```dax
Gross Margin % =
DIVIDE ( [Total Gross Margin], [Total Revenue], BLANK () ) * 100

Operating Margin % =
DIVIDE ( [Total Operating Income], [Total Revenue], BLANK () ) * 100
```

Governed definitions these match (`mart_metric_dictionary`):
`gross_margin_pct = (revenue + cogs) / revenue * 100`,
`operating_margin_pct = (revenue + cogs + opex) / revenue * 100` — identical
math, just re-aggregated correctly instead of averaged.

## `mart_cost_analysis`

```dax
Total Cost = SUM(mart_cost_analysis[cost_inr])
```

## `mart_revenue_trends`

```dax
Total Trend Revenue = SUM(mart_revenue_trends[revenue_inr])
```

## Month-over-month change % and variance % — read the caveat first

`revenue_mom_change`, `cost_mom_change`, and `oi_variance_pct` are different
from the measures above: dbt computed them with a *window function*
(`LAG() OVER (PARTITION BY segment ORDER BY period_month)`), comparing each
row to the previous row **at the mart's native grain**. A `DIVIDE()`-based
re-aggregation like the ones above doesn't work here — "current vs. prior
period" isn't expressible as a SUM ratio, it genuinely needs either the
mart's precomputed prior-period column, or real DAX time intelligence
(`DATEADD`, `PARALLELPERIOD`) against a proper Date table, which this
`.pbix` doesn't have set up (there's no marked Date table in the model as
far as I can tell from the mart schemas — I couldn't open the `.pbix` itself
to confirm, since it's binary).

Two honest options, pick based on how the report visual is built:

**Option A — safe, matches the mart exactly, but only correct when the
visual is at the mart's native grain** (one row per period+segment for
variance; per period+segment+project for revenue trends;
per period+segment+cost_centre+account for cost analysis). If your table
visual shows exactly those columns, use the precomputed value directly:

```dax
OI Variance % (native grain) = SELECTEDVALUE(mart_variance_analysis[oi_variance_pct])

Revenue MoM % (native grain) = SELECTEDVALUE(mart_revenue_trends[mom_change_pct])

Cost MoM % (native grain) = SELECTEDVALUE(mart_cost_analysis[mom_change_pct])
```

`SELECTEDVALUE` returns blank (not a wrong number) the moment the filter
context spans more than one native-grain row — safer than `AVERAGE`, which
would silently give you a misleading number instead of a visible blank.

**Option B — correct at any grain, needs a real Date table.** If you want
these to work when rolled up (e.g. quarterly, or across segments), mark
`period_month` as a Date table (or build a proper calendar table and relate
it), then:

```dax
Prior Period Operating Income =
CALCULATE ( [Total Operating Income], DATEADD ( 'Date'[period_month], -1, MONTH ) )

OI Variance % (any grain) =
DIVIDE (
    [Total Operating Income] - [Prior Period Operating Income],
    ABS ( [Prior Period Operating Income] ),
    BLANK ()
) * 100
```

I'm not setting this up automatically since it changes the model (new Date
table + relationships) rather than just adding a measure — that's a deliberate
model change worth doing in Power BI Desktop where you can see the effect,
not something to paste blind.

## `ai_narrative_snippets` — dynamic AI headline card (Wk 15, added 2026-09-03)

AI-1 now also writes a short, bounded headline into `ai_narrative_snippets`
every run (see `ai_narrative_snippets_schema.sql`) — same Gemini call that
already produces the full P&L narrative, no extra LLM call. It's an
append-only log (one row per run), so the card needs to pick out the LATEST
row for its `snippet_type`, never just `SUM`/`AVERAGE` the column:

```dax
PnL Executive Headline =
VAR LatestTime =
    CALCULATE (
        MAX ( ai_narrative_snippets[generated_at] ),
        ai_narrative_snippets[snippet_type] = "pnl_executive_headline"
    )
RETURN
    CALCULATE (
        SELECTEDVALUE ( ai_narrative_snippets[snippet_text] ),
        ai_narrative_snippets[snippet_type] = "pnl_executive_headline",
        ai_narrative_snippets[generated_at] = LatestTime
    )
```

Add this measure on the `ai_narrative_snippets` table, then drop it into a
Card visual on the P&L Summary page (replacing the static "Enterprise drives
90%+..." text box) — it updates on its own every time the pipeline runs and
`sync_gold_to_databricks.py` pushes a fresh row, no manual editing. More
`snippet_type` values (e.g. for Variance Analysis) get the same treatment —
one new `CALCULATE(... snippet_type = "...")` measure each, added one BI
card at a time as agreed.

### Revenue Trends card (Wk 15/16, added 2026-09-03 — second increment)

`ai1_pnl_narrative.py` now also makes a small second Gemini call
(`run_ai1_revenue_headline()`) reading `mart_revenue_trends`, writing
`snippet_type='revenue_trends_headline'` to the same table. Same
latest-row-per-type pattern as the P&L measure above, just a different
`snippet_type` filter:

```dax
Revenue Trends Executive Headline =
VAR LatestTime =
    CALCULATE (
        MAX ( ai_narrative_snippets[generated_at] ),
        ai_narrative_snippets[snippet_type] = "revenue_trends_headline"
    )
RETURN
    CALCULATE (
        SELECTEDVALUE ( ai_narrative_snippets[snippet_text] ),
        ai_narrative_snippets[snippet_type] = "revenue_trends_headline",
        ai_narrative_snippets[generated_at] = LatestTime
    )
```

Add this measure the same way (right-click `ai_narrative_snippets` in the
**Fields** pane → **New measure** → paste → Enter), then drop it into a Card
visual on the Revenue Trends page, replacing the static "Enterprise drives
90%+ with MidMarket showing high MoM volatility" insight text called out in
the Mid-Term doc's EV-12.

**One real difference from the P&L card, worth knowing before you rely on
it:** unlike the P&L headline (same LLM call as the full narrative, so it's
as reliable as `ai1_pnl_narrative` succeeding at all), this one is a
second, independent Gemini call inside the same task, wrapped as
best-effort/non-fatal against the 20-requests/day shared quota — see that
function's docstring in `ai_chains/ai1_pnl_narrative.py`. If it hits the
quota on a given run, the card simply keeps showing the last successfully
generated headline rather than going blank or erroring the visual.

**Two real bugs hit and fixed getting this card live (2026-09-03), worth
knowing if the card ever goes blank again:**

1. `scripts/sync_gold_to_databricks.py`'s `MARTS` list never included
   `ai_narrative_snippets` — this .pbix's `ai_narrative_snippets` query
   reads from Databricks (`workspace.finlineage_gold.ai_narrative_snippets`),
   same as the 5 real marts, so any new headline written to local DuckDB
   never reached the copy Power BI was reading no matter how many times you
   refreshed. Fixed by adding it to that script's `MARTS` list, mirroring
   the identical fix already made in `export_marts_to_csv.py`.
2. Running Power Query's **Start Diagnostics** tool against the
   `ai_narrative_snippets` query left 4 permanent `Diagnostics` queries
   (`..._Counters`, `..._Detailed_`, `..._Aggregated_`, `..._Partition_`)
   saved into the .pbix. Because a diagnostics query's job is to capture the
   evaluation trace of the query it profiled, its mere presence in the model
   made Power BI's engine detect a cyclic reference and block refresh for
   every query sharing that Databricks connection (6 queries, not just this
   one) — visible as `Refresh → "A cyclic reference was encountered during
   evaluation"` on `ai_narrative_snippets`. Fixed by deleting all 4
   diagnostics queries from the Queries pane. If diagnostics are ever run
   again for troubleshooting, delete the resulting queries afterwards rather
   than leaving them in the saved file.

**Repeatable refresh pattern for the demo**, proven working end-to-end:
`python ai_chains/ai1_pnl_narrative.py` (generates both headlines) →
`python scripts/sync_gold_to_databricks.py` (pushes the fresh
`ai_narrative_snippets` rows to Databricks, same as the 5 marts) → Power BI
Desktop **Home → Refresh**. Verified live: the Revenue Trends card now shows
"Enterprise drives revenue, contributing over 80% each month, peaking at
₹48.77 Cr in April." — a real, dynamically generated headline, not the
static Mid-Term text box.

### Cost Analysis card (Wk 15/16, added 2026-09-03 — third increment)

```dax
Cost Analysis Executive Headline =
VAR LatestTime =
    CALCULATE (
        MAX ( ai_narrative_snippets[generated_at] ),
        ai_narrative_snippets[snippet_type] = "cost_analysis_headline"
    )
RETURN
    CALCULATE (
        SELECTEDVALUE ( ai_narrative_snippets[snippet_text] ),
        ai_narrative_snippets[snippet_type] = "cost_analysis_headline",
        ai_narrative_snippets[generated_at] = LatestTime
    )
```

Drop into a Card visual on the Cost Analysis page.

### Variance Analysis cards — 2 (Wk 15/16, added 2026-09-03 — fourth/fifth increment)

Two cards, replacing the two static insight text boxes the page shipped
with ("Mid Market shows extreme volatility..." and "Enterprise and Internal
segments show moderate but consistent variances."):

```dax
Variance Volatility Headline =
VAR LatestTime =
    CALCULATE (
        MAX ( ai_narrative_snippets[generated_at] ),
        ai_narrative_snippets[snippet_type] = "variance_volatility_headline"
    )
RETURN
    CALCULATE (
        SELECTEDVALUE ( ai_narrative_snippets[snippet_text] ),
        ai_narrative_snippets[snippet_type] = "variance_volatility_headline",
        ai_narrative_snippets[generated_at] = LatestTime
    )

Variance Stability Headline =
VAR LatestTime =
    CALCULATE (
        MAX ( ai_narrative_snippets[generated_at] ),
        ai_narrative_snippets[snippet_type] = "variance_stability_headline"
    )
RETURN
    CALCULATE (
        SELECTEDVALUE ( ai_narrative_snippets[snippet_text] ),
        ai_narrative_snippets[snippet_type] = "variance_stability_headline",
        ai_narrative_snippets[generated_at] = LatestTime
    )
```

`Variance Volatility Headline` replaces the "Mid Market..." box, `Variance
Stability Headline` replaces the "Enterprise and Internal..." box — both as
Card visuals in the same positions.

### Pipeline & AI Operations Status page (Wk 15/16, added 2026-09-03)

The 5th page (previously a raw `ai_narrative_snippets` table dump plus a
"Last Refreshed" card) is repurposed into one consolidated status view:
the existing "Last Refreshed" card (bound to `MAX(_pipeline_sync_log[synced_at])`)
stays, and the raw table is replaced with a table visual filtered to the
*latest row per snippet_type only* — 5 rows (one per card headline) instead
of every historical row ever generated:

```dax
Is Latest Snippet Per Type =
VAR ThisType = ai_narrative_snippets[snippet_type]
VAR ThisTime = ai_narrative_snippets[generated_at]
VAR LatestForType =
    CALCULATE (
        MAX ( ai_narrative_snippets[generated_at] ),
        ALLEXCEPT ( ai_narrative_snippets, ai_narrative_snippets[snippet_type] )
    )
RETURN
    ThisTime = LatestForType
```

Add this as a calculated column on `ai_narrative_snippets`, then on the
table visual: Filters pane → drag `Is Latest Snippet Per Type` into
Filters on this visual → set to `is true`. Columns to show: `snippet_type`,
`snippet_text`, `generated_at`. Optionally add a second card for
`Total rows synced` (`SUM(_pipeline_sync_log[total_rows_synced])` for the
latest `sync_run_id`) alongside "Last Refreshed" for a one-glance pipeline
health summary.

## `is_material_variance` — not a measure

It's already a boolean column on `mart_variance_analysis`, meant for
filtering/slicing or conditional formatting, not aggregation. Per the
existing Known Issues note in the knowledge base: format conditionally on
`oi_variance_pct` (numeric) rather than `is_material_variance` (text) —
Power BI's conditional formatting on booleans-as-text doesn't render the
way you'd expect.
