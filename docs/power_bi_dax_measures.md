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

## `is_material_variance` — not a measure

It's already a boolean column on `mart_variance_analysis`, meant for
filtering/slicing or conditional formatting, not aggregation. Per the
existing Known Issues note in the knowledge base: format conditionally on
`oi_variance_pct` (numeric) rather than `is_material_variance` (text) —
Power BI's conditional formatting on booleans-as-text doesn't render the
way you'd expect.
