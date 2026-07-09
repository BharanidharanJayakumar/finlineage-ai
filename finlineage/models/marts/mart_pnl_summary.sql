-- mart_pnl_summary: Governed P&L summary by period (year-month) and segment.
-- Materialized as table for Power BI consumption.

{{ config(materialized='table') }}

with entries as (
    select * from {{ ref('int_entries_cost_centre_mapped') }}
),

pivoted as (
    select
        date_trunc('month', entry_date)::date          as period_month,
        segment_governed                                as segment,
        sum(case when account_type = 'Revenue' then amount_inr else 0 end)  as revenue,
        sum(case when account_type = 'COGS'    then amount_inr else 0 end)  as cogs,
        sum(case when account_type = 'OpEx'    then amount_inr else 0 end)  as opex,
        count(*)                                                              as entry_count
    from entries
    group by 1, 2
)

select
    period_month,
    segment,
    revenue,
    cogs,
    revenue + cogs                                                            as gross_margin,
    opex,
    revenue + cogs + opex                                                     as operating_income,
    case when revenue = 0 then null
         else round((revenue + cogs) / revenue * 100, 2)
    end                                                                       as gross_margin_pct,
    case when revenue = 0 then null
         else round((revenue + cogs + opex) / revenue * 100, 2)
    end                                                                       as operating_margin_pct,
    entry_count
from pivoted
order by period_month, segment