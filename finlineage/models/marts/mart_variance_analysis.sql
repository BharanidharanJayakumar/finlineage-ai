-- mart_variance_analysis: Period-over-period variance by segment.
-- Compares each month's P&L to the prior month to surface material variances.
-- Powers the Power BI Variance Analysis view.

{{ config(materialized='table') }}

with pnl as (
    select * from {{ ref('mart_pnl_summary') }}
),

with_lag as (
    select
        period_month,
        segment,
        revenue,
        cogs,
        gross_margin,
        opex,
        operating_income,

        lag(revenue)          over w as prev_revenue,
        lag(cogs)             over w as prev_cogs,
        lag(gross_margin)     over w as prev_gross_margin,
        lag(opex)             over w as prev_opex,
        lag(operating_income) over w as prev_operating_income
    from pnl
    window w as (partition by segment order by period_month)
)

select
    period_month,
    segment,

    revenue,
    prev_revenue,
    revenue - coalesce(prev_revenue, 0)                                  as revenue_variance,
    case when prev_revenue is null or prev_revenue = 0 then null
         else round((revenue - prev_revenue) / abs(prev_revenue) * 100, 2)
    end                                                                   as revenue_variance_pct,

    gross_margin,
    prev_gross_margin,
    gross_margin - coalesce(prev_gross_margin, 0)                        as gm_variance,
    case when prev_gross_margin is null or prev_gross_margin = 0 then null
         else round((gross_margin - prev_gross_margin) / abs(prev_gross_margin) * 100, 2)
    end                                                                   as gm_variance_pct,

    operating_income,
    prev_operating_income,
    operating_income - coalesce(prev_operating_income, 0)                as oi_variance,
    case when prev_operating_income is null or prev_operating_income = 0 then null
         else round((operating_income - prev_operating_income) / abs(prev_operating_income) * 100, 2)
    end                                                                   as oi_variance_pct,

    -- Materiality flag: variance > 10% in either direction on operating income
    case when prev_operating_income is not null
          and prev_operating_income != 0
          and abs((operating_income - prev_operating_income) / abs(prev_operating_income)) > 0.10
         then true else false
    end                                                                   as is_material_variance

from with_lag
order by period_month, segment