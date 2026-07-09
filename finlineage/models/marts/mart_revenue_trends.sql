-- mart_revenue_trends: Revenue by period, segment, and project.
-- Powers the Power BI Revenue Trends view with QoQ comparison.

{{ config(materialized='table') }}

with entries as (
    select * from {{ ref('int_entries_cost_centre_mapped') }}
    where account_type = 'Revenue'
),

monthly as (
    select
        date_trunc('month', entry_date)::date   as period_month,
        segment_governed                         as segment,
        project_code,
        project_name_governed                    as project_name,
        client_name,
        sum(amount_inr)                          as revenue_inr,
        count(*)                                 as entry_count
    from entries
    group by 1, 2, 3, 4, 5
)

select
    m.*,
    lag(m.revenue_inr) over (
        partition by m.segment, m.project_code
        order by m.period_month
    )                                            as prev_month_revenue,
    case
        when lag(m.revenue_inr) over (
            partition by m.segment, m.project_code
            order by m.period_month
        ) is null or lag(m.revenue_inr) over (
            partition by m.segment, m.project_code
            order by m.period_month
        ) = 0 then null
        else round(
            (m.revenue_inr - lag(m.revenue_inr) over (
                partition by m.segment, m.project_code
                order by m.period_month
            )) / abs(lag(m.revenue_inr) over (
                partition by m.segment, m.project_code
                order by m.period_month
            )) * 100, 2
        )
    end                                          as mom_change_pct
from monthly m
order by period_month, segment, project_code